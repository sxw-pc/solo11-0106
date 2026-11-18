"""内容寻址块仓 + 每来源有序 manifest 的在线归档。

- 块仓不可改写：块按内容 SHA-256 命名，写 tmp 后原子 rename，同内容只物理存一次。
- 两种无损表示：fixed_64 每 64 字节切块；record_chunks 每条原始 JSONL 行连同
  原分隔字节为一块，前导 BOM 与无末尾换行的末尾状态各自独立成块。
- 迁移期间旧 raw BLOB 与新 manifest 并存；只有重构字节与旧 BLOB/SHA 完全一致
  才发布、切换读指针（source_state.generation 比较交换）并清理冗余 BLOB。
- GC 只回收不被当前/旧已发布 manifest、building manifest 或活跃读者钉住的块，
  不按固定宽限秒数猜测。
"""
import hashlib
import json
import os
import uuid
from pathlib import Path
from .store import connect, source_state, switch_generation, prune_raw

BOM = b'\xef\xbb\xbf'
REPS = ('fixed_64', 'record_chunks')
FIXED_BLOCK = 64


# ---------- 表示：原字节 -> 有序块序列（不重序列化任何 JSON） ----------

def split_fixed_64(raw):
    return [raw[i:i + FIXED_BLOCK] for i in range(0, len(raw), FIXED_BLOCK)]


def split_record_chunks(raw):
    """每条原始行连同其原分隔字节（\\r\\n / \\n / \\r）为一块；
    前导 BOM 独立成块；末尾无分隔符的残行独立成块（保留末尾状态）。"""
    chunks = []
    i = 0
    if raw.startswith(BOM):
        chunks.append(BOM)
        i = len(BOM)
    n = len(raw)
    while i < n:
        j = i
        while j < n and raw[j] not in b'\r\n':
            j += 1
        if j < n:
            end = j + 2 if raw[j:j + 2] == b'\r\n' else j + 1
        else:
            end = j  # 末尾状态：最后一行无分隔符
        chunks.append(raw[i:end])
        i = end
    return chunks


def split(rep, raw):
    if rep == 'fixed_64':
        return split_fixed_64(raw)
    if rep == 'record_chunks':
        return split_record_chunks(raw)
    raise ValueError('archive_unknown_rep')


def rep_meta(rep, raw, chunks):
    if rep == 'fixed_64':
        return {'rep': rep, 'block_bytes': FIXED_BLOCK}
    return {'rep': rep, 'has_bom': raw.startswith(BOM),
            'trailing': 'none' if not raw else ('delimiter' if raw[-1:] in b'\r\n' else 'partial_line')}


# ---------- 块仓 ----------

def chunk_dir(db_path):
    p = Path(db_path)
    return p.parent / (p.name + '.chunks')


def _write_chunk(directory, sha, data):
    """不可改写写入：已存在即跳过；写 tmp 后原子 rename，崩溃只留无名 tmp。"""
    final = directory / sha
    if final.exists():
        return False
    tmp = directory / f'{sha}.tmp-{os.getpid()}-{uuid.uuid4().hex}'
    tmp.write_bytes(data)
    os.replace(tmp, final)
    return True


def _read_chunk(directory, sha):
    f = directory / sha
    if not f.exists():
        raise ValueError('archive_chunk_missing')
    data = f.read_bytes()
    if hashlib.sha256(data).hexdigest() != sha:
        raise ValueError('archive_chunk_corrupt')
    return data


# ---------- 读取主路径（世代在打开时固定） ----------

def _db_path(db):
    return db.execute("SELECT file FROM pragma_database_list WHERE name='main'").fetchone()['file']


def _reconstruct(db, directory, manifest):
    refs = db.execute('SELECT chunk_sha256 FROM manifest_chunk WHERE manifest_id=? ORDER BY seq',
                      (manifest['id'],)).fetchall()
    parts = [_read_chunk(directory, r['chunk_sha256']) for r in refs]
    data = b''.join(parts)
    if len(data) != manifest['byte_length'] or hashlib.sha256(data).hexdigest() != manifest['sha256']:
        raise ValueError('archive_manifest_corrupt')
    return data


def read_source(db, source_id, generation=None):
    """按来源 ID 读原件逐字节内容。generation 缺省取当前发布世代并钉住所见。"""
    st = source_state(db, source_id)
    g = st['generation'] if generation is None else generation
    if g == 0:
        if not st['raw_retained']:
            raise ValueError('archive_raw_pruned')
        return bytes(st['raw'])
    m = db.execute("SELECT * FROM manifest WHERE source_id=? AND version=? AND status='published'",
                   (source_id, g)).fetchone()
    if m is None:
        raise ValueError('archive_manifest_missing')
    data = _reconstruct(db, chunk_dir(_db_path(db)), m)
    if hashlib.sha256(data).hexdigest() != st['sha256']:
        raise ValueError('archive_read_mismatch')
    return data


class PinnedSource:
    """长读取：打开时把所见世代钉进 reader_pin，GC 不得回收其块，直到关闭。"""

    def __init__(self, db_path, source_id):
        self.db_path = db_path
        self.source_id = source_id
        self.token = uuid.uuid4().hex
        self.generation = None
        self._closed = False

    def __enter__(self):
        db = connect(self.db_path)
        try:
            db.execute('BEGIN IMMEDIATE')
            st = source_state(db, self.source_id)
            self.generation = st['generation']
            db.execute("INSERT INTO reader_pin VALUES (?,?,?,datetime('now'))",
                       (self.token, self.source_id, self.generation))
            db.commit()
        finally:
            db.close()
        return self

    def read(self):
        db = connect(self.db_path)
        try:
            return read_source(db, self.source_id, generation=self.generation)
        finally:
            db.close()

    def close(self):
        if self._closed:
            return
        self._closed = True
        db = connect(self.db_path)
        try:
            with db:
                db.execute('DELETE FROM reader_pin WHERE token=?', (self.token,))
        finally:
            db.close()

    def __exit__(self, *exc):
        self.close()


def open_source(db_path, source_id):
    """打开一个钉住当前世代的长读取句柄（上下文管理器）。"""
    return PinnedSource(db_path, source_id)


def list_pins(db_path):
    db = connect(db_path)
    try:
        return [dict(r) for r in db.execute('SELECT * FROM reader_pin ORDER BY started_at')]
    finally:
        db.close()


# ---------- 迁移：begin -> chunks -> verify -> publish -> switch -> prune ----------

def _planned(db, source_id, rep):
    data = read_source(db, source_id)  # 当前世代（迁移期旧 BLOB 或旧 manifest）
    return data, split(rep, data)


def migrate_source(db_path, source_id, rep, hook=None):
    """幂等迁移一个来源到指定表示。hook(point, **kw) 用于故障注入。

    任何时刻崩溃都至少保留一份完整可核验原件：切换前是 raw BLOB，
    切换后是已发布 manifest；清理 BLOB 只发生在切换之后。
    """
    if rep not in REPS:
        raise ValueError('archive_unknown_rep')
    hook = hook or (lambda point, **kw: None)
    db = connect(db_path)
    try:
        st = source_state(db, source_id)
        building = db.execute("SELECT * FROM manifest WHERE source_id=? AND status='building' "
                              'ORDER BY version DESC', (source_id,)).fetchall()
        if building and any(m['rep'] != rep for m in building):
            raise ValueError('archive_migration_in_progress')
        current = db.execute("SELECT * FROM manifest WHERE source_id=? AND version=? AND status='published'",
                             (source_id, st['generation'])).fetchone() if st['generation'] else None
        if building:
            m = building[0]
        elif current is not None and current['rep'] == rep:
            m = current  # 已发布同表示：只补做 switch/prune（崩溃续迁）
        else:
            hook('begin', source_id=source_id)
            data, chunks = _planned(db, source_id, rep)
            version = (db.execute('SELECT COALESCE(MAX(version),0) FROM manifest WHERE source_id=?',
                                  (source_id,)).fetchone()[0]) + 1
            with db:
                cur = db.execute(
                    "INSERT INTO manifest(source_id,version,rep,chunk_count,byte_length,sha256,meta,status) "
                    "VALUES (?,?,?,?,?,?,?,'building')",
                    (source_id, version, rep, len(chunks), len(data), hashlib.sha256(data).hexdigest(),
                     json.dumps(rep_meta(rep, data, chunks), sort_keys=True)))
                mid = cur.lastrowid
            m = db.execute('SELECT * FROM manifest WHERE id=?', (mid,)).fetchone()
        # 块写入 + 挂链：逐块提交，写一半崩溃可幂等续迁。
        data, chunks = _planned(db, source_id, m['rep'])
        if len(chunks) != m['chunk_count'] or hashlib.sha256(data).hexdigest() != m['sha256']:
            raise ValueError('archive_plan_changed')
        directory = chunk_dir(db_path)
        directory.mkdir(parents=True, exist_ok=True)
        written = 0
        for seq, chunk in enumerate(chunks):
            hook('chunk', source_id=source_id, seq=seq, total=len(chunks))
            sha = hashlib.sha256(chunk).hexdigest()
            if _write_chunk(directory, sha, chunk):
                written += 1
            with db:
                db.execute('INSERT OR IGNORE INTO manifest_chunk VALUES (?,?,?,?)',
                           (m['id'], seq, sha, len(chunk)))
        hook('chunks_done', source_id=source_id)
        # 校验：重构字节必须与旧 BLOB/SHA 完全一致，否则不得切换。
        hook('verify', source_id=source_id)
        rebuilt = _reconstruct(db, directory, m)
        if rebuilt != data or hashlib.sha256(rebuilt).hexdigest() != st['sha256']:
            raise ValueError('archive_verify_mismatch')
        if st['raw_retained'] and rebuilt != bytes(st['raw']):
            raise ValueError('archive_verify_mismatch')
        hook('publish', source_id=source_id)
        with db:
            db.execute("UPDATE manifest SET status='published' WHERE id=? AND status='building'", (m['id'],))
        hook('switch', source_id=source_id)
        with db:
            generation = switch_generation(db, source_id, st['generation'], m['version'])
        hook('prune', source_id=source_id)
        with db:
            pruned = prune_raw(db, source_id, generation)
        hook('done', source_id=source_id)
        return {'source_id': source_id, 'version': m['version'], 'rep': m['rep'],
                'generation': generation, 'chunks': m['chunk_count'], 'chunks_written': written,
                'byte_length': m['byte_length'], 'raw_pruned': pruned,
                'status': 'already' if m['status'] == 'published' and m['id'] == (current['id'] if current else None) else 'migrated'}
    finally:
        db.close()


def migrate_all(db_path, rep, hook=None):
    db = connect(db_path)
    try:
        ids = [r['id'] for r in db.execute('SELECT id FROM source ORDER BY id')]
    finally:
        db.close()
    return [migrate_source(db_path, sid, rep, hook=hook) for sid in ids]


def verify_source(db_path, source_id):
    """按当前发布世代重构原件并核对 source.sha256。"""
    db = connect(db_path)
    try:
        st = source_state(db, source_id)
        data = read_source(db, source_id)
        ok = len(data) == st['byte_length'] and hashlib.sha256(data).hexdigest() == st['sha256']
        if not ok:
            raise ValueError('archive_verify_mismatch')
        return {'source_id': source_id, 'generation': st['generation'],
                'raw_retained': bool(st['raw_retained']), 'byte_length': st['byte_length'],
                'sha256': st['sha256'], 'ok': True}
    finally:
        db.close()


def verify_all(db_path):
    db = connect(db_path)
    try:
        ids = [r['id'] for r in db.execute('SELECT id FROM source ORDER BY id')]
    finally:
        db.close()
    return [verify_source(db_path, sid) for sid in ids]


# ---------- 垃圾回收 ----------

def gc(db_path, vacuum=False):
    """退役无读者钉住的旧世代 manifest，回收不再被任何 manifest 引用的块。

    保留集 = 当前世代 manifest ∪ 旧已发布 manifest ∪ 活跃读者钉住的世代 ∪
    building manifest 引用的块；不使用任何固定宽限秒数。
    """
    db = connect(db_path)
    retired = 0
    try:
        db.execute('BEGIN IMMEDIATE')
        pins = {(r['source_id'], r['generation']) for r in db.execute('SELECT source_id,generation FROM reader_pin')}
        current = {r['source_id']: r['generation'] for r in db.execute('SELECT source_id,generation FROM source_state')}
        for m in db.execute("SELECT * FROM manifest WHERE status='published'").fetchall():
            if m['version'] == current.get(m['source_id']):
                continue
            if (m['source_id'], m['version']) in pins:
                continue
            db.execute('DELETE FROM manifest_chunk WHERE manifest_id=?', (m['id'],))
            db.execute('DELETE FROM manifest WHERE id=?', (m['id'],))
            retired += 1
        keep = {r['chunk_sha256'] for r in db.execute('SELECT chunk_sha256 FROM manifest_chunk')}
        # 补做被活跃读者推迟的 BLOB 清理（世代已切换且不再有人钉 generation 0）。
        pruned = 0
        for st in db.execute('SELECT source_id,generation FROM source_state WHERE raw_retained=1 AND generation>0').fetchall():
            if prune_raw(db, st['source_id'], st['generation']):
                pruned += 1
        directory = chunk_dir(db_path)
        deleted = deleted_bytes = tmp_removed = 0
        if directory.exists():
            for f in sorted(directory.iterdir()):
                if '.tmp-' in f.name:
                    f.unlink()
                    tmp_removed += 1
                elif f.name not in keep:
                    deleted_bytes += f.stat().st_size
                    f.unlink()
                    deleted += 1
        db.commit()
    finally:
        db.close()
    if vacuum:
        db = connect(db_path)
        try:
            db.execute('VACUUM')
        finally:
            db.close()
    return {'retired_manifests': retired, 'deleted_chunks': deleted,
            'deleted_chunk_bytes': deleted_bytes, 'tmp_removed': tmp_removed,
            'pruned_blobs': pruned}


# ---------- 物理存储账 ----------

def archive_stats(db_path):
    """数据库文件与块仓的实际占用；逻辑去重数单独列出，不冒充磁盘节省。"""
    db_path = Path(db_path)
    db = connect(db_path)
    try:
        sources = [dict(r) for r in db.execute(
            "SELECT s.id,s.name,s.sha256,st.byte_length,st.generation,st.raw_retained,"
            "length(s.raw) AS raw_bytes,m.rep "
            'FROM source s JOIN source_state st ON st.source_id=s.id '
            "LEFT JOIN manifest m ON m.source_id=s.id AND m.version=st.generation AND m.status='published' "
            'ORDER BY s.id')]
        manifests = {r['status']: r['n'] for r in db.execute('SELECT status,count(*) n FROM manifest GROUP BY status')}
        refs = db.execute('SELECT count(*) FROM manifest_chunk').fetchone()[0]
        raw_blob_bytes = db.execute('SELECT COALESCE(SUM(length(raw)),0) FROM source').fetchone()[0]
        logical = db.execute('SELECT COALESCE(SUM(byte_length),0) FROM source_state').fetchone()[0]
    finally:
        db.close()
    directory = chunk_dir(db_path)
    chunk_files = chunk_bytes = tmp_files = 0
    if directory.exists():
        for f in directory.iterdir():
            if '.tmp-' in f.name:
                tmp_files += 1
            else:
                chunk_files += 1
                chunk_bytes += f.stat().st_size
    db_file = db_path.stat().st_size if db_path.exists() else 0
    return {'db_file_bytes': db_file, 'chunk_dir_bytes': chunk_bytes, 'chunk_files': chunk_files,
            'tmp_files': tmp_files, 'physical_bytes': db_file + chunk_bytes,
            'raw_blob_bytes': raw_blob_bytes, 'logical_source_bytes': logical,
            'unique_chunk_bytes': chunk_bytes, 'manifest_refs': refs,
            'manifests': manifests, 'sources': sources,
            'note': 'physical=数据库文件+块仓文件实际字节；logical=各来源原字节合计；'
                    '去重只有在 BLOB 清理并 GC/VACUUM 后才体现为磁盘节省'}


# ---------- 冻结归档用例（起点行为，读取改为世代感知） ----------

def register(db_path, case_id, fixture):
    source = Path(fixture).resolve()
    raw = source.read_bytes()
    doc = json.loads(raw)
    if not case_id or not doc.get('sources') or not doc.get('representations'):
        raise ValueError('bad_archive_case')
    if any(r.get('id') not in REPS for r in doc['representations']):
        raise ValueError('bad_archive_case')
    db = connect(db_path)
    try:
        with db:
            records = []
            for name in doc['sources']:
                file = (source.parent / name).resolve()
                row = db.execute('SELECT * FROM source WHERE name=?', (str(file),)).fetchone()
                if row is None:
                    raise ValueError('archive_source_missing')
                original = read_source(db, row['id'])
                if hashlib.sha256(original).hexdigest() != row['sha256']:
                    raise ValueError('archive_source_hash_mismatch')
                records.append({'id': row['id'], 'name': row['name'], 'sha256': row['sha256'],
                                'length': len(original)})
            payload = json.dumps({'fixture': str(source), 'digest': hashlib.sha256(raw).hexdigest(),
                                  'doc': doc, 'sources': records}, sort_keys=True, separators=(',', ':'))
            old = db.execute('SELECT payload FROM archive_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('archive_case_conflict')
            db.execute('INSERT OR IGNORE INTO archive_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'sources': len(records)}
