"""内容寻址块仓：两种无损表示、逐字节复原、真实物理布局与兼容主读路径。"""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan.rules import evaluate
from shelfscan.report import report
from shelfscan import archive
from shelfscan.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'
A_BYTES = (FIX / 'archive-source-a.jsonl').read_bytes()
B_BYTES = (FIX / 'archive-source-b.jsonl').read_bytes()


def setup_db(tmp_path, sources=('archive-source-a.jsonl', 'archive-source-b.jsonl')):
    db = tmp_path / 'cas.db'
    initialize(db, FIX / 'config.json')
    for name in sources:
        ingest(FIX / name, db)
    return db


def manifests(db):
    conn = connect(db)
    try:
        return [dict(r) for r in conn.execute('SELECT * FROM manifest ORDER BY source_id,version')]
    finally:
        conn.close()


def test_fixed_64_layout_and_byte_exact_restore(tmp_path):
    db = setup_db(tmp_path)
    res = archive.migrate_all(db, 'fixed_64')
    assert [r['chunks'] for r in res] == [34, 36]  # 2151/2258 字节按 64 切块
    ms = manifests(db)
    assert [(m['source_id'], m['chunk_count'], m['rep']) for m in ms] == [(1, 34, 'fixed_64'), (2, 36, 'fixed_64')]
    assert all(json.loads(m['meta'])['block_bytes'] == 64 for m in ms)
    st = archive.archive_stats(db)
    # B 前插 107 字节使后缀边界整体错位：定长块零共享，物理占用 = 逻辑占用
    assert st['chunk_files'] == 70 and st['unique_chunk_bytes'] == 4409
    assert st['logical_source_bytes'] == 4409
    conn = connect(db)
    try:
        assert archive.read_source(conn, 1) == A_BYTES
        assert archive.read_source(conn, 2) == B_BYTES
    finally:
        conn.close()
    assert [v['ok'] for v in archive.verify_all(db)] == [True, True]


def test_record_chunks_share_identical_records(tmp_path):
    db = setup_db(tmp_path)
    res = archive.migrate_all(db, 'record_chunks')
    assert [r['chunks'] for r in res] == [20, 21]
    assert res[1]['chunks_written'] == 1  # B 只有前插那一条是新块，20 条原始记录复用 A
    st = archive.archive_stats(db)
    assert st['chunk_files'] == 21 and st['unique_chunk_bytes'] == 2258
    assert st['manifest_refs'] == 41  # 20+21 条有序引用，manifest 开销独立记账
    assert st['logical_source_bytes'] == 4409  # 逻辑量不变，去重只体现在块仓
    conn = connect(db)
    try:
        assert archive.read_source(conn, 1) == A_BYTES
        assert archive.read_source(conn, 2) == B_BYTES
        assert hashlib.sha256(archive.read_source(conn, 1)).hexdigest() != hashlib.sha256(
            archive.read_source(conn, 2)).hexdigest()
    finally:
        conn.close()


def test_two_representations_form_distinct_real_layouts(tmp_path):
    db = setup_db(tmp_path, sources=('archive-source-a.jsonl',))
    archive.migrate_source(db, 1, 'record_chunks')
    archive.migrate_source(db, 1, 'fixed_64')  # 重迁到另一表示 = 新世代
    ms = manifests(db)
    assert [(m['version'], m['rep'], m['chunk_count'], m['status']) for m in ms] == [
        (1, 'record_chunks', 20, 'published'), (2, 'fixed_64', 34, 'published')]
    st = archive.archive_stats(db)
    assert st['chunk_files'] == 54  # 两种布局的块物理并存（记录块 ~107B 与 64B 块不重合）
    assert st['manifest_refs'] == 54  # manifest 开销随表示不同而不同
    conn = connect(db)
    try:
        assert archive.read_source(conn, 1) == A_BYTES  # 当前世代 fixed_64
        assert archive.read_source(conn, 1, generation=1) == A_BYTES  # 旧世代 record_chunks
    finally:
        conn.close()
    g = archive.gc(db)
    assert g['retired_manifests'] == 1 and g['deleted_chunks'] == 20
    assert archive.archive_stats(db)['chunk_files'] == 34
    conn = connect(db)
    try:
        assert archive.read_source(conn, 1) == A_BYTES
    finally:
        conn.close()


def test_mixed_encoding_source_roundtrip_both_reps(tmp_path):
    bom = b'\xef\xbb\xbf'
    l1 = json.dumps({'kind': 'scan', 'device': 'R1', 'seq': 1, 'copy_id': 'C1',
                     'at': '2026-01-01T08:00:00Z', 'shelf': 'S1', 'signal': 75}).encode()
    l2 = json.dumps({'kind': 'scan', 'device': 'R1', 'seq': 2, 'copy_id': 'C2',
                     'at': '2026-01-01T08:01:00Z', 'shelf': '架S2', 'signal': 60},
                    ensure_ascii=False).encode()
    l3 = json.dumps({'kind': 'scan', 'device': 'R1', 'seq': 3, 'copy_id': 'C1',
                     'at': '2026-01-01T08:02:00Z', 'shelf': 'S1', 'signal': 70}).encode()
    # BOM + CRLF + LF + 重复行 + 非 ASCII + 无末尾换行
    raw = bom + l1 + b'\r\n' + l2 + b'\n' + l1 + b'\r\n' + l3
    src = tmp_path / 'mixed.jsonl'
    src.write_bytes(raw)
    db = tmp_path / 'enc.db'
    initialize(db, FIX / 'config.json')
    assert ingest(src, db)['imported'] == 3  # 重复行按设备+序号幂等跳过
    r = archive.migrate_source(db, 1, 'record_chunks')
    assert r['chunks'] == 5  # BOM、三条行块（含各自分隔符）、末尾无换行残行
    ms = manifests(db)
    meta = json.loads(ms[0]['meta'])
    assert meta['has_bom'] is True and meta['trailing'] == 'partial_line'
    st = archive.archive_stats(db)
    assert st['chunk_files'] == 4  # 重复行块物理只存一次
    conn = connect(db)
    try:
        assert archive.read_source(conn, 1) == raw
    finally:
        conn.close()
    archive.migrate_source(db, 1, 'fixed_64')  # 换表示重迁仍逐字节复原
    conn = connect(db)
    try:
        assert archive.read_source(conn, 1) == raw
        assert hashlib.sha256(archive.read_source(conn, 1)).hexdigest() == hashlib.sha256(raw).hexdigest()
    finally:
        conn.close()
    assert archive.verify_source(db, 1)['ok']


def test_report_and_evidence_replayable_across_migration(tmp_path):
    db = setup_db(tmp_path)
    evaluate(db)
    before = json.loads(report(db, tmp_path / 'r1.json').read_text(encoding='utf-8'))
    assert [s['byte_length'] for s in before['source']] == [2151, 2258]
    archive.migrate_all(db, 'record_chunks')
    archive.gc(db, vacuum=True)
    after = json.loads(report(db, tmp_path / 'r2.json').read_text(encoding='utf-8'))
    for key in ('source', 'event', 'evidence', 'decision', 'issue'):
        assert after[key] == before[key], key
    # 来源身份不变：event.source_id 与 evidence.source_sha256 仍指向原来源
    assert {e['source_sha256'] for e in after['evidence']} == {s['sha256'] for s in after['source']}
    again = report(db, tmp_path / 'r3.json').read_text(encoding='utf-8')
    assert again == (tmp_path / 'r2.json').read_text(encoding='utf-8')  # 重复报告可重放


def test_ingest_after_migration_and_duplicate_file(tmp_path):
    db = setup_db(tmp_path)
    archive.migrate_all(db, 'record_chunks')
    archive.gc(db, vacuum=True)
    new = tmp_path / 'new.jsonl'
    new.write_text(json.dumps({'kind': 'scan', 'device': 'R9', 'seq': 1, 'copy_id': 'C9',
                               'at': '2026-01-01T08:00:00Z', 'shelf': 'S9', 'signal': 50}) + '\n')
    assert ingest(new, db)['imported'] == 1  # 迁移后新导入照常落 raw 世代
    conn = connect(db)
    try:
        row = conn.execute('SELECT generation,raw_retained FROM source_state WHERE source_id=3').fetchone()
        assert (row['generation'], row['raw_retained']) == (0, 1)
        assert archive.read_source(conn, 3) == new.read_bytes()
    finally:
        conn.close()
    # 已迁移来源的整文件去重仍生效（不依赖 raw BLOB 是否已清）
    assert ingest(FIX / 'archive-source-a.jsonl', db)['duplicate_file'] is True
    data = json.loads(report(db, tmp_path / 'r.json').read_text(encoding='utf-8'))
    assert [s['byte_length'] for s in data['source']] == [2151, 2258, len(new.read_bytes())]
    archive.migrate_source(db, 3, 'fixed_64')
    conn = connect(db)
    try:
        assert archive.read_source(conn, 3) == new.read_bytes()
    finally:
        conn.close()


def test_register_after_prune(tmp_path):
    db = setup_db(tmp_path)
    archive.migrate_all(db, 'record_chunks')
    archive.gc(db, vacuum=True)
    assert archive.archive_stats(db)['raw_blob_bytes'] == 0
    # BLOB 已清，冻结归档用例仍从主读路径核对来源身份
    assert archive.register(db, 'archive-01', FIX / 'archive-case.json') == {'case_id': 'archive-01', 'sources': 2}


def test_empty_and_bom_only_sources_roundtrip(tmp_path):
    db = tmp_path / 'edge.db'
    initialize(db, FIX / 'config.json')
    empty = tmp_path / 'empty.jsonl'
    empty.write_bytes(b'')
    bom_only = tmp_path / 'bom.jsonl'
    bom_only.write_bytes(b'\xef\xbb\xbf')
    ingest(empty, db)
    ingest(bom_only, db)
    assert archive.migrate_source(db, 1, 'record_chunks')['chunks'] == 0
    assert archive.migrate_source(db, 2, 'record_chunks')['chunks'] == 1  # BOM 独立成块
    conn = connect(db)
    try:
        assert archive.read_source(conn, 1) == b''
        assert archive.read_source(conn, 2) == b'\xef\xbb\xbf'
    finally:
        conn.close()
    data = json.loads(report(db, tmp_path / 'r.json').read_text(encoding='utf-8'))
    assert [s['byte_length'] for s in data['source']] == [0, 3]
    assert [v['ok'] for v in archive.verify_all(db)] == [True, True]


def test_pre_archive_database_upgraded_in_place(tmp_path):
    # 旧版库（无归档表）打开时就地补账本，raw BLOB 世代继续可读可迁
    import sqlite3
    db = tmp_path / 'old.db'
    conn = sqlite3.connect(db)
    conn.executescript(
        'CREATE TABLE source(id INTEGER PRIMARY KEY,name TEXT NOT NULL,sha256 TEXT NOT NULL UNIQUE,raw BLOB NOT NULL);')
    conn.execute('INSERT INTO source VALUES (?,?,?,?)',
                 (1, 'legacy.jsonl', hashlib.sha256(A_BYTES).hexdigest(), A_BYTES))
    conn.commit()
    conn.close()
    data = json.loads(report(db, tmp_path / 'r.json').read_text(encoding='utf-8'))
    assert data['source'][0]['byte_length'] == len(A_BYTES)  # 旧库报告字节数正确
    archive.migrate_source(db, 1, 'record_chunks')
    conn = connect(db)
    try:
        assert archive.read_source(conn, 1) == A_BYTES
    finally:
        conn.close()
    assert archive.verify_source(db, 1)['ok']


def test_cli_archive_flow(tmp_path):
    db = tmp_path / 'cli.db'

    def run(*args):
        p = subprocess.run([sys.executable, '-m', 'shelfscan', *args],
                           capture_output=True, text=True, cwd=tmp_path)
        assert p.returncode == 0, p.stderr
        return p.stdout.strip()

    run('init', '--db', str(db), '--config', str(FIX / 'config.json'))
    run('ingest', str(FIX / 'archive-source-a.jsonl'), '--db', str(db))
    run('ingest', str(FIX / 'archive-source-b.jsonl'), '--db', str(db))
    mig = json.loads(run('archive', '--db', str(db), 'migrate', '--rep', 'record_chunks'))
    assert [m['chunks'] for m in mig] == [20, 21]
    ver = json.loads(run('archive', '--db', str(db), 'verify'))
    assert all(v['ok'] for v in ver)
    out = tmp_path / 'a.bin'
    run('archive', '--db', str(db), 'cat', '--source-id', '1', '--out', str(out))
    assert out.read_bytes() == A_BYTES
    stats = json.loads(run('archive', '--db', str(db), 'stats'))
    assert stats['chunk_dir_bytes'] == 2258 and stats['raw_blob_bytes'] == 0
    before = stats['db_file_bytes']
    g = json.loads(run('archive', '--db', str(db), 'gc', '--vacuum'))
    assert g['deleted_chunks'] == 0
    stats2 = json.loads(run('archive', '--db', str(db), 'stats'))
    assert stats2['db_file_bytes'] < before
    rep = run('report', '--db', str(db), '--out', str(tmp_path / 'r.json'))
    data = json.loads(Path(rep).read_text(encoding='utf-8'))
    assert [s['byte_length'] for s in data['source']] == [2151, 2258]
