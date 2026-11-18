"""在线迁移故障注入：五个故障点重启恢复、钉住读取与 GC 交错、物理账。"""
import json
from pathlib import Path

import pytest

from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan import archive
from shelfscan.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'
A_BYTES = (FIX / 'archive-source-a.jsonl').read_bytes()
B_BYTES = (FIX / 'archive-source-b.jsonl').read_bytes()


class Crash(Exception):
    pass


def killer(point, seq=None):
    def hook(p, **kw):
        if p == point and (seq is None or kw.get('seq') == seq):
            raise Crash(p)
    return hook


def setup_db(tmp_path, sources=('archive-source-a.jsonl', 'archive-source-b.jsonl')):
    db = tmp_path / 'fault.db'
    initialize(db, FIX / 'config.json')
    for name in sources:
        ingest(FIX / name, db)
    return db


def state(db, source_id=1):
    conn = connect(db)
    try:
        row = conn.execute('SELECT generation,raw_retained FROM source_state WHERE source_id=?',
                           (source_id,)).fetchone()
        man = conn.execute('SELECT version,status,rep FROM manifest WHERE source_id=? ORDER BY version',
                           (source_id,)).fetchall()
        return (row['generation'], row['raw_retained']), [tuple(m) for m in man]
    finally:
        conn.close()


def read(db, source_id=1):
    conn = connect(db)
    try:
        return archive.read_source(conn, source_id)
    finally:
        conn.close()


def test_fault_chunk_write_half_done_then_resume(tmp_path):
    db = setup_db(tmp_path, sources=('archive-source-a.jsonl',))
    with pytest.raises(Crash):
        archive.migrate_source(db, 1, 'fixed_64', hook=killer('chunk', seq=5))
    (gen, retained), ms = state(db)
    assert (gen, retained) == (0, 1)  # 读指针未动，raw BLOB 仍是完整原件
    assert ms == [(1, 'building', 'fixed_64')]
    conn = connect(db)
    try:
        assert conn.execute('SELECT count(*) FROM manifest_chunk').fetchone()[0] == 5
    finally:
        conn.close()
    assert archive.archive_stats(db)['chunk_files'] == 5  # 34 块只写了 5 块
    assert read(db) == A_BYTES  # 崩溃后旧原件完整可读
    res = archive.migrate_source(db, 1, 'fixed_64')  # 重启幂等续迁
    assert res['status'] == 'migrated' and res['chunks'] == 34
    assert archive.archive_stats(db)['chunk_files'] == 34
    assert read(db) == A_BYTES
    assert archive.verify_source(db, 1)['ok']


def test_fault_manifest_not_published_then_resume(tmp_path):
    db = setup_db(tmp_path, sources=('archive-source-a.jsonl',))
    with pytest.raises(Crash):
        archive.migrate_source(db, 1, 'record_chunks', hook=killer('publish'))
    (gen, retained), ms = state(db)
    assert (gen, retained) == (0, 1) and ms == [(1, 'building', 'record_chunks')]
    assert archive.archive_stats(db)['chunk_files'] == 20  # 块齐全但 manifest 未发布
    assert read(db) == A_BYTES  # 读者仍见 raw 世代
    res = archive.migrate_source(db, 1, 'record_chunks')
    assert res['chunks_written'] == 0  # 已有块不重复物理写入
    (gen, retained), ms = state(db)
    assert (gen, retained) == (1, 0) and ms == [(1, 'published', 'record_chunks')]
    assert read(db) == A_BYTES


def test_fault_pointer_switched_blob_not_pruned(tmp_path):
    db = setup_db(tmp_path, sources=('archive-source-a.jsonl',))
    with pytest.raises(Crash):
        archive.migrate_source(db, 1, 'record_chunks', hook=killer('prune'))
    (gen, retained), _ = state(db)
    assert (gen, retained) == (1, 1)  # 指针已切、BLOB 未清：两份原件并存
    assert archive.archive_stats(db)['raw_blob_bytes'] == len(A_BYTES)
    assert read(db) == A_BYTES  # 主读路径已走 manifest
    res = archive.migrate_source(db, 1, 'record_chunks')  # 重启只补清理
    assert res['status'] == 'already' and res['raw_pruned'] is True
    (gen, retained), _ = state(db)
    assert (gen, retained) == (1, 0)
    assert archive.archive_stats(db)['raw_blob_bytes'] == 0
    assert read(db) == A_BYTES


def test_fault_blob_pruned_then_process_dies(tmp_path):
    db = setup_db(tmp_path, sources=('archive-source-a.jsonl',))
    with pytest.raises(Crash):
        archive.migrate_source(db, 1, 'record_chunks', hook=killer('done'))
    (gen, retained), ms = state(db)
    assert (gen, retained) == (1, 0) and ms == [(1, 'published', 'record_chunks')]
    # 重启：BLOB 已清，manifest 世代是唯一原件，必须完整可核验
    assert archive.verify_source(db, 1)['ok']
    assert read(db) == A_BYTES
    res = archive.migrate_source(db, 1, 'record_chunks')
    assert res['status'] == 'already'
    assert read(db) == A_BYTES


def test_fault_gc_interleaved_with_old_generation_reader(tmp_path):
    db = setup_db(tmp_path, sources=('archive-source-a.jsonl',))
    archive.migrate_source(db, 1, 'record_chunks')  # v1
    with archive.open_source(db, 1) as reader:
        assert reader.generation == 1
        archive.migrate_source(db, 1, 'fixed_64')  # v2 切换读指针
        g = archive.gc(db)
        assert g['retired_manifests'] == 0 and g['deleted_chunks'] == 0  # 钉住的旧世代受保护
        assert archive.archive_stats(db)['chunk_files'] == 54
        assert reader.read() == A_BYTES  # 旧读者读完所见世代
    g = archive.gc(db)  # 读者关闭后旧世代才可退役
    assert g['retired_manifests'] == 1 and g['deleted_chunks'] == 20
    assert archive.archive_stats(db)['chunk_files'] == 34
    assert read(db) == A_BYTES


def test_pinned_raw_generation_defers_blob_prune(tmp_path):
    db = setup_db(tmp_path, sources=('archive-source-a.jsonl',))
    with archive.open_source(db, 1) as reader:
        assert reader.generation == 0  # 钉在 raw BLOB 世代
        res = archive.migrate_source(db, 1, 'record_chunks')
        assert res['raw_pruned'] is False  # 有读者钉 generation 0，清理推迟而非猜测超时
        assert reader.read() == A_BYTES
        assert archive.gc(db)['pruned_blobs'] == 0
        assert reader.read() == A_BYTES
    assert archive.gc(db)['pruned_blobs'] == 1  # 读者关闭后 GC 补做清理
    assert archive.archive_stats(db)['raw_blob_bytes'] == 0
    assert read(db) == A_BYTES


def test_concurrent_ingest_while_migration_paused(tmp_path):
    db = setup_db(tmp_path, sources=('archive-source-a.jsonl',))
    with pytest.raises(Crash):
        archive.migrate_source(db, 1, 'record_chunks', hook=killer('chunks_done'))
    new = tmp_path / 'new.jsonl'
    new.write_text(json.dumps({'kind': 'scan', 'device': 'R9', 'seq': 1, 'copy_id': 'C9',
                               'at': '2026-01-01T08:00:00Z', 'shelf': 'S9', 'signal': 50}) + '\n')
    assert ingest(new, db)['imported'] == 1  # 迁移中途新导入不受阻
    assert ingest(new, db)['duplicate_file'] is True
    archive.migrate_source(db, 1, 'record_chunks')  # 续迁完成
    archive.migrate_source(db, 2, 'fixed_64')
    assert read(db) == A_BYTES
    assert read(db, 2) == new.read_bytes()
    assert [v['ok'] for v in archive.verify_all(db)] == [True, True]


def test_remigrate_conflict_and_idempotent_resume(tmp_path):
    db = setup_db(tmp_path, sources=('archive-source-a.jsonl',))
    with pytest.raises(Crash):
        archive.migrate_source(db, 1, 'fixed_64', hook=killer('publish'))
    with pytest.raises(ValueError, match='archive_migration_in_progress'):
        archive.migrate_source(db, 1, 'record_chunks')  # 未完成的迁移不可被另一表示抢注
    archive.migrate_source(db, 1, 'fixed_64')  # 原表示幂等续迁
    again = archive.migrate_source(db, 1, 'fixed_64')
    assert again['status'] == 'already' and again['version'] == 1
    assert archive.archive_stats(db)['chunk_files'] == 34


def test_physical_accounting_around_blob_reclaim(tmp_path):
    db = setup_db(tmp_path)
    s0 = archive.archive_stats(db)
    assert s0['raw_blob_bytes'] == 4409 and s0['chunk_dir_bytes'] == 0
    assert s0['logical_source_bytes'] == 4409
    archive.migrate_all(db, 'record_chunks')
    s1 = archive.archive_stats(db)
    # BLOB 已清但 SQLite 文件不自动收缩：逻辑去重 ≠ 磁盘节省
    assert s1['raw_blob_bytes'] == 0
    assert s1['db_file_bytes'] >= s0['db_file_bytes']
    assert s1['chunk_dir_bytes'] == 2258 and s1['chunk_files'] == 21
    assert s1['logical_source_bytes'] == 4409
    archive.gc(db, vacuum=True)
    s2 = archive.archive_stats(db)
    assert s2['db_file_bytes'] < s1['db_file_bytes']  # VACUUM 后才体现为磁盘节省
    assert s2['physical_bytes'] == s2['db_file_bytes'] + s2['chunk_dir_bytes']
    assert s2['unique_chunk_bytes'] == 2258 < s2['logical_source_bytes']
    # 定长块在此 fixture 下零共享，物理占用等于逻辑占用
    db2 = setup_db(tmp_path / 'fixed')
    archive.migrate_all(db2, 'fixed_64')
    sf = archive.archive_stats(db2)
    assert sf['chunk_files'] == 70 and sf['unique_chunk_bytes'] == 4409


def test_orphan_tmp_and_stray_chunk_collected(tmp_path):
    db = setup_db(tmp_path, sources=('archive-source-a.jsonl',))
    archive.migrate_source(db, 1, 'record_chunks')
    d = archive.chunk_dir(db)
    stray = d / ('0' * 64)
    stray.write_bytes(b'orphan')  # 崩溃遗留的未挂链孤儿块
    tmp = d / 'abc.tmp-1-zz'
    tmp.write_bytes(b'half')  # 写一半崩溃的 tmp 文件
    g = archive.gc(db)
    assert g['tmp_removed'] == 1 and g['deleted_chunks'] == 1
    assert g['deleted_chunk_bytes'] == len(b'orphan')
    assert archive.archive_stats(db)['chunk_files'] == 20
    assert read(db) == A_BYTES
