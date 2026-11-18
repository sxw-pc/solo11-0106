import sqlite3
from pathlib import Path
SCHEMA="""
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS batch(id TEXT PRIMARY KEY,version TEXT NOT NULL,start TEXT NOT NULL,end TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('待接收','已校验','待核','已判定','作废')));
CREATE TABLE IF NOT EXISTS copy(id TEXT PRIMARY KEY,planned_shelf TEXT NOT NULL,loaned INTEGER NOT NULL CHECK(loaned IN (0,1)),batch TEXT NOT NULL REFERENCES batch(id));
CREATE TABLE IF NOT EXISTS reader(id TEXT PRIMARY KEY,shelf TEXT NOT NULL,window_start TEXT NOT NULL,window_end TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS source(id INTEGER PRIMARY KEY,name TEXT NOT NULL,sha256 TEXT NOT NULL UNIQUE,raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS event(id INTEGER PRIMARY KEY,source_id INTEGER NOT NULL REFERENCES source(id),line_no INTEGER NOT NULL,device TEXT NOT NULL,seq INTEGER NOT NULL,kind TEXT NOT NULL,at TEXT NOT NULL,copy_id TEXT NOT NULL,shelf TEXT,signal REAL,payload TEXT NOT NULL,sha256 TEXT NOT NULL,UNIQUE(device,seq));
CREATE TABLE IF NOT EXISTS issue(id INTEGER PRIMARY KEY,code TEXT NOT NULL,copy_id TEXT NOT NULL,event_id INTEGER REFERENCES event(id),detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS decision(copy_id TEXT PRIMARY KEY,status TEXT NOT NULL,actual_shelf TEXT,detail TEXT NOT NULL,rule_version TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS evidence(id INTEGER PRIMARY KEY, event_id INTEGER NOT NULL REFERENCES event(id), source_sha256 TEXT NOT NULL, payload_sha256 TEXT NOT NULL, role TEXT NOT NULL, UNIQUE(event_id,role));
CREATE TABLE IF NOT EXISTS run(id INTEGER PRIMARY KEY, status TEXT NOT NULL CHECK(status IN ('待接收','已校验','待核','已判定','作废')), started_at TEXT NOT NULL, ended_at TEXT);
CREATE TABLE IF NOT EXISTS archive_case(id TEXT PRIMARY KEY,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS source_state(source_id INTEGER PRIMARY KEY REFERENCES source(id),generation INTEGER NOT NULL DEFAULT 0,raw_retained INTEGER NOT NULL DEFAULT 1 CHECK(raw_retained IN (0,1)),byte_length INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS manifest(id INTEGER PRIMARY KEY,source_id INTEGER NOT NULL REFERENCES source(id),version INTEGER NOT NULL,rep TEXT NOT NULL CHECK(rep IN ('fixed_64','record_chunks')),chunk_count INTEGER NOT NULL,byte_length INTEGER NOT NULL,sha256 TEXT NOT NULL,meta TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('building','published')),UNIQUE(source_id,version));
CREATE TABLE IF NOT EXISTS manifest_chunk(manifest_id INTEGER NOT NULL REFERENCES manifest(id),seq INTEGER NOT NULL,chunk_sha256 TEXT NOT NULL,chunk_length INTEGER NOT NULL,PRIMARY KEY(manifest_id,seq));
CREATE TABLE IF NOT EXISTS reader_pin(token TEXT PRIMARY KEY,source_id INTEGER NOT NULL REFERENCES source(id),generation INTEGER NOT NULL,started_at TEXT NOT NULL);
"""
# generation 0 = 读 source.raw 原 BLOB；generation>0 = 读对应已发布 manifest 的块。
# 切换与 BLOB 清理只改 source_state / source.raw，event/evidence 的来源身份不变。
def connect(path):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True);db=sqlite3.connect(path);db.row_factory=sqlite3.Row;db.executescript(SCHEMA)
    # 旧库在线迁移：为建库前已存在的 source 补齐世代账本（幂等）。
    db.execute('INSERT OR IGNORE INTO source_state(source_id,generation,raw_retained,byte_length) SELECT id,0,1,length(raw) FROM source')
    db.commit();return db

def source_state(db,source_id):
    row=db.execute('SELECT s.sha256,s.name,s.raw,st.generation,st.raw_retained,st.byte_length FROM source s JOIN source_state st ON st.source_id=s.id WHERE s.id=?',(source_id,)).fetchone()
    if row is None:raise ValueError('archive_source_missing')
    return row

def switch_generation(db,source_id,expected,new_generation):
    """比较交换发布世代；返回实际生效的世代。并发下只有一代能赢。"""
    cur=db.execute('UPDATE source_state SET generation=? WHERE source_id=? AND generation=?',(new_generation,source_id,expected))
    if cur.rowcount==1:return new_generation
    now=db.execute('SELECT generation FROM source_state WHERE source_id=?',(source_id,)).fetchone()
    if now and now['generation']==new_generation:return new_generation
    raise ValueError('archive_generation_conflict')

def prune_raw(db,source_id,generation):
    """清掉冗余 raw BLOB。仅在读指针已切到已发布世代后调用，两步同事务。

    若有读者仍钉着 generation 0（raw BLOB 世代），清理推迟，返回 False；
    读者关闭后由 gc 或续迁补做。不按时间猜测读者是否离开。"""
    st=db.execute('SELECT generation,raw_retained FROM source_state WHERE source_id=?',(source_id,)).fetchone()
    if st is None:raise ValueError('archive_source_missing')
    if st['generation']!=generation:raise ValueError('archive_prune_before_switch')
    if not st['raw_retained']:return True
    if db.execute('SELECT 1 FROM reader_pin WHERE source_id=? AND generation=0',(source_id,)).fetchone():return False
    db.execute("UPDATE source SET raw=X'' WHERE id=?",(source_id,))
    db.execute('UPDATE source_state SET raw_retained=0 WHERE source_id=?',(source_id,))
    return True
