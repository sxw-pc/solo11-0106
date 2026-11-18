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
CREATE TABLE IF NOT EXISTS donation_case(id TEXT PRIMARY KEY,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS authorization(deed_id TEXT NOT NULL,version INTEGER NOT NULL,donor TEXT NOT NULL,options TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('有效','已撤回')),revoked_at TEXT,PRIMARY KEY(deed_id,version));
CREATE TABLE IF NOT EXISTS acceptance(case_id TEXT PRIMARY KEY REFERENCES donation_case(id),tag TEXT NOT NULL,deed_id TEXT NOT NULL,deed_version INTEGER NOT NULL,seal TEXT NOT NULL,custodian TEXT NOT NULL,stage TEXT NOT NULL CHECK(stage IN ('待核对','待确认','争议','已撤回','已确认','权属已登记','快照已生成','凭证已出具','已补偿')),path TEXT CHECK(path IS NULL OR path IN ('outright_gift','long_term_deposit')),librarian TEXT,owner TEXT,circulation TEXT CHECK(circulation IS NULL OR circulation IN ('禁止流通','普通流通','仅限阅览室')),return_by TEXT,batch_id TEXT REFERENCES batch(id),dispute_code TEXT,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS holding(tag TEXT PRIMARY KEY,case_id TEXT NOT NULL UNIQUE REFERENCES donation_case(id),batch_id TEXT REFERENCES batch(id),path TEXT NOT NULL CHECK(path IN ('outright_gift','long_term_deposit')),owner TEXT NOT NULL,circulation TEXT NOT NULL CHECK(circulation IN ('禁止流通','普通流通','仅限阅览室')),catalog_status TEXT NOT NULL CHECK(catalog_status IN ('待编目','已编目','不适用')),return_by TEXT,registered_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS voucher(id TEXT PRIMARY KEY,case_id TEXT NOT NULL UNIQUE REFERENCES donation_case(id),kind TEXT NOT NULL CHECK(kind IN ('赠与证明','寄存返还单')),tag TEXT NOT NULL,owner TEXT NOT NULL,circulation TEXT NOT NULL,return_by TEXT,deed_id TEXT NOT NULL,deed_version INTEGER NOT NULL,seal TEXT NOT NULL,issued_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS stage_log(id INTEGER PRIMARY KEY,case_id TEXT NOT NULL,stage TEXT NOT NULL,event TEXT NOT NULL,detail TEXT NOT NULL,at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS decision_history(run_id INTEGER NOT NULL REFERENCES run(id),copy_id TEXT NOT NULL,status TEXT NOT NULL,actual_shelf TEXT,detail TEXT NOT NULL,rule_version TEXT NOT NULL,PRIMARY KEY(run_id,copy_id));
"""
def connect(path):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(path,timeout=30)
    db.row_factory=sqlite3.Row
    db.execute('PRAGMA foreign_keys=ON')
    db.execute('PRAGMA busy_timeout=10000')
    db.executescript(SCHEMA)
    cols={r['name'] for r in db.execute('PRAGMA table_info(issue)')}
    if 'run_id' not in cols:
        db.execute('ALTER TABLE issue ADD COLUMN run_id INTEGER REFERENCES run(id)')
        db.commit()
    return db
