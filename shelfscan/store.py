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
CREATE TABLE IF NOT EXISTS firmware_case(id TEXT PRIMARY KEY,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS firmware_device(id TEXT PRIMARY KEY,cell TEXT NOT NULL,version TEXT NOT NULL,revision INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS firmware_operation(id TEXT PRIMARY KEY,device TEXT NOT NULL,action TEXT NOT NULL,result TEXT NOT NULL,revision_before INTEGER NOT NULL,revision_after INTEGER NOT NULL);
"""
def connect(path):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True);db=sqlite3.connect(path);db.row_factory=sqlite3.Row;db.executescript(SCHEMA);return db
