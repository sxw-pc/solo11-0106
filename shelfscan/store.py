import sqlite3
from pathlib import Path

SCHEMA = """
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
CREATE TABLE IF NOT EXISTS label_case(id TEXT PRIMARY KEY,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS label_template(case_id TEXT PRIMARY KEY REFERENCES label_case(id),version TEXT NOT NULL,updated_at TEXT NOT NULL);

-- A label job confirms exactly one pre-authorised in-house layout.  The
-- alternate layout is rendered as an unexecuted control in label_control.
CREATE TABLE IF NOT EXISTS label_job(
 id INTEGER PRIMARY KEY,
 case_id TEXT NOT NULL REFERENCES label_case(id),
 contract TEXT NOT NULL CHECK(contract IN ('public_call_number','opaque_tag')),
 template_version TEXT NOT NULL,
 input_sha256 TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('planned','active','interrupted','completed','aborted')),
 created_at TEXT NOT NULL,
 ended_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS label_job_one_active ON label_job(case_id)
 WHERE status IN ('planned','active','interrupted');

-- One row per serial ever reserved.  payload is the exact byte string sent to
-- the printer.  A partial unique index permits at most one live label per copy
-- (reserved/sent/emitted/verified/applied).
CREATE TABLE IF NOT EXISTS label(
 serial TEXT PRIMARY KEY,
 job_id INTEGER NOT NULL REFERENCES label_job(id),
 copy_id TEXT NOT NULL REFERENCES copy(id),
 contract TEXT NOT NULL CHECK(contract IN ('public_call_number','opaque_tag')),
 template_version TEXT NOT NULL,
 payload BLOB NOT NULL,
 payload_sha256 TEXT NOT NULL,
 subject_sha256 TEXT NOT NULL,
 public_shelf TEXT,
 opaque_token TEXT,
 replaces_serial TEXT,
 void_reason TEXT,
 state TEXT NOT NULL CHECK(state IN ('reserved','sent','emitted','verified','applied','voided','quarantined','destroyed','superseded')),
 reserved_at TEXT NOT NULL,
 updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS label_one_live_per_copy ON label(copy_id)
 WHERE state IN ('reserved','sent','emitted','verified','applied');
CREATE INDEX IF NOT EXISTS label_job_idx ON label(job_id);
CREATE INDEX IF NOT EXISTS label_copy_idx ON label(copy_id);

-- A print command.  nonce is non-repeatable and unique; request binds the nonce
-- to the exact paper bytes and payload digest.
CREATE TABLE IF NOT EXISTS label_command(
 id INTEGER PRIMARY KEY,
 job_id INTEGER NOT NULL REFERENCES label_job(id),
 serial TEXT NOT NULL REFERENCES label(serial),
 nonce TEXT NOT NULL UNIQUE,
 request BLOB NOT NULL,
 request_sha256 TEXT NOT NULL,
 paper_sha256 TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('sent','emitted','acked','failed')),
 created_at TEXT NOT NULL,
 ack_at TEXT,
 ack_payload TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS label_command_serial ON label_command(serial);

-- Mirror of the simulated PRN-1 durable spool.  The device journal (a separate
-- SQLite file) is authoritative; this mirror is rebuilt by sync_device() and is
-- append-only here.
CREATE TABLE IF NOT EXISTS printer_emission(
 id INTEGER PRIMARY KEY,
 command_id INTEGER UNIQUE,
 job_id INTEGER NOT NULL,
 serial TEXT NOT NULL UNIQUE,
 copy_id TEXT NOT NULL,
 nonce TEXT NOT NULL UNIQUE,
 paper BLOB NOT NULL,
 paper_sha256 TEXT NOT NULL,
 device TEXT NOT NULL,
 emitted_at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS printer_emission_no_update BEFORE UPDATE ON printer_emission
 BEGIN SELECT RAISE(ABORT,'immutable_emission'); END;
CREATE TRIGGER IF NOT EXISTS printer_emission_no_delete BEFORE DELETE ON printer_emission
 BEGIN SELECT RAISE(ABORT,'immutable_emission'); END;

-- Simulated VERIFY-1 read-back.  Failed scans are kept as evidence too.
CREATE TABLE IF NOT EXISTS label_verification(
 id INTEGER PRIMARY KEY,
 job_id INTEGER REFERENCES label_job(id),
 serial TEXT NOT NULL,
 copy_id TEXT NOT NULL,
 attached_copy TEXT NOT NULL,
 expected_sha256 TEXT NOT NULL,
 observed_sha256 TEXT NOT NULL,
 observed_serial TEXT,
 device_read_id INTEGER,
 result TEXT NOT NULL CHECK(result IN ('ok','wrong_book','content_mismatch','serial_mismatch','unknown_serial','no_paper','serial_not_valid','snapshot_stale')),
 operator TEXT NOT NULL,
 at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS label_verification_no_update BEFORE UPDATE ON label_verification
 BEGIN SELECT RAISE(ABORT,'immutable_verification'); END;
CREATE TRIGGER IF NOT EXISTS label_verification_no_delete BEFORE DELETE ON label_verification
 BEGIN SELECT RAISE(ABORT,'immutable_verification'); END;

-- Physical destruction is only accepted with a named witness.
CREATE TABLE IF NOT EXISTS label_destruction(
 id INTEGER PRIMARY KEY,
 serial TEXT NOT NULL UNIQUE REFERENCES label(serial),
 witness TEXT NOT NULL,
 reason TEXT NOT NULL,
 at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS label_destruction_no_update BEFORE UPDATE ON label_destruction
 BEGIN SELECT RAISE(ABORT,'immutable_destruction'); END;
CREATE TRIGGER IF NOT EXISTS label_destruction_no_delete BEFORE DELETE ON label_destruction
 BEGIN SELECT RAISE(ABORT,'immutable_destruction'); END;

-- Step-by-step append-only label audit trail.
CREATE TABLE IF NOT EXISTS label_evidence(
 id INTEGER PRIMARY KEY,
 job_id INTEGER,
 serial TEXT,
 step TEXT NOT NULL,
 evidence_sha256 TEXT NOT NULL,
 previous_sha256 TEXT,
 actor TEXT,
 detail TEXT,
 at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS label_evidence_no_update BEFORE UPDATE ON label_evidence
 BEGIN SELECT RAISE(ABORT,'immutable_label_evidence'); END;
CREATE TRIGGER IF NOT EXISTS label_evidence_no_delete BEFORE DELETE ON label_evidence
 BEGIN SELECT RAISE(ABORT,'immutable_label_evidence'); END;

-- The non-executed layout is retained as a rendered comparison only.  It can
-- never be marked executed, reserved or emitted.
CREATE TABLE IF NOT EXISTS label_control(
 job_id INTEGER NOT NULL REFERENCES label_job(id),
 copy_id TEXT NOT NULL,
 contract TEXT NOT NULL CHECK(contract IN ('public_call_number','opaque_tag')),
 serial TEXT,
 payload BLOB NOT NULL,
 payload_sha256 TEXT NOT NULL,
 executed INTEGER NOT NULL DEFAULT 0 CHECK(executed=0),
 PRIMARY KEY(job_id,copy_id,contract)
);
"""


def connect(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.row_factory = sqlite3.Row
    db.executescript(SCHEMA)
    return db


def connect_tx(path):
    """Connection for explicit BEGIN IMMEDIATE transactions.

    Autocommit is disabled at the sqlite3 level so callers control commit and
    rollback boundaries precisely (a printer may emit while a library
    transaction is still open).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.executescript(SCHEMA)
    return db
