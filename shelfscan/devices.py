"""Simulated physical devices with their own durable, append-only journals.

The device journal deliberately lives in a separate SQLite file from the
library database.  A printer emits paper in the physical world and that fact
must survive any rollback of the library transaction that requested it.
"""
from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DEVICE_SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS device_command(
 id INTEGER PRIMARY KEY,
 device TEXT NOT NULL,
 job_id INTEGER NOT NULL,
 copy_id TEXT NOT NULL,
 serial TEXT NOT NULL,
 nonce TEXT NOT NULL UNIQUE,
 paper BLOB NOT NULL,
 paper_sha256 TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('sent','emitted','acked','failed')),
 created_at TEXT NOT NULL,
 emitted_at TEXT,
 acked_at TEXT,
 ack TEXT,
 UNIQUE(device,serial)
);
CREATE TABLE IF NOT EXISTS emission(
 id INTEGER PRIMARY KEY,
 command_id INTEGER NOT NULL UNIQUE REFERENCES device_command(id),
 device TEXT NOT NULL,
 job_id INTEGER NOT NULL,
 copy_id TEXT NOT NULL,
 serial TEXT NOT NULL UNIQUE,
 nonce TEXT NOT NULL UNIQUE,
 paper BLOB NOT NULL,
 paper_sha256 TEXT NOT NULL,
 emitted_at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS emission_no_update BEFORE UPDATE ON emission
 BEGIN SELECT RAISE(ABORT,'immutable_emission'); END;
CREATE TRIGGER IF NOT EXISTS emission_no_delete BEFORE DELETE ON emission
 BEGIN SELECT RAISE(ABORT,'immutable_emission'); END;
CREATE TABLE IF NOT EXISTS readback(
 id INTEGER PRIMARY KEY,
 device TEXT NOT NULL,
 job_id INTEGER,
 attached_copy TEXT NOT NULL,
 observed_serial TEXT NOT NULL,
 paper BLOB,
 paper_sha256 TEXT NOT NULL,
 at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS readback_no_update BEFORE UPDATE ON readback
 BEGIN SELECT RAISE(ABORT,'immutable_readback'); END;
CREATE TRIGGER IF NOT EXISTS readback_no_delete BEFORE DELETE ON readback
 BEGIN SELECT RAISE(ABORT,'immutable_readback'); END;
"""


class DeviceError(RuntimeError):
    pass


class DuplicateCommand(DeviceError):
    """The same nonce was presented twice; never re-emit the paper."""


class SerialAlreadyUsed(DeviceError):
    """This physical serial already has a print command on this device."""


class AckLost(DeviceError):
    """Paper was emitted but the acknowledgement never came back."""

    def __init__(self, nonce, command_id, serial):
        super().__init__('paper_emitted_before_ack')
        self.nonce = nonce
        self.command_id = command_id
        self.serial = serial


def now():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def connect_device(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.executescript(DEVICE_SCHEMA)
    return db


class SimPrinter:
    """PRN-1: durable command + emission journal, paper can never be re-emitted."""

    def __init__(self, path, device='PRN-1'):
        self.path = Path(path)
        self.device = device

    def _db(self):
        return connect_device(self.path)

    def print_command(self, command_id, job_id, copy_id, serial, nonce, paper,
                      suppress_ack=False):
        paper = bytes(paper)
        db = self._db()
        try:
            try:
                db.execute('BEGIN IMMEDIATE')
                db.execute(
                    'INSERT INTO device_command(id,device,job_id,copy_id,serial,nonce,paper,paper_sha256,state,created_at) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (command_id, self.device, job_id, copy_id, serial, nonce,
                     paper, sha256(paper), 'sent', now()))
                db.execute('COMMIT')
            except sqlite3.IntegrityError:
                db.execute('ROLLBACK')
                duplicate = db.execute('SELECT * FROM device_command WHERE nonce=?', (nonce,)).fetchone()
                if duplicate:
                    raise DuplicateCommand('duplicate_nonce')
                used = db.execute('SELECT * FROM device_command WHERE device=? AND serial=?',
                                  (self.device, serial)).fetchone()
                if used:
                    raise SerialAlreadyUsed('serial_already_used')
                raise

            # Paper physically leaves the printer: append-only emission record.
            db.execute('BEGIN IMMEDIATE')
            emitted_at = now()
            db.execute(
                'INSERT INTO emission(command_id,device,job_id,copy_id,serial,nonce,paper,paper_sha256,emitted_at) '
                'VALUES (?,?,?,?,?,?,?,?,?)',
                (command_id, self.device, job_id, copy_id, serial, nonce,
                 paper, sha256(paper), emitted_at))
            db.execute(
                "UPDATE device_command SET state='emitted', emitted_at=? WHERE id=?",
                (emitted_at, command_id))
            db.execute('COMMIT')

            if suppress_ack:
                raise AckLost(nonce, command_id, serial)

            acked_at = now()
            ack = ('ACK %s %s %s' % (self.device, serial, sha256(paper))).encode()
            db.execute('BEGIN IMMEDIATE')
            db.execute(
                "UPDATE device_command SET state='acked', acked_at=?, ack=? WHERE id=?",
                (acked_at, ack.decode(), command_id))
            db.execute('COMMIT')
            return {'status': 'acked', 'command_id': command_id, 'serial': serial,
                    'nonce': nonce, 'paper_sha256': sha256(paper),
                    'emitted_at': emitted_at, 'acked_at': acked_at,
                    'ack': ack.decode()}
        finally:
            db.close()

    def deliver_ack(self, nonce):
        """Deliver an acknowledgement that was delayed in transit."""
        db = self._db()
        try:
            row = db.execute('SELECT * FROM device_command WHERE nonce=?', (nonce,)).fetchone()
            if row is None:
                raise DeviceError('unknown_command')
            if row['state'] == 'sent' and db.execute(
                    'SELECT 1 FROM emission WHERE command_id=?', (row['id'],)).fetchone() is None:
                raise DeviceError('command_not_emitted')
            if row['state'] == 'acked':
                return dict(row)
            acked_at = now()
            ack = ('ACK %s %s %s' % (self.device, row['serial'], row['paper_sha256'])).encode()
            db.execute('BEGIN IMMEDIATE')
            db.execute("UPDATE device_command SET state='acked', acked_at=?, ack=? WHERE id=?",
                       (acked_at, ack.decode(), row['id']))
            db.execute('COMMIT')
            return dict(db.execute('SELECT * FROM device_command WHERE id=?', (row['id'],)).fetchone())
        finally:
            db.close()

    def emission(self, serial):
        db = self._db()
        try:
            row = db.execute('SELECT * FROM emission WHERE serial=? AND device=?',
                             (serial, self.device)).fetchone()
            return dict(row) if row else None
        finally:
            db.close()

    def command_for_serial(self, serial):
        db = self._db()
        try:
            row = db.execute('SELECT * FROM device_command WHERE serial=? AND device=?',
                             (serial, self.device)).fetchone()
            return dict(row) if row else None
        finally:
            db.close()

    def command(self, nonce):
        db = self._db()
        try:
            row = db.execute('SELECT * FROM device_command WHERE nonce=? AND device=?',
                             (nonce, self.device)).fetchone()
            return dict(row) if row else None
        finally:
            db.close()

    def emissions(self):
        db = self._db()
        try:
            return [dict(r) for r in db.execute(
                'SELECT * FROM emission WHERE device=? ORDER BY id', (self.device,))]
        finally:
            db.close()

    def commands(self):
        db = self._db()
        try:
            return [dict(r) for r in db.execute(
                'SELECT * FROM device_command WHERE device=? ORDER BY id', (self.device,))]
        finally:
            db.close()


class SimVerifier:
    """VERIFY-1: records what was physically read from the book in front of it."""

    def __init__(self, path, device='VERIFY-1'):
        self.path = Path(path)
        self.device = device

    def record_read(self, attached_copy, observed_serial, paper, job_id=None):
        paper_bytes = None if paper is None else bytes(paper)
        digest = sha256(paper_bytes) if paper_bytes is not None else sha256(b'')
        db = connect_device(self.path)
        try:
            db.execute('BEGIN IMMEDIATE')
            rid = db.execute(
                'INSERT INTO readback(device,job_id,attached_copy,observed_serial,paper,paper_sha256,at) '
                'VALUES (?,?,?,?,?,?,?)',
                (self.device, job_id, attached_copy, observed_serial, paper_bytes,
                 digest, now())).lastrowid
            db.execute('COMMIT')
            return {'id': rid, 'device': self.device, 'attached_copy': attached_copy,
                    'observed_serial': observed_serial,
                    'paper_sha256': digest, 'has_paper': paper_bytes is not None}
        finally:
            db.close()

    def reads(self):
        db = connect_device(self.path)
        try:
            return [dict(r) for r in db.execute(
                'SELECT * FROM readback WHERE device=? ORDER BY id', (self.device,))]
        finally:
            db.close()
