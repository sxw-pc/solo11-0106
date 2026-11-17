"""Simulated local reader endpoints with deterministic lost ACK and rejection."""
import hashlib
import json
from pathlib import Path
from .store import connect


def register(db_path, case_id, fixture):
    source = Path(fixture).resolve()
    raw = source.read_bytes()
    doc = json.loads(raw)
    if not case_id or not doc.get('readers') or not doc.get('contracts'):
        raise ValueError('bad_firmware_case')
    db = connect(db_path)
    try:
        with db:
            observed = []
            for item in doc['readers']:
                rid = item['id']
                if db.execute('SELECT 1 FROM reader WHERE id=?', (rid,)).fetchone() is None:
                    raise ValueError('firmware_reader_unknown')
                rows = [dict(row) for row in db.execute(
                    'SELECT e.id,e.device,e.seq,e.at,e.sha256,s.sha256 AS source_sha256 '
                    'FROM event e JOIN source s ON s.id=e.source_id '
                    'WHERE e.device=? ORDER BY e.seq', (rid,))]
                if not rows:
                    raise ValueError('firmware_reader_unobserved')
                db.execute('INSERT OR IGNORE INTO firmware_device VALUES (?,?,?,0)',
                           (rid, item['cell'], item['current_version']))
                observed.append({'reader': item, 'events': rows})
            payload = json.dumps({'fixture': str(source), 'digest': hashlib.sha256(raw).hexdigest(),
                                  'doc': doc, 'observed': observed}, sort_keys=True, separators=(',', ':'))
            old = db.execute('SELECT payload FROM firmware_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('firmware_case_conflict')
            db.execute('INSERT OR IGNORE INTO firmware_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'readers': len(observed)}


def state(db_path, reader_id):
    db = connect(db_path)
    try:
        row = db.execute('SELECT * FROM firmware_device WHERE id=?', (reader_id,)).fetchone()
        if row is None:
            raise ValueError('firmware_reader_unknown')
        return dict(row)
    finally:
        db.close()


def invoke(db_path, reader_id, operation_id, expected_revision, action='install'):
    db = connect(db_path)
    try:
        with db:
            prior = db.execute('SELECT * FROM firmware_operation WHERE id=?', (operation_id,)).fetchone()
            if prior is not None:
                if prior['device'] != reader_id or prior['action'] != action:
                    raise ValueError('firmware_operation_conflict')
                result = prior['result']
                return {'status': result, 'revision': prior['revision_after']}
            row = db.execute('SELECT * FROM firmware_device WHERE id=?', (reader_id,)).fetchone()
            if row is None:
                raise ValueError('firmware_reader_unknown')
            if row['revision'] != expected_revision:
                raise ValueError('firmware_revision_conflict')
            case = db.execute('SELECT payload FROM firmware_case LIMIT 1').fetchone()
            if case is None:
                raise ValueError('firmware_case_missing')
            doc = json.loads(case['payload'])['doc']
            if action not in ('install', 'rollback'):
                raise ValueError('firmware_action_unknown')
            fault = doc['faults'].get(reader_id) if action == 'install' else None
            if fault == 'reject':
                result = 'rejected'
                after = row['revision']
            else:
                original = next(item['current_version'] for item in doc['readers'] if item['id'] == reader_id)
                version = doc['target_version'] if action == 'install' else original
                after = row['revision'] + 1
                db.execute('UPDATE firmware_device SET version=?,revision=? WHERE id=?',
                           (version, after, reader_id))
                result = 'committed_ack_lost' if fault == 'commit_then_ack_lost' else 'committed'
            db.execute('INSERT INTO firmware_operation VALUES (?,?,?,?,?,?)',
                       (operation_id, reader_id, action, result, row['revision'], after))
        return {'status': 'timeout' if result == 'committed_ack_lost' else result, 'revision': after}
    finally:
        db.close()
