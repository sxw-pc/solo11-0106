"""Freeze an unknown RFID tag, physical custody, and donor authorization."""
import hashlib
import json
from pathlib import Path
from .store import connect


def register(db_path, case_id, fixture):
    source = Path(fixture).resolve()
    raw = source.read_bytes()
    doc = json.loads(raw)
    if not case_id or not doc.get('tag') or not doc.get('deed') or not doc.get('custody'):
        raise ValueError('bad_donation_case')
    db = connect(db_path)
    try:
        with db:
            tag = doc['tag']
            if db.execute('SELECT 1 FROM copy WHERE id=?', (tag,)).fetchone():
                raise ValueError('donation_tag_already_catalogued')
            events = [dict(row) for row in db.execute(
                'SELECT e.id,e.copy_id,e.device,e.seq,e.at,e.shelf,e.signal,e.sha256,'
                's.sha256 AS source_sha256 FROM event e JOIN source s ON s.id=e.source_id '
                'WHERE e.copy_id=? ORDER BY e.at,e.id', (tag,))]
            if not events:
                raise ValueError('donation_scan_missing')
            payload = json.dumps({'fixture': str(source), 'digest': hashlib.sha256(raw).hexdigest(),
                                  'doc': doc, 'events': events}, sort_keys=True, separators=(',', ':'))
            old = db.execute('SELECT payload FROM donation_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('donation_case_conflict')
            db.execute('INSERT OR IGNORE INTO donation_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'tag': tag, 'scans': len(events)}
