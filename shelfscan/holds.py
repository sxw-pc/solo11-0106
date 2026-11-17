"""Freeze patron demand alongside imported copy and inventory evidence."""
import hashlib
import json
from pathlib import Path
from .store import connect


def register(db_path, case_id, fixture):
    source = Path(fixture).resolve()
    raw = source.read_bytes()
    doc = json.loads(raw)
    if not case_id or not doc.get('copies') or not doc.get('holds') or not doc.get('contracts'):
        raise ValueError('bad_hold_case')
    db = connect(db_path)
    try:
        with db:
            copies = []
            for item in doc['copies']:
                cid = item['copy_id']
                book = db.execute('SELECT * FROM copy WHERE id=?', (cid,)).fetchone()
                if book is None:
                    raise ValueError('unknown_hold_copy')
                events = [dict(row) for row in db.execute(
                    'SELECT e.id,e.device,e.seq,e.at,e.shelf,e.signal,e.sha256,'
                    's.sha256 AS source_sha256 FROM event e JOIN source s ON s.id=e.source_id '
                    'WHERE e.copy_id=? ORDER BY e.at,e.id', (cid,))]
                if not events:
                    raise ValueError('hold_copy_unobserved')
                copies.append({'copy': dict(book), 'events': events})
            payload = json.dumps({'fixture': str(source), 'digest': hashlib.sha256(raw).hexdigest(),
                                  'doc': doc, 'copies': copies}, sort_keys=True, separators=(',', ':'))
            old = db.execute('SELECT payload FROM hold_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('hold_case_conflict')
            db.execute('INSERT OR IGNORE INTO hold_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'copies': len(copies), 'holds': len(doc['holds'])}
