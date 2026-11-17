"""Freeze two catalogued copies and a simulated physical label job."""
import hashlib
import json
from pathlib import Path
from .store import connect


def register(db_path, case_id, fixture):
    source = Path(fixture).resolve()
    raw = source.read_bytes()
    doc = json.loads(raw)
    if not case_id or not doc.get('copies') or not doc.get('contracts') or not doc.get('printer'):
        raise ValueError('bad_label_case')
    db = connect(db_path)
    try:
        with db:
            copies = []
            for cid in doc['copies']:
                row = db.execute('SELECT * FROM copy WHERE id=?', (cid,)).fetchone()
                if row is None:
                    raise ValueError('unknown_label_copy')
                events = [dict(event) for event in db.execute(
                    'SELECT e.id,e.device,e.seq,e.at,e.sha256,s.sha256 AS source_sha256 '
                    'FROM event e JOIN source s ON s.id=e.source_id '
                    'WHERE e.copy_id=? ORDER BY e.at,e.id', (cid,))]
                copies.append({'copy': dict(row), 'events': events})
            payload = json.dumps({'fixture': str(source), 'digest': hashlib.sha256(raw).hexdigest(),
                                  'doc': doc, 'copies': copies}, sort_keys=True, separators=(',', ':'))
            old = db.execute('SELECT payload FROM label_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('label_case_conflict')
            db.execute('INSERT OR IGNORE INTO label_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'copies': len(copies), 'serials': len(doc['serials'])}
