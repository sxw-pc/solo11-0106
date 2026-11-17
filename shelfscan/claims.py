"""Freeze borrower handoff records and independent condition inspections."""
import hashlib
import json
from pathlib import Path
from .store import connect


def register(db_path, case_id, fixture):
    source = Path(fixture).resolve()
    raw = source.read_bytes()
    doc = json.loads(raw)
    if not case_id or not doc.get('claims') or not doc.get('contracts'):
        raise ValueError('bad_damage_case')
    db = connect(db_path)
    try:
        with db:
            records = []
            for claim in doc['claims']:
                cid = claim['copy_id']
                book = db.execute('SELECT * FROM copy WHERE id=?', (cid,)).fetchone()
                if book is None:
                    raise ValueError('unknown_damage_copy')
                events = [dict(row) for row in db.execute(
                    'SELECT e.id,e.device,e.seq,e.kind,e.at,e.payload,e.sha256,'
                    's.sha256 AS source_sha256 FROM event e JOIN source s ON s.id=e.source_id '
                    'WHERE e.copy_id=? ORDER BY e.at,e.id', (cid,))]
                if not {'borrow', 'return'}.issubset({e['kind'] for e in events}):
                    raise ValueError('damage_loan_events_missing')
                records.append({'copy': dict(book), 'events': events})
            payload = json.dumps({'fixture': str(source), 'digest': hashlib.sha256(raw).hexdigest(),
                                  'doc': doc, 'records': records}, sort_keys=True, separators=(',', ':'))
            old = db.execute('SELECT payload FROM damage_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('damage_case_conflict')
            db.execute('INSERT OR IGNORE INTO damage_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'claims': len(records)}
