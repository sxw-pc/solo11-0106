"""Freeze actual borrower events and a prior aggregate disclosure ledger."""
import hashlib
import json
from pathlib import Path
from .store import connect


def register(db_path, case_id, fixture):
    source = Path(fixture).resolve()
    raw = source.read_bytes()
    doc = json.loads(raw)
    if not case_id or not doc.get('patrons') or not doc.get('prior_releases') or not doc.get('proposed'):
        raise ValueError('bad_patron_privacy_case')
    db = connect(db_path)
    try:
        with db:
            borrowed = []
            for patron in doc['patrons']:
                rows = [dict(row) for row in db.execute(
                    'SELECT e.id,e.copy_id,e.kind,e.at,e.payload,e.sha256,'
                    's.sha256 AS source_sha256 FROM event e JOIN source s ON s.id=e.source_id '
                    "WHERE e.kind='borrow' ORDER BY e.at,e.id")]
                matches = [row for row in rows if json.loads(row['payload']).get('patron') == patron]
                if not matches:
                    raise ValueError('patron_borrow_source_missing')
                borrowed.append({'patron': patron, 'events': matches})
            payload = json.dumps({'fixture': str(source), 'digest': hashlib.sha256(raw).hexdigest(),
                                  'doc': doc, 'borrowed': borrowed},
                                 sort_keys=True, separators=(',', ':'))
            old = db.execute('SELECT payload FROM patron_privacy_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('patron_privacy_case_conflict')
            db.execute('INSERT OR IGNORE INTO patron_privacy_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'patrons': len(borrowed),
            'borrow_events': sum(len(item['events']) for item in borrowed)}
