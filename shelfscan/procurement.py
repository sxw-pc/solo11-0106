"""Freeze missing-copy demand and supplier quotes without placing orders."""
import hashlib
import json
from pathlib import Path
from .store import connect


def register(db_path, case_id, fixture):
    source = Path(fixture).resolve()
    raw = source.read_bytes()
    doc = json.loads(raw)
    if not case_id or not doc.get('requests') or not doc.get('quotes') or not doc.get('contracts'):
        raise ValueError('bad_replacement_case')
    db = connect(db_path)
    try:
        with db:
            requests = []
            for item in doc['requests']:
                cid = item['missing_copy']
                decision = db.execute('SELECT * FROM decision WHERE copy_id=?', (cid,)).fetchone()
                if decision is None or decision['status'] != '缺失':
                    raise ValueError('replacement_copy_not_missing')
                book = db.execute('SELECT * FROM copy WHERE id=?', (cid,)).fetchone()
                requests.append({'request': item, 'copy': dict(book), 'decision': dict(decision)})
            evidence = [dict(row) for row in db.execute(
                'SELECT e.id,e.copy_id,e.kind,e.at,e.sha256,s.sha256 AS source_sha256 '
                'FROM event e JOIN source s ON s.id=e.source_id ORDER BY e.id')]
            payload = json.dumps({'fixture': str(source), 'digest': hashlib.sha256(raw).hexdigest(),
                                  'doc': doc, 'requests': requests, 'evidence': evidence},
                                 sort_keys=True, separators=(',', ':'))
            old = db.execute('SELECT payload FROM replacement_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('replacement_case_conflict')
            db.execute('INSERT OR IGNORE INTO replacement_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'requests': len(requests), 'quotes': len(doc['quotes'])}
