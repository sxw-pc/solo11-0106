"""Freeze external clock observations and existing loan/RFID event evidence."""
import hashlib,json
from pathlib import Path
from .store import connect

def register(db_path,case_id,fixture):
    if not case_id:raise ValueError('missing_loan_clock_case_id')
    source=Path(fixture).resolve();raw=source.read_bytes();doc=json.loads(raw)
    if not doc.get('copy_id') or not doc.get('anchors') or not doc.get('models'):
        raise ValueError('bad_loan_clock_contract')
    db=connect(db_path)
    try:
        with db:
            book=db.execute('SELECT * FROM copy WHERE id=?',(doc['copy_id'],)).fetchone()
            if not book:raise ValueError('unknown_loan_copy')
            events=[dict(r) for r in db.execute('SELECT e.id,e.device,e.seq,e.kind,e.at,e.copy_id,e.shelf,e.signal,e.sha256,s.sha256 AS source_hash FROM event e JOIN source s ON s.id=e.source_id WHERE e.copy_id=? ORDER BY e.at,e.id',(doc['copy_id'],))]
            if not {'borrow','return','scan'}.issubset({e['kind'] for e in events}):
                raise ValueError('loan_clock_events_missing')
            payload=json.dumps({'fixture':str(source),'digest':hashlib.sha256(raw).hexdigest(),
                                'doc':doc,'copy':dict(book),'events':events},sort_keys=True,separators=(',',':'))
            old=db.execute('SELECT payload FROM loan_clock_case WHERE id=?',(case_id,)).fetchone()
            if old and old['payload']!=payload:raise ValueError('loan_clock_case_conflict')
            db.execute('INSERT OR IGNORE INTO loan_clock_case VALUES (?,?)',(case_id,payload))
    finally:db.close()
    return {'case_id':case_id,'digest':hashlib.sha256(raw).hexdigest(),'events':len(events)}
