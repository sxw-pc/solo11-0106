"""Freeze synthetic reshelving work against actual misplaced-copy decisions."""
import hashlib,json
from pathlib import Path
from .store import connect

def register(db_path,case_id,fixture):
    if not case_id:raise ValueError('missing_route_case_id')
    source=Path(fixture).resolve();raw=source.read_bytes();doc=json.loads(raw)
    if not doc.get('tasks') or not doc.get('positions') or not doc.get('objectives'):
        raise ValueError('bad_route_contract')
    db=connect(db_path)
    try:
        with db:
            decisions=[]
            for item in doc['tasks']:
                row=db.execute('SELECT * FROM decision WHERE copy_id=?',(item['copy_id'],)).fetchone()
                if not row or row['status']!='串架' or row['actual_shelf']!=item['pickup_shelf']:
                    raise ValueError('route_copy_not_misplaced')
                book=db.execute('SELECT planned_shelf FROM copy WHERE id=?',(item['copy_id'],)).fetchone()
                if not book or book['planned_shelf']!=item['drop_shelf']:
                    raise ValueError('route_destination_mismatch')
                decisions.append(dict(row))
            events=[dict(r) for r in db.execute('SELECT e.id,e.copy_id,e.device,e.seq,e.sha256,s.sha256 AS source_hash FROM event e JOIN source s ON s.id=e.source_id WHERE e.copy_id IN (%s) ORDER BY e.id' % ','.join('?' for _ in doc['tasks']),tuple(x['copy_id'] for x in doc['tasks']))]
            payload=json.dumps({'fixture':str(source),'digest':hashlib.sha256(raw).hexdigest(),
                                'doc':doc,'decisions':decisions,'events':events},sort_keys=True,separators=(',',':'))
            old=db.execute('SELECT payload FROM route_case WHERE id=?',(case_id,)).fetchone()
            if old and old['payload']!=payload:raise ValueError('route_case_conflict')
            db.execute('INSERT OR IGNORE INTO route_case VALUES (?,?)',(case_id,payload))
    finally:db.close()
    return {'case_id':case_id,'digest':hashlib.sha256(raw).hexdigest(),'tasks':len(decisions)}
