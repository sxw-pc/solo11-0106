import hashlib,json
from datetime import datetime
from pathlib import Path
from .store import connect
KINDS={'scan','borrow','return','heartbeat'}
def when(s):
    d=datetime.fromisoformat(s.replace('Z','+00:00'))
    if d.tzinfo is None:raise ValueError('timezone_required')
    return d
def ingest(path,db_path):
    raw=Path(path).read_bytes();h=hashlib.sha256(raw).hexdigest();db=connect(db_path)
    if db.execute('SELECT 1 FROM source WHERE sha256=?',(h,)).fetchone():db.close();return {'imported':0,'duplicate_file':True}
    count=0
    with db:
        sid=db.execute('INSERT INTO source(name,sha256,raw) VALUES (?,?,?)',(str(path),h,raw)).lastrowid
        for line,text in enumerate(raw.decode('utf-8-sig').splitlines(),1):
            try:
                r=json.loads(text);kind=r['kind'];device=str(r['device']);seq=int(r['seq']);copy=str(r['copy_id']);when(r['at'])
                if kind not in KINDS:raise ValueError('unknown_kind')
                digest=hashlib.sha256(json.dumps(r,sort_keys=True,separators=(',',':')).encode()).hexdigest()
                old=db.execute('SELECT sha256 FROM event WHERE device=? AND seq=?',(device,seq)).fetchone()
                if old:
                    if old['sha256']!=digest:db.execute('INSERT INTO issue(code,copy_id,detail) VALUES (?,?,?)',('identity_conflict',copy,f'{device}:{seq}'))
                    continue
                eid=db.execute('INSERT INTO event(source_id,line_no,device,seq,kind,at,copy_id,shelf,signal,payload,sha256) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                    (sid,line,device,seq,kind,r['at'],copy,r.get('shelf'),r.get('signal'),text,digest)).lastrowid;db.execute('INSERT INTO evidence(event_id,source_sha256,payload_sha256,role) VALUES (?,?,?,?)',(eid,h,digest,'inventory_event'));count+=1
            except (ValueError,KeyError,TypeError,json.JSONDecodeError) as e:
                db.execute('INSERT INTO issue(code,copy_id,detail) VALUES (?,?,?)',('parse_error',str(line),str(e)))
    db.close();return {'imported':count,'duplicate_file':False}
