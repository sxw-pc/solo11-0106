import json
from collections import defaultdict
from .store import connect
from .ingest import when
VERSION='1'
def evaluate(path):
    db=connect(path);results={}
    with db:
        run_id=db.execute("INSERT INTO run(status,started_at) VALUES ('待接收',CURRENT_TIMESTAMP)").lastrowid
        db.execute("UPDATE run SET status='已校验' WHERE id=?",(run_id,))
        # Historical issues (e.g. unexpected_copy from earlier batches) are evidence and must
        # survive recomputation. New issues are inserted idempotently keyed by their content.
        existing={tuple(r) for r in db.execute(
            'SELECT code,copy_id,event_id,detail FROM issue').fetchall()}
        def add_issue(code,copy_id,event_id,detail):
            key=(code,copy_id,event_id if isinstance(event_id,int) else None,str(detail))
            if key in existing:return
            existing.add(key)
            db.execute('INSERT INTO issue(code,copy_id,event_id,detail,run_id) VALUES (?,?,?,?,?)',
                (code,copy_id,key[3] is not None and event_id or None,detail,run_id))
        db.execute('DELETE FROM decision')
        events=[dict(r) for r in db.execute('SELECT * FROM event ORDER BY at,id')]
        known={r['id'] for r in db.execute('SELECT id FROM copy')}
        for e in events:
            if e['copy_id'] not in known:
                add_issue('unexpected_copy',e['copy_id'],e['id'],'not in frozen snapshot')
        readers={r['id']:dict(r) for r in db.execute('SELECT * FROM reader')}
        devices=defaultdict(list);gaps=set()
        for e in events:devices[e['device']].append(e)
        for device,ee in devices.items():
            seq={e['seq'] for e in ee}
            if max(seq)-min(seq)+1!=len(seq):gaps.update(e['copy_id'] for e in ee)
        for book in db.execute('SELECT * FROM copy'):
            cid=book['id'];batch=db.execute('SELECT * FROM batch WHERE id=?',(book['batch'],)).fetchone();ee=[e for e in events if e['copy_id']==cid]
            loaned=bool(book['loaned']);scans=[];issues=[]
            if cid in gaps:issues.append(('sequence_gap',None))
            for e in ee:
                t=when(e['at'])
                if not when(batch['start'])<=t<=when(batch['end']):issues.append(('outside_batch',e['id']));continue
                if e['kind']=='borrow':
                    if loaned:issues.append(('bad_loan_transition',e['id']))
                    loaned=True
                elif e['kind']=='return':
                    if not loaned:issues.append(('bad_loan_transition',e['id']))
                    loaned=False
                else:
                    reader=readers.get(e['device'])
                    if not reader or not when(reader['window_start'])<=t<when(reader['window_end']) or e['shelf']!=reader['shelf']:
                        issues.append(('reader_location',e['id']));continue
                    if e['signal'] is None or not 0<=e['signal']<=100:issues.append(('signal_invalid',e['id']));continue
                    scans.append(e)
            candidate=defaultdict(float)
            for e in scans:candidate[e['shelf']]+=e['signal']
            actual=None
            if candidate:
                ranks=sorted(candidate.items(),key=lambda x:(-x[1],x[0]))
                if len(ranks)>1 and ranks[0][1]<ranks[1][1]*1.5:issues.append(('multi_shelf',ranks[0][0]))
                else:actual=ranks[0][0]
            if loaned:status='借出'
            elif any(code=='multi_shelf' for code,_ in issues) or db.execute("SELECT 1 FROM issue WHERE code='identity_conflict' AND copy_id=?",(cid,)).fetchone():status='待核'
            elif actual is None:status='缺失'
            elif actual!=book['planned_shelf']:status='串架'
            else:status='在架'
            if not book['id'] in [e['copy_id'] for e in events]:issues.append(('no_scan',None))
            for code,eid in issues:
                add_issue(code,cid,eid if isinstance(eid,int) else None,str(eid))
            detail=json.dumps({'snapshot_version':batch['version'],'event_ids':[e['id'] for e in ee],'candidate_scores':dict(candidate),'issues':[c for c,_ in issues]},ensure_ascii=False)
            db.execute('INSERT INTO decision VALUES (?,?,?,?,?)',(cid,status,actual,detail,VERSION))
            db.execute('INSERT OR IGNORE INTO decision_history VALUES (?,?,?,?,?,?)',
                (run_id,cid,status,actual,detail,VERSION))
            results[cid]=status
        db.execute("UPDATE run SET status=CASE WHEN EXISTS(SELECT 1 FROM decision WHERE status IN ('待核','缺失','串架')) THEN '待核' ELSE '已判定' END,ended_at=CURRENT_TIMESTAMP WHERE id=?",(run_id,))
    db.close();return results
