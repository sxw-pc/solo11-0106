import json
from collections import defaultdict
from .store import connect
from .ingest import when
VERSION='2'
STRATEGIES=('cumulative','burst_neutral')
DEFAULT_CONTRACT={'strategy':'cumulative','burst_window_seconds':30.0,'confidence_multiplier':1.5,'contract_version':'fc-1'}

def load_contract(db):
    row=db.execute('SELECT * FROM fusion_contract WHERE id=1').fetchone()
    return dict(row) if row else dict(DEFAULT_CONTRACT)

def build_bursts(scans,window_seconds):
    """同一设备、同一册、同一架位的时间窗突发；不同设备永不归并。"""
    groups=defaultdict(list)
    for e in scans:groups[(e['device'],e['shelf'])].append(e)
    bursts=[]
    for (device,shelf),ee in groups.items():
        ee=sorted(ee,key=lambda e:(when(e['at']),e['seq'],e['id']))
        cur=[];anchor=None
        for e in ee:
            t=when(e['at'])
            if anchor is None or (t-anchor).total_seconds()>window_seconds:
                if cur:bursts.append(_make_burst(device,shelf,cur))
                cur=[e];anchor=t
            else:cur.append(e)
        if cur:bursts.append(_make_burst(device,shelf,cur))
    return sorted(bursts,key=lambda b:(b['start_at'],b['device'],b['shelf'],b['peak_event_id']))

def _make_burst(device,shelf,ee):
    peak=max(ee,key=lambda e:(e['signal'],-(when(e['at']).timestamp()),-e['seq'],-e['id']))
    return {'device':device,'shelf':shelf,
            'start_at':ee[0]['at'],'end_at':ee[-1]['at'],
            'peak':peak['signal'],'peak_event_id':peak['id'],
            'event_ids':[e['id'] for e in ee],'seqs':[e['seq'] for e in ee],
            'signals':[e['signal'] for e in ee]}

def cumulative_scores(scans):
    scores=defaultdict(float)
    for e in scans:scores[e['shelf']]+=e['signal']
    return dict(scores)

def burst_scores(scans,window_seconds):
    scores=defaultdict(float)
    for b in build_bursts(scans,window_seconds):scores[b['shelf']]+=b['peak']
    return dict(scores)

def rank(candidate,multiplier):
    if not candidate:return None,False,[]
    ranks=sorted(candidate.items(),key=lambda x:(-x[1],x[0]))
    multi=len(ranks)>1 and ranks[0][1]<ranks[1][1]*multiplier
    return (ranks[0][0] if not multi else None),multi,ranks

def evaluate(path,strategy=None):
    db=connect(path);results={}
    if strategy is not None and strategy not in STRATEGIES:raise ValueError(f'unknown_strategy:{strategy}')
    with db:
        contract=load_contract(db)
        adopted=strategy or contract['strategy']
        window=float(contract['burst_window_seconds']);multiplier=float(contract['confidence_multiplier'])
        run_id=db.execute("INSERT INTO run(status,started_at) VALUES ('待接收',CURRENT_TIMESTAMP)").lastrowid
        db.execute("UPDATE run SET status='已校验' WHERE id=?",(run_id,))
        db.execute('DELETE FROM decision');db.execute("DELETE FROM issue WHERE code NOT IN ('parse_error','identity_conflict')")
        events=[dict(r) for r in db.execute('SELECT * FROM event ORDER BY at,id')]
        known={r['id'] for r in db.execute('SELECT id FROM copy')}
        for e in events:
            if e['copy_id'] not in known:db.execute('INSERT INTO issue(code,copy_id,event_id,detail) VALUES (?,?,?,?)',('unexpected_copy',e['copy_id'],e['id'],'not in frozen snapshot'))
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
                if e['kind']=='heartbeat':continue
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
            cum=cumulative_scores(scans)
            bursts=build_bursts(scans,window)
            neu=defaultdict(float)
            for b in bursts:neu[b['shelf']]+=b['peak']
            neu=dict(neu)
            adopted_scores=cum if adopted=='cumulative' else neu
            actual,multi,ranks=rank(adopted_scores,multiplier)
            if multi:issues.append(('multi_shelf',ranks[0][0]))
            if loaned:status='借出'
            elif any(code=='multi_shelf' for code,_ in issues) or db.execute("SELECT 1 FROM issue WHERE code='identity_conflict' AND copy_id=?",(cid,)).fetchone():status='待核'
            elif actual is None:status='缺失'
            elif actual!=book['planned_shelf']:status='串架'
            else:status='在架'
            if not book['id'] in [e['copy_id'] for e in events]:issues.append(('no_scan',None))
            for code,eid in issues:
                db.execute('INSERT INTO issue(code,copy_id,event_id,detail) VALUES (?,?,?,?)',(code,cid,eid if isinstance(eid,int) else None,str(eid)))
            detail=json.dumps({'snapshot_version':batch['version'],'event_ids':[e['id'] for e in ee],
                'fusion':{'adopted_strategy':adopted,'requested_strategy':strategy,'frozen_strategy':contract['strategy'],
                          'burst_window_seconds':window,'confidence_multiplier':multiplier,'contract_version':contract['contract_version']},
                'raw_readings':[{'event_id':e['id'],'device':e['device'],'seq':e['seq'],'at':e['at'],'shelf':e['shelf'],'signal':e['signal']} for e in scans],
                'bursts':bursts,'cumulative_scores':cum,'burst_scores':neu,
                'candidate_scores':adopted_scores,'candidate_scores_strategy':adopted,
                'issues':[c for c,_ in issues]},ensure_ascii=False)
            db.execute('INSERT INTO decision VALUES (?,?,?,?,?)',(cid,status,actual,detail,VERSION));results[cid]=status
        db.execute("UPDATE run SET status=CASE WHEN EXISTS(SELECT 1 FROM decision WHERE status IN ('待核','缺失','串架')) THEN '待核' ELSE '已判定' END,ended_at=CURRENT_TIMESTAMP WHERE id=?",(run_id,))
    db.close();return results
