import json
from pathlib import Path
from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan.rules import evaluate, build_bursts, cumulative_scores, burst_scores
from shelfscan.store import connect
from shelfscan.report import report

T0='2026-01-01T08:00:00+00:00'

def make_config(tmp, **fc):
    cfg={
        'batch':[{'id':'INV-1','version':'v1','start':'2026-01-01T00:00:00+00:00','end':'2026-01-02T00:00:00+00:00','status':'待接收'}],
        'copy':[{'id':'C1','planned_shelf':'S1','loaned':0,'batch':'INV-1'},
                {'id':'C2','planned_shelf':'S2','loaned':1,'batch':'INV-1'}],
        'reader':[{'id':'R1','shelf':'S1','window_start':'2026-01-01T00:00:00+00:00','window_end':'2026-01-02T00:00:00+00:00'},
                  {'id':'R2','shelf':'S2','window_start':'2026-01-01T00:00:00+00:00','window_end':'2026-01-02T00:00:00+00:00'}],
        'fusion_contract':{'strategy':'cumulative','burst_window_seconds':30.0,'confidence_multiplier':1.5,'contract_version':'fc-1'}}
    cfg['fusion_contract'].update(fc)
    p=tmp/'config.json';p.write_text(json.dumps(cfg,ensure_ascii=False),encoding='utf-8');return p

def setup(tmp,**fc):
    db=tmp/f"db-{fc.get('strategy','cum')}-{fc.get('burst_window_seconds',30)}-{fc.get('confidence_multiplier',1.5)}.db"
    initialize(db,make_config(tmp,**fc));return db

def ev(device,seq,at,shelf,signal,copy='C1',kind='scan'):
    return {'kind':kind,'device':device,'seq':seq,'copy_id':copy,'at':at,'shelf':shelf,'signal':signal}

def add(tmp,db,rows):
    p=tmp/'e.jsonl';p.write_text('\n'.join(json.dumps(r,ensure_ascii=False) for r in rows),encoding='utf-8');ingest(p,db)

def detail(db,cid='C1'):
    return json.loads(db.execute('SELECT detail FROM decision WHERE copy_id=?',(cid,)).fetchone()[0])

def counterexample_rows():
    # 有心跳、连续序号、读者位置有效：S1 一次 80；邻架 S2 30 秒内三次各 50
    return [ev('R1',1,T0,'S1',80),
            {'kind':'heartbeat','device':'R1','seq':2,'copy_id':'C1','at':'2026-01-01T08:00:05+00:00','shelf':None,'signal':None},
            ev('R2',1,'2026-01-01T08:00:10+00:00','S2',50),
            ev('R2',2,'2026-01-01T08:00:20+00:00','S2',50),
            ev('R2',3,'2026-01-01T08:00:30+00:00','S2',50)]

def test_cumulative_strategy_judges_neighbor_shelf(tmp_path):
    db=setup(tmp_path)  # 冻结策略：cumulative
    add(tmp_path,db,counterexample_rows())
    assert evaluate(db)['C1']=='串架'
    c=connect(db);d=detail(c)
    assert d['fusion']['adopted_strategy']=='cumulative'
    assert d['cumulative_scores']=={'S1':80.0,'S2':150.0}
    assert d['burst_scores']=={'S1':80.0,'S2':50.0}
    assert d['candidate_scores']==d['cumulative_scores']
    c.close()

def test_burst_neutral_strategy_judges_book_on_planned_shelf(tmp_path):
    db=setup(tmp_path,strategy='burst_neutral')
    add(tmp_path,db,counterexample_rows())
    assert evaluate(db)['C1']=='在架'
    c=connect(db);d=detail(c)
    assert d['fusion']['adopted_strategy']=='burst_neutral'
    assert d['candidate_scores']==d['burst_scores']=={'S1':80.0,'S2':50.0}
    c.close()

def test_report_lists_raw_readings_burst_boundaries_both_scores_and_policy(tmp_path):
    db=setup(tmp_path,strategy='burst_neutral')
    add(tmp_path,db,counterexample_rows());evaluate(db)
    out=report(db,tmp_path/'r.json');data=json.loads(out.read_text(encoding='utf-8'))
    d=json.loads(data['decision'][0]['detail'])
    assert [r['signal'] for r in d['raw_readings']]==[80,50,50,50]          # 原始读数
    assert {r['device'] for r in d['raw_readings']}=={'R1','R2'}
    r2=[b for b in d['bursts'] if b['device']=='R2'][0]                    # burst 边界
    assert r2['signals']==[50,50,50] and r2['peak']==50
    assert r2['start_at']=='2026-01-01T08:00:10+00:00' and r2['end_at']=='2026-01-01T08:00:30+00:00'
    assert r2['seqs']==[1,2,3]
    assert d['cumulative_scores']=={'S1':80.0,'S2':150.0}                  # 两种分数
    assert d['burst_scores']=={'S1':80.0,'S2':50.0}
    assert d['fusion']['adopted_strategy']=='burst_neutral'                # 最终采用的快照策略
    assert d['fusion']['frozen_strategy']=='burst_neutral'
    assert len(data['event'])==5 and len(data['evidence'])==5              # 原始扫描与证据未丢

def test_input_events_are_never_rewritten(tmp_path):
    db=setup(tmp_path,strategy='burst_neutral')
    add(tmp_path,db,counterexample_rows())
    before=[dict(r) for r in connect(db).execute('SELECT device,seq,kind,shelf,signal,sha256 FROM event ORDER BY id')]
    evaluate(db);evaluate(db,'cumulative');evaluate(db,'burst_neutral')
    c=connect(db)
    after=[dict(r) for r in c.execute('SELECT device,seq,kind,shelf,signal,sha256 FROM event ORDER BY id')]
    assert before==after
    assert c.execute('SELECT count(*) FROM evidence').fetchone()[0]==5
    hb=c.execute("SELECT kind FROM event WHERE device='R1' AND seq=2").fetchone()[0]
    assert hb=='heartbeat'  # 心跳占序号且保留
    c.close()

def test_window_head_tail_boundary_inclusive_then_new_burst(tmp_path):
    db=setup(tmp_path)
    rows=[ev('R1',1,'2026-01-01T09:00:00+00:00','S1',40),
          ev('R1',2,'2026-01-01T09:00:30+00:00','S1',60),   # 恰好 30s，仍在窗内（含首尾）
          ev('R1',3,'2026-01-01T09:00:31+00:00','S1',55)]   # 超窗，新 burst
    add(tmp_path,db,rows)
    c=connect(db);scans=[r for r in c.execute("SELECT * FROM event WHERE kind='scan' ORDER BY id")]
    scans=[dict(r) for r in scans]
    bursts=build_bursts(scans,30.0)
    assert [b['signals'] for b in bursts]==[[40,60],[55]]
    assert bursts[0]['peak']==60
    assert burst_scores(scans,30.0)=={'S1':115.0}
    assert cumulative_scores(scans)=={'S1':155.0}
    c.close()

def test_identical_timestamps_stay_in_one_device_burst(tmp_path):
    db=setup(tmp_path)
    rows=[ev('R1',1,'2026-01-01T09:00:00+00:00','S1',60),
          ev('R1',2,'2026-01-01T09:00:00+00:00','S1',40)]   # 相同时间戳，同设备同架位
    add(tmp_path,db,rows)
    c=connect(db);scans=[dict(r) for r in c.execute("SELECT * FROM event WHERE kind='scan'")]
    bursts=build_bursts(scans,30.0)
    assert len(bursts)==1 and bursts[0]['signals']==[60,40] and bursts[0]['peak']==60
    c.close()

def test_adjacent_windows_split_at_gap_not_at_fixed_grid(tmp_path):
    db=setup(tmp_path)
    rows=[ev('R1',1,'2026-01-01T09:00:00+00:00','S1',10),
          ev('R1',2,'2026-01-01T09:00:20+00:00','S1',20),
          ev('R1',3,'2026-01-01T09:00:40+00:00','S1',30),   # 距首读 40s>30s，跨界新 burst
          ev('R1',4,'2026-01-01T09:01:00+00:00','S1',40)]   # 距 09:00:40 仅 20s，同 burst
    add(tmp_path,db,rows)
    c=connect(db);scans=[dict(r) for r in c.execute("SELECT * FROM event WHERE kind='scan'")]
    bursts=build_bursts(scans,30.0)
    assert [b['seqs'] for b in bursts]==[[1,2],[3,4]]
    assert [b['peak'] for b in bursts]==[20,40]
    c.close()

def test_different_readers_same_time_are_never_merged(tmp_path):
    db=setup(tmp_path,strategy='burst_neutral')
    rows=[ev('R1',1,'2026-01-01T09:00:00+00:00','S1',70),
          ev('R2',1,'2026-01-01T09:00:00+00:00','S2',70)]   # 同时同册，不同读者设备
    add(tmp_path,db,rows);evaluate(db)
    c=connect(db);d=detail(c)
    assert len(d['bursts'])==2
    assert {b['device'] for b in d['bursts']}=={'R1','R2'}  # 归并只影响同一设备
    assert d['burst_scores']=={'S1':70.0,'S2':70.0}
    assert c.execute("SELECT status FROM decision WHERE copy_id='C1'").fetchone()[0]=='待核'  # 1.5 倍差不满足
    c.close()

def test_loaned_copy_and_loan_timing_unchanged_across_strategies(tmp_path):
    db=setup(tmp_path)
    rows=[ev('L1',1,'2026-01-01T08:10:00+00:00',None,None,kind='borrow'),
          ev('L1',2,'2026-01-01T08:20:00+00:00',None,None,kind='return'),
          ev('R1',3,'2026-01-01T08:30:00+00:00','S1',75)]
    add(tmp_path,db,rows)
    assert evaluate(db,'cumulative')==evaluate(db,'burst_neutral')
    c=connect(db)
    assert c.execute("SELECT status FROM decision WHERE copy_id='C1'").fetchone()[0]=='在架'
    assert c.execute("SELECT status FROM decision WHERE copy_id='C2'").fetchone()[0]=='借出'  # 借出册状态不受策略影响
    assert c.execute("SELECT count(*) FROM issue WHERE code='bad_loan_transition'").fetchone()[0]==0
    kinds=[r[0] for r in c.execute("SELECT kind FROM event ORDER BY id")]
    assert kinds==['borrow','return','scan']  # 借还时序事件原样保留
    c.close()

def test_strategy_switch_only_changes_scores_not_other_copy_state(tmp_path):
    db=setup(tmp_path)
    add(tmp_path,db,counterexample_rows())
    evaluate(db,'cumulative');c=connect(db)
    s1={r['copy_id']:r['status'] for r in c.execute('SELECT copy_id,status FROM decision')}
    ev1=[dict(r) for r in c.execute('SELECT id,device,seq,kind,at,copy_id,shelf,signal FROM event ORDER BY id')]
    c.close()
    evaluate(db,'burst_neutral');c=connect(db)
    s2={r['copy_id']:r['status'] for r in c.execute('SELECT copy_id,status FROM decision')}
    ev2=[dict(r) for r in c.execute('SELECT id,device,seq,kind,at,copy_id,shelf,signal FROM event ORDER BY id')]
    assert ev1==ev2
    assert s1['C2']==s2['C2']=='借出'
    assert s1['C1']=='串架' and s2['C1']=='在架'  # 仅目标册判定随策略变化
    d=detail(c);assert d['fusion']['requested_strategy']=='burst_neutral' and d['fusion']['frozen_strategy']=='cumulative'
    c.close()

def test_heartbeat_keeps_sequence_continuous_and_excluded_from_scores(tmp_path):
    db=setup(tmp_path,strategy='burst_neutral')
    add(tmp_path,db,counterexample_rows());evaluate(db)
    c=connect(db);d=detail(c)
    assert len(d['raw_readings'])==4  # 心跳不进位置分
    assert c.execute("SELECT count(*) FROM issue WHERE code='sequence_gap'").fetchone()[0]==0
    c.close()

def test_contract_window_and_multiplier_participate_in_decision(tmp_path):
    db=setup(tmp_path,confidence_multiplier=2.0)  # 150 < 80*2=160 -> 多架待核
    add(tmp_path,db,counterexample_rows())
    assert evaluate(db)['C1']=='待核'
    db2=setup(tmp_path,burst_window_seconds=5.0,strategy='burst_neutral')  # 三次弱读切成三个 burst，仍各只取峰
    add(tmp_path,db2,counterexample_rows())
    c=connect(db2);evaluate(db2);d=detail(c)
    r2=[b for b in d['bursts'] if b['device']=='R2']
    assert len(r2)==3 and d['burst_scores']=={'S1':80.0,'S2':150.0}
    c.close()

def test_unknown_strategy_rejected(tmp_path):
    db=setup(tmp_path);add(tmp_path,db,counterexample_rows())
    import pytest
    with pytest.raises(ValueError):evaluate(db,'max_only')

def test_frozen_contract_persisted_in_snapshot(tmp_path):
    db=setup(tmp_path,strategy='burst_neutral',burst_window_seconds=20.0,confidence_multiplier=1.6)
    c=connect(db);row=dict(c.execute('SELECT * FROM fusion_contract WHERE id=1').fetchone())
    assert row['strategy']=='burst_neutral' and row['burst_window_seconds']==20.0 and row['confidence_multiplier']==1.6
    c.close()
