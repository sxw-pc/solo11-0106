import json
from pathlib import Path
from shelfscan.__main__ import initialize,demo
from shelfscan.ingest import ingest
from shelfscan.rules import evaluate
from shelfscan.store import connect
FIX=Path(__file__).resolve().parent.parent/'fixtures'
def setup(tmp):
    db=tmp/'x.db';initialize(db,FIX/'config.json');return db
def add(tmp,db,rr):
    p=tmp/'e.jsonl';p.write_text('\n'.join(json.dumps(r) for r in rr),encoding='utf-8');ingest(p,db)
def event(**changes):
    r={'kind':'scan','device':'R1','seq':1,'copy_id':'C1','at':'2026-01-01T08:00:00+00:00','shelf':'S1','signal':75};r.update(changes);return r
def test_sequence_gap_is_audited(tmp_path):
    db=setup(tmp_path);add(tmp_path,db,[event(),event(seq=3,at='2026-01-01T08:01:00+00:00')]);evaluate(db)
    c=connect(db);assert c.execute("SELECT count(*) FROM issue WHERE code='sequence_gap'").fetchone()[0]>=1
def test_bad_borrow_transition(tmp_path):
    db=setup(tmp_path);add(tmp_path,db,[event(copy_id='C2',device='L1',kind='borrow',shelf=None,signal=None)]);evaluate(db)
    c=connect(db);assert c.execute("SELECT count(*) FROM issue WHERE code='bad_loan_transition'").fetchone()[0]==1
def test_invalid_signal_not_location(tmp_path):
    db=setup(tmp_path);add(tmp_path,db,[event(signal=150)]);assert evaluate(db)['C1']=='缺失'
def test_snapshot_version_in_decision(tmp_path):
    data=json.loads(demo(tmp_path).read_text(encoding='utf-8'));assert json.loads(data['decision'][0]['detail'])['snapshot_version']=='v1'
def test_report_keeps_multiple_scans(tmp_path):
    data=json.loads(demo(tmp_path).read_text(encoding='utf-8'));assert len(data['event'])==2;assert len(data['evidence'])==2
