import json
from pathlib import Path
from shelfscan.__main__ import demo,initialize
from shelfscan.ingest import ingest
from shelfscan.rules import evaluate
from shelfscan.store import connect
FIX=Path(__file__).resolve().parent.parent/'fixtures'
def setup(tmp_path):
    db=tmp_path/'x.db';initialize(db,FIX/'config.json');return db
def event(**changes):
    r={'kind':'scan','device':'R1','seq':1,'copy_id':'C1','at':'2026-01-01T08:00:00+00:00','shelf':'S1','signal':75};r.update(changes);return r
def add(tmp_path,db,rows):
    p=tmp_path/'x.jsonl';p.write_text('\n'.join(json.dumps(x) for x in rows),encoding='utf-8');return ingest(p,db)
def test_demo(tmp_path):
    p=demo(tmp_path);j=json.loads(p.read_text(encoding='utf-8'));assert j['decision'][0]['status']=='在架';assert j['decision'][1]['status']=='借出';assert j['source'][0]['byte_length']>0
def test_duplicate(tmp_path):
    db=setup(tmp_path);assert ingest(FIX/'events.jsonl',db)['imported']==2;assert ingest(FIX/'events.jsonl',db)['duplicate_file']
def test_missing(tmp_path):
    db=setup(tmp_path);c=connect(db);c.execute("UPDATE copy SET loaned=0 WHERE id='C2'");c.commit();c.close();assert evaluate(db)['C2']=='缺失'
def test_wrong_shelf(tmp_path):
    db=setup(tmp_path);add(tmp_path,db,[event(device='R2',shelf='S2')]);assert evaluate(db)['C1']=='串架'
def test_multi_shelf(tmp_path):
    db=setup(tmp_path);add(tmp_path,db,[event(),event(device='R2',seq=1,shelf='S2',signal=70)]);assert evaluate(db)['C1']=='待核'
def test_borrow(tmp_path):
    db=setup(tmp_path);add(tmp_path,db,[event(kind='borrow',device='L1',shelf=None,signal=None)]);assert evaluate(db)['C1']=='借出'
def test_return(tmp_path):
    db=setup(tmp_path);add(tmp_path,db,[event(copy_id='C2',device='L1',kind='return',shelf=None,signal=None)]);assert evaluate(db)['C2']=='缺失'
def test_reader_window(tmp_path):
    db=setup(tmp_path);add(tmp_path,db,[event(at='2026-01-03T08:00:00+00:00')]);assert evaluate(db)['C1']=='缺失'
def test_identity_conflict(tmp_path):
    db=setup(tmp_path);ingest(FIX/'events.jsonl',db);add(tmp_path,db,[event(signal=20)]);c=connect(db);assert c.execute("SELECT count(*) FROM issue WHERE code='identity_conflict'").fetchone()[0]==1
