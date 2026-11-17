from pathlib import Path
from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan.rules import evaluate
from shelfscan.routes import register
from shelfscan.store import connect
import pytest

FIX=Path(__file__).resolve().parents[1]/'fixtures'

def test_route_case_freezes_two_misplaced_copies_without_plan(tmp_path):
    db=tmp_path/'route.db';initialize(db,FIX/'route-config.json')
    assert ingest(FIX/'route-events.jsonl',db)['imported']==2
    assert evaluate(db)=={'A':'串架','B':'串架'}
    got=register(db,'route-01',FIX/'route-case.json')
    assert got['tasks']==2
    assert register(db,'route-01',FIX/'route-case.json')==got
    conn=connect(db)
    try:assert conn.execute('SELECT COUNT(*) FROM route_case').fetchone()[0]==1
    finally:conn.close()

def test_route_case_rejects_not_yet_evaluated_inventory(tmp_path):
    db=tmp_path/'route.db';initialize(db,FIX/'route-config.json')
    with pytest.raises(ValueError,match='route_copy_not_misplaced'):
        register(db,'route-01',FIX/'route-case.json')
