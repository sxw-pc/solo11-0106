from pathlib import Path
from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan.loan_clock import register
from shelfscan.rules import evaluate
from shelfscan.store import connect
import pytest

FIX=Path(__file__).resolve().parents[1]/'fixtures'

def test_freezes_actual_loan_events_and_anchors_without_calibration(tmp_path):
    db=tmp_path/'loan.db';initialize(db,FIX/'loan-clock-config.json')
    assert ingest(FIX/'loan-clock-events.jsonl',db)['imported']==3
    assert evaluate(db)=={'C5':'借出'}
    got=register(db,'clock-01',FIX/'loan-clock-case.json')
    assert got['events']==3
    assert register(db,'clock-01',FIX/'loan-clock-case.json')==got
    conn=connect(db)
    try:assert conn.execute('SELECT COUNT(*) FROM loan_clock_case').fetchone()[0]==1
    finally:conn.close()

def test_clock_case_rejects_missing_events(tmp_path):
    db=tmp_path/'loan.db';initialize(db,FIX/'loan-clock-config.json')
    with pytest.raises(ValueError,match='loan_clock_events_missing'):
        register(db,'clock-01',FIX/'loan-clock-case.json')
