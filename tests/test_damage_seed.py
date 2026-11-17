from pathlib import Path
import pytest
from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan.rules import evaluate
from shelfscan.claims import register
from shelfscan.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def test_damage_case_freezes_loans_and_inspections(tmp_path):
    db = tmp_path / 'damage.db'
    initialize(db, FIX / 'damage-config.json')
    assert ingest(FIX / 'damage-events.jsonl', db)['imported'] == 8
    assert evaluate(db) == {'C1': '在架', 'C2': '在架'}
    expected = {'case_id': 'damage-01', 'claims': 3}
    assert register(db, 'damage-01', FIX / 'damage-case.json') == expected
    assert register(db, 'damage-01', FIX / 'damage-case.json') == expected
    conn = connect(db)
    try:
        assert conn.execute('SELECT COUNT(*) FROM damage_case').fetchone()[0] == 1
    finally:
        conn.close()


def test_damage_case_requires_borrow_return_evidence(tmp_path):
    db = tmp_path / 'damage.db'
    initialize(db, FIX / 'damage-config.json')
    with pytest.raises(ValueError, match='damage_loan_events_missing'):
        register(db, 'damage-01', FIX / 'damage-case.json')

