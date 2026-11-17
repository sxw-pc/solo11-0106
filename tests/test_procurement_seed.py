from pathlib import Path
import pytest
from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan.rules import evaluate
from shelfscan.procurement import register
from shelfscan.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def test_replacement_case_freezes_missing_decisions_and_quotes(tmp_path):
    db = tmp_path / 'procurement.db'
    initialize(db, FIX / 'replacement-config.json')
    assert ingest(FIX / 'replacement-events.jsonl', db)['imported'] == 1
    assert evaluate(db) == {'C1': '缺失', 'C2': '缺失', 'C3': '在架'}
    expected = {'case_id': 'replacement-01', 'requests': 2, 'quotes': 3}
    assert register(db, 'replacement-01', FIX / 'replacement-case.json') == expected
    assert register(db, 'replacement-01', FIX / 'replacement-case.json') == expected
    conn = connect(db)
    try:
        assert conn.execute('SELECT COUNT(*) FROM replacement_case').fetchone()[0] == 1
    finally:
        conn.close()


def test_replacement_case_requires_missing_decision(tmp_path):
    db = tmp_path / 'procurement.db'
    initialize(db, FIX / 'replacement-config.json')
    with pytest.raises(ValueError, match='replacement_copy_not_missing'):
        register(db, 'replacement-01', FIX / 'replacement-case.json')
