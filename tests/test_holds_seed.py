from pathlib import Path
import pytest
from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan.rules import evaluate
from shelfscan.holds import register
from shelfscan.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def test_hold_case_freezes_available_copy_evidence(tmp_path):
    db = tmp_path / 'holds.db'
    initialize(db, FIX / 'holds-config.json')
    assert ingest(FIX / 'holds-events.jsonl', db)['imported'] == 3
    assert evaluate(db) == {'C1': '在架', 'C2': '在架', 'C3': '在架'}
    expected = {'case_id': 'holds-01', 'copies': 3, 'holds': 4}
    assert register(db, 'holds-01', FIX / 'holds-case.json') == expected
    assert register(db, 'holds-01', FIX / 'holds-case.json') == expected
    conn = connect(db)
    try:
        assert conn.execute('SELECT COUNT(*) FROM hold_case').fetchone()[0] == 1
    finally:
        conn.close()


def test_hold_case_requires_imported_inventory(tmp_path):
    db = tmp_path / 'holds.db'
    initialize(db, FIX / 'holds-config.json')
    with pytest.raises(ValueError, match='hold_copy_unobserved'):
        register(db, 'holds-01', FIX / 'holds-case.json')
