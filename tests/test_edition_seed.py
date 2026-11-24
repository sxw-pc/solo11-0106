from pathlib import Path
import pytest
from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan.rules import evaluate
from shelfscan.edition import register
from shelfscan.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'

def test_freezes_present_copy_and_spectral_intake(tmp_path):
    db = tmp_path / 'edition.db'
    initialize(db, FIX / 'config.json')
    assert ingest(FIX / 'events.jsonl', db)['imported'] == 2
    assert evaluate(db)['C1'] == '在架'
    got = register(db, 'ED-1', FIX / 'edition-case.json')
    assert got['observations'] == 4
    assert register(db, 'ED-1', FIX / 'edition-case.json') == got
    con = connect(db)
    try:
        assert con.execute('SELECT COUNT(*) FROM edition_case').fetchone()[0] == 1
    finally:
        con.close()

def test_rejects_copy_without_location_decision(tmp_path):
    with pytest.raises(ValueError, match='edition_copy_not_present'):
        register(tmp_path / 'none.db', 'ED-1', FIX / 'edition-case.json')
