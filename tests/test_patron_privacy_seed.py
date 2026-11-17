from pathlib import Path
import pytest
from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan.rules import evaluate
from shelfscan.patron_privacy import register
from shelfscan.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def test_patron_privacy_case_freezes_real_borrow_counts(tmp_path):
    db = tmp_path / 'privacy.db'
    initialize(db, FIX / 'patron-privacy-config.json')
    assert ingest(FIX / 'patron-privacy-events.jsonl', db)['imported'] == 15
    assert evaluate(db) == {'C1': '在架', 'C2': '在架', 'C3': '在架'}
    expected = {'case_id': 'privacy-01', 'patrons': 3, 'borrow_events': 6}
    assert register(db, 'privacy-01', FIX / 'patron-privacy-case.json') == expected
    assert register(db, 'privacy-01', FIX / 'patron-privacy-case.json') == expected
    conn = connect(db)
    try:
        assert conn.execute('SELECT COUNT(*) FROM patron_privacy_case').fetchone()[0] == 1
    finally:
        conn.close()


def test_patron_privacy_case_requires_source_events(tmp_path):
    db = tmp_path / 'privacy.db'
    initialize(db, FIX / 'patron-privacy-config.json')
    with pytest.raises(ValueError, match='patron_borrow_source_missing'):
        register(db, 'privacy-01', FIX / 'patron-privacy-case.json')
