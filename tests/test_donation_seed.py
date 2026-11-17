from pathlib import Path
import pytest
from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan.rules import evaluate
from shelfscan.donations import register
from shelfscan.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def test_donation_case_freezes_unknown_tag_and_deed(tmp_path):
    db = tmp_path / 'donation.db'
    initialize(db, FIX / 'donation-config.json')
    assert ingest(FIX / 'donation-events.jsonl', db)['imported'] == 2
    assert evaluate(db) == {'C1': '在架'}
    expected = {'case_id': 'donation-01', 'tag': 'CNEW', 'scans': 1}
    assert register(db, 'donation-01', FIX / 'donation-case.json') == expected
    assert register(db, 'donation-01', FIX / 'donation-case.json') == expected
    conn = connect(db)
    try:
        assert conn.execute('SELECT COUNT(*) FROM donation_case').fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM issue WHERE code='unexpected_copy'").fetchone()[0] == 1
    finally:
        conn.close()


def test_donation_case_requires_unknown_scan(tmp_path):
    db = tmp_path / 'donation.db'
    initialize(db, FIX / 'donation-config.json')
    with pytest.raises(ValueError, match='donation_scan_missing'):
        register(db, 'donation-01', FIX / 'donation-case.json')
