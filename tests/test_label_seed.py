from pathlib import Path
import pytest
from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan.labels import register
from shelfscan.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def test_label_case_freezes_real_copies_without_scoring(tmp_path):
    db = tmp_path / 'labels.db'
    initialize(db, FIX / 'config.json')
    assert ingest(FIX / 'events.jsonl', db)['imported'] == 2
    expected = {'case_id': 'label-01', 'copies': 2, 'serials': 3}
    assert register(db, 'label-01', FIX / 'label-case.json') == expected
    assert register(db, 'label-01', FIX / 'label-case.json') == expected
    conn = connect(db)
    try:
        assert conn.execute('SELECT COUNT(*) FROM label_case').fetchone()[0] == 1
    finally:
        conn.close()


def test_label_case_requires_catalogued_copy(tmp_path):
    db = tmp_path / 'labels.db'
    with pytest.raises(ValueError, match='unknown_label_copy'):
        register(db, 'label-01', FIX / 'label-case.json')
