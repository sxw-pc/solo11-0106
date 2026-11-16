import sqlite3
from pathlib import Path
import pytest
from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan.store import connect

FIX = Path(__file__).resolve().parent.parent / 'fixtures'

def test_failed_evidence_insert_rolls_back_source_and_event(tmp_path):
    db_path = tmp_path / 'rollback.db'
    initialize(db_path, FIX / 'config.json')
    db = connect(db_path)
    db.execute("CREATE TRIGGER reject_evidence BEFORE INSERT ON evidence BEGIN SELECT RAISE(ABORT,'injected'); END")
    db.commit()
    db.close()
    with pytest.raises(sqlite3.IntegrityError):
        ingest(FIX / 'events.jsonl', db_path)
    db = connect(db_path)
    assert db.execute('SELECT count(*) FROM source').fetchone()[0] == 0
    assert db.execute('SELECT count(*) FROM event').fetchone()[0] == 0
    db.close()
