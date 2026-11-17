from pathlib import Path
import pytest
from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan.firmware_lab import register, invoke, state

FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def test_firmware_case_freezes_real_readers_and_endpoint_faults(tmp_path):
    db = tmp_path / 'firmware.db'
    initialize(db, FIX / 'firmware-config.json')
    assert ingest(FIX / 'firmware-events.jsonl', db)['imported'] == 3
    expected = {'case_id': 'firmware-01', 'readers': 3}
    assert register(db, 'firmware-01', FIX / 'firmware-case.json') == expected
    assert register(db, 'firmware-01', FIX / 'firmware-case.json') == expected
    assert invoke(db, 'R2', 'op-R2', 0)['status'] == 'timeout'
    assert state(db, 'R2')['version'] == 'v2'
    assert state(db, 'R2')['revision'] == 1
    assert invoke(db, 'R2', 'op-R2', 0)['revision'] == 1
    assert invoke(db, 'R3', 'op-R3', 0)['status'] == 'rejected'
    assert state(db, 'R3')['version'] == 'v1'
    assert invoke(db, 'R2', 'undo-R2', 1, 'rollback')['status'] == 'committed'
    assert state(db, 'R2')['version'] == 'v1'


def test_firmware_case_requires_observed_reader(tmp_path):
    db = tmp_path / 'firmware.db'
    initialize(db, FIX / 'firmware-config.json')
    with pytest.raises(ValueError, match='firmware_reader_unobserved'):
        register(db, 'firmware-01', FIX / 'firmware-case.json')
