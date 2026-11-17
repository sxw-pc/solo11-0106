import json
from pathlib import Path
import pytest
from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan.climate_control import register
from shelfscan.store import connect


FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def test_climate_fixture_is_linked_to_real_shelf_scans(tmp_path):
    db_path = tmp_path / 'climate.db'
    initialize(db_path, FIX / 'climate-config.json')
    assert ingest(FIX / 'climate-events.jsonl', db_path)['imported'] == 2
    assert register(db_path, 'climate-01', FIX / 'climate-case.json',
                    FIX / 'climate-sensors.csv') == {'case_id': 'climate-01', 'shelves': 2}
    db = connect(db_path)
    payload = json.loads(db.execute('SELECT payload FROM climate_case').fetchone()[0])
    db.close()
    assert [item['inventory'][0]['copy_id'] for item in payload['observed']] == ['C1', 'C2']
    assert len(payload['sensor_sha256']) == 64


def test_climate_fixture_requires_observed_shelves(tmp_path):
    db_path = tmp_path / 'climate.db'
    initialize(db_path, FIX / 'climate-config.json')
    with pytest.raises(ValueError, match='climate_shelf_unobserved'):
        register(db_path, 'climate-01', FIX / 'climate-case.json', FIX / 'climate-sensors.csv')
