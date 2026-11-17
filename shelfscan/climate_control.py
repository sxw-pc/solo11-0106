"""Frozen shelf climate inputs, linked to observed RFID inventory; no planner."""
import csv
import hashlib
import io
import json
from pathlib import Path
from .store import connect


def register(db_path, case_id, fixture, sensors):
    case_path = Path(fixture).resolve()
    sensor_path = Path(sensors).resolve()
    case_raw = case_path.read_bytes()
    sensor_raw = sensor_path.read_bytes()
    case = json.loads(case_raw)
    readings = list(csv.DictReader(io.StringIO(sensor_raw.decode('utf-8-sig'))))
    if not case_id or not case.get('shelves') or not case.get('contracts'):
        raise ValueError('climate_case_invalid')
    if len(readings) != len(case['shelves']):
        raise ValueError('climate_sensor_count')
    indexed = {row['shelf']: row for row in readings}
    if len(indexed) != len(readings):
        raise ValueError('climate_sensor_duplicate')
    db = connect(db_path)
    try:
        observed = []
        for shelf in case['shelves']:
            sid = shelf['id']
            reading = indexed.get(sid)
            if reading is None or float(reading['temperature_c']) != shelf['initial_c']:
                raise ValueError('climate_sensor_mismatch')
            rows = [dict(row) for row in db.execute(
                'SELECT c.id AS copy_id,r.id AS reader_id,e.id AS event_id,e.at,s.sha256 AS source_sha256 '
                'FROM copy c JOIN event e ON e.copy_id=c.id AND e.kind=? '
                'JOIN reader r ON r.id=e.device AND r.shelf=c.planned_shelf '
                'JOIN source s ON s.id=e.source_id '
                'WHERE c.planned_shelf=? ORDER BY e.id', ('scan', sid))]
            if not rows:
                raise ValueError('climate_shelf_unobserved')
            observed.append({'shelf': sid, 'inventory': rows, 'sensor': reading})
        payload = json.dumps({'case': case, 'case_path': str(case_path),
                              'case_sha256': hashlib.sha256(case_raw).hexdigest(),
                              'sensor_path': str(sensor_path),
                              'sensor_sha256': hashlib.sha256(sensor_raw).hexdigest(),
                              'observed': observed}, sort_keys=True, separators=(',', ':'))
        with db:
            old = db.execute('SELECT payload FROM climate_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('climate_case_conflict')
            db.execute('INSERT OR IGNORE INTO climate_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'shelves': len(observed)}
