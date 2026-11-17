"""Freeze imported RFID source BLOB identities before online migration."""
import hashlib
import json
from pathlib import Path
from .store import connect


def register(db_path, case_id, fixture):
    source = Path(fixture).resolve()
    raw = source.read_bytes()
    doc = json.loads(raw)
    if not case_id or not doc.get('sources') or not doc.get('representations'):
        raise ValueError('bad_archive_case')
    db = connect(db_path)
    try:
        with db:
            records = []
            for name in doc['sources']:
                file = (source.parent / name).resolve()
                row = db.execute('SELECT * FROM source WHERE name=?', (str(file),)).fetchone()
                if row is None:
                    raise ValueError('archive_source_missing')
                original = bytes(row['raw'])
                if hashlib.sha256(original).hexdigest() != row['sha256']:
                    raise ValueError('archive_source_hash_mismatch')
                records.append({'id': row['id'], 'name': row['name'], 'sha256': row['sha256'],
                                'length': len(original)})
            payload = json.dumps({'fixture': str(source), 'digest': hashlib.sha256(raw).hexdigest(),
                                  'doc': doc, 'sources': records}, sort_keys=True, separators=(',', ':'))
            old = db.execute('SELECT payload FROM archive_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('archive_case_conflict')
            db.execute('INSERT OR IGNORE INTO archive_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'sources': len(records)}
