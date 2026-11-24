"""Freeze a physically present library copy and a local spectral intake."""
import hashlib
import json
from pathlib import Path
from .store import connect


def register(db_path, case_id, fixture):
    raw = Path(fixture).read_bytes()
    doc = json.loads(raw)
    if not case_id or not doc.get('copy_id') or len(doc.get('observations', [])) < 3 or len(doc.get('candidate_materials', [])) < 2:
        raise ValueError('bad_edition_case')
    db = connect(db_path)
    try:
        with db:
            decision = db.execute('SELECT status FROM decision WHERE copy_id=?', (doc['copy_id'],)).fetchone()
            if not decision or decision['status'] != '在架':
                raise ValueError('edition_copy_not_present')
            scans = [dict(e) for e in db.execute('''SELECT e.id,e.at,e.sha256,s.sha256 AS source_sha256
                 FROM event e JOIN source s ON s.id=e.source_id
                 WHERE e.copy_id=? AND e.kind='scan' ORDER BY e.at,e.id''', (doc['copy_id'],))]
            if not scans:
                raise ValueError('edition_scan_missing')
            digest = hashlib.sha256(raw).hexdigest()
            old = db.execute('SELECT digest FROM edition_case WHERE id=?', (case_id,)).fetchone()
            if old and old['digest'] != digest:
                raise ValueError('edition_case_conflict')
            db.execute('INSERT OR IGNORE INTO edition_case VALUES (?,?,?)',
                       (case_id, json.dumps({'doc': doc, 'rfid_evidence': scans}, sort_keys=True), digest))
    finally:
        db.close()
    return {'case_id': case_id, 'digest': digest, 'observations': len(doc['observations'])}
