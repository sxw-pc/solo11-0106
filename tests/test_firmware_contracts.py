"""End-to-end firmware upgrade contract tests on the frozen fault fixture."""
import json
from pathlib import Path

import pytest

from shelfscan.__main__ import firmware_seed
from shelfscan.firmware_release import plan, run
from shelfscan.firmware_lab import state
from shelfscan.firmware_report import build_report, write_report
from shelfscan.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def seed(tmp_path, name='fw.db'):
    db = tmp_path / name
    firmware_seed(db, FIX / 'firmware-config.json', FIX / 'firmware-events.jsonl',
                  FIX / 'firmware-case.json')
    return db


def live_map(db):
    c = connect(db)
    rows = {r['id']: (r['version'], r['revision'])
            for r in c.execute('SELECT * FROM firmware_device ORDER BY id')}
    c.close()
    return rows


def test_atomic_fleet_rolls_back_every_reader_after_rejection(tmp_path):
    db = seed(tmp_path)
    info = plan(db, 'intent-atomic', 'firmware-01', 'atomic_fleet')
    assert info['readers'] == 3 and len(info['package_sha256']) == 64
    summary = run(db, 'intent-atomic')
    assert summary['status'] == 'rejected'
    assert live_map(db) == {'R1': ('v1', 2), 'R2': ('v1', 2), 'R3': ('v1', 0)}
    c = connect(db)
    states = dict(c.execute('SELECT device,state FROM firmware_intent_device').fetchall())
    assert states == {'R1': 'rolled_back', 'R2': 'rolled_back', 'R3': 'rejected'}
    cells = dict(c.execute('SELECT cell,status FROM firmware_release').fetchall())
    assert cells == {'CELL-1': 'released_v1', 'CELL-2': 'released_v1'}
    avail = c.execute('SELECT eligible FROM firmware_reader_availability').fetchall()
    assert all(row['eligible'] == 0 for row in avail)
    c.close()


def test_cell_isolated_keeps_healthy_cell_on_v2(tmp_path):
    db = seed(tmp_path)
    plan(db, 'intent-cell', 'firmware-01', 'cell_isolated')
    summary = run(db, 'intent-cell')
    assert summary['status'] == 'rejected'
    assert live_map(db) == {'R1': ('v2', 1), 'R2': ('v2', 1), 'R3': ('v1', 0)}
    c = connect(db)
    cells = dict(c.execute('SELECT cell,status FROM firmware_release').fetchall())
    assert cells == {'CELL-1': 'released_v2', 'CELL-2': 'released_v1'}
    avail = dict((r['device'], (r['eligible'], r['reason']))
                 for r in c.execute('SELECT device,eligible,reason FROM firmware_reader_availability'))
    assert avail['R1'] == (1, 'released_on_target')
    assert avail['R2'] == (1, 'released_on_target')
    assert avail['R3'] == (0, 'released_on_original')
    c.close()


def test_lost_ack_is_reconciled_not_marked_failed_or_reinstalled(tmp_path):
    db = seed(tmp_path)
    plan(db, 'intent-ack', 'firmware-01', 'atomic_fleet')
    run(db, 'intent-ack')
    c = connect(db)
    r2_state = c.execute("SELECT state,detail FROM firmware_intent_device WHERE device='R2'").fetchone()
    assert r2_state['state'] == 'rolled_back'  # confirmed v2 first, then fleet rollback
    detail = json.loads(r2_state['detail'])
    install = c.execute("SELECT * FROM firmware_operation WHERE id='intent-ack:R2:install'").fetchone()
    assert install['result'] == 'committed_ack_lost'
    assert install['revision_after'] == 1
    rollback = c.execute("SELECT * FROM firmware_operation WHERE id='intent-ack:R2:rollback'").fetchone()
    assert rollback['result'] == 'committed'
    # exactly one install and one rollback per device: no blind reinstall
    counts = dict(c.execute(
        "SELECT device,count(*) FROM firmware_operation GROUP BY device").fetchall())
    assert counts == {'R1': 2, 'R2': 2, 'R3': 1}
    journal = [r['event'] for r in c.execute('SELECT event FROM firmware_journal ORDER BY id')]
    assert 'install_confirmed_after_timeout' in journal
    c.close()
    assert state(db, 'R2')['version'] == 'v1'


def test_repeated_run_does_not_repeat_installs_or_rollbacks(tmp_path):
    db = seed(tmp_path)
    plan(db, 'intent-repeat', 'firmware-01', 'atomic_fleet')
    run(db, 'intent-repeat')
    c = connect(db)
    before = [dict(r) for r in c.execute('SELECT * FROM firmware_operation ORDER BY id')]
    journal_before = c.execute('SELECT count(*) FROM firmware_journal').fetchone()[0]
    c.close()
    again = run(db, 'intent-repeat')
    assert again['status'] == 'rejected'
    c = connect(db)
    after = [dict(r) for r in c.execute('SELECT * FROM firmware_operation ORDER BY id')]
    assert after == before
    assert c.execute('SELECT count(*) FROM firmware_journal').fetchone()[0] == journal_before
    c.close()
    assert live_map(db) == {'R1': ('v1', 2), 'R2': ('v1', 2), 'R3': ('v1', 0)}


def test_report_is_verifiable_and_links_scan_evidence(tmp_path):
    db = seed(tmp_path)
    plan(db, 'intent-report', 'firmware-01', 'cell_isolated')
    run(db, 'intent-report')
    out = write_report(db, 'intent-report', tmp_path / 'up.json')
    doc = json.loads(out.read_text(encoding='utf-8'))
    stored = doc.pop('report_sha256')
    canonical = json.dumps(doc, sort_keys=True, separators=(',', ':'),
                           ensure_ascii=False).encode('utf-8')
    import hashlib
    assert hashlib.sha256(canonical).hexdigest() == stored
    sidecar = out.with_name('up.sha256').read_text(encoding='utf-8').split()[0]
    assert sidecar == stored
    assert out.with_suffix('.md').exists()
    links = {r['device']: r for r in doc['scan_evidence_links']}
    c = connect(db)
    for rid in ('R1', 'R2', 'R3'):
        ev = c.execute(
            'SELECT e.id,e.sha256,s.sha256 AS ssha FROM event e JOIN source s ON s.id=e.source_id '
            'WHERE e.device=? ORDER BY e.seq DESC LIMIT 1', (rid,)).fetchone()
        assert links[rid]['latest_event_id'] == ev['id']
        assert links[rid]['latest_event_sha256'] == ev['sha256']
        assert links[rid]['latest_source_sha256'] == ev['ssha']
    c.close()
    assert doc['intent']['package_sha256'] == build_report(db, 'intent-report')['intent']['package_sha256']


def test_intended_readers_and_evidence_are_frozen_before_install(tmp_path):
    db = seed(tmp_path)
    info = plan(db, 'intent-freeze', 'firmware-01', 'atomic_fleet')
    c = connect(db)
    rows = c.execute('SELECT * FROM firmware_intent_device ORDER BY device').fetchall()
    assert [r['device'] for r in rows] == ['R1', 'R2', 'R3']
    assert all(r['frozen_revision'] == 0 and r['current_version'] == 'v1'
               and r['target_version'] == 'v2' for r in rows)
    assert all(r['install_operation_id'].endswith(':install')
               and r['rollback_operation_id'].endswith(':rollback') for r in rows)
    assert c.execute('SELECT count(*) FROM firmware_operation').fetchone()[0] == 0
    assert c.execute("SELECT event FROM firmware_journal WHERE event='intent_planned'").fetchone()
    # replay with the same id is stable; a changed contract must conflict
    again = plan(db, 'intent-freeze', 'firmware-01', 'atomic_fleet')
    assert again['replayed'] is True and again['intent_sha256'] == info['intent_sha256']
    with pytest.raises(ValueError, match='firmware_intent_conflict'):
        plan(db, 'intent-freeze', 'firmware-01', 'cell_isolated')
    c.close()
