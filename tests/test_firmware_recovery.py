"""Recovery tests for the four documented interruption points."""
from pathlib import Path

import pytest

from shelfscan.__main__ import firmware_seed
from shelfscan.firmware_release import plan, run, OrchestrationCrash
from shelfscan.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def seed(tmp_path):
    db = tmp_path / 'crash.db'
    firmware_seed(db, FIX / 'firmware-config.json', FIX / 'firmware-events.jsonl',
                  FIX / 'firmware-case.json')
    return db


def states(db):
    c = connect(db)
    out = dict(c.execute('SELECT device,state FROM firmware_intent_device').fetchall())
    c.close()
    return out


def live(db):
    c = connect(db)
    out = {r['id']: (r['version'], r['revision'])
           for r in c.execute('SELECT * FROM firmware_device ORDER BY id')}
    c.close()
    return out


def test_crash_after_intent_written_resumes_without_device_calls(tmp_path):
    db = seed(tmp_path)
    with pytest.raises(OrchestrationCrash, match='after_intent_written'):
        plan(db, 'i', 'firmware-01', 'atomic_fleet',
             crash_after={'after_intent_written': 1})
    c = connect(db)
    assert c.execute("SELECT status FROM firmware_intent WHERE id='i'").fetchone()['status'] == 'planned'
    assert c.execute('SELECT count(*) FROM firmware_operation').fetchone()[0] == 0
    c.close()
    plan(db, 'i', 'firmware-01', 'atomic_fleet')  # replay intent, no barrier
    summary = run(db, 'i')
    assert summary['status'] == 'rejected'
    assert live(db) == {'R1': ('v1', 2), 'R2': ('v1', 2), 'R3': ('v1', 0)}


def test_crash_after_install_commit_before_confirmation(tmp_path):
    db = seed(tmp_path)
    plan(db, 'i', 'firmware-01', 'atomic_fleet')
    with pytest.raises(OrchestrationCrash, match='after_install_commit:R2'):
        run(db, 'i', crash_after={'after_install_commit:R2': 1})
    # R2's device commit is durable; orchestrator never confirmed it
    assert live(db) == {'R1': ('v2', 1), 'R2': ('v2', 1), 'R3': ('v1', 0)}
    assert states(db)['R2'] == 'pending'
    summary = run(db, 'i')  # resume reconciles, no reinstall
    assert summary['status'] == 'rejected'
    assert live(db) == {'R1': ('v1', 2), 'R2': ('v1', 2), 'R3': ('v1', 0)}
    c = connect(db)
    r2_installs = c.execute(
        "SELECT count(*) FROM firmware_operation WHERE device='R2' AND action='install'").fetchone()[0]
    assert r2_installs == 1
    c.close()


def test_crash_during_cross_cell_release_finishes_remaining_cell(tmp_path):
    db = seed(tmp_path)
    plan(db, 'i', 'firmware-01', 'cell_isolated')
    with pytest.raises(OrchestrationCrash, match='during_cell_release:CELL-1'):
        run(db, 'i', crash_after={'during_cell_release:CELL-1': 1})
    c = connect(db)
    rel = dict(c.execute('SELECT cell,status FROM firmware_release').fetchall())
    assert rel == {'CELL-1': 'released_v2', 'CELL-2': 'pending'}
    c.close()
    summary = run(db, 'i')
    assert summary['status'] == 'rejected'
    c = connect(db)
    rel = dict(c.execute('SELECT cell,status FROM firmware_release').fetchall())
    assert rel == {'CELL-1': 'released_v2', 'CELL-2': 'released_v1'}
    avail = dict((r['device'], r['eligible'])
                 for r in c.execute('SELECT device,eligible FROM firmware_reader_availability'))
    assert avail == {'R1': 1, 'R2': 1, 'R3': 0}
    c.close()
    assert live(db) == {'R1': ('v2', 1), 'R2': ('v2', 1), 'R3': ('v1', 0)}


def test_crash_after_rollback_commit_before_confirmation(tmp_path):
    db = seed(tmp_path)
    plan(db, 'i', 'firmware-01', 'atomic_fleet')
    with pytest.raises(OrchestrationCrash, match='after_rollback_commit:R1'):
        run(db, 'i', crash_after={'after_rollback_commit:R1': 1})
    # R1 already physically back on v1; R2 still v2
    assert live(db) == {'R1': ('v1', 2), 'R2': ('v2', 1), 'R3': ('v1', 0)}
    summary = run(db, 'i')
    assert summary['status'] == 'rejected'
    assert live(db) == {'R1': ('v1', 2), 'R2': ('v1', 2), 'R3': ('v1', 0)}
    c = connect(db)
    # R1 rollback was not re-issued
    assert c.execute(
        "SELECT count(*) FROM firmware_operation WHERE device='R1' AND action='rollback'").fetchone()[0] == 1
    c.close()
