"""External revision conflicts, operation ID rules and evidence preservation."""
from pathlib import Path

import pytest

from shelfscan.__main__ import firmware_seed
from shelfscan.firmware_release import plan, run
from shelfscan.firmware_lab import invoke
from shelfscan.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def seed(tmp_path):
    db = tmp_path / 'conflict.db'
    firmware_seed(db, FIX / 'firmware-config.json', FIX / 'firmware-events.jsonl',
                  FIX / 'firmware-case.json')
    return db


def test_external_revision_change_blocks_without_forced_write(tmp_path):
    db = seed(tmp_path)
    plan(db, 'i', 'firmware-01', 'atomic_fleet')
    c = connect(db)
    c.execute("UPDATE firmware_device SET revision=5, version='v1' WHERE id='R1'")
    c.commit()
    c.close()
    summary = run(db, 'i')
    assert summary['status'] == 'awaiting_manual'
    c = connect(db)
    r1 = c.execute("SELECT * FROM firmware_device WHERE id='R1'").fetchone()
    assert (r1['version'], r1['revision']) == ('v1', 5)
    held = c.execute("SELECT state,detail FROM firmware_intent_device WHERE device='R1'").fetchone()
    assert held['state'] == 'manual_hold'
    reason = c.execute(
        "SELECT detail FROM firmware_journal WHERE event='manual_hold' AND device='R1'").fetchone()
    import json
    assert json.loads(reason['detail'])['reason'] == 'external_revision_conflict'
    # no operation was ever issued against the stale frozen revision
    assert c.execute(
        "SELECT count(*) FROM firmware_operation WHERE device='R1'").fetchone()[0] == 0
    c.close()
    # repeated commands keep waiting; they must not mutate the device
    assert run(db, 'i')['status'] == 'awaiting_manual'
    c = connect(db)
    r1 = c.execute("SELECT * FROM firmware_device WHERE id='R1'").fetchone()
    assert (r1['version'], r1['revision']) == ('v1', 5)
    c.close()


def test_external_revision_change_during_rollback_blocks(tmp_path):
    db = seed(tmp_path)
    plan(db, 'i', 'firmware-01', 'atomic_fleet')
    # drive installs only: crash barrier after R2's durable install leaves R1/R2
    # on v2 and R3 untouched; an external actor then bumps R1's revision.
    from shelfscan.firmware_release import OrchestrationCrash
    with pytest.raises(OrchestrationCrash):
        run(db, 'i', crash_after={'after_install_commit:R2': 1})
    c = connect(db)
    c.execute("UPDATE firmware_device SET revision=7 WHERE id='R1'")
    c.commit()
    c.close()
    summary = run(db, 'i')  # resumes; rollback of R1 now sees an alien revision
    assert summary['status'] == 'awaiting_manual'
    c = connect(db)
    held = c.execute("SELECT state FROM firmware_intent_device WHERE device='R1'").fetchone()
    assert held['state'] == 'manual_hold'
    r1 = c.execute("SELECT * FROM firmware_device WHERE id='R1'").fetchone()
    assert (r1['version'], r1['revision']) == ('v2', 7)  # never force-written back
    c.close()


def test_operation_id_cannot_be_reused_on_other_device_or_action(tmp_path):
    db = seed(tmp_path)
    assert invoke(db, 'R1', 'op-x', 0, 'install')['status'] == 'committed'
    with pytest.raises(ValueError, match='firmware_operation_conflict'):
        invoke(db, 'R2', 'op-x', 0, 'install')
    with pytest.raises(ValueError, match='firmware_operation_conflict'):
        invoke(db, 'R1', 'op-x', 1, 'rollback')
    # same id, same device+action replays the stored result idempotently
    again = invoke(db, 'R1', 'op-x', 0, 'install')
    assert again == {'status': 'committed', 'revision': 1}
    # plan refuses generated IDs that already exist
    invoke(db, 'R2', 'taken:R2:install', 0, 'install')
    with pytest.raises(ValueError, match='firmware_operation_conflict'):
        plan(db, 'taken', 'firmware-01', 'atomic_fleet')


def test_unknown_contract_rejected(tmp_path):
    db = seed(tmp_path)
    with pytest.raises(ValueError, match='firmware_contract_unknown'):
        plan(db, 'bad', 'firmware-01', 'blue_green')


def test_existing_rfid_evidence_and_inventory_state_untouched(tmp_path):
    db = seed(tmp_path)
    c = connect(db)
    before = {
        'events': [dict(r) for r in c.execute('SELECT * FROM event ORDER BY id')],
        'sources': [dict(r) for r in c.execute('SELECT id,name,sha256,raw FROM source ORDER BY id')],
        'evidence': [dict(r) for r in c.execute('SELECT * FROM evidence ORDER BY id')],
        'readers': [dict(r) for r in c.execute('SELECT * FROM reader ORDER BY id')],
    }
    c.close()
    plan(db, 'i', 'firmware-01', 'cell_isolated')
    run(db, 'i')
    c = connect(db)
    after = {
        'events': [dict(r) for r in c.execute('SELECT * FROM event ORDER BY id')],
        'sources': [dict(r) for r in c.execute('SELECT id,name,sha256,raw FROM source ORDER BY id')],
        'evidence': [dict(r) for r in c.execute('SELECT * FROM evidence ORDER BY id')],
        'readers': [dict(r) for r in c.execute('SELECT * FROM reader ORDER BY id')],
    }
    assert after == before
    # no inventory recompute tables were created/modified by the orchestrator
    assert c.execute('SELECT count(*) FROM decision').fetchone()[0] == 0
    assert c.execute('SELECT count(*) FROM run').fetchone()[0] == 0
    # frozen source sha still matches the stored source bytes
    frozen = c.execute(
        'SELECT DISTINCT latest_source_sha256 FROM firmware_intent_device').fetchall()
    source_shas = {r['sha256'] for r in c.execute('SELECT sha256 FROM source')}
    assert {row['latest_source_sha256'] for row in frozen} <= source_shas
    c.close()
