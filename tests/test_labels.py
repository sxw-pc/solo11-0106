import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from shelfscan import labels
from shelfscan.__main__ import initialize
from shelfscan.devices import SimPrinter, SimVerifier
from shelfscan.ingest import ingest
from shelfscan.report import report
from shelfscan.rules import evaluate
from shelfscan.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def setup(tmp_path, degrees=True):
    db = tmp_path / 'lib.db'
    initialize(db, FIX / 'config.json')
    ingest(FIX / 'events.jsonl', db)
    evaluate(db)
    labels.register(db, 'label-01', FIX / 'label-case.json')
    return db


def paths(tmp_path):
    return tmp_path / 'printer.db', tmp_path / 'verifier.db'


def paper_of(printer_path, serial):
    return SimPrinter(printer_path).emission(serial)['paper']


def start(tmp_path, contract='public_call_number', db=None):
    db = db or setup(tmp_path)
    printer_path, verifier_path = paths(tmp_path)
    job = labels.open_job(db, 'label-01', contract)
    return db, job['job_id'], printer_path, verifier_path, SimPrinter(printer_path), SimVerifier(verifier_path)


# ---------------------------------------------------------------------------
# deterministic paper and the two layouts
# ---------------------------------------------------------------------------

def test_public_layout_prints_shelf_copy_and_serial(tmp_path):
    db, job_id, printer_path, _, _, verifier = start(tmp_path)
    out = labels.run_job(db, job_id, printer_path, verifier=verifier, inject_fault=False)
    assert out['status'] == 'completed'
    text = paper_of(printer_path, 'TAG-0001').decode()
    assert 'CONTRACT public_call_number' in text
    assert 'COPY C1' in text and 'SERIAL TAG-0001' in text
    assert 'SHELF S1' in text and 'TEMPLATE v1' in text
    assert 'TOKEN' not in text
    c = connect(db)
    row = c.execute("SELECT * FROM label WHERE serial='TAG-0001'").fetchone()
    assert row['payload'] == paper_of(printer_path, 'TAG-0001')
    assert row['state'] == 'applied'
    c.close()


def test_opaque_layout_hides_shelf_and_points_to_same_copy(tmp_path):
    db, job_id, printer_path, _, _, verifier = start(tmp_path, 'opaque_tag')
    out = labels.run_job(db, job_id, printer_path, verifier=verifier, inject_fault=False)
    assert out['status'] == 'completed'
    text = paper_of(printer_path, 'TAG-0001').decode()
    assert 'TOKEN TK-C1-A' in text and 'COPY C1' in text
    assert '\nSHELF S1\n' not in text and 'SHELFSCAN-LABEL' in text
    c = connect(db)
    row = c.execute("SELECT * FROM label WHERE serial='TAG-0001'").fetchone()
    assert row['copy_id'] == 'C1' and row['state'] == 'applied'
    # The internal token mapping still resolves to the catalogued copy/shelf.
    copy = c.execute("SELECT planned_shelf FROM copy WHERE id=?", (row['copy_id'],)).fetchone()
    assert copy['planned_shelf'] == 'S1'
    c.close()


def test_two_contracts_render_different_bytes_for_the_same_copy():
    public = labels.render_paper('public_call_number', 'C1', 'TAG-0001', 'v1', 'v1',
                                 public_shelf='S1', opaque_token='TK-C1-A')
    opaque = labels.render_paper('opaque_tag', 'C1', 'TAG-0001', 'v1', 'v1',
                                 public_shelf='S1', opaque_token='TK-C1-A')
    assert public != opaque
    assert b'SHELF S1' in public and b'SHELF S1' not in opaque
    assert b'TOKEN TK-C1-A' in opaque and b'TOKEN' not in public
    # Both carry the copy ID and serial that bind the sheet to the same copy.
    assert b'COPY C1' in public and b'COPY C1' in opaque
    assert b'SERIAL TAG-0001' in public and b'SERIAL TAG-0001' in opaque


def test_render_is_deterministic_and_rejects_missing_inputs():
    args = ('public_call_number', 'C2', 'TAG-0002', 'v1', 'v1')
    assert labels.render_paper(*args, public_shelf='S2') == labels.render_paper(*args, public_shelf='S2')
    with pytest.raises(labels.LabelError, match='public_shelf_required'):
        labels.render_paper(*args)
    with pytest.raises(labels.LabelError, match='opaque_token_required'):
        labels.render_paper('opaque_tag', 'C2', 'TAG-0002', 'v1', 'v1')
    with pytest.raises(labels.LabelError, match='unknown_contract'):
        labels.render_paper('barcode', 'C2', 'TAG-0002', 'v1', 'v1')


def test_default_fixture_fault_interrupts_after_paper_before_ack(tmp_path):
    db, job_id, printer_path, _, printer, verifier = start(tmp_path)
    out = labels.run_job(db, job_id, printer_path, verifier=verifier)
    assert out['status'] == 'interrupted' and out['serial'] == 'TAG-0001'
    # The sheet physically exists even though no ACK was returned.
    assert printer.emission('TAG-0001') is not None
    cmd = printer.command_for_serial('TAG-0001')
    assert cmd is not None and cmd['state'] == 'emitted'
    c = connect(db)
    assert c.execute("SELECT state FROM label_command WHERE serial='TAG-0001'").fetchone()[0] == 'sent'
    c.close()


def test_other_contract_is_render_only_control(tmp_path):
    db, job_id, printer_path, _, _, _ = start(tmp_path)
    c = connect(db)
    controls = [dict(r) for r in c.execute('SELECT * FROM label_control')]
    c.close()
    assert {r['contract'] for r in controls} == {'opaque_tag'}
    assert all(r['executed'] == 0 for r in controls)
    assert any(b'TOKEN TK-C1-A' in r['payload'] for r in controls)
    with pytest.raises(labels.LabelError, match='job_already_active'):
        labels.open_job(db, 'label-01', 'public_call_number')


# ---------------------------------------------------------------------------
# binding to ingest + catalogued snapshot
# ---------------------------------------------------------------------------

def test_open_job_rejects_wrong_or_second_contract(tmp_path):
    db = setup(tmp_path)
    job = labels.open_job(db, 'label-01', 'public_call_number')
    labels.reserve_copy(db, job['job_id'], 'C1')
    # While a job is still open a second layout cannot start.
    with pytest.raises(labels.LabelError, match='job_already_active'):
        labels.open_job(db, 'label-01', 'opaque_tag')
    # Finish it; now the case is locked to the contract that produced applied
    # labels and the alternate layout permanently stays a render-only control.
    c = sqlite3.connect(db)
    c.execute("DELETE FROM label WHERE serial='TAG-0001'")
    c.execute("UPDATE label_job SET status='completed' WHERE id=?", (job['job_id'],))
    c.commit()
    c.close()
    job2 = labels.open_job(db, 'label-01', 'opaque_tag')
    labels.run_job(db, job2['job_id'], tmp_path / 'printer.db',
                   verifier=SimVerifier(tmp_path / 'verifier.db'), inject_fault=False)
    with pytest.raises(labels.LabelError, match='case_contract_already_executed'):
        labels.open_job(db, 'label-01', 'public_call_number')
    with pytest.raises(labels.LabelError, match='unknown_contract'):
        labels.open_job(db, 'label-01', 'barcode')


def test_contract_not_authorised_by_case(tmp_path):
    db = setup(tmp_path)
    c = sqlite3.connect(db)
    c.execute("UPDATE label_case SET payload=replace(payload, 'opaque_tag', 'other_layout')")
    c.commit()
    c.close()
    with pytest.raises(labels.LabelError, match='contract_not_authorised'):
        labels.open_job(db, 'label-01', 'opaque_tag')


def test_subject_digest_uses_ingest_evidence_and_rules(tmp_path):
    db = setup(tmp_path)
    snap = labels.snapshot(db, 'label-01')
    assert [e['id'] for e in snap['C1']['ingest_events']] == [1, 2]
    assert snap['C1']['decision']['status'] == '在架'
    assert snap['C2']['decision']['status'] == '借出'
    assert snap['C1']['subject_sha256'] != snap['C2']['subject_sha256']


def test_new_ingest_evidence_changes_subject_and_invalidates_paper(tmp_path):
    db, job_id, printer_path, verifier_path, _, verifier = start(tmp_path)
    old_subject = labels.snapshot(db, 'label-01')['C1']['subject_sha256']
    labels.run_job(db, job_id, printer_path, verifier=verifier, inject_fault=False)
    extra = tmp_path / 'extra.jsonl'
    extra.write_text(json.dumps({'kind': 'scan', 'device': 'R1', 'seq': 3, 'copy_id': 'C1',
                                 'at': '2026-01-01T08:02:00+00:00', 'shelf': 'S1',
                                 'signal': 78}), encoding='utf-8')
    assert ingest(extra, db)['imported'] == 1
    evaluate(db)
    assert labels.snapshot(db, 'label-01')['C1']['subject_sha256'] != old_subject
    res = labels.verify_label(db, 'TAG-0001', 'C1', 'TAG-0001',
                              paper_of(printer_path, 'TAG-0001'), verifier=verifier)
    assert res['result'] == 'snapshot_stale' and not res['applied']


def test_new_template_is_rendered_on_fresh_job(tmp_path):
    db = setup(tmp_path)
    labels.set_template(db, 'label-01', 'v2')
    job = labels.open_job(db, 'label-01', 'public_call_number')
    labels.run_job(db, job['job_id'], tmp_path / 'printer.db',
                   verifier=SimVerifier(tmp_path / 'verifier.db'), inject_fault=False)
    text = paper_of(tmp_path / 'printer.db', 'TAG-0001').decode()
    assert 'TEMPLATE v2' in text


def test_shelf_change_makes_old_paper_stale_and_reissue_prints_new_bytes(tmp_path):
    db, job_id, printer_path, _, _, verifier = start(tmp_path)
    labels.run_job(db, job_id, printer_path, verifier=verifier, inject_fault=False)
    old_paper = paper_of(printer_path, 'TAG-0001')
    c = sqlite3.connect(db)
    c.execute("UPDATE copy SET planned_shelf='S9' WHERE id='C1'")
    c.commit()
    c.close()
    evaluate(db)
    # Old sheet is physically unchanged; verification must flag the snapshot.
    result = labels.verify_label(db, 'TAG-0001', 'C1', 'TAG-0001', old_paper,
                                 verifier=SimVerifier(tmp_path / 'verifier.db'))
    assert result['result'] == 'snapshot_stale' and not result['applied']
    out = labels.reissue_copy(db, 'label-01', 'C1', printer_path, verifier=verifier)
    assert out['serial'] == 'TAG-0003'
    assert b'SHELF S9' in paper_of(printer_path, 'TAG-0003')
    assert old_paper != paper_of(printer_path, 'TAG-0003')
    c = connect(db)
    old = c.execute("SELECT state,void_reason FROM label WHERE serial='TAG-0001'").fetchone()
    new = c.execute("SELECT state,replaces_serial FROM label WHERE serial='TAG-0003'").fetchone()
    assert old['state'] == 'superseded' and old['void_reason'] == 'source_snapshot_changed'
    assert new['state'] == 'applied' and new['replaces_serial'] == 'TAG-0001'
    c.close()


def test_template_change_changes_render_and_old_paper_cannot_pass(tmp_path):
    db, job_id, printer_path, _, _, verifier = start(tmp_path)
    labels.run_job(db, job_id, printer_path, verifier=verifier, inject_fault=False)
    old_paper = paper_of(printer_path, 'TAG-0001')
    labels.set_template(db, 'label-01', 'v2')
    assert b'TEMPLATE v2' in labels.render_paper(
        'public_call_number', 'C1', 'TAG-0003', 'v2', 'v1', public_shelf='S1')
    res = labels.verify_label(db, 'TAG-0001', 'C1', 'TAG-0001', old_paper,
                              verifier=SimVerifier(tmp_path / 'verifier.db'))
    assert res['result'] == 'snapshot_stale'


# ---------------------------------------------------------------------------
# concurrency and duplicate commands
# ---------------------------------------------------------------------------

def test_two_librarians_concurrently_reserve_distinct_serials(tmp_path):
    db = setup(tmp_path)
    job = labels.open_job(db, 'label-01', 'public_call_number')
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(labels.reserve_copy, db, job['job_id'], cid, 'librarian-' + cid)
                   for cid in ('C1', 'C2')]
        rows = [f.result() for f in futures]
    assert {r['copy_id'] for r in rows} == {'C1', 'C2'}
    assert len({r['serial'] for r in rows}) == 2
    c = connect(db)
    assert c.execute("SELECT count(*) FROM label WHERE state IN ('reserved','sent','emitted','verified','applied')").fetchone()[0] == 2
    assert c.execute('SELECT count(*) FROM label WHERE copy_id=?', ('C1',)).fetchone()[0] == 1
    c.close()


def test_same_copy_cannot_get_two_live_labels(tmp_path):
    db, job_id, printer_path, _, _, verifier = start(tmp_path)
    labels.reserve_copy(db, job_id, 'C1')
    with pytest.raises(labels.LabelError, match='copy_already_labelled'):
        labels.reserve_copy(db, job_id, 'C1')


def test_concurrent_reservation_of_same_copy_has_one_winner(tmp_path):
    db = setup(tmp_path)
    job = labels.open_job(db, 'label-01', 'public_call_number')
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(labels.reserve_copy, db, job['job_id'], 'C1', 'librarian-%d' % i)
                   for i in range(2)]
    outcomes = []
    for future in futures:
        try:
            outcomes.append(('ok', future.result()['serial']))
        except labels.LabelError as exc:
            outcomes.append(('err', str(exc)))
    assert sorted(kind for kind, _ in outcomes) == ['err', 'ok']
    assert [v for k, v in outcomes if k == 'err'] == ['copy_already_labelled']
    c = connect(db)
    assert c.execute("SELECT count(*) FROM label WHERE copy_id='C1'").fetchone()[0] == 1
    c.close()


def test_serial_can_only_have_one_print_command(tmp_path):
    db, job_id, printer_path, _, _, verifier = start(tmp_path)
    label = labels.reserve_copy(db, job_id, 'C1')
    c = connect(db)
    job = dict(c.execute('SELECT * FROM label_job WHERE id=?', (job_id,)).fetchone())
    lab = c.execute('SELECT * FROM label WHERE serial=?', (label['serial'],)).fetchone()
    first = labels._create_command(c, job, lab, 'librarian')
    second = labels._create_command(c, job, lab, 'librarian')
    c.commit()
    assert first['nonce'] == second['nonce']
    with pytest.raises(sqlite3.IntegrityError):
        c.execute("INSERT INTO label_command(job_id,serial,nonce,request,request_sha256,paper_sha256,state,created_at) "
                  "VALUES (?,?,?,?,?,?,?,?)",
                  (job_id, label['serial'], 'another-nonce', b'{}', 'r', label['payload_sha256'],
                   'sent', '2026-01-01T00:00:00+00:00'))
    c.close()


def test_duplicate_print_command_never_emits_second_sheet(tmp_path):
    db, job_id, printer_path, _, printer, verifier = start(tmp_path)
    out = labels.run_job(db, job_id, printer_path, verifier=verifier)
    assert out['status'] == 'interrupted'
    c = connect(db)
    cmd = dict(c.execute("SELECT * FROM label_command WHERE serial='TAG-0001'").fetchone())
    c.close()
    again = labels.send_command(db, cmd['id'], printer)
    assert again['status'] == 'emitted' and again['device'].get('duplicate_command')
    assert len([e for e in printer.emissions() if e['serial'] == 'TAG-0001']) == 1


def test_late_ack_is_only_a_command_reply(tmp_path):
    db, job_id, printer_path, _, printer, verifier = start(tmp_path)
    out = labels.run_job(db, job_id, printer_path, verifier=verifier)
    assert out['status'] == 'interrupted'
    # Physically the correct sheet is on C1 before the ACK arrives.
    paper = paper_of(printer_path, 'TAG-0001')
    res = labels.verify_label(db, 'TAG-0001', 'C1', 'TAG-0001', paper,
                              verifier=SimVerifier(tmp_path / 'verifier.db'))
    assert res['result'] == 'ok'
    printer.deliver_ack(printer.command_for_serial('TAG-0001')['nonce'])
    labels.sync_device(db, printer)
    c = connect(db)
    assert c.execute("SELECT state FROM label_command WHERE serial='TAG-0001'").fetchone()[0] == 'acked'
    c.close()


def test_ack_alone_does_not_apply_without_verifier(tmp_path):
    db, job_id, printer_path, _, printer, _ = start(tmp_path)
    out = labels.run_job(db, job_id, printer_path, inject_fault=False)
    assert out['status'] == 'active'  # not completed: nothing was physically verified
    c = connect(db)
    states = {r['serial']: r['state'] for r in c.execute('SELECT serial,state FROM label')}
    c.close()
    assert states == {'TAG-0001': 'emitted', 'TAG-0002': 'emitted'}
    assert all(cmd['state'] == 'acked' for cmd in printer.commands())


# ---------------------------------------------------------------------------
# physical verification failures
# ---------------------------------------------------------------------------

def _emitted(tmp_path):
    db, job_id, printer_path, verifier_path, printer, verifier = start(tmp_path)
    labels.run_job(db, job_id, printer_path, verifier=verifier)
    return db, printer_path, verifier_path, SimVerifier(verifier_path)


def test_wrong_book_scan_is_rejected(tmp_path):
    db, printer_path, verifier_path, verifier = _emitted(tmp_path)
    res = labels.verify_label(db, 'TAG-0001', 'C2', 'TAG-0001',
                              paper_of(printer_path, 'TAG-0001'), verifier=verifier)
    assert res['result'] == 'wrong_book' and not res['applied']
    c = connect(db)
    assert c.execute("SELECT state FROM label WHERE serial='TAG-0001'").fetchone()[0] == 'emitted'
    assert c.execute("SELECT count(*) FROM label_verification WHERE result='wrong_book'").fetchone()[0] == 1
    c.close()


def test_reservation_created_before_shelf_change_cannot_print_stale_bytes(tmp_path):
    db = setup(tmp_path)
    job = labels.open_job(db, 'label-01', 'public_call_number')
    label = labels.reserve_copy(db, job['job_id'], 'C1')
    c = sqlite3.connect(db)
    c.execute("UPDATE copy SET planned_shelf='S9' WHERE id='C1'")
    c.commit()
    c.close()
    evaluate(db)
    c = connect(db)
    # The stale reservation is refused before any print command exists.
    with pytest.raises(labels.LabelError, match='label_snapshot_stale'):
        labels._create_command(c, dict(c.execute(
            'SELECT * FROM label_job WHERE id=?', (job['job_id'],)).fetchone()),
            c.execute('SELECT * FROM label WHERE serial=?', (label['serial'],)).fetchone(), 'x')
    c.close()
    assert SimPrinter(tmp_path / 'printer.db').emission(label['serial']) is None


def test_content_digest_mismatch_is_rejected(tmp_path):
    db, printer_path, verifier_path, verifier = _emitted(tmp_path)
    paper = paper_of(printer_path, 'TAG-0001') + b'TAMPERED'
    res = labels.verify_label(db, 'TAG-0001', 'C1', 'TAG-0001', paper, verifier=verifier)
    assert res['result'] == 'content_mismatch' and not res['applied']


def test_ack_without_matching_paper_is_not_enough(tmp_path):
    db, printer_path, verifier_path, verifier = _emitted(tmp_path)
    res = labels.verify_label(db, 'TAG-0001', 'C1', 'TAG-0001', None, verifier=verifier)
    assert res['result'] == 'no_paper' and not res['applied']


def test_old_superseded_paper_cannot_be_reapplied(tmp_path):
    db, job_id, printer_path, verifier_path, _, verifier = start(tmp_path)
    labels.run_job(db, job_id, printer_path, verifier=verifier, inject_fault=False)
    c = sqlite3.connect(db)
    c.execute("UPDATE copy SET planned_shelf='S9' WHERE id='C1'")
    c.commit()
    c.close()
    evaluate(db)
    out = labels.reissue_copy(db, 'label-01', 'C1', printer_path, verifier=verifier)
    res = labels.verify_label(db, 'TAG-0001', 'C1', 'TAG-0001',
                              paper_of(printer_path, 'TAG-0001'), verifier=verifier)
    assert res['result'] == 'serial_not_valid'
    assert out['state'] == 'applied'


# ---------------------------------------------------------------------------
# interruption recovery
# ---------------------------------------------------------------------------

def test_plain_run_will_not_retry_after_interruption(tmp_path):
    db, job_id, printer_path, verifier_path, printer, verifier = start(tmp_path)
    labels.run_job(db, job_id, printer_path, verifier=verifier)
    with pytest.raises(labels.LabelError, match='recovery_required'):
        labels.run_job(db, job_id, printer_path, verifier=verifier, inject_fault=False)
    # Still exactly one sheet: the retry did not print a second TAG-0001.
    assert len([e for e in printer.emissions() if e['serial'] == 'TAG-0001']) == 1
    verifier.record_read('C1', 'TAG-0001', paper_of(printer_path, 'TAG-0001'))
    out = labels.recover_job(db, job_id, printer_path,
                             {'TAG-0001': {'state': 'attached_ok'}}, verifier=verifier)
    assert out['findings'][-1] == {'action': 'resume', 'status': 'completed'}


def test_recovery_keeps_emitted_serial_when_sheet_is_confirmed_on_copy(tmp_path):
    db, job_id, printer_path, verifier_path, printer, verifier = start(tmp_path)
    run = labels.run_job(db, job_id, printer_path, verifier=verifier)
    assert run['status'] == 'interrupted'
    verifier.record_read('C1', 'TAG-0001', paper_of(printer_path, 'TAG-0001'))
    out = labels.recover_job(db, job_id, printer_path,
                             {'TAG-0001': {'state': 'attached_ok'}}, verifier=verifier)
    assert [r['action'] for r in out['findings'][:1]] == ['verified']
    assert out['findings'][-1] == {'action': 'resume', 'status': 'completed'}
    c = connect(db)
    assert c.execute("SELECT state FROM label WHERE serial='TAG-0001'").fetchone()[0] == 'applied'
    assert c.execute("SELECT count(*) FROM printer_emission WHERE serial='TAG-0001'").fetchone()[0] == 1
    c.close()


def test_recovery_requires_physical_readback_not_just_emission(tmp_path):
    db, job_id, printer_path, verifier_path, printer, verifier = start(tmp_path)
    labels.run_job(db, job_id, printer_path, verifier=verifier)
    with pytest.raises(labels.LabelError, match='physical_readback_required'):
        labels.recover_job(db, job_id, printer_path,
                           {'TAG-0001': {'state': 'attached_ok'}}, verifier=verifier)


def test_recovery_on_wrong_book_blocks_printing(tmp_path):
    db, job_id, printer_path, verifier_path, printer, verifier = start(tmp_path)
    labels.run_job(db, job_id, printer_path, verifier=verifier)
    # The sheet came out but was stuck onto C2 by mistake.
    verifier.record_read('C2', 'TAG-0001', paper_of(printer_path, 'TAG-0001'))
    out = labels.recover_job(db, job_id, printer_path,
                             {'TAG-0001': {'state': 'attached_ok'}}, verifier=verifier)
    assert out['status'] == 'interrupted'
    assert out['needs_physical_attention'] == ['TAG-0001']
    c = connect(db)
    assert c.execute("SELECT state FROM label WHERE serial='TAG-0001'").fetchone()[0] == 'emitted'
    assert c.execute('SELECT count(*) FROM label').fetchone()[0] == 1
    c.close()


def test_destroyed_sheet_needs_witness_and_reprints_with_new_serial(tmp_path):
    db, job_id, printer_path, verifier_path, printer, verifier = start(tmp_path)
    labels.run_job(db, job_id, printer_path, verifier=verifier)
    with pytest.raises(labels.LabelError, match='destruction_witness_required'):
        labels.recover_job(db, job_id, printer_path,
                           {'TAG-0001': {'state': 'destroyed'}}, verifier=verifier)
    out = labels.recover_job(db, job_id, printer_path,
                             {'TAG-0001': {'state': 'destroyed', 'witness': 'LIB-7', 'reason': 'torn'}},
                             verifier=verifier)
    c = connect(db)
    old = dict(c.execute("SELECT * FROM label WHERE serial='TAG-0001'").fetchone())
    new = dict(c.execute("SELECT * FROM label WHERE copy_id='C1' AND replaces_serial='TAG-0001'").fetchone())
    destruction = dict(c.execute("SELECT * FROM label_destruction WHERE serial='TAG-0001'").fetchone())
    c.close()
    assert old['state'] == 'destroyed' and destruction['witness'] == 'LIB-7'
    assert new['serial'] == 'TAG-0002' and new['state'] == 'applied'
    assert new['payload_sha256'] != old['payload_sha256']
    # Only one physical sheet ever carried TAG-0001.
    assert len([e for e in printer.emissions() if e['serial'] == 'TAG-0001']) == 1


def test_unknown_whereabouts_quarantines_serial_and_never_reuses_it(tmp_path):
    db, job_id, printer_path, verifier_path, printer, verifier = start(tmp_path)
    labels.run_job(db, job_id, printer_path, verifier=verifier)
    out = labels.recover_job(db, job_id, printer_path,
                             {'TAG-0001': {'state': 'unknown'}}, verifier=verifier)
    c = connect(db)
    old = dict(c.execute("SELECT * FROM label WHERE serial='TAG-0001'").fetchone())
    new = dict(c.execute("SELECT * FROM label WHERE copy_id='C1' AND state='applied'").fetchone())
    c.close()
    assert old['state'] == 'quarantined' and new['serial'] != 'TAG-0001'
    assert new['replaces_serial'] == 'TAG-0001'
    emissions = [e['serial'] for e in printer.emissions()]
    assert emissions.count('TAG-0001') == 1


def test_never_emitted_serial_is_voided_not_quarantined(tmp_path):
    db, job_id, printer_path, verifier_path, _, verifier = start(tmp_path)
    label = labels.reserve_copy(db, job_id, 'C1')
    # A command was marked sent but the device journal proves no paper exists.
    c = sqlite3.connect(db)
    c.execute("INSERT INTO label_command(job_id,serial,nonce,request,request_sha256,paper_sha256,state,created_at) "
              "VALUES (?,?,?,?,?,?,?,?)",
              (job_id, label['serial'], 'nonce-without-paper', b'{}', 'req', label['payload_sha256'],
               'sent', '2026-01-01T00:00:00+00:00'))
    c.execute("UPDATE label SET state='sent' WHERE serial=?", (label['serial'],))
    c.commit()
    c.close()
    out = labels.recover_job(db, job_id, printer_path, {}, verifier=verifier,
                             continue_job=False)
    c = connect(db)
    row = c.execute('SELECT state,void_reason FROM label WHERE serial=?',
                    (label['serial'],)).fetchone()
    c.close()
    assert dict(row) == {'state': 'voided', 'void_reason': 'never_emitted'}
    assert out['findings'][0]['action'] == 'voided_never_emitted'


def test_database_rollback_does_not_erase_emitted_paper(tmp_path):
    db, job_id, printer_path, verifier_path, printer, verifier = start(tmp_path)
    # Simulate the library transaction failing right after physical emission.
    c = sqlite3.connect(db)
    c.execute("CREATE TRIGGER reject_emission BEFORE INSERT ON printer_emission "
              "BEGIN SELECT RAISE(ABORT,'injected'); END")
    c.commit()
    c.close()
    with pytest.raises(sqlite3.IntegrityError):
        labels.run_job(db, job_id, printer_path, verifier=verifier)
    assert printer.emission('TAG-0001') is not None
    c = connect(db)
    assert c.execute("SELECT count(*) FROM printer_emission").fetchone()[0] == 0
    c.execute('DROP TRIGGER reject_emission')
    c.commit()
    c.close()
    # The next reconciliation rebuilds the mirror from the immutable journal.
    labels.sync_device(db, printer)
    c = connect(db)
    assert c.execute("SELECT serial FROM printer_emission").fetchone()[0] == 'TAG-0001'
    c.close()
    verifier.record_read('C1', 'TAG-0001', paper_of(printer_path, 'TAG-0001'))
    out = labels.recover_job(db, job_id, printer_path,
                             {'TAG-0001': {'state': 'attached_ok'}}, verifier=verifier)
    assert out['findings'][-1] == {'action': 'resume', 'status': 'completed'}


# ---------------------------------------------------------------------------
# audit report
# ---------------------------------------------------------------------------

def test_report_before_any_job_still_lists_copies(tmp_path):
    db = setup(tmp_path)
    data = json.loads(report(db, tmp_path / 'r.json').read_text(encoding='utf-8'))
    assert {r['copy_id'] for r in data['label_summary']} == {'C1', 'C2'}
    assert all(r['current_serial'] is None for r in data['label_summary'])


def test_json_and_markdown_reports_are_written(tmp_path):
    db = setup(tmp_path)
    p = report(db, tmp_path / 'nested' / 'report.json')
    assert p.exists() and p.with_suffix('.md').exists()
    data = json.loads(p.read_text(encoding='utf-8'))
    assert 'label_evidence' in data and 'label_control' in data


def test_report_lists_current_serials_printed_and_uncertain_sheets(tmp_path):
    db, job_id, printer_path, verifier_path, printer, verifier = start(tmp_path)
    labels.run_job(db, job_id, printer_path, verifier=verifier)
    labels.recover_job(db, job_id, printer_path,
                       {'TAG-0001': {'state': 'unknown'}}, verifier=verifier)
    p = report(db, tmp_path / 'report.json', printer_path=printer_path,
               verifier_path=verifier_path)
    data = json.loads(p.read_text(encoding='utf-8'))
    md = p.with_suffix('.md').read_text(encoding='utf-8')
    c1 = next(r for r in data['label_summary'] if r['copy_id'] == 'C1')
    assert c1['current_serial'] == 'TAG-0002'
    assert set(c1['printed_serials']) == {'TAG-0001', 'TAG-0002'}
    assert c1['uncertain_serials'] == ['TAG-0001']
    assert 'SHELF S1' in md and 'TAG-0001' in md and '不确定' in md
    assert any('TOKEN TK-C1-A' in r['payload_text'] for r in data['label_control'])
    assert data['device_journal']['emissions']
    assert data['verifier_journal']
