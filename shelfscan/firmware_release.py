"""Recoverable batch firmware upgrade orchestration on top of the local
reader endpoints in :mod:`shelfscan.firmware_lab`.

The orchestrator never computes an offline success flag. Every decision is
reconciled against the durable device endpoint (firmware_device /
firmware_operation) and the replayable intent log, so an interrupted process
resumes from device reality instead of re-issuing installs blindly.

Orchestration writes use short, immediately committed transactions; device
endpoint calls (which own their own connection and transaction) are never
made while this module holds an open write transaction.
"""
import hashlib
import json
from datetime import datetime, timezone

from . import firmware_lab
from .store import connect

CONTRACTS = ('atomic_fleet', 'cell_isolated')


class OrchestrationCrash(RuntimeError):
    """Raised by an injected crash barrier to simulate process interruption."""


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def _canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def _sha256_text(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def package_sha256(case_payload):
    """Hash of the target package described by a frozen firmware case."""
    doc = json.loads(case_payload)['doc']
    return _sha256_text(_canon({'target_version': doc['target_version'],
                                'readers': sorted(r['id'] for r in doc['readers'])}))


def _case_payload(db, case_id):
    row = db.execute('SELECT payload FROM firmware_case WHERE id=?', (case_id,)).fetchone()
    if row is None:
        raise ValueError('firmware_case_missing')
    return row['payload']


def _log(db, intent_id, event, device=None, cell=None, detail=None):
    db.execute('INSERT INTO firmware_journal(intent_id,device,cell,event,detail,at) VALUES (?,?,?,?,?,?)',
               (intent_id, device, cell, event, _canon(detail or {}), _now()))


def plan(db_path, intent_id, case_id, contract, crash_after=None):
    """Freeze a replayable upgrade intent. Idempotent on intent_id."""
    crash_after = dict(crash_after or {})
    if contract not in CONTRACTS:
        raise ValueError('firmware_contract_unknown')
    db = connect(db_path)
    try:
        payload = _case_payload(db, case_id)
        pkg = package_sha256(payload)
        doc = json.loads(payload)['doc']
        frozen = []
        for item in sorted(doc['readers'], key=lambda r: r['id']):
            rid = item['id']
            dev = db.execute('SELECT * FROM firmware_device WHERE id=?', (rid,)).fetchone()
            if dev is None:
                raise ValueError('firmware_reader_unknown')
            ev = db.execute(
                'SELECT e.id,e.sha256,s.sha256 AS source_sha256 FROM event e '
                'JOIN source s ON s.id=e.source_id WHERE e.device=? ORDER BY e.seq DESC, e.id DESC LIMIT 1',
                (rid,)).fetchone()
            if ev is None:
                raise ValueError('firmware_reader_unobserved')
            install_op = f'{intent_id}:{rid}:install'
            rollback_op = f'{intent_id}:{rid}:rollback'
            # An operation ID is bound to one device and one action forever.
            for op_id in (install_op, rollback_op):
                clash = db.execute('SELECT device,action FROM firmware_operation WHERE id=?',
                                   (op_id,)).fetchone()
                if clash is not None:
                    raise ValueError('firmware_operation_conflict')
            frozen.append({'device': rid, 'cell': item['cell'],
                           'current_version': item['current_version'],
                           'frozen_revision': dev['revision'],
                           'target_version': doc['target_version'],
                           'latest_event_id': ev['id'], 'latest_event_sha256': ev['sha256'],
                           'latest_source_sha256': ev['source_sha256'],
                           'install_operation_id': install_op,
                           'rollback_operation_id': rollback_op})
        intent_hash = _sha256_text(_canon(
            {'case_id': case_id, 'contract': contract,
             'target_version': doc['target_version'], 'package_sha256': pkg,
             'devices': frozen}))
        existing = db.execute('SELECT intent_sha256 FROM firmware_intent WHERE id=?',
                              (intent_id,)).fetchone()
        if existing is not None:
            if existing['intent_sha256'] != intent_hash:
                raise ValueError('firmware_intent_conflict')
            return {'intent_id': intent_id, 'replayed': True, 'readers': len(frozen),
                    'package_sha256': pkg, 'intent_sha256': intent_hash}
        now = _now()
        with db:
            db.execute('INSERT INTO firmware_intent VALUES (?,?,?,?,?,?,?,?,?)',
                       (intent_id, case_id, contract, doc['target_version'], pkg,
                        intent_hash, 'planned', now, now))
            for f in frozen:
                db.execute('INSERT INTO firmware_intent_device VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                           (intent_id, f['device'], f['cell'], f['current_version'],
                            f['frozen_revision'], f['target_version'], f['latest_event_id'],
                            f['latest_event_sha256'], f['latest_source_sha256'],
                            f['install_operation_id'], f['rollback_operation_id'],
                            'pending', _canon({'frozen_at': now})))
                db.execute('INSERT OR IGNORE INTO firmware_release VALUES (?,?,?,?)',
                           (intent_id, f['cell'], 'pending', now))
            _log(db, intent_id, 'intent_planned', detail={'contract': contract,
                                                          'readers': [f['device'] for f in frozen],
                                                          'package_sha256': pkg,
                                                          'intent_sha256': intent_hash})
        if crash_after.get('after_intent_written'):
            raise OrchestrationCrash('after_intent_written')
        return {'intent_id': intent_id, 'replayed': False, 'readers': len(frozen),
                'package_sha256': pkg, 'intent_sha256': intent_hash}
    finally:
        db.close()


def _devices(db, intent_id):
    return [dict(r) for r in db.execute(
        'SELECT * FROM firmware_intent_device WHERE intent_id=? ORDER BY device', (intent_id,))]


def _record_state(db, intent_id, device, state, detail, journal_event=None, cell=None,
                  journal_detail=None):
    with db:
        db.execute('UPDATE firmware_intent_device SET state=?,detail=? WHERE intent_id=? AND device=?',
                   (state, _canon(detail), intent_id, device))
        if journal_event is not None:
            _log(db, intent_id, journal_event, device=device, cell=cell,
                 detail=journal_detail if journal_detail is not None else detail)


def _touch(db, intent_id, status):
    with db:
        db.execute('UPDATE firmware_intent SET status=?,updated_at=? WHERE id=?',
                   (status, _now(), intent_id))


def _manual(db, intent_id, device, reason, detail):
    """Freeze the run for human handling; never force a version write."""
    _record_state(db, intent_id, device, 'manual_hold', detail,
                  journal_event='manual_hold', journal_detail={'reason': reason, **detail})
    _touch(db, intent_id, 'awaiting_manual')


def _settle_install_after_timeout(db_path, db, intent_id, f):
    """ACK lost: reconcile against endpoint state and the operation record."""
    live = firmware_lab.state(db_path, f['device'])
    op = db.execute('SELECT * FROM firmware_operation WHERE id=?',
                    (f['install_operation_id'],)).fetchone()
    detail = {'endpoint_status': 'timeout', 'observed_version': live['version'],
              'observed_revision': live['revision']}
    if op is not None and op['result'] in ('committed', 'committed_ack_lost'):
        detail['operation_result'] = op['result']
    if live['version'] == f['target_version'] and op is not None \
            and op['result'] in ('committed', 'committed_ack_lost'):
        _record_state(db, intent_id, f['device'], 'installed', detail,
                      journal_event='install_confirmed_after_timeout', cell=f['cell'])
        return True
    if live['version'] == f['current_version']:
        _record_state(db, intent_id, f['device'], 'install_failed', detail,
                      journal_event='install_not_committed', cell=f['cell'])
        return False
    _manual(db, intent_id, f['device'], 'unexpected_device_version', detail)
    return None


def _settle_rollback_after_timeout(db_path, db, intent_id, f):
    live = firmware_lab.state(db_path, f['device'])
    op = db.execute('SELECT * FROM firmware_operation WHERE id=?',
                    (f['rollback_operation_id'],)).fetchone()
    detail = {'endpoint_status': 'timeout', 'observed_version': live['version'],
              'observed_revision': live['revision']}
    if op is not None:
        detail['operation_result'] = op['result']
    if live['version'] == f['current_version'] and op is not None \
            and op['result'] in ('committed', 'committed_ack_lost'):
        _record_state(db, intent_id, f['device'], 'rolled_back', detail,
                      journal_event='rollback_confirmed_after_timeout', cell=f['cell'])
        return True
    if live['version'] == f['target_version']:
        _record_state(db, intent_id, f['device'], 'rollback_failed', detail,
                      journal_event='rollback_not_committed', cell=f['cell'])
        return False
    _manual(db, intent_id, f['device'], 'unexpected_device_version', detail)
    return None


def run(db_path, intent_id, crash_after=None):
    """Drive the intent to a terminal/recoverable state. Safe to re-run.

    crash_after maps an interruption point to the number of times it may fire
    (1 simulates a single crash; a resumed run does not carry the barrier).
    """
    crash_after = dict(crash_after or {})
    fired = set()

    def barrier(point, device=None):
        keys = [point] if device is None else [f'{point}:{device}', point]
        for key in keys:
            if crash_after.get(key) and key not in fired:
                fired.add(key)
                raise OrchestrationCrash(key)

    db = connect(db_path)
    try:
        intent = db.execute('SELECT * FROM firmware_intent WHERE id=?', (intent_id,)).fetchone()
        if intent is None:
            raise ValueError('firmware_intent_unknown')
        if intent['status'] in ('completed', 'rejected', 'awaiting_manual'):
            return _summary(db, intent_id)
        contract = intent['contract']

        # ---- phase 1: installs, reconciled with the real endpoint --------
        for f in _devices(db, intent_id):
            if f['state'] in ('installed', 'rejected', 'install_failed',
                              'rolled_back', 'rollback_failed', 'manual_hold'):
                continue
            # Resume first: an endpoint operation may already exist even though
            # the orchestrator state never advanced (crash after device commit,
            # before confirmation). Reconcile from reality instead of re-invoking.
            prior_op = db.execute('SELECT * FROM firmware_operation WHERE id=?',
                                  (f['install_operation_id'],)).fetchone()
            if prior_op is not None:
                live = firmware_lab.state(db_path, f['device'])
                if prior_op['result'] in ('committed', 'committed_ack_lost') \
                        and live['version'] == f['target_version']:
                    if _settle_install_after_timeout(db_path, db, intent_id, f) is None:
                        continue
                    continue
                if prior_op['result'] == 'rejected':
                    _record_state(db, intent_id, f['device'], 'rejected',
                                  {'endpoint_status': 'rejected',
                                   'observed_revision': prior_op['revision_after'],
                                   'recovered_from_operation_log': True},
                                  journal_event='install_rejected_recovered', cell=f['cell'],
                                  journal_detail={'operation_id': f['install_operation_id']})
                    continue
                _manual(db, intent_id, f['device'], 'unexpected_operation_state',
                        {'operation_result': prior_op['result'],
                         'observed_version': live['version']})
                continue
            live = firmware_lab.state(db_path, f['device'])
            if live['revision'] != f['frozen_revision']:
                _manual(db, intent_id, f['device'], 'external_revision_conflict',
                        {'frozen_revision': f['frozen_revision'],
                         'observed_revision': live['revision'],
                         'observed_version': live['version']})
                continue
            if live['version'] == f['target_version']:
                # A prior attempt already committed (possibly with a lost ACK).
                if _settle_install_after_timeout(db_path, db, intent_id, f) is None:
                    continue
                continue
            try:
                resp = firmware_lab.invoke(db_path, f['device'],
                                           f['install_operation_id'],
                                           f['frozen_revision'], 'install')
            except ValueError as exc:
                if str(exc) == 'firmware_revision_conflict':
                    live = firmware_lab.state(db_path, f['device'])
                    _manual(db, intent_id, f['device'], 'external_revision_conflict',
                            {'frozen_revision': f['frozen_revision'],
                             'observed_revision': live['revision'],
                             'observed_version': live['version']})
                    continue
                raise
            # Device commit is durable here; the ACK may already be lost.
            barrier('after_install_commit', device=f['device'])
            if resp['status'] == 'timeout':
                if _settle_install_after_timeout(db_path, db, intent_id, f) is None:
                    continue
            elif resp['status'] == 'rejected':
                _record_state(db, intent_id, f['device'], 'rejected',
                              {'endpoint_status': 'rejected', 'observed_revision': resp['revision']},
                              journal_event='install_rejected', cell=f['cell'],
                              journal_detail={'operation_id': f['install_operation_id']})
            elif resp['status'] == 'committed':
                _record_state(db, intent_id, f['device'], 'installed',
                              {'endpoint_status': 'committed', 'observed_revision': resp['revision']},
                              journal_event='install_committed', cell=f['cell'],
                              journal_detail={'operation_id': f['install_operation_id'],
                                              'revision': resp['revision']})
            else:  # pragma: no cover - defensive, endpoint contract is fixed
                _manual(db, intent_id, f['device'], 'unknown_endpoint_status',
                        {'response': resp})

        if any(d['state'] == 'manual_hold' for d in _devices(db, intent_id)):
            return _summary(db, intent_id)

        # ---- phase 2: rollback decision under the chosen contract --------
        fresh = {d['device']: d for d in _devices(db, intent_id)}
        failed_devices = {d for d, f in fresh.items()
                          if f['state'] in ('rejected', 'install_failed')}
        cells = sorted({f['cell'] for f in fresh.values()})
        failed_cells = {fresh[d]['cell'] for d in failed_devices}
        if failed_devices:
            rollback_cells = set(cells) if contract == 'atomic_fleet' else set(failed_cells)
            rollback_devices = [d for d, f in sorted(fresh.items())
                                if f['cell'] in rollback_cells
                                and f['state'] in ('installed', 'rollback_failed')]
        else:
            rollback_cells, rollback_devices = set(), []

        halt = False
        for rid in rollback_devices:
            f = next(d for d in _devices(db, intent_id) if d['device'] == rid)
            if f['state'] == 'rolled_back':
                continue
            live = firmware_lab.state(db_path, rid)
            if live['version'] == f['current_version']:
                # Rollback already committed before an interruption.
                if _settle_rollback_after_timeout(db_path, db, intent_id, f) is None:
                    halt = True
                    break
                continue
            if live['version'] != f['target_version']:
                _manual(db, intent_id, rid, 'unexpected_device_version',
                        {'observed_version': live['version'],
                         'expected_target': f['target_version']})
                halt = True
                break
            if live['revision'] != f['frozen_revision'] + 1:
                _manual(db, intent_id, rid, 'external_revision_conflict',
                        {'frozen_revision': f['frozen_revision'],
                         'observed_revision': live['revision']})
                halt = True
                break
            try:
                resp = firmware_lab.invoke(db_path, rid, f['rollback_operation_id'],
                                           live['revision'], 'rollback')
            except ValueError as exc:
                if str(exc) == 'firmware_revision_conflict':
                    live = firmware_lab.state(db_path, rid)
                    _manual(db, intent_id, rid, 'external_revision_conflict',
                            {'expected_revision': f['frozen_revision'] + 1,
                             'observed_revision': live['revision']})
                    halt = True
                    break
                raise
            barrier('after_rollback_commit', device=rid)
            if resp['status'] == 'timeout':
                if _settle_rollback_after_timeout(db_path, db, intent_id, f) is None:
                    halt = True
                    break
            elif resp['status'] == 'committed':
                _record_state(db, intent_id, rid, 'rolled_back',
                              {'endpoint_status': 'committed', 'observed_revision': resp['revision']},
                              journal_event='rollback_committed', cell=f['cell'],
                              journal_detail={'operation_id': f['rollback_operation_id'],
                                              'revision': resp['revision']})
            else:  # pragma: no cover - defensive
                _manual(db, intent_id, rid, 'unknown_endpoint_status', {'response': resp})
                halt = True
                break
        if halt or any(d['state'] == 'manual_hold' for d in _devices(db, intent_id)):
            return _summary(db, intent_id)

        # ---- phase 3: per-cell release and collection eligibility --------
        final = {d['device']: d for d in _devices(db, intent_id)}
        for cell in cells:
            rel = db.execute('SELECT status FROM firmware_release WHERE intent_id=? AND cell=?',
                             (intent_id, cell)).fetchone()
            if rel is not None and rel['status'] != 'pending':
                continue
            members = [d for d in final.values() if d['cell'] == cell]
            if all(d['state'] == 'installed' for d in members):
                cell_status = 'released_v2'
            elif all(d['state'] in ('rolled_back', 'rejected', 'install_failed',
                                    'rollback_failed') for d in members):
                cell_status = 'released_v1'
            else:
                cell_status = 'blocked'
            now = _now()
            with db:
                db.execute('UPDATE firmware_release SET status=?,updated_at=? WHERE intent_id=? AND cell=?',
                           (cell_status, now, intent_id, cell))
                for d in members:
                    live = firmware_lab.state(db_path, d['device'])
                    if cell_status == 'released_v2' and live['version'] == d['target_version']:
                        eligible, reason = 1, 'released_on_target'
                    elif cell_status == 'released_v1' and live['version'] == d['current_version']:
                        eligible, reason = 0, 'released_on_original'
                    else:
                        eligible, reason = 0, 'release_unconfirmed'
                    db.execute('INSERT OR REPLACE INTO firmware_reader_availability VALUES (?,?,?,?,?,?,?,?)',
                               (intent_id, d['device'], cell, eligible, reason,
                                live['version'], live['revision'], now))
                    _log(db, intent_id, 'availability_decided', device=d['device'], cell=cell,
                         detail={'eligible': bool(eligible), 'reason': reason,
                                 'version': live['version'], 'revision': live['revision']})
                _log(db, intent_id, 'cell_released', cell=cell, detail={'status': cell_status})
            barrier('during_cell_release', device=cell)

        # ---- phase 4: intent terminal status ------------------------------
        final_rows = _devices(db, intent_id)
        if any(d['state'] == 'manual_hold' for d in final_rows):
            status = 'awaiting_manual'
        elif all(d['state'] == 'installed' for d in final_rows):
            status = 'completed'
        elif any(d['state'] in ('rejected', 'install_failed', 'rolled_back',
                                'rollback_failed') for d in final_rows):
            status = 'rejected'
        else:
            status = 'blocked'
        _touch(db, intent_id, status)
        return _summary(db, intent_id)
    finally:
        db.close()


def _summary(db, intent_id):
    intent = db.execute('SELECT * FROM firmware_intent WHERE id=?', (intent_id,)).fetchone()
    return {'intent_id': intent_id, 'status': intent['status'],
            'contract': intent['contract'],
            'devices': [{'device': d['device'], 'state': d['state']}
                        for d in _devices(db, intent_id)]}
