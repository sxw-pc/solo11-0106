"""Physical label issuance for catalogued copies.

The flow is deliberately split into separately evidenced steps:

    register -> open_job -> reserve -> command -> print (paper emitted)
             -> verify (physical read-back) -> applied
             -> void / quarantine / destroy -> reprint

Library facts come from the frozen ingest evidence and the catalogued copy
snapshot; the printer and verifier keep their own durable journals so paper
that has physically left the printer cannot be made to disappear by rolling
back the library database.
"""
from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .devices import (
    AckLost,
    DuplicateCommand,
    SerialAlreadyUsed,
    SimPrinter,
    SimVerifier,
)
from .store import connect, connect_tx

CONTRACTS = ('public_call_number', 'opaque_tag')
LIVE_STATES = ('reserved', 'sent', 'emitted', 'verified', 'applied')
PRINTED_STATES = ('sent', 'emitted', 'verified', 'applied')


class LabelError(RuntimeError):
    """A label business rule was violated."""


def _now():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')


def _sha(data):
    if isinstance(data, str):
        data = data.encode('utf-8')
    return hashlib.sha256(data).hexdigest()


def _canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def _tx(db_path):
    db = connect_tx(db_path)
    db.execute('BEGIN IMMEDIATE')
    return db


def _commit(db):
    db.execute('COMMIT')
    db.close()


def _rollback(db):
    try:
        db.execute('ROLLBACK')
    finally:
        db.close()


# ---------------------------------------------------------------------------
# case registration
# ---------------------------------------------------------------------------

def register(db_path, case_id, fixture):
    """Freeze the label case together with real copy/ingest evidence.

    Kept compatible with the original seed API: it only registers input and the
    declared fault point, it does not print anything.
    """
    source = Path(fixture).resolve()
    raw = source.read_bytes()
    doc = json.loads(raw)
    if not case_id or not doc.get('copies') or not doc.get('contracts') or not doc.get('printer'):
        raise ValueError('bad_label_case')
    db = connect(db_path)
    try:
        with db:
            copies = []
            for cid in doc['copies']:
                row = db.execute('SELECT * FROM copy WHERE id=?', (cid,)).fetchone()
                if row is None:
                    raise ValueError('unknown_label_copy')
                events = [dict(event) for event in db.execute(
                    'SELECT e.id,e.device,e.seq,e.at,e.sha256,s.sha256 AS source_sha256 '
                    'FROM event e JOIN source s ON s.id=e.source_id '
                    'WHERE e.copy_id=? ORDER BY e.at,e.id', (cid,))]
                copies.append({'copy': dict(row), 'events': events})
            payload = json.dumps({'fixture': str(source), 'digest': hashlib.sha256(raw).hexdigest(),
                                  'doc': doc, 'copies': copies}, sort_keys=True, separators=(',', ':'))
            old = db.execute('SELECT payload FROM label_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('label_case_conflict')
            db.execute('INSERT OR IGNORE INTO label_case VALUES (?,?)', (case_id, payload))
            db.execute('INSERT OR IGNORE INTO label_template(case_id,version,updated_at) VALUES (?,?,?)',
                       (case_id, doc.get('template_version', 'v1'), _now()))
    finally:
        db.close()
    return {'case_id': case_id, 'copies': len(copies), 'serials': len(doc['serials'])}


def _case(db, case_id):
    row = db.execute('SELECT payload FROM label_case WHERE id=?', (case_id,)).fetchone()
    if row is None:
        raise LabelError('unknown_label_case')
    return json.loads(row['payload'])


def _doc(db, case_id):
    return _case(db, case_id)['doc']


def current_template(db_path, case_id):
    db = connect_tx(db_path)
    try:
        row = db.execute('SELECT version FROM label_template WHERE case_id=?', (case_id,)).fetchone()
        if row is None:
            return _doc(db, case_id).get('template_version', 'v1')
        return row['version']
    finally:
        db.close()


def set_template(db_path, case_id, version, actor='admin'):
    """Record a house template change.

    Any label printed with the previous template version will fail physical
    verification as ``snapshot_stale``; its bytes cannot masquerade as the new
    template.
    """
    if not version:
        raise LabelError('template_version_required')
    db = _tx(db_path)
    try:
        _case(db, case_id)
        old = db.execute('SELECT version FROM label_template WHERE case_id=?', (case_id,)).fetchone()
        if old and old['version'] == version:
            _commit(db)
            return {'case_id': case_id, 'template_version': version, 'changed': False}
        db.execute('INSERT INTO label_template(case_id,version,updated_at) VALUES (?,?,?) '
                   'ON CONFLICT(case_id) DO UPDATE SET version=excluded.version,updated_at=excluded.updated_at',
                   (case_id, version, _now()))
        _append_evidence(db, None, None, 'template_changed', actor,
                         {'case_id': case_id, 'from': old['version'] if old else None, 'to': version})
        _commit(db)
        return {'case_id': case_id, 'template_version': version, 'changed': True}
    except Exception:
        _rollback(db)
        raise


# ---------------------------------------------------------------------------
# deterministic paper rendering and snapshot binding
# ---------------------------------------------------------------------------

def render_paper(contract, copy_id, serial, template_version, snapshot_version,
                 public_shelf=None, opaque_token=None):
    """Render the exact bytes that are physically printed on one label.

    The public layout exposes the shelf call number so readers can find the
    book directly.  The opaque layout exposes only an in-house token; mapping
    that token to a shelf requires the staff authorisation mapping.
    """
    if contract not in CONTRACTS:
        raise LabelError('unknown_contract')
    if contract == 'public_call_number' and not public_shelf:
        raise LabelError('public_shelf_required')
    if contract == 'opaque_tag' and not opaque_token:
        raise LabelError('opaque_token_required')
    lines = [
        'SHELFSCAN-LABEL',
        'CONTRACT ' + contract,
        'COPY ' + str(copy_id),
        'SERIAL ' + (str(serial) if serial is not None else '<unassigned>'),
        'TEMPLATE ' + str(template_version),
        'SNAPSHOT ' + str(snapshot_version),
    ]
    if contract == 'public_call_number':
        lines.append('SHELF ' + str(public_shelf))
    else:
        lines.append('TOKEN ' + str(opaque_token))
    return ('\n'.join(lines) + '\n').encode('utf-8')


def _doc_value(doc, table, cid):
    return (doc.get(table) or {}).get(cid)


def _subject_from_db(db, case_id, copy_id, doc=None):
    """Identity + ingest provenance + shelf snapshot + rules conclusion.

    A change to any of these changes the digest and therefore invalidates old
    paper for the copy.
    """
    doc = doc or _doc(db, case_id)
    copy_row = db.execute('SELECT * FROM copy WHERE id=?', (copy_id,)).fetchone()
    if copy_row is None:
        raise LabelError('unknown_label_copy')
    batch = db.execute('SELECT * FROM batch WHERE id=?', (copy_row['batch'],)).fetchone()
    events = [dict(r) for r in db.execute(
        'SELECT e.id,e.device,e.seq,e.kind,e.at,e.sha256,s.sha256 AS source_sha256 '
        'FROM event e JOIN source s ON s.id=e.source_id WHERE e.copy_id=? ORDER BY e.at,e.id',
        (copy_id,))]
    decision = db.execute('SELECT * FROM decision WHERE copy_id=?', (copy_id,)).fetchone()
    template_row = db.execute('SELECT version FROM label_template WHERE case_id=?', (case_id,)).fetchone()
    template_version = template_row['version'] if template_row else doc.get('template_version', 'v1')
    subject = {
        'case_id': case_id,
        'case_sha256': _sha(_canonical(doc)),
        'copy_id': copy_id,
        'planned_shelf': copy_row['planned_shelf'],
        'batch': copy_row['batch'],
        'batch_version': batch['version'] if batch else None,
        'template_version': template_version,
        'ingest_events': [
            {'id': e['id'], 'device': e['device'], 'seq': e['seq'], 'kind': e['kind'],
             'at': e['at'], 'event_sha256': e['sha256'], 'source_sha256': e['source_sha256']}
            for e in events
        ],
        'decision': None if decision is None else {
            'status': decision['status'],
            'actual_shelf': decision['actual_shelf'],
            'rule_version': decision['rule_version'],
        },
    }
    subject['subject_sha256'] = _sha(_canonical({k: v for k, v in subject.items()
                                                 if k != 'subject_sha256'}))
    return subject


def snapshot(db_path, case_id):
    db = connect_tx(db_path)
    try:
        doc = _doc(db, case_id)
        return {cid: _subject_from_db(db, case_id, cid, doc) for cid in doc['copies']}
    finally:
        db.close()


# ---------------------------------------------------------------------------
# job planning
# ---------------------------------------------------------------------------

def _input_digest(case_doc, case_sha, contract, template_version, subjects):
    return _sha(_canonical({
        'case_sha256': case_sha,
        'contract': contract,
        'template_version': template_version,
        'subjects': {cid: s['subject_sha256'] for cid, s in sorted(subjects.items())},
    }))


def _ensure_decisions(db_path):
    """Rules conclusions are part of the snapshot; compute them if absent."""
    from .rules import evaluate
    db = connect(db_path)
    try:
        has = db.execute('SELECT 1 FROM decision LIMIT 1').fetchone()
    finally:
        db.close()
    if not has:
        evaluate(db_path)


def open_job(db_path, case_id, contract, actor='cli'):
    """Open one label job for exactly one pre-authorised contract.

    The other contract is rendered into label_control as an unexecuted
    comparison: the exact bytes are retained, but it can never be reserved,
    sent or applied from this job.
    """
    if contract not in CONTRACTS:
        raise LabelError('unknown_contract')
    _ensure_decisions(db_path)
    db = _tx(db_path)
    try:
        doc = _doc(db, case_id)
        if doc.get('printer', {}).get('durable_spool') is not True:
            raise LabelError('printer_spool_not_durable')
        if contract not in doc.get('contracts', ()):
            raise LabelError('contract_not_authorised')
        trow = db.execute('SELECT version FROM label_template WHERE case_id=?', (case_id,)).fetchone()
        template_version = trow['version'] if trow else doc.get('template_version', 'v1')
        active = db.execute("SELECT id FROM label_job WHERE case_id=? AND status IN ('planned','active','interrupted')",
                            (case_id,)).fetchone()
        if active:
            raise LabelError('job_already_active')
        executed = {r['contract'] for r in db.execute(
            'SELECT DISTINCT j.contract FROM label_job j JOIN label l ON l.job_id=j.id '
            "WHERE j.case_id=? AND l.state='applied'", (case_id,))}
        if executed and contract not in executed:
            raise LabelError('case_contract_already_executed')
        subjects = {cid: _subject_from_db(db, case_id, cid, doc) for cid in doc['copies']}
        input_sha = _input_digest(doc, _sha(_canonical(doc)), contract, template_version, subjects)
        jid = db.execute(
            'INSERT INTO label_job(case_id,contract,template_version,input_sha256,status,created_at) '
            'VALUES (?,?,?,?,?,?)',
            (case_id, contract, template_version, input_sha, 'planned', _now())).lastrowid

        serials = list(doc['serials'])
        controls = []
        other = [c for c in CONTRACTS if c != contract][0]
        for idx, cid in enumerate(doc['copies']):
            subj = subjects[cid]
            try:
                # No serial is allocated to a layout that is never printed.
                payload = render_paper(
                    other, cid, None, template_version, subj['batch_version'],
                    public_shelf=subj['planned_shelf'],
                    opaque_token=_doc_value(doc, 'opaque_tokens', cid))
            except LabelError:
                # A layout the case cannot render is still recorded as an
                # unexecuted control, with its required input marked missing.
                payload = ('SHELFSCAN-LABEL\nCONTRACT %s\nCOPY %s\nSERIAL <unassigned>\n'
                           'TEMPLATE %s\nSNAPSHOT %s\nBLOCKED missing_input\n'
                           % (other, cid, template_version,
                              subj['batch_version'])).encode('utf-8')
            # ``serial`` stays NULL: no physical serial is allocated to a
            # layout that is only ever rendered for comparison.
            db.execute(
                'INSERT INTO label_control(job_id,copy_id,contract,serial,payload,payload_sha256,executed) '
                'VALUES (?,?,?,?,?,?,0)',
                (jid, cid, other, None, payload, _sha(payload)))
            controls.append({'copy_id': cid, 'contract': other, 'serial': None,
                             'payload_sha256': _sha(payload)})
        _append_evidence(db, jid, None, 'job_opened', actor,
                         {'case_id': case_id, 'contract': contract,
                          'template_version': template_version, 'input_sha256': input_sha,
                          'control_contract': other})
        _commit(db)
        return {'job_id': jid, 'case_id': case_id, 'contract': contract,
                'template_version': template_version, 'input_sha256': input_sha,
                'copies': list(doc['copies']), 'controls': controls}
    except Exception:
        _rollback(db)
        raise


def _job(db, job_id):
    row = db.execute('SELECT * FROM label_job WHERE id=?', (job_id,)).fetchone()
    if row is None:
        raise LabelError('unknown_label_job')
    return dict(row)


def _live_label(db, job_id, copy_id):
    return db.execute(
        "SELECT * FROM label WHERE job_id=? AND copy_id=? AND state IN ('reserved','sent','emitted','verified','applied')",
        (job_id, copy_id)).fetchone()


def _label_for_serial(db, serial):
    return db.execute('SELECT * FROM label WHERE serial=?', (serial,)).fetchone()


def _append_evidence(db, job_id, serial, step, actor, detail):
    prev = db.execute('SELECT evidence_sha256 FROM label_evidence ORDER BY id DESC LIMIT 1').fetchone()
    previous = prev['evidence_sha256'] if prev else None
    body = {
        'job_id': job_id,
        'serial': serial,
        'step': step,
        'actor': actor,
        'detail': detail,
        'at': _now(),
        'previous_sha256': previous,
    }
    digest = _sha(_canonical(body))
    db.execute('INSERT INTO label_evidence(job_id,serial,step,evidence_sha256,previous_sha256,actor,detail,at) '
               'VALUES (?,?,?,?,?,?,?,?)',
               (job_id, serial, step, digest, previous, actor, _canonical(detail), body['at']))
    return digest


def _set_job(db, job_id, status, ended=False):
    if ended:
        db.execute('UPDATE label_job SET status=?,ended_at=? WHERE id=?', (status, _now(), job_id))
    else:
        db.execute('UPDATE label_job SET status=? WHERE id=?', (status, job_id))


# ---------------------------------------------------------------------------
# reservation
# ---------------------------------------------------------------------------

def _select_serial(db, doc):
    used = {r['serial'] for r in db.execute('SELECT serial FROM label')}
    for serial in doc['serials']:
        if serial not in used:
            return serial
    raise LabelError('no_serial_available')


def _reserve_locked(db, job, copy_id, actor, replaces_serial=None):
    doc = _doc(db, job['case_id'])
    if copy_id not in doc['copies']:
        raise LabelError('copy_not_in_case')
    live = _live_label(db, job['id'], copy_id)
    if live is not None:
        raise LabelError('copy_already_labelled')
    serial = _select_serial(db, doc)
    subject = _subject_from_db(db, job['case_id'], copy_id, doc)
    payload = render_paper(
        job['contract'], copy_id, serial, job['template_version'],
        subject['batch_version'], public_shelf=subject['planned_shelf'],
        opaque_token=_doc_value(doc, 'opaque_tokens', copy_id))
    now = _now()
    db.execute(
        'INSERT INTO label(serial,job_id,copy_id,contract,template_version,payload,payload_sha256,'
        'subject_sha256,public_shelf,opaque_token,replaces_serial,state,reserved_at,updated_at) '
        'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (serial, job['id'], copy_id, job['contract'], job['template_version'], payload,
         _sha(payload), subject['subject_sha256'], subject['planned_shelf'],
         _doc_value(doc, 'opaque_tokens', copy_id), replaces_serial, 'reserved', now, now))
    if job['status'] == 'planned':
        _set_job(db, job['id'], 'active')
    _append_evidence(db, job['id'], serial, 'reserved', actor,
                     {'copy_id': copy_id, 'payload_sha256': _sha(payload),
                      'subject_sha256': subject['subject_sha256'],
                      'replaces_serial': replaces_serial})
    return dict(db.execute('SELECT * FROM label WHERE serial=?', (serial,)).fetchone())


def reserve_copy(db_path, job_id, copy_id, actor='librarian'):
    """Atomically reserve an unused serial for one copy.

    BEGIN IMMEDIATE serialises concurrent librarians; the partial unique index
    on live labels is the second line of defence.
    """
    db = _tx(db_path)
    try:
        job = _job(db, job_id)
        if job['status'] not in ('planned', 'active', 'interrupted'):
            raise LabelError('job_not_open')
        label = _reserve_locked(db, job, copy_id, actor)
        _commit(db)
        return label
    except sqlite3.IntegrityError:
        _rollback(db)
        # Second line of defence: the partial unique index refused a second
        # live label for this copy (or the serial was taken concurrently).
        raise LabelError('copy_already_labelled')
    except Exception:
        _rollback(db)
        raise


# ---------------------------------------------------------------------------
# print commands
# ---------------------------------------------------------------------------

def _create_command(db, job, label, actor):
    existing = db.execute('SELECT * FROM label_command WHERE serial=?', (label['serial'],)).fetchone()
    if existing:
        return dict(existing)
    # Refuse to spend paper on a reservation that no longer matches the
    # catalogued copy, its ingest evidence, its shelf or the template.
    current = _subject_from_db(db, job['case_id'], label['copy_id'])
    if current['subject_sha256'] != label['subject_sha256']:
        raise LabelError('label_snapshot_stale')
    if current['template_version'] != label['template_version']:
        raise LabelError('label_template_stale')
    nonce = secrets.token_hex(16)
    request = _canonical({
        'job_id': job['id'],
        'copy_id': label['copy_id'],
        'serial': label['serial'],
        'contract': label['contract'],
        'template_version': label['template_version'],
        'nonce': nonce,
        'paper_sha256': label['payload_sha256'],
    }).encode('utf-8')
    cid = db.execute(
        'INSERT INTO label_command(job_id,serial,nonce,request,request_sha256,paper_sha256,state,created_at) '
        'VALUES (?,?,?,?,?,?,?,?)',
        (job['id'], label['serial'], nonce, request, _sha(request),
         label['payload_sha256'], 'sent', _now())).lastrowid
    _append_evidence(db, job['id'], label['serial'], 'send', actor,
                     {'nonce': nonce, 'request_sha256': _sha(request),
                      'paper_sha256': label['payload_sha256']})
    return dict(db.execute('SELECT * FROM label_command WHERE id=?', (cid,)).fetchone())


def _record_emission(db, emission, actor):
    """Mirror one physical emission into the library, idempotently."""
    existing = db.execute('SELECT * FROM printer_emission WHERE nonce=?', (emission['nonce'],)).fetchone()
    if existing:
        if existing['paper_sha256'] != emission['paper_sha256'] or existing['serial'] != emission['serial']:
            raise LabelError('emission_tampered')
        return False
    db.execute(
        'INSERT INTO printer_emission(command_id,job_id,serial,copy_id,nonce,paper,paper_sha256,device,emitted_at) '
        'VALUES (?,?,?,?,?,?,?,?,?)',
        (emission['command_id'], emission['job_id'], emission['serial'], emission['copy_id'],
         emission['nonce'], emission['paper'], emission['paper_sha256'],
         emission['device'], emission['emitted_at']))
    label = _label_for_serial(db, emission['serial'])
    if label is not None and label['state'] in ('reserved', 'sent'):
        db.execute("UPDATE label SET state='emitted',updated_at=? WHERE serial=?",
                   (_now(), emission['serial']))
    _append_evidence(db, emission['job_id'], emission['serial'], 'paper_emitted', actor,
                     {'nonce': emission['nonce'], 'paper_sha256': emission['paper_sha256'],
                      'device': emission['device'], 'emitted_at': emission['emitted_at']})
    return True


def sync_device(db_path, printer, actor='system'):
    """Reconcile the library mirror with the immutable device journals."""
    db = _tx(db_path)
    try:
        emissions = printer.emissions()
        commands = {c['nonce']: c for c in printer.commands()}
        changed = []
        for emission in emissions:
            if _record_emission(db, emission, actor):
                changed.append(emission['serial'])
            command = db.execute('SELECT * FROM label_command WHERE nonce=?', (emission['nonce'],)).fetchone()
            device_cmd = commands.get(emission['nonce'])
            if command is not None and device_cmd is not None and device_cmd['state'] == 'acked':
                if command['state'] != 'acked':
                    db.execute("UPDATE label_command SET state='acked',ack_at=?,ack_payload=? WHERE id=?",
                               (device_cmd['acked_at'], device_cmd['ack'], command['id']))
                    _append_evidence(db, command['job_id'], command['serial'], 'ack_received', actor,
                                     {'nonce': emission['nonce'], 'ack': device_cmd['ack'],
                                      'acked_at': device_cmd['acked_at']})
        _commit(db)
        return {'emissions': len(emissions), 'new': changed}
    except Exception:
        _rollback(db)
        raise


def _send(db_path, command, printer, actor, suppress_ack):
    label = None
    db = connect_tx(db_path)
    try:
        label = _label_for_serial(db, command['serial'])
        job = _job(db, command['job_id'])
    finally:
        db.close()

    try:
        result = printer.print_command(
            command['id'], job['id'], label['copy_id'], label['serial'],
            command['nonce'], label['payload'], suppress_ack=suppress_ack)
        status = 'acked'
    except AckLost as exc:
        result = {'status': 'ack_lost', 'serial': exc.serial, 'nonce': exc.nonce,
                  'command_id': exc.command_id}
        status = 'ack_lost'
    except DuplicateCommand:
        # The printer journal already knows this nonce.  Never emit again.
        existing = printer.command(command['nonce'])
        if existing is None or existing['paper_sha256'] != label['payload_sha256']:
            raise LabelError('duplicate_nonce_conflict')
        result = {'status': 'acked' if existing['state'] == 'acked' else existing['state'],
                  'serial': existing['serial'], 'nonce': existing['nonce'],
                  'command_id': existing['id'], 'duplicate_command': True}
        status = result['status']
        db = _tx(db_path)
        try:
            _append_evidence(db, job['id'], label['serial'], 'duplicate_command', actor,
                             {'nonce': command['nonce'], 'device_state': existing['state'],
                              'emissions': 1})
            _commit(db)
        except Exception:
            _rollback(db)
            raise
    except SerialAlreadyUsed:
        db = _tx(db_path)
        try:
            _append_evidence(db, job['id'], label['serial'], 'send_failed', actor,
                             {'reason': 'serial_already_used', 'nonce': command['nonce']})
            _commit(db)
        except Exception:
            _rollback(db)
        raise LabelError('serial_already_used')

    sync_device(db_path, printer, actor=actor)
    db = _tx(db_path)
    try:
        row = db.execute('SELECT state FROM label_command WHERE id=?', (command['id'],)).fetchone()
        if status == 'ack_lost':
            _set_job(db, job['id'], 'interrupted')
            _append_evidence(db, job['id'], label['serial'], 'ack_lost', actor,
                             {'nonce': command['nonce'], 'paper_emitted': True})
        elif row is not None and row['state'] != 'acked':
            db.execute("UPDATE label_command SET state='acked' WHERE id=?", (command['id'],))
        _commit(db)
    except Exception:
        _rollback(db)
        raise
    return {'command_id': command['id'], 'serial': label['serial'],
            'nonce': command['nonce'], 'status': status,
            'paper_sha256': label['payload_sha256'], 'device': result}


def send_command(db_path, command_id, printer, actor='librarian', suppress_ack=False):
    db = connect_tx(db_path)
    try:
        command = db.execute('SELECT * FROM label_command WHERE id=?', (command_id,)).fetchone()
        if command is None:
            raise LabelError('unknown_label_command')
        label = _label_for_serial(db, command['serial'])
    finally:
        db.close()
    if label is None:
        raise LabelError('unknown_label_serial')
    if label['state'] not in ('reserved', 'sent', 'emitted'):
        raise LabelError('label_not_sendable')
    return _send(db_path, dict(command), printer, actor, suppress_ack)


# ---------------------------------------------------------------------------
# physical verification
# ---------------------------------------------------------------------------

def _verification_result(db, label, attached_copy, observed_serial, observed_paper,
                         expected_sha, serial_known=True):
    if observed_paper is None:
        return 'no_paper'
    if label is None or not serial_known:
        return 'unknown_serial'
    if label['state'] not in ('sent', 'emitted', 'verified', 'applied'):
        return 'serial_not_valid'
    if observed_serial != label['serial']:
        return 'serial_mismatch'
    if attached_copy != label['copy_id']:
        return 'wrong_book'
    observed_sha = _sha(bytes(observed_paper))
    if observed_sha != expected_sha or observed_sha != label['payload_sha256']:
        return 'content_mismatch'
    emission = db.execute('SELECT * FROM printer_emission WHERE serial=?', (label['serial'],)).fetchone()
    if emission is None or emission['paper_sha256'] != observed_sha:
        return 'no_paper'
    job = _job(db, label['job_id'])
    subject = _subject_from_db(db, job['case_id'], label['copy_id'])
    if subject['subject_sha256'] != label['subject_sha256']:
        return 'snapshot_stale'
    current_template = db.execute('SELECT version FROM label_template WHERE case_id=?',
                                  (job['case_id'],)).fetchone()
    if current_template is not None and current_template['version'] != label['template_version']:
        return 'snapshot_stale'
    return 'ok'


def verify_label(db_path, serial, attached_copy, observed_serial, observed_paper,
                 verifier=None, operator='librarian', job_id=None):
    """VERIFY-1 read-back.  Only a matching physical read applies the label.

    ``observed_paper`` must be the bytes actually read from the physical label
    attached to ``attached_copy``; an ACK alone is never sufficient.
    """
    read = None
    if verifier is not None:
        read = verifier.record_read(attached_copy, observed_serial, observed_paper, job_id=job_id)

    db = _tx(db_path)
    try:
        label = _label_for_serial(db, serial)
        expected = label['payload_sha256'] if label is not None else _sha(b'')
        result = _verification_result(db, label, attached_copy, observed_serial,
                                      observed_paper, expected)
        jid = label['job_id'] if label is not None else job_id
        row = db.execute(
            'INSERT INTO label_verification(job_id,serial,copy_id,attached_copy,expected_sha256,'
            'observed_sha256,observed_serial,device_read_id,result,operator,at) '
            'VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            (jid, serial, label['copy_id'] if label else '', attached_copy, expected,
             _sha(bytes(observed_paper)) if observed_paper is not None else '',
             observed_serial, read['id'] if read else None, result, operator, _now()))
        vid = row.lastrowid
        if result == 'ok':
            repeat = label['state'] == 'applied'
            if not repeat:
                db.execute("UPDATE label SET state='applied',updated_at=? WHERE serial=?",
                           (_now(), serial))
                _append_evidence(db, jid, serial, 'applied', operator,
                                 {'verification_id': vid, 'attached_copy': attached_copy,
                                  'observed_sha256': _sha(bytes(observed_paper)),
                                  'device_read_id': read['id'] if read else None})
            else:
                _append_evidence(db, jid, serial, 'verify_repeat_ok', operator,
                                 {'verification_id': vid, 'attached_copy': attached_copy})
            _finalize_job(db, jid)
        else:
            _append_evidence(db, jid, serial, 'verify_failed', operator,
                             {'verification_id': vid, 'result': result,
                              'attached_copy': attached_copy,
                              'observed_serial': observed_serial})
        _commit(db)
        return {'result': result, 'verification_id': vid,
                'serial': serial, 'applied': result == 'ok',
                'device_read_id': read['id'] if read else None}
    except Exception:
        _rollback(db)
        raise


def _finalize_job(db, job_id):
    job = _job(db, job_id)
    if job['status'] not in ('active', 'interrupted'):
        return
    doc = _doc(db, job['case_id'])
    for cid in doc['copies']:
        live = _live_label(db, job_id, cid)
        if live is None or live['state'] != 'applied':
            return
    _set_job(db, job_id, 'completed', ended=True)
    _append_evidence(db, job_id, None, 'job_completed', 'system',
                     {'contract': job['contract'], 'input_sha256': job['input_sha256']})


# ---------------------------------------------------------------------------
# void / destroy / quarantine
# ---------------------------------------------------------------------------

def void_label(db_path, serial, operator, witness=None, reason='voided',
               destroyed=False, proven_absent=False):
    """Retire a physical sheet.

    * ``destroyed`` requires a named witness.
    * ``proven_absent`` records that the device journal proves no paper was
      ever emitted, so the serial is plainly voided.
    * anything else is quarantined: the sheet may exist somewhere and the
      serial must never be reprinted.
    """
    if destroyed and not witness:
        raise LabelError('destruction_witness_required')
    if not reason:
        raise LabelError('void_reason_required')
    db = _tx(db_path)
    try:
        label = _label_for_serial(db, serial)
        if label is None:
            raise LabelError('unknown_label_serial')
        if label['state'] in ('destroyed', 'quarantined', 'superseded', 'voided'):
            raise LabelError('label_already_retired')
        if label['state'] == 'applied':
            raise LabelError('label_applied_cannot_void')
        if destroyed:
            db.execute('INSERT INTO label_destruction(serial,witness,reason,at) VALUES (?,?,?,?)',
                       (serial, witness, reason, _now()))
            state = 'destroyed'
            step = 'destroyed'
        elif proven_absent:
            state = 'voided'
            step = 'voided'
        else:
            state = 'quarantined'
            step = 'quarantined'
        db.execute('UPDATE label SET state=?,void_reason=?,updated_at=? WHERE serial=?',
                   (state, reason, _now(), serial))
        _append_evidence(db, label['job_id'], serial, step, operator,
                         {'reason': reason, 'witness': witness,
                          'proven_absent': proven_absent})
        _commit(db)
        return {'serial': serial, 'state': state, 'witness': witness}
    except Exception:
        _rollback(db)
        raise


# ---------------------------------------------------------------------------
# job execution and recovery
# ---------------------------------------------------------------------------

def _fault_matches(doc, serial, inject_fault):
    fault = doc.get('fault') or {}
    return bool(inject_fault and fault.get('serial') == serial
                and fault.get('point') == 'paper_emitted_before_ack')


def _auto_verify(db_path, printer, verifier, serial, actor, attached_copy=None):
    # No verifier, no applied state: an ACK is only a command acknowledgement.
    if verifier is None:
        return None
    emission = printer.emission(serial)
    if emission is None:
        return None
    return verify_label(db_path, serial, attached_copy or emission['copy_id'],
                        serial, emission['paper'], verifier=verifier,
                        operator=actor, job_id=emission['job_id'])


def run_job(db_path, job_id, printer_path, verifier=None,
            actor='librarian', inject_fault=True, auto_verify=True, device_path=None):
    """Reserve, print and (optionally) physically verify every copy in a job.

    An injected fault leaves the job ``interrupted`` with paper emitted but no
    ACK.  Recovery must consult the device journals and a physical read-back;
    it is never safe to blindly print the same serial again.
    """
    printer = SimPrinter(printer_path)
    verifier = verifier or (SimVerifier(device_path) if device_path else None)
    db = connect_tx(db_path)
    try:
        job = _job(db, job_id)
        doc = _doc(db, job['case_id'])
    finally:
        db.close()
    if job['status'] not in ('planned', 'active'):
        if job['status'] == 'interrupted':
            raise LabelError('recovery_required')
        raise LabelError('job_not_open')

    for cid in doc['copies']:
        db = connect_tx(db_path)
        try:
            label = _live_label(db, job_id, cid)
        finally:
            db.close()
        if label is not None and label['state'] == 'applied':
            continue
        if label is not None and label['state'] in ('sent', 'emitted', 'verified'):
            # Paper may already exist for this reservation; only recover_job
            # with physical evidence may resolve it.
            raise LabelError('recovery_required')
        if label is None:
            label = reserve_copy(db_path, job_id, cid, actor=actor)
        if label['state'] == 'reserved':
            db = _tx(db_path)
            try:
                job = _job(db, job_id)
                label = _label_for_serial(db, label['serial'])
                command = _create_command(db, job, label, actor)
                _commit(db)
            except Exception:
                _rollback(db)
                raise
            suppress = _fault_matches(doc, label['serial'], inject_fault)
            sent = send_command(db_path, command['id'], printer, actor=actor, suppress_ack=suppress)
            if sent['status'] == 'ack_lost':
                return {'job_id': job_id, 'status': 'interrupted',
                        'serial': label['serial'], 'reason': 'paper_emitted_before_ack'}
        if auto_verify:
            db = connect_tx(db_path)
            try:
                current = _label_for_serial(db, label['serial'])
            finally:
                db.close()
            if current is not None and current['state'] in ('emitted', 'sent'):
                result = _auto_verify(db_path, printer, verifier, label['serial'], actor)
                if result is not None and result['result'] != 'ok':
                    return {'job_id': job_id, 'status': 'verification_failed',
                            'serial': label['serial'], 'result': result['result']}

    db = _tx(db_path)
    try:
        _finalize_job(db, job_id)
        job = _job(db, job_id)
        labels = [dict(r) for r in db.execute(
            'SELECT serial,copy_id,state,payload_sha256 FROM label WHERE job_id=? ORDER BY serial',
            (job_id,))]
        _commit(db)
        return {'job_id': job_id, 'status': job['status'], 'labels': labels}
    except Exception:
        _rollback(db)
        raise


def _reprint(db_path, job_id, printer, verifier, copy_id, old_serial,
             actor, auto_verify):
    db = _tx(db_path)
    try:
        job = _job(db, job_id)
        if job['status'] == 'planned':
            _set_job(db, job_id, 'active')
        if job['status'] == 'completed':
            raise LabelError('job_completed')
        label = _reserve_locked(db, job, copy_id, actor, replaces_serial=old_serial)
        job = _job(db, job_id)
        command = _create_command(db, job, label, actor)
        _append_evidence(db, job_id, label['serial'], 'reprint', actor,
                         {'replaces_serial': old_serial, 'nonce': command['nonce']})
        _commit(db)
    except Exception:
        _rollback(db)
        raise
    sent = send_command(db_path, command['id'], printer, actor=actor, suppress_ack=False)
    if auto_verify:
        return _auto_verify(db_path, printer, verifier, label['serial'], actor)
    return {'serial': label['serial'], 'status': sent['status'], 'verification': None}


def recover_job(db_path, job_id, printer_path, findings, verifier=None,
                actor='librarian', device_path=None, continue_job=True,
                auto_verify=True):
    """Recover an interrupted job from immutable device evidence.

    ``findings`` maps serial -> one of::

        {'state': 'attached_ok'}
        {'state': 'destroyed', 'witness': 'W-1', 'reason': 'torn'}
        {'state': 'unknown'}

    Only the physical read-back can justify continuing with the same serial.
    A destroyed sheet requires a witness before voiding and reprinting with a
    new serial.  An uncertain sheet is quarantined; the serial is never reused.
    """
    printer = SimPrinter(printer_path)
    verifier = verifier or (SimVerifier(device_path) if device_path else None)
    sync_device(db_path, printer, actor=actor)

    db = connect_tx(db_path)
    try:
        job = _job(db, job_id)
        doc = _doc(db, job['case_id'])
        labels = [dict(r) for r in db.execute(
            "SELECT * FROM label WHERE job_id=? AND state IN ('sent','emitted','verified') ORDER BY serial",
            (job_id,))]
    finally:
        db.close()
    if job['status'] not in ('interrupted', 'active'):
        raise LabelError('job_not_interrupted')
    if findings and verifier is None and any(
            isinstance(v, dict) and v.get('state') == 'attached_ok' for v in findings.values()):
        raise LabelError('verifier_required')

    results = []
    failed = []
    for label in labels:
        serial = label['serial']
        emission = printer.emission(serial)
        finding = findings.get(serial) if isinstance(findings, dict) else None
        if emission is None:
            # Device journal proves no paper exists; retire the serial cleanly.
            void_label(db_path, serial, actor, reason='never_emitted', proven_absent=True)
            results.append({'serial': serial, 'action': 'voided_never_emitted',
                            'copy_id': label['copy_id']})
            results.append({'serial': None, 'action': 'reprint_required',
                            'copy_id': label['copy_id']})
            continue
        if finding is None:
            raise LabelError('physical_finding_required:' + serial)
        state = finding.get('state')
        if state == 'attached_ok':
            # Verify against the verifier journal, not against an assumption.
            reads = [r for r in verifier.reads() if r['observed_serial'] == serial
                     and r['paper_sha256'] == emission['paper_sha256']]
            if not reads:
                raise LabelError('physical_readback_required:' + serial)
            read = reads[-1]
            result = verify_label(db_path, serial, read['attached_copy'], read['observed_serial'],
                                  read['paper'], verifier=None, operator=actor, job_id=job_id)
            results.append({'serial': serial, 'action': 'verified',
                            'result': result['result'], 'readback_id': read['id']})
            if not result['applied']:
                failed.append(serial)
        elif state == 'destroyed':
            void_label(db_path, serial, actor, witness=finding.get('witness'),
                       reason=finding.get('reason', 'damaged'), destroyed=True)
            results.append({'serial': serial, 'action': 'destroyed',
                            'copy_id': label['copy_id'],
                            'witness': finding.get('witness')})
            results.append({'serial': None, 'action': 'reprint',
                            'copy_id': label['copy_id']})
        elif state == 'unknown':
            void_label(db_path, serial, actor, reason='whereabouts_unknown', destroyed=False)
            results.append({'serial': serial, 'action': 'quarantined',
                            'copy_id': label['copy_id']})
            results.append({'serial': None, 'action': 'reprint',
                            'copy_id': label['copy_id']})
        else:
            raise LabelError('bad_physical_finding')

    # Any copy that needs a replacement gets a brand new serial.
    needs = [r for r in results if r['action'] == 'reprint']
    retired = {}
    for r in results:
        if r.get('serial') and r.get('copy_id') and r['action'] in (
                'destroyed', 'quarantined', 'voided_never_emitted'):
            retired[r['copy_id']] = r['serial']
    for item in needs:
        out = _reprint(db_path, job_id, printer, verifier, item['copy_id'],
                       retired.get(item['copy_id']), actor, auto_verify)
        results.append({'serial': out.get('serial'), 'action': 'reprinted',
                        'copy_id': item['copy_id'],
                        'result': (out.get('verification') or {}).get('result')})

    if failed:
        # Physical evidence contradicts the label: do not print or apply
        # anything else until staff resolve the book in hand.
        db = _tx(db_path)
        try:
            job = _job(db, job_id)
            if job['status'] == 'active':
                _set_job(db, job_id, 'interrupted')
            _append_evidence(db, job_id, None, 'recovery_blocked', actor,
                             {'serials': failed})
            _commit(db)
        except Exception:
            _rollback(db)
            raise
        return {'job_id': job_id, 'status': 'interrupted',
                'findings': results, 'needs_physical_attention': failed}

    if continue_job:
        db = _tx(db_path)
        try:
            if _job(db, job_id)['status'] == 'interrupted':
                _set_job(db, job_id, 'active')
                _append_evidence(db, job_id, None, 'recovery_resolved', actor,
                                 {'findings': findings})
            _commit(db)
        except Exception:
            _rollback(db)
            raise
        resumed = run_job(db_path, job_id, printer_path, verifier=verifier, actor=actor,
                          inject_fault=False, auto_verify=auto_verify)
        results.append({'action': 'resume', 'status': resumed['status']})
    return {'job_id': job_id, 'findings': results}


# ---------------------------------------------------------------------------
# reissue after a source/shelf change
# ---------------------------------------------------------------------------

def supersede_label(db_path, serial, actor='librarian', reason='snapshot_changed'):
    """Retire a still-attached label after its shelf/identity snapshot changed.

    The physical sheet stays on the book until staff replace it; it simply
    stops being a current valid label, and a fresh serial must be printed.
    """
    db = _tx(db_path)
    try:
        label = _label_for_serial(db, serial)
        if label is None:
            raise LabelError('unknown_label_serial')
        if label['state'] not in ('emitted', 'verified', 'applied'):
            raise LabelError('label_not_supersedable')
        db.execute("UPDATE label SET state='superseded',void_reason=?,updated_at=? WHERE serial=?",
                   (reason, _now(), serial))
        _append_evidence(db, label['job_id'], serial, 'superseded', actor,
                         {'reason': reason})
        _commit(db)
        return {'serial': serial, 'state': 'superseded', 'reason': reason}
    except Exception:
        _rollback(db)
        raise


def reissue_copy(db_path, case_id, copy_id, printer_path, verifier=None,
                 actor='librarian', device_path=None):
    """Supersede any existing label for one copy and issue a fresh serial.

    Opens a new one-contract job when the previous job has already completed.
    The new paper is rendered from the *current* snapshot, never from the old
    bytes.
    """
    printer = SimPrinter(printer_path)
    verifier = verifier or (SimVerifier(device_path) if device_path else None)
    db = connect_tx(db_path)
    try:
        live = [dict(r) for r in db.execute(
            "SELECT * FROM label WHERE copy_id=? AND state IN ('emitted','verified','applied') "
            'ORDER BY rowid', (copy_id,))]
        openjob = db.execute(
            "SELECT * FROM label_job WHERE case_id=? AND status IN ('planned','active','interrupted')",
            (case_id,)).fetchone()
        lastjob = db.execute(
            'SELECT contract FROM label_job WHERE case_id=? ORDER BY id DESC LIMIT 1',
            (case_id,)).fetchone()
        doc = _doc(db, case_id)
    finally:
        db.close()
    for label in live:
        supersede_label(db_path, label['serial'], actor=actor,
                        reason='source_snapshot_changed')
    if openjob is None:
        contract = lastjob['contract'] if lastjob else doc['contracts'][0]
        job = open_job(db_path, case_id, contract, actor=actor)
        job_id = job['job_id']
    else:
        job_id = openjob['id']
    db = _tx(db_path)
    try:
        job = _job(db, job_id)
        label = _reserve_locked(db, job, copy_id, actor,
                                replaces_serial=live[-1]['serial'] if live else None)
        job = _job(db, job_id)
        command = _create_command(db, job, label, actor)
        _commit(db)
    except Exception:
        _rollback(db)
        raise
    sent = send_command(db_path, command['id'], printer, actor=actor, suppress_ack=False)
    verification = _auto_verify(db_path, printer, verifier, label['serial'], actor)
    return {'job_id': job_id, 'serial': label['serial'], 'state': 'applied'
            if verification and verification.get('applied') else 'emitted',
            'paper_text': label['payload'].decode('utf-8'),
            'paper_sha256': label['payload_sha256'],
            'send': sent, 'verification': verification}
