"""Verifiable firmware upgrade reports: per-device, per-cell and an envelope
whose canonical digest is written to a sidecar so the report itself can be
verified. Reports only read tables; RFID inventory state is never recomputed.
"""
import hashlib
import json
from pathlib import Path

from .store import connect


def device_report(db_path, intent_id):
    db = connect(db_path)
    try:
        rows = []
        for f in db.execute(
                'SELECT * FROM firmware_intent_device WHERE intent_id=? ORDER BY device',
                (intent_id,)):
            live = db.execute('SELECT version,revision FROM firmware_device WHERE id=?',
                              (f['device'],)).fetchone()
            op_install = db.execute('SELECT * FROM firmware_operation WHERE id=?',
                                    (f['install_operation_id'],)).fetchone()
            op_rollback = db.execute('SELECT * FROM firmware_operation WHERE id=?',
                                     (f['rollback_operation_id'],)).fetchone()
            avail = db.execute('SELECT * FROM firmware_reader_availability WHERE intent_id=? AND device=?',
                               (intent_id, f['device'])).fetchone()
            rows.append({
                'device': f['device'],
                'cell': f['cell'],
                'frozen': {'version': f['current_version'], 'revision': f['frozen_revision']},
                'live': {'version': live['version'], 'revision': live['revision']},
                'target_version': f['target_version'],
                'state': f['state'],
                'detail': json.loads(f['detail']),
                'install_operation': None if op_install is None else {
                    'operation_id': op_install['id'], 'action': op_install['action'],
                    'result': op_install['result'],
                    'revision_before': op_install['revision_before'],
                    'revision_after': op_install['revision_after'],
                    'ack': 'lost' if op_install['result'] == 'committed_ack_lost'
                    else 'confirmed'},
                'rollback_operation': None if op_rollback is None else {
                    'operation_id': op_rollback['id'], 'action': op_rollback['action'],
                    'result': op_rollback['result'],
                    'revision_before': op_rollback['revision_before'],
                    'revision_after': op_rollback['revision_after'],
                    'ack': 'lost' if op_rollback['result'] == 'committed_ack_lost'
                    else 'confirmed'},
                'scan_evidence': {
                    'latest_event_id': f['latest_event_id'],
                    'event_sha256': f['latest_event_sha256'],
                    'source_sha256': f['latest_source_sha256']},
                'collection_availability': None if avail is None else {
                    'eligible': bool(avail['eligible']), 'reason': avail['reason'],
                    'version': avail['version'], 'revision': avail['revision']},
            })
        return rows
    finally:
        db.close()


def cell_report(db_path, intent_id):
    db = connect(db_path)
    try:
        out = []
        for rel in db.execute(
                'SELECT * FROM firmware_release WHERE intent_id=? ORDER BY cell', (intent_id,)):
            members = [dict(r) for r in db.execute(
                'SELECT device,state FROM firmware_intent_device WHERE intent_id=? AND cell=? ORDER BY device',
                (intent_id, rel['cell']))]
            out.append({'cell': rel['cell'], 'release_status': rel['status'],
                        'updated_at': rel['updated_at'], 'devices': members})
        return out
    finally:
        db.close()


def build_report(db_path, intent_id):
    db = connect(db_path)
    try:
        intent = db.execute('SELECT * FROM firmware_intent WHERE id=?', (intent_id,)).fetchone()
        if intent is None:
            raise ValueError('firmware_intent_unknown')
        case = db.execute('SELECT payload FROM firmware_case WHERE id=?',
                          (intent['case_id'],)).fetchone()
        fixture = json.loads(case['payload'])['fixture']
        journal = [dict(r) for r in db.execute(
            'SELECT id,intent_id,device,cell,event,detail,at FROM firmware_journal '
            'WHERE intent_id=? ORDER BY id', (intent_id,))]
        evidence_links = [dict(r) for r in db.execute(
            'SELECT d.device,d.cell,d.latest_event_id,d.latest_event_sha256,d.latest_source_sha256,'
            's.name AS source_name FROM firmware_intent_device d '
            'JOIN event e ON e.id=d.latest_event_id JOIN source s ON s.id=e.source_id '
            'WHERE d.intent_id=? ORDER BY d.device', (intent_id,))]
    finally:
        db.close()
    report = {
        'report_kind': 'firmware_upgrade_report',
        'intent': {
            'intent_id': intent['id'], 'case_id': intent['case_id'],
            'contract': intent['contract'], 'status': intent['status'],
            'target_version': intent['target_version'],
            'package_sha256': intent['package_sha256'],
            'intent_sha256': intent['intent_sha256'],
            'created_at': intent['created_at'], 'updated_at': intent['updated_at']},
        'case_fixture': fixture,
        'devices': device_report(db_path, intent_id),
        'cells': cell_report(db_path, intent_id),
        'scan_evidence_links': evidence_links,
        'journal': journal,
    }
    canonical = json.dumps(report, sort_keys=True, separators=(',', ':'),
                           ensure_ascii=False).encode('utf-8')
    report['report_sha256'] = hashlib.sha256(canonical).hexdigest()
    return report


def write_report(db_path, intent_id, out_path):
    report = build_report(db_path, intent_id)
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    digest = report['report_sha256']
    out.with_name(out.stem + '.sha256').write_text(f'{digest}  {out.name}\n', encoding='utf-8')
    lines = ['# 读写器固件升级报告', '',
             f"- 意图: {report['intent']['intent_id']} ({report['intent']['status']})",
             f"- 合同: {report['intent']['contract']}",
             f"- 目标包 sha256: `{report['intent']['package_sha256']}`",
             f"- 报告 sha256: `{digest}`", '', '## 逐设备', '']
    for d in report['devices']:
        avail = d['collection_availability']
        avail_text = '未裁定' if avail is None else (
            f"可用({avail['reason']})" if avail['eligible'] else f"不可用({avail['reason']})")
        lines.append(
            f"- {d['device']} [{d['cell']}]: {d['frozen']['version']}/rev{d['frozen']['revision']}"
            f" -> {d['live']['version']}/rev{d['live']['revision']} | 状态 {d['state']}"
            f" | 安装 {_op(d['install_operation'])} | 回退 {_op(d['rollback_operation'])}"
            f" | 采录 {avail_text}"
            f" | 证据 event#{d['scan_evidence']['latest_event_id']}"
            f" source {d['scan_evidence']['source_sha256'][:12]}…")
    lines += ['', '## 单元发布', '']
    for c in report['cells']:
        lines.append(f"- {c['cell']}: {c['release_status']} ("
                     + ', '.join(f"{m['device']}={m['state']}" for m in c['devices']) + ')')
    out.with_suffix('.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    return out


def _op(op):
    if op is None:
        return '未执行'
    return f"{op['operation_id']}={op['result']}/ack={op['ack']}"
