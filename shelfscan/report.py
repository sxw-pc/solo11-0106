import json
from pathlib import Path

from .devices import SimPrinter, SimVerifier
from .store import connect

LABEL_TABLES = ('label_job', 'label_case', 'label_command', 'printer_emission',
                'label_verification', 'label_destruction', 'label_evidence',
                'label_control')


def _text(value):
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode('utf-8', errors='replace')
    return value


def _label_data(db):
    data = {}
    for table in LABEL_TABLES:
        cols = [r[1] for r in db.execute(f'PRAGMA table_info({table})')]
        rows = []
        for row in db.execute(f'SELECT * FROM {table} ORDER BY rowid'):
            item = {}
            for col in cols:
                item[col] = _text(row[col])
                if col in ('payload', 'paper', 'request') and item[col] is not None:
                    item[col + '_text'] = item[col]
            rows.append(item)
        data[table] = rows
    data['label'] = []
    for row in db.execute('SELECT * FROM label ORDER BY reserved_at,serial'):
        item = {k: _text(row[k]) for k in row.keys()}
        item['payload_text'] = _text(row['payload'])
        data['label'].append(item)
    return data


def _summary(labels, copy_ids=()):
    by_copy = {}
    for cid in copy_ids:
        by_copy[cid] = {'copy_id': cid, 'current_serial': None,
                        'current_state': None, 'printed_serials': [],
                        'uncertain_serials': [], 'retired_serials': []}
    for item in labels:
        entry = by_copy.setdefault(item['copy_id'], {
            'copy_id': item['copy_id'],
            'current_serial': None,
            'current_state': None,
            'printed_serials': [],
            'uncertain_serials': [],
            'retired_serials': [],
        })
        # Every state past ``reserved`` means a sheet may exist physically.
        if item['state'] != 'reserved':
            entry['printed_serials'].append(item['serial'])
        if item['state'] in ('quarantined',):
            entry['uncertain_serials'].append(item['serial'])
        if item['state'] in ('voided', 'destroyed', 'superseded'):
            entry['retired_serials'].append(item['serial'])
        if item['state'] == 'applied':
            entry['current_serial'] = item['serial']
            entry['current_state'] = item['state']
    return [by_copy[k] for k in sorted(by_copy)]


def report(db_path, out, printer_path=None, verifier_path=None):
    db = connect(db_path)
    data = {t: [dict(r) for r in db.execute(f'SELECT * FROM {t} ORDER BY 1')]
            for t in ('batch', 'copy', 'reader', 'event', 'evidence', 'issue', 'decision', 'run')}
    data['source'] = [dict(r) for r in db.execute(
        'SELECT id,name,sha256,length(raw) AS byte_length FROM source ORDER BY id')]
    data.update(_label_data(db))
    data['label_summary'] = _summary(
        data['label'], [r['id'] for r in data.get('copy', [])])
    if printer_path is not None:
        printer = SimPrinter(printer_path)
        data['device_journal'] = {
            'printer': [{k: _text(v) for k, v in dict(r).items()}
                        for r in printer.commands()],
            'emissions': [{k: _text(v) for k, v in dict(e).items()}
                          for e in printer.emissions()],
        }
        for e in data['device_journal']['emissions']:
            e['paper_text'] = e['paper']
    if verifier_path is not None:
        data['verifier_journal'] = [
            {k: _text(v) for k, v in dict(r).items()}
            for r in SimVerifier(verifier_path).reads()]
        for r in data['verifier_journal']:
            r['paper_text'] = r['paper'] or ''
    db.close()

    p = Path(out)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    p.with_suffix('.md').write_text(_markdown(data), encoding='utf-8')
    return p


def _markdown(data):
    lines = ['# 馆藏盘点差异', '']
    lines += [f"- {r['copy_id']}: {r['status']} {r['actual_shelf'] or ''}"
              for r in data['decision']]
    lines += ['', '## 当前有效实体标签', '',
              '| copy | 当前序号 | 状态 | 已打印序号 | 隔离 | 已作废/销毁/替代 |',
              '| --- | --- | --- | --- | --- | --- |']
    for row in data.get('label_summary', []):
        lines.append('| {copy_id} | {current_serial} | {current_state} | {printed} | {uncertain} | {retired} |'.format(
            copy_id=row['copy_id'], current_serial=row['current_serial'] or '—',
            current_state=row['current_state'] or '—',
            printed=', '.join(row['printed_serials']) or '—',
            uncertain=', '.join(row['uncertain_serials']) or '—',
            retired=', '.join(row['retired_serials']) or '—'))

    lines += ['', '## 实际纸面字节（已打印）', '']
    for item in data.get('label', []):
        if item['state'] in ('reserved',):
            continue
        lines += [f"### {item['serial']} → {item['copy_id']} ({item['state']}, {item['contract']})", '',
                  f"- payload_sha256: `{item['payload_sha256']}`",
                  f"- template_version: `{item['template_version']}`",
                  '', '```', item['payload_text'].rstrip('\n'), '```', '']

    uncertain = [r for r in data.get('label', []) if r['state'] in ('quarantined',)]
    if uncertain:
        lines += ['## 不确定/隔离纸片', '']
        for row in uncertain:
            lines.append(f"- {row['serial']} ({row['copy_id']}): {row['void_reason']}；不得重试同一序号")
        lines.append('')

    destroyed = data.get('label_destruction', [])
    if destroyed:
        lines += ['## 销毁见证', '', '| 序号 | 见证人 | 原因 | 时间 |', '| --- | --- | --- | --- |']
        for row in destroyed:
            lines.append(f"| {row['serial']} | {row['witness']} | {row['reason']} | {row['at']} |")
        lines.append('')

    jobs = data.get('label_job', [])
    if jobs:
        lines += ['## 标签作业与合同', '', '| job | contract | template | status | input_sha256 |',
                  '| --- | --- | --- | --- | --- |']
        for row in jobs:
            lines.append(f"| {row['id']} | {row['contract']} | {row['template_version']} | "
                         f"{row['status']} | `{row['input_sha256'][:16]}…` |")
        lines.append('')

    controls = data.get('label_control', [])
    if controls:
        lines += ['## 未执行对照版式（渲染但未打印）', '']
        for row in controls:
            lines += [f"### {row['contract']} → {row['copy_id']} (未分配序号, executed=0)", '',
                      '```', row['payload_text'].rstrip('\n'), '```', '']

    evidence = data.get('label_evidence', [])
    if evidence:
        lines += ['## 逐步证据链', '', '| # | 步骤 | 序号 | 操作者 | evidence sha256 | 前序 |',
                  '| --- | --- | --- | --- | --- | --- |']
        for row in evidence:
            lines.append(f"| {row['id']} | {row['step']} | {row['serial'] or '—'} | {row['actor']} | "
                         f"`{row['evidence_sha256'][:16]}…` | `{(row['previous_sha256'] or '')[:16]}` |")
        lines.append('')

    verifications = data.get('label_verification', [])
    if verifications:
        lines += ['## 验扫记录', '', '| 序号 | 贴附书 | 结果 | 读取摘要 | 操作者 |',
                  '| --- | --- | --- | --- | --- |']
        for row in verifications:
            lines.append(f"| {row['serial']} | {row['attached_copy']} | {row['result']} | "
                         f"`{row['observed_sha256'][:16]}…` | {row['operator']} |")
        lines.append('')
    return '\n'.join(lines)
