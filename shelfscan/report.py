import json
from pathlib import Path
from .store import connect
TABLES=('batch','copy','reader','event','evidence','issue','decision','decision_history','run',
        'donation_case','authorization','acceptance','holding','voucher','stage_log')
def report(db_path,out):
    db=connect(db_path)
    data={t:[dict(r) for r in db.execute(f'SELECT * FROM {t} ORDER BY 1')] for t in TABLES}
    data['source']=[dict(r) for r in db.execute('SELECT id,name,sha256,length(raw) AS byte_length FROM source ORDER BY id')]
    p=Path(out);p.parent.mkdir(parents=True,exist_ok=True)
    p.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
    lines=['# 馆藏盘点差异','',
           '\n'.join(f"- {r['copy_id']}: {r['status']} {r['actual_shelf'] or ''}" for r in data['decision']),
           '', '# 捐赠接收与入藏', '']
    for a in data['acceptance']:
        lines.append(f"- 案件 {a['case_id']} 标签 {a['tag']}: 阶段={a['stage']} "
                     f"路径={a['path'] or '未选择'} 产权人={a['owner'] or '未确定'} "
                     f"流通={a['circulation'] or '未定'} 返还义务={a['return_by'] or '无'} "
                     f"争议={a['dispute_code'] or '无'}")
    lines += ['', '## 凭证', '']
    lines += [f"- {v['id']} [{v['kind']}] {v['tag']}: 产权人={v['owner']} "
              f"流通={v['circulation']} 返还={v['return_by'] or '无'}"
              for v in data['voucher']] or ['- (无)']
    lines += ['', '## 旧批未知标签证据（不可回溯消除）', '']
    lines += [f"- issue#{i['id']} {i['copy_id']} {i['code']} run={i['run_id']} {i['detail']}"
              for i in data['issue'] if i['code'] == 'unexpected_copy'] or ['- (无)']
    p.with_suffix('.md').write_text('\n'.join(lines),encoding='utf-8')
    db.close();return p
