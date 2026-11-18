import json
from pathlib import Path
from .store import connect
def report(db_path,out):
    db=connect(db_path)
    data={t:[dict(r) for r in db.execute(f'SELECT * FROM {t} ORDER BY 1')] for t in ('batch','copy','reader','event','evidence','issue','decision','run')}
    data['source']=[dict(r) for r in db.execute('SELECT id,name,sha256,length(raw) AS byte_length FROM source ORDER BY id')]
    data['climate_case']=[dict(r) for r in db.execute('SELECT id,payload FROM climate_case ORDER BY id')]
    data['climate_plan']=[dict(r) for r in db.execute('SELECT case_id,plan_version,status,objective,payload,created_at FROM climate_plan ORDER BY case_id,plan_version')]
    data['climate_command']=[dict(r) for r in db.execute('SELECT case_id,plan_version,period,shelf,status,price_cents FROM climate_command ORDER BY case_id,plan_version,period')]
    data['climate_feedback']=[dict(r) for r in db.execute('SELECT id,case_id,base_version,new_version,through_period,readings_sha256,created_at FROM climate_feedback ORDER BY id')]
    p=Path(out);p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
    p.with_suffix('.md').write_text('# 馆藏盘点差异\n\n'+'\n'.join(f"- {r['copy_id']}: {r['status']} {r['actual_shelf'] or ''}" for r in data['decision']),encoding='utf-8');db.close();return p

def climate_report(db_path,out):
    """JSON + Markdown executable cooling report (commands, exposure, cost)."""
    db=connect(db_path)
    plans=[json.loads(r['payload']) for r in db.execute(
        'SELECT payload FROM climate_plan ORDER BY case_id,plan_version')]
    commands=[dict(r) for r in db.execute(
        'SELECT case_id,plan_version,period,shelf,status,price_cents FROM climate_command ORDER BY case_id,plan_version,period')]
    feedbacks=[dict(r) for r in db.execute(
        'SELECT case_id,base_version,new_version,through_period,readings_sha256,created_at FROM climate_feedback ORDER BY id')]
    db.close()
    payload={'plans':plans,'commands':commands,'feedback':feedbacks}
    p=Path(out);p.parent.mkdir(parents=True,exist_ok=True)
    p.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8')
    p.with_suffix('.md').write_text(_render_markdown(plans,commands,feedbacks),encoding='utf-8')
    return p

def _render_markdown(plans,commands,feedbacks):
    lines=['# 书架温控冷却计划','']
    if not plans:
        lines.append('（无温控计划）')
        return '\n'.join(lines)
    for plan in plans:
        lines.append(f"## {plan['case_id']} / {plan['plan_version']} / {plan['objective']} — {plan['status']}")
        lines.append('')
        fb=plan.get('feedback')
        if fb:
            lines.append(f"- 迟到反馈基准: {fb['base_version']}，已冻结至时段 {fb['through_period']}，"
                         f"读数 SHA256: `{fb['readings_sha256']}`")
            lines.append(f"- 已付费用 {fb['frozen_cost_cents']} 分；已发生暴露 {fb['frozen_exposure_c_periods']}")
        totals=plan.get('totals',{})
        lines.append(f"- 总费用: **{totals.get('cost_cents')} 分**；各架最大累计暴露: "
                     f"**{totals.get('max_shelf_exposure_c_periods')} °C·时段**；"
                     f"总暴露: {totals.get('total_exposure_c_periods')} °C·时段（合同上限 "
                     f"{plan['constraints']['total_exposure_limit_c_periods']}）")
        lines.append('')
        if plan['status']=='待核':
            ev=plan['optimality_evidence']
            lines.append(f"- ⚠️ 整份建议待核，不发出局部命令；最低可达总暴露 "
                         f"{ev.get('min_achievable_total_exposure_c_periods')} °C·时段")
            lines.append('')
            continue
        lines.append('| 时段 | 起点温度(°C) | 命令 | 设备占用 | 结束温度(°C) | 本时段暴露(°C) | 电价(分) |')
        lines.append('| --- | --- | --- | --- | --- | --- | --- |')
        for r in plan['periods']:
            start=', '.join(f"{k}:{v:g}" for k,v in r['start_temps'].items())
            end=', '.join(f"{k}:{v:g}" for k,v in r['end_temps'].items())
            exp=', '.join(f"{k}:{v:g}" for k,v in r['period_exposure_c'].items())
            cmd=r['command_shelf'] or '（不开冷却）'
            lines.append(f"| {r['period']} | {start} | {cmd} | {'是' if r['cooler_occupied'] else '否'} "
                         f"| {end} | {exp} | {r['price_cents']} |")
        lines.append('')
        lines.append('- 逐架累计暴露: '+', '.join(f"{k}={v:g}" for k,v in plan['exposure_by_shelf'].items()))
        ev=plan['optimality_evidence']
        lines.append(f"- 最优性证据: {ev['method']}；逐层状态数 {ev['state_space_by_layer']}；"
                     f"枚举终点 {ev['terminal_states_enumerated']}，可行终点 {ev['feasible_terminal_states']}")
        lines.append(f"- 选择次序: {ev['selection_order']}；{ev['global_optimum_basis']}")
        lines.append('')
    lines.append('## 设备命令台账')
    for c in commands:
        if c['shelf'] is None:
            continue
        lines.append(f"- {c['case_id']} {c['plan_version']} 时段 {c['period']} → 冷却 {c['shelf']}，"
                     f"{c['price_cents']} 分，状态 {c['status']}")
    for f in feedbacks:
        lines.append(f"- 反馈链: {f['base_version']} → {f['new_version']}（至时段 {f['through_period']}，"
                     f"`{f['readings_sha256']}`）")
    return '\n'.join(lines)
