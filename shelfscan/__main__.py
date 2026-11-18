import argparse,json,tempfile
from pathlib import Path
from .store import connect
from .ingest import ingest
from .rules import evaluate
from .report import report, climate_report
from .climate_control import register as climate_register, plan as climate_plan, apply_feedback
def initialize(db_path,config):
    c=json.loads(Path(config).read_text(encoding='utf-8'));db=connect(db_path)
    with db:
        for table,cols in [('batch',['id','version','start','end','status']),('copy',['id','planned_shelf','loaned','batch']),('reader',['id','shelf','window_start','window_end'])]:
            for row in c[table]:db.execute(f'INSERT OR IGNORE INTO {table} VALUES ({",".join("?" for _ in cols)})',[row[k] for k in cols])
    db.close()
def demo(workdir=None):
    root=Path(workdir or tempfile.mkdtemp(prefix='shelfscan-'));root.mkdir(parents=True,exist_ok=True);fix=Path(__file__).resolve().parent.parent/'fixtures';db=root/'shelfscan.db'
    initialize(db,fix/'config.json');ingest(fix/'events.jsonl',db);evaluate(db);return report(db,root/'report.json')
def main():
    p=argparse.ArgumentParser(description='离线馆藏 RFID 盘点');s=p.add_subparsers(dest='command',required=True)
    d=s.add_parser('demo');d.add_argument('--workdir')
    i=s.add_parser('init');i.add_argument('--db',required=True);i.add_argument('--config',required=True)
    a=s.add_parser('ingest');a.add_argument('file');a.add_argument('--db',required=True)
    e=s.add_parser('evaluate');e.add_argument('--db',required=True)
    r=s.add_parser('report');r.add_argument('--db',required=True);r.add_argument('--out',required=True)
    x=s.add_parser('inspect');x.add_argument('--db',required=True)
    cr=s.add_parser('climate-register',help='冻结温控案例并核对真实架位扫描')
    cr.add_argument('--db',required=True);cr.add_argument('--case-id',required=True)
    cr.add_argument('--case',required=True);cr.add_argument('--sensors',required=True)
    cp=s.add_parser('climate-plan',help='精确最优冷却计划')
    cp.add_argument('--db',required=True);cp.add_argument('--case-id',required=True)
    cp.add_argument('--objective',required=True,choices=('min_energy','min_max_shelf_exposure'))
    cf=s.add_parser('climate-feedback',help='迟到真实读数：冻结已执行时段并重求剩余计划')
    cf.add_argument('--db',required=True);cf.add_argument('--case-id',required=True)
    cf.add_argument('--base-version',required=True);cf.add_argument('--objective',required=True,
                     choices=('min_energy','min_max_shelf_exposure'))
    cf.add_argument('--readings',required=True);cf.add_argument('--through-period',required=True,type=int)
    cf.add_argument('--new-version')
    cw=s.add_parser('climate-report',help='输出冷却命令与保存风险 JSON/Markdown')
    cw.add_argument('--db',required=True);cw.add_argument('--out',required=True)
    q=p.parse_args()
    if q.command=='demo':print(demo(q.workdir))
    elif q.command=='init':initialize(q.db,q.config)
    elif q.command=='ingest':print(ingest(q.file,q.db))
    elif q.command=='evaluate':print(evaluate(q.db))
    elif q.command=='report':print(report(q.db,q.out))
    elif q.command=='inspect':
        db=connect(q.db);print(json.dumps([dict(z) for z in db.execute('SELECT * FROM decision')],ensure_ascii=False));db.close()
    elif q.command=='climate-register':
        print(climate_register(q.db,q.case_id,q.case,q.sensors))
    elif q.command=='climate-plan':
        p=climate_plan(q.db,q.case_id,q.objective)
        print(json.dumps({'plan_version':p['plan_version'],'status':p['status'],
                          'totals':p['totals'],
                          'commands':[(c['period'],c['shelf']) for c in p['commands']]},
                         ensure_ascii=False))
    elif q.command=='climate-feedback':
        raw=Path(q.readings).read_bytes()
        payload=apply_feedback(q.db,q.case_id,q.base_version,q.objective,raw,
                               q.through_period,q.new_version)
        print(json.dumps({'plan_version':payload['plan_version'],'status':payload['status'],
                          'totals':payload['totals']},ensure_ascii=False))
    elif q.command=='climate-report':
        print(climate_report(q.db,q.out))
if __name__=='__main__':main()
