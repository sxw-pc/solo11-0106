import argparse,json,tempfile
from pathlib import Path
from .store import connect
from .ingest import ingest
from .rules import evaluate
from .report import report
from .firmware_lab import register
from .firmware_release import plan as fw_plan, run as fw_run
from .firmware_report import write_report
def initialize(db_path,config):
    c=json.loads(Path(config).read_text(encoding='utf-8'));db=connect(db_path)
    with db:
        for table,cols in [('batch',['id','version','start','end','status']),('copy',['id','planned_shelf','loaned','batch']),('reader',['id','shelf','window_start','window_end'])]:
            for row in c[table]:db.execute(f'INSERT OR IGNORE INTO {table} VALUES ({",".join("?" for _ in cols)})',[row[k] for k in cols])
    db.close()
def demo(workdir=None):
    root=Path(workdir or tempfile.mkdtemp(prefix='shelfscan-'));root.mkdir(parents=True,exist_ok=True);fix=Path(__file__).resolve().parent.parent/'fixtures';db=root/'shelfscan.db'
    initialize(db,fix/'config.json');ingest(fix/'events.jsonl',db);evaluate(db);return report(db,root/'report.json')
def firmware_seed(db,config,events,case,case_id='firmware-01'):
    """Initialize inventory, import real scans and register observed readers."""
    initialize(db,config);ingest(events,db)
    return register(db,case_id,case)
def firmware_demo(workdir=None):
    """Run both legal release contracts against the same fault fixture."""
    root=Path(workdir or tempfile.mkdtemp(prefix='shelfscan-fw-'));root.mkdir(parents=True,exist_ok=True)
    fix=Path(__file__).resolve().parent.parent/'fixtures'
    outputs={}
    for contract in ('atomic_fleet','cell_isolated'):
        db=root/f'{contract}.db'
        firmware_seed(db,fix/'firmware-config.json',fix/'firmware-events.jsonl',fix/'firmware-case.json')
        intent_id=f'{contract}-demo'
        fw_plan(db,intent_id,'firmware-01',contract)
        summary=fw_run(db,intent_id)
        out=write_report(db,intent_id,root/f'{contract}-report.json')
        outputs[contract]={'db':str(db),'summary':summary,'report':str(out)}
    return {'workdir':str(root),'contracts':outputs}
def main():
    p=argparse.ArgumentParser(description='离线馆藏 RFID 盘点');s=p.add_subparsers(dest='command',required=True)
    d=s.add_parser('demo');d.add_argument('--workdir')
    i=s.add_parser('init');i.add_argument('--db',required=True);i.add_argument('--config',required=True)
    a=s.add_parser('ingest');a.add_argument('file');a.add_argument('--db',required=True)
    e=s.add_parser('evaluate');e.add_argument('--db',required=True)
    r=s.add_parser('report');r.add_argument('--db',required=True);r.add_argument('--out',required=True)
    x=s.add_parser('inspect');x.add_argument('--db',required=True)
    fs=s.add_parser('fw-seed',help='初始化馆藏/扫描并登记有扫描证据的读写器')
    fs.add_argument('--db',required=True);fs.add_argument('--config',required=True)
    fs.add_argument('--events',required=True);fs.add_argument('--case',required=True)
    fs.add_argument('--case-id',default='firmware-01')
    fp=s.add_parser('fw-plan',help='冻结可重放的升级意图日志')
    fp.add_argument('--db',required=True);fp.add_argument('--intent',required=True)
    fp.add_argument('--case-id',default='firmware-01')
    fp.add_argument('--contract',required=True,choices=('atomic_fleet','cell_isolated'))
    fr=s.add_parser('fw-run',help='执行/恢复升级编排（可重复调用，不重复安装）')
    fr.add_argument('--db',required=True);fr.add_argument('--intent',required=True)
    fpr=s.add_parser('fw-report',help='输出逐设备/单元可核验升级报告')
    fpr.add_argument('--db',required=True);fpr.add_argument('--intent',required=True);fpr.add_argument('--out',required=True)
    fd=s.add_parser('fw-demo',help='同一故障夹具下演示两种发布合同');fd.add_argument('--workdir')
    q=p.parse_args()
    if q.command=='demo':print(demo(q.workdir))
    elif q.command=='init':initialize(q.db,q.config)
    elif q.command=='ingest':print(ingest(q.file,q.db))
    elif q.command=='evaluate':print(evaluate(q.db))
    elif q.command=='report':print(report(q.db,q.out))
    elif q.command=='inspect':
        db=connect(q.db);print(json.dumps([dict(z) for z in db.execute('SELECT * FROM decision')],ensure_ascii=False));db.close()
    elif q.command=='fw-seed':print(json.dumps(firmware_seed(q.db,q.config,q.events,q.case,q.case_id),ensure_ascii=False))
    elif q.command=='fw-plan':print(json.dumps(fw_plan(q.db,q.intent,q.case_id,q.contract),ensure_ascii=False))
    elif q.command=='fw-run':print(json.dumps(fw_run(q.db,q.intent),ensure_ascii=False))
    elif q.command=='fw-report':print(str(write_report(q.db,q.intent,q.out)))
    elif q.command=='fw-demo':print(json.dumps(firmware_demo(q.workdir),ensure_ascii=False))
if __name__=='__main__':main()
