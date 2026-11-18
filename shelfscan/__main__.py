import argparse,json,tempfile
from pathlib import Path
from .store import connect
from .ingest import ingest
from .rules import evaluate
from .report import report
from .donations import register
from . import accession
def initialize(db_path,config):
    c=json.loads(Path(config).read_text(encoding='utf-8'));db=connect(db_path)
    with db:
        for table,cols in [('batch',['id','version','start','end','status']),('copy',['id','planned_shelf','loaned','batch']),('reader',['id','shelf','window_start','window_end'])]:
            for row in c[table]:db.execute(f'INSERT OR IGNORE INTO {table} VALUES ({",".join("?" for _ in cols)})',[row[k] for k in cols])
    db.close()
def demo(workdir=None):
    root=Path(workdir or tempfile.mkdtemp(prefix='shelfscan-'));root.mkdir(parents=True,exist_ok=True);fix=Path(__file__).resolve().parent.parent/'fixtures';db=root/'shelfscan.db'
    initialize(db,fix/'config.json');ingest(fix/'events.jsonl',db);evaluate(db);return report(db,root/'report.json')
def donation_demo(workdir=None,path='outright_gift'):
    """跑通捐赠接收→权利确认→权属登记→下一批快照→凭证，输出可观察状态。"""
    root=Path(workdir or tempfile.mkdtemp(prefix='shelfscan-donation-'));root.mkdir(parents=True,exist_ok=True)
    fix=Path(__file__).resolve().parent.parent/'fixtures';db=root/'shelfscan.db'
    initialize(db,fix/'donation-config.json')
    ingest(fix/'donation-events.jsonl',db)
    evaluate(db)
    register(db,'donation-01',fix/'donation-case.json')
    accession.verify(db,'donation-01')
    accession.confirm(db,'donation-01','LIB-B-librarian-1',path)
    accession.register_rights(db,'donation-01')
    accession.build_snapshot(db,'donation-01','INV-NEXT','S1',
                             '2030-06-02T00:00:00Z','2031-06-01T00:00:00Z')
    accession.issue_voucher(db,'donation-01')
    report(db,root/'report.json')
    return accession.status(db,'donation-01')
def main():
    p=argparse.ArgumentParser(description='离线馆藏 RFID 盘点');s=p.add_subparsers(dest='command',required=True)
    d=s.add_parser('demo');d.add_argument('--workdir')
    dd=s.add_parser('donation-demo');dd.add_argument('--workdir');dd.add_argument('--path',choices=accession.PATHS,default='outright_gift')
    i=s.add_parser('init');i.add_argument('--db',required=True);i.add_argument('--config',required=True)
    a=s.add_parser('ingest');a.add_argument('file');a.add_argument('--db',required=True)
    e=s.add_parser('evaluate');e.add_argument('--db',required=True)
    r=s.add_parser('report');r.add_argument('--db',required=True);r.add_argument('--out',required=True)
    x=s.add_parser('inspect');x.add_argument('--db',required=True)
    reg=s.add_parser('donation-register');reg.add_argument('case_id');reg.add_argument('fixture');reg.add_argument('--db',required=True)
    v=s.add_parser('donation-verify');v.add_argument('case_id');v.add_argument('--db',required=True)
    rv=s.add_parser('authorization-revoke');rv.add_argument('deed_id');rv.add_argument('version',type=int);rv.add_argument('--db',required=True)
    cf=s.add_parser('donation-confirm');cf.add_argument('case_id');cf.add_argument('librarian');cf.add_argument('path',choices=accession.PATHS);cf.add_argument('--db',required=True)
    rr=s.add_parser('donation-rights');rr.add_argument('case_id');rr.add_argument('--db',required=True)
    sn=s.add_parser('donation-snapshot');sn.add_argument('case_id');sn.add_argument('batch_id');sn.add_argument('planned_shelf');sn.add_argument('start');sn.add_argument('end');sn.add_argument('--version',default='v1');sn.add_argument('--db',required=True)
    vo=s.add_parser('donation-voucher');vo.add_argument('case_id');vo.add_argument('--voucher-id');vo.add_argument('--db',required=True)
    cp=s.add_parser('donation-compensate');cp.add_argument('case_id');cp.add_argument('--db',required=True)
    st=s.add_parser('donation-status');st.add_argument('case_id');st.add_argument('--db',required=True)
    q=p.parse_args()
    if q.command=='demo':print(demo(q.workdir))
    elif q.command=='donation-demo':print(json.dumps(donation_demo(q.workdir,q.path),ensure_ascii=False,indent=2))
    elif q.command=='init':initialize(q.db,q.config)
    elif q.command=='ingest':print(json.dumps(ingest(q.file,q.db),ensure_ascii=False))
    elif q.command=='evaluate':print(json.dumps(evaluate(q.db),ensure_ascii=False))
    elif q.command=='report':print(report(q.db,q.out))
    elif q.command=='inspect':
        db=connect(q.db);print(json.dumps([dict(z) for z in db.execute('SELECT * FROM decision')],ensure_ascii=False));db.close()
    elif q.command=='donation-register':print(json.dumps(register(q.db,q.case_id,q.fixture),ensure_ascii=False))
    elif q.command=='donation-verify':print(json.dumps(accession.verify(q.db,q.case_id),ensure_ascii=False,default=str))
    elif q.command=='authorization-revoke':print(json.dumps(accession.revoke_authorization(q.db,q.deed_id,q.version),ensure_ascii=False))
    elif q.command=='donation-confirm':print(json.dumps(accession.confirm(q.db,q.case_id,q.librarian,q.path),ensure_ascii=False,default=str))
    elif q.command=='donation-rights':print(json.dumps(accession.register_rights(q.db,q.case_id),ensure_ascii=False,default=str))
    elif q.command=='donation-snapshot':print(json.dumps(accession.build_snapshot(q.db,q.case_id,q.batch_id,q.planned_shelf,q.start,q.end,q.version),ensure_ascii=False))
    elif q.command=='donation-voucher':print(json.dumps(accession.issue_voucher(q.db,q.case_id,q.voucher_id),ensure_ascii=False,default=str))
    elif q.command=='donation-compensate':print(json.dumps(accession.compensate(q.db,q.case_id),ensure_ascii=False,default=str))
    elif q.command=='donation-status':print(json.dumps(accession.status(q.db,q.case_id),ensure_ascii=False,indent=2,default=str))
if __name__=='__main__':
    import sys
    try:
        main()
    except ValueError as exc:
        print(json.dumps({'error': str(exc)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(2)
