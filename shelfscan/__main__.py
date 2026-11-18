import argparse,json,tempfile
from pathlib import Path
from .store import connect
from .ingest import ingest
from .rules import evaluate
from .report import report
from . import archive
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
    ar=s.add_parser('archive');ar.add_argument('--db',required=True)
    sub=ar.add_subparsers(dest='archive_command',required=True)
    rg=sub.add_parser('register');rg.add_argument('--case-id',required=True);rg.add_argument('--fixture',required=True)
    mg=sub.add_parser('migrate');mg.add_argument('--rep',choices=archive.REPS,required=True);mg.add_argument('--source-id',type=int)
    vf=sub.add_parser('verify');vf.add_argument('--source-id',type=int)
    gc_=sub.add_parser('gc');gc_.add_argument('--vacuum',action='store_true')
    sub.add_parser('stats')
    ct=sub.add_parser('cat');ct.add_argument('--source-id',type=int,required=True);ct.add_argument('--out',required=True)
    sub.add_parser('pins')
    q=p.parse_args()
    if q.command=='demo':print(demo(q.workdir))
    elif q.command=='init':initialize(q.db,q.config)
    elif q.command=='ingest':print(ingest(q.file,q.db))
    elif q.command=='evaluate':print(evaluate(q.db))
    elif q.command=='report':print(report(q.db,q.out))
    elif q.command=='inspect':
        db=connect(q.db);print(json.dumps([dict(z) for z in db.execute('SELECT * FROM decision')],ensure_ascii=False));db.close()
    elif q.command=='archive':
        c=q.archive_command
        if c=='register':print(json.dumps(archive.register(q.db,q.case_id,q.fixture),ensure_ascii=False))
        elif c=='migrate':
            if q.source_id is not None:r=[archive.migrate_source(q.db,q.source_id,q.rep)]
            else:r=archive.migrate_all(q.db,q.rep)
            print(json.dumps(r,ensure_ascii=False))
        elif c=='verify':
            print(json.dumps(archive.verify_source(q.db,q.source_id) if q.source_id is not None else archive.verify_all(q.db),ensure_ascii=False))
        elif c=='gc':print(json.dumps(archive.gc(q.db,vacuum=q.vacuum),ensure_ascii=False))
        elif c=='stats':print(json.dumps(archive.archive_stats(q.db),ensure_ascii=False))
        elif c=='cat':
            db=connect(q.db)
            try:data=archive.read_source(db,q.source_id)
            finally:db.close()
            Path(q.out).write_bytes(data);print(json.dumps({'source_id':q.source_id,'byte_length':len(data),'out':q.out}))
        elif c=='pins':print(json.dumps(archive.list_pins(q.db),ensure_ascii=False))
if __name__=='__main__':main()
