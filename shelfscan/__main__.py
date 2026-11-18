import argparse
import json
import tempfile
from pathlib import Path

from .ingest import ingest
from .report import report
from .rules import evaluate
from .store import connect


def initialize(db_path, config):
    c = json.loads(Path(config).read_text(encoding='utf-8'))
    db = connect(db_path)
    with db:
        for table, cols in [('batch', ['id', 'version', 'start', 'end', 'status']),
                            ('copy', ['id', 'planned_shelf', 'loaned', 'batch']),
                            ('reader', ['id', 'shelf', 'window_start', 'window_end'])]:
            for row in c[table]:
                db.execute(f'INSERT OR IGNORE INTO {table} VALUES ({",".join("?" for _ in cols)})',
                           [row[k] for k in cols])
    db.close()


def demo(workdir=None):
    root = Path(workdir or tempfile.mkdtemp(prefix='shelfscan-'))
    root.mkdir(parents=True, exist_ok=True)
    fix = Path(__file__).resolve().parent.parent / 'fixtures'
    db = root / 'shelfscan.db'
    initialize(db, fix / 'config.json')
    ingest(fix / 'events.jsonl', db)
    evaluate(db)
    return report(db, root / 'report.json')


def _label_paths(db, root):
    root = Path(root or Path(db).resolve().parent)
    return root / 'printer.db', root / 'verifier.db'


def label_demo(workdir=None, contract='public_call_number'):
    """End-to-end physical label demo for the frozen two-copy case.

    Executes one pre-authorised contract, hits the injected
    paper-emitted-before-ACK fault, recovers from the immutable device journals
    and leaves the alternate contract as an unexecuted comparison.
    """
    from . import labels
    from .devices import SimPrinter, SimVerifier

    root = Path(workdir or tempfile.mkdtemp(prefix='shelfscan-label-'))
    root.mkdir(parents=True, exist_ok=True)
    fix = Path(__file__).resolve().parent.parent / 'fixtures'
    db = root / 'shelfscan.db'
    initialize(db, fix / 'config.json')
    ingest(fix / 'events.jsonl', db)
    evaluate(db)
    labels.register(db, 'label-01', fix / 'label-case.json')
    printer_path, verifier_path = _label_paths(db, root)
    verifier = SimVerifier(verifier_path)
    job = labels.open_job(db, 'label-01', contract)
    run = labels.run_job(db, job['job_id'], printer_path, verifier=verifier)
    printer = SimPrinter(printer_path)
    emission = printer.emission(run.get('serial', ''))
    recovery = None
    if run['status'] == 'interrupted' and emission is not None:
        # Staff re-read the sheet that did come out while attached to its copy.
        verifier.record_read(emission['copy_id'], emission['serial'], emission['paper'])
        recovery = labels.recover_job(db, job['job_id'], printer_path,
                                      {emission['serial']: {'state': 'attached_ok'}},
                                      verifier=verifier)
    out = report(db, root / 'report.json', printer_path=printer_path,
                 verifier_path=verifier_path)
    return {'report': out, 'job': job['job_id'], 'contract': contract,
            'devices': {'printer': printer_path, 'verifier': verifier_path},
            'run': run, 'recovery': recovery}


def _label_cli(argv):
    from . import labels
    from .devices import SimPrinter, SimVerifier

    p = argparse.ArgumentParser(prog='shelfscan label')
    sp = p.add_subparsers(dest='op', required=True)
    g = sp.add_parser('register')
    g.add_argument('--db', required=True); g.add_argument('--case', required=True); g.add_argument('--fixture', required=True)
    g = sp.add_parser('open')
    g.add_argument('--db', required=True); g.add_argument('--case', required=True)
    g.add_argument('--contract', required=True, choices=('public_call_number', 'opaque_tag'))
    g = sp.add_parser('run')
    g.add_argument('--db', required=True); g.add_argument('--job', type=int, required=True)
    g.add_argument('--devices', required=True)
    g.add_argument('--no-fault', action='store_true')
    g = sp.add_parser('recover')
    g.add_argument('--db', required=True); g.add_argument('--job', type=int, required=True)
    g.add_argument('--devices', required=True); g.add_argument('--findings', required=True)
    g = sp.add_parser('verify')
    g.add_argument('--db', required=True); g.add_argument('--serial', required=True)
    g.add_argument('--copy', required=True); g.add_argument('--devices', required=True)
    g = sp.add_parser('reissue')
    g.add_argument('--db', required=True); g.add_argument('--case', required=True)
    g.add_argument('--copy', required=True); g.add_argument('--devices', required=True)
    g = sp.add_parser('template')
    g.add_argument('--db', required=True); g.add_argument('--case', required=True)
    g.add_argument('--version', required=True)
    g = sp.add_parser('report')
    g.add_argument('--db', required=True); g.add_argument('--out', required=True)
    g.add_argument('--devices')
    g = sp.add_parser('demo')
    g.add_argument('--workdir'); g.add_argument('--contract', default='public_call_number')
    q = p.parse_args(argv)

    def devs(path):
        root = Path(path)
        return root / 'printer.db', root / 'verifier.db'

    if q.op == 'register':
        print(json.dumps(labels.register(q.db, q.case, q.fixture), ensure_ascii=False))
    elif q.op == 'open':
        print(json.dumps(labels.open_job(q.db, q.case, q.contract), ensure_ascii=False))
    elif q.op == 'run':
        printer_path, verifier_path = devs(q.devices)
        print(json.dumps(labels.run_job(q.db, q.job, printer_path,
                                        verifier=SimVerifier(verifier_path),
                                        inject_fault=not q.no_fault),
                         ensure_ascii=False, default=str))
    elif q.op == 'recover':
        printer_path, verifier_path = devs(q.devices)
        findings = json.loads(Path(q.findings).read_text(encoding='utf-8'))
        print(json.dumps(labels.recover_job(q.db, q.job, printer_path, findings,
                                            verifier=SimVerifier(verifier_path)),
                         ensure_ascii=False, default=str))
    elif q.op == 'verify':
        printer_path, verifier_path = devs(q.devices)
        emission = SimPrinter(printer_path).emission(q.serial)
        if emission is None:
            raise SystemExit('unknown_emission')
        print(json.dumps(labels.verify_label(q.db, q.serial, q.copy, q.serial,
                                             emission['paper'],
                                             verifier=SimVerifier(verifier_path)),
                         ensure_ascii=False, default=str))
    elif q.op == 'reissue':
        printer_path, verifier_path = devs(q.devices)
        print(json.dumps(labels.reissue_copy(q.db, q.case, q.copy, printer_path,
                                             verifier=SimVerifier(verifier_path)),
                         ensure_ascii=False, default=str))
    elif q.op == 'template':
        print(json.dumps(labels.set_template(q.db, q.case, q.version), ensure_ascii=False))
    elif q.op == 'report':
        printer_path, verifier_path = devs(q.devices) if q.devices else (None, None)
        print(report(q.db, q.out, printer_path=printer_path, verifier_path=verifier_path))
    elif q.op == 'demo':
        result = label_demo(q.workdir, q.contract)
        print(json.dumps({k: str(v) if isinstance(v, Path) else v for k, v in result.items()},
                         ensure_ascii=False, default=str))


def main():
    p = argparse.ArgumentParser(description='离线馆藏 RFID 盘点')
    s = p.add_subparsers(dest='command', required=True)
    d = s.add_parser('demo'); d.add_argument('--workdir')
    i = s.add_parser('init'); i.add_argument('--db', required=True); i.add_argument('--config', required=True)
    a = s.add_parser('ingest'); a.add_argument('file'); a.add_argument('--db', required=True)
    e = s.add_parser('evaluate'); e.add_argument('--db', required=True)
    r = s.add_parser('report'); r.add_argument('--db', required=True); r.add_argument('--out', required=True)
    x = s.add_parser('inspect'); x.add_argument('--db', required=True)
    l = s.add_parser('label', help='馆藏实体标签制发')
    l.add_argument('args', nargs=argparse.REMAINDER)
    q = p.parse_args()
    if q.command == 'label':
        _label_cli(q.args)
    elif q.command == 'demo':
        print(demo(q.workdir))
    elif q.command == 'init':
        initialize(q.db, q.config)
    elif q.command == 'ingest':
        print(ingest(q.file, q.db))
    elif q.command == 'evaluate':
        print(evaluate(q.db))
    elif q.command == 'report':
        print(report(q.db, q.out))
    elif q.command == 'inspect':
        db = connect(q.db)
        print(json.dumps([dict(z) for z in db.execute('SELECT * FROM decision')], ensure_ascii=False))
        db.close()


if __name__ == '__main__':
    main()
