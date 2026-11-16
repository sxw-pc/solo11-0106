from shelfscan.__main__ import demo
from shelfscan.store import connect
import json
def test_evidence_and_run(tmp_path):
    p=demo(tmp_path);data=json.loads(p.read_text(encoding='utf-8'))
    assert len(data['evidence'])==2
    assert len(data['run'])==1 and data['run'][0]['status'] in ('已判定','待核')
    db=connect(tmp_path/'shelfscan.db')
    assert db.execute('PRAGMA foreign_key_check').fetchall()==[]
    assert db.execute('SELECT count(*) FROM evidence').fetchone()[0]==2
    db.close()
def test_rerun_keeps_evidence_and_run_history(tmp_path):
    demo(tmp_path)
    from shelfscan.rules import evaluate
    evaluate(tmp_path/'shelfscan.db')
    db=connect(tmp_path/'shelfscan.db')
    assert db.execute('SELECT count(*) FROM evidence').fetchone()[0]==2
    assert db.execute('SELECT count(*) FROM run').fetchone()[0]==2
    db.close()
