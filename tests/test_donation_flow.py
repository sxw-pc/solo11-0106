"""捐赠接收、权利确认与入藏全流程测试。

覆盖：双路径产权差异、重复读、copy 碰撞、漏签、封条破损/不一致、授权撤回、
双馆员竞争、确认与编目间中断恢复、重复确认、旧批重算不回溯、补偿。
"""
import json
import sqlite3
import threading
from pathlib import Path

import pytest

from shelfscan.__main__ import initialize, donation_demo
from shelfscan.ingest import ingest
from shelfscan.rules import evaluate
from shelfscan.donations import register
from shelfscan import accession
from shelfscan.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'
CASE = 'donation-01'
NEXT_BATCH = ('INV-NEXT', 'S1', '2030-06-02T00:00:00Z', '2031-06-01T00:00:00Z')


def seed(tmp_path, case_file='donation-case.json', case_id=CASE, extra_events=()):
    db = tmp_path / 'donation.db'
    initialize(db, FIX / 'donation-config.json')
    rows = [json.loads(l) for l in (FIX / 'donation-events.jsonl').read_text().splitlines()]
    rows.extend(extra_events)
    p = tmp_path / 'events.jsonl'
    p.write_text('\n'.join(json.dumps(r) for r in rows), encoding='utf-8')
    ingest(p, db)
    evaluate(db)
    register(db, case_id, FIX / case_file)
    return db


def run_gift(db, case_id=CASE, librarian='LIB-B-l1'):
    accession.verify(db, case_id)
    accession.confirm(db, case_id, librarian, 'outright_gift')
    accession.register_rights(db, case_id)
    accession.build_snapshot(db, case_id, *NEXT_BATCH)
    accession.issue_voucher(db, case_id)


def run_deposit(db, case_id=CASE):
    accession.verify(db, case_id)
    accession.confirm(db, case_id, 'LIB-B-l1', 'long_term_deposit')
    accession.register_rights(db, case_id)
    accession.build_snapshot(db, case_id, *NEXT_BATCH)
    accession.issue_voucher(db, case_id)


# ---------- 两条合法路径产生互斥的产权结果 ----------

def test_gift_path_owner_is_library_and_circulates_after_cataloging(tmp_path):
    db = seed(tmp_path)
    acc = accession.verify(db, CASE)
    assert acc['stage'] == '待确认' and acc['custodian'] == 'LIB-B'
    accession.confirm(db, CASE, 'LIB-B-l1', 'outright_gift')
    holding = accession.register_rights(db, CASE)
    # 编目前禁止流通，不能直接进入普通流通
    assert holding['owner'] == 'LIB-B'
    assert holding['catalog_status'] == '待编目' and holding['circulation'] == '禁止流通'
    snap = accession.build_snapshot(db, CASE, *NEXT_BATCH)
    assert snap['cataloged'] is True
    voucher = accession.issue_voucher(db, CASE)
    c = connect(db)
    try:
        assert voucher['kind'] == '赠与证明' and voucher['owner'] == 'LIB-B'
        assert dict(c.execute("SELECT * FROM copy WHERE id='CNEW'").fetchone())['batch'] == 'INV-NEXT'
        st = accession.status(db, CASE)
        assert st['stage'] == '凭证已出具' and st['circulation'] == '普通流通'
        assert st['return_by'] is None
        assert c.execute('SELECT COUNT(*) FROM voucher').fetchone()[0] == 1
    finally:
        c.close()


def test_deposit_path_donor_keeps_ownership_reading_room_only_with_return_duty(tmp_path):
    db = seed(tmp_path)
    run_deposit(db)
    c = connect(db)
    try:
        # 寄存物不得登记为可外借的自有 copy
        assert c.execute("SELECT 1 FROM copy WHERE id='CNEW'").fetchone() is None
        h = dict(c.execute("SELECT * FROM holding WHERE case_id=?", (CASE,)).fetchone())
        assert h['owner'] == 'LI-WEN' and h['circulation'] == '仅限阅览室'
        assert h['catalog_status'] == '不适用' and h['batch_id'] == 'INV-NEXT'
        v = dict(c.execute('SELECT * FROM voucher WHERE case_id=?', (CASE,)).fetchone())
        assert v['kind'] == '寄存返还单'
        assert v['return_by'] == '2031-06-01T00:00:00Z'
    finally:
        c.close()


def test_two_paths_cannot_both_emit_vouchers(tmp_path):
    db = seed(tmp_path)
    run_gift(db)
    with pytest.raises(ValueError, match='legal_path_conflict'):
        accession.confirm(db, CASE, 'LIB-B-l2', 'long_term_deposit')
    c = connect(db)
    try:
        assert c.execute('SELECT COUNT(*) FROM voucher').fetchone()[0] == 1
        assert c.execute("SELECT kind FROM voucher").fetchone()[0] == '赠与证明'
    finally:
        c.close()


# ---------- 核对闸门：禁止自动转馆藏 ----------

def test_broken_seal_blocks_acceptance(tmp_path):
    db = seed(tmp_path, case_file='donation-case-broken-seal.json')
    acc = accession.verify(db, CASE)
    assert acc['stage'] == '争议' and acc['dispute_code'] == 'broken_seal'
    with pytest.raises(ValueError, match='acceptance_stage_争议'):
        accession.confirm(db, CASE, 'LIB-B-l1', 'outright_gift')
    c = connect(db)
    try:
        assert c.execute('SELECT 1 FROM holding WHERE tag=?', ('CNEW',)).fetchone() is None
        assert c.execute('SELECT 1 FROM copy WHERE id=?', ('CNEW',)).fetchone() is None
    finally:
        c.close()


def test_seal_mismatch_blocks_acceptance(tmp_path):
    db = seed(tmp_path, case_file='donation-case-seal-mismatch.json')
    acc = accession.verify(db, CASE)
    assert acc['dispute_code'] == 'seal_mismatch'


def test_unsigned_handoff_blocks_acceptance(tmp_path):
    db = seed(tmp_path, case_file='donation-case-unsigned.json')
    acc = accession.verify(db, CASE)
    assert acc['stage'] == '争议' and acc['dispute_code'] == 'custody_unsigned'


def test_tag_colliding_with_frozen_copy_never_accepted(tmp_path):
    db = seed(tmp_path)
    c = connect(db)
    # 既有冻结 copy 与未知标签同号
    c.execute("INSERT INTO batch VALUES ('INV-OLD','v1','2029-01-01T00:00:00Z','2029-01-02T00:00:00Z','已判定')")
    c.execute("INSERT INTO copy VALUES ('CNEW','S9',0,'INV-OLD')")
    c.commit(); c.close()
    acc = accession.verify(db, CASE)
    assert acc['dispute_code'] == 'copy_collision'
    with pytest.raises(ValueError, match='acceptance_stage_争议'):
        accession.confirm(db, CASE, 'LIB-B-l1', 'outright_gift')


def test_repeated_reads_of_same_tag_keep_single_unknown_evidence(tmp_path):
    dup = {'kind': 'scan', 'device': 'R1', 'seq': 3, 'copy_id': 'CNEW',
           'at': '2030-06-01T09:55:00Z', 'shelf': 'S1', 'signal': 88}
    db = seed(tmp_path, extra_events=[dup])
    c = connect(db)
    try:
        assert c.execute("SELECT COUNT(*) FROM event WHERE copy_id='CNEW'").fetchone()[0] == 2
        assert c.execute("SELECT COUNT(*) FROM issue WHERE code='unexpected_copy' "
                         "AND copy_id='CNEW'").fetchone()[0] == 2
    finally:
        c.close()
    acc = accession.verify(db, CASE)
    assert acc['stage'] == '待确认'  # 同标签重复读不阻碍接收，证据全部保留


def test_verify_requires_frozen_unknown_scan_evidence(tmp_path):
    db = tmp_path / 'no-scan.db'
    initialize(db, FIX / 'donation-config.json')
    ingest(FIX / 'donation-events.jsonl', db)
    evaluate(db)
    # 没有未知标签读数时，案件登记本身被拒绝
    c = connect(db)
    c.execute("DELETE FROM issue WHERE event_id IN (SELECT id FROM event WHERE copy_id='CNEW')")
    c.execute("DELETE FROM evidence WHERE event_id IN (SELECT id FROM event WHERE copy_id='CNEW')")
    c.execute("DELETE FROM event WHERE copy_id='CNEW'")
    c.commit(); c.close()
    with pytest.raises(ValueError, match='donation_scan_missing'):
        register(db, CASE, FIX / 'donation-case.json')

    # 读数存在但证据身份被破坏时，核对闸门进入争议而非入藏
    db2 = seed(tmp_path / 'sub')
    c = connect(db2)
    c.execute("DELETE FROM evidence WHERE event_id IN (SELECT id FROM event WHERE copy_id='CNEW')")
    c.commit(); c.close()
    acc = accession.verify(db2, CASE)
    assert acc['stage'] == '争议' and acc['dispute_code'] == 'tag_evidence_mismatch'


# ---------- 授权撤回 ----------

def test_donor_revocation_before_confirm_blocks_and_marks_withdrawn(tmp_path):
    db = seed(tmp_path)
    accession.verify(db, CASE)
    out = accession.revoke_authorization(db, 'DEED-1', 1)
    assert out['status'] == '已撤回'
    with pytest.raises(ValueError, match='authorization_revoked|acceptance_stage_已撤回'):
        accession.confirm(db, CASE, 'LIB-B-l1', 'outright_gift')
    acc = dict(connect(db).execute("SELECT * FROM acceptance WHERE case_id=?", (CASE,)).fetchone())
    assert acc['stage'] == '已撤回' and acc['dispute_code'] == 'authorization_revoked'


def test_revocation_after_confirm_is_rejected(tmp_path):
    db = seed(tmp_path)
    accession.verify(db, CASE)
    accession.confirm(db, CASE, 'LIB-B-l1', 'outright_gift')
    with pytest.raises(ValueError, match='authorization_locked_after_acceptance'):
        accession.revoke_authorization(db, 'DEED-1', 1)


def test_path_not_listed_in_deed_is_rejected(tmp_path):
    doc = json.loads((FIX / 'donation-case.json').read_text(encoding='utf-8'))
    doc['deed']['authorized_options'] = ['long_term_deposit']
    case_file = tmp_path / 'case-restricted.json'
    case_file.write_text(json.dumps(doc, ensure_ascii=False), encoding='utf-8')
    db = tmp_path / 'donation.db'
    initialize(db, FIX / 'donation-config.json')
    ingest(FIX / 'donation-events.jsonl', db)
    evaluate(db)
    register(db, CASE, case_file)
    accession.verify(db, CASE)
    with pytest.raises(ValueError, match='path_not_authorized'):
        accession.confirm(db, CASE, 'LIB-B-l1', 'outright_gift')


# ---------- 双馆员竞争 ----------

def test_two_librarians_concurrent_different_paths_only_one_wins(tmp_path):
    db = seed(tmp_path)
    accession.verify(db, CASE)
    errors = []

    def choose(path):
        try:
            accession.confirm(db, CASE, f'LIB-B-{path}', path)
        except ValueError as exc:
            errors.append(str(exc))

    t1 = threading.Thread(target=choose, args=('outright_gift',))
    t2 = threading.Thread(target=choose, args=('long_term_deposit',))
    t1.start(); t2.start(); t1.join(); t2.join()
    c = connect(db)
    try:
        rows = c.execute("SELECT path,stage,COUNT(*) FROM acceptance GROUP BY path,stage").fetchall()
        assert len(rows) == 1  # 只有一种产权状态落库
        assert c.execute('SELECT COUNT(*) FROM acceptance').fetchone()[0] == 1
    finally:
        c.close()
    assert any('conflict' in e or 'acceptance_stage_已确认' in e for e in errors)


# ---------- 中断恢复与幂等 ----------

def test_interruption_after_confirm_resumes_same_path(tmp_path):
    db = seed(tmp_path)
    accession.verify(db, CASE)
    accession.confirm(db, CASE, 'LIB-B-l1', 'outright_gift')
    # 进程中断：重启后续跑，每个阶段幂等
    same = accession.confirm(db, CASE, 'LIB-B-l1', 'outright_gift')
    assert same['stage'] == '已确认' and same['path'] == 'outright_gift'
    accession.register_rights(db, CASE)
    accession.register_rights(db, CASE)  # 重复调用不产生第二条 holding
    accession.build_snapshot(db, CASE, *NEXT_BATCH)
    accession.build_snapshot(db, CASE, *NEXT_BATCH)  # 幂等，不重复建 copy/batch
    accession.issue_voucher(db, CASE)
    accession.issue_voucher(db, CASE)
    c = connect(db)
    try:
        assert c.execute('SELECT COUNT(*) FROM holding').fetchone()[0] == 1
        assert c.execute("SELECT COUNT(*) FROM copy WHERE id='CNEW'").fetchone()[0] == 1
        assert c.execute('SELECT COUNT(*) FROM voucher').fetchone()[0] == 1
    finally:
        c.close()


def test_catalog_write_failure_leaves_recoverable_stage_no_half_state(tmp_path):
    db = seed(tmp_path)
    accession.verify(db, CASE)
    accession.confirm(db, CASE, 'LIB-B-l1', 'outright_gift')
    accession.register_rights(db, CASE)
    c = connect(db)
    c.execute("CREATE TRIGGER reject_copy BEFORE INSERT ON copy "
              "BEGIN SELECT RAISE(ABORT,'catalog failure'); END")
    c.commit(); c.close()
    with pytest.raises(sqlite3.IntegrityError):
        accession.build_snapshot(db, CASE, *NEXT_BATCH)
    c = connect(db)
    try:
        # 阶段停在权属已登记：无半成品 copy，holding 仍待编目
        assert c.execute("SELECT stage FROM acceptance WHERE case_id=?", (CASE,)).fetchone()[0] \
            == '权属已登记'
        assert c.execute("SELECT 1 FROM copy WHERE id='CNEW'").fetchone() is None
        assert dict(c.execute("SELECT * FROM holding WHERE case_id=?", (CASE,)).fetchone())[
            'catalog_status'] == '待编目'
        c.execute('DROP TRIGGER reject_copy'); c.commit()
    finally:
        c.close()
    # 修复后续跑成功
    accession.build_snapshot(db, CASE, *NEXT_BATCH)
    accession.issue_voucher(db, CASE)
    assert accession.status(db, CASE)['stage'] == '凭证已出具'


def test_duplicate_confirm_same_path_is_idempotent(tmp_path):
    db = seed(tmp_path)
    accession.verify(db, CASE)
    first = accession.confirm(db, CASE, 'LIB-B-l1', 'long_term_deposit')
    second = accession.confirm(db, CASE, 'LIB-B-l2', 'long_term_deposit')
    assert first['path'] == second['path'] and first['librarian'] == second['librarian']
    assert connect(db).execute('SELECT COUNT(*) FROM acceptance').fetchone()[0] == 1


def test_compensation_after_confirm_reverts_rights_but_keeps_evidence(tmp_path):
    db = seed(tmp_path)
    accession.verify(db, CASE)
    accession.confirm(db, CASE, 'LIB-B-l1', 'outright_gift')
    accession.register_rights(db, CASE)
    accession.build_snapshot(db, CASE, *NEXT_BATCH)
    out = accession.compensate(db, CASE)
    assert out['stage'] == '已补偿' and out['path'] is None
    c = connect(db)
    try:
        assert c.execute('SELECT 1 FROM holding WHERE case_id=?', (CASE,)).fetchone() is None
        assert c.execute("SELECT 1 FROM copy WHERE id='CNEW'").fetchone() is None
        assert c.execute("SELECT COUNT(*) FROM issue WHERE code='unexpected_copy' "
                         "AND copy_id='CNEW'").fetchone()[0] >= 1
        assert c.execute('SELECT COUNT(*) FROM stage_log WHERE case_id=?', (CASE,)).fetchone()[0] >= 1
    finally:
        c.close()


def test_compensation_blocked_after_voucher(tmp_path):
    db = seed(tmp_path)
    run_gift(db)
    with pytest.raises(ValueError, match='compensation_locked_voucher_issued'):
        accession.compensate(db, CASE)


# ---------- 旧批重算不回溯 ----------

def test_recompute_old_batch_keeps_historical_unexpected_copy(tmp_path):
    db = seed(tmp_path)
    c = connect(db)
    before = c.execute("SELECT COUNT(*) FROM issue WHERE code='unexpected_copy' "
                       "AND copy_id='CNEW'").fetchone()[0]
    runs_before = c.execute('SELECT COUNT(*) FROM run').fetchone()[0]
    c.close()
    evaluate(db); evaluate(db)  # 旧批次重算
    c = connect(db)
    try:
        assert c.execute("SELECT COUNT(*) FROM issue WHERE code='unexpected_copy' "
                         "AND copy_id='CNEW'").fetchone()[0] == before
        assert c.execute('SELECT COUNT(*) FROM run').fetchone()[0] == runs_before + 2
        # 每次 run 的 decision 历史都在
        assert c.execute('SELECT COUNT(DISTINCT run_id) FROM decision_history').fetchone()[0] == 3
        # 未接收的 CNEW 始终不在 copy 快照
        assert c.execute("SELECT 1 FROM copy WHERE id='CNEW'").fetchone() is None
    finally:
        c.close()


def test_acceptance_does_not_erase_old_evidence_and_report_contains_it(tmp_path):
    db = seed(tmp_path)
    run_gift(db)
    st = accession.status(db, CASE)
    assert len(st['unexpected_copy_evidence']) == 1
    assert st['unexpected_copy_evidence'][0]['run_id'] == 1
    out = (tmp_path / 'report.json')
    from shelfscan.report import report
    report(db, out)
    data = json.loads(out.read_text(encoding='utf-8'))
    assert any(i['code'] == 'unexpected_copy' and i['copy_id'] == 'CNEW' for i in data['issue'])
    assert data['acceptance'][0]['owner'] == 'LIB-B'
    assert data['voucher'][0]['kind'] == '赠与证明'


# ---------- CLI demo 端到端 ----------

def test_donation_demo_gift_and_deposit_end_to_end(tmp_path):
    gift = donation_demo(tmp_path / 'gift', 'outright_gift')
    assert gift['stage'] == '凭证已出具' and gift['owner'] == 'LIB-B'
    dep = donation_demo(tmp_path / 'dep', 'long_term_deposit')
    assert dep['stage'] == '凭证已出具' and dep['owner'] == 'LI-WEN'
    assert dep['circulation'] == '仅限阅览室'
    assert dep['return_by'] == '2031-06-01T00:00:00Z'
    assert dep['unexpected_copy_evidence']  # INV-G 原始未知标签证据仍在
