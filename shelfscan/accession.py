"""捐赠接收、权利确认与下一馆藏快照入藏流程。

阶段（acceptance.stage，持久且可恢复）：
  待核对 → 待确认 → 已确认 → 权属已登记 → 快照已生成 → 凭证已出具
                 ↘ 争议 / 已撤回            （中断后只续跑同一路径，或补偿→已补偿）

两条互斥法律路径（同一 DEED、同一封条、同一签署资料，只能择一）：
  outright_gift     无条件赠与：所有权转图书馆，编目完成后方可普通流通
  long_term_deposit 一年期寄存：LI-WEN 保留所有权，仅限阅览室，2031-06-01 前返还
"""
import json
import sqlite3
from datetime import datetime, timezone
from .store import connect

PATHS = ('outright_gift', 'long_term_deposit')
PATH_CN = {'outright_gift': '无条件赠与', 'long_term_deposit': '一年期寄存借展'}
# 确认之前的阶段允许重新核对/撤回；确认之后产权路径冻结
PRE_CONFIRM_STAGES = ('待核对', '待确认', '争议', '已撤回')


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _log(db, case_id, stage, event, detail):
    db.execute('INSERT INTO stage_log(case_id,stage,event,detail,at) VALUES (?,?,?,?,?)',
               (case_id, stage, event,
                json.dumps(detail, ensure_ascii=False, sort_keys=True), _now()))


def _load_case(db, case_id):
    row = db.execute('SELECT payload FROM donation_case WHERE id=?', (case_id,)).fetchone()
    if not row:
        raise ValueError('case_not_found')
    return json.loads(row['payload'])


def _upsert_acceptance(db, case_id, tag, deed, seal, custodian, stage, **kw):
    db.execute(
        'INSERT INTO acceptance(case_id,tag,deed_id,deed_version,seal,custodian,stage,updated_at) '
        'VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(case_id) DO UPDATE SET '
        'tag=excluded.tag,deed_id=excluded.deed_id,deed_version=excluded.deed_version,'
        'seal=excluded.seal,custodian=excluded.custodian,stage=excluded.stage,'
        'updated_at=excluded.updated_at',
        (case_id, tag, deed['id'], int(deed['version']), seal, custodian, stage, _now()))
    if kw:
        sets = ','.join(f'{k}=?' for k in kw)
        db.execute(f'UPDATE acceptance SET {sets} WHERE case_id=?',
                   (*kw.values(), case_id))


def _current_custodian(custody):
    for entry in reversed(custody):
        if entry.get('to'):
            return entry['to']
    raise ValueError('custody_gap')


def sync_authorization(db_path, case_id):
    """按 deed 登记授权版本；已撤回的授权不会被同步覆盖回有效。"""
    db = connect(db_path)
    try:
        with db:
            doc = _load_case(db, case_id)['doc']
            deed = doc['deed']
            db.execute(
                'INSERT OR IGNORE INTO authorization(deed_id,version,donor,options,status) '
                'VALUES (?,?,?,?,?)',
                (deed['id'], int(deed['version']), doc['donor'],
                 json.dumps(deed['authorized_options']), '有效'))
            row = db.execute('SELECT * FROM authorization WHERE deed_id=? AND version=?',
                             (deed['id'], int(deed['version']))).fetchone()
            return dict(row)
    finally:
        db.close()


def revoke_authorization(db_path, deed_id, version):
    """捐赠人在正式接收前撤回授权；已确认的 deed 版本不得撤回。"""
    db = connect(db_path)
    try:
        with db:
            db.execute('BEGIN IMMEDIATE')
            auth = db.execute('SELECT * FROM authorization WHERE deed_id=? AND version=?',
                              (deed_id, int(version))).fetchone()
            if not auth:
                raise ValueError('authorization_not_found')
            locked = db.execute(
                "SELECT 1 FROM acceptance WHERE deed_id=? AND deed_version=? "
                "AND stage NOT IN ('待核对','待确认','争议','已撤回','已补偿')",
                (deed_id, int(version))).fetchone()
            if locked:
                raise ValueError('authorization_locked_after_acceptance')
            db.execute('UPDATE authorization SET status=?,revoked_at=? WHERE deed_id=? AND version=?',
                       ('已撤回', _now(), deed_id, int(version)))
            for row in db.execute(
                    "SELECT case_id FROM acceptance WHERE deed_id=? AND deed_version=? "
                    "AND stage IN ('待核对','待确认','争议')",
                    (deed_id, int(version))).fetchall():
                db.execute("UPDATE acceptance SET stage='已撤回',dispute_code='authorization_revoked',"
                           'updated_at=? WHERE case_id=?', (_now(), row['case_id']))
                _log(db, row['case_id'], '已撤回', 'authorization_revoked',
                     {'deed_id': deed_id, 'version': int(version)})
            return {'deed_id': deed_id, 'version': int(version), 'status': '已撤回'}
    finally:
        db.close()


def verify(db_path, case_id):
    """实物核对：标签/证据身份、捐赠人签署、授权版本、封条、连续双签保管。

    任一闸门失败 → acceptance 进入“争议”并记录 dispute_code，绝不自动入藏。
    """
    db = connect(db_path)
    try:
        with db:
            db.execute('BEGIN IMMEDIATE')
            payload = _load_case(db, case_id)
            doc, events = payload['doc'], payload['events']
            tag, deed, seal = doc['tag'], doc['deed'], doc['seal']
            acc = db.execute('SELECT * FROM acceptance WHERE case_id=?', (case_id,)).fetchone()
            if acc and acc['stage'] not in PRE_CONFIRM_STAGES:
                return dict(acc)  # 已确认后的路径冻结，不再重新核对
            sync_authorization_txn(db, case_id, doc)
            auth = db.execute('SELECT * FROM authorization WHERE deed_id=? AND version=?',
                              (deed['id'], int(deed['version']))).fetchone()

            def dispute(code, detail):
                custodian = None
                try:
                    custodian = _current_custodian(doc['custody'])
                except ValueError:
                    pass
                _upsert_acceptance(db, case_id, tag, deed, seal, custodian, '争议',
                                   dispute_code=code)
                _log(db, case_id, '争议', 'verify_failed', {'code': code, **detail})
                return dict(db.execute('SELECT * FROM acceptance WHERE case_id=?',
                                       (case_id,)).fetchone())

            # 1. 标签与实物：ingest 的未知标签证据必须存在且与 case 标签一致
            if not events or any(e['copy_id'] != tag for e in events) or \
                    not all(db.execute('SELECT 1 FROM evidence WHERE event_id=? AND role=?',
                                       (e['id'], 'inventory_event')).fetchone() for e in events):
                return dispute('tag_evidence_mismatch', {'tag': tag, 'events': len(events)})
            # 2. 与既有冻结 copy 碰撞 / 已入藏 holding → 禁止追认
            if db.execute('SELECT 1 FROM copy WHERE id=?', (tag,)).fetchone() or \
                    db.execute('SELECT 1 FROM holding WHERE tag=?', (tag,)).fetchone():
                return dispute('copy_collision', {'tag': tag})
            # 3. 捐赠人签署
            if deed.get('signed_by') != doc.get('donor') or not deed.get('signed_at') \
                    or not deed.get('authorized_options'):
                return dispute('deed_signature_invalid', {'deed': deed.get('id')})
            # 4. 授权版本当前有效
            if not auth or auth['status'] != '有效':
                return dispute('authorization_revoked',
                               {'deed_id': deed['id'], 'version': deed['version']})
            # 5. 封条：全程同封条、无破损、有封存检查
            custody = doc.get('custody', [])
            if any(e.get('seal') != seal for e in custody):
                return dispute('seal_mismatch', {'expected': seal})
            if any(e.get('broken') is True or e.get('seal_status') == 'broken'
                   for e in custody):
                return dispute('broken_seal', {'seal': seal})
            if not any(e.get('kind') == 'seal_check' for e in custody):
                return dispute('seal_check_missing', {'seal': seal})
            # 6. 连续双签保管链：receive 捐赠人+馆方，handoff 双馆双签，时间有序
            code = _custody_chain_code(custody, doc['donor'])
            if code:
                return dispute(code, {'seal': seal})
            custodian = _current_custodian(custody)
            _upsert_acceptance(db, case_id, tag, deed, seal, custodian, '待确认',
                               dispute_code=None)
            _log(db, case_id, '待确认', 'verified',
                 {'tag': tag, 'custodian': custodian,
                  'options': deed['authorized_options']})
            return dict(db.execute('SELECT * FROM acceptance WHERE case_id=?',
                                   (case_id,)).fetchone())
    finally:
        db.close()


def sync_authorization_txn(db, case_id, doc):
    deed = doc['deed']
    db.execute('INSERT OR IGNORE INTO authorization(deed_id,version,donor,options,status) '
               'VALUES (?,?,?,?,?)',
               (deed['id'], int(deed['version']), doc['donor'],
                json.dumps(deed['authorized_options']), '有效'))


def _custody_chain_code(custody, donor):
    if not custody:
        return 'custody_gap'
    ordered = sorted(custody, key=lambda e: e['at'])
    if ordered != custody:
        return 'custody_gap'
    first = custody[0]
    if first.get('kind') != 'receive' or donor not in first.get('signed_by', []) \
            or first.get('to') not in first.get('signed_by', []):
        return 'custody_unsigned'
    prev_to = first['to']
    for entry in custody[1:]:
        if entry.get('kind') == 'handoff':
            signs = entry.get('signed_by', [])
            if entry.get('from') != prev_to or entry.get('from') not in signs \
                    or entry.get('to') not in signs:
                return 'custody_unsigned'
            prev_to = entry['to']
        elif entry.get('kind') == 'seal_check':
            if entry.get('by') not in entry.get('signed_by', []):
                return 'custody_unsigned'
    return None


def confirm(db_path, case_id, librarian, path):
    """不可分割的接收确认：同一 deed 版本 + 当前保管人 + 封条 + 标签身份一次提交。

    两名馆员并发选择不同路径时，只有一个条件更新能成功；重复确认同路径幂等。
    """
    if path not in PATHS:
        raise ValueError('unknown_legal_path')
    db = connect(db_path)
    try:
        with db:
            db.execute('BEGIN IMMEDIATE')
            acc = db.execute('SELECT * FROM acceptance WHERE case_id=?', (case_id,)).fetchone()
            if not acc:
                raise ValueError('acceptance_not_verified')
            if acc['stage'] not in ('待确认',) and acc['stage'] not in (
                    '已确认', '权属已登记', '快照已生成', '凭证已出具'):
                raise ValueError(f"acceptance_stage_{acc['stage']}")
            if acc['stage'] != '待确认':
                if acc['path'] != path:
                    raise ValueError('legal_path_conflict')
                return dict(acc)  # 重复确认：幂等返回，不产生第二种产权状态
            doc = _load_case(db, case_id)['doc']
            deed = doc['deed']
            if path not in deed['authorized_options']:
                raise ValueError('path_not_authorized')
            auth = db.execute('SELECT * FROM authorization WHERE deed_id=? AND version=?',
                              (acc['deed_id'], acc['deed_version'])).fetchone()
            if not auth or auth['status'] != '有效':
                raise ValueError('authorization_revoked')
            if path == 'outright_gift':
                owner, circulation, return_by = acc['custodian'], '禁止流通', None
            else:
                owner, circulation = doc['donor'], '仅限阅览室'
                return_by = deed.get('deposit_return_by')
                if not return_by:
                    raise ValueError('deposit_return_date_missing')
            cur = db.execute(
                "UPDATE acceptance SET stage='已确认',path=?,librarian=?,owner=?,"
                'circulation=?,return_by=?,dispute_code=NULL,updated_at=? '
                "WHERE case_id=? AND stage='待确认'",
                (path, librarian, owner, circulation, return_by, _now(), case_id))
            if cur.rowcount == 0:  # 并发馆员抢先确认了另一路径
                raise ValueError('acceptance_conflict')
            _log(db, case_id, '已确认', 'confirmed',
                 {'librarian': librarian, 'path': path, 'path_cn': PATH_CN[path],
                  'owner': owner, 'circulation': circulation, 'return_by': return_by,
                  'deed_id': acc['deed_id'], 'deed_version': acc['deed_version'],
                  'seal': acc['seal'], 'tag': acc['tag']})
            return dict(db.execute('SELECT * FROM acceptance WHERE case_id=?',
                                   (case_id,)).fetchone())
    finally:
        db.close()


def register_rights(db_path, case_id):
    """阶段二：登记权属（holding）。gift 待编目、禁止流通；deposit 不编目、限阅览室。"""
    db = connect(db_path)
    try:
        with db:
            db.execute('BEGIN IMMEDIATE')
            acc = db.execute('SELECT * FROM acceptance WHERE case_id=?', (case_id,)).fetchone()
            if not acc:
                raise ValueError('acceptance_not_found')
            row = db.execute('SELECT * FROM holding WHERE case_id=?', (case_id,)).fetchone()
            if row:
                if acc['stage'] in ('待确认',):
                    raise ValueError('acceptance_stage_待确认')
                return dict(row)  # 幂等续跑
            if acc['stage'] != '已确认':
                raise ValueError(f"acceptance_stage_{acc['stage']}")
            catalog = '待编目' if acc['path'] == 'outright_gift' else '不适用'
            db.execute('INSERT INTO holding(tag,case_id,path,owner,circulation,catalog_status,'
                       'return_by,registered_at) VALUES (?,?,?,?,?,?,?,?)',
                       (acc['tag'], case_id, acc['path'], acc['owner'], acc['circulation'],
                        catalog, acc['return_by'], _now()))
            db.execute("UPDATE acceptance SET stage='权属已登记',updated_at=? WHERE case_id=?",
                       (_now(), case_id))
            _log(db, case_id, '权属已登记', 'rights_registered',
                 {'path': acc['path'], 'owner': acc['owner'],
                  'circulation': acc['circulation'], 'catalog_status': catalog})
            return dict(db.execute('SELECT * FROM holding WHERE case_id=?',
                                   (case_id,)).fetchone())
    finally:
        db.close()


def build_snapshot(db_path, case_id, batch_id, planned_shelf, start, end, version='v1'):
    """阶段三：生成下一批馆藏快照。gift 编目写入 copy 后才开放普通流通；
    deposit 只登记 batch/holding，绝不产生可外借 copy。编目失败可原地重试。"""
    db = connect(db_path)
    try:
        with db:
            db.execute('BEGIN IMMEDIATE')
            acc = db.execute('SELECT * FROM acceptance WHERE case_id=?', (case_id,)).fetchone()
            holding = db.execute('SELECT * FROM holding WHERE case_id=?',
                                 (case_id,)).fetchone()
            if not acc or not holding:
                raise ValueError('rights_not_registered')
            if acc['stage'] == '快照已生成':
                return {'batch_id': acc['batch_id'], 'tag': acc['tag'],
                        'cataloged': acc['path'] == 'outright_gift', 'idempotent': True}
            if acc['stage'] != '权属已登记':
                raise ValueError(f"acceptance_stage_{acc['stage']}")
            if not db.execute('SELECT 1 FROM batch WHERE id=?', (batch_id,)).fetchone():
                db.execute("INSERT INTO batch VALUES (?,?,?,?,?)",
                           (batch_id, version, start, end, '已校验'))
            if holding['path'] == 'outright_gift':
                if db.execute('SELECT 1 FROM copy WHERE id=?', (acc['tag'],)).fetchone():
                    raise ValueError('copy_collision')  # 编目写入碰撞：不推进阶段
                db.execute('INSERT INTO copy(id,planned_shelf,loaned,batch) VALUES (?,?,0,?)',
                           (acc['tag'], planned_shelf, batch_id))
                db.execute("UPDATE holding SET batch_id=?,catalog_status='已编目',"
                           "circulation='普通流通' WHERE case_id=?", (batch_id, case_id))
                db.execute("UPDATE acceptance SET batch_id=?,circulation='普通流通' WHERE case_id=?",
                           (batch_id, case_id))
            else:
                db.execute("UPDATE holding SET batch_id=? WHERE case_id=?", (batch_id, case_id))
                db.execute('UPDATE acceptance SET batch_id=? WHERE case_id=?',
                           (batch_id, case_id))
            db.execute("UPDATE acceptance SET stage='快照已生成',updated_at=? WHERE case_id=?",
                       (_now(), case_id))
            _log(db, case_id, '快照已生成', 'snapshot_built',
                 {'batch_id': batch_id, 'path': holding['path'],
                  'cataloged': holding['path'] == 'outright_gift',
                  'circulation': '普通流通' if holding['path'] == 'outright_gift'
                  else '仅限阅览室'})
            return {'batch_id': batch_id, 'tag': acc['tag'],
                    'cataloged': holding['path'] == 'outright_gift', 'idempotent': False}
    finally:
        db.close()


def issue_voucher(db_path, case_id, voucher_id=None):
    """阶段四：出具凭证。gift→赠与证明；deposit→寄存返还单。case 唯一，二者不可并存。"""
    db = connect(db_path)
    try:
        with db:
            db.execute('BEGIN IMMEDIATE')
            acc = db.execute('SELECT * FROM acceptance WHERE case_id=?', (case_id,)).fetchone()
            if not acc:
                raise ValueError('acceptance_not_found')
            old = db.execute('SELECT * FROM voucher WHERE case_id=?', (case_id,)).fetchone()
            if old:
                return dict(old)  # 幂等
            if acc['stage'] != '快照已生成':
                raise ValueError(f"acceptance_stage_{acc['stage']}")
            kind = '赠与证明' if acc['path'] == 'outright_gift' else '寄存返还单'
            vid = voucher_id or (f"VOUCHER-GIFT-{case_id}" if kind == '赠与证明'
                                 else f"VOUCHER-DEPOSIT-{case_id}")
            db.execute('INSERT INTO voucher(id,case_id,kind,tag,owner,circulation,return_by,'
                       'deed_id,deed_version,seal,issued_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                       (vid, case_id, kind, acc['tag'], acc['owner'], acc['circulation'],
                        acc['return_by'], acc['deed_id'], acc['deed_version'], acc['seal'], _now()))
            db.execute("UPDATE acceptance SET stage='凭证已出具',updated_at=? WHERE case_id=?",
                       (_now(), case_id))
            _log(db, case_id, '凭证已出具', 'voucher_issued',
                 {'voucher_id': vid, 'kind': kind, 'owner': acc['owner'],
                  'circulation': acc['circulation'], 'return_by': acc['return_by']})
            return dict(db.execute('SELECT * FROM voucher WHERE case_id=?',
                                   (case_id,)).fetchone())
    finally:
        db.close()


def compensate(db_path, case_id):
    """确认后、凭证出具前的补偿：撤销 holding/copy，路径回到未选择状态（已补偿）。

    已出具凭证的案件锁定，不可补偿；历史 issue 与 stage_log 永不删除。
    """
    db = connect(db_path)
    try:
        with db:
            db.execute('BEGIN IMMEDIATE')
            acc = db.execute('SELECT * FROM acceptance WHERE case_id=?', (case_id,)).fetchone()
            if not acc:
                raise ValueError('acceptance_not_found')
            if acc['stage'] == '凭证已出具':
                raise ValueError('compensation_locked_voucher_issued')
            if acc['stage'] not in ('已确认', '权属已登记', '快照已生成'):
                raise ValueError(f"acceptance_stage_{acc['stage']}")
            holding = db.execute('SELECT * FROM holding WHERE case_id=?',
                                 (case_id,)).fetchone()
            if holding and holding['catalog_status'] == '已编目':
                db.execute('DELETE FROM copy WHERE id=?', (acc['tag'],))
            db.execute('DELETE FROM holding WHERE case_id=?', (case_id,))
            db.execute("UPDATE acceptance SET stage='已补偿',path=NULL,librarian=NULL,owner=NULL,"
                       "circulation=NULL,return_by=NULL,batch_id=NULL,updated_at=? WHERE case_id=?",
                       (_now(), case_id))
            _log(db, case_id, '已补偿', 'compensated',
                 {'reverted_from': acc['stage']})
            return dict(db.execute('SELECT * FROM acceptance WHERE case_id=?',
                                   (case_id,)).fetchone())
    finally:
        db.close()


def status(db_path, case_id):
    """可观察结果：接收/争议阶段、实际产权人、流通资格、返还义务、证据摘要。"""
    db = connect(db_path)
    try:
        acc = db.execute('SELECT * FROM acceptance WHERE case_id=?', (case_id,)).fetchone()
        if not acc:
            raise ValueError('acceptance_not_found')
        out = dict(acc)
        out['holding'] = dict(db.execute('SELECT * FROM holding WHERE case_id=?',
                                         (case_id,)).fetchone() or {})
        out['voucher'] = dict(db.execute('SELECT * FROM voucher WHERE case_id=?',
                                         (case_id,)).fetchone() or {})
        out['unexpected_copy_evidence'] = [dict(r) for r in db.execute(
            "SELECT id,code,copy_id,event_id,detail,run_id FROM issue "
            "WHERE code='unexpected_copy' AND copy_id=?", (acc['tag'],))]
        out['stage_log'] = [dict(r) for r in db.execute(
            'SELECT stage,event,detail,at FROM stage_log WHERE case_id=? ORDER BY id',
            (case_id,))]
        return out
    finally:
        db.close()
