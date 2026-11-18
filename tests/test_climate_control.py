import csv
import hashlib
import io
import json
from pathlib import Path

import pytest

from shelfscan.__main__ import initialize
from shelfscan.ingest import ingest
from shelfscan import climate_control as cc
from shelfscan.climate_control import (
    register, plan, apply_feedback, build_plan, validate_case, solve)
from shelfscan.report import climate_report, report
from shelfscan.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def make_case(shelves, tariff=(100, 1000), limit=6, capacity=1, cooling=2):
    return {
        'shelves': [{'id': sid, 'initial_c': init, 'limit_c': lim, 'warming_c': warm}
                    for sid, init, lim, warm in shelves],
        'cooler': {'max_shelves_per_period': capacity, 'cooling_c': cooling,
                   'tariff_cents': list(tariff)},
        'total_exposure_limit': limit,
        'contracts': ['min_energy', 'min_max_shelf_exposure'],
    }


FIXTURE_CASE = make_case([('S1', 20, 22, [3, 3]), ('S2', 20, 21, [2, 2])])


def seed_climate(tmp_path, case=None, sensors=None, events=None,
                 config_name='climate-config.json', events_name='climate-events.jsonl',
                 case_name='case.json', sensors_name='sensors.csv', case_id='climate-01'):
    case = FIXTURE_CASE if case is None else case
    db_path = tmp_path / 'climate.db'
    initialize(db_path, FIX / config_name)
    if events is not None:
        events_path = tmp_path / events_name
        events_path.write_text('\n'.join(json.dumps(e) for e in events), encoding='utf-8')
        ingest(events_path, db_path)
    else:
        ingest(FIX / events_name, db_path)
    case_path = tmp_path / case_name
    case_path.write_text(json.dumps(case), encoding='utf-8')
    if sensors is None:
        rows = [('shelf', 'at', 'temperature_c', 'unit')]
        rows += [(s['id'], '2030-06-01T09:00:00Z', s['initial_c'], 'C')
                 for s in case['shelves']]
    else:
        rows = sensors
    sensor_path = tmp_path / sensors_name
    with sensor_path.open('w', encoding='utf-8', newline='') as fh:
        csv.writer(fh).writerows(rows)
    register(db_path, case_id, case_path, sensor_path)
    return db_path, case_path, sensor_path


def feedback_csv(rows):
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(('shelf', 'period', 'at', 'temperature_c', 'unit'))
    for row in rows:
        writer.writerow(list(row) + ['C'])
    return buf.getvalue().encode('utf-8')


# ------------------------------------------------------------- frozen fixture
def test_min_energy_plan_matches_contract_fixture(tmp_path):
    db_path, _, _ = seed_climate(tmp_path)
    payload = plan(db_path, 'climate-01', 'min_energy')
    assert payload['status'] == '可执行'
    assert payload['plan_version'] == 'v1-min_energy'
    assert [(c['period'], c['shelf']) for c in payload['commands']] == [(0, 'S1')]
    assert payload['totals'] == {'cost_cents': 100,
                                 'max_shelf_exposure_c_periods': 4.0,
                                 'total_exposure_c_periods': 6.0}
    assert payload['exposure_by_shelf'] == {'S1': 2.0, 'S2': 4.0}
    # chiller is occupied at most once per period
    assert sum(1 for r in payload['periods'] if r['cooler_occupied']) <= 2
    assert all(not r['cooler_occupied'] or r['command_shelf'] for r in payload['periods'])


def test_min_max_plan_matches_contract_fixture(tmp_path):
    db_path, _, _ = seed_climate(tmp_path)
    payload = plan(db_path, 'climate-01', 'min_max_shelf_exposure')
    assert payload['plan_version'] == 'v1-min_max_shelf_exposure'
    assert [(c['period'], c['shelf']) for c in payload['commands']] == [(0, 'S1'), (1, 'S2')]
    assert payload['totals'] == {'cost_cents': 1100,
                                 'max_shelf_exposure_c_periods': 2.0,
                                 'total_exposure_c_periods': 4.0}
    assert payload['exposure_by_shelf'] == {'S1': 2.0, 'S2': 2.0}


def test_objectives_are_distinct_complete_plans(tmp_path):
    db_path, _, _ = seed_climate(tmp_path)
    a = plan(db_path, 'climate-01', 'min_energy')
    b = plan(db_path, 'climate-01', 'min_max_shelf_exposure')
    # both plans coexist rather than one overwriting the other
    db = connect(db_path)
    versions = [r[0] for r in db.execute('SELECT plan_version FROM climate_plan ORDER BY 1')]
    db.close()
    assert versions == ['v1-min_energy', 'v1-min_max_shelf_exposure']
    assert [c['shelf'] for c in a['commands']] != [c['shelf'] for c in b['commands']]
    assert a['totals']['cost_cents'] != b['totals']['cost_cents']


def test_optimality_evidence_is_exhaustive(tmp_path):
    db_path, _, _ = seed_climate(tmp_path)
    payload = plan(db_path, 'climate-01', 'min_energy')
    ev = payload['optimality_evidence']
    assert ev['method'] == 'exact_dynamic_programming'
    assert ev['state_space_by_layer'][0] == 1
    assert ev['feasible_terminal_states'] >= 1
    assert ev['objective_value']['cost_cents'] == 100
    # independently enumerate all 3^2 = 9 allocation sequences and confirm
    # no feasible plan beats the DP optimum on either objective dimension
    case = FIXTURE_CASE
    best_cost, best_max = None, None
    for a0 in (None, 'S1', 'S2'):
        for a1 in (None, 'S1', 'S2'):
            temps = {'S1': 20.0, 'S2': 20.0}
            exp = {'S1': 0.0, 'S2': 0.0}
            cost = 0
            for t, action in enumerate((a0, a1)):
                for sid in ('S1', 'S2'):
                    temps[sid] += 3 if sid == 'S1' else 2
                    if action == sid:
                        temps[sid] -= 2
                    over = temps[sid] - (22 if sid == 'S1' else 21)
                    exp[sid] += max(over, 0)
                cost += case['cooler']['tariff_cents'][t] if action else 0
            if sum(exp.values()) <= 6:
                best_cost = cost if best_cost is None else min(best_cost, cost)
                worst = max(exp.values())
                best_max = worst if best_max is None else min(best_max, worst)
    assert best_cost == 100 and best_max == 2.0


# ------------------------------------------------------------- validation
def test_sensor_identity_conflict_holds_whole_plan(tmp_path):
    db_path, _, _ = seed_climate(tmp_path)
    conflict = tmp_path / 'conflict-sensors.csv'
    with conflict.open('w', encoding='utf-8', newline='') as fh:
        csv.writer(fh).writerows([
            ('shelf', 'at', 'temperature_c', 'unit'),
            ('S1', '2030-06-01T09:00:00Z', 20, 'C'),
            ('S1', '2030-06-01T09:05:00Z', 20, 'C')])
    with pytest.raises(ValueError, match='climate_sensor_duplicate'):
        register(db_path, 'climate-x', FIX / 'climate-case.json', conflict)
    # no partial plan or command can exist after a rejected registration
    db = connect(db_path)
    assert db.execute('SELECT count(*) FROM climate_plan').fetchone()[0] == 0
    db.close()


def test_timezone_inconsistent_reading_rejected(tmp_path):
    db_path, _, _ = seed_climate(tmp_path)
    naive = tmp_path / 'naive-sensors.csv'
    with naive.open('w', encoding='utf-8', newline='') as fh:
        csv.writer(fh).writerows([
            ('shelf', 'at', 'temperature_c', 'unit'),
            ('S1', '2030-06-01T09:00:00', 20, 'C'),
            ('S2', '2030-06-01T09:00:00Z', 20, 'C')])
    with pytest.raises(ValueError, match='climate_sensor_timezone'):
        register(db_path, 'climate-x', FIX / 'climate-case.json', naive)


def test_missing_warming_rejected(tmp_path):
    case = make_case([('S1', 20, 22, [3]), ('S2', 20, 21, [2, 2])])
    with pytest.raises(ValueError, match='climate_warming_missing'):
        validate_case(case)
    case2 = make_case([('S1', 20, 22, [3, None]), ('S2', 20, 21, [2, 2])])
    with pytest.raises(ValueError, match='climate_warming_missing'):
        validate_case(case2)


def test_non_physical_cooling_rejected(tmp_path):
    case = make_case([('S1', 20, 22, [3, 3]), ('S2', 20, 21, [2, 2])], cooling=0)
    with pytest.raises(ValueError, match='climate_cooler_invalid'):
        validate_case(case)


def test_infeasible_case_emits_review_plan_without_commands(tmp_path):
    # one shelf, two periods of +5 warming, cap 1: even cooling every period
    # leaves end temps 23 / 26 against limit 22 -> exposure 1 + 4 = 5 > 1
    case = make_case([('S1', 20, 22, [5, 5])], tariff=(10, 20), limit=1)
    sensors = [('shelf', 'at', 'temperature_c', 'unit'),
               ('S1', '2030-06-01T09:00:00Z', 20, 'C')]
    events = [{'kind': 'scan', 'device': 'R1', 'seq': 1, 'copy_id': 'C1',
               'at': '2030-06-01T08:00:00Z', 'shelf': 'S1', 'signal': 80}]
    db_path, case_path, sensor_path = seed_climate(
        tmp_path, case=case, sensors=sensors, events=events)
    payload = plan(db_path, 'climate-01', 'min_energy')
    assert payload['status'] == '待核'
    assert payload['commands'] == []
    assert payload['optimality_evidence']['min_achievable_total_exposure_c_periods'] == 5.0
    db = connect(db_path)
    assert db.execute("SELECT status FROM climate_plan").fetchone()[0] == '待核'
    assert db.execute('SELECT count(*) FROM climate_command').fetchone()[0] == 0
    db.close()


# ------------------------------------------------------------- capacity
def test_chiller_capacity_two_shelves_same_period(tmp_path):
    case = make_case([('S1', 20, 22, [3]), ('S2', 20, 21, [2])],
                     tariff=(50,), limit=10, capacity=2)
    result = solve(case, [20, 20])
    actions = result['min_max_shelf_exposure']['actions']
    assert actions[0] == (0, 1)  # both shelves served in the single period
    payload = build_plan('c', case, 'min_max_shelf_exposure', plan_version='v')
    assert payload['periods'][0]['cooler_occupied'] is True
    assert payload['periods'][0]['command_shelf'] == 'S1'


def test_device_exactly_full_blocks_third_shelf(tmp_path):
    case = make_case([('S1', 20, 30, [1]), ('S2', 20, 30, [1]), ('S3', 20, 30, [1])],
                     tariff=(5,), limit=100, capacity=2)
    result = solve(case, [20, 20, 20])
    action = result['min_energy']['actions'][0]
    assert len(action) <= 2  # shared chiller cannot be double-booked
    assert len(set(action)) == len(action)


# ------------------------------------------------------------- greedy vs DP
def test_dp_beats_hottest_shelf_greedy_on_tariff(tmp_path):
    # expensive period 0, cheap period 1. S1 is the hotter shelf, so a greedy
    # "cool the hottest now" policy pays the 1000-cent peak tariff; the exact
    # DP waits and cools S1 at the cheap period (idle at t0 is infeasible).
    case = make_case([('S1', 20, 22, [3, 0]), ('S2', 20, 21, [1, 0])],
                     tariff=(1000, 10), limit=1)
    payload = plan_direct(case, 'min_energy')
    assert [(c['period'], c['shelf']) for c in payload['commands']] == [(1, 'S1')]
    assert payload['totals']['cost_cents'] == 10
    assert payload['totals']['total_exposure_c_periods'] <= 1


def plan_direct(case, objective):
    return build_plan('direct', case, objective, plan_version='v1')


# ------------------------------------------------------------- feedback
def test_late_feedback_causally_changes_remaining_commands(tmp_path):
    # The model needs no cooling at t0 and nothing later. A late real reading
    # shows S2 ran hot (24C against a 22C limit): the re-solved suffix must
    # then issue a real cooling command that v1 never contained.
    case = make_case([('S1', 20, 25, [1, 1]), ('S2', 20, 22, [1, 1])],
                     tariff=(100, 1000), limit=4)
    sensors = [('shelf', 'at', 'temperature_c', 'unit'),
               ('S1', '2030-06-01T09:00:00Z', 20, 'C'),
               ('S2', '2030-06-01T09:00:00Z', 20, 'C')]
    db_path, case_path, sensor_path = seed_climate(tmp_path, case=case, sensors=sensors)
    v1 = plan(db_path, 'climate-01', 'min_energy')
    assert v1['commands'] == []  # model needs no cooling
    raw = feedback_csv([('S1', 0, '2030-06-01T10:00:00Z', 21),
                        ('S2', 0, '2030-06-01T10:00:00Z', 24)])
    v2 = apply_feedback(db_path, 'climate-01', v1['plan_version'], 'min_energy', raw, 0)
    assert v2['plan_version'] == 'v2-min_energy'
    suffix = [c for c in v2['commands'] if c['period'] >= 1]
    assert suffix and suffix[0]['shelf'] == 'S2'  # new reading changed remaining plan
    assert v2['periods'][0]['end_temp_source'] == 'actual_feedback'


def test_feedback_never_rewrites_executed_commands_or_cost(tmp_path):
    db_path, _, _ = seed_climate(tmp_path)
    v1 = plan(db_path, 'climate-01', 'min_max_shelf_exposure')
    raw = feedback_csv([('S1', 0, '2030-06-01T10:00:00Z', 21),
                        ('S2', 0, '2030-06-01T10:00:00Z', 23)])
    v2 = apply_feedback(db_path, 'climate-01', v1['plan_version'],
                        'min_max_shelf_exposure', raw, 0)
    # paid 100 cents at t0 and it stays fixed; v2 never re-issues period 0
    assert v2['feedback']['frozen_cost_cents'] == 100
    assert all(c['period'] > 0 for c in v2['commands'])
    db = connect(db_path)
    rows = {(r['plan_version'], r['period']): (r['shelf'], r['status'], r['price_cents'])
            for r in db.execute('SELECT plan_version,period,shelf,status,price_cents FROM climate_command')}
    assert rows[(v1['plan_version'], 0)] == ('S1', '已执行', 100)
    assert rows[(v1['plan_version'], 1)] == ('S2', '已作废', 1000)
    assert rows[(v2['plan_version'], 1)][1] == '待执行'
    assert v2['periods'][0]['command_shelf'] == 'S1'  # historical command preserved
    db.close()


def test_missing_feedback_reading_is_not_treated_as_zero(tmp_path):
    db_path, _, _ = seed_climate(tmp_path)
    v1 = plan(db_path, 'climate-01', 'min_energy')
    raw = feedback_csv([('S1', 0, '2030-06-01T10:00:00Z', 21)])  # S2 missing
    with pytest.raises(ValueError, match='climate_sensor_missing'):
        apply_feedback(db_path, 'climate-01', v1['plan_version'], 'min_energy', raw, 0)
    db = connect(db_path)
    assert db.execute('SELECT count(*) FROM climate_feedback').fetchone()[0] == 0
    db.close()


def test_over_limit_exposure_cannot_be_offset_by_future(tmp_path):
    # real t0 reading already breaches S2's limit; cumulative exposure must
    # keep that breach even though the suffix itself stays at zero exposure
    db_path, _, _ = seed_climate(tmp_path)
    v1 = plan(db_path, 'climate-01', 'min_energy')
    raw = feedback_csv([('S1', 0, '2030-06-01T10:00:00Z', 21),
                        ('S2', 0, '2030-06-01T10:00:00Z', 25)])  # 4 over S2 limit 21
    v2 = apply_feedback(db_path, 'climate-01', v1['plan_version'], 'min_energy', raw, 0)
    assert v2['exposure_by_shelf']['S2'] >= 4.0
    assert all(v >= 0 for r in v2['periods']
               for v in r['period_exposure_c'].values())
    # suffix exposure is additive only; no negative term exists anywhere
    assert all(v >= 0 for v in v2['exposure_by_shelf'].values())


def test_cascading_feedback_versions_freeze_each_prefix(tmp_path):
    db_path, _, _ = seed_climate(tmp_path)
    v1 = plan(db_path, 'climate-01', 'min_max_shelf_exposure')
    r0 = feedback_csv([('S1', 0, '2030-06-01T10:00:00Z', 21),
                       ('S2', 0, '2030-06-01T10:00:00Z', 22)])
    v2 = apply_feedback(db_path, 'climate-01', v1['plan_version'],
                        'min_max_shelf_exposure', r0, 0)
    r1 = feedback_csv([
        ('S1', 0, '2030-06-01T10:00:00Z', 21),
        ('S2', 0, '2030-06-01T10:00:00Z', 22),
        ('S1', 1, '2030-06-01T11:00:00Z', 22),
        ('S2', 1, '2030-06-01T11:00:00Z', 22),
    ])
    v3 = apply_feedback(db_path, 'climate-01', v2['plan_version'],
                        'min_max_shelf_exposure', r1, 1)
    assert v3['plan_version'] == 'v3-min_max_shelf_exposure'
    assert v3['feedback']['through_period'] == 1
    assert len(v3['periods']) == 2
    assert all(r['end_temp_source'] == 'actual_feedback' for r in v3['periods'])
    assert v3['totals']['cost_cents'] == v3['feedback']['frozen_cost_cents']


# ------------------------------------------------------------- conservation
def test_exposure_and_cost_conservation_under_feedback(tmp_path):
    db_path, _, _ = seed_climate(tmp_path)
    v1 = plan(db_path, 'climate-01', 'min_max_shelf_exposure')
    raw = feedback_csv([('S1', 0, '2030-06-01T10:00:00Z', 21),
                        ('S2', 0, '2030-06-01T10:00:00Z', 23)])
    v2 = apply_feedback(db_path, 'climate-01', v1['plan_version'],
                        'min_max_shelf_exposure', raw, 0)
    # cost conservation: frozen prefix cost + suffix period prices
    suffix_cost = sum(r['price_cents'] for r in v2['periods'][1:])
    assert v2['totals']['cost_cents'] == v2['feedback']['frozen_cost_cents'] + suffix_cost
    # exposure conservation: per-shelf sum of period exposures equals cumulative
    for sid in ('S1', 'S2'):
        assert sum(r['period_exposure_c'][sid] for r in v2['periods']) == \
            pytest.approx(v2['exposure_by_shelf'][sid])
    assert sum(v2['exposure_by_shelf'].values()) == \
        pytest.approx(v2['totals']['total_exposure_c_periods'])


def test_deterministic_recompute(tmp_path):
    db_path, _, _ = seed_climate(tmp_path)
    a = plan(db_path, 'climate-01', 'min_energy')
    b = plan(db_path, 'climate-01', 'min_energy')
    assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)
    raw = feedback_csv([('S1', 0, '2030-06-01T10:00:00Z', 21),
                        ('S2', 0, '2030-06-01T10:00:00Z', 22)])
    c = apply_feedback(db_path, 'climate-01', 'v1-min_energy', 'min_energy', raw, 0,
                       new_version='v2-min_energy')
    d = apply_feedback(db_path, 'climate-01', 'v1-min_energy', 'min_energy', raw, 0,
                       new_version='v3-min_energy')
    c['plan_version'] = d['plan_version'] = 'v'
    c['feedback']['new_version'] = d['feedback']['new_version'] = 'v'
    assert json.dumps(c, sort_keys=True) == json.dumps(d, sort_keys=True)


# ------------------------------------------------------------- provenance
def test_register_keeps_original_scan_and_sensor_sha(tmp_path):
    db_path, _, sensor_path = seed_climate(tmp_path)
    before = connect(db_path)
    event_sha = before.execute('SELECT sha256 FROM event ORDER BY id').fetchall()
    source_sha = before.execute('SELECT sha256 FROM source').fetchone()[0]
    before.close()
    plan(db_path, 'climate-01', 'min_energy')
    raw = feedback_csv([('S1', 0, '2030-06-01T10:00:00Z', 21),
                        ('S2', 0, '2030-06-01T10:00:00Z', 22)])
    apply_feedback(db_path, 'climate-01', 'v1-min_energy', 'min_energy', raw, 0)
    db = connect(db_path)
    assert [r[0] for r in db.execute('SELECT sha256 FROM event ORDER BY id')] == \
        [r[0] for r in event_sha]
    assert db.execute('SELECT sha256 FROM source').fetchone()[0] == source_sha
    envelope = json.loads(db.execute('SELECT payload FROM climate_case').fetchone()[0])
    assert envelope['sensor_sha256'] == hashlib.sha256(
        sensor_path.read_bytes()).hexdigest()
    # inventory decisions are untouched by climate planning
    statuses = [r[0] for r in db.execute('SELECT status FROM decision')]
    db.close()
    assert statuses == []  # climate flow never evaluates inventory decisions


def test_reports_contain_commands_and_risk(tmp_path):
    db_path, _, _ = seed_climate(tmp_path)
    plan(db_path, 'climate-01', 'min_energy')
    raw = feedback_csv([('S1', 0, '2030-06-01T10:00:00Z', 21),
                        ('S2', 0, '2030-06-01T10:00:00Z', 22)])
    apply_feedback(db_path, 'climate-01', 'v1-min_energy', 'min_energy', raw, 0)
    out = climate_report(db_path, tmp_path / 'climate-report.json')
    data = json.loads(out.read_text(encoding='utf-8'))
    assert len(data['plans']) == 2 and len(data['feedback']) == 1
    assert any(c['status'] == '已执行' for c in data['commands'])
    md = out.with_suffix('.md').read_text(encoding='utf-8')
    assert '书架温控冷却计划' in md and '最优性证据' in md and 'S1' in md
    # general inventory report now carries climate tables too
    general = report(db_path, tmp_path / 'report.json')
    blob = json.loads(general.read_text(encoding='utf-8'))
    assert blob['climate_plan'] and blob['climate_command']


def test_unknown_objective_rejected(tmp_path):
    db_path, _, _ = seed_climate(tmp_path)
    with pytest.raises(ValueError, match='climate_objective_unknown'):
        plan(db_path, 'climate-01', 'min_electricity')


def test_objective_switching_recomputes_and_keeps_both_plans(tmp_path):
    db_path, _, _ = seed_climate(tmp_path)
    first = plan(db_path, 'climate-01', 'min_energy')
    second = plan(db_path, 'climate-01', 'min_max_shelf_exposure')
    again = plan(db_path, 'climate-01', 'min_energy')  # switch back, stable version
    assert again['plan_version'] == first['plan_version']
    db = connect(db_path)
    rows = db.execute('SELECT plan_version,objective FROM climate_plan ORDER BY 1').fetchall()
    db.close()
    assert [tuple(r) for r in rows] == [
        ('v1-min_energy', 'min_energy'),
        ('v1-min_max_shelf_exposure', 'min_max_shelf_exposure')]
    assert first['totals']['cost_cents'] != second['totals']['cost_cents']


def test_climate_flow_preserves_inventory_decisions(tmp_path):
    # evaluate inventory first; climate planning/feedback must not touch
    # 在架/借出 decisions, batches or issues
    from shelfscan.rules import evaluate
    db_path, _, _ = seed_climate(tmp_path)
    evaluate(db_path)
    db = connect(db_path)
    before = [tuple(r) for r in db.execute(
        'SELECT copy_id,status,actual_shelf FROM decision ORDER BY copy_id')]
    issues_before = db.execute('SELECT count(*) FROM issue').fetchone()[0]
    db.close()
    plan(db_path, 'climate-01', 'min_energy')
    raw = feedback_csv([('S1', 0, '2030-06-01T10:00:00Z', 21),
                        ('S2', 0, '2030-06-01T10:00:00Z', 22)])
    apply_feedback(db_path, 'climate-01', 'v1-min_energy', 'min_energy', raw, 0)
    db = connect(db_path)
    after = [tuple(r) for r in db.execute(
        'SELECT copy_id,status,actual_shelf FROM decision ORDER BY copy_id')]
    issues_after = db.execute('SELECT count(*) FROM issue').fetchone()[0]
    db.close()
    assert after == before and issues_after == issues_before
