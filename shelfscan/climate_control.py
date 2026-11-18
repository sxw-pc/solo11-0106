"""Frozen shelf climate inputs, exact cooling planner and late-reading re-planning.

The cooling plan is an exact global optimum found by dynamic programming over
(temperature, cumulative exposure) states.  The shared chiller can serve at
most ``max_shelves_per_period`` shelves per period; every feasible allocation
sequence is enumerated, so a greedy "cool the hottest shelf" policy cannot hide
a cheaper or lower-exposure plan.
"""
import csv
import hashlib
import io
import itertools
import json
import math
from datetime import datetime
from pathlib import Path

from .store import connect

MODEL_VERSION = 'climate-dp-1'
OBJECTIVES = ('min_energy', 'min_max_shelf_exposure')
STATE_QUANTUM = 9  # decimal places used when keying float temperatures/exposure


# ---------------------------------------------------------------- validation
def _when(value):
    """Parse an ISO-8601 timestamp and require an explicit timezone."""
    d = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if d.tzinfo is None:
        raise ValueError('climate_sensor_timezone')
    return d


def _finite_number(value, code):
    try:
        n = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(code) from exc
    if not math.isfinite(n):
        raise ValueError(code)
    return n


def validate_case(case):
    """Validate the frozen climate case; raise ValueError(code) on any defect."""
    if not isinstance(case, dict) or not case.get('shelves') or not case.get('contracts'):
        raise ValueError('climate_case_invalid')
    shelves = case['shelves']
    if not isinstance(shelves, list) or not (1 <= len(shelves) <= 4):
        raise ValueError('climate_case_invalid')
    ids = [s.get('id') for s in shelves]
    if any(not sid for sid in ids) or len(set(ids)) != len(ids):
        raise ValueError('climate_sensor_duplicate')
    horizon = None
    for shelf in shelves:
        if 'initial_c' not in shelf or 'limit_c' not in shelf:
            raise ValueError('climate_case_invalid')
        _finite_number(shelf['initial_c'], 'climate_case_invalid')
        _finite_number(shelf['limit_c'], 'climate_case_invalid')
        warming = shelf.get('warming_c')
        if not isinstance(warming, list) or not warming:
            raise ValueError('climate_warming_missing')
        if any(w is None for w in warming):
            raise ValueError('climate_warming_missing')
        for w in warming:
            _finite_number(w, 'climate_warming_missing')
        if horizon is None:
            horizon = len(warming)
        elif len(warming) != horizon:
            raise ValueError('climate_warming_missing')
    if horizon is None or horizon > 8:
        raise ValueError('climate_case_invalid')
    cooler = case.get('cooler')
    if not isinstance(cooler, dict):
        raise ValueError('climate_case_invalid')
    capacity = cooler.get('max_shelves_per_period')
    cooling_c = cooler.get('cooling_c')
    if not isinstance(capacity, int) or not (1 <= capacity <= len(shelves)):
        raise ValueError('climate_cooler_invalid')
    cooling_c = _finite_number(cooling_c, 'climate_cooler_invalid')
    if cooling_c <= 0:
        raise ValueError('climate_cooler_invalid')
    tariff = cooler.get('tariff_cents')
    if not isinstance(tariff, list) or len(tariff) != horizon:
        raise ValueError('climate_tariff_invalid')
    for price in tariff:
        if not isinstance(price, int) or price < 0:
            raise ValueError('climate_tariff_invalid')
    limit = case.get('total_exposure_limit')
    if not isinstance(limit, (int, float)) or limit < 0:
        raise ValueError('climate_case_invalid')
    return {'horizon': horizon, 'capacity': capacity, 'cooling_c': cooling_c}


def parse_sensor_rows(raw_bytes):
    """Parse the frozen sensor CSV, enforcing identity and timezone rules."""
    rows = list(csv.DictReader(io.StringIO(raw_bytes.decode('utf-8-sig'))))
    seen = set()
    for row in rows:
        sid = row.get('shelf')
        if not sid or sid in seen:
            raise ValueError('climate_sensor_duplicate')
        seen.add(sid)
        _when(row.get('at'))
        _finite_number(row.get('temperature_c'), 'climate_sensor_mismatch')
        if (row.get('unit') or '').strip().upper() not in ('C', '°C', ''):
            raise ValueError('climate_sensor_mismatch')
    return rows


# ---------------------------------------------------------------- registration
def register(db_path, case_id, fixture, sensors):
    case_path = Path(fixture).resolve()
    sensor_path = Path(sensors).resolve()
    case_raw = case_path.read_bytes()
    sensor_raw = sensor_path.read_bytes()
    case = json.loads(case_raw)
    readings = parse_sensor_rows(sensor_raw)
    if not case_id or not case.get('shelves') or not case.get('contracts'):
        raise ValueError('climate_case_invalid')
    shape = validate_case(case)
    if len(readings) != len(case['shelves']):
        raise ValueError('climate_sensor_count')
    indexed = {row['shelf']: row for row in readings}
    if len(indexed) != len(readings):
        raise ValueError('climate_sensor_duplicate')
    db = connect(db_path)
    try:
        observed = []
        for shelf in case['shelves']:
            sid = shelf['id']
            reading = indexed.get(sid)
            if reading is None or float(reading['temperature_c']) != shelf['initial_c']:
                raise ValueError('climate_sensor_mismatch')
            rows = [dict(row) for row in db.execute(
                'SELECT c.id AS copy_id,r.id AS reader_id,e.id AS event_id,e.at,s.sha256 AS source_sha256 '
                'FROM copy c JOIN event e ON e.copy_id=c.id AND e.kind=? '
                'JOIN reader r ON r.id=e.device AND r.shelf=c.planned_shelf '
                'JOIN source s ON s.id=e.source_id '
                'WHERE c.planned_shelf=? ORDER BY e.id', ('scan', sid))]
            if not rows:
                raise ValueError('climate_shelf_unobserved')
            observed.append({'shelf': sid, 'inventory': rows, 'sensor': reading})
        payload = json.dumps({'case': case, 'case_path': str(case_path),
                              'case_sha256': hashlib.sha256(case_raw).hexdigest(),
                              'sensor_path': str(sensor_path),
                              'sensor_sha256': hashlib.sha256(sensor_raw).hexdigest(),
                              'observed': observed}, sort_keys=True, separators=(',', ':'))
        with db:
            old = db.execute('SELECT payload FROM climate_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('climate_case_conflict')
            db.execute('INSERT OR IGNORE INTO climate_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'shelves': len(observed)}


# ---------------------------------------------------------------- exact solver
def _q(value):
    """Quantize a physical value so float noise never splits one DP state."""
    return round(float(value), STATE_QUANTUM)


def _iter_actions(n_shelves, capacity):
    """All cooler allocations for one period, empty (idle) first, deterministic."""
    yield ()
    for size in range(1, capacity + 1):
        for combo in itertools.combinations(range(n_shelves), size):
            yield combo


def _objective_key(node, objective):
    exposure = node['exp']
    ordered = tuple(sorted((_q(v) for v in exposure), reverse=True))
    if objective == 'min_energy':
        # primary: integer cents; secondary: worst per-shelf cumulative exposure
        return (node['cost'], max(_q(v) for v in exposure), ordered,
                tuple(_q(v) for v in node['temps']), tuple(_q(v) for v in exposure))
    # min_max_shelf_exposure: primary worst shelf, then integer cents
    return (max(_q(v) for v in exposure), node['cost'], ordered,
            tuple(_q(v) for v in node['temps']), tuple(_q(v) for v in exposure))


def solve(case, start_temps, start_period=0, fixed_exposure=None):
    """Exact DP over remaining periods.

    State = (end temperature per shelf, cumulative exposure per shelf).  For
    every reachable state only the cheapest predecessor is kept, because future
    transitions depend on temperature/exposure only, never on money already
    spent.  The complete final feasible frontier is then sorted by the
    authorized objective, so the returned plan is the global lexicographic
    optimum and the layer sizes are an auditable exhaustive-search certificate.
    """
    shape = validate_case(case)
    horizon = shape['horizon']
    capacity = shape['capacity']
    cooling_c = shape['cooling_c']
    n = len(case['shelves'])
    limits = [s['limit_c'] for s in case['shelves']]
    warming = [s['warming_c'] for s in case['shelves']]
    tariff = case['cooler']['tariff_cents']
    cap = case['total_exposure_limit']
    if fixed_exposure is None:
        fixed_exposure = [0.0] * n
    if len(start_temps) != n or len(fixed_exposure) != n:
        raise ValueError('climate_case_invalid')
    if not (0 <= start_period <= horizon):
        raise ValueError('climate_case_invalid')

    start_key = (tuple(_q(v) for v in start_temps), tuple(_q(v) for v in fixed_exposure))
    start_node = {'temps': [float(v) for v in start_temps],
                  'exp': [float(v) for v in fixed_exposure], 'cost': 0}
    frontier = {start_key: start_node}
    layers = [dict(frontier)]
    for t in range(start_period, horizon):
        nxt = {}
        for key, node in frontier.items():
            for action in _iter_actions(n, capacity):
                temps, exp = [], []
                for s in range(n):
                    end = node['temps'][s] + warming[s][t] - (
                        cooling_c if s in action else 0.0)
                    over = end - limits[s]
                    temps.append(end)
                    exp.append(node['exp'][s] + (over if over > 0 else 0.0))
                cost = node['cost'] + (tariff[t] if action else 0)
                nkey = (tuple(_q(v) for v in temps), tuple(_q(v) for v in exp))
                incumbent = nxt.get(nkey)
                if incumbent is None or cost < incumbent['cost']:
                    nxt[nkey] = {'temps': temps, 'exp': exp, 'cost': cost,
                                 'parent': key, 'action': action}
        frontier = nxt
        layers.append(dict(frontier))
        if not frontier:
            return {'feasible': False, 'state_counts': [len(layer) for layer in layers],
                    'min_total_exposure': None}

    def total_exposure(node):
        return sum(_q(v) for v in node['exp'])

    feasible = [node for node in frontier.values() if total_exposure(node) <= cap]
    counts = [len(layer) for layer in layers]
    if not feasible:
        loosest = min(frontier.values(), key=lambda nd: (total_exposure(nd), nd['cost']))
        return {'feasible': False, 'state_counts': counts,
                'min_total_exposure': total_exposure(loosest),
                'min_exposure_cost_cents': loosest['cost']}

    def reconstruct(node):
        key = (tuple(_q(v) for v in node['temps']), tuple(_q(v) for v in node['exp']))
        actions = {}
        for layer_index in range(len(layers) - 1, 0, -1):
            current = layers[layer_index][key]
            actions[start_period + layer_index - 1] = current['action']
            key = current['parent']
        return actions

    result = {}
    for objective in OBJECTIVES:
        best = min(feasible, key=lambda nd: _objective_key(nd, objective))
        result[objective] = {'node': best, 'actions': reconstruct(best),
                             'frontier_size': len(feasible)}
    result['feasible'] = True
    result['state_counts'] = counts
    result['all_terminal_states'] = len(frontier)
    return result


# ---------------------------------------------------------------- plan building
def _simulate(case, actions, start_temps, start_period, fixed_exposure,
              start_sources):
    """Deterministically re-simulate a solved suffix into per-period records."""
    shape = validate_case(case)
    cooling_c = shape['cooling_c']
    n = len(case['shelves'])
    ids = [s['id'] for s in case['shelves']]
    limits = [s['limit_c'] for s in case['shelves']]
    tariff = case['cooler']['tariff_cents']
    temps = [float(v) for v in start_temps]
    exp = [float(v) for v in fixed_exposure]
    records = []
    cost = 0
    for t in range(start_period, len(tariff)):
        action = actions.get(t, ())
        command_shelf = ids[action[0]] if action else None
        start = list(temps)
        end, period_exp = [], []
        for s in range(n):
            value = temps[s] + case['shelves'][s]['warming_c'][t] - (
                cooling_c if s in action else 0.0)
            over = value - limits[s]
            end.append(value)
            period_exp.append(over if over > 0 else 0.0)
            exp[s] += period_exp[-1]
        price = tariff[t] if action else 0
        cost += price
        records.append({
            'period': t,
            'start_temps': {ids[s]: start[s] for s in range(n)},
            'start_temp_source': start_sources,
            'warming_c': {ids[s]: case['shelves'][s]['warming_c'][t] for s in range(n)},
            'command_shelf': command_shelf,
            'cooler_occupied': bool(action),
            'end_temps': {ids[s]: end[s] for s in range(n)},
            'end_temp_source': 'predicted',
            'period_exposure_c': {ids[s]: period_exp[s] for s in range(n)},
            'cumulative_exposure_c': {ids[s]: exp[s] for s in range(n)},
            'price_cents': price,
        })
        temps = end
    return records, cost, list(exp)


def _evidence(case, solution, objective):
    if not solution['feasible']:
        return {
            'method': 'exact_dynamic_programming',
            'state_space_by_layer': solution['state_counts'],
            'contract_total_exposure_limit_c_periods': case['total_exposure_limit'],
            'min_achievable_total_exposure_c_periods': solution['min_total_exposure'],
            'min_exposure_plan_cost_cents': solution.get('min_exposure_cost_cents'),
            'infeasible_certificate': 'every terminal DP state exceeds the contract limit',
            'global_optimum_basis': 'DP enumerates every feasible chiller allocation; '
                                    'no plan can satisfy the exposure cap',
        }
    chosen = solution[objective]
    node = chosen['node']
    return {
        'method': 'exact_dynamic_programming',
        'model_version': MODEL_VERSION,
        'state_space_by_layer': solution['state_counts'],
        'terminal_states_enumerated': solution['all_terminal_states'],
        'feasible_terminal_states': chosen['frontier_size'],
        'selection_order': ('integer cost cents, then maximum per-shelf cumulative exposure'
                            if objective == 'min_energy'
                            else 'maximum per-shelf cumulative exposure, then integer cost cents'),
        'objective_value': {'cost_cents': node['cost'],
                            'max_shelf_exposure_c_periods': max(_q(v) for v in node['exp']),
                            'total_exposure_c_periods': sum(_q(v) for v in node['exp'])},
        'dominance_rule': 'for each (temperature, exposure) state only the cheapest predecessor '
                          'is retained; future transitions never depend on money already spent',
        'global_optimum_basis': 'all cooler allocation sequences are enumerated; the complete '
                                'feasible frontier is sorted lexicographically by the authorized '
                                'objective, therefore no better plan exists',
    }


def build_plan(case_id, case, objective, *, plan_version, start_period=0,
               start_temps=None, fixed_exposure=None, prefix_records=None,
               fixed_cost=0, feedback=None, input_digests=None,
               source_summary=None):
    """Pure planner: returns a fully rendered, JSON-serializable plan payload."""
    if objective not in OBJECTIVES:
        raise ValueError('climate_objective_unknown')
    shape = validate_case(case)
    n = len(case['shelves'])
    ids = [s['id'] for s in case['shelves']]
    if start_temps is None:
        start_temps = [s['initial_c'] for s in case['shelves']]
    if fixed_exposure is None:
        fixed_exposure = [0.0] * n
    solution = solve(case, start_temps, start_period, fixed_exposure)
    base = {
        'case_id': case_id,
        'plan_version': plan_version,
        'objective': objective,
        'model_version': MODEL_VERSION,
        'status': '可执行' if solution['feasible'] else '待核',
        'input_digests': input_digests or {},
        'source_summary': source_summary or {},
        'constraints': {
            'shelf_limit_c': {s['id']: s['limit_c'] for s in case['shelves']},
            'cooler_max_shelves_per_period': shape['capacity'],
            'cooling_c_per_command': shape['cooling_c'],
            'total_exposure_limit_c_periods': case['total_exposure_limit'],
        },
        'tariff_cents': list(case['cooler']['tariff_cents']),
        'feedback': feedback,
        'optimality_evidence': _evidence(case, solution, objective),
    }
    if not solution['feasible']:
        base.update({'periods': prefix_records or [], 'commands': [],
                     'totals': {'cost_cents': fixed_cost,
                                'max_shelf_exposure_c_periods': max(_q(v) for v in fixed_exposure),
                                'total_exposure_c_periods': sum(_q(v) for v in fixed_exposure)},
                     'exposure_by_shelf': {ids[s]: fixed_exposure[s] for s in range(n)}})
        return base

    actions = solution[objective]['actions']
    sources = {sid: ('observed' if start_period == 0 else 'actual_feedback')
               for sid in ids}
    suffix_records, suffix_cost, final_exp = _simulate(
        case, actions, start_temps, start_period, fixed_exposure, sources)
    records = list(prefix_records or []) + suffix_records
    # New commands exist only for the re-solved suffix; executed prefix
    # periods are history and must never be re-issued.
    commands = [{'period': r['period'], 'shelf': r['command_shelf'],
                 'price_cents': r['price_cents']}
                for r in suffix_records if r['command_shelf'] is not None]
    total_cost = fixed_cost + suffix_cost
    payload = dict(base)
    payload.update({
        'periods': records,
        'commands': commands,
        'exposure_by_shelf': {ids[s]: final_exp[s] for s in range(n)},
        'totals': {
            'cost_cents': total_cost,
            'max_shelf_exposure_c_periods': max(_q(v) for v in final_exp),
            'total_exposure_c_periods': sum(_q(v) for v in final_exp),
        },
    })
    return payload


def _load_case(db, case_id):
    row = db.execute('SELECT payload FROM climate_case WHERE id=?', (case_id,)).fetchone()
    if row is None:
        raise ValueError('climate_case_unknown')
    envelope = json.loads(row['payload'])
    return envelope


def _digests(envelope):
    return {'case_sha256': envelope['case_sha256'],
            'sensor_sha256': envelope['sensor_sha256']}


def _source_summary(envelope):
    return {
        'case_fixture': envelope['case_path'],
        'sensor_csv': envelope['sensor_path'],
        'shelves': [{
            'shelf': item['shelf'],
            'sensor_at': item['sensor']['at'],
            'sensor_temperature_c': float(item['sensor']['temperature_c']),
            'observed_scans': [{'copy_id': inv['copy_id'], 'reader': inv['reader_id'],
                                'event_id': inv['event_id'], 'at': inv['at'],
                                'source_sha256': inv['source_sha256']}
                               for inv in item['inventory']],
        } for item in envelope['observed']],
    }


def _version_for(objective, serial=1):
    return f'v{serial}-{objective}'


def _bump_version(version):
    head, _, tail = version.partition('-')
    serial = int(head[1:]) if head.startswith('v') and head[1:].isdigit() else 1
    suffix = tail or ''
    return f'v{serial + 1}-{suffix}' if suffix else f'v{serial + 1}'


def plan(db_path, case_id, objective):
    """Solve and persist the v1 plan for a registered, fully observed case."""
    if objective not in OBJECTIVES:
        raise ValueError('climate_objective_unknown')
    db = connect(db_path)
    try:
        envelope = _load_case(db, case_id)
        case = envelope['case']
        version = _version_for(objective)
        payload = build_plan(
            case_id, case, objective, plan_version=version,
            input_digests=_digests(envelope), source_summary=_source_summary(envelope))
        _persist_plan(db, case_id, version, payload)
        return payload
    finally:
        db.close()


def _persist_plan(db, case_id, version, payload):
    with db:
        db.execute('INSERT OR REPLACE INTO climate_plan(case_id,plan_version,status,objective,payload) '
                   'VALUES (?,?,?,?,?)',
                   (case_id, version, payload['status'], payload['objective'],
                    json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))))
        db.execute('DELETE FROM climate_command WHERE case_id=? AND plan_version=?',
                   (case_id, version))
        for command in payload['commands']:
            db.execute('INSERT INTO climate_command VALUES (?,?,?,?,?,?,?)',
                       (case_id, version, command['period'], command['shelf'],
                        '待执行', command['price_cents'],
                        json.dumps(command, ensure_ascii=False, sort_keys=True)))


# ---------------------------------------------------------------- late feedback
def apply_feedback(db_path, case_id, base_version, objective, readings_raw,
                   through_period, new_version=None):
    """Freeze executed periods from real late readings, then re-solve the rest.

    Already executed commands and paid cents are never rewritten: their cost
    and the exposure measured at the real end temperatures are fixed.  Only
    unexecuted periods get new commands, under a new plan version.
    """
    if objective not in OBJECTIVES:
        raise ValueError('climate_objective_unknown')
    rows = list(csv.DictReader(io.StringIO(readings_raw.decode('utf-8-sig'))))
    readings_sha = hashlib.sha256(readings_raw).hexdigest()
    db = connect(db_path)
    try:
        envelope = _load_case(db, case_id)
        case = envelope['case']
        shape = validate_case(case)
        horizon = shape['horizon']
        cooling_c = shape['cooling_c']
        ids = [s['id'] for s in case['shelves']]
        limits = [s['limit_c'] for s in case['shelves']]
        tariff = case['cooler']['tariff_cents']
        base_row = db.execute(
            'SELECT status,payload FROM climate_plan WHERE case_id=? AND plan_version=?',
            (case_id, base_version)).fetchone()
        if base_row is None:
            raise ValueError('climate_plan_unknown')
        if base_row['status'] != '可执行':
            raise ValueError('climate_plan_not_actionable')
        base_payload = json.loads(base_row['payload'])
        if not isinstance(through_period, int) or not (0 <= through_period < horizon):
            raise ValueError('climate_feedback_period_invalid')
        if len(base_payload.get('periods', [])) != horizon:
            raise ValueError('climate_feedback_period_invalid')
        # One real reading per shelf per executed period, keyed by an explicit
        # period column; a missing reading is never substituted with zero.
        indexed = {}
        for row in rows:
            sid = row.get('shelf')
            if sid not in ids:
                raise ValueError('climate_sensor_mismatch')
            try:
                period = int(row.get('period'))
            except (TypeError, ValueError) as exc:
                raise ValueError('climate_feedback_period_invalid') from exc
            if not (0 <= period <= through_period):
                raise ValueError('climate_feedback_period_invalid')
            _when(row.get('at'))
            temperature = _finite_number(row.get('temperature_c'), 'climate_sensor_mismatch')
            key = (sid, period)
            if key in indexed:
                raise ValueError('climate_sensor_duplicate')
            indexed[key] = {'at': row['at'], 'temperature_c': temperature}
        for sid in ids:
            for period in range(through_period + 1):
                if (sid, period) not in indexed:
                    raise ValueError('climate_sensor_missing')
        actual = {period: {sid: indexed[(sid, period)]['temperature_c'] for sid in ids}
                  for period in range(through_period + 1)}

        existing = [r[0] for r in db.execute(
            'SELECT plan_version FROM climate_plan WHERE case_id=? ORDER BY plan_version',
            (case_id,))]
        if new_version is None:
            new_version = _bump_version(base_version)
        elif new_version in existing:
            raise ValueError('climate_plan_version_conflict')

        base_commands = {c['period']: c for c in base_payload['commands']}

        # Replay executed periods with the real end temperatures.
        temps = [float(s['initial_c']) for s in case['shelves']]
        fixed_exp = [0.0] * len(ids)
        fixed_cost = 0
        prefix = []
        for t in range(through_period + 1):
            start = list(temps)
            command = base_commands.get(t)
            action_index = ids.index(command['shelf']) if command else None
            model_end, period_exp = [], []
            for s, sid in enumerate(ids):
                modeled = start[s] + case['shelves'][s]['warming_c'][t] - (
                    cooling_c if s == action_index else 0.0)
                model_end.append(modeled)
                over = actual[t][sid] - limits[s]
                period_exp.append(over if over > 0 else 0.0)
                fixed_exp[s] += period_exp[-1]
            price = tariff[t] if command else 0
            fixed_cost += price
            prefix.append({
                'period': t,
                'start_temps': {ids[s]: start[s] for s in range(len(ids))},
                'start_temp_source': 'observed' if t == 0 else 'actual_feedback',
                'warming_c': {sid: case['shelves'][i]['warming_c'][t]
                              for i, sid in enumerate(ids)},
                'command_shelf': command['shelf'] if command else None,
                'cooler_occupied': command is not None,
                'model_end_temps': {ids[s]: model_end[s] for s in range(len(ids))},
                'end_temps': {sid: actual[t][sid] for sid in ids},
                'end_temp_source': 'actual_feedback',
                'period_exposure_c': {ids[s]: period_exp[s] for s in range(len(ids))},
                'cumulative_exposure_c': {ids[s]: fixed_exp[s] for s in range(len(ids))},
                'price_cents': price,
            })
            temps = [actual[t][sid] for sid in ids]

        feedback_meta = {
            'base_version': base_version,
            'new_version': new_version,
            'through_period': through_period,
            'readings_sha256': readings_sha,
            'frozen_cost_cents': fixed_cost,
            'frozen_exposure_c_periods': {ids[s]: fixed_exp[s] for s in range(len(ids))},
            'readings': [
                {'period': t, 'shelf': sid, 'at': indexed[(sid, t)]['at'],
                 'temperature_c': actual[t][sid]}
                for t in range(through_period + 1) for sid in ids],
        }
        payload = build_plan(
            case_id, case, objective, plan_version=new_version,
            start_period=through_period + 1, start_temps=temps,
            fixed_exposure=fixed_exp, prefix_records=prefix,
            fixed_cost=fixed_cost, feedback=feedback_meta,
            input_digests=dict(_digests(envelope), **{'feedback_sha256': readings_sha}),
            source_summary=_source_summary(envelope))

        with db:
            db.execute('UPDATE climate_command SET status=? WHERE case_id=? AND plan_version=? '
                       'AND period<=?', ('已执行', case_id, base_version, through_period))
            db.execute('UPDATE climate_command SET status=? WHERE case_id=? AND plan_version=? '
                       'AND period>?', ('已作废', case_id, base_version, through_period))
            _persist_plan(db, case_id, new_version, payload)
            db.execute('INSERT INTO climate_feedback(case_id,base_version,new_version,'
                       'through_period,readings_sha256,payload) VALUES (?,?,?,?,?,?)',
                       (case_id, base_version, new_version, through_period, readings_sha,
                        json.dumps(feedback_meta, ensure_ascii=False, sort_keys=True)))
        return payload
    finally:
        db.close()
