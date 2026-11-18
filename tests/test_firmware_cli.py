"""End-to-end CLI coverage for the upgrade/recovery commands."""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / 'fixtures'


def run_cli(*args):
    proc = subprocess.run([sys.executable, '-B', '-m', 'shelfscan', *args],
                          cwd=ROOT, capture_output=True, text=True, check=True)
    return proc.stdout.strip()


def test_firmware_cli_full_lifecycle(tmp_path):
    db = tmp_path / 'cli.db'
    out = run_cli('fw-seed', '--db', str(db),
                  '--config', str(FIX / 'firmware-config.json'),
                  '--events', str(FIX / 'firmware-events.jsonl'),
                  '--case', str(FIX / 'firmware-case.json'))
    assert json.loads(out)['readers'] == 3
    run_cli('fw-plan', '--db', str(db), '--intent', 'cli-intent',
            '--contract', 'atomic_fleet')
    summary = json.loads(run_cli('fw-run', '--db', str(db), '--intent', 'cli-intent'))
    assert summary['status'] == 'rejected'
    report = tmp_path / 'cli-report.json'
    run_cli('fw-report', '--db', str(db), '--intent', 'cli-intent', '--out', str(report))
    doc = json.loads(report.read_text(encoding='utf-8'))
    assert [d['device'] for d in doc['devices']] == ['R1', 'R2', 'R3']
    assert all(d['live']['version'] == 'v1' for d in doc['devices'])
    assert (tmp_path / 'cli-report.sha256').exists()
    assert (tmp_path / 'cli-report.md').exists()


def test_firmware_demo_diverges_by_contract(tmp_path):
    out = json.loads(run_cli('fw-demo', '--workdir', str(tmp_path)))
    assert set(out['contracts']) == {'atomic_fleet', 'cell_isolated'}
    atomic = json.loads((tmp_path / 'atomic_fleet-report.json').read_text(encoding='utf-8'))
    isolated = json.loads((tmp_path / 'cell_isolated-report.json').read_text(encoding='utf-8'))
    assert {d['device']: d['live']['version'] for d in atomic['devices']} == \
        {'R1': 'v1', 'R2': 'v1', 'R3': 'v1'}
    assert {d['device']: d['live']['version'] for d in isolated['devices']} == \
        {'R1': 'v2', 'R2': 'v2', 'R3': 'v1'}
