#!/usr/bin/env python3
"""
Integrations runner for Coupa, PIEE and SAM.gov.
- Reads ledger.csv and clearing/clearing_report.json from the --input directory
- Constructs minimal reporting payloads and POSTs them to configured endpoints
- Writes integrations_report.json and appends logs to integrations.log

Important: set API endpoints and keys as repository secrets and expose them into the workflow env:
- COUPA_API_URL, COUPA_API_KEY
- PIEE_API_URL, PIEE_API_KEY
- SAM_API_URL, SAM_API_KEY

This script is intentionally conservative: if an endpoint/key is not set it will skip that integration.
"""
import os
import sys
import csv
import json
import argparse
import time
from datetime import datetime
import requests


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--input', required=True, help='Path to output directory produced by processor')
    return p.parse_args()


def load_ledger(path):
    ledger = []
    ledger_path = os.path.join(path, 'ledger.csv')
    if not os.path.exists(ledger_path):
        return ledger
    with open(ledger_path, newline='') as f:
        reader = csv.DictReader(f)
        for r in reader:
            ledger.append(r)
    return ledger


def load_clearing(path):
    cr = os.path.join(path, 'clearing', 'clearing_report.json')
    if not os.path.exists(cr):
        return None
    with open(cr) as f:
        return json.load(f)


def post(url, key, payload):
    headers = {'Content-Type': 'application/json'}
    if key:
        headers['Authorization'] = f'Bearer {key}'
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
        return {'ok': resp.ok, 'status': resp.status_code, 'body': resp.text}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def run():
    args = parse_args()
    inp = args.input
    os.makedirs(inp, exist_ok=True)
    log_path = os.path.join(inp, 'integrations.log')

    def log(msg):
        timestamp = datetime.utcnow().isoformat() + 'Z'
        line = f'{timestamp} {msg}\n'
        with open(log_path, 'a') as lf:
            lf.write(line)
        print(line, end='')

    ledger = load_ledger(inp)
    clearing = load_clearing(inp)
    report = {'run_ts': datetime.utcnow().isoformat() + 'Z', 'counts': {'ledger_rows': len(ledger)}, 'results': {}}

    # Environment / enterprise identifiers to include in payloads
    enterprise = {
        'uei': os.getenv('UEI', ''),
        'cage': os.getenv('CAGE_CODE', ''),
        'dodaac_contracting': os.getenv('DODAAC_CONTRACTING', ''),
        'dodaac_funding': os.getenv('DODAAC_FUNDING', ''),
        'paying_dodaac': os.getenv('PAYING_DODAAC', ''),
        'fedstrip': os.getenv('FEDSTRIP', ''),
        'finance_unitid': os.getenv('FINANCE_UNITID', ''),
        'cag_code': os.getenv('CAG_CODE', ''),
        'ba_codes': os.getenv('BA_CODES', ''),
        'scf_code': os.getenv('SCF_CODE', ''),
        'district_cd': os.getenv('DISTRICT_CD', ''),
        'eps': os.getenv('EPS', '')
    }
    report['enterprise'] = enterprise

    # Validate critical enterprise identifiers
    missing = []
    if not enterprise.get('uei'):
        missing.append('UEI')
    if not enterprise.get('cage'):
        missing.append('CAGE_CODE')
    if missing:
        # write log and fail-fast to avoid posting incomplete authoritative reports
        log(f"Missing required enterprise identifiers: {', '.join(missing)}. Aborting integrations.")
        # Save partial report for troubleshooting
        out_report = os.path.join(inp, 'integrations_report.json')
        with open(out_report, 'w') as of:
            json.dump({'error': 'missing_identifiers', 'missing': missing, 'run_ts': report['run_ts']}, of, indent=2)
        sys.exit(3)

    # Warn about recommended but non-critical fields
    recommended = ['dodaac_contracting','paying_dodaac','fedstrip','finance_unitid']
    warn_missing = [k for k in recommended if not enterprise.get(k)]
    if warn_missing:
        log(f"Warning: recommended enterprise identifiers missing: {', '.join(warn_missing)}. Reports will proceed but may be incomplete.")

    # Coupa
    coupa_url = os.getenv('COUPA_API_URL')
    coupa_key = os.getenv('COUPA_API_KEY')
    if coupa_url and coupa_key:
        log('Posting to Coupa...')
        # Build a minimal summary payload; adapt to your Coupa schema
        payload = {
            'summary_ts': report['run_ts'],
            'source': 'vbtn_email_processor',
            'enterprise': enterprise,
            'items': [
                {'message_id': r.get('message_id'), 'vendor': r.get('from'), 'amount': r.get('amount'), 'currency': r.get('currency'), 'subject': r.get('subject')} for r in ledger
            ]
        }
        res = post(coupa_url, coupa_key, payload)
        report['results']['coupa'] = res
        log(f'Coupa response: {res}')
    else:
        log('Skipping Coupa: credentials or URL not set')

    # PIEE
    piee_url = os.getenv('PIEE_API_URL')
    piee_key = os.getenv('PIEE_API_KEY')
    if piee_url and piee_key:
        log('Posting to PIEE...')
        payload = {
            'report_ts': report['run_ts'],
            'enterprise': enterprise,
            'items_count': len(ledger),
            'total_amount': sum([float(r.get('amount') or 0) for r in ledger])
        }
        res = post(piee_url, piee_key, payload)
        report['results']['piee'] = res
        log(f'PIEE response: {res}')
    else:
        log('Skipping PIEE: credentials or URL not set')

    # SAM.gov
    sam_url = os.getenv('SAM_API_URL')
    sam_key = os.getenv('SAM_API_KEY')
    if sam_url and sam_key:
        log('Posting to SAM.gov...')
        payload = {
            'uei': enterprise.get('uei'),
            'cage': enterprise.get('cage'),
            'dodaac_contracting': enterprise.get('dodaac_contracting'),
            'dodaac_funding': enterprise.get('dodaac_funding'),
            'paying_dodaac': enterprise.get('paying_dodaac'),
            'fedstrip': enterprise.get('fedstrip'),
            'finance_unitid': enterprise.get('finance_unitid'),
            'cag_code': enterprise.get('cag_code'),
            'ba_codes': enterprise.get('ba_codes'),
            'scf_code': enterprise.get('scf_code'),
            'district_cd': enterprise.get('district_cd'),
            'eps': enterprise.get('eps'),
            'items': len(ledger),
            'timestamp': report['run_ts']
        }
        res = post(sam_url, sam_key, payload)
        report['results']['sam'] = res
        log(f'SAM response: {res}')
    else:
        log('Skipping SAM.gov: credentials or URL not set')

    # Save report
    out_report = os.path.join(inp, 'integrations_report.json')
    with open(out_report, 'w') as of:
        json.dump(report, of, indent=2)
    log(f'Wrote integrations_report.json ({out_report})')
    return 0


if __name__ == '__main__':
    run()
