#!/usr/bin/env python3
"""
Nightly audit script:
- Scans mailboxes for TARGET_RECIPIENTS for the day (or since a given date)
- Collects SMS via Twilio (messages sent/received for configured numbers)
- Collects HP Fax emails (by scanning hp-fax inbound mailbox or provided HP_FAX_EMAIL)
- Queries Apple MDM inventory (if configured) to pull device list and metadata
- Aggregates all artifacts, metadata, and telemetry into a daily or backfill folder and index
- Optionally uploads to CLOUD_ARCHIVE_URL with CLOUD_ARCHIVE_KEY
"""
import os
import sys
import argparse
import json
from datetime import datetime, timedelta, date
import mailparser
from imapclient import IMAPClient
import requests


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--output-dir', required=True)
    p.add_argument('--targets', required=True)
    p.add_argument('--since', required=False, help='Start date (YYYY-MM-DD) to pull from; defaults to today')
    return p.parse_args()


def ensure_dir(p):
    os.makedirs(p, exist_ok=True)


def fetch_emails(imap_host, user, pwd, port, targets, since_date, out_dir):
    client = IMAPClient(imap_host, port=port, use_uid=True, ssl=True)
    client.login(user, pwd)
    client.select_folder('INBOX')
    # search since since_date
    criteria = ['SINCE', since_date.strftime('%d-%b-%Y')]
    msgs = client.search(criteria)
    results = []
    for uid, data in client.fetch(msgs, ['RFC822']).items():
        raw = data[b'RFC822']
        parsed = mailparser.parse_from_bytes(raw)
        recipients = [r.lower() for r in (parsed.to or [])]
        if any(t in recipients for t in targets):
            # save raw
            path = os.path.join(out_dir, f'{uid}.eml')
            with open(path, 'wb') as f:
                f.write(raw)
            results.append({'uid': uid, 'subject': parsed.subject, 'from': parsed.from_, 'to': parsed.to, 'date': parsed.date})
    client.logout()
    return results


def fetch_twilio_messages(sid, token, phone_numbers, since_date, out_dir):
    if not sid or not token:
        return []
    from twilio.rest import Client
    client = Client(sid, token)
    results = []
    for num in phone_numbers:
        # Twilio Python client supports date_sent filtering; convert since_date to date
        msgs = client.messages.list(to=num, date_sent_after=since_date)
        for m in msgs:
            results.append({'sid': m.sid, 'from': m.from_, 'to': m.to, 'body': m.body, 'date_sent': str(m.date_sent)})
    # write to file
    with open(os.path.join(out_dir, 'sms.json'), 'w') as f:
        json.dump(results, f, indent=2)
    return results


def fetch_apple_mdm(api_url, api_key, out_dir):
    if not api_url or not api_key:
        return None
    headers = {'Authorization': f'Bearer {api_key}', 'Accept': 'application/json'}
    try:
        r = requests.get(api_url.rstrip('/') + '/devices', headers=headers, timeout=30)
        if r.ok:
            devices = r.json()
            with open(os.path.join(out_dir, 'devices.json'), 'w') as f:
                json.dump(devices, f, indent=2)
            return devices
        else:
            return {'error': f'status {r.status_code}', 'body': r.text}
    except Exception as e:
        return {'error': str(e)}


def run():
    args = parse_args()
    out_base = args.output_dir
    targets = [t.strip().lower() for t in args.targets.split(',')]
    ensure_dir(out_base)
    daily_root = os.path.join(out_base, 'daily')
    ensure_dir(daily_root)

    # Determine since date
    if args.since:
        try:
            since_dt = datetime.strptime(args.since, '%Y-%m-%d').date()
        except Exception:
            print('Invalid --since date format; use YYYY-MM-DD', file=sys.stderr)
            sys.exit(2)
    else:
        since_dt = datetime.utcnow().date()

    # For backfill we create a folder per run with the since date in the name
    run_folder = os.path.join(daily_root, f'from-{since_dt.isoformat()}')
    ensure_dir(run_folder)

    # env
    imap_host = os.getenv('IMAP_HOST')
    imap_user = os.getenv('IMAP_USER')
    imap_pass = os.getenv('IMAP_PASSWORD')
    imap_port = int(os.getenv('IMAP_PORT', '993'))
    twilio_sid = os.getenv('TWILIO_SID')
    twilio_token = os.getenv('TWILIO_TOKEN')
    # include requested phone by default
    phone_nums_env = os.getenv('PHONE_NUMBERS','')
    phone_nums = [p.strip() for p in phone_nums_env.split(',') if p.strip()]
    if '2704018770' not in phone_nums:
        phone_nums.append('2704018770')
    # HP fax and MDM
    hp_fax_email = os.getenv('HP_FAX_EMAIL')
    apple_mdm_url = os.getenv('APPLE_MDM_API_URL')
    apple_mdm_key = os.getenv('APPLE_MDM_API_KEY')

    # fetch emails since date
    emails = []
    try:
        emails = fetch_emails(imap_host, imap_user, imap_pass, imap_port, targets, since_dt, run_folder)
    except Exception as e:
        print('Email fetch failed', e)

    # fetch sms
    sms = fetch_twilio_messages(twilio_sid, twilio_token, phone_nums, since_dt, run_folder)

    # fetch apple mdm inventory
    devices = fetch_apple_mdm(apple_mdm_url, apple_mdm_key, run_folder)

    # assemble index
    index = {'run_from': since_dt.isoformat(), 'generated_ts': datetime.utcnow().isoformat()+'Z', 'emails_count': len(emails), 'sms_count': len(sms), 'devices_count': (len(devices) if isinstance(devices, list) else 0)}
    with open(os.path.join(run_folder, 'index.json'), 'w') as f:
        json.dump(index, f, indent=2)

    # option upload
    cloud_url = os.getenv('CLOUD_ARCHIVE_URL')
    cloud_key = os.getenv('CLOUD_ARCHIVE_KEY')
    if cloud_url and cloud_key:
        files = {'file': open(os.path.join(run_folder, 'index.json'),'rb')}
        headers = {'Authorization': f'Bearer {cloud_key}'}
        try:
            r = requests.post(cloud_url, files=files, headers=headers)
            print('Upload status', r.status_code)
        except Exception as e:
            print('Upload failed', e)
    print('Nightly audit done')

if __name__ == '__main__':
    run()
