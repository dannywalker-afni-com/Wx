import csv
import os
import time
import random
import requests
from typing import Dict, Any, List
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- Config ----------
BASE = "https://webexapis.com/v1/people"
INPUT_EMAILS = "emails.csv"        # input with header: email
OUT_PERSONIDS = "personid.csv"     # output with headers: email, personid
OUT_DETAILS = "persondetail.csv"   # output: flattened columns from people details
REQUEST_TIMEOUT = 15               # seconds

# Assign your Webex Bearer Token directly here ↓↓↓
bearer = "YWViZTAwZjEtZGIwZi00NjEzLWFiNTgtZGE2MzAzZjllYjgxM2MxMWY4NzktZWM2_PF84_ebc31646-bc26-4a20-8500-c3030ebd6a52"

# ---------- Helpers ----------
def make_session() -> requests.Session:
    retry = Retry(
        total=6, connect=6, read=6, status=6,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods={"GET"},
        respect_retry_after_header=True,
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=1, pool_maxsize=10))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s

def jitter_sleep(base: float) -> None:
    time.sleep(base * random.uniform(0.8, 1.3))

def get_person_id_by_email(session: requests.Session, headers: Dict[str, str], email: str) -> str:
    r = session.get(BASE, headers=headers, params={"email": email}, timeout=REQUEST_TIMEOUT)
    if r.status_code == 200:
        data = r.json() if r.content else {}
        items = data.get("items") or []
        return items[0]["id"] if items else "NOT_FOUND"
    if r.status_code == 429:
        wait = int(r.headers.get("Retry-After", "1"))
        time.sleep(wait)
        r = session.get(BASE, headers=headers, params={"email": email}, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            data = r.json() if r.content else {}
            items = data.get("items") or []
            return items[0]["id"] if items else "NOT_FOUND"
    return f"ERROR_{r.status_code}"

def get_person_details(session: requests.Session, headers: Dict[str, str], person_id: str) -> Dict[str, Any]:
    url = f"{BASE}/{person_id}"
    r = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    if r.status_code == 429:
        wait = int(r.headers.get("Retry-After", "1"))
        time.sleep(wait)
        r = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json() if r.content else {}

def flatten(prefix: str, obj: Any, out: Dict[str, Any]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else k
            flatten(key, v, out)
    elif isinstance(obj, list):
        for idx, v in enumerate(obj):
            key = f"{prefix}[{idx}]"
            flatten(key, v, out)
    else:
        out[prefix] = obj

def write_csv_atomic(path: str, fieldnames: List[str], rows: List[Dict[str, Any]]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)
    os.replace(tmp, path)

# ---------- Main ----------
def main():
    print("\nYou are running userDetails.py\n")

    headers = {"Authorization": f"Bearer {bearer}"}
    session = make_session()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    emails_csv = os.path.join(script_dir, INPUT_EMAILS)
    personids_csv = os.path.join(script_dir, OUT_PERSONIDS)
    details_csv = os.path.join(script_dir, OUT_DETAILS)

    # Phase 1: read emails.csv fully into rows, then close the file
    if not os.path.exists(emails_csv):
        print(f"Error: '{emails_csv}' not found in script directory.")
        return

    with open(emails_csv, "r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        if "email" not in (reader.fieldnames or []):
            print("Error: CSV must contain header 'email'.")
            return
        email_rows = list(reader)

    # For each row, resolve personId; write to personid.csv (email,personid)
    pid_rows = []
    print(f"\n[Phase 1] Resolving personId for {len(email_rows)} emails…\n")
    for i, row in enumerate(email_rows, start=1):
        email = (row.get("email") or "").strip()
        if not email:
            print(f"{i}. (blank email) → skipped")
            continue

        person_id = None
        for attempt in range(1, 4):
            try:
                person_id = get_person_id_by_email(session, headers, email)
                break
            except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
                if attempt < 3:
                    print(f"{i}. {email} → transient network/SSL issue, retrying ({attempt}/3)…")
                    jitter_sleep(0.7 * attempt)
                else:
                    print(f"{i}. {email} → ERROR_CONNECT ({e})")
                    person_id = "ERROR_CONNECT"

        print(f"{i}. {email} → {person_id}")
        pid_rows.append({"email": email, "personid": person_id})
        jitter_sleep(0.15)

    write_csv_atomic(personids_csv, ["email", "personid"], pid_rows)
    print(f"\n[Phase 1] Wrote person IDs to '{personids_csv}'.\n")

    # Phase 2: read personid.csv into rows, then fetch person details for each id
    with open(personids_csv, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not {"email", "personid"}.issubset(set(reader.fieldnames or [])):
            print("Error: personid.csv must contain headers 'email' and 'personid'.")
            return
        rows = list(reader)

    print(f"[Phase 2] Fetching person details for {len(rows)} entries…\n")
    flattened_rows: List[Dict[str, Any]] = []
    for i, row in enumerate(rows, start=1):
        email = (row.get("email") or "").strip()
        person_id = (row.get("personid") or "").strip()

        if not person_id or person_id in ("NOT_FOUND", "ERROR_CONNECT") or person_id.startswith("ERROR_"):
            print(f"{i}. {email} → skipped (personid={person_id})")
            continue

        details = None
        for attempt in range(1, 4):
            try:
                details = get_person_details(session, headers, person_id)
                break
            except requests.exceptions.HTTPError as he:
                status = getattr(he.response, "status_code", "HTTP_ERR")
                print(f"{i}. {email} ({person_id}) → HTTP {status} (missing scope or not found)")
                details = {"email": email, "personId": person_id, "error": f"HTTP_{status}"}
                break
            except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
                if attempt < 3:
                    print(f"{i}. {email} → transient network/SSL issue, retrying ({attempt}/3)…")
                    jitter_sleep(0.8 * attempt)
                else:
                    print(f"{i}. {email} → ERROR_CONNECT ({e})")
                    details = {"email": email, "personId": person_id, "error": "ERROR_CONNECT"}

        if details is None:
            details = {"email": email, "personId": person_id, "error": "UNKNOWN"}

        flat: Dict[str, Any] = {}
        flat["email"] = email
        flat["personId"] = person_id
        flatten("", details, flat)
        flattened_rows.append(flat)

        print(f"{i}. {email} → details captured")
        jitter_sleep(0.15)

    all_keys = set()
    for r in flattened_rows:
        all_keys.update(r.keys())
    ordered = ["email", "personId"]
    remaining = sorted(k for k in all_keys if k not in ordered)
    fieldnames = ordered + remaining

    write_csv_atomic(details_csv, fieldnames, flattened_rows)
    print(f"\n[Phase 2] Wrote person details (including calling data when available) to '{details_csv}'.\n")

if __name__ == "__main__":
    main()
