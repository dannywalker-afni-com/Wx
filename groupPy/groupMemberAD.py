#!/usr/bin/env python3
"""
Made these changes to branch dww202311070933

Bulk manage Webex Groups members from group_members.csv

Input CSV columns (minimum):
  groupId,groupName,personId,displayName,email,action

Behavior:
  - Blank action  -> NO-OP: list info (console + log).
  - 'a'/'A'       -> Add member to group (PUT /v1/groups/{groupId}/members/{personId}).
  - 'd'/'D'       -> Delete member from group (DELETE /v1/groups/{groupId}/members/{personId}).
  - Anything else -> Invalid action (logged as skip) — no 'U' support.

ENV (required):
  WEBEX_TOKEN   = admin OAuth token
                  (identity:groups_read, identity:groups_write, spark-admin:people_read)
  WEBEX_ORG_ID  = (recommended) customer org id
  WEBEX_BASE    = https://webexapis.com  (FedRAMP: https://api-usgov.webex.com)

TLS (optional):
  REQUESTS_CA_BUNDLE or SSL_CERT_FILE = path to PEM bundle
  WEBEX_VERIFY=false  -> temporarily disable TLS verification (testing only)
"""

import os
import sys
import csv
import time
import json
import traceback
import requests
from typing import Dict, Any, Optional, Tuple, List
from urllib.parse import quote

# -------------------- Config --------------------
BASE    = os.getenv("WEBEX_BASE", "https://webexapis.com").rstrip("/")
TOKEN   = (os.getenv("WEBEX_TOKEN") or "").strip()
ORG_ID  = (os.getenv("WEBEX_ORG_ID") or "").strip()
TIMEOUT = 30

INPUT_CSV = "group_members.csv"
LOG_CSV   = "groupMemberACDLog.csv"

# TLS toggle / bundle
CA_BUNDLE = os.getenv("REQUESTS_CA_BUNDLE") or os.getenv("SSL_CERT_FILE")
VERIFY_TLS = CA_BUNDLE if CA_BUNDLE else (os.getenv("WEBEX_VERIFY", "true").lower() != "false")
print(f"[TLS] verify={VERIFY_TLS if isinstance(VERIFY_TLS, bool) else 'file:' + VERIFY_TLS}", flush=True)

# -------------- HTTP helpers / retry ------------
def die(msg: str) -> None:
    print(f"ERROR: {msg}", flush=True)
    sys.exit(1)

def hdrs(json_ct: bool = True) -> Dict[str, str]:
    if not TOKEN:
        die("Set WEBEX_TOKEN first.")
    h = {"Authorization": f"Bearer {TOKEN}"}
    if json_ct:
        h["Content-Type"] = "application/json"
    return h

def backoff_request(method: str, url: str, **kwargs) -> requests.Response:
    backoff = 1.0
    kwargs.setdefault("verify", VERIFY_TLS)
    for _ in range(6):
        r = requests.request(method, url, timeout=TIMEOUT, **kwargs)
        if r.status_code in (429, 502, 503, 504):
            wait = float(r.headers.get("Retry-After", backoff))
            time.sleep(min(wait, 10.0))
            backoff *= 1.6
            continue
        return r
    return r

# ---------------- Utilities / Group helpers ----------------
def normalize_group_id(group_id: str) -> str:
    """
    Identity sometimes returns a composite id like:
    Y2lz.../SCIM_GROUP/<uuid>:<orgId>
    For member operations, use ONLY the opaque id before the colon.
    """
    if not group_id:
        return group_id
    parts = group_id.split(":")
    return parts[0] if len(parts) > 1 else group_id

def get_group(group_id: str) -> Optional[Dict[str, Any]]:
    gid = normalize_group_id(group_id)
    url = f"{BASE}/v1/groups/{quote(gid, safe='')}"
    params = {}
    if ORG_ID:
        params["orgId"] = ORG_ID
    r = backoff_request("GET", url, headers=hdrs(False), params=params)
    if r.status_code == 200:
        return r.json()
    return None

def group_is_writable(group_id: str) -> Tuple[bool, str]:
    g = get_group(group_id)
    if not g:
        return (False, "group not found (id/org mismatch or no access)")
    src = (g.get("source") or "").lower()
    if src and src != "local":
        return (False, f"group source is '{src}' (directory/SCIM-synced); membership is read-only")
    return (True, "ok")

# ---------------- People helpers --------------------
_PERSON_EMAIL_CACHE: Dict[str, str] = {}

def resolve_person(person_id: Optional[str], email: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (personId, email). If personId missing but email present, lookup by email.
    Read-only; does not modify user profiles.
    """
    if person_id:
        # Best-effort: fill email from cache if missing (optional)
        if not email and person_id in _PERSON_EMAIL_CACHE:
            email = _PERSON_EMAIL_CACHE[person_id]
        return (person_id, email)

    if not email:
        return (None, None)

    params = {"email": email}
    if ORG_ID:
        params["orgId"] = ORG_ID
    r = backoff_request("GET", f"{BASE}/v1/people", headers=hdrs(False), params=params)
    if r.status_code == 200:
        data = r.json().get("items", [])
        if data:
            pid = data[0].get("id")
            ems = data[0].get("emails") or []
            found_email = ems[0] if ems else email
            _PERSON_EMAIL_CACHE[pid] = found_email
            return (pid, found_email)
    return (None, email)

# ---------- Group membership helpers ----------
def is_member(group_id: str, person_id: str) -> bool:
    gid = normalize_group_id(group_id)
    params = {"itemsPerPage": 200, "startIndex": 1}
    if ORG_ID:
        params["orgId"] = ORG_ID
    while True:
        r = backoff_request("GET", f"{BASE}/v1/groups/{quote(gid, safe='')}/members", headers=hdrs(False), params=params)
        if r.status_code != 200:
            return False
        data = r.json()
        members = data.get("members") or data.get("items") or data.get("users") or []
        for m in members:
            if (m.get("personId") or m.get("id")) == person_id:
                return True
        total = int(data.get("totalResults", 0) or 0)
        per   = int(data.get("itemsPerPage", len(members)) or 0)
        start = int(data.get("startIndex", params["startIndex"]))
        if total and (start + per) <= total:
            params["startIndex"] = start + per
        else:
            break
    return False

def add_member(group_id: str, person_id: str) -> Tuple[bool, str]:
    """
    Identity Groups add = PUT member resource (NOT POST /members).
    """
    ok, why = group_is_writable(group_id)
    if not ok:
        return (False, why)

    gid = normalize_group_id(group_id)
    url = f"{BASE}/v1/groups/{quote(gid, safe='')}/members/{quote(person_id, safe='')}"
    params = {}
    if ORG_ID:
        params["orgId"] = ORG_ID

    r = backoff_request("PUT", url, headers=hdrs(), params=params, data="{}")
    if r.status_code in (200, 201, 204):
        return (True, "Added to group")
    if r.status_code == 409:
        return (True, "Already a member")
    try:
        err = r.json()
    except Exception:
        err = r.text
    return (False, f"Add failed ({r.status_code}): {err}")

def delete_member(group_id: str, person_id: str) -> Tuple[bool, str]:
    gid = normalize_group_id(group_id)
    url = f"{BASE}/v1/groups/{quote(gid, safe='')}/members/{quote(person_id, safe='')}"
    params = {}
    if ORG_ID:
        params["orgId"] = ORG_ID

    r = backoff_request("DELETE", url, headers=hdrs(False), params=params)
    if r.status_code in (200, 204):
        return (True, "Removed from group")
    if r.status_code == 404:
        if not is_member(gid, person_id):
            return (True, "Not a member (no-op)")
    try:
        err = r.json()
    except Exception:
        err = r.text
    return (False, f"Delete failed ({r.status_code}): {err}")

# ---------------- Logging -----------------------
def write_log(ops: List[Dict[str, Any]]) -> None:
    try:
        with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=[
                "row","action","groupId","groupName","personId","displayName",
                "email","result","detail"
            ])
            w.writeheader()
            w.writerows(ops)
        print(f"\n✅ Log written to {LOG_CSV}", flush=True)
    except Exception as e:
        print(f"ERROR writing log: {e}", flush=True)
        traceback.print_exc()

# ---------------- Core --------------------------
def main() -> List[Dict[str, Any]]:
    if not os.path.exists(INPUT_CSV):
        die(f"Missing input file: {INPUT_CSV}")

    print(f"Logging to: {LOG_CSV}", flush=True)

    ops: List[Dict[str, Any]] = []

    with open(INPUT_CSV, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        rows = list(r)

    print(f"🔧 Processing {len(rows)} rows from {INPUT_CSV} ...\n", flush=True)

    for i, row in enumerate(rows, 1):
        group_id_raw = (row.get("groupId") or "").strip()
        group_id     = normalize_group_id(group_id_raw)
        group_name   = (row.get("groupName") or "").strip()
        person_id    = (row.get("personId") or "").strip()
        display      = (row.get("displayName") or "").strip()
        email        = (row.get("email") or "").strip()
        action_raw   = (row.get("action") or "").strip()
        action       = action_raw[:1].lower() if action_raw else ""

        # ---------- NO-OP ----------
        if not action:
            msg = f"[{i}] 🔎 No action — {group_name or group_id} | {display or person_id} <{email}>"
            print(msg, flush=True)
            ops.append({
                "row": i, "action": "(none)", "groupId": group_id, "groupName": group_name,
                "personId": person_id, "displayName": display, "email": email,
                "result": "noop", "detail": "no action (blank)"
            })
            continue

        # ---------- Invalid ----------
        if action not in ("a", "d"):
            print(f"[{i}] ⚠️ Invalid action '{action_raw}' — expected A/D or blank", flush=True)
            ops.append({
                "row": i, "action": action_raw, "groupId": group_id, "groupName": group_name,
                "personId": person_id, "displayName": display, "email": email,
                "result": "skip", "detail": "invalid action (use A/D or blank)"
            })
            continue

        # ---------- Resolve Person ----------
        pid, resolved_email = resolve_person(person_id, email)
        person_id = pid or person_id
        email     = resolved_email or email

        # ---------- Add ----------
        if action == "a":
            if not group_id or not person_id:
                print(f"[{i}] ❌ Add requires groupId and personId/email", flush=True)
                ops.append({
                    "row": i, "action": "A", "groupId": group_id, "groupName": group_name,
                    "personId": person_id, "displayName": display, "email": email,
                    "result": "error", "detail": "groupId and personId/email required"
                })
                continue

            ok, detail = add_member(group_id, person_id)
            status = "ok" if ok else "error"
            print(f"[{i}] ➕ Add | {group_name or group_id} ← {email or person_id}: {detail}", flush=True)

            ops.append({
                "row": i, "action": "A", "groupId": group_id, "groupName": group_name,
                "personId": person_id, "displayName": display, "email": email,
                "result": status, "detail": detail
            })
            continue

        # ---------- Delete ----------
        if action == "d":
            if not group_id or not person_id:
                print(f"[{i}] ❌ Delete requires groupId and personId/email", flush=True)
                ops.append({
                    "row": i, "action": "D", "groupId": group_id, "groupName": group_name,
                    "personId": person_id, "displayName": display, "email": email,
                    "result": "error", "detail": "groupId and personId/email required"
                })
                continue

            ok, detail = delete_member(group_id, person_id)
            status = "ok" if ok else "error"
            print(f"[{i}] ➖ Del | {group_name or group_id} × {email or person_id}: {detail}", flush=True)

            ops.append({
                "row": i, "action": "D", "groupId": group_id, "groupName": group_name,
                "personId": person_id, "displayName": display, "email": email,
                "result": status, "detail": detail
            })
            continue

    print("\n✅ Processing complete.", flush=True)
    return ops

# --------------- Entry point ---------------------
if __name__ == "__main__":
    all_ops: List[Dict[str, Any]] = []
    try:
        all_ops = main()
    except SystemExit:
        pass
    except Exception as e:
        print("ERROR:", e, flush=True)
        traceback.print_exc()
    finally:
        write_log(all_ops if isinstance(all_ops, list) else [])
