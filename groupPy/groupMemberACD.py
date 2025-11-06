#!/usr/bin/env python3
"""
Bulk manage Webex Groups members from group_members.csv

Input CSV columns (minimum):
  groupId,groupName,personId,displayName,email,action,newDisplayName,newEmail

Action rules:
  - Blank action  -> NO-OP: list info (console + log).
  - 'u'/'U'       -> Update user (display name/email; blank new* ignored).
  - 'a'/'A'       -> Add member to group.
  - 'd'/'D'       -> Delete member from group.

ENV:
  WEBEX_TOKEN   = admin OAuth token
                  (identity:groups_read, identity:groups_write,
                   spark-admin:people_read, spark-admin:people_write)
  WEBEX_ORG_ID  = (recommended) customer org id
  WEBEX_BASE    = https://webexapis.com   (FedRAMP: https://api-usgov.webex.com)
"""

import os
import sys
import csv
import time
import json
import traceback
import requests
from typing import Dict, Any, Optional, Tuple, List

# -------------------- Config --------------------
BASE    = os.getenv("WEBEX_BASE", "https://webexapis.com").rstrip("/")
TOKEN   = (os.getenv("WEBEX_TOKEN") or "").strip()
ORG_ID  = (os.getenv("WEBEX_ORG_ID") or "").strip()
TIMEOUT = 30

INPUT_CSV = "group_members.csv"
LOG_CSV   = "groupMemberACDLog.csv"

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
    for _ in range(6):
        r = requests.request(method, url, timeout=TIMEOUT, **kwargs)
        if r.status_code in (429, 502, 503, 504):
            wait = float(r.headers.get("Retry-After", backoff))
            time.sleep(min(wait, 10.0))
            backoff *= 1.6
            continue
        return r
    return r

# -------------- People helpers ------------------
_PERSON_EMAIL_CACHE: Dict[str, str] = {}

def resolve_person(person_id: Optional[str], email: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (personId, email). If personId missing but email present, lookup by email.
    If email missing but personId present, later fill from cache/people/{id}.
    """
    if person_id:
        # Ensure we have email if needed later
        if not email:
            email = get_email_for_person(person_id)
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

def get_email_for_person(person_id: str) -> str:
    if not person_id:
        return ""
    if person_id in _PERSON_EMAIL_CACHE:
        return _PERSON_EMAIL_CACHE[person_id]

    params = {}
    if ORG_ID:
        params["orgId"] = ORG_ID
    r = backoff_request("GET", f"{BASE}/v1/people/{person_id}", headers=hdrs(False), params=params)
    if r.status_code == 200:
        data = r.json()
        emails = data.get("emails") or []
        email = emails[0] if emails else ""
        if email:
            _PERSON_EMAIL_CACHE[person_id] = email
        return email
    return ""

def update_person(person_id: str, new_display: Optional[str], new_email: Optional[str]) -> Tuple[bool, str]:
    """Update displayName and/or email via People API. Ignores blanks."""
    if not person_id:
        return (False, "personId required for update_person()")

    body: Dict[str, Any] = {}
    if new_display:
        body["displayName"] = new_display
    if new_email:
        body["emails"] = [new_email]

    if not body:
        return (True, "No-op: nothing to update")

    r = backoff_request("PUT", f"{BASE}/v1/people/{person_id}", headers=hdrs(), data=json.dumps(body))
    if r.status_code in (200, 204):
        # refresh cache if email changed
        if new_email:
            _PERSON_EMAIL_CACHE[person_id] = new_email
        return (True, "Updated user profile")
    try:
        err = r.json()
    except Exception:
        err = r.text
    return (False, f"People update failed ({r.status_code}): {err}")

# ---------- Group membership helpers (Identity) ----------
def is_member(group_id: str, person_id: str) -> bool:
    params = {"itemsPerPage": 200, "startIndex": 1}
    if ORG_ID:
        params["orgId"] = ORG_ID
    while True:
        r = backoff_request("GET", f"{BASE}/v1/groups/{group_id}/members", headers=hdrs(False), params=params)
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
    body = {"personId": person_id}
    if ORG_ID:
        body["orgId"] = ORG_ID
    r = backoff_request("POST", f"{BASE}/v1/groups/{group_id}/members", headers=hdrs(), data=json.dumps(body))
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
    url = f"{BASE}/v1/groups/{group_id}/members/{person_id}"
    if ORG_ID:
        url += f"?orgId={ORG_ID}"
    r = backoff_request("DELETE", url, headers=hdrs(False))
    if r.status_code in (200, 204):
        return (True, "Removed from group")
    if r.status_code == 404:
        # If not actually a member, treat as no-op
        if not is_member(group_id, person_id):
            return (True, "Not a member (no-op)")
    try:
        err = r.json()
    except Exception:
        err = r.text
    return (False, f"Delete failed ({r.status_code}): {err}")

# -------------------- Core ----------------------
def write_log(ops: List[Dict[str, Any]]) -> None:
    try:
        with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=[
                "row","action","groupId","groupName","personId","email","result","detail"
            ])
            w.writeheader()
            w.writerows(ops)
        print(f"\n✅ Log written to {LOG_CSV}", flush=True)
    except Exception as e:
        print(f"ERROR writing log: {e}", flush=True)
        traceback.print_exc()

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
        group_id   = (row.get("groupId") or "").strip()
        group_name = (row.get("groupName") or "").strip()
        person_id  = (row.get("personId") or "").strip()
        display    = (row.get("displayName") or "").strip()
        email      = (row.get("email") or "").strip()
        action_raw = (row.get("action") or "").strip()
        action     = action_raw[:1].lower() if action_raw else ""  # A/D/U only if present
        new_disp   = (row.get("newDisplayName") or "").strip()
        new_email  = (row.get("newEmail") or "").strip()

        # Blank action => list info, no action
        if not action:
            msg = f"[{i}] 🔎 No action — {group_name or group_id} | {display or person_id} <{email}>"
            print(msg, flush=True)
            ops.append({
                "row": i, "action": "(none)", "groupId": group_id, "groupName": group_name,
                "personId": person_id, "email": email, "result": "noop",
                "detail": "no action (blank)"
            })
            continue

        # Validate action token
        if action not in ("a", "d", "u"):
            print(f"[{i}] ⚠️  Invalid action '{action_raw}' — expected A/D/U or blank for no-op", flush=True)
            ops.append({
                "row": i, "action": action_raw, "groupId": group_id, "groupName": group_name,
                "personId": person_id, "email": email, "result": "skip",
                "detail": "invalid action (use A/D/U or blank)"
            })
            continue

        # Resolve person if we will actually act (A/D/U)
        pid, resolved_email = resolve_person(person_id, email)
        person_id = pid or person_id
        email     = resolved_email or email

        if action in ("a", "d") and (not group_id or not person_id):
            print(f"[{i}] ❌ {('Add' if action=='a' else 'Del')} requires groupId and personId/email", flush=True)
            ops.append({
                "row": i, "action": action_raw, "groupId": group_id, "groupName": group_name,
                "personId": person_id, "email": email, "result": "error",
                "detail": "groupId and personId/email required"
            })
            continue

        if action == "a":
            ok, detail = add_member(group_id, person_id)
            status = "ok" if ok else "error"
            print(f"[{i}] ➕ Add  | {group_name or group_id} ← {email or person_id}: {detail}", flush=True)

        elif action == "d":
            ok, detail = delete_member(group_id, person_id)
            status = "ok" if ok else "error"
            print(f"[{i}] ➖ Del  | {group_name or group_id} × {email or person_id}: {detail}", flush=True)

        else:  # 'u'
            if not person_id:
                print(f"[{i}] ❌ Update requires personId or resolvable email", flush=True)
                ops.append({
                    "row": i, "action": action_raw, "groupId": group_id, "groupName": group_name,
                    "personId": "", "email": email, "result": "error",
                    "detail": "person not found"
                })
                continue
            if not (new_disp or new_email):
                print(f"[{i}] ⚠️  Update no-op (no newDisplayName/newEmail provided)", flush=True)
                ops.append({
                    "row": i, "action": action_raw, "groupId": group_id, "groupName": group_name,
                    "personId": person_id, "email": email, "result": "skip", "detail": "no changes"
                })
                continue
            ok, detail = update_person(person_id, new_disp or None, new_email or None)
            status = "ok" if ok else "error"
            print(f"[{i}] ✏️  Upd  | {email or person_id}: {detail}", flush=True)

        ops.append({
            "row": i, "action": action_raw, "groupId": group_id, "groupName": group_name,
            "personId": person_id, "email": email, "result": status, "detail": detail
        })

        time.sleep(0.2)  # be nice to the API

    print("\n✅ Processing complete.", flush=True)
    return ops

# ---------------- Entry point (guaranteed log) --------------
if __name__ == "__main__":
    all_ops: List[Dict[str, Any]] = []
    try:
        all_ops = main()
    except SystemExit:
        # die() already printed; still try to flush any ops if collected
        pass
    except Exception as e:
        print("ERROR:", e, flush=True)
        traceback.print_exc()
    finally:
        if isinstance(all_ops, list) and all_ops:
            write_log(all_ops)
        else:
            # even if nothing happened, write an empty log with header for consistency
            write_log([])
