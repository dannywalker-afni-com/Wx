#!/usr/bin/env python3
"""
Process group_members.csv to add / update / delete Webex Group members.

Input CSV columns (minimum):
  groupId,groupName,personId,displayName,email,action,newDisplayName,newEmail

Rules:
- Blank action  -> NO-OP: list the row info (console + log).
- 'u'/'U'       -> Update user (display name/email; blank new* ignored).
- 'a'/'A'       -> Add member.
- 'd'/'D'       -> Delete member.

ENV:
  WEBEX_TOKEN   = admin OAuth token (identity:groups_read, identity:groups_write,
                                     spark-admin:people_read, spark-admin:people_write)
  WEBEX_ORG_ID  = (recommended) customer org id
  WEBEX_BASE    = https://webexapis.com   (FedRAMP: https://api-usgov.webex.com)
"""

import os, sys, csv, time, json, requests
from typing import Dict, Any, Optional, Tuple

BASE   = os.getenv("WEBEX_BASE", "https://webexapis.com").rstrip("/")
TOKEN  = (os.getenv("WEBEX_TOKEN") or "").strip()
ORG_ID = (os.getenv("WEBEX_ORG_ID") or "").strip()
TIMEOUT = 30
LOG_CSV = "group_ops_log.csv"

def die(msg: str) -> None:
    print(f"ERROR: {msg}")
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

# ---------- People helpers ----------
def resolve_person(person_id: Optional[str], email: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Return (personId, email). If personId missing but email provided, look it up."""
    if person_id:
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
            return (pid, (ems[0] if ems else email))
    return (None, email)

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
        return (True, "Updated user profile")
    try:
        err = r.json()
    except Exception:
        err = r.text
    return (False, f"People update failed ({r.status_code}): {err}")

# ---------- Group membership helpers ----------
def is_member(group_id: str, person_id: str) -> bool:
    params = {"itemsPerPage": 100, "startIndex": 1}
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
        if not is_member(group_id, person_id):
            return (True, "Not a member (no-op)")
    try:
        err = r.json()
    except Exception:
        err = r.text
    return (False, f"Delete failed ({r.status_code}): {err}")

# ---------- Main ----------
def main():
    in_csv = "group_members.csv"
    if not os.path.exists(in_csv):
        die(f"Missing input file: {in_csv}")

    ops = []
    with open(in_csv, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        rows = list(r)

    print(f"🔧 Processing {len(rows)} rows from group_members.csv ...\n")

    for i, row in enumerate(rows, 1):
        group_id   = (row.get("groupId") or "").strip()
        group_name = (row.get("groupName") or "").strip()
        person_id  = (row.get("personId") or "").strip()
        display    = (row.get("displayName") or "").strip()
        email      = (row.get("email") or "").strip()
        action_raw = (row.get("action") or "").strip()
        action     = action_raw[:1].lower() if action_raw else ""  # only first letter matters
        new_disp   = (row.get("newDisplayName") or "").strip()
        new_email  = (row.get("newEmail") or "").strip()

        # BLANK ACTION => NO-OP (list info only)
        if not action:
            msg = f"[{i}] 🔎 No action — {group_name or group_id} | {display or person_id} <{email}>"
            print(msg)
            ops.append({
                "row": i, "action": "(none)", "groupId": group_id, "groupName": group_name,
                "personId": person_id, "email": email, "result": "noop",
                "detail": "no action (blank)"
            })
            continue

        # ADD / DELETE / UPDATE classification (first letter only)
        if action not in ("a", "d", "u"):
            print(f"[{i}] ⚠️  Invalid action '{action_raw}' — expected A/D/U or blank for no-op")
            ops.append({"row": i, "action": action_raw_
