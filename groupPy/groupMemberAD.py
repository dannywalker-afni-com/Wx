#!/usr/bin/env python3
"""
Bulk manage Webex Groups members from group_members.csv (REWRITTEN FOR EXPLICIT SCIM PATCH)

This script uses the explicit SCIM Identity API path (/identity/scim/{orgId}/v2/Groups/{groupId})
to bypass routing issues found with the standard /v1/groups endpoint.

CSV columns (minimum):
  groupId,groupName,personId,displayName,email,action

Behavior:
  - Blank action  -> NO-OP: list info (console + log).
  - 'a'/'A'       -> Add member to group using SCIM PATCH.
  - 'd'/'D'       -> Delete member from group using SCIM PATCH.
  - Anything else -> Invalid action (logged as skip).

ENV (required):
  WEBEX_TOKEN   = admin OAuth token (Scopes: identity:groups_read, identity:groups_write, spark-admin:people_read)
  WEBEX_ORG_ID  = target customer org UUID (36-char UUID, REQUIRED for this SCIM path)
  WEBEX_BASE    = https://webexapis.com  (FedRAMP: https://api-usgov.webex.com)

TLS (optional):
  REQUESTS_CA_BUNDLE or SSL_CERT_FILE = path to PEM bundle
  WEBEX_VERIFY=false  -> temporarily disable TLS verification (testing only)

Debug (optional):
  WEBEX_DEBUG=true -> print request URLs/params before calls
"""

import os
import sys
import csv
import time
import json
import base64
import re
import traceback
import requests
from typing import Dict, Any, Optional, Tuple, List
from urllib.parse import quote

# -------------------- Config --------------------
BASE    = os.getenv("WEBEX_BASE", "https://webexapis.com").rstrip("/")
TOKEN   = (os.getenv("WEBEX_TOKEN") or "").strip()
# IMPORTANT: UUID form (36 chars), required for explicit SCIM path
ORG_ID  = (os.getenv("WEBEX_ORG_ID") or "").strip()
DEBUG   = (os.getenv("WEBEX_DEBUG", "false").lower() == "true")

INPUT_CSV = "group_members.csv"
LOG_CSV   = "groupMemberACDLog.csv"

# TLS toggle / bundle
CA_BUNDLE = os.getenv("REQUESTS_CA_BUNDLE") or os.getenv("SSL_CERT_FILE")
VERIFY_TLS = CA_BUNDLE if CA_BUNDLE else (os.getenv("WEBEX_VERIFY", "true").lower() != "false")
print(f"[TLS] verify={VERIFY_TLS if isinstance(VERIFY_TLS, bool) else 'file:' + VERIFY_TLS}", flush=True)

# Timeouts & Retry
TIMEOUT = 30  # seconds

# -------------- Helpers / exit ------------------
def die(msg: str) -> None:
    print(f"ERROR: {msg}", flush=True)
    sys.exit(1)

def hdrs(json_ct: bool = True) -> Dict[str, str]:
    """
    Constructs the standard headers, including Content-Type if requested.
    FIXED: Removed unmatched '}' character.
    """
    if not TOKEN:
        die("Set WEBEX_TOKEN first.")
    h = {"Authorization": f"Bearer {TOKEN}"}
    if json_ct:
        h["Content-Type"] = "application/json"
    return h

def dbg(*args):
    if DEBUG:
        print("DEBUG:", *args, flush=True)

# -------------- HTTP with backoff --------------
def backoff_request(method: str, url: str, **kwargs) -> requests.Response:
    backoff = 1.0
    kwargs.setdefault("verify", VERIFY_TLS)
    kwargs.setdefault("timeout", TIMEOUT)
    for attempt in range(6):
        if DEBUG:
            p = kwargs.get("params")
            d = kwargs.get("data")
            print(f"DEBUG {method} {url} params={p} data={d}", flush=True)
        try:
            r = requests.request(method, url, **kwargs)
        except requests.exceptions.RequestException as e:
            print(f"ERROR: Request failed ({method} {url}): {e}", flush=True)
            raise

        if r.status_code in (429, 502, 503, 504):
            wait = float(r.headers.get("Retry-After", backoff))
            print(f"WARN: Rate limited or server error, retrying in {wait:.1f}s...", flush=True)
            time.sleep(min(wait, 10.0))
            backoff *= 1.6
            continue
        return r
    return r

# --------- Hydra/ID utils ----------------------
def _b64url_decode(s: str) -> Optional[str]:
    try:
        pad = "=" * ((4 - len(s) % 4) % 4)
        raw = base64.b64decode(s.replace("-", "+").replace("_", "/") + pad)
        return raw.decode("utf-8", errors="ignore")
    except Exception:
        return None

def hydra_group_org_uuid(group_hydra_id: str) -> Optional[str]:
    """
    Hydra group id (Y2lz...) decodes to:
      ciscospark://us/SCIM_GROUP/<group-uuid>:<org-uuid>
    Return the <org-uuid> (lowercase) if present.
    """
    if not group_hydra_id:
        return None
    decoded = _b64url_decode(group_hydra_id)
    if not decoded:
        return None
    m = re.search(r"/SCIM_GROUP/([0-9a-fA-F-]{36}):([0-9a-fA-F-]{36})$", decoded)
    if m:
        return m.group(2).lower()
    m = re.search(r"/GROUP/([0-9a-fA-F-]{36}):([0-9a-fA-F-]{36})$", decoded)
    if m:
        return m.group(2).lower()
    return None

def hydra_person_org_uuid(person_hydra_org_id: str) -> Optional[str]:
    """
    Decode person orgId Hydra (Y2lz...) -> .../ORGANIZATION/<org-uuid>, return <org-uuid>.
    """
    if not person_hydra_org_id:
        return None
    decoded = _b64url_decode(person_hydra_org_id)
    if not decoded:
        return None
    parts = decoded.split("/")
    tail = parts[-1] if parts else ""
    return tail.lower() if len(tail) == 36 else None

# --------- ID normalization (paths expect Hydra) --------
def normalize_group_id(group_id: str) -> str:
    """
    Removes the trailing :<org-uuid> from the Hydra ID for API path use.
    """
    if not group_id:
        return group_id
    head, sep, tail = group_id.rpartition(":")
    return head if sep and len(tail) == 36 else group_id

# --------- SCIM URL Helper ----------------------
def scim_group_url(group_id: str, suffix: str = "") -> str:
    """Constructs the explicit SCIM Group API URL."""
    if not ORG_ID:
        die("WEBEX_ORG_ID must be set for the explicit SCIM API path.")
    
    gid = normalize_group_id(group_id)
    # Using the explicit SCIM path: /identity/scim/{orgId}/v2/Groups/{groupId}
    url = f"{BASE}/identity/scim/{quote(ORG_ID, safe='')}/v2/Groups/{quote(gid, safe='')}{suffix}"
    return url

# ---------------- Groups ------------------------
def get_group(group_id: str) -> Optional[Dict[str, Any]]:
    # Uses the new explicit SCIM path, which includes ORG_ID
    url = scim_group_url(group_id)
    r = backoff_request("GET", url, headers=hdrs(False))
    if r.status_code == 200:
        return r.json()
    return None

def group_is_writable(group_id: str) -> Tuple[bool, str]:
    g = get_group(group_id)
    if not g:
        # 404/403 often means not found, but we let the PATCH attempt handle it
        return (True, "group not found during pre-check (continuing to PATCH)")
    src = (g.get("source") or "").lower()
    # SCIM groups can only be modified if the source is 'local' or not specified
    if src and src != "local":
        return (False, f"group source is '{src}' (directory/SCIM-synced); membership is read-only")
    return (True, "ok")

# ---------------- People ------------------------
_PERSON_EMAIL_CACHE: Dict[str, str] = {}

def resolve_person(person_id: Optional[str], email: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (personId, email). If personId missing but email present, lookup by email.
    """
    if person_id:
        if not email and person_id in _PERSON_EMAIL_CACHE:
            email = _PERSON_EMAIL_CACHE[person_id]
        return (person_id, email)

    if not email:
        return (None, None)

    params = {"email": email}
    if ORG_ID:
        # Only use orgId for people lookup if available
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

def get_person_org_uuid(person_id: str) -> Optional[str]:
    r = backoff_request("GET", f"{BASE}/v1/people/{quote(person_id, safe='')}", headers=hdrs(False))
    if r.status_code == 200:
        hydra_org = (r.json().get("orgId") or "").strip()
        return hydra_person_org_uuid(hydra_org)
    return None

# ---------- Group membership helpers ------------
def is_member(group_id: str, person_id: str) -> bool:
    # Uses the new explicit SCIM path for members list
    url = scim_group_url(group_id, suffix="/members")
    params = {"itemsPerPage": 200, "startIndex": 1}
    while True:
        r = backoff_request("GET", url, headers=hdrs(False), params=params)
        if r.status_code != 200:
            return False
        data = r.json()
        members = data.get("members") or data.get("items") or data.get("users") or []
        for m in members:
            # SCIM group members use 'value' as the personId
            if (m.get("value") or m.get("id")) == person_id:
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
    SCIM Groups add = PATCH /identity/scim/{orgId}/v2/Groups/{groupId} with 'add' operation.
    """
    ok, why = group_is_writable(group_id)
    if not ok:
        return (False, why)

    # Uses the new explicit SCIM path
    url = scim_group_url(group_id)

    # SCIM PATCH payload for adding a member
    payload = {
      "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
      "Operations": [
        {
          "op": "add",
          "path": "members",
          "value": [
            { "value": person_id }
          ]
        }
      ]
    }

    # Use PATCH method for SCIM member modification
    r = backoff_request("PATCH", url, headers=hdrs(), data=json.dumps(payload))

    if r.status_code in (200, 204):
        return (True, "Added to group via explicit SCIM PATCH")
    
    # Handle known SCIM error for member already existing (often 400 Bad Request with unique constraint error)
    if r.status_code == 400:
        try:
            err_json = r.json()
            # SCIM 400 error for uniqueness means user is already a member
            if err_json.get("scimType") == "uniqueness":
                 return (True, "Already a member (SCIM uniqueness error)")
        except:
            pass
            
    try:
        err = r.json()
    except Exception:
        err = r.text
        
    return (False, f"Add failed ({r.status_code}): {err}")

def delete_member(group_id: str, person_id: str) -> Tuple[bool, str]:
    """
    SCIM Groups delete = PATCH /identity/scim/{orgId}/v2/Groups/{groupId} with 'remove' operation.
    """
    ok, why = group_is_writable(group_id)
    if not ok:
        return (False, why)

    # Uses the new explicit SCIM path
    url = scim_group_url(group_id)

    # SCIM PATCH payload for removing a member
    payload = {
      "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
      "Operations": [
        {
          "op": "remove",
          # The path specifies the member to remove by filtering on the 'value' (personId)
          "path": f"members[value eq \"{person_id}\"]"
        }
      ]
    }

    # Use PATCH method for SCIM member modification
    r = backoff_request("PATCH", url, headers=hdrs(), data=json.dumps(payload))

    if r.status_code in (200, 204):
        return (True, "Removed from group via explicit SCIM PATCH")
    
    # If the PATCH fails with 400/404, check explicitly if the member is gone.
    if r.status_code in (400, 404) and not is_member(group_id, person_id):
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

    # Check if ORG_ID is set (required for the new SCIM path)
    if not ORG_ID:
        die("WEBEX_ORG_ID environment variable is required for the SCIM API path.")

    print(f"Logging to: {LOG_CSV}", flush=True)

    ops: List[Dict[str, Any]] = []

    with open(INPUT_CSV, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        rows = list(r)

    print(f"🔧 Processing {len(rows)} rows from {INPUT_CSV} ...\n", flush=True)

    for i, row in enumerate(rows, 1):
        group_id_raw = (row.get("groupId") or "").strip()
        group_id     = normalize_group_id(group_id_raw)  # hydra path id
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
                "row": i, "action": "(none)", "groupId": group_id_raw or group_id, "groupName": group_name,
                "personId": person_id, "displayName": display, "email": email,
                "result": "noop", "detail": "no action (blank)"
            })
            continue

        # ---------- Invalid ----------
        if action not in ("a", "d"):
            print(f"[{i}] ⚠️ Invalid action '{action_raw}' — expected A/D or blank", flush=True)
            ops.append({
                "row": i, "action": action_raw, "groupId": group_id_raw or group_id, "groupName": group_name,
                "personId": person_id, "displayName": display, "email": email,
                "result": "skip", "detail": "invalid action (use A/D or blank)"
            })
            continue

        # ---------- Resolve Person ----------
        pid, resolved_email = resolve_person(person_id, email)
        person_id = pid or person_id
        email     = resolved_email or email

        # ---------- Pre-check org alignment ----------
        # Derive group org UUID by decoding the Hydra groupId (CSV value)
        group_org_uuid = hydra_group_org_uuid(group_id_raw) or (ORG_ID or "")
        person_org_uuid = get_person_org_uuid(person_id) if person_id else None

        if action == "a":
            if not group_id or not person_id:
                print(f"[{i}] ❌ Add requires groupId and personId/email", flush=True)
                ops.append({
                    "row": i, "action": "A", "groupId": group_id_raw or group_id, "groupName": group_name,
                    "personId": person_id, "displayName": display, "email": email,
                    "result": "error", "detail": "groupId and personId/email required"
                })
                continue

            if group_org_uuid and person_org_uuid and group_org_uuid != person_org_uuid:
                detail = f"cross-org: person org {person_org_uuid} != group org {group_org_uuid}"
                print(f"[{i}] ❌ Add blocked — {detail}", flush=True)
                ops.append({
                    "row": i, "action": "A", "groupId": group_id_raw or group_id, "groupName": group_name,
                    "personId": person_id, "displayName": display, "email": email,
                    "result": "error", "detail": detail
                })
                continue

            ok, detail = add_member(group_id_raw or group_id, person_id)
            status = "ok" if ok else "error"
            print(f"[{i}] ➕ Add | {group_name or group_id} ← {email or person_id}: {detail}", flush=True)
            ops.append({
                "row": i, "action": "A", "groupId": group_id_raw or group_id, "groupName": group_name,
                "personId": person_id, "displayName": display, "email": email,
                "result": status, "detail": detail
            })
            continue

        if action == "d":
            if not group_id or not person_id:
                print(f"[{i}] ❌ Delete requires groupId and personId/email", flush=True)
                ops.append({
                    "row": i, "action": "D", "groupId": group_id_raw or group_id, "groupName": group_name,
                    "personId": person_id, "displayName": display, "email": email,
                    "result": "error", "detail": "groupId and personId/email required"
                })
                continue

            ok, detail = delete_member(group_id_raw or group_id, person_id)
            status = "ok" if ok else "error"
            print(f"[{i}] ➖ Del | {group_name or group_id} × {email or person_id}: {detail}", flush=True)
            ops.append({
                "row": i, "action": "D", "groupId": group_id_raw or group_id, "groupName": group_name,
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