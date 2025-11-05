#!/usr/bin/env python3
"""
Reads group names from groupName.csv (column: groupName) and exports:
  - groups.csv
  - group_members.csv
  - group_assignments.csv (stub note: not exposed by public API)

Tries SCIM search first (identity/scim/{orgId}/v2/Groups), then falls back to /v1/groups.
Members are read via /v1/groups/{groupId}/members.
"""

import os, sys, csv, time, requests
from typing import Dict, Any, Iterable, List, Optional

BASE = os.getenv("WEBEX_BASE", "https://webexapis.com").rstrip("/")
TOKEN = (os.getenv("WEBEX_TOKEN") or "").strip()
ORG_ID = (os.getenv("WEBEX_ORG_ID") or "").strip()
TIMEOUT = 30

OUT_GROUPS = "groups.csv"
OUT_MEMBERS = "group_members.csv"
OUT_ASSIGN = "group_assignments.csv"

def hdrs() -> Dict[str, str]:
    if not TOKEN:
        sys.exit("ERROR: set WEBEX_TOKEN to an admin token (identity:people_read or identity:groups_read).")
    return {"Authorization": f"Bearer {TOKEN}"}

def _get(url: str, params: Dict[str, Any] | None = None) -> requests.Response:
    backoff = 1.0
    for _ in range(5):
        r = requests.get(url, headers=hdrs(), params=params or {}, timeout=TIMEOUT)
        if r.status_code in (429, 502, 503, 504):
            sleep_for = float(r.headers.get("Retry-After", backoff))
            time.sleep(min(sleep_for, 10.0))
            backoff *= 1.6
            continue
        return r
    return r

def paginate(url: str, params: Dict[str, Any] | None = None) -> Iterable[Dict[str, Any]]:
    """Follow RFC5988 Link headers."""
    while True:
        r = _get(url, params)
        r.raise_for_status()
        data = r.json()
        items = data.get("items") or data.get("results") or data.get("Resources") or []
        for it in items:
            yield it
        link = r.headers.get("Link", "")
        next_url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                start = part.find("<") + 1
                end = part.find(">")
                next_url = part[start:end]
                break
        if not next_url:
            break
        url, params = next_url, None

# ---------- LOOKUPS ----------
def scim_find_group_by_name(name: str) -> Optional[Dict[str, Any]]:
    """
    SCIM search by displayName (identity/scim/{orgId}/v2/Groups).
    Returns raw SCIM group dict or None.
    """
    if not ORG_ID:
        return None
    url = f"{BASE}/v1/identity/scim/{ORG_ID}/v2/Groups"
    # SCIM filter: displayName eq "Name"
    params = {"filter": f'displayName eq "{name}"', "count": 100}
    r = _get(url, params)
    if r.status_code == 200:
        data = r.json()
        resources = data.get("Resources") or []
        # Case-insensitive match, favor exact
        for g in resources:
            if (g.get("displayName") or "").lower() == name.lower():
                return g
    return None

def groups_list_all() -> Iterable[Dict[str, Any]]:
    """Fallback: list /v1/groups (Webex Groups API)."""
    url = f"{BASE}/v1/groups"
    params = {"max": 500}
    if ORG_ID:
        params["orgId"] = ORG_ID
    yield from paginate(url, params)

def groups_find_by_name(name: str) -> Optional[Dict[str, Any]]:
    """Fallback search in /v1/groups by iterating and matching displayName."""
    for g in groups_list_all():
        if (g.get("displayName") or "").lower() == name.lower():
            return g
    return None

def get_group_members(group_id: str) -> List[Dict[str, Any]]:
    """Use /v1/groups/{groupId}/members."""
    url = f"{BASE}/v1/groups/{group_id}/members"
    params = {"max": 500}
    if ORG_ID:
        params["orgId"] = ORG_ID
    return list(paginate(url, params))

# ---------- NORMALIZATION ----------
def normalize_group(g: Dict[str, Any]) -> Dict[str, Any]:
    # Handle both SCIM and /v1 groups shapes
    if "id" in g and "displayName" in g:
        return {
            "groupId": g.get("id", ""),
            "name": g.get("displayName") or g.get("name") or "",
            "description": g.get("description") or "",
            "source": g.get("source") or "",             # only on /v1 sometimes
            "lastModified": g.get("lastModified") or g.get("modified") or g.get("meta", {}).get("lastModified", ""),
            "usage": g.get("usage") or ""
        }
    # SCIM often uses "id"/"displayName"/"meta"
    return {
        "groupId": g.get("id", ""),
        "name": g.get("displayName") or "",
        "description": g.get("description") or "",
        "source": "",  # not in SCIM
        "lastModified": g.get("meta", {}).get("lastModified", ""),
        "usage": ""
    }

def normalize_member(m: Dict[str, Any]) -> Dict[str, Any]:
    pid = m.get("personId") or m.get("id") or ""
    dname = m.get("displayName") or m.get("name") or ""
    emails = m.get("emails") or m.get("email") or []
    if isinstance(emails, list):
        email = emails[0] if emails else ""
    else:
        email = emails or ""
    return {"personId": pid, "displayName": dname, "email": email}

# ---------- MAIN ----------
def main():
    if not os.path.exists("groupName.csv"):
        sys.exit("Missing input file: groupName.csv (must contain column 'groupName').")

    with open("groupName.csv", newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        if not r.fieldnames or "groupName" not in r.fieldnames:
            sys.exit("groupName.csv must have a 'groupName' header.")
        names = [row["groupName"].strip() for row in r if row.get("groupName")]

    if not names:
        sys.exit("No group names found in groupName.csv.")

    groups_out, members_out, assign_out = [], [], []

    for name in names:
        print(f"🔍 Looking up group: {name}")

        # 1) Try SCIM search (needs ORG_ID + identity:people_read/_rw)
        g = scim_find_group_by_name(name)

        # 2) Fallback to /v1/groups list + match (needs identity:groups_read)
        if not g:
            g = groups_find_by_name(name)

        if not g:
            print(f"❌ Group not found via SCIM or /v1: {name}")
            continue

        norm = normalize_group(g)
        gid = norm["groupId"]
        groups_out.append({**norm})

        # Members via /v1/groups/{id}/members
        try:
            ms = get_group_members(gid)
        except requests.HTTPError as e:
            print(f"⚠️ Members fetch failed for {name}: {e}")
            ms = []

        for m in ms:
            members_out.append({
                "groupId": gid,
                "groupName": norm["name"],
                **normalize_member(m)
            })

        # Assignments: still not exposed publicly
        assign_out.append({
            "groupId": gid,
            "groupName": norm["name"],
            "assignments": "NOT EXPOSED BY PUBLIC API (use Control Hub UI)"
        })

        time.sleep(0.2)

    # Write CSVs
    with open(OUT_GROUPS, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["groupId","name","description","source","lastModified","memberCount","usage"])
        # compute memberCount
        counts = {}
        for m in members_out:
            counts[m["groupId"]] = counts.get(m["groupId"], 0) + 1
        for row in groups_out:
            row["memberCount"] = counts.get(row["groupId"], 0)
        w.writeheader(); w.writerows(groups_out)

    with open(OUT_MEMBERS, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["groupId","groupName","personId","displayName","email"])
        w.writeheader(); w.writerows(members_out)

    with open(OUT_ASSIGN, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["groupId","groupName","assignments"])
        w.writeheader(); w.writerows(assign_out)

    print(f"✅ Groups exported: {len(groups_out)} → {OUT_GROUPS}")
    print(f"✅ Members exported: {len(members_out)} → {OUT_MEMBERS}")
    print(f"✅ Assignments stub: {len(assign_out)} → {OUT_ASSIGN}")

if __name__ == "__main__":
    main()
