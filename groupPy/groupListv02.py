#!/usr/bin/env python3
"""
Reads group names from groupName.csv (header: groupName)
Matches them against Webex 'Webex groups' (NOT SCIM) and exports:
  - groups.csv
  - group_members.csv
  - group_assignments.csv (stub; not exposed by API)

ENV:
  WEBEX_TOKEN     admin token with identity:groups_read
  WEBEX_ORG_ID    (optional but recommended)
  WEBEX_BASE      default https://webexapis.com  (FedRAMP: https://api-usgov.webex.com)
"""

import os, sys, csv, time, re, requests
from typing import Dict, Any, Iterable, List, Optional

BASE = os.getenv("WEBEX_BASE", "https://webexapis.com").rstrip("/")
TOKEN = (os.getenv("WEBEX_TOKEN") or "").strip()
ORG_ID = (os.getenv("WEBEX_ORG_ID") or "").strip()
TIMEOUT = 30

OUT_GROUPS   = "groups.csv"
OUT_MEMBERS  = "group_members.csv"
OUT_ASSIGN   = "group_assignments.csv"

# add near the top with other globals
PERSON_EMAIL_CACHE: dict[str, str] = {}


def hdrs() -> Dict[str, str]:
    if not TOKEN:
        sys.exit("ERROR: set WEBEX_TOKEN to an admin token with identity:groups_read.")
    return {"Authorization": f"Bearer {TOKEN}"}

def _get(url: str, params: Dict[str, Any] | None = None) -> requests.Response:
    backoff = 1.0
    for _ in range(5):
        r = requests.get(url, headers=hdrs(), params=params or {}, timeout=TIMEOUT)
        if r.status_code in (429, 502, 503, 504):
            sleep_for = float(r.headers.get("Retry-After", backoff))
            time.sleep(min(sleep_for, 10.0)); backoff *= 1.6
            continue
        return r
    return r

# helper for _get() function
def get_email_for_person(person_id: str) -> str:
    if not person_id:
        return ""
    if person_id in PERSON_EMAIL_CACHE:
        return PERSON_EMAIL_CACHE[person_id]

    url = f"{BASE}/v1/people/{person_id}"
    params = {}
    if ORG_ID:
        params["orgId"] = ORG_ID

    r = _get(url, params)
    if r.status_code == 200:
        try:
            data = r.json()
            emails = data.get("emails") or []
            email = emails[0] if emails else ""
            PERSON_EMAIL_CACHE[person_id] = email
            return email
        except Exception:
            return ""
    # don’t crash the export if one lookup fails
    return ""


def paginate(url: str, params: Dict[str, Any] | None = None) -> Iterable[Dict[str, Any]]:
    while True:
        r = _get(url, params)
        if r.status_code == 401:
            sys.exit("401 Unauthorized. Check WEBEX_TOKEN and scopes (need identity:groups_read).")
        if r.status_code == 403:
            sys.exit("403 Forbidden. Token likely lacks identity:groups_read, or wrong org/region.")
        r.raise_for_status()
        data = r.json()
        items = data.get("items") or []
        for it in items:
            yield it
        link = r.headers.get("Link", "")
        next_url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                start = part.find("<") + 1; end = part.find(">")
                next_url = part[start:end]; break
        if not next_url: break
        url, params = next_url, None

# ----------------- Webex Groups -----------------
def list_all_groups() -> List[Dict[str, Any]]:
    url = f"{BASE}/v1/groups"
    params = {"itemsPerPage": 200, "startIndex": 1}
    if ORG_ID:
        params["orgId"] = ORG_ID

    out: List[Dict[str, Any]] = []
    while True:
        r = _get(url, params)
        if r.status_code == 401:
            sys.exit("401 Unauthorized. Check WEBEX_TOKEN and scopes (need identity:groups_read).")
        if r.status_code == 403:
            sys.exit("403 Forbidden. Token likely lacks identity:groups_read, or wrong org/region.")
        r.raise_for_status()
        data = r.json()

        # Identity API returns "groups"; older payloads sometimes used "items"
        batch = data.get("groups") or data.get("items") or []
        out.extend(batch)

        total = int(data.get("totalResults", 0) or 0)
        start = int(data.get("startIndex", params["startIndex"]))
        per   = int(data.get("itemsPerPage", len(batch)) or 0)

        if total and per and (start + per) <= total:
            params["startIndex"] = start + per
        else:
            break
    return out

def get_group_members(group_id: str) -> List[Dict[str, Any]]:
    url = f"{BASE}/v1/groups/{group_id}/members"
    params = {"itemsPerPage": 200, "startIndex": 1}
    if ORG_ID:
        params["orgId"] = ORG_ID

    out: List[Dict[str, Any]] = []
    while True:
        r = _get(url, params)
        r.raise_for_status()
        data = r.json()

        # Try common keys the Identity service has used
        batch = data.get("members") or data.get("items") or data.get("users") or []
        out.extend(batch)

        total = int(data.get("totalResults", 0) or 0)
        start = int(data.get("startIndex", params["startIndex"]))
        per   = int(data.get("itemsPerPage", len(batch)) or 0)

        if total and per and (start + per) <= total:
            params["startIndex"] = start + per
        else:
            break
    return out

# ----------------- Helpers -----------------
_ws_re = re.compile(r"\s+")

def norm(s: str) -> str:
    """Casefold, trim, collapse whitespace."""
    return _ws_re.sub(" ", (s or "").strip()).casefold()

def normalize_member(m: Dict[str, Any]) -> Dict[str, Any]:
    pid = m.get("personId") or m.get("id") or ""
    dname = m.get("displayName") or m.get("name") or ""

    # Try what the groups API returns; if empty, resolve via People API
    emails = m.get("emails") or m.get("email") or []
    email = (emails[0] if isinstance(emails, list) and emails else (emails or ""))
    if not email and pid:
        email = get_email_for_person(pid)

    return {"personId": pid, "displayName": dname, "email": email}


def main():
    # Read requested names
    if not os.path.exists("groupName.csv"):
        sys.exit("Missing input file: groupName.csv (must have a 'groupName' header).")
    with open("groupName.csv", newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        if not r.fieldnames or "groupName" not in r.fieldnames:
            sys.exit("groupName.csv must contain header 'groupName'.")
        target_names = [row["groupName"].strip() for row in r if row.get("groupName")]

    # Dump all groups once
    print("ℹ️  Fetching all Webex groups via /v1/groups ...")
    all_groups = list_all_groups()
    print(f"ℹ️  API returned {len(all_groups)} groups.")

    if len(all_groups) == 0:
        print("\nNo groups returned. Check the following:\n"
              "  • Token has scope identity:groups_read (service app / admin integration)\n"
              "  • WEBEX_ORG_ID is set to your org (especially if you are a partner admin)\n"
              "  • WEBEX_BASE matches your region (commercial vs FedRAMP)\n"
              "  • You’re not mixing Bot tokens (Bots do NOT support groups APIs)\n")
        sys.exit(1)

    # Build name index
    by_norm_name: Dict[str, Dict[str, Any]] = {}
    for g in all_groups:
        disp = g.get("displayName") or g.get("name") or ""
        by_norm_name[norm(disp)] = g

    groups_out, members_out, assign_out = [], [], []
    not_found: List[str] = []

    for raw_name in target_names:
        print(f"🔍 Looking up group: {raw_name}")
        wanted = norm(raw_name)

        g = by_norm_name.get(wanted)
        if not g:
            # try relaxed contains match
            candidates = [gg for k, gg in by_norm_name.items() if wanted in k]
            g = candidates[0] if candidates else None

        if not g:
            print(f"❌ Not found in /v1/groups: {raw_name}")
            not_found.append(raw_name)
            continue

        gid = g.get("id", "")
        name = g.get("displayName") or g.get("name") or ""
        desc = g.get("description") or ""
        src  = g.get("source") or ""  # often 'local' for Webex groups
        last = g.get("lastModified") or g.get("modified") or ""
        usage = g.get("usage") or ""

        # members
        try:
            ms = get_group_members(gid)
        except requests.HTTPError as e:
            print(f"⚠️ Members fetch failed for {name}: {e}")
            ms = []

        groups_out.append({
            "groupId": gid,
            "name": name,
            "description": desc,
            "source": src,
            "lastModified": last,
            "memberCount": len(ms),
            "usage": usage
        })

        for m in ms:
            members_out.append({
                "groupId": gid,
                "groupName": name,
                **normalize_member(m)
            })

        assign_out.append({
            "groupId": gid,
            "groupName": name,
            "assignments": "NOT EXPOSED BY PUBLIC API (use Control Hub UI)"
        })

        time.sleep(0.2)

    # Write CSVs
    with open(OUT_GROUPS, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "groupId","name","description","source","lastModified","memberCount","usage"
        ])
        w.writeheader(); w.writerows(groups_out)

    with open(OUT_MEMBERS, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["groupId","groupName","personId","displayName","email"])
        w.writeheader(); w.writerows(members_out)

    with open(OUT_ASSIGN, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["groupId","groupName","assignments"])
        w.writeheader(); w.writerows(assign_out)

    print(f"\n✅ Groups exported: {len(groups_out)} → {OUT_GROUPS}")
    print(f"✅ Members exported: {len(members_out)} → {OUT_MEMBERS}")
    print(f"✅ Assignments stub: {len(assign_out)} → {OUT_ASSIGN}")

    if not_found:
        print("\n⚠️ Names not found (after normalization):")
        for n in not_found:
            print("   -", n)

if __name__ == "__main__":
    main()
