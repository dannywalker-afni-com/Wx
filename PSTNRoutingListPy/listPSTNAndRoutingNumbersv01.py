#!/usr/bin/env python3
"""
Exports Control Hub → Services → PSTN & Routing → Numbers
Fields: phoneNumber, extension, locationId, locationName, status
Extras: assigned (yes/no), ownerType/ownerName/ownerId, numberType
Output: controlhub_numbers.csv
"""

import os, sys, csv, requests, certifi

BASE   = (os.getenv("WEBEX_BASE", "https://webexapis.com") or "").strip()
TOKEN  = (os.getenv("WEBEX_TOKEN", "") or "").strip()
ORG_ID = (os.getenv("WEBEX_ORG_ID", "") or "").strip()

if not TOKEN:
    sys.exit("ERROR: Set WEBEX_TOKEN to a valid admin token.")

TIMEOUT = 30
PAGE_MAX = 2000  # per API; large page reduces loops

# Shared session w/ certifi bundle for TLS
SESSION = requests.Session()
SESSION.verify = certifi.where()

def hdrs():
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

def add_org(params=None):
    params = dict(params or {})
    if ORG_ID:
        params["orgId"] = ORG_ID
    return params

def safe(d, *path, default=None):
    cur = d or {}
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def list_org_numbers(location_id=None, extension=None, available=None, number_type=None):
    """
    Uses GET /v1/telephony/config/numbers with start/max pagination.
    """
    url = f"{BASE}/v1/telephony/config/numbers"
    start = 0
    while True:
        params = {
            "max": PAGE_MAX,
            "start": start,
        }
        if location_id: params["locationId"] = location_id
        if extension:   params["extension"]  = extension
        if available is not None: params["available"] = str(bool(available)).lower()
        if number_type: params["numberType"] = number_type
        params = add_org(params)

        r = SESSION.get(url, headers=hdrs(), params=params, timeout=TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")

        data = r.json()
        items = data.get("phoneNumbers") or data.get("numbers") or []
        if not items:
            break

        for n in items:
            yield n

        if len(items) < PAGE_MAX:
            break
        start += PAGE_MAX

# ---------- Owner name resolution (with caching) ----------
_people_cache: dict[str, str] = {}
_workspace_cache: dict[str, str] = {}
_generic_owner_cache: dict[str, str] = {}  # fallback cache by raw ownerId

def _get_json(url: str, params=None):
    r = SESSION.get(url, headers=hdrs(), params=add_org(params), timeout=TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
    return r.json()

def resolve_owner_name(owner_type: str | None, owner_id: str | None, owner_name_in_payload: str | None) -> str:
    """
    Returns a human-friendly name for the owner.
    Priority:
      1) Use name from the numbers payload if present
      2) PERSON  -> /v1/people/{id}         -> displayName
      3) WORKSPACE -> /v1/workspaces/{id}   -> displayName
      4) Fallback: cache/empty string
    """
    # If API already provided a name, trust it.
    if owner_name_in_payload:
        return owner_name_in_payload

    if not owner_id:
        return ""

    # Cache by exact owner_id first (covers any prior resolution path)
    if owner_id in _generic_owner_cache:
        return _generic_owner_cache[owner_id]

    t = (owner_type or "").upper()

    try:
        if t == "PERSON":
            if owner_id in _people_cache:
                name = _people_cache[owner_id]
            else:
                data = _get_json(f"{BASE}/v1/people/{owner_id}")
                name = data.get("displayName") or data.get("nickName") or data.get("firstName") or ""
                _people_cache[owner_id] = name
            _generic_owner_cache[owner_id] = name
            return name

        if t == "WORKSPACE":
            if owner_id in _workspace_cache:
                name = _workspace_cache[owner_id]
            else:
                data = _get_json(f"{BASE}/v1/workspaces/{owner_id}")
                name = data.get("displayName") or data.get("name") or ""
                _workspace_cache[owner_id] = name
            _generic_owner_cache[owner_id] = name
            return name

        # Other feature types:
        # Often the Numbers API includes a 'name'. If not, we leave blank here,
        # because resolving requires a feature-specific endpoint (not provided in payload).
        # You can extend this with a map once you know exact feature types you see.
        _generic_owner_cache[owner_id] = ""
        return ""

    except Exception:
        # On any lookup failure, return empty but cache to avoid repeat calls
        _generic_owner_cache[owner_id] = ""
        return ""

def export_numbers():
    out_csv = "controlhub_numbers.csv"
    total = 0
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "phoneNumber",
                "extension",
                "status",
                "locationId",
                "locationName",
                "assigned",
                "ownerType",
                "ownerName",   # <- NEW COLUMN populated via lookup
                "ownerId",
                "numberType",
            ],
        )
        w.writeheader()

        for num in list_org_numbers():
            total += 1
            phone = num.get("phoneNumber") or ""
            ext   = num.get("extension") or ""
            stat  = (num.get("status") or "").upper()  # ACTIVE / INACTIVE
            loc_id   = safe(num, "location", "id", default="")
            loc_name = safe(num, "location", "name", default="")
            number_type = (num.get("numberType") or "").upper()  # STANDARD / SERVICE / MOBILE

            owner = num.get("owner") or {}
            owner_type = owner.get("type") or ""   # PERSON / WORKSPACE / FEATURE / ...
            owner_id   = owner.get("id") or ""
            owner_name_in_payload = owner.get("name") or ""
            assigned   = "yes" if owner_type else "no"

            # Resolve ownerName if missing in payload
            owner_name = resolve_owner_name(owner_type, owner_id, owner_name_in_payload)

            w.writerow({
                "phoneNumber": phone,
                "extension": ext,
                "status": stat,
                "locationId": loc_id,
                "locationName": loc_name,
                "assigned": assigned,
                "ownerType": owner_type,
                "ownerName": owner_name,   # filled from lookup/payload
                "ownerId": owner_id,
                "numberType": number_type,
            })

    print(f"✅ Wrote {out_csv}")
    print(f"   Total numbers exported: {total}")

def main():
    print("▶ Exporting Control Hub numbers (phone, extension, location, status) with ownerName lookup…")
    try:
        export_numbers()
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
