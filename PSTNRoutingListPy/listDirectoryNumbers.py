#!/usr/bin/env python3
"""
Lists Webex PSTN Routing Directory Number assignments:
Route Lists -> assigned phone numbers (directory numbers).

Outputs:
  - pstn_directory_number_assignments.csv
"""

import os, sys, csv, requests

# ---------- Config ----------
BASE   = (os.getenv("WEBEX_BASE", "https://webexapis.com") or "").strip()
TOKEN  = (os.getenv("WEBEX_TOKEN", "") or "").strip()
ORG_ID = (os.getenv("WEBEX_ORG_ID", "") or "").strip()

if not TOKEN:
    sys.exit("ERROR: Set WEBEX_TOKEN to a valid Webex admin token.")

TIMEOUT = 30
PAGE_SZ = 100

def hdrs():
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

def add_org(params=None):
    params = dict(params or {})
    if ORG_ID:
        params["orgId"] = ORG_ID
    return params

def get_pages(url: str, params: dict | None = None):
    """
    Generic pager that follows Link: rel="next".
    """
    params = add_org(params)
    while True:
        r = requests.get(url, headers=hdrs(), params=params, timeout=TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
        data = r.json()
        yield data

        link = r.headers.get("Link", "")
        next_url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                seg = part.strip().split(";")[0].strip()
                if seg.startswith("<") and seg.endswith(">"):
                    next_url = seg[1:-1]
        if not next_url:
            break
        url, params = next_url, None  # next link already includes params

def get_json(url: str, params: dict | None = None):
    r = requests.get(url, headers=hdrs(), params=add_org(params), timeout=TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
    return r.json()

# ---------- Helpers ----------
def safe(d: dict | None, *path, default=None):
    cur = d or {}
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

# ---------- Route Lists & Numbers ----------
def list_route_lists():
    """
    Yields route list dicts:
      { id, name, location: { id, name }, routeGroup: { id, name }, ... }
    """
    url = f"{BASE}/v1/telephony/config/premisePstn/routeLists"
    try:
        for page in get_pages(url, {"max": PAGE_SZ}):
            for rl in page.get("routeLists", []):
                yield rl
    except RuntimeError as e:
        # Some orgs may not have Route Lists enabled; bubble up for a clear message
        raise

def get_route_list(rl_id: str):
    url = f"{BASE}/v1/telephony/config/premisePstn/routeLists/{rl_id}"
    return get_json(url)

def get_route_list_numbers(rl_id: str):
    """
    Returns: { numbers: ["+15551234567", ...] }
    """
    url = f"{BASE}/v1/telephony/config/premisePstn/routeLists/{rl_id}/numbers"
    return get_json(url)

# ---------- Main export ----------
def export_directory_number_assignments():
    out_csv = "pstn_directory_number_assignments.csv"
    count_rl = 0
    count_nums = 0

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "routeListId",
                "routeListName",
                "locationId",
                "locationName",
                "routeGroupId",
                "routeGroupName",
                "directoryNumber"
            ],
        )
        w.writeheader()

        for rl in list_route_lists():
            count_rl += 1
            rl_id = rl.get("id")
            # Pull full RL details (ensures we have location + RG)
            detail = get_route_list(rl_id)

            rl_name   = detail.get("name") or rl.get("name")
            loc_id    = safe(detail, "location", "id")
            loc_name  = safe(detail, "location", "name")
            rg_id     = safe(detail, "routeGroup", "id")
            rg_name   = safe(detail, "routeGroup", "name")

            # Numbers assigned to this Route List
            try:
                numbers_resp = get_route_list_numbers(rl_id)
                numbers = numbers_resp.get("numbers", [])
            except Exception as e:
                numbers = []

            if not numbers:
                # If a RL has no numbers, you can still emit a row or skip.
                # Here we skip to keep the file strictly "assignments".
                continue

            for num in numbers:
                count_nums += 1
                w.writerow({
                    "routeListId": rl_id,
                    "routeListName": rl_name,
                    "locationId": loc_id,
                    "locationName": loc_name,
                    "routeGroupId": rg_id,
                    "routeGroupName": rg_name,
                    "directoryNumber": num
                })

    print("✅ Wrote:", out_csv)
    print(f"   Route Lists scanned: {count_rl}")
    print(f"   Directory numbers found: {count_nums}")

def main():
    print("▶ Exporting PSTN Routing Directory Number assignments (Route List numbers)…")
    try:
        export_directory_number_assignments()
    except RuntimeError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
