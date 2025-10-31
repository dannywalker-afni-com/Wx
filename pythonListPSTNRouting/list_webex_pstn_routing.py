#!/usr/bin/env python3
import os, sys, csv, time, requests
from urllib.parse import urlencode

# -------- Config --------
BASE  = os.getenv("WEBEX_BASE", "https://webexapis.com").strip()
TOKEN = os.getenv("WEBEX_TOKEN").strip()  # REQUIRED: admin token
ORG_ID = os.getenv("WEBEX_ORG_ID").strip()  # optional (required if partner admin)

if not TOKEN:
    sys.exit("ERROR: Set WEBEX_TOKEN to a valid Webex admin token.")

TIMEOUT = 30
MAX = 100  # page size

def hdrs():
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

def add_org(params: dict | None = None):
    params = dict(params or {})
    if ORG_ID:
        params["orgId"] = ORG_ID
    return params

def get_pages(url: str, params: dict | None = None):
    """
    Generic RFC5988 pagination helper (follows Link: rel="next")
    """
    params = add_org(params)
    while True:
        r = requests.get(url, headers=hdrs(), params=params, timeout=TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
        data = r.json()
        yield data
        # Follow Link: <...>; rel="next"
        link = r.headers.get("Link", "")
        next_url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                # format: <URL>; rel="next"
                l = part.strip().split(";")[0].strip()
                if l.startswith("<") and l.endswith(">"):
                    next_url = l[1:-1]
        if not next_url:
            break
        # When a 'next' link is present it includes all params already
        url, params = next_url, None

def get_json(url: str, params: dict | None = None):
    r = requests.get(url, headers=hdrs(), params=add_org(params), timeout=TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
    return r.json()

def safe(d: dict, *path, default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

# -------- 1) LOCATIONS + PSTN CONNECTION --------
def list_locations():
    url = f"{BASE}/v1/locations"
    for page in get_pages(url, {"max": MAX}):
        for loc in page.get("items", page.get("locations", [])):
            yield loc

def get_location_pstn_connection(location_id: str):
    # PSTN connection for a location (type/provider details)
    url = f"{BASE}/v1/telephony/pstn/locations/{location_id}/connection"
    try:
        return get_json(url)
    except Exception as e:
        # Some tenants may lack permission; return marker
        return {"_error": str(e)}

def export_locations_with_pstn():
    with open("locations_pstn.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "locationId","locationName","country","addressLine1","city","state","zip",
            "pstnType","provider","connectionStatus","connectionId","_error"
        ])
        w.writeheader()
        for loc in list_locations():
            loc_id = loc.get("id")
            pstn = get_location_pstn_connection(loc_id)
            w.writerow({
                "locationId": loc_id,
                "locationName": loc.get("name"),
                "country": safe(loc, "address", "country"),
                "addressLine1": safe(loc, "address", "address1"),
                "city": safe(loc, "address", "city"),
                "state": safe(loc, "address", "state"),
                "zip": safe(loc, "address", "zip"),
                "pstnType": pstn.get("type") or pstn.get("connectionType"),
                "provider": pstn.get("provider") or pstn.get("ccpProvider"),
                "connectionStatus": pstn.get("status"),
                "connectionId": pstn.get("id"),
                "_error": pstn.get("_error","")
            })

# -------- 2) TRUNKS (Local Gateways) --------
def list_trunks():
    url = f"{BASE}/v1/telephony/config/premisePstn/trunks"
    for page in get_pages(url, {"max": MAX}):
        for t in page.get("trunks", []):
            yield t

def get_trunk(trunk_id: str):
    url = f"{BASE}/v1/telephony/config/premisePstn/trunks/{trunk_id}"
    return get_json(url)

def export_trunks():
    with open("trunks.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "trunkId","name","locationId","locationName","trunkType","deviceType",
            "sipRegistrar","outboundProxy","port","status","inUse","routeGroupCount"
        ])
        w.writeheader()
        for t in list_trunks():
            tid = t.get("id")
            d = {}
            try:
                d = get_trunk(tid)
            except Exception as e:
                d = {"name": t.get("name"), "location": t.get("location",{}), "_error": str(e)}
            w.writerow({
                "trunkId": tid,
                "name": d.get("name") or t.get("name"),
                "locationId": safe(d,"location","id") or safe(t,"location","id"),
                "locationName": safe(d,"location","name") or safe(t,"location","name"),
                "trunkType": d.get("trunkType") or d.get("type"),
                "deviceType": safe(d,"deviceType","name"),
                "sipRegistrar": safe(d,"sipRegistration","domain") or safe(d,"termination","domain"),
                "outboundProxy": safe(d,"termination","outboundProxyAddress"),
                "port": safe(d,"termination","port"),
                "status": d.get("status"),
                "inUse": d.get("inUse"),
                "routeGroupCount": len(safe(d,"routeGroups", default=[])),
            })

# -------- 3) ROUTE GROUPS + USAGE --------
def list_route_groups():
    url = f"{BASE}/v1/telephony/config/premisePstn/routeGroups"
    for page in get_pages(url, {"max": MAX}):
        for rg in page.get("routeGroups", []):
            yield rg

def get_route_group(rg_id: str):
    url = f"{BASE}/v1/telephony/config/premisePstn/routeGroups/{rg_id}"
    return get_json(url)

def get_route_group_usage_pstn(rg_id: str):
    url = f"{BASE}/v1/telephony/config/premisePstn/routeGroups/{rg_id}/usagePstnConnection"
    return get_json(url)

def export_route_groups():
    with open("route_groups.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "routeGroupId","name","inUse","localGatewayCount","localGateways"
        ])
        w.writeheader()
        for rg in list_route_groups():
            rgid = rg.get("id")
            detail = get_route_group(rgid)
            lgs = detail.get("localGateways", [])
            w.writerow({
                "routeGroupId": rgid,
                "name": detail.get("name") or rg.get("name"),
                "inUse": detail.get("inUse"),
                "localGatewayCount": len(lgs),
                "localGateways": ";".join([f"{safe(x,'name')}@{safe(x,'location','name')}" for x in lgs]),
            })

    # Usage (where this RG is referenced for PSTN connections)
    with open("route_group_usage_pstn.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["routeGroupId","routeGroupName","locationId","locationName","pstnType"])
        w.writeheader()
        for rg in list_route_groups():
            rgid = rg.get("id")
            usage = get_route_group_usage_pstn(rgid)
            for loc in usage.get("locations", []):
                w.writerow({
                    "routeGroupId": rgid,
                    "routeGroupName": rg.get("name"),
                    "locationId": safe(loc,"id"),
                    "locationName": safe(loc,"name"),
                    "pstnType": usage.get("pstnType"),
                })

# -------- 4) ROUTE LISTS + NUMBERS --------
def list_route_lists():
    url = f"{BASE}/v1/telephony/config/premisePstn/routeLists"
    try:
        for page in get_pages(url, {"max": MAX}):
            for rl in page.get("routeLists", []):
                yield rl
    except RuntimeError:
        # Not all orgs have this feature; silently skip
        return

def get_route_list(rl_id: str):
    url = f"{BASE}/v1/telephony/config/premisePstn/routeLists/{rl_id}"
    return get_json(url)

def get_route_list_numbers(rl_id: str):
    url = f"{BASE}/v1/telephony/config/premisePstn/routeLists/{rl_id}/numbers"
    return get_json(url)

def export_route_lists():
    with open("route_lists.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "routeListId","name","locationId","locationName","routeGroupId","routeGroupName"
        ])
        w.writeheader()
        for rl in list_route_lists():
            d = get_route_list(rl["id"])
            w.writerow({
                "routeListId": d.get("id"),
                "name": d.get("name"),
                "locationId": safe(d,"location","id"),
                "locationName": safe(d,"location","name"),
                "routeGroupId": safe(d,"routeGroup","id"),
                "routeGroupName": safe(d,"routeGroup","name"),
            })

    with open("route_list_numbers.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["routeListId","routeListName","number"])
        w.writeheader()
        for rl in list_route_lists():
            try:
                nums = get_route_list_numbers(rl["id"])
                for n in nums.get("numbers", []):
                    w.writerow({
                        "routeListId": rl["id"],
                        "routeListName": rl.get("name"),
                        "number": n,
                    })
            except Exception:
                continue

# -------- 5) DIAL PLANS --------
def list_dial_plans():
    url = f"{BASE}/v1/telephony/config/dialPlans"
    try:
        for page in get_pages(url, {"max": MAX}):
            for dp in page.get("dialPlans", []):
                yield dp
    except RuntimeError:
        return

def export_dial_plans():
    with open("dial_plans.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "dialPlanId","name","description","routingChoice","trunkId","routeGroupId","patternCount"
        ])
        w.writeheader()
        for dp in list_dial_plans():
            # Some tenants expose more detail via GET /dialPlans/{id}; keep it simple
            w.writerow({
                "dialPlanId": dp.get("id"),
                "name": dp.get("name"),
                "description": dp.get("description"),
                "routingChoice": dp.get("routingChoice"),
                "trunkId": safe(dp,"trunk","id"),
                "routeGroupId": safe(dp,"routeGroup","id"),
                "patternCount": len(dp.get("dialPatterns", [])),
            })

# -------- Main --------
def main():
    print("▶ Exporting Locations + PSTN connection …")
    export_locations_with_pstn()
    print("▶ Exporting Trunks …")
    export_trunks()
    print("▶ Exporting Route Groups + PSTN usage …")
    export_route_groups()
    print("▶ Exporting Route Lists + numbers …")
    export_route_lists()
    print("▶ Exporting Dial Plans …")
    export_dial_plans()
    print("\n✅ Done. CSVs written:")
    print("  - locations_pstn.csv")
    print("  - trunks.csv")
    print("  - route_groups.csv")
    print("  - route_group_usage_pstn.csv")
    print("  - route_lists.csv")
    print("  - route_list_numbers.csv")
    print("  - dial_plans.csv")

if __name__ == "__main__":
    main()
