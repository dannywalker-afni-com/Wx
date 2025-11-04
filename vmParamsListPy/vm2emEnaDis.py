
import os
import csv
import sys
import time
import argparse
import requests

# ----- Environment / defaults -----
BASE = os.getenv("WEBEX_BASE", "https://webexapis.com")     # FedRAMP: https://api-usgov.webex.com

# WEBEX_TOKEN = "Mjc0ZTg2MDQtMmYyNy00ZDUyLWFmZTUtZTExY2UzNGUzM2RmNjEyOWU2ODYtMTVm_PF84_ebc31646-bc26-4a20-8500-c3030ebd6a52"
# WEBEX_ORG_ID = "ebc31646-bc26-4a20-8500-c3030ebd6a52"
TOKEN = os.getenv("WEBEX_TOKEN").strip()                            # REQUIRED: admin token with people_read/write scopes
ORG_ID = os.getenv("WEBEX_ORG_ID").strip()                          # optional (partner admins)

CSV_PATH_DEFAULT = "../email2personID.csv"

# ----- HTTP helpers -----
def _hdrs():
    if not TOKEN:
        sys.exit("ERROR: Set WEBEX_TOKEN environment variable to a valid admin bearer token.")
    return {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def _put(path, params=None, json=None):
    url = f"{BASE}{path}"
    r = requests.put(url, headers=_hdrs(), params=params or {}, json=json, timeout=30)
    return r

# ----- Payload builder -----
def build_voicemail_body(vm_on: bool, mode: str, dest_email: str | None):
    """
    mode: 'internal' | 'copy' | 'external'
    """
    # Base body (include these even when disabled for clarity)
    body = {
        "enabled": bool(vm_on),
        "notifications": {"enabled": False, "destination": ""},
        "transferToNumber": {"enabled": False, "destination": ""},
        # You can also set sendAllCalls/sendBusyCalls/sendUnansweredCalls blocks if needed
    }

    if not vm_on:
        return body

    mode = mode.lower()
    if mode == "internal":
        body["messageStorage"] = {"mwiEnabled": True, "storageType": "INTERNAL"}
        body["emailCopyOfMessage"] = {"enabled": False, "emailId": ""}
    elif mode == "copy":
        # keep internal mailbox and also email a copy
        body["messageStorage"] = {"mwiEnabled": True, "storageType": "INTERNAL"}
        body["emailCopyOfMessage"] = {
            "enabled": bool(dest_email),
            "emailId": dest_email or ""
        }
    elif mode == "external":
        if not dest_email:
            raise ValueError("external mode requires a destination email (use --dest or have email in CSV).")
        # store ONLY in external mailbox (not visible in Webex App/phones)
        body["messageStorage"] = {
            "mwiEnabled": False,
            "storageType": "EXTERNAL",
            "externalEmail": dest_email
        }
        body["emailCopyOfMessage"] = {"enabled": False, "emailId": dest_email}
    else:
        raise ValueError("mode must be one of: internal | copy | external")

    return body

# ----- Worker -----
def set_voicemail_for_person(person_id: str, vm_on: bool, mode: str, dest_email: str | None):
    params = {"orgId": ORG_ID} if ORG_ID else {}
    payload = build_voicemail_body(vm_on, mode, dest_email)
    r = _put(f"/v1/people/{person_id}/features/voicemail", params=params, json=payload)
    if r.status_code in (200, 204):
        return True, ""
    return False, f"{r.status_code} {r.text}"

# ----- Main -----
def main():
    ap = argparse.ArgumentParser(description="Process each row in email2personID.csv to update Webex Calling voicemail settings.")
    ap.add_argument("--csv", default=CSV_PATH_DEFAULT, help="Path to CSV (default: email2personID.csv)")
    ap.add_argument("--vm", choices=["on", "off"], default="on", help="Turn voicemail on or off (default: on)")
    ap.add_argument("--mode", choices=["internal", "copy", "external"], default="copy",
                    help="When VM is on: internal, copy (default), or external")
    ap.add_argument("--dest", default=None,
                    help="Override destination email for all rows (default: use 'email' column from CSV)")
    ap.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between requests (default: 0.2)")
    args = ap.parse_args()

    if not os.path.exists(args.csv):
        sys.exit(f"CSV not found: {args.csv}")

    total = 0
    ok = 0
    fail = 0

    with open(args.csv, newline="") as f:
        reader = csv.DictReader(f)
        required = {"email", "personid"}
        if not required.issubset({c.lower() for c in reader.fieldnames or []}):
            sys.exit("CSV must contain headers: email,personid")

        for row in reader:
            total += 1
            email = (row.get("email") or "").strip()
            person_id = (row.get("personid") or "").strip()

            if not person_id:
                print(f"❌ Row {total}: missing personid, skipping.")
                fail += 1
                continue

            dest_email = args.dest or email or None
            vm_on = (args.vm == "on")

            print(f"→ [{total}] {email or '(no-email)'} :: {person_id} :: vm={args.vm} mode={args.mode} dest={dest_email or '-'}")
            try:
                success, err = set_voicemail_for_person(person_id, vm_on, args.mode, dest_email)
                if success:
                    print(f"   ✅ updated")
                    ok += 1
                else:
                    print(f"   ❌ {err}")
                    fail += 1
            except Exception as e:
                print(f"   ❌ Exception: {e}")
                fail += 1

            time.sleep(args.sleep)

    print(f"\nDone. Total: {total}  OK: {ok}  Failed: {fail}")
    if fail:
        sys.exit(1)

if __name__ == "__main__":
    main()
