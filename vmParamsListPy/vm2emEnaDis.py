#!/usr/bin/env python3
"""
vm2emEnaDis.py
Update Webex Calling voicemail settings for users from a CSV, then print & export
the resulting voicemail parameters for each user to vmParms.csv as aligned key: value lists.

CSV headers (case-insensitive): email, personid
"""

import os
import csv
import sys
import time
import argparse
import requests
from typing import Dict, Any, Optional, Tuple, List

# ----- Environment / defaults -----
BASE = os.getenv("WEBEX_BASE", "https://webexapis.com").rstrip("/")  # FedRAMP: https://api-usgov.webex.com
TOKEN = (os.getenv("WEBEX_TOKEN") or "").strip()                     # REQUIRED: admin token with people/telephony write scopes
ORG_ID = (os.getenv("WEBEX_ORG_ID") or "").strip()                   # optional (partner admin)

CSV_PATH_DEFAULT = "../email2personID.csv"
TIMEOUT = 30
OUT_CSV = "vmParms.csv"

# ----- Console helpers -----
def _print_kv_block(title: str, kv: Dict[str, Any], order: List[str]) -> None:
    print(title)
    width = max(len(k) for k in order)
    for k in order:
        v = kv.get(k, "")
        if v is None:
            v = ""
        print(f"  {k:<{width}} : {v}")
    print("")

def _print_step(step_title: str) -> None:
    print(step_title)

# ----- HTTP helpers -----
def _hdrs() -> Dict[str, str]:
    if not TOKEN:
        sys.exit("ERROR: Set WEBEX_TOKEN environment variable to a valid admin bearer token.")
    return {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def _put(path: str, params: Optional[Dict[str, Any]] = None, json: Optional[Dict[str, Any]] = None) -> requests.Response:
    url = f"{BASE}{path}"
    backoff = 1.0
    for _ in range(5):
        r = requests.put(url, headers=_hdrs(), params=params or {}, json=json, timeout=TIMEOUT)
        if r.status_code in (429, 502, 503, 504):
            sleep_for = float(r.headers.get("Retry-After", backoff))
            time.sleep(min(sleep_for, 10.0))
            backoff *= 1.6
            continue
        return r
    return r

def _get(path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
    url = f"{BASE}{path}"
    backoff = 1.0
    for _ in range(5):
        r = requests.get(url, headers=_hdrs(), params=params or {}, timeout=TIMEOUT)
        if r.status_code in (429, 502, 503, 504):
            sleep_for = float(r.headers.get("Retry-After", backoff))
            time.sleep(min(sleep_for, 10.0))
            backoff *= 1.6
            continue
        return r
    return r

# ----- Payload builder -----
def build_voicemail_body(
    vm_on: bool,
    mode: str,
    dest_email: Optional[str],
    *,
    mwi: Optional[str] = None,         # "on" | "off" | None
    external: Optional[str] = None,    # None=leave, ""=clear, "<email>"=set
    notify: Optional[str] = None,      # "on" | "off" | None
    notify_dest: Optional[str] = None  # None=leave/choose, ""=clear, "<email>"=set
) -> Dict[str, Any]:
    """
    mode: 'internal' | 'copy' | 'external'
    mwi:  None (no override) | 'on' | 'off'
    external: None (do not force externalEmail) | "" (clear) | "<email>" (set)
    notify: None (no override) | 'on' | 'off'
    notify_dest: None (use row/leave), "" (clear), or an email address
    """
    mode = (mode or "").lower()
    mwi_override: Optional[bool] = None if mwi is None else (mwi == "on")
    notify_override: Optional[bool] = None if notify is None else (notify == "on")

    body: Dict[str, Any] = {
        "enabled": bool(vm_on),
        "notifications": {"enabled": False, "destination": ""},
        "transferToNumber": {"enabled": False, "destination": ""},
        "messageStorage": {},
        "emailCopyOfMessage": {"enabled": False, "emailId": ""}
    }

    def apply_mwi(default_val: bool) -> bool:
        return default_val if mwi_override is None else mwi_override

    def apply_notify(default_enabled: bool, default_dest: str) -> Dict[str, Any]:
        enabled = default_enabled if notify_override is None else notify_override
        # Destination selection: explicit CLI beats everything, else default_dest (caller passes CSV email)
        if notify_dest is not None:
            dest = notify_dest  # empty string clears, otherwise provided email
        else:
            dest = default_dest
        # If enabling without a destination, force disabled & blank to avoid API errors
        if enabled and not dest:
            enabled = False
            dest = ""
        return {"enabled": enabled, "destination": dest or ""}

    # ----- Voicemail OFF: force INTERNAL, disable MWI, clear external/copy & notifications -----
    if not vm_on:
        body["messageStorage"]["storageType"] = "INTERNAL"
        body["messageStorage"]["mwiEnabled"] = apply_mwi(False)
        body["messageStorage"]["externalEmail"] = ""  # explicit clear
        body["emailCopyOfMessage"]["enabled"] = False
        body["emailCopyOfMessage"]["emailId"] = ""
        body["notifications"] = apply_notify(False, "")
        return body

    # ----- Voicemail ON -----
    if mode == "internal":
        body["messageStorage"]["storageType"] = "INTERNAL"
        body["messageStorage"]["mwiEnabled"] = apply_mwi(True)
        body["messageStorage"]["externalEmail"] = ""  # ensure cleared

    elif mode == "copy":
        body["messageStorage"]["storageType"] = "INTERNAL"
        body["messageStorage"]["mwiEnabled"] = apply_mwi(True)
        body["messageStorage"]["externalEmail"] = ""  # ensure cleared
        if dest_email:
            body["emailCopyOfMessage"]["enabled"] = True
            body["emailCopyOfMessage"]["emailId"] = dest_email
        else:
            body["emailCopyOfMessage"]["enabled"] = False
            body["emailCopyOfMessage"]["emailId"] = ""

    elif mode == "external":
        body["messageStorage"]["storageType"] = "EXTERNAL"
        body["messageStorage"]["mwiEnabled"] = apply_mwi(False)  # typically off for EXTERNAL
        if external is not None:
            body["messageStorage"]["externalEmail"] = external   # "" clears; email sets
        else:
            body["messageStorage"]["externalEmail"] = dest_email or ""
        body["emailCopyOfMessage"]["enabled"] = False
        body["emailCopyOfMessage"]["emailId"] = ""

    else:
        raise ValueError("mode must be one of: internal | copy | external")

    # Notifications: default_dest is the row email (passed from caller)
    body["notifications"] = apply_notify(False, dest_email or "")
    return body

# ----- Normalize GET response into a flat row (keep CSV/console consistent) -----
def extract_row(email: str, person_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
    ms = data.get("messageStorage", {}) or {}
    ec = data.get("emailCopyOfMessage", {}) or {}
    notif = data.get("notifications", {}) or {}
    xfer = data.get("transferToNumber", {}) or {}

    email_copy_enabled = bool(ec.get("enabled"))
    email_copy_id = (ec.get("emailId") or "")
    if not email_copy_enabled:
        email_copy_id = ""

    return {
        "email": email,
        "personId": person_id,
        "enabled": data.get("enabled"),
        "storageType": ms.get("storageType"),
        "mwiEnabled": ms.get("mwiEnabled"),
        "externalEmail": ms.get("externalEmail") or "",
        "emailCopyEnabled": email_copy_enabled,
        "emailCopyEmailId": email_copy_id,
        "notificationsEnabled": notif.get("enabled"),
        "notificationsDestination": notif.get("destination") or "",
        "transferEnabled": xfer.get("enabled"),
        "transferDestination": xfer.get("destination") or ""
    }

# Pretty console output order
KV_ORDER = [
    "email",
    "personId",
    "enabled",
    "storageType",
    "mwiEnabled",
    "externalEmail",
    "emailCopyEnabled",
    "emailCopyEmailId",
    "notificationsEnabled",
    "notificationsDestination",
    "transferEnabled",
    "transferDestination",
]

# ----- Worker -----
def set_voicemail_for_person(
    person_id: str,
    vm_on: bool,
    mode: str,
    dest_email: Optional[str],
    *,
    mwi: Optional[str] = None,
    external: Optional[str] = None,
    notify: Optional[str] = None,
    notify_dest: Optional[str] = None
) -> Tuple[bool, str]:
    params = {"orgId": ORG_ID} if ORG_ID else {}
    payload = build_voicemail_body(vm_on, mode, dest_email, mwi=mwi, external=external,
                                   notify=notify, notify_dest=notify_dest)
    r = _put(f"/v1/people/{person_id}/features/voicemail", params=params, json=payload)
    if r.status_code in (200, 204):
        return True, ""
    return False, f"{r.status_code} {r.text}"

def get_voicemail_for_person(person_id: str) -> Tuple[bool, Dict[str, Any], str]:
    params = {"orgId": ORG_ID} if ORG_ID else {}
    r = _get(f"/v1/people/{person_id}/features/voicemail", params=params)
    if r.status_code == 200:
        return True, r.json(), ""
    return False, {}, f"{r.status_code} {r.text}"

# ----- Main -----
def main():
    ap = argparse.ArgumentParser(
        description="Update Webex Calling voicemail settings for users from a CSV, then print & export resulting parameters."
    )
    ap.add_argument("--csv", default=CSV_PATH_DEFAULT, help="Path to CSV (default: ../email2personID.csv)")
    ap.add_argument("--vm", choices=["on", "off"], default="on", help="Turn voicemail on or off (default: on)")
    ap.add_argument("--mode", choices=["internal", "copy", "external"], default="copy",
                    help="When VM is on: internal, copy (default), or external")
    ap.add_argument("--dest", default=None,
                    help="For mode=copy: email to send a copy to. If not given, falls back to the CSV 'email' column.")
    ap.add_argument("--mwi", choices=["on", "off"], default=None,
                    help="Explicitly set Message Waiting Indicator. If omitted, a sensible default is used.")
    ap.add_argument("--external", default=None,
                    help='Set external voicemail mailbox. Use "" to CLEAR any existing external email.')
    ap.add_argument("--notify", choices=["on", "off"], default=None,
                    help="Enable/disable voicemail notifications. If omitted, a sensible default is used.")
    ap.add_argument("--notify-dest", default=None,
                    help='Notification destination email. Omit to use the row CSV email. Use "" to CLEAR.')
    ap.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between requests (default: 0.2)")
    args = ap.parse_args()

    if not os.path.exists(args.csv):
        sys.exit(f"CSV not found: {args.csv}")

    rows_out: List[Dict[str, Any]] = []
    total = ok = fail = 0

    with open(args.csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = {h.lower(): h for h in (reader.fieldnames or [])}
        if "email" not in headers or "personid" not in headers:
            sys.exit("CSV must contain headers: email,personid")

        for row in reader:
            total += 1
            email = (row.get(headers["email"]) or "").strip()
            person_id = (row.get(headers["personid"]) or "").strip()

            if not person_id:
                _print_step(f"❌ Row {total}: missing personid, skipping.")
                fail += 1
                continue

            # For mode=copy, prefer CLI --dest; otherwise fall back to CSV 'email' column
            if args.mode == "copy":
                dest_email = (args.dest if args.dest not in (None, "") else (email or None))
            else:
                dest_email = (args.dest or email or None)

            # Notifications: prefer CLI --notify-dest; otherwise use CSV email
            effective_notify_dest = args.notify_dest if args.notify_dest is not None else (email or "")
            vm_on = (args.vm == "on")

            # REQUEST preview (effective values)
            copy_enabled_req = bool(vm_on and args.mode == "copy" and dest_email)
            copy_email_req   = dest_email if copy_enabled_req else ""
            # Effective notifications for preview
            notify_enabled_req = (args.notify == "on") if args.notify is not None else False
            # If enabling with no destination, show as disabled/blank to match payload logic
            if notify_enabled_req and not effective_notify_dest:
                notify_enabled_req = False
            notify_email_req = effective_notify_dest if notify_enabled_req else ""

            _print_kv_block(
                f"→ [{total}] REQUEST",
                {
                    "email": email or "(no-email)",
                    "personId": person_id,
                    "enabled": vm_on,
                    "storageType": args.mode.upper(),
                    "mwiEnabled": args.mwi if args.mwi is not None else "(default)",
                    "externalEmail": (args.external if args.external is not None else "(leave-as-is)"),
                    "emailCopyEnabled": copy_enabled_req,
                    "emailCopyEmailId": copy_email_req,
                    "notificationsEnabled": notify_enabled_req if args.notify is not None else "(default/false)",
                    "notificationsDestination": notify_email_req,
                    "transferEnabled": False,
                    "transferDestination": ""
                },
                KV_ORDER
            )

            # Apply change
            success, err = set_voicemail_for_person(
                person_id, vm_on, args.mode, dest_email,
                mwi=args.mwi, external=args.external,
                notify=args.notify, notify_dest=args.notify_dest if args.notify_dest is not None else (email or "")
            )
            if not success:
                _print_step(f"   ❌ UPDATE ERROR: {err}")
                fail += 1
            else:
                ok += 1

            # Read back & print normalized parameters
            got, data, gerr = get_voicemail_for_person(person_id)
            if got:
                flat = extract_row(email, person_id, data)
                rows_out.append(flat)
                _print_kv_block("   ✅ CURRENT PARAMS", flat, KV_ORDER)
            else:
                _print_step(f"   ❌ READBACK ERROR: {gerr}")

            time.sleep(args.sleep)

    # Write vmParms.csv
    if rows_out:
        fieldnames = KV_ORDER
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as outf:
            writer = csv.DictWriter(outf, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows_out:
                writer.writerow(r)
        print(f"📄 Wrote {OUT_CSV} with {len(rows_out)} rows.")

    print(f"Done. Total: {total}  Updated OK: {ok}  Failed updates: {fail}")
    if fail and ok == 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
