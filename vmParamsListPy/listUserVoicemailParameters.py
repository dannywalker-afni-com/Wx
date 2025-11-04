import os
import csv
import sys
import requests

# ----- Environment setup -----
BASE = os.getenv("WEBEX_BASE", "https://webexapis.com")
TOKEN = os.getenv("WEBEX_TOKEN")  # Set this before running
ORG_ID = os.getenv("WEBEX_ORG_ID")
# TOKEN = "MTkxZDZjODctODA5My00ZWZmLThlZmUtMzNjZDkzM2I5NjkxMGVmYTg0MjAtYzRm_PF84_ebc31646-bc26-4a20-8500-c3030ebd6a52"
# ORG_ID = "ebc31646-bc26-4a20-8500-c3030ebd6a52"

if not TOKEN:
    sys.exit("ERROR: Set the WEBEX_TOKEN environment variable to a valid admin token.")

# ----- HTTP headers -----
def hdrs():
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

# ----- Get voicemail parameters -----
def get_voicemail_for_person(person_id: str):
    params = {"orgId": ORG_ID} if ORG_ID else {}
    url = f"{BASE}/v1/people/{person_id}/features/voicemail"
    r = requests.get(url, headers=hdrs(), params=params, timeout=30)
    if r.status_code == 200:
        return r.json()
    else:
        print(f"❌ {person_id}: {r.status_code} {r.text}")
        return None

# ----- Main workflow -----
def main():
    input_csv = "../email2personID.csv"
    output_csv = "userVoicemailParameters.csv"

    if not os.path.exists(input_csv):
        sys.exit(f"CSV file not found: {input_csv}")

    with open(input_csv, newline="") as infile, open(output_csv, "w", newline="") as outfile:
        reader = csv.DictReader(infile)
        fieldnames = ["email", "personid", "enabled", "storageType", "mwiEnabled",
                      "externalEmail", "emailCopyEnabled", "emailCopyId"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            email = row.get("email", "").strip()
            person_id = row.get("personid", "").strip()
            if not person_id:
                print(f"⚠️  Skipping missing personid for {email}")
                continue

            data = get_voicemail_for_person(person_id)
            if not data:
                continue

            # Extract key voicemail parameters
            enabled = data.get("enabled")
            messageStorage = data.get("messageStorage", {})
            emailCopy = data.get("emailCopyOfMessage", {})

            record = {
                "email": email,
                "personid": person_id,
                "enabled": enabled,
                "storageType": messageStorage.get("storageType"),
                "mwiEnabled": messageStorage.get("mwiEnabled"),
                "externalEmail": messageStorage.get("externalEmail"),
                "emailCopyEnabled": emailCopy.get("enabled"),
                "emailCopyId": emailCopy.get("emailId")
            }

            writer.writerow(record)

            # Console output
            print(f"\n📧 {email} ({person_id})")
            print(f"  Voicemail Enabled: {enabled}")
            print(f"  Storage Type: {record['storageType']}")
            print(f"  MWI Enabled: {record['mwiEnabled']}")
            print(f"  External Email: {record['externalEmail']}")
            print(f"  Email Copy Enabled: {record['emailCopyEnabled']}")
            print(f"  Email Copy ID: {record['emailCopyId']}")

    print(f"\n✅ All results written to {output_csv}")

# ----- Entrypoint -----
if __name__ == "__main__":
    main()
