import csv
import requests
import getpass
import os
import time

def update_person_ids():
    
    
    # ----- Environment setup -----
    # base_url = "https://webexapis.com/v1/people"
    BASE = os.getenv("WEBEX_BASE", "https://webexapis.com/v1/people")

    # TOKEN = "MTkxZDZjODctODA5My00ZWZmLThlZmUtMzNjZDkzM2I5NjkxMGVmYTg0MjAtYzRm_PF84_ebc31646-bc26-4a20-8500-c3030ebd6a52"    
    TOKEN = os.getenv("WEBEX_TOKEN").strip()  # Set this before running
    
    #     ORG_ID = "ebc31646-bc26-4a20-8500-c3030ebd6a52"
    ORG_ID = os.getenv("WEBEX_ORG_ID").strip()



    # Bearer URL https://developer.webex.com/calling/docs/getting-started use this URL to validate the TOKEN and ORG_ID
    # use sec WEBEX from CMD 
    
    
    bearer_token = TOKEN
   
    base_url = BASE

    csv_file = os.path.join(os.path.dirname(__file__), "email2personID.csv")

    if not os.path.exists(csv_file):
        print(f"Error: '{csv_file}' not found in script directory.")
        return

    with open(csv_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames

        if 'email' not in fieldnames:
            print("Error: CSV must have a header named 'email'.")
            return
        if 'personid' not in fieldnames:
            fieldnames.append('personid')

        rows = list(reader)

    for row in rows:
        email = row['email'].strip()
        personid = row.get('personid', '').strip()

        if not email:
            continue
        
        if personid:
            print(f"Skipping {email} (already has ID)")
            continue

        params = {'email': email}
        headers = {'Authorization': f'Bearer {bearer_token}'}
        response = requests.get(base_url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            if data.get('items'):
                row['personid'] = data['items'][0]['id']
                print(f"Found {email} → {row['personid']}")
            else:
                row['personid'] = 'NOT_FOUND'
                print(f"No match for {email}")
        else:
            row['personid'] = f"ERROR_{response.status_code}"
            print(f"API error {response.status_code} for {email}")

        time.sleep(0.25)

    with open(csv_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nFinished updating '{csv_file}' with person IDs.")

if __name__ == "__main__":
    update_person_ids()
