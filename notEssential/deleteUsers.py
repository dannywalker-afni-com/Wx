import csv
import requests
import getpass
import os

def delete_people():
    bearer_token = getpass.getpass("Enter your Webex Bearer Token: ")

    base_url = "https://webexapis.com/v1/people/"

    csv_file = os.path.join(os.path.dirname(__file__), "deleteperson.csv")

    if not os.path.exists(csv_file):
        print(f"Error: '{csv_file}' not found in the same directory as this script.")
        return

    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        if 'personid' not in reader.fieldnames:
            print("Error: CSV must have a header named 'personid'.")
            return

        for row in reader:
            person_id = row['personid'].strip()
            if not person_id:
                continue

            url = f"{base_url}{person_id}"
            headers = {
                "Authorization": f"Bearer {bearer_token}"
            }

            response = requests.delete(url, headers=headers)

            # Output result
            if response.status_code == 204:
                print(f"Successfully deleted: {person_id}")
            else:
                print(f"Failed to delete: {person_id} (Status: {response.status_code})")
                try:
                    print("   Response:", response.json())
                except Exception:
                    print("   No JSON response body")

if __name__ == "__main__":
    delete_people()
