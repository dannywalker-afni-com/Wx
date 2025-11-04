import csv, requests, getpass, os, time, random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def make_session():
    retry = Retry(
        total=6, connect=6, read=6, status=6,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods={"DELETE"},
        respect_retry_after_header=True,
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=1, pool_maxsize=10))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s

def delete_people():
    bearer_token = getpass.getpass("Enter your Webex Bearer Token: ").strip()
    base_url = "https://webexapis.com/v1/people/"
    csv_file = os.path.join(os.path.dirname(__file__), "deleteperson.csv")
    if not os.path.exists(csv_file):
        print(f"Error: '{csv_file}' not found in the same directory as this script.")
        return

    session = make_session()
    headers = {"Authorization": f"Bearer {bearer_token}"}

    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        if 'personid' not in reader.fieldnames:
            print("Error: CSV must have a header named 'personid'.")
            return

        for i, row in enumerate(reader, 1):
            person_id = (row.get('personid') or '').strip()
            if not person_id:
                continue

            url = f"{base_url}{person_id}"

            # retry a few times if we hit SSLError (OpenSSL chain hiccup)
            for attempt in range(1, 4):
                try:
                    resp = session.delete(url, headers=headers, timeout=15)
                    break
                except requests.exceptions.SSLError as e:
                    if attempt < 3:
                        print(f"[{i}] SSL verify hiccup, retrying ({attempt}/3)…")
                        time.sleep(0.6 * attempt * random.uniform(0.8, 1.3))
                        continue
                    else:
                        print(f"[{i}] Failed to delete {person_id}: SSLError {e}")
                        resp = None
                except requests.exceptions.ConnectionError as e:
                    if attempt < 3:
                        print(f"[{i}] Connection error, retrying ({attempt}/3)…")
                        time.sleep(0.6 * attempt * random.uniform(0.8, 1.3))
                        continue
                    else:
                        print(f"[{i}] Failed to delete {person_id}: ConnectionError {e}")
                        resp = None

            if resp is None:
                continue

            # Handle rate limit gracefully
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", "1"))
                print(f"[{i}] Rate limited. Sleeping {wait}s…")
                time.sleep(wait)
                # one immediate retry after waiting
                resp = session.delete(url, headers=headers, timeout=15)

            if resp.status_code == 204:
                print(f"[{i}] Successfully deleted: {person_id}")
            else:
                print(f"[{i}] Failed to delete: {person_id} (Status: {resp.status_code})")
                try:
                    print("     Response:", resp.json())
                except Exception:
                    if resp.text:
                        print("     Body:", resp.text[:300])
                    else:
                        print("     No JSON response body")

if __name__ == "__main__":
    delete_people()
