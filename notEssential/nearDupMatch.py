import csv
import re
from difflib import SequenceMatcher
from pathlib import Path

INPUT = Path("emails.txt")
OUTPUT = Path("possible_duplicates.csv")

# --- TUNABLES ---
VERY_LIKELY_THRESHOLD = 0.90
REVIEW_THRESHOLD = 0.80
TREAT_GMAIL_DOTS_AS_INSIGNIFICANT = True
STRIP_PLUS_TAGS = True  # remove +anything in the local part
COMPARE_WHOLE_EMAIL_TOO = True  # besides local-part-only

def normalize_email(addr: str) -> str:
    addr = addr.strip().lower()
    # Simple validation-ish
    if "@" not in addr:
        return addr
    local, domain = addr.split("@", 1)

    if STRIP_PLUS_TAGS:
        local = local.split("+", 1)[0]

    if TREAT_GMAIL_DOTS_AS_INSIGNIFICANT and domain in {"gmail.com", "googlemail.com"}:
        local = local.replace(".", "")

    # You can add other domain-specific normalizations here if your environment warrants it.
    return f"{local}@{domain}"

def local_part(addr: str) -> str:
    return addr.split("@", 1)[0].lower().strip()

def sim(a: str, b: str) -> float:
    # SequenceMatcher ratio is [0..1]
    return SequenceMatcher(None, a, b).ratio()

def main():
    if not INPUT.exists():
        print(f"Input file not found: {INPUT.resolve()}")
        return

    with INPUT.open("r", encoding="utf-8") as f:
        raw_emails = [line.strip() for line in f if line.strip()]

    # Keep original + normalized for reporting
    rows = []
    for i, e in enumerate(raw_emails, start=1):
        norm = normalize_email(e)
        rows.append((i, e, norm, local_part(norm)))

    suspicious = []
    for i in range(len(rows) - 1):
        line_a, raw_a, norm_a, local_a = rows[i]
        line_b, raw_b, norm_b, local_b = rows[i + 1]

        # Compare local parts (most telling)
        s_local = sim(local_a, local_b)

        # Optional: also compare full normalized emails (helps catch domain typos)
        s_full = sim(norm_a, norm_b) if COMPARE_WHOLE_EMAIL_TOO else None

        # Choose the higher score for decision, but include both in report
        score_for_flag = max(s_local, s_full or 0)

        if score_for_flag >= REVIEW_THRESHOLD:
            label = "VERY LIKELY" if score_for_flag >= VERY_LIKELY_THRESHOLD else "REVIEW"
            suspicious.append({
                "label": label,
                "line_a": line_a,
                "email_a": raw_a,
                "norm_a": norm_a,
                "line_b": line_b,
                "email_b": raw_b,
                "norm_b": norm_b,
                "local_similarity": round(s_local, 3),
                "full_similarity": round(s_full, 3) if s_full is not None else "",
            })

    # Write CSV
    with OUTPUT.open("w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=[
            "label", "line_a", "email_a", "norm_a", "line_b", "email_b", "norm_b",
            "local_similarity", "full_similarity"
        ])
        writer.writeheader()
        writer.writerows(suspicious)

    print(f"Found {len(suspicious)} suspicious adjacent pairs.")
    print(f"Report written to: {OUTPUT.resolve()}")

if __name__ == "__main__":
    main()

