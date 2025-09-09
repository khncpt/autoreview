import os
import io
import random
import requests
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# --- Config ---
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
CSV_URL = os.environ["CSV_URL"]  
REVIEWED_USERNAME = "air"
REVIEWED_EMAIL = "Ceokhan@gmail.com"  

# --- Init ---
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def load_reviews() -> list[str]:
    """Download CSV from HTTPS URL and extract reviews."""
    resp = requests.get(CSV_URL, timeout=30)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text), header=None)
    return df[0].dropna().astype(str).tolist()


def main():
    try:
        reviews = load_reviews()

        # Fetch usernames from user and instagramProfile
        users = supabase.table("user").select("username").execute()
        insta_profiles = supabase.table("instagramProfile").select("username").execute()

        all_usernames = [
            *(u["username"] for u in users.data if u.get("username")),
            *(i["username"] for i in insta_profiles.data if i.get("username")),
        ]

        # Already reviewed usernames
        existing = supabase.table("review") \
            .select("reviewer_username") \
            .eq("reviewed_username", REVIEWED_USERNAME) \
            .execute()
        already_reviewed = {r["reviewer_username"] for r in existing.data}

        inserted = 0
        skipped = []

        for reviewer_username in all_usernames:
            if (
                reviewer_username.lower() == REVIEWED_USERNAME.lower()
                or reviewer_username in already_reviewed
            ):
                skipped.append(reviewer_username)
                continue

            random_review = random.choice(reviews)

            supabase.table("review").insert({
                "reviewed_username": REVIEWED_USERNAME,
                "reviewer_username": reviewer_username,
                "review": random_review,
            }).execute()

            inserted += 1

        print({
            "message": "Review assignment completed",
            "totalUsernames": len(all_usernames),
            "inserted": inserted,
            "skipped": len(skipped),
        })

    except Exception as e:
        print({"error": str(e)})


if __name__ == "__main__":
    main()
