# scripts/scraper.py

from google_play_scraper import reviews, Sort
import pandas as pd
from tqdm import tqdm

BANKS = {
    "CBE": "com.cbe.customerapp",
    "BOA": "com.boa.bankapp",
    "Dashen": "com.dashen.bankapp"
}


def scrape_reviews(app_id, bank_name, num_reviews=400):
    all_reviews = []
    count = 0

    print(f"Scraping {bank_name}...")
    while count < num_reviews:
        r, _ = reviews(
            app_id,
            count=200,
            filter_score_with=None,
            sort=Sort.NEWEST
        )
        for review in r:
            if count >= num_reviews:
                break
            all_reviews.append({
                "review": review.get("content", "").strip(),
                "rating": review.get("score", None),
                "date": review.get("at").date().isoformat() if review.get("at") else None,
                "bank": bank_name,
                "source": "Google Play"
            })
            count += 1

    return pd.DataFrame(all_reviews)


def scrape_all_banks(output_path="data/raw/reviews_raw.csv"):
    dfs = []
    for bank, app_id in BANKS.items():
        df = scrape_reviews(app_id, bank)
        dfs.append(df)

    full_df = pd.concat(dfs, ignore_index=True)
    full_df.to_csv(output_path, index=False)
    print(f"\nSaved all reviews to: {output_path}")


if __name__ == "__main__":
    scrape_all_banks()
