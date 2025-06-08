# scripts/preprocess.py

import pandas as pd


def preprocess_reviews(input_path="data/raw/reviews_raw.csv", output_path="data/cleaned/reviews_cleaned.csv"):
    df = pd.read_csv(input_path)

    # Drop missing and duplicate reviews
    df.dropna(subset=["review", "rating", "date"], inplace=True)
    df.drop_duplicates(subset=["review", "bank"], inplace=True)

    # Clean text
    df["review"] = df["review"].str.strip()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime('%Y-%m-%d')

    df.to_csv(output_path, index=False)
    print(f"Cleaned data saved to: {output_path}")
    return df


if __name__ == "__main__":
    preprocess_reviews()
