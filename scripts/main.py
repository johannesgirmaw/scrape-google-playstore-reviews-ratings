
import pandas as pd
from google_play_scraper import Sort, reviews
from datetime import datetime
import os
import time


class GooglePlayReviewScraper:
    """
    A class to scrape user reviews from the Google Play Store for specified banking apps,
    preprocess the collected data, and save them to separate CSV files (one per bank).
    """

    def __init__(self, raw_output_dir="data/raw", cleaned_output_dir="data/cleaned", target_reviews_per_bank=400):
        """
        Initializes the scraper with app configurations and output settings.

        Args:
            raw_output_dir (str): Directory to save the raw output CSV files.
            cleaned_output_dir (str): Directory to save the cleaned output CSV files.
            target_reviews_per_bank (int): Minimum number of reviews to scrape per bank.
        """
        self.raw_output_dir = raw_output_dir
        self.cleaned_output_dir = cleaned_output_dir
        self.target_reviews_per_bank = target_reviews_per_bank
        self.app_configs = {
            # Verified app ID (common)
            "Commercial Bank of Ethiopia Mobile": "com.combanketh.mobilebanking",
            # Verified app ID (common)
            "Bank of Abyssinia Mobile": "com.boa.boaMobileBanking",
            # Verified app ID (common)
            "Dashen Bank Mobile": "com.dashen.dashensuperapp"
        }

        # Create output directories if they don't exist
        if not os.path.exists(self.raw_output_dir):
            os.makedirs(self.raw_output_dir)
            print(f"Created raw output directory: {self.raw_output_dir}")
        if not os.path.exists(self.cleaned_output_dir):
            os.makedirs(self.cleaned_output_dir)
            print(
                f"Created cleaned output directory: {self.cleaned_output_dir}")

    def _scrape_single_app(self, app_id, app_name, count):
        """
        Scrapes reviews for a single application from Google Play Store.

        Args:
            app_id (str): The package name (ID) of the app on Google Play.
            app_name (str): The user-friendly name of the app/bank.
            count (int): The target number of reviews to fetch.

        Returns:
            list: A list of dictionaries, where each dictionary represents a review.
        """
        print(f"\n--- Scraping reviews for {app_name} (App ID: {app_id}) ---")
        scraped_data = []
        continuation_token = None
        fetched_count = 0

        # Loop to ensure enough reviews are fetched, handling continuation tokens
        while fetched_count < count:
            try:
                # Fetch reviews using google-play-scraper
                # Setting lang='en' for English reviews, country='us' for general access
                result, token = reviews(
                    app_id,
                    lang='en',      # Language of the reviews
                    country='us',   # Country to fetch reviews from
                    sort=Sort.NEWEST,  # Sort by newest reviews
                    # Fetch in batches, max 200 per call
                    count=min(200, count - fetched_count),
                    continuation_token=continuation_token
                )

                # Check if result is None or empty before extending
                if result is None:
                    print(
                        f"No reviews returned for {app_name} in this batch. Breaking loop.")
                    break

                scraped_data.extend(result)
                fetched_count += len(result)
                continuation_token = token

                print(
                    f"Fetched {len(result)} new reviews. Total for {app_name}: {fetched_count}")

                # If no more reviews or target reached, break loop
                # Also break if no reviews were fetched in the last call but a token exists
                if not token or len(result) == 0:
                    print(
                        f"Reached end of available reviews or target met for {app_name}.")
                    break

                # Add a small delay to avoid hitting rate limits
                time.sleep(1)

            except Exception as e:
                print(f"Error scraping {app_name}: {e}")
                # Log the error but continue to next app if possible
                break  # Break from current app scraping loop on error

        processed_reviews = []
        for review in scraped_data:
            processed_reviews.append({
                'review': review.get('content'),
                'rating': review.get('score'),
                'date': review.get('at'),  # 'at' key holds the datetime object
                'bank': app_name,
                'source': 'Google Play Store'
            })
        print(
            f"Finished scraping {app_name}. Collected {len(processed_reviews)} reviews.")
        return processed_reviews

    def preprocess_reviews(self, df):
        """
        Preprocesses the raw DataFrame of scraped reviews.

        Args:
            df (pd.DataFrame): The DataFrame containing raw scraped reviews.

        Returns:
            pd.DataFrame: The cleaned and preprocessed DataFrame.
        """
        print("\n--- Starting data preprocessing ---")

        # 1. Remove duplicates based on 'review' and 'date'
        initial_rows = len(df)
        # Ensure 'bank' is also part of the duplicate check for uniqueness across banks
        df.drop_duplicates(subset=['review', 'date', 'bank'], inplace=True)
        print(
            f"Removed {initial_rows - len(df)} duplicate rows. Remaining: {len(df)}")

        # 2. Handle missing data
        # Drop rows where 'review' or 'rating' is missing, as these are critical
        df.dropna(subset=['review', 'rating', 'date', 'bank'], inplace=True)
        print(f"Removed rows with missing critical data. Remaining: {len(df)}")

        # 3. Normalize dates to YYYY-MM-DD format
        # Convert 'date' column to datetime objects first, then format
        try:
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            print("Dates normalized to YYYY-MM-DD format.")
        except Exception as e:
            print(f"Error normalizing dates: {e}")
            # If error, try to convert to date only, then to string
            df['date'] = pd.to_datetime(
                df['date'], errors='coerce').dt.date.astype(str)
            # Drop rows where date conversion failed
            df.dropna(subset=['date'], inplace=True)
            print("Attempted alternative date normalization and dropped invalid dates.")

        # Ensure 'rating' is integer type
        df['rating'] = pd.to_numeric(
            df['rating'], errors='coerce').fillna(0).astype(int)

        print("--- Preprocessing complete ---")
        print(f"Final dataset size after preprocessing: {len(df)} rows")
        return df

    def run(self):
        """
        Executes the full scraping and preprocessing pipeline, saving reviews
        for each bank to separate raw and cleaned CSV files.
        """
        print("\n--- Starting scraping and processing for all apps ---")
        for app_name, app_id in self.app_configs.items():
            # Scrape reviews for the current app
            app_reviews_list = self._scrape_single_app(
                app_id, app_name, self.target_reviews_per_bank)

            if not app_reviews_list:
                print(
                    f"No reviews collected for {app_name}. Skipping CSV creation.")
                continue

            # Create a DataFrame for the current app's reviews (raw)
            raw_df = pd.DataFrame(app_reviews_list)
            print(
                f"\nRaw DataFrame created for {app_name} with {len(raw_df)} entries.")

            # Sanitize app name for filename (replace spaces and special chars)
            filename_safe_app_name = app_name.replace(
                " ", "_").replace("/", "_").replace("\\", "_")

            # --- Save Raw Data ---
            raw_output_filename = f"{filename_safe_app_name}_mobile_banking_reviews_raw.csv"
            raw_output_filepath = os.path.join(
                self.raw_output_dir, raw_output_filename)
            try:
                raw_df.to_csv(raw_output_filepath,
                              index=False, encoding='utf-8')
                print(
                    f"Successfully saved raw reviews for {app_name} to: {raw_output_filepath}")
            except Exception as e:
                print(f"Error saving raw data for {app_name} to CSV: {e}")

            # Preprocess the current app's reviews
            # Use a copy to avoid modifying raw_df in place
            processed_df = self.preprocess_reviews(raw_df.copy())

            # --- Save Cleaned Data ---
            cleaned_output_filename = f"{filename_safe_app_name}_mobile_banking_reviews_cleaned.csv"
            cleaned_output_filepath = os.path.join(
                self.cleaned_output_dir, cleaned_output_filename)

            try:
                processed_df.to_csv(
                    cleaned_output_filepath, index=False, encoding='utf-8')
                print(
                    f"\nSuccessfully saved processed reviews for {app_name} to: {cleaned_output_filepath}")
                print(
                    f"Total reviews saved for {app_name}: {len(processed_df)}")

                # Basic data quality check as requested by KPIs
                missing_data_percentage = processed_df.isnull().sum().sum() / \
                    processed_df.size * 100
                print(
                    f"Missing data percentage in final dataset for {app_name}: {missing_data_percentage:.2f}%")
                if missing_data_percentage < 5:
                    print(
                        "Missing data percentage is within the acceptable range (<5%).")
                else:
                    print(
                        "WARNING: Missing data percentage is higher than recommended.")

            except Exception as e:
                print(f"Error saving cleaned data for {app_name} to CSV: {e}")

        print("\n--- All app scraping and processing complete ---")


if __name__ == "__main__":
    # Example usage:
    # Initialize the scraper to target 400 reviews per bank,
    # and specify raw and cleaned output directories
    scraper = GooglePlayReviewScraper(
        raw_output_dir="data/raw",
        cleaned_output_dir="data/cleaned",
        target_reviews_per_bank=400
    )
    # Run the scraping and preprocessing pipeline
    scraper.run()
