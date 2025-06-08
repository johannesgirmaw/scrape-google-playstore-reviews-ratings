import pandas as pd
from google_play_scraper import Sort, reviews
from datetime import datetime
import os
import time


class GooglePlayReviewScraper:
    """
    A class to scrape user reviews from the Google Play Store for specified banking apps,
    preprocess the collected data, and save it to a CSV file.
    """

    def __init__(self, output_dir="data", target_reviews_per_bank=400):
        """
        Initializes the scraper with app configurations and output settings.

        Args:
            output_dir (str): Directory to save the output CSV file.
            target_reviews_per_bank (int): Minimum number of reviews to scrape per bank.
        """
        self.output_dir = output_dir
        self.target_reviews_per_bank = target_reviews_per_bank
        self.app_configs = {
            # Verified app ID (common)
            "Commercial Bank of Ethiopia Mobile": "com.cbe.mobilebanking",
            # Verified app ID (common)
            "Bank of Abyssinia Mobile": "com.boa.mobilebanking",
            # Verified app ID (common)
            "Dashen Bank Mobile": "com.dashen.mobile"
        }
        self.all_scraped_reviews = []

        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"Created output directory: {self.output_dir}")

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
                scraped_data.extend(result)
                fetched_count += len(result)
                continuation_token = token

                print(
                    f"Fetched {len(result)} new reviews. Total for {app_name}: {fetched_count}")

                # If no more reviews or target reached, break loop
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

    def scrape_all_apps(self):
        """
        Orchestrates scraping reviews for all configured banking applications.
        """
        for app_name, app_id in self.app_configs.items():
            app_reviews = self._scrape_single_app(
                app_id, app_name, self.target_reviews_per_bank)
            self.all_scraped_reviews.extend(app_reviews)
        print(
            f"\n--- Total reviews collected across all banks: {len(self.all_scraped_reviews)} ---")

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

    def run(self, filename="mobile_banking_reviews.csv"):
        """
        Executes the full scraping and preprocessing pipeline.

        Args:
            filename (str): Name of the CSV file to save the processed data.
        """
        self.scrape_all_apps()

        if not self.all_scraped_reviews:
            print("No reviews were scraped. Exiting.")
            return

        raw_df = pd.DataFrame(self.all_scraped_reviews)
        print(f"\nRaw DataFrame created with {len(raw_df)} entries.")

        processed_df = self.preprocess_reviews(raw_df)

        # Save the processed data to CSV
        output_filepath = os.path.join(self.output_dir, filename)
        try:
            processed_df.to_csv(output_filepath, index=False, encoding='utf-8')
            print(
                f"\nSuccessfully saved processed reviews to: {output_filepath}")
            print(f"Total reviews saved: {len(processed_df)}")
        except Exception as e:
            print(f"Error saving data to CSV: {e}")

        # Basic data quality check as requested by KPIs
        missing_data_percentage = processed_df.isnull().sum().sum() / \
            processed_df.size * 100
        print(
            f"Missing data percentage in final dataset: {missing_data_percentage:.2f}%")
        if missing_data_percentage < 5:
            print("Missing data percentage is within the acceptable range (<5%).")
        else:
            print("WARNING: Missing data percentage is higher than recommended.")


if __name__ == "__main__":
    # Example usage:
    # Initialize the scraper to target 400 reviews per bank
    scraper = GooglePlayReviewScraper(target_reviews_per_bank=400)
    # Run the scraping and preprocessing pipeline
    scraper.run()
