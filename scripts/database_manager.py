import pandas as pd
import psycopg2
import os
import glob
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT  # For creating databases


class ReviewDatabaseManager:
    """
    Manages the PostgreSQL database operations for storing cleaned and processed
    banking app review data, including sentiment and thematic analysis results.
    """

    def __init__(self, db_config, cleaned_data_dir="data/sentiment_analysis"):
        """
        Initializes the ReviewDatabaseManager.

        Args:
            db_config (dict): A dictionary containing PostgreSQL connection parameters
                              (host, port, user, password, database).
            cleaned_data_dir (str): Directory containing cleaned and analyzed review CSVs.
                                    These CSVs are expected to be the output of Task 2.
        """
        self.db_config = db_config
        self.cleaned_data_dir = cleaned_data_dir
        self.conn = None
        self.cursor = None

    def _connect(self, db_name=None, autocommit=False):
        """
        Internal method to establish a database connection.

        Args:
            db_name (str, optional): The name of the database to connect to.
                                     Defaults to the 'database' in db_config if None.
            autocommit (bool): If True, sets the connection to autocommit mode.
                               Useful for DDL operations like CREATE DATABASE.

        Returns:
            bool: True if connection is successful, False otherwise.
        """
        try:
            config = self.db_config.copy()
            if db_name:
                config['database'] = db_name
            else:
                # Ensure we have a database specified in config for connection
                if 'database' not in config:
                    print("Error: 'database' key missing in db_config.")
                    return False
                # Connect to the specified bank_reviews DB
                config['database'] = self.db_config['database']

            self.conn = psycopg2.connect(**config)
            if autocommit:
                self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor = self.conn.cursor()
            print(
                f"Successfully connected to PostgreSQL database '{config['database']}'.")
            return True
        except psycopg2.Error as e:
            print(
                f"Error connecting to database '{config.get('database', 'N/A')}': {e}")
            self.conn = None
            self.cursor = None
            return False

    def _close(self):
        """Internal method to close database connection and cursor."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        # print("Database connection closed.") # Suppress for cleaner output during pipeline

    def create_database_if_not_exists(self, db_name):
        """
        Creates a new PostgreSQL database if it does not already exist.
        Requires connecting to a default database (e.g., 'postgres') first.

        Args:
            db_name (str): The name of the database to create.

        Returns:
            bool: True if the database exists or was created, False otherwise.
        """
        print(f"Attempting to ensure database '{db_name}' exists...")
        # Connect to a default database like 'postgres' to create the new one
        if not self._connect(db_name='postgres', autocommit=True):
            print("Could not connect to default database ('postgres') to create new database. Please check credentials or if 'postgres' database exists.")
            return False

        try:
            # Check if database exists
            self.cursor.execute(
                sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [db_name])
            if not self.cursor.fetchone():
                self.cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
                print(f"Database '{db_name}' created successfully.")
            else:
                print(f"Database '{db_name}' already exists.")
            return True
        except psycopg2.Error as e:
            print(f"Error creating database '{db_name}': {e}")
            return False
        finally:
            self._close()  # Always close the connection to 'postgres'

    def setup_tables(self):
        """
        Connects to the specified target database and creates the 'banks' and 'reviews' tables
        if they do not already exist.
        """
        print(
            f"Setting up tables in database '{self.db_config['database']}'...")
        if not self._connect(db_name=self.db_config['database']):
            print("Failed to connect to target database to set up tables.")
            return False

        try:
            # Set to standard isolation level for DDL/DML operations
            self.conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            # Re-create cursor if connection was in autocommit mode
            self.cursor = self.conn.cursor()

            # SQL to create the 'banks' table
            create_banks_table_sql = """
            CREATE TABLE IF NOT EXISTS banks (
                bank_id SERIAL PRIMARY KEY,
                bank_name VARCHAR(255) UNIQUE NOT NULL
            );
            """

            # SQL to create the 'reviews' table
            # review_id is BIGINT to accommodate Python's hash() output
            # identified_themes stored as TEXT (e.g., "UI; Features; Performance")
            # review_date avoids conflict with SQL keyword 'DATE'
            create_reviews_table_sql = """
            CREATE TABLE IF NOT EXISTS reviews (
                review_id BIGINT PRIMARY KEY,
                review_text TEXT,
                sentiment_label VARCHAR(50),
                sentiment_score NUMERIC(5, 4),
                identified_themes TEXT,
                rating INTEGER,
                review_date DATE,
                source VARCHAR(100),
                bank_id INTEGER NOT NULL,
                FOREIGN KEY (bank_id) REFERENCES banks (bank_id) ON DELETE CASCADE
            );
            """
            self.cursor.execute(create_banks_table_sql)
            self.cursor.execute(create_reviews_table_sql)
            self.conn.commit()
            print("Tables 'banks' and 'reviews' created or already exist.")
            return True
        except psycopg2.Error as e:
            print(f"Error setting up tables: {e}")
            self.conn.rollback()  # Rollback changes in case of error
            return False
        finally:
            self._close()

    def _get_bank_id(self, bank_name):
        """
        Internal method to get the bank_id for a given bank name,
        inserting the bank into the 'banks' table if it doesn't exist.
        Assumes an active connection.

        Args:
            bank_name (str): The name of the bank.

        Returns:
            int or None: The bank_id if successful, None otherwise.
        """
        try:
            # Check if bank exists
            self.cursor.execute(
                "SELECT bank_id FROM banks WHERE bank_name = %s", (bank_name,))
            result = self.cursor.fetchone()
            if result:
                return result[0]
            else:
                # If not, insert the bank and return its new ID
                self.cursor.execute(
                    "INSERT INTO banks (bank_name) VALUES (%s) RETURNING bank_id",
                    (bank_name,)
                )
                bank_id = self.cursor.fetchone()[0]
                self.conn.commit()
                # print(f"Inserted new bank: '{bank_name}' with ID: {bank_id}") # Suppress for cleaner output
                return bank_id
        except psycopg2.Error as e:
            print(f"Error getting/inserting bank ID for '{bank_name}': {e}")
            self.conn.rollback()  # Rollback on error
            return None

    def insert_review_data(self, df):
        """
        Inserts cleaned and processed review data from a pandas DataFrame into the 'reviews' table.
        It also ensures that associated bank names are present in the 'banks' table.

        Args:
            df (pd.DataFrame): DataFrame containing review data. Expected columns:
                              'review', 'sentiment_label', 'sentiment_score',
                              'rating', 'date', 'bank', 'source'.
        """
        print(
            f"Connecting to database '{self.db_config['database']}' to insert review data...")
        if not self._connect(db_name=self.db_config['database']):
            print("Failed to connect to database for data insertion. Skipping insertion.")
            return

        try:
            self.conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            self.cursor = self.conn.cursor()

            inserted_count = 0

            # Map bank names to their IDs
            bank_id_map = {}
            unique_banks = df['bank'].unique()
            for bank_name in unique_banks:
                bank_id = self._get_bank_id(bank_name)
                if bank_id is not None:
                    bank_id_map[bank_name] = bank_id
                else:
                    print(
                        f"Warning: Could not get/insert bank_id for '{bank_name}'. Reviews for this bank will be skipped.")

            reviews_to_insert = []
            for index, row in df.iterrows():
                bank_name = row['bank']
                bank_id = bank_id_map.get(bank_name)

                if bank_id is None:
                    # This review's bank could not be processed, skip it
                    continue

                # Prepare data, handling potential NaN/None values for database insertion
                # Generate review_id from review text hash since it's not in the CSV
                review_text = row['review'] if pd.notna(row['review']) else ""
                review_id = hash(review_text) if review_text else None

                sentiment_label = row['sentiment_label'] if pd.notna(
                    row['sentiment_label']) else None
                sentiment_score = float(row['sentiment_score']) if pd.notna(
                    row['sentiment_score']) else None
                # Set identified_themes to None since it's not in the CSV
                identified_themes = None
                rating = int(row['rating']) if pd.notna(
                    row['rating']) else None

                # Convert date to standard Python date object, handling errors
                review_date_str = str(row['date']) if pd.notna(
                    row['date']) else None
                review_date = None
                if review_date_str:
                    try:
                        review_date = pd.to_datetime(review_date_str).date()
                    except (ValueError, TypeError):
                        print(
                            f"Warning: Could not parse date '{review_date_str}' for review_id {review_id}. Setting to NULL.")

                source = row['source'] if pd.notna(row['source']) else None

                reviews_to_insert.append((
                    review_id,
                    review_text,
                    sentiment_label,
                    sentiment_score,
                    identified_themes,
                    rating,
                    review_date,
                    source,
                    bank_id
                ))

            if not reviews_to_insert:
                print(f"No valid reviews to insert from current DataFrame.")
                return

            # Use executemany for efficient bulk insertion
            # ON CONFLICT (review_id) DO NOTHING will prevent errors for duplicate review_ids
            insert_reviews_sql = """
            INSERT INTO reviews (review_id, review_text, sentiment_label, sentiment_score, identified_themes, rating, review_date, source, bank_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (review_id) DO NOTHING;
            """
            self.cursor.executemany(insert_reviews_sql, reviews_to_insert)
            self.conn.commit()

            # rowcount for executemany can be tricky with ON CONFLICT.
            # It generally returns the number of rows inserted/updated, not total attempted.
            # For simplicity, we just confirm the operation committed.
            print(f"Attempted to insert reviews. Transaction committed.")

        except psycopg2.Error as e:
            print(
                f"Error inserting review data: {e}. Rolling back transaction.")
            self.conn.rollback()
        except Exception as e:
            print(f"An unexpected error occurred during insertion: {e}")
            self.conn.rollback()
        finally:
            self._close()  # Close connection after each insertion batch

    def run_ingestion_pipeline(self):
        """
        Orchestrates the entire data ingestion pipeline:
        1. Ensures the target database exists.
        2. Sets up necessary tables ('banks', 'reviews').
        3. Loads analyzed review data from CSVs (output of Task 2).
        4. Inserts data into the database.
        5. Performs a KPI check for populated entries.
        """
        print("\n--- Starting Data Ingestion to PostgreSQL ---")

        # 1. Create the database if it doesn't exist
        db_to_create = self.db_config['database']
        if not self.create_database_if_not_exists(db_to_create):
            print(
                f"Aborting ingestion as database '{db_to_create}' could not be ensured.")
            return

        # 2. Setup tables within the database
        if not self.setup_tables():
            print("Aborting ingestion as failed to set up tables.")
            return
        # 3. Load all processed review data from CSVs
        search_pattern = os.path.join(
            self.cleaned_data_dir, "*_reviews_with_sentiment.csv")
        print(
            f"DEBUG: Searching for files matching pattern: '{search_pattern}'")

        all_files = glob.glob(search_pattern)

        if not all_files:
            print(
                f"No analyzed review CSV files found in '{self.cleaned_data_dir}'. Please ensure Task 2 was run correctly.")
            return

        total_files_processed = 0
        for filepath in all_files:
            total_files_processed += 1
            print(
                f"\nProcessing file {total_files_processed}/{len(all_files)}: {os.path.basename(filepath)}")
            try:
                df = pd.read_csv(filepath)
                if df.empty:
                    print(
                        f"Warning: {os.path.basename(filepath)} is empty. Skipping.")
                    continue
                self.insert_review_data(df)
            except Exception as e:
                print(
                    f"Error loading or inserting data from '{os.path.basename(filepath)}': {e}")

        # 4. Verify total entries (KPI check)
        print("\n--- Performing KPI Check ---")
        # Re-establish connection for query
        if self._connect(db_name=self.db_config['database']):
            try:
                self.cursor.execute("SELECT COUNT(*) FROM reviews;")
                total_reviews_in_db = self.cursor.fetchone()[0]
                print(
                    f"Total entries populated in 'reviews' table: {total_reviews_in_db}")
                if total_reviews_in_db >= 1000:
                    print("KPI Met: Database populated with >= 1,000 entries.")
                else:
                    print(
                        f"KPI Not Met: Total entries ({total_reviews_in_db}) is less than 1,000.")

                self.cursor.execute("SELECT COUNT(*) FROM banks;")
                total_banks_in_db = self.cursor.fetchone()[0]
                print(
                    f"Total entries populated in 'banks' table: {total_banks_in_db}")

            except psycopg2.Error as e:
                print(f"Error verifying data in database: {e}")
            finally:
                self._close()
        else:
            print("Could not connect to database to perform KPI check.")

        print("\n--- Data Ingestion Pipeline Complete ---")


# --- Main Execution Block ---
if __name__ == "__main__":
    # !!! IMPORTANT: DATABASE CONFIGURATION !!!
    # Replace these with your PostgreSQL database credentials.
    # For a real-world scenario, use environment variables (e.g., os.environ.get('DB_PASSWORD')).
    # For this exercise, direct assignment is used for simplicity,
    # but be aware of security implications if sharing this code with actual credentials.
    db_config = {
        'host': 'localhost',  # Or your PostgreSQL server IP/hostname
        'port': '5432',       # Default PostgreSQL port
        'user': 'postgres',   # Your PostgreSQL username (e.g., 'postgres')
        'password': 'postgres',  # Replace with your actual password
        'database': 'bank_reviews'  # The name of the database to create/use
    }

    # Instantiate the manager and run the ingestion pipeline
    # Ensure you have run Task 1 (GooglePlayReviewScraper) and Task 2 (ReviewAnalyzer)
    # to generate cleaned and analyzed data in 'data/thematic_analysis' first.
    db_manager = ReviewDatabaseManager(db_config=db_config)
    db_manager.run_ingestion_pipeline()

    # --- Instructions for SQL Dump (Manual Step for User) ---
    print("\n--- SQL Schema Dump for GitHub ---")
    print("To fulfill the 'SQL dump committed to GitHub' KPI, you will typically use the `pg_dump` utility from your terminal.")
    print("Ensure PostgreSQL's `bin` directory (containing `pg_dump`) is in your system's PATH.")
    print(f"Database Name: {db_config['database']}")
    print(f"Database User: {db_config['user']}")
    print(f"Database Host: {db_config['host']}")
    print(f"Database Port: {db_config['port']}")

    print("\nRecommended commands (run these in your terminal, replacing `your_password` if prompted):")
    print(f"1. To dump only the database schema (recommended for Git):")
    print(
        f"   PGPASSWORD={db_config['password']} pg_dump -h {db_config['host']} -p {db_config['port']} -U {db_config['user']} -s {db_config['database']} > bank_reviews_schema.sql")
    print(f"2. To dump data from specific tables (be cautious with very large datasets):")
    print(f"   PGPASSWORD={db_config['password']} pg_dump -h {db_config['host']} -p {db_config['port']} -U {db_config['user']} -a -t banks -t reviews {db_config['database']} > bank_reviews_data.sql")
    print("\nAfter generating 'bank_reviews_schema.sql' (and optionally 'bank_reviews_data.sql'), commit these files to your 'task-3' branch on GitHub.")
