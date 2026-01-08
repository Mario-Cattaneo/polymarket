import requests
import json
import logging
import sqlite3
from tqdm import tqdm
import time
import os

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
API_URL = "https://gamma-api.polymarket.com/events"
POLY_CSV = os.getenv('POLY_CSV', os.path.expanduser('~/polymarket_data'))
DB_FILE = os.path.join(POLY_CSV, "polymarket_events.db")
TABLE_NAME = "events"
# Use an aggressive limit as requested to fetch faster
LIMIT = 500

def setup_database():
    """
    Connects to the SQLite database, drops the existing table to ensure a fresh start,
    and creates a new table to store raw event data.
    """
    # Ensure POLY_CSV directory exists
    os.makedirs(POLY_CSV, exist_ok=True)
    
    logger.info(f"Setting up database '{DB_FILE}'...")
    db_path = DB_FILE
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Drop the table if it exists to reset completely on every run
    logger.info(f"Dropping table '{TABLE_NAME}' if it exists...")
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    
    # Create a new table with a simple schema: an auto-incrementing ID and a text field for the JSON
    logger.info(f"Creating new table '{TABLE_NAME}'...")
    cursor.execute(f"""
    CREATE TABLE {TABLE_NAME} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data TEXT NOT NULL
    )
    """)
    
    conn.commit()
    logger.info("Database setup complete.")
    return conn

def fetch_and_store_events(conn):
    """
    Fetches all events from the API and stores the raw JSON for each event
    in the specified database table.
    """
    offset = 0
    total_events_stored = 0
    
    logger.info("🚀 Starting full API scrape to fetch and store all events...")
    cursor = conn.cursor()

    with tqdm(desc="Fetching Pages", unit=" pages") as pbar:
        while True:
            try:
                params = {"order": "id", "ascending": "false", "limit": LIMIT, "offset": offset}
                response = requests.get(API_URL, params=params, timeout=60)
                response.raise_for_status()
                events_data = response.json()
                
                if not events_data:
                    logger.info("\n✅ Scrape complete. Reached the end of the event list.")
                    break

                # Prepare data for bulk insertion
                # We store the raw JSON as a string
                events_to_insert = [(json.dumps(event),) for event in events_data]
                
                # Use executemany for efficient bulk insertion
                cursor.executemany(f"INSERT INTO {TABLE_NAME} (data) VALUES (?)", events_to_insert)
                conn.commit()
                
                total_events_stored += len(events_data)
                pbar.update(1)
                pbar.set_postfix({"Total Events Stored": f"{total_events_stored:,}"})
                
                offset += LIMIT
                time.sleep(0.2) # Be polite to the API
                
            except requests.exceptions.RequestException as e:
                logger.error(f"\n❌ Network Error at offset {offset}: {e}")
                break
            except Exception as e:
                logger.error(f"\n❌ An unexpected error occurred at offset {offset}: {e}")
                break
                
    logger.info(f"Successfully fetched and stored a total of {total_events_stored:,} events in '{DB_FILE}'.")

if __name__ == "__main__":
    connection = None
    try:
        # 1. Set up the database (creates/resets the table)
        connection = setup_database()
        # 2. Fetch data from API and store it in the database
        fetch_and_store_events(connection)
    except Exception as e:
        logger.critical(f"A critical error occurred: {e}")
    finally:
        if connection:
            connection.close()
            logger.info("Database connection closed.")