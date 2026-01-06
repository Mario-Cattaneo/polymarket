import os
import asyncio
import asyncpg
import logging
import json
import time
import aiohttp

# --- Environment Variable Setup ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger()

# --- Kalshi API Configuration ---
KALSHI_V1_REST_URL = "https://api.elections.kalshi.com/v1"

# --- Database Schema Definition ---
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS kalshi_events (
    ticker TEXT PRIMARY KEY,
    found_time_ms BIGINT NOT NULL,
    event JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kalshi_events_series ON kalshi_events((event->>'series_ticker'));
CREATE INDEX IF NOT EXISTS idx_kalshi_events_target_datetime ON kalshi_events((event->>'target_datetime'));
"""

# --- Database Functions ---

async def init_db(pool):
    """Initializes the database by creating tables if they don't exist."""
    async with pool.acquire() as connection:
        await connection.execute(CREATE_TABLES_SQL)
    logger.info("Database tables and indexes initialized successfully.")

async def upsert_event(pool, event_json):
    """Inserts or updates an event in the kalshi_events table."""
    ticker = event_json.get('ticker')
    if not ticker:
        logger.warning(f"Event JSON missing ticker field: {event_json}")
        return
    
    await pool.execute("""
        INSERT INTO kalshi_events (ticker, found_time_ms, event)
        VALUES ($1, $2, $3)
        ON CONFLICT (ticker) DO UPDATE SET
            found_time_ms = EXCLUDED.found_time_ms,
            event = EXCLUDED.event;
    """, ticker, int(time.time() * 1000), json.dumps(event_json))
    logger.info(f"Upserted event '{ticker}' into the database.")

# --- Main Logic ---

async def fetch_kxbtcd_events(session, pool):
    """Fetches all events from the KXBTCD series via HTTP and stores them in the DB."""
    logger.info("--- Starting KXBTCD Event Collection ---")
    
    series = 'KXBTCD'
    page_number = 1
    total_events = 0
    
    while True:
        url = (f"{KALSHI_V1_REST_URL}/events/?status=open,unopened"
               f"&series_tickers={series}&page_size=100&page_number={page_number}")
        
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    logger.warning(f"Failed to fetch page {page_number}. Status: {response.status}")
                    break
                
                data = await response.json()
                events = data.get('events', [])
                
                if not events:
                    logger.info(f"No more events found at page {page_number}.")
                    break
                
                logger.info(f"Processing page {page_number} with {len(events)} events...")
                
                for event in events:
                    await upsert_event(pool, event)
                    total_events += 1
                
                page_number += 1
                
        except aiohttp.ClientError as e:
            logger.error(f"Client error during event fetch for {series} page {page_number}: {e}")
            break
        except Exception as e:
            logger.error(f"Unexpected error during event fetch: {e}", exc_info=True)
            break
    
    logger.info(f"--- Event Collection Complete. Total events processed: {total_events} ---")
    return total_events

# --- Main Execution ---

async def main():
    """Main function to fetch and store KXBTCD events."""
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST]):
        logger.error("One or more required environment variables are not set.")
        return

    pool = None
    try:
        pool = await asyncpg.create_pool(
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            host=PG_HOST,
            port=PG_PORT
        )
        logger.info("Successfully connected to PostgreSQL database.")
        
        await init_db(pool)
        
        async with aiohttp.ClientSession() as session:
            total = await fetch_kxbtcd_events(session, pool)
        
        logger.info(f"Script completed successfully. Total events stored: {total}")

    except Exception as e:
        logger.critical(f"A critical error occurred in the main function: {e}", exc_info=True)
    finally:
        if pool:
            await pool.close()
            logger.info("Database connection pool closed.")

if __name__ == "__main__":
    asyncio.run(main())
