import asyncio
import asyncpg
import os
import logging
import pandas as pd
from datetime import datetime

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Configuration ---
DB_CONFIG = {
    "socket_env": "PG_SOCKET",
    "port_env": "POLY_PG_PORT",
    "name_env": "POLY_DB",
    "user_env": "POLY_DB_CLI",
    "pass_env": "POLY_DB_CLI_PASS"
}

# The specific oracle we are interested in
TARGET_ORACLE = "0x58e1745bedda7312c4cddb72618923da1b90efde"

# --- Helper Functions ---
async def get_db_pool(db_config: dict):
    """Creates an asyncpg database connection pool."""
    try:
        for key in db_config.values():
            if key not in os.environ:
                logger.error(f"Missing required environment variable: {key}")
                return None
        return await asyncpg.create_pool(
            host=os.environ.get(db_config['socket_env']),
            port=os.environ.get(db_config['port_env']),
            database=os.environ.get(db_config['name_env']),
            user=os.environ.get(db_config['user_env']),
            password=os.environ.get(db_config['pass_env']),
        )
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        raise

async def fetch_oracle_condition_ids(pool, oracle_address: str) -> set:
    """
    Fetches all ConditionPreparation event condition IDs for a specific oracle
    using a robust Python-side filtering method.
    """
    logger.info(f"Fetching all ConditionPreparation events to filter for oracle: {oracle_address}...")
    # --- CORRECTED QUERY: Fetch all events and filter in Python ---
    query = "SELECT topics FROM events_conditional_tokens WHERE event_name = 'ConditionPreparation'"
    
    condition_ids = set()
    async with pool.acquire() as conn:
        async with conn.transaction():
            async for record in conn.cursor(query):
                try:
                    # Extract oracle address from topics[2] (the last 40 hex characters)
                    record_oracle_address = f"0x{record['topics'][2][-40:]}".lower()
                    
                    # If it matches our target, store the condition ID from topics[1]
                    if record_oracle_address == oracle_address:
                        condition_ids.add(record['topics'][1])
                except (IndexError, TypeError):
                    continue
    
    logger.info(f"Found {len(condition_ids):,} unique condition IDs for the target oracle.")
    return condition_ids

def load_and_prepare_csv(csv_path: str) -> pd.DataFrame:
    """Loads the CSV and prepares the start_date column for sorting."""
    logger.info(f"Attempting to load CSV from: {csv_path}")
    if not os.path.exists(csv_path):
        logger.critical(f"CSV file not found at path: {csv_path}. Cannot proceed.")
        return None

    try:
        df = pd.read_csv(csv_path, low_memory=False)
        logger.info(f"Loaded {len(df):,} rows from CSV.")
    except Exception as e:
        logger.critical(f"Failed to read CSV file: {e}")
        return None

    if 'start_date' in df.columns:
        df['start_date_dt'] = pd.to_datetime(df['start_date'], errors='coerce', utc=True)
    else:
        logger.error("Required column 'start_date' not found in CSV.")
        return None
        
    return df

async def main():
    """Main execution function."""
    csv_env_var = 'POLY_CSV'
    csv_dir = os.environ.get(csv_env_var)
    if not csv_dir:
        logger.critical(f"Environment variable ${csv_env_var} is not set. Exiting.")
        return
        
    csv_path = os.path.join(csv_dir, 'gamma_markets.csv')
    markets_df = load_and_prepare_csv(csv_path)
    if markets_df is None:
        return

    db_pool = await get_db_pool(DB_CONFIG)
    if not db_pool:
        return
        
    try:
        oracle_ids = await fetch_oracle_condition_ids(db_pool, TARGET_ORACLE)
        if not oracle_ids:
            logger.warning("No condition IDs found for the target oracle. Exiting.")
            return

        matched_markets = markets_df[markets_df['condition_id'].isin(oracle_ids)].copy()
        logger.info(f"Found {len(matched_markets):,} total rows in CSV matching the oracle's condition IDs.")

        matched_markets.dropna(subset=['start_date_dt'], inplace=True)
        unique_matched_markets = matched_markets.drop_duplicates(subset=['condition_id'])
        logger.info(f"Analyzing {len(unique_matched_markets):,} unique markets.")

        sorted_markets = unique_matched_markets.sort_values(by='start_date_dt', ascending=False)
        top_10_recent = sorted_markets.head(10)

        print("\n" + "="*80)
        print(f"      10 Most Recent Questions Prepared by Oracle @ {TARGET_ORACLE[:8]}...")
        print("="*80)
        
        if top_10_recent.empty:
            print("No matching markets found.")
        else:
            for index, row in top_10_recent.iterrows():
                start_time_str = row['start_date_dt'].strftime('%Y-%m-%d %H:%M:%S %Z')
                print(f"\nStart Time: {start_time_str}")
                print(f"  Question: {row['question']}")
        
        print("="*80)

    finally:
        await db_pool.close()
        logger.info("Database connection closed.")

if __name__ == "__main__":
    asyncio.run(main())