import os
import json
import asyncio
import asyncpg
import logging
from decimal import Decimal, getcontext

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger()

# Set precision for Decimal calculations
getcontext().prec = 10

async def get_db_connection(config):
    """Establishes an async connection to the PostgreSQL database."""
    db_config = config['general']['db']
    try:
        conn = await asyncpg.connect(
            host=os.getenv(db_config['socket_env']),
            port=os.getenv(db_config['port_env']),
            database=os.getenv(db_config['name_env']),
            user=os.getenv(db_config['user_env']),
            password=os.getenv(db_config['pass_env'])
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

async def calculate_event_distribution(config, conn):
    """
    Calculates and prints a compact probability distribution of events for each contract,
    correctly filtering by contract address.
    """
    for contract in config['contracts']:
        contract_id = contract['id']
        table_name = contract['table_name']
        contract_address = contract['address'].lower()

        try:
            # Get total events, scoped to the specific contract address
            total_events_query = f"SELECT COUNT(*) FROM {table_name} WHERE contract_address = $1;"
            total_events = await conn.fetchval(total_events_query, contract_address)

            if not total_events or total_events == 0:
                logger.warning(f"No events found for contract '{contract_id}'. Skipping.")
                continue

            # Get event counts, also scoped to the specific contract address
            event_counts_query = f"""
                SELECT event_name, COUNT(*) as event_count
                FROM {table_name}
                WHERE contract_address = $1
                GROUP BY event_name
                ORDER BY event_count DESC;
            """
            event_counts_records = await conn.fetch(event_counts_query, contract_address)

            if not event_counts_records:
                continue

            # --- COMPACT OUTPUT FORMATTING ---
            # 1. Find the longest event name to use for alignment padding
            max_name_len = max(len(r['event_name']) for r in event_counts_records)

            # 2. Print a compact, single-line header
            print(f"\n--- {contract_id} (Total Events: {total_events}) ---")

            # 3. Print each event's data in an aligned format
            for record in event_counts_records:
                event_name = record['event_name']
                count = record['event_count']
                probability = (Decimal(count) / Decimal(total_events))
                
                # Left-justify the event name for clean alignment
                aligned_name = event_name.ljust(max_name_len)
                
                # Format probability to 6 decimal places and include the count
                print(f"  {aligned_name} | P={probability:<.6f} ({count})")
            
        except asyncpg.exceptions.UndefinedTableError:
             logger.error(f"Table '{table_name}' does not exist. Skipping contract '{contract_id}'.")
        except Exception as e:
            logger.error(f"An error occurred while querying for contract '{contract_id}': {e}")

async def main():
    """
    Main async function to load config, connect to DB, and run calculations.
    """
    config_file = "../polygon_collection/collect_events_config.json"
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file '{config_file}' not found.")
        return
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from '{config_file}'.")
        return

    conn = await get_db_connection(config)
    if conn:
        try:
            await calculate_event_distribution(config, conn)
        finally:
            await conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    asyncio.run(main())