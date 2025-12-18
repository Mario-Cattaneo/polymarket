import os
import json
import asyncio
import asyncpg
import logging
from typing import List, Dict, Any

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger()

# --- CONFIGURATION ---
COLLECTOR_CONFIG_FILE = "../polygon_collection/collect_events_config.json"
OUTPUT_SCHEMA_FILE = "event_args.json"

# --- Database Credentials ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

def parse_signature_types(signature: str) -> List[str]:
    """Parses a signature to get a simple list of argument types."""
    try:
        params_str = signature.split('(')[1].split(')')[0]
        if not params_str: return []
        # This handles simple types. Note: It doesn't handle complex types like tuples.
        return [p.strip() for p in params_str.split(',')]
    except IndexError:
        return []

async def generate_schema():
    """
    Connects to the DB and efficiently builds a complete argument schema by sampling
    the database for event topic counts.
    """
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials are not set. Please set PG_SOCKET, POLY_PG_PORT, etc.")
        return

    try:
        with open(COLLECTOR_CONFIG_FILE, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        logger.error(f"Collector config file '{COLLECTOR_CONFIG_FILE}' not found.")
        return

    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Successfully connected to the database.")

    full_schema: Dict[str, Any] = {}
    logger.info("Generating event argument schema by sampling database...")

    for contract in config.get('contracts', []):
        table_name = contract['table_name']
        logger.info(f"Processing table: '{table_name}'...")
        full_schema[table_name] = {}

        query = f"""
            SELECT event_name, topics
            FROM (
                SELECT event_name, topics, ROW_NUMBER() OVER(PARTITION BY event_name) as rn
                FROM {table_name}
            ) as subquery
            WHERE rn = 1;
        """
        
        try:
            all_samples = await pool.fetch(query)
        except asyncpg.exceptions.UndefinedTableError:
            logger.error(f"Table '{table_name}' does not exist. Skipping.")
            continue

        sample_map = {row['event_name']: row for row in all_samples}

        for event in contract.get('events', []):
            signature = event['signature']
            event_name = signature.split('(')[0]
            
            sample_row = sample_map.get(event_name)
            if not sample_row:
                logger.warning(f"No sample found for '{event_name}' in table '{table_name}'. Skipping.")
                continue

            arg_types = parse_signature_types(signature)
            num_indexed_args = len(sample_row['topics']) - 1
            
            event_schema: Dict[str, Any] = {}
            indexed_arg_count = 0
            non_indexed_arg_count = 0

            for i, arg_type in enumerate(arg_types):
                # This logic now correctly handles ALL types, not just 'address'
                if indexed_arg_count < num_indexed_args:
                    # This argument is indexed
                    event_schema[str(i)] = {
                        "location": "topics",
                        "slot": indexed_arg_count + 1, # Slot is 1-based for topics array
                        "type": arg_type
                    }
                    indexed_arg_count += 1
                else:
                    # This argument is not indexed (in data)
                    event_schema[str(i)] = {
                        "location": "data",
                        "slot": non_indexed_arg_count, # Slot is 0-based for data segments
                        "type": arg_type
                    }
                    non_indexed_arg_count += 1
            
            if event_schema:
                full_schema[table_name][event_name] = event_schema

    await pool.close()

    try:
        with open(OUTPUT_SCHEMA_FILE, 'w') as f:
            json.dump(full_schema, f, indent=4)
        logger.info(f"Successfully generated and saved complete schema to '{OUTPUT_SCHEMA_FILE}'")
    except Exception as e:
        logger.error(f"Failed to write schema file: {e}")

if __name__ == "__main__":
    asyncio.run(generate_schema())