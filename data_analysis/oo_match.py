import asyncio
import asyncpg
import os
import logging
import itertools
import statistics
from collections import defaultdict
from typing import Dict, List, Tuple

# web3 is only used for its utility functions, no node connection is needed.
from eth_abi import decode as abi_decode
from hexbytes import HexBytes

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Database Configuration ---
DB_CONFIG = {
    "socket_env": "PG_SOCKET",
    "port_env": "POLY_PG_PORT",
    "name_env": "POLY_DB",
    "user_env": "POLY_DB_CLI",
    "pass_env": "POLY_DB_CLI_PASS"
}

# --- Event Definitions for Ancillary Data Extraction ---
# We only need the 'data_types' to extract the ancillaryData field.
# From the previous script, we know it's the third argument (index 2) in the data part.
OOV2_DATA_TYPES = {
    "RequestPrice": ["bytes32", "uint256", "bytes", "address", "uint256", "uint256"],
    "ProposePrice": ["bytes32", "uint256", "bytes", "int256", "uint256", "address"],
    "DisputePrice": ["bytes32", "uint256", "bytes", "int256"],
    "Settle":       ["bytes32", "uint256", "bytes", "int256", "uint256"]
}
EVENT_NAMES = list(OOV2_DATA_TYPES.keys())

# --- Helper Functions ---
async def get_db_pool(db_config: dict, pool_size: int):
    """Creates an asyncpg database connection pool."""
    try:
        for key in db_config.values():
            if key not in os.environ:
                logger.error(f"Missing required environment variable: {key}")
                return None
        pool = await asyncpg.create_pool(
            host=os.environ.get(db_config['socket_env']),
            port=os.environ.get(db_config['port_env']),
            database=os.environ.get(db_config['name_env']),
            user=os.environ.get(db_config['user_env']),
            password=os.environ.get(db_config['pass_env']),
            min_size=pool_size,
            max_size=pool_size
        )
        return pool
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        raise e

def calculate_stats(diffs: List[float]) -> str:
    """Calculates and formats statistics for a list of time differences."""
    if not diffs:
        return "N/A"
    
    count = len(diffs)
    # Convert milliseconds to seconds for readability
    diffs_in_seconds = [d / 1000 for d in diffs]
    
    avg = statistics.mean(diffs_in_seconds)
    median = statistics.median(diffs_in_seconds)
    
    # Variance requires at least 2 data points
    if count > 1:
        variance = statistics.variance(diffs_in_seconds)
        return f"Avg: {avg:,.2f}s, Median: {median:,.2f}s, Variance: {variance:,.2f}s²"
    else:
        return f"Avg: {avg:,.2f}s, Median: {median:,.2f}s, Variance: N/A"

# --- Main Analysis Logic ---
async def analyze_oov2_events():
    """
    Connects to the DB, fetches all OOV2 event data, and performs a
    detailed analysis on counts and shared ancillary data.
    """
    logger.info("Initializing database connection...")
    db_pool = await get_db_pool(DB_CONFIG, pool_size=2)
    if not db_pool:
        return

    # This will be our main data structure:
    # { event_name: { ancillary_data: [timestamp1, timestamp2, ...], ... }, ... }
    events_by_ancillary_data = {name: defaultdict(list) for name in EVENT_NAMES}
    event_counts = {name: 0 for name in EVENT_NAMES}

    try:
        async with db_pool.acquire() as conn:
            logger.info(f"Fetching all records for events: {', '.join(EVENT_NAMES)}...")
            query = f"""
                SELECT event_name, data, timestamp_ms
                FROM "oov2"
                WHERE event_name IN ({', '.join(f"'{name}'" for name in EVENT_NAMES)})
            """
            # Use a cursor for potentially large result sets
            async with conn.transaction():
                cursor = await conn.cursor(query)
                while True:
                    records = await cursor.fetch(1000)
                    if not records:
                        break
                    
                    for record in records:
                        event_name = record['event_name']
                        data_hex = record['data']
                        timestamp = record['timestamp_ms']

                        if not data_hex or not timestamp:
                            continue

                        try:
                            # Decode the data part to extract ancillaryData
                            data_types = OOV2_DATA_TYPES[event_name]
                            decoded_data = abi_decode(data_types, HexBytes(data_hex))
                            ancillary_data = decoded_data[2] # It's the 3rd element (index 2)

                            events_by_ancillary_data[event_name][ancillary_data].append(timestamp)
                            event_counts[event_name] += 1
                        except Exception:
                            # Skip records that fail to decode
                            continue
        
        logger.info("Data fetching and processing complete. Generating report...")

        # --- 1. Print Event Counts ---
        print("\n" + "="*60)
        print("1. OOV2 Event Counts")
        print("="*60)
        total_count = 0
        for name, count in event_counts.items():
            print(f"- {name:<15}: {count:,}")
            total_count += count
        print("-" * 25)
        print(f"- {'Total':<15}: {total_count:,}")

        # --- 2. Analyze Shared Ancillary Data and Timestamps ---
        print("\n" + "="*60)
        print("2. Shared Ancillary Data Analysis")
        print("="*60)

        for r in range(2, len(EVENT_NAMES) + 1):
            print(f"\n--- Shared Between {r} Event Types ---")
            for combo in itertools.combinations(EVENT_NAMES, r):
                # Find the intersection of ancillary_data keys for the combination
                anc_data_sets = [set(events_by_ancillary_data[name].keys()) for name in combo]
                shared_keys = anc_data_sets[0].intersection(*anc_data_sets[1:])
                
                time_diffs = []
                if shared_keys:
                    for key in shared_keys:
                        all_timestamps = []
                        for event_name in combo:
                            all_timestamps.extend(events_by_ancillary_data[event_name][key])
                        
                        if len(all_timestamps) > 1:
                            time_diff = max(all_timestamps) - min(all_timestamps)
                            time_diffs.append(time_diff)
                
                combo_name = " & ".join(combo)
                stats_str = calculate_stats(time_diffs)
                print(f"\nCombination: {combo_name}")
                print(f"  - Shared Ancillary Data Count: {len(shared_keys):,}")
                print(f"  - Timestamp Difference Stats: {stats_str}")

    except Exception as e:
        logger.critical(f"An error occurred during analysis: {e}")
    finally:
        if db_pool:
            await db_pool.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    asyncio.run(analyze_oov2_events())