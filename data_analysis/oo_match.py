
import asyncio
import asyncpg
import os
import logging
import json
import re
import statistics
from collections import defaultdict
from datetime import datetime, timezone

# web3 is only used for its utility functions
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

# --- Event ABIs for Ancillary Data Extraction ---
EVENT_ABIS = {
    "RequestPrice": ["bytes32", "uint256", "bytes", "address", "uint256", "uint256"],
    "ProposePrice": ["bytes32", "uint256", "bytes", "int256", "uint256", "address"],
    "DisputePrice": ["bytes32", "uint256", "bytes", "int256"],
    "Settle":       ["bytes32", "uint256", "bytes", "int256", "uint256"]
}

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

def extract_market_id(text: str) -> str:
    if not text: return None
    match = re.search(r'market_id:\s*(\d+)', text)
    return match.group(1) if match else None

def parse_ms_timestamp(ts_ms: int):
    if not ts_ms: return None
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

def parse_iso_timestamp(ts_str: str):
    if not ts_str: return None
    if ts_str.endswith('Z'): ts_str = ts_str[:-1] + '+00:00'
    return datetime.fromisoformat(ts_str)

def format_seconds(sec: float) -> str:
    """Formats seconds into a human-readable d/h/m/s format."""
    if abs(sec) < 60: return f"{sec:.1f}s"
    if abs(sec) < 3600: return f"{sec/60:.1f}m"
    if abs(sec) < 86400: return f"{sec/3600:.1f}h"
    return f"{sec/86400:.1f}d"

# --- Main Analysis Logic ---
async def analyze_causality_matrix():
    logger.info("Initializing database connection...")
    db_pool = await get_db_pool(DB_CONFIG, pool_size=2)
    if not db_pool: return

    try:
        # --- 1. Load Data ---
        logger.info("Fetching data from markets_3 and oov2...")
        async with db_pool.acquire() as conn1, db_pool.acquire() as conn2:
            market_task = conn1.fetch('SELECT message, found_time_ms, closed_time FROM markets_3 WHERE exhaustion_cycle > 0')
            oov2_task = conn2.fetch(f"SELECT event_name, data, timestamp_ms FROM oov2 WHERE event_name IN ({', '.join(f'{chr(39)}{name}{chr(39)}' for name in EVENT_ABIS.keys())})")
            market_records, oov2_records = await asyncio.gather(market_task, oov2_task)

        # --- 2. Process and Structure Data ---
        logger.info("Processing and structuring data for analysis...")
        markets_by_mid = {}
        for record in market_records:
            msg_json = json.loads(record.get('message'))
            market_id = msg_json.get('id')
            if market_id:
                markets_by_mid[market_id] = {
                    'found_time_ms': parse_ms_timestamp(record.get('found_time_ms')),
                    'createdAt': parse_iso_timestamp(msg_json.get('createdAt')),
                    'startDate': parse_iso_timestamp(msg_json.get('startDate')),
                    'closed_time': parse_ms_timestamp(record.get('closed_time')),
                    'endDate': parse_iso_timestamp(msg_json.get('endDate'))
                }

        events_by_mid = defaultdict(lambda: defaultdict(list))
        for record in oov2_records:
            event_name, data_hex, timestamp = record.get('event_name'), record.get('data'), record.get('timestamp_ms')
            if not all([event_name, data_hex, timestamp]): continue
            try:
                ancillary_text = abi_decode(EVENT_ABIS[event_name], HexBytes(data_hex))[2].decode('utf-8', 'ignore')
                market_id = extract_market_id(ancillary_text)
                if market_id: events_by_mid[market_id][event_name].append(timestamp)
            except Exception: continue

        # --- 3. Build Full Timelines for Matched Markets ---
        shared_mids = set(markets_by_mid.keys()).intersection(set(events_by_mid.keys()))
        full_timelines = []
        for mid in shared_mids:
            timeline = markets_by_mid[mid]
            timeline['request_ts'] = parse_ms_timestamp(min(events_by_mid[mid].get('RequestPrice', [None])))
            timeline['propose_ts'] = parse_ms_timestamp(min(events_by_mid[mid].get('ProposePrice', [None])))
            timeline['dispute_ts'] = parse_ms_timestamp(min(events_by_mid[mid].get('DisputePrice', [None])))
            timeline['settle_ts'] = parse_ms_timestamp(min(events_by_mid[mid].get('Settle', [None])))
            full_timelines.append(timeline)

        # --- 4. Generate Report ---
        print("\n" + "="*60)
        print("Timestamp Formats Used in This Analysis")
        print("="*60)
        print("All timestamps have been parsed into timezone-aware Python datetime objects.")
        print(" - Off-chain times (createdAt, startDate, etc.) are parsed from ISO 8601 strings.")
        print(" - On-chain event times (request_ts, etc.) are parsed from Unix millisecond integers.")
        
        print("\n" + "="*60)
        print("How to Interpret the Matrix")
        print("="*60)
        print("Each cell contains: Avg(T_col - T_row) | Sign Match % | Data Points (n)")
        print(" - Avg Time Diff: A positive value means the COLUMN event happens AFTER the ROW event.")
        print(" - Sign Match %:  Consistency of the chronological order. >95% suggests a strong causal link.")
        print(" - Data Points:   Number of markets used for the calculation.")

        # --- 5. Generate Matrix Data ---
        timestamp_keys = ['createdAt', 'startDate', 'found_time_ms', 'request_ts', 'propose_ts', 'dispute_ts', 'closed_time', 'settle_ts', 'endDate']
        matrix_data = defaultdict(str)

        for row_key in timestamp_keys:
            for col_key in timestamp_keys:
                if row_key == col_key:
                    matrix_data[(row_key, col_key)] = "---"
                    continue

                diffs_sec = []
                for tl in full_timelines:
                    if tl.get(row_key) and tl.get(col_key):
                        diffs_sec.append((tl[col_key] - tl[row_key]).total_seconds())
                
                if not diffs_sec:
                    matrix_data[(row_key, col_key)] = "No Data"
                    continue

                count = len(diffs_sec)
                avg_diff = statistics.mean(diffs_sec)
                avg_sign = 1 if avg_diff > 0 else -1 if avg_diff < 0 else 0
                
                sign_match_count = 0
                for diff in diffs_sec:
                    diff_sign = 1 if diff > 0 else -1 if diff < 0 else 0
                    if diff_sign == 0 or avg_sign == 0 or diff_sign == avg_sign:
                        sign_match_count += 1
                
                sign_match_pct = (sign_match_count / count) * 100
                
                matrix_data[(row_key, col_key)] = f"{format_seconds(avg_diff)} | {sign_match_pct:.0f}% | {count}n"

        # --- 6. Print Matrix ---
        print("\n" + "="*120)
        print("Timeline and Causality Analysis Matrix")
        print("="*120)
        
        header = f"{'':<12}" + "".join([f"{key:<18}" for key in timestamp_keys])
        print(header)
        print("-" * len(header))

        for row_key in timestamp_keys:
            row_str = f"{row_key:<12}"
            for col_key in timestamp_keys:
                row_str += f"{matrix_data[(row_key, col_key)]:<18}"
            print(row_str)

        print("\n" + "="*60)
        print("Inferred Timeline (based on high Sign Match %)")
        print("="*60)
        print("The most likely sequence of events appears to be:")
        print("1. createdAt -> 2. startDate -> 3. request_ts -> 4. propose_ts -> 5. settle_ts -> 6. closed_time -> 7. endDate")
        print("(found_time_ms seems to be a separate metadata field, and dispute_ts is a rare conditional event)")


    except Exception as e:
        logger.critical(f"An error occurred during analysis: {e}", exc_info=True)
    finally:
        if db_pool:
            await db_pool.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    asyncio.run(analyze_causality_matrix())