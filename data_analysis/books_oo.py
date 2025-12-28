import asyncio
import asyncpg
import os
import logging
import json
import re
import statistics
from collections import defaultdict, Counter
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
async def analyze_summary_and_matrix_final():
    logger.info("Initializing database connection...")
    db_pool = await get_db_pool(DB_CONFIG, pool_size=2)
    if not db_pool: return

    try:
        # --- 1. Load Data ---
        logger.info("Fetching data from markets_3, oov2, and events_managed_oracle...")
        async with db_pool.acquire() as conn1, db_pool.acquire() as conn2:
            market_task = conn1.fetch('SELECT message, found_time_ms, closed_time FROM markets_3 WHERE exhaustion_cycle > 0')
            event_names_str = ', '.join(f"'{name}'" for name in EVENT_ABIS.keys())
            oracle_query = f"""
                (SELECT event_name, data, timestamp_ms FROM oov2 WHERE event_name IN ({event_names_str}))
                UNION ALL
                (SELECT event_name, data, timestamp_ms FROM events_managed_oracle WHERE event_name IN ({event_names_str}))
            """
            oracle_task = conn2.fetch(oracle_query)
            market_records, oracle_records = await asyncio.gather(market_task, oracle_task)

        # --- 2. Process and Structure Data ---
        logger.info("Processing and structuring data for analysis...")
        markets_by_mid = {
            json.loads(rec.get('message')).get('id'): rec
            for rec in market_records if json.loads(rec.get('message')).get('id')
        }

        total_event_counts = Counter(rec['event_name'] for rec in oracle_records)
        events_by_mid = defaultdict(lambda: defaultdict(list))
        for record in oracle_records:
            event_name, data_hex = record.get('event_name'), record.get('data')
            if not all([event_name, data_hex]): continue
            try:
                ancillary_text = abi_decode(EVENT_ABIS[event_name], HexBytes(data_hex))[2].decode('utf-8', 'ignore')
                market_id = extract_market_id(ancillary_text)
                if market_id: events_by_mid[market_id][event_name].append(record)
            except Exception: continue

        # --- 3. Calculate Summary Statistics ---
        market_mids = set(markets_by_mid.keys())
        request_mids = {mid for mid, events in events_by_mid.items() if 'RequestPrice' in events}
        matched_orderbooks_with_request = market_mids.intersection(request_mids)
        
        propose_with_request, total_propose_markets = 0, 0
        dispute_with_request, total_dispute_markets = 0, 0
        settle_with_request, total_settle_markets = 0, 0

        for mid, events in events_by_mid.items():
            has_request = 'RequestPrice' in events
            if 'ProposePrice' in events:
                total_propose_markets += 1
                if has_request: propose_with_request += 1
            if 'DisputePrice' in events:
                total_dispute_markets += 1
                if has_request: dispute_with_request += 1
            if 'Settle' in events:
                total_settle_markets += 1
                if has_request: settle_with_request += 1

        # --- 4. Generate Initial Summary Report ---
        print("\n" + "="*60)
        print("1. Initial Data Summary & Match Rates")
        print("="*60)
        print("\n--- Total Counts ---")
        print(f"- Total Order Books (markets_3): {len(markets_by_mid):,}")
        for name, count in sorted(total_event_counts.items()):
            print(f"- Total {name} Events (from all oracles): {count:,}")

        print("\n--- Match Analysis ---")
        print(f"- Order Books matched with a RequestPrice: {len(matched_orderbooks_with_request):,} ({len(matched_orderbooks_with_request) / len(markets_by_mid):.2%})")
        print(f"- ProposePrice events linked to a RequestPrice: {propose_with_request:,} / {total_propose_markets:,} ({propose_with_request / total_propose_markets:.2%})")
        print(f"- DisputePrice events linked to a RequestPrice: {dispute_with_request:,} / {total_dispute_markets:,} ({dispute_with_request / total_dispute_markets:.2%})")
        print(f"- Settle events linked to a RequestPrice: {settle_with_request:,} / {total_settle_markets:,} ({settle_with_request / total_settle_markets:.2%})")

        # --- 5. Build Full Timelines for Matched Markets ---
        full_timelines = []
        for mid in matched_orderbooks_with_request:
            market_rec = markets_by_mid[mid]
            msg_json = json.loads(market_rec.get('message'))
            
            # FIX: Safely get the minimum timestamp or None
            propose_timestamps = [rec['timestamp_ms'] for rec in events_by_mid[mid].get('ProposePrice', [])]
            dispute_timestamps = [rec['timestamp_ms'] for rec in events_by_mid[mid].get('DisputePrice', [])]
            settle_timestamps = [rec['timestamp_ms'] for rec in events_by_mid[mid].get('Settle', [])]

            timeline = {
                'found_time_ms': parse_ms_timestamp(market_rec.get('found_time_ms')),
                'createdAt': parse_iso_timestamp(msg_json.get('createdAt')),
                'startDate': parse_iso_timestamp(msg_json.get('startDate')),
                'closed_time': parse_ms_timestamp(market_rec.get('closed_time')),
                'endDate': parse_iso_timestamp(msg_json.get('endDate')),
                'request_ts': parse_ms_timestamp(min(rec['timestamp_ms'] for rec in events_by_mid[mid]['RequestPrice'])),
                'propose_ts': parse_ms_timestamp(min(propose_timestamps) if propose_timestamps else None),
                'dispute_ts': parse_ms_timestamp(min(dispute_timestamps) if dispute_timestamps else None),
                'settle_ts': parse_ms_timestamp(min(settle_timestamps) if settle_timestamps else None)
            }
            full_timelines.append(timeline)

        # --- 6. Generate Matrix ---
        timestamp_keys = ['createdAt', 'startDate', 'found_time_ms', 'request_ts', 'propose_ts', 'dispute_ts', 'closed_time', 'settle_ts', 'endDate']
        matrix_data = defaultdict(str)

        for row_key in timestamp_keys:
            for col_key in timestamp_keys:
                if row_key == col_key:
                    matrix_data[(row_key, col_key)] = "---"
                    continue

                diffs_sec = [(tl[col_key] - tl[row_key]).total_seconds() for tl in full_timelines if tl.get(row_key) and tl.get(col_key)]
                if not diffs_sec:
                    matrix_data[(row_key, col_key)] = "No Data"
                    continue

                count = len(diffs_sec)
                avg_diff = statistics.mean(diffs_sec)
                avg_sign = 1 if avg_diff > 0 else -1 if avg_diff < 0 else 0
                sign_match_count = sum(1 for diff in diffs_sec if (d_sign := 1 if diff > 0 else -1 if diff < 0 else 0) == 0 or avg_sign == 0 or d_sign == avg_sign)
                sign_match_pct = (sign_match_count / count) * 100
                
                matrix_data[(row_key, col_key)] = f"{format_seconds(avg_diff)} | {sign_match_pct:.0f}% | {count}n"

        print("\n" + "="*120)
        print("2. Timeline and Causality Analysis Matrix (for matched markets)")
        print("="*120)
        print("Each cell contains: Avg(T_col - T_row) | Sign Match % | Data Points (n)")
        
        header = f"{'':<12}" + "".join([f"{key:<18}" for key in timestamp_keys])
        print(header)
        print("-" * len(header))

        for row_key in timestamp_keys:
            row_str = f"{row_key:<12}"
            for col_key in timestamp_keys:
                row_str += f"{matrix_data[(row_key, col_key)]:<18}"
            print(row_str)

    except Exception as e:
        logger.critical(f"An error occurred during analysis: {e}", exc_info=True)
    finally:
        if db_pool:
            await db_pool.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    asyncio.run(analyze_summary_and_matrix_final())