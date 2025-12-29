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
# The timestamp is expected to be the second element (index 1)
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
    try:
        return datetime.fromisoformat(ts_str)
    except ValueError:
        return None

def format_seconds(sec: float) -> str:
    """Formats seconds into a human-readable d/h/m/s format."""
    if abs(sec) < 60: return f"{sec:.1f}s"
    if abs(sec) < 3600: return f"{sec/60:.1f}m"
    if abs(sec) < 86400: return f"{sec/3600:.1f}h"
    return f"{sec/86400:.1f}d"

def get_avg_datetime(dates: list):
    """Calculates the average of a list of datetime objects."""
    if not dates: return None
    timestamps = [d.timestamp() for d in dates]
    avg_timestamp = statistics.mean(timestamps)
    return datetime.fromtimestamp(avg_timestamp, tz=timezone.utc)

# --- Main Analysis Logic ---
async def analyze_summary_and_matrix_final():
    logger.info("Initializing database connection...")
    db_pool = await get_db_pool(DB_CONFIG, pool_size=2)
    if not db_pool: return

    try:
        # --- 1. Load Data ---
        logger.info("Fetching data from markets_4, events, oov2, and events_managed_oracle...")
        async with db_pool.acquire() as conn1, db_pool.acquire() as conn2:
            market_query = """
                SELECT m.market_id, m.found_time_ms, m.closed_time_ms, e.message
                FROM markets_4 AS m JOIN events AS e ON m.event_id = e.event_id
            """
            market_task = conn1.fetch(market_query)
            
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
        markets_by_mid = {rec.get('market_id'): rec for rec in market_records if rec.get('market_id')}

        total_event_counts = Counter(rec['event_name'] for rec in oracle_records)
        events_by_mid = defaultdict(lambda: defaultdict(list))
        timestamp_diffs = defaultdict(list)

        for record in oracle_records:
            event_name, data_hex = record.get('event_name'), record.get('data')
            if not all([event_name, data_hex]): continue
            try:
                decoded_data = abi_decode(EVENT_ABIS[event_name], HexBytes(data_hex))
                ancillary_text = decoded_data[2].decode('utf-8', 'ignore')
                market_id = extract_market_id(ancillary_text)
                
                if market_id:
                    events_by_mid[market_id][event_name].append(record)

                # Timestamp analysis
                row_ts_ms = record.get('timestamp_ms')
                arg_ts_sec = decoded_data[1] # Timestamp is the second argument
                if row_ts_ms and arg_ts_sec:
                    diff_ms = row_ts_ms - (arg_ts_sec * 1000)
                    timestamp_diffs[event_name].append(diff_ms / 1000) # Store diff in seconds

            except Exception:
                continue

        # --- 3. Calculate Summary Statistics ---
        market_mids = set(markets_by_mid.keys())
        request_mids = {mid for mid, events in events_by_mid.items() if 'RequestPrice' in events}
        matched_orderbooks_with_request = market_mids.intersection(request_mids)
        unmatched_orderbooks = market_mids - matched_orderbooks_with_request
        
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

        # --- 4. Generate Reports ---
        print("\n" + "="*60)
        print("1. Initial Data Summary & Match Rates")
        print("="*60)
        print("\n--- Total Counts ---")
        print(f"- Total Order Books (markets_4, all exhaustion cycles): {len(markets_by_mid):,}")
        for name, count in sorted(total_event_counts.items()):
            print(f"- Total {name} Events (from all oracles): {count:,}")

        print("\n--- Match Analysis ---")
        match_pct = (len(matched_orderbooks_with_request) / len(markets_by_mid) * 100) if markets_by_mid else 0
        propose_pct = (propose_with_request / total_propose_markets * 100) if total_propose_markets > 0 else 0
        dispute_pct = (dispute_with_request / total_dispute_markets * 100) if total_dispute_markets > 0 else 0
        settle_pct = (settle_with_request / total_settle_markets * 100) if total_settle_markets > 0 else 0

        print(f"- Order Books matched with a RequestPrice: {len(matched_orderbooks_with_request):,} ({match_pct:.2f}%)")
        print(f"- Order Books unmatched: {len(unmatched_orderbooks):,}")
        print(f"- ProposePrice events linked to a RequestPrice: {propose_with_request:,} / {total_propose_markets:,} ({propose_pct:.2f}%)")
        print(f"- DisputePrice events linked to a RequestPrice: {dispute_with_request:,} / {total_dispute_markets:,} ({dispute_pct:.2f}%)")
        print(f"- Settle events linked to a RequestPrice: {settle_with_request:,} / {total_settle_markets:,} ({settle_pct:.2f}%)")

        # --- 4a. Unmatched Order Book Analysis ---
        print("\n" + "="*60)
        print("2. Unmatched Order Book Analysis")
        print("="*60)
        unmatched_created = [parse_iso_timestamp(json.loads(markets_by_mid[mid]['message']).get('createdAt')) for mid in unmatched_orderbooks]
        unmatched_started = [parse_iso_timestamp(json.loads(markets_by_mid[mid]['message']).get('startDate')) for mid in unmatched_orderbooks]
        unmatched_created = [d for d in unmatched_created if d]
        unmatched_started = [d for d in unmatched_started if d]

        if unmatched_created:
            print(f"\n--- For {len(unmatched_created):,} Unmatched Markets with Creation Date ---")
            print(f"- Avg createdAt: {get_avg_datetime(unmatched_created).strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"- Max createdAt: {max(unmatched_created).strftime('%Y-%m-%d %H:%M:%S')}")
        if unmatched_started:
            print(f"\n--- For {len(unmatched_started):,} Unmatched Markets with Start Date ---")
            print(f"- Avg startDate: {get_avg_datetime(unmatched_started).strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"- Max startDate: {max(unmatched_started).strftime('%Y-%m-%d %H:%M:%S')}")

        # --- 4b. Event Timestamp Analytics ---
        print("\n" + "="*60)
        print("3. Event Timestamp Discrepancy Analysis")
        print("="*60)
        print("This section analyzes the difference between two timestamps for each oracle event:")
        print("1. Block Commit Timestamp (Row Timestamp): The estimated time the event's transaction was mined.")
        print("2. On-Chain Event Timestamp (Argument Timestamp): The timestamp value passed as an argument to the smart contract.")
        print("\nThe calculation performed is: Difference = (Block Commit Timestamp) - (On-Chain Event Timestamp)")
        print("\n**Interpretation Insights:**")
        print("- For `RequestPrice`, this difference shows the accuracy of our block-based timestamp estimation.")
        print("- For `ProposePrice`, `DisputePrice`, and `Settle`, the On-Chain Event Timestamp refers to the *original request's timestamp*.")
        print("  Therefore, the difference represents the time elapsed between the initial request and the subsequent action.")
        print("\nNOTE: The main causality matrix below uses the **Block Commit Timestamp** (`timestamp_ms`).")
        
        event_calc_map = {
            "RequestPrice": "(Time of Request Commit) - (Time of Request Event)",
            "ProposePrice": "(Time of Proposal) - (Time of Original Request)",
            "DisputePrice": "(Time of Dispute) - (Time of Original Request)",
            "Settle": "(Time of Settlement) - (Time of Original Request)"
        }

        for event_name, diffs in sorted(timestamp_diffs.items()):
            if not diffs: continue
            avg_diff = statistics.mean(diffs)
            median_diff = statistics.median(diffs)
            stdev_diff = statistics.stdev(diffs) if len(diffs) > 1 else 0
            print(f"\n--- {event_name} ({len(diffs):,} events) ---")
            print(f"Calculation: {event_calc_map.get(event_name, 'N/A')}")
            print(f"- Average Difference: {format_seconds(avg_diff)}")
            print(f"- Median Difference:  {format_seconds(median_diff)}")
            print(f"- Std Deviation:      {format_seconds(stdev_diff)}")

        # --- 5. Build Full Timelines for Matched Markets ---
        full_timelines = []
        for mid in matched_orderbooks_with_request:
            market_rec = markets_by_mid[mid]
            msg_json = json.loads(market_rec.get('message'))
            
            propose_timestamps = [rec['timestamp_ms'] for rec in events_by_mid[mid].get('ProposePrice', [])]
            dispute_timestamps = [rec['timestamp_ms'] for rec in events_by_mid[mid].get('DisputePrice', [])]
            settle_timestamps = [rec['timestamp_ms'] for rec in events_by_mid[mid].get('Settle', [])]

            timeline = {
                'found_time_ms': parse_ms_timestamp(market_rec.get('found_time_ms')),
                'createdAt': parse_iso_timestamp(msg_json.get('createdAt')),
                'startDate': parse_iso_timestamp(msg_json.get('startDate')),
                'closed_time': parse_ms_timestamp(market_rec.get('closed_time_ms')),
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
        print("4. Timeline and Causality Analysis Matrix (for matched markets)")
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