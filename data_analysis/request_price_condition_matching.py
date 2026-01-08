import asyncio
import asyncpg
import os
import logging
import re
import pandas as pd
from typing import Dict, List
from hexbytes import HexBytes
from eth_abi import decode as abi_decode
from datetime import timedelta
import numpy as np

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Database Configuration ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- Configuration ---
POLY_CSV_DIR = os.getenv("POLY_CSV")
POLY_CSV_PATH = os.path.join(POLY_CSV_DIR, "gamma_markets.csv") if POLY_CSV_DIR else "gamma_markets.csv"

# Conditional Tokens Contract Address
ADDR = {
    'conditional_tokens': "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower(),
}

# Oracle Aliases for readability
ORACLE_ALIASES = {
    "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7": "MOOV2 Adapter",
    "0x58e1745bedda7312c4cddb72618923da1b90efde": "Centralized Adapter",
    "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296": "Negrisk UmaCtfAdapter",
}

# Specific addresses for the new analysis
MOOV2_REQ_TABLE = 'events_managed_oracle'
MOOV2_ADAPTER_ORACLE = "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7"

# Percentiles to calculate (as requested)
PERCENTILES_TO_CALCULATE = [0.01, 0.50, 0.99]

# Percentiles for non-zero differences analysis
PERCENTILES_NON_ZERO = [0.1, 10, 33.3, 50, 66.6, 90, 99.9]

# Top N tags to display
TOP_TAGS_COUNT = 15

# RequestPrice Event Definition
REQUEST_PRICE_EVENT = {
    "signature": "RequestPrice(address,bytes32,uint256,bytes,address,uint256,uint256)",
    "indexed_types": ["address"],
    "data_types": ["bytes32", "uint256", "bytes", "address", "uint256", "uint256"],
    "arg_names": ["requester", "identifier", "timestamp", "ancillaryData", "currency", "reward", "finalFee"]
}

# ----------------------------------------------------------------
# 1. HELPER FUNCTIONS
# ----------------------------------------------------------------

def decode_argument(arg_type: str, value) -> str:
    """Decodes a single argument into a more readable format."""
    if isinstance(value, bytes):
        if "bytes32" in arg_type:
            try:
                return value.strip(b'\x00').decode('utf-8')
            except UnicodeDecodeError:
                return value.hex()
        else:
            try:
                decoded_string = value.decode('utf-8')
                return ''.join(char for char in decoded_string if char.isprintable())
            except UnicodeDecodeError:
                return f"<Non-UTF8 Bytes, len: {len(value)}>"
    return str(value)

def extract_market_id(ancillary_data_str: str) -> int:
    """Extract market_id from ancillary data string."""
    try:
        pattern = r'market_id:\s*(\d+)'
        match = re.search(pattern, ancillary_data_str)
        if match:
            return int(match.group(1))
    except Exception as e:
        logger.debug(f"Failed to extract market_id: {e}")
    return None

def format_timedelta(seconds):
    """Helper to format seconds into a readable timedelta string."""
    if pd.isna(seconds): return "N/A"
    
    sign = "-" if seconds < 0 else ""
    total_seconds = int(abs(seconds))
    
    # Calculate days, hours, minutes, seconds
    days = total_seconds // 86400
    remaining = total_seconds % 86400
    hours = remaining // 3600
    remaining = remaining % 3600
    minutes = remaining // 60
    secs = remaining % 60
    
    if days > 0:
        return f"{sign}{days}d {hours:02d}:{minutes:02d}:{secs:02d}"
    else:
        return f"{sign}{hours:02d}:{minutes:02d}:{secs:02d}"

# ----------------------------------------------------------------
# 2. DATABASE FUNCTIONS
# ----------------------------------------------------------------

async def fetch_request_price_events(pool, table_name: str) -> List[Dict]:
    """Fetch and decode RequestPrice events, including timestamp_ms."""
    query = f'''
        SELECT block_number, timestamp_ms, data, topics
        FROM "{table_name}"
        WHERE event_name = 'RequestPrice'
        ORDER BY block_number
    '''
    
    all_events = []
    
    try:
        async with pool.acquire() as conn:
            records = await conn.fetch(query)
            logger.info(f"Found {len(records):,} RequestPrice events in {table_name}")
            
            for record in records:
                try:
                    block_timestamp_ms = record['timestamp_ms']
                    topics = record['topics']
                    data_hex = record['data']
                    
                    if not topics or len(topics) < 2:
                        continue
                    
                    indexed_args_raw = [HexBytes(t) for t in topics[1:]]
                    data_bytes = HexBytes(data_hex)
                    
                    # Decode logic to extract ancillaryData
                    decoded_data_args = abi_decode(REQUEST_PRICE_EVENT['data_types'], data_bytes)
                    decoded_indexed_args = [
                        abi_decode([dtype], val)[0] 
                        for dtype, val in zip(REQUEST_PRICE_EVENT['indexed_types'], indexed_args_raw)
                    ]
                    
                    all_args = []
                    indexed_args_iter = iter(decoded_indexed_args)
                    data_args_iter = iter(decoded_data_args)
                    
                    for i, arg_name in enumerate(REQUEST_PRICE_EVENT['arg_names']):
                        if i < len(REQUEST_PRICE_EVENT['indexed_types']):
                            all_args.append(next(indexed_args_iter))
                        else:
                            all_args.append(next(data_args_iter))
                    
                    event_data = {}
                    all_types = REQUEST_PRICE_EVENT['indexed_types'] + REQUEST_PRICE_EVENT['data_types']
                    for name, type_str, value in zip(REQUEST_PRICE_EVENT['arg_names'], all_types, all_args):
                        event_data[name] = value
                    
                    ancillary_data_str = decode_argument('bytes', event_data.get('ancillaryData', b''))
                    market_id = extract_market_id(ancillary_data_str)
                    
                    if market_id is not None:
                        all_events.append({
                            'market_id': market_id,
                            'req_timestamp_ms': block_timestamp_ms,
                            'req_table': table_name
                        })
                
                except Exception as e:
                    logger.debug(f"Failed to decode event in {table_name}: {e}")
                    continue
    
    except Exception as e:
        logger.error(f"Error querying {table_name}: {e}")
    
    return all_events

async def fetch_condition_preparation_events(pool) -> List[Dict]:
    """Fetch ConditionPreparation events, including timestamp_ms and oracle."""
    logger.info("Fetching ConditionPreparation events...")
    
    query = f"""
        SELECT topics[2] as condition_id, timestamp_ms as prep_timestamp_ms, '0x' || substring(topics[3] from 27) as oracle 
        FROM events_conditional_tokens 
        WHERE event_name = 'ConditionPreparation' 
        AND LOWER(contract_address) = $1
    """
    
    try:
        async with pool.acquire() as conn:
            records = await conn.fetch(query, ADDR['conditional_tokens'])
            logger.info(f"Found {len(records):,} ConditionPreparation events.")
            
            events = [dict(row) for row in records]
            
            for event in events:
                if isinstance(event['condition_id'], HexBytes):
                    event['condition_id'] = event['condition_id'].hex().lower()
                elif isinstance(event['condition_id'], str):
                    event['condition_id'] = event['condition_id'].lower()
                
                if isinstance(event['oracle'], str):
                    event['oracle'] = event['oracle'].lower()
            
            return events
            
    except Exception as e:
        logger.error(f"Error querying events_conditional_tokens: {e}")
        return []

# ----------------------------------------------------------------
# 3. CSV LOADING
# ----------------------------------------------------------------

def load_market_mapping(csv_path: str) -> pd.DataFrame:
    """Load market_id ('id'), conditionId mapping, and tags from CSV."""
    logger.info(f"Loading market mapping from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, usecols=['id', 'conditionId', 'tags'], low_memory=False)
        
        df = df.rename(columns={'id': 'market_id'})
        
        df['market_id'] = df['market_id'].astype(int)
        df['conditionId'] = df['conditionId'].astype(str).str.lower()
        
        # Parse tags column (it's a string representation of a list)
        def parse_tags(tag_str):
            if pd.isna(tag_str):
                return []
            if isinstance(tag_str, str):
                try:
                    import ast
                    tags_list = ast.literal_eval(tag_str)
                    return tags_list if isinstance(tags_list, list) else []
                except:
                    return []
            return []
        
        # Parse tags
        df['tags'] = df['tags'].apply(parse_tags)
        
        # Deduplicate by conditionId, keeping the first market_id
        df_unique = df.sort_values('market_id').drop_duplicates(subset='conditionId', keep='first')
        
        logger.info(f"Loaded {len(df_unique):,} unique market mappings from CSV.")
        return df_unique
    except Exception as e:
        logger.error(f"Error loading market mapping: {e}")
        return pd.DataFrame()

# ----------------------------------------------------------------
# 4. MAIN ANALYSIS
# ----------------------------------------------------------------

async def main():
    logger.info("Starting RequestPrice and ConditionPreparation Cardinality and Timing Analysis...")
    
    # 1. Load CSV Mapping
    df_mapping = load_market_mapping(POLY_CSV_PATH)
    if df_mapping.empty:
        logger.error("Failed to load market mapping from CSV.")
        return
    
    # 2. Database Connection
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Connected to database.")
    
    try:
        # 3. Fetch RequestPrice Events
        req_price_events_oov2 = await fetch_request_price_events(pool, 'oov2')
        req_price_events_moov2 = await fetch_request_price_events(pool, 'events_managed_oracle')
        
        all_req_price_events = req_price_events_oov2 + req_price_events_moov2
        df_req_price = pd.DataFrame(all_req_price_events)
        
        total_req_price = len(df_req_price)
        logger.info(f"Total RequestPrice events with extracted market_id: {total_req_price:,}")
        
        if df_req_price.empty:
            logger.info("No RequestPrice events to process.")
            return
        
        # 4. Fetch ConditionPreparation Events
        prep_events = await fetch_condition_preparation_events(pool)
        df_prep_event = pd.DataFrame(prep_events)
        
        # Deduplicate ConditionPreparation events by condition_id, keeping the earliest one
        df_prep_event = df_prep_event.sort_values('prep_timestamp_ms').drop_duplicates(subset='condition_id', keep='first')
        total_prep_event = len(df_prep_event)
        logger.info(f"Total unique ConditionPreparation events: {total_prep_event:,}")
        
        if df_prep_event.empty:
            logger.info("No ConditionPreparation events to process.")
            return

        # 5. Perform Matching (RequestPrice -> CSV -> ConditionPreparation)
        
        # Step 5.1: Match RequestPrice events with CSV mapping on market_id
        df_matched_req_csv = pd.merge(
            df_req_price, 
            df_mapping, 
            on='market_id', 
            how='inner'
        )
        logger.info(f"RequestPrice events matched with CSV mapping: {len(df_matched_req_csv):,}")
        
        # Step 5.2: Match the result with ConditionPreparation events on conditionId/condition_id
        # This creates the final DataFrame containing ONLY the matched pairs and their timestamps/metadata
        df_final_match = pd.merge(
            df_matched_req_csv, 
            df_prep_event[['condition_id', 'oracle', 'prep_timestamp_ms']], 
            left_on='conditionId', 
            right_on='condition_id', 
            how='inner'
        )
        
        total_matched = len(df_final_match)
        logger.info(f"Total Matched RequestPrice <-> ConditionPreparation: {total_matched:,}")
        
        if total_matched == 0:
            logger.info("No events were successfully matched.")
            return
        
        # 6. Cardinality Analysis
        
        match_counts = df_final_match.groupby(['req_table', 'oracle']).size().reset_index(name='match_count')
        
        logger.info("\n--- Detailed Match Cardinality (RequestPrice Source vs. ConditionPreparation Oracle) ---")
        
        for req_table in ['oov2', 'events_managed_oracle']:
            table_alias = "MOOV2" if req_table == "events_managed_oracle" else "OOV2"
            logger.info(f"\nMatches for {table_alias} ({req_table}):")
            
            table_matches = match_counts[match_counts['req_table'] == req_table]
            
            if table_matches.empty:
                logger.info("  No matches found for this table.")
                continue
                
            total_for_table = table_matches['match_count'].sum()
            logger.info(f"  Total matches for {table_alias}: {total_for_table:,}")
            
            for index, row in table_matches.sort_values(by='match_count', ascending=False).iterrows():
                oracle_alias = ORACLE_ALIASES.get(row['oracle'], row['oracle'])
                logger.info(f"    - Oracle {oracle_alias}: {row['match_count']:,}")
        
        # 7. Comprehensive Timing Analysis (Min, Max, Mean, Std, Skew, Kurtosis, Percentiles)
        
        # Calculate time difference: Delta T = T_prep - T_req (in seconds)
        df_final_match['time_diff_sec'] = (df_final_match['prep_timestamp_ms'] - df_final_match['req_timestamp_ms']) / 1000
        
        # Step 7.1: Calculate Moments (Count, Min, Max, Mean, Std, Skew, Kurtosis)
        timing_stats = df_final_match.groupby(['req_table', 'oracle'])['time_diff_sec'].agg(
            ['count', 'min', 'max', 'mean', 'std', 'skew', pd.Series.kurt]
        ).reset_index()
        timing_stats = timing_stats.rename(columns={'kurt': 'kurtosis'})
        
        # Step 7.2: Calculate Percentiles
        quantile_stats = df_final_match.groupby(['req_table', 'oracle'])['time_diff_sec'].quantile(PERCENTILES_TO_CALCULATE).unstack()
        
        # Rename columns for clarity (e.g., 0.01 -> p1, 0.5 -> p50)
        quantile_stats.columns = [f'p{int(p*100)}' for p in PERCENTILES_TO_CALCULATE]
        quantile_stats = quantile_stats.reset_index()
        
        # Step 7.3: Merge all statistics
        timing_stats = pd.merge(timing_stats, quantile_stats, on=['req_table', 'oracle'], how='left')
        
        logger.info("\n--- Comprehensive Timing Analysis (T_prep - T_req) on Matched Pairs ---")
        
        MIN_COUNT_FOR_MOMENTS = 5 # Threshold to print Skewness/Kurtosis
        
        for index, row in timing_stats.iterrows():
            req_table = row['req_table']
            oracle = row['oracle']
            
            req_alias = "MOOV2" if req_table == "events_managed_oracle" else "OOV2"
            oracle_alias = ORACLE_ALIASES.get(oracle, oracle)
            
            logger.info(f"\nPair: {req_alias} RequestPrice <-> {oracle_alias} ConditionPreparation (n={row['count']:,})")
            logger.info(f"  Min Time Difference: {format_timedelta(row['min'])} ({row['min']:.2f} seconds)")
            logger.info(f"  Max Time Difference: {format_timedelta(row['max'])} ({row['max']:.2f} seconds)")
            logger.info(f"  Mean Time Difference: {format_timedelta(row['mean'])} ({row['mean']:.2f} seconds)")
            logger.info(f"  Standard Deviation (Std): {row['std']:.2f} seconds")
            
            # Print moments only if count is sufficient and values are not NaN
            if row['count'] >= MIN_COUNT_FOR_MOMENTS and not np.isnan(row['skew']):
                logger.info(f"  Skewness (Normalized 3rd Moment - Asymmetry): {row['skew']:.4f}")
                logger.info(f"  Kurtosis (Heavy-Tailedness): {row['kurtosis']:.4f}")
            else:
                logger.info(f"  Skewness/Kurtosis: N/A (Insufficient data points or calculation failed)")
            
            # Print Percentiles
            logger.info("  --- Percentiles ---")
            for p in PERCENTILES_TO_CALCULATE:
                col_name = f'p{int(p*100)}'
                value = row[col_name]
                logger.info(f"  {p*100:.0f}th Percentile: {format_timedelta(value)} ({value:.2f} seconds)")
            
        # 8. Zero/Non-Zero Difference Analysis for ALL Combinations with Tag Analysis
        
        logger.info("\n--- Zero/Non-Zero Time Difference Analysis for All Combinations ---")
        
        for index, row in timing_stats.iterrows():
            req_table = row['req_table']
            oracle = row['oracle']
            
            req_alias = "MOOV2" if req_table == "events_managed_oracle" else "OOV2"
            oracle_alias = ORACLE_ALIASES.get(oracle, oracle)
            
            # Filter for this specific combination
            df_pair = df_final_match[
                (df_final_match['req_table'] == req_table) & 
                (df_final_match['oracle'] == oracle)
            ].copy()
            
            total_pair = len(df_pair)
            
            if total_pair > 0:
                # Count zero and non-zero differences
                zero_diff_count = (df_pair['time_diff_sec'] == 0).sum()
                non_zero_diff_count = total_pair - zero_diff_count
                
                logger.info(f"\n{req_alias} RequestPrice <-> {oracle_alias} ConditionPreparation")
                logger.info(f"  Total Matches: {total_pair:,}")
                logger.info(f"  Count with Zero Time Difference (T_prep = T_req): {zero_diff_count:,}")
                logger.info(f"  Count with Non-Zero Time Difference (T_prep != T_req): {non_zero_diff_count:,}")
                
                # ===== ZERO DIFFERENCE ANALYSIS =====
                if zero_diff_count > 0:
                    df_zero = df_pair[df_pair['time_diff_sec'] == 0]
                    
                    # Extract and count tags for zero-difference matches
                    all_tags_zero = []
                    for tags_list in df_zero['tags']:
                        if isinstance(tags_list, list):
                            all_tags_zero.extend(tags_list)
                    
                    if all_tags_zero:
                        from collections import Counter
                        tag_counts_zero = Counter(all_tags_zero)
                        top_tags_zero = tag_counts_zero.most_common(TOP_TAGS_COUNT)
                        
                        logger.info(f"  --- Top {TOP_TAGS_COUNT} Tags (Zero Difference, n={zero_diff_count:,}) ---")
                        for tag, count in top_tags_zero:
                            percentage = (count / len(all_tags_zero)) * 100
                            logger.info(f"    {tag}: {count:,} ({percentage:.1f}%)")
                    else:
                        logger.info(f"  --- Top Tags (Zero Difference, n={zero_diff_count:,}) ---")
                        logger.info(f"    No tags found for zero-difference matches")
                
                # ===== NON-ZERO DIFFERENCE ANALYSIS =====
                if non_zero_diff_count > 0:
                    df_non_zero = df_pair[df_pair['time_diff_sec'] != 0].copy()
                    df_non_zero_times = df_non_zero['time_diff_sec']
                    
                    min_val = df_non_zero_times.min()
                    max_val = df_non_zero_times.max()
                    avg_val = df_non_zero_times.mean()
                    std_val = df_non_zero_times.std()
                    
                    logger.info(f"  --- Statistics for Non-Zero Differences (n={non_zero_diff_count:,}) ---")
                    logger.info(f"    Min: {format_timedelta(min_val)} ({min_val:.2f} seconds)")
                    logger.info(f"    Max: {format_timedelta(max_val)} ({max_val:.2f} seconds)")
                    logger.info(f"    Avg: {format_timedelta(avg_val)} ({avg_val:.2f} seconds)")
                    logger.info(f"    Std: {std_val:.2f} seconds")
                    
                    # Calculate and print percentiles for non-zero differences
                    logger.info(f"    --- Percentiles ---")
                    for p in PERCENTILES_NON_ZERO:
                        percentile_val = df_non_zero_times.quantile(p / 100.0)
                        logger.info(f"    {p:.1f}th Percentile: {format_timedelta(percentile_val)} ({percentile_val:.2f} seconds)")
                    
                    # Extract and count tags for non-zero-difference matches
                    all_tags_non_zero = []
                    for tags_list in df_non_zero['tags']:
                        if isinstance(tags_list, list):
                            all_tags_non_zero.extend(tags_list)
                    
                    if all_tags_non_zero:
                        from collections import Counter
                        tag_counts_non_zero = Counter(all_tags_non_zero)
                        top_tags_non_zero = tag_counts_non_zero.most_common(TOP_TAGS_COUNT)
                        
                        logger.info(f"  --- Top {TOP_TAGS_COUNT} Tags (Non-Zero Difference, n={non_zero_diff_count:,}) ---")
                        for tag, count in top_tags_non_zero:
                            percentage = (count / len(all_tags_non_zero)) * 100
                            logger.info(f"    {tag}: {count:,} ({percentage:.1f}%)")
        
        logger.info("\nAnalysis complete!")
    
    finally:
        await pool.close()

if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials not set. Please set PG_SOCKET, POLY_DB, POLY_DB_CLI, and POLY_DB_CLI_PASS.")
    elif not POLY_CSV_DIR:
        logger.error("CSV directory not set. Please set the POLY_CSV environment variable.")
    elif not os.path.exists(POLY_CSV_PATH):
        logger.error(f"CSV file not found at {POLY_CSV_PATH}. Please ensure POLY_CSV is the correct directory path.")
    else:
        asyncio.run(main())