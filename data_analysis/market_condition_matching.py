import os
import asyncio
import asyncpg
import pandas as pd
import logging
from datetime import datetime, timezone
from collections import Counter

# ----------------------------------------------------------------
# 1. SETUP & CONFIGURATION
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

# --- Database Credentials ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- Configuration ---
POLY_CSV_DIR = os.getenv("POLY_CSV")
POLY_CSV_PATH = os.path.join(POLY_CSV_DIR, "gamma_markets.csv") if POLY_CSV_DIR else "gamma_markets.csv"

ADDR = {
    'conditional_tokens': "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower(),
}

# --- Oracle Aliases for Plot Legend ---
ORACLE_ALIASES = {
    "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7": "MOOV2 Adapter",
    "0x58e1745bedda7312c4cddb72618923da1b90efde": "Centralized Adapter",
    "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296": "Negrisk Adapter",
}

# ========== USER: SET YOUR CONFIGURATION HERE ==========
# Filter by block number
START_BLOCK = 79172086  # Nov 18, 2025 (inclusive)
END_BLOCK = 81225971    # Jan 5, 2026 00:00 UTC (inclusive)
BLOCK_BATCH_SIZE = 100000  # Process in batches of 100k blocks
# ========== END CONFIGURATION ==========

# --- Top Tags Configuration ---
TOP_TAGS_COUNT = 15

# ----------------------------------------------------------------
# 2. DATA FETCHING & LOADING
# ----------------------------------------------------------------

async def fetch_data(pool, query, params=[]):
    """Fetches data with timing logging."""
    try:
        async with pool.acquire() as connection:
            rows = await connection.fetch(query, *params)
            if rows:
                logger.info(f"Query fetched {len(rows):,} rows")
            else:
                logger.warning(f"Query returned 0 rows.")
            return pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()
    except Exception as e:
        logger.error(f"SQL Error: {e}")
        logger.error(f"Query: {query}")
        logger.error(f"Params: {params}")
        return pd.DataFrame()

def load_market_data(csv_path):
    """Loads market data from CSV including conditionId and tags."""
    logger.info(f"Loading market data from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, usecols=['createdAt', 'startDate', 'acceptingOrdersTimestamp', 'conditionId', 'tags'])
        df['conditionId'] = df['conditionId'].str.lower()
        
        df['createdAt_dt'] = pd.to_datetime(df['createdAt'], utc=True, format='ISO8601')
        df['startDate_dt'] = pd.to_datetime(df['startDate'], utc=True, format='ISO8601', errors='coerce')
        df['acceptingOrdersTimestamp_dt'] = pd.to_datetime(df['acceptingOrdersTimestamp'], utc=True, format='ISO8601', errors='coerce')
        
        # Parse tags column
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
        
        df['tags'] = df['tags'].apply(parse_tags)
        
        df_unique = df.sort_values('createdAt_dt').drop_duplicates(subset='conditionId', keep='first')
        logger.info(f"Loaded {len(df_unique):,} unique markets from CSV.")
        return df_unique[['createdAt_dt', 'startDate_dt', 'acceptingOrdersTimestamp_dt', 'conditionId', 'tags']]
    except FileNotFoundError:
        logger.error(f"CSV file not found at {csv_path}.")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading/processing CSV: {e}")
        return pd.DataFrame()

# ----------------------------------------------------------------
# 3. ANALYSIS HELPERS
# ----------------------------------------------------------------

def analyze_tags(df_markets, title):
    """Analyzes and prints top tags from markets."""
    if df_markets.empty or 'tags' not in df_markets.columns:
        logger.info(f"{title}: No data or tags column")
        return
    
    all_tags = []
    for tags_list in df_markets['tags']:
        if isinstance(tags_list, list):
            all_tags.extend(tags_list)
    
    if all_tags:
        tag_counts = Counter(all_tags)
        top_tags = tag_counts.most_common(TOP_TAGS_COUNT)
        
        logger.info(f"\n{title} (n={len(df_markets):,} markets, {len(all_tags):,} total tags)")
        for tag, count in top_tags:
            percentage = (count / len(all_tags)) * 100
            logger.info(f"  {tag}: {count:,} ({percentage:.1f}%)")
    else:
        logger.info(f"{title}: No tags found")

# ----------------------------------------------------------------
# 4. MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    logger.info(f"Using block range: {START_BLOCK} to {END_BLOCK} (inclusive)")
    
    # 1. Load CSV Data
    df_markets = load_market_data(POLY_CSV_PATH)
    if df_markets.empty:
        return
    
    logger.info(f"Total Markets in CSV: {len(df_markets):,}")

    # 2. Database Connection
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Connected to database.")

    # 3. Fetch ConditionPreparation events in block range (batched)
    logger.info(f"Fetching ConditionPreparation events in batches of {BLOCK_BATCH_SIZE:,} blocks...")
    
    q_conditions = """
        SELECT 
            topics[2] as condition_id, 
            timestamp_ms as timestamp, 
            '0x' || substring(topics[3] from 27) as oracle, 
            block_number
        FROM events_conditional_tokens 
        WHERE event_name = 'ConditionPreparation' 
        AND LOWER(contract_address) = $1
        AND block_number >= $2
        AND block_number <= $3
    """
    
    # Process in batches
    df_conditions_list = []
    current_block = START_BLOCK
    batch_num = 0
    
    while current_block <= END_BLOCK:
        batch_num += 1
        batch_end = min(current_block + BLOCK_BATCH_SIZE - 1, END_BLOCK)
        logger.info(f"Batch {batch_num}: blocks {current_block:,} to {batch_end:,}")
        
        df_batch = await fetch_data(pool, q_conditions, [ADDR['conditional_tokens'], current_block, batch_end])
        
        if not df_batch.empty:
            df_batch['condition_id'] = df_batch['condition_id'].str.lower()
            df_conditions_list.append(df_batch)
            logger.info(f"  Fetched {len(df_batch):,} events in this batch")
        else:
            logger.info(f"  No events in this batch")
        
        current_block = batch_end + 1
    
    if not df_conditions_list:
        logger.error(f"No ConditionPreparation events found in block range.")
        await pool.close()
        return
    
    # Combine all batches
    df_conditions = pd.concat(df_conditions_list, ignore_index=True)
    logger.info(f"\nTotal fetched: {len(df_conditions):,} ConditionPreparation events across {batch_num} batches")
    
    # Deduplicate by condition_id (keep first/earliest)
    df_conditions_dedup = df_conditions.sort_values('timestamp').drop_duplicates(subset='condition_id', keep='first')
    logger.info(f"After deduplication: {len(df_conditions_dedup):,} unique condition IDs")
    
    await pool.close()

    # 4. Match markets with conditions
    logger.info("\n--- MATCHING ANALYSIS ---")
    df_matched = pd.merge(
        df_markets,
        df_conditions_dedup,
        left_on='conditionId',
        right_on='condition_id',
        how='inner'
    )
    logger.info(f"Matched {len(df_matched):,} markets with ConditionPreparation events")
    
    # 5. Statistics by Oracle
    logger.info("\n--- ORACLE STATISTICS ---")
    total_matched = len(df_matched)
    total_conditions = len(df_conditions_dedup)
    
    logger.info(f"Total ConditionPreparation events (all oracles): {total_conditions:,}")
    logger.info(f"Total matched markets: {total_matched:,}")
    logger.info(f"Match rate: {(total_matched/len(df_markets)*100):.2f}% of all markets\n")
    
    if not df_matched.empty:
        by_oracle = df_matched.groupby('oracle').agg({
            'condition_id': 'count',
            'conditionId': 'nunique'
        }).rename(columns={'condition_id': 'events', 'conditionId': 'unique_markets'})
        
        logger.info("Per-Oracle Breakdown:")
        for oracle_addr in sorted(ORACLE_ALIASES.keys()):
            if oracle_addr in by_oracle.index:
                row = by_oracle.loc[oracle_addr]
                alias = ORACLE_ALIASES.get(oracle_addr, oracle_addr)
                events = int(row['events'])
                markets = int(row['unique_markets'])
                pct = (markets / total_matched) * 100
                
                logger.info(f"  {alias}:")
                logger.info(f"    Events: {events:,}")
                logger.info(f"    Unique Markets: {markets:,} ({pct:.2f}% of matched)")
    
    # 6. Unmatched Analysis
    unmatched_count = len(df_markets) - total_matched
    unmatched_pct = (unmatched_count / len(df_markets)) * 100
    logger.info(f"\n--- UNMATCHED MARKETS ---")
    logger.info(f"Unmatched: {unmatched_count:,} ({unmatched_pct:.2f}% of all markets)")
    
    # 7. Tag Analysis
    logger.info("\n--- TAG ANALYSIS ---")
    analyze_tags(df_matched, "Top Tags in Matched Markets")
    
    for oracle_addr in sorted(ORACLE_ALIASES.keys()):
        oracle_matched = df_matched[df_matched['oracle'] == oracle_addr]
        if not oracle_matched.empty:
            alias = ORACLE_ALIASES.get(oracle_addr, oracle_addr)
            analyze_tags(oracle_matched, f"Top Tags in {alias} Matched Markets")

if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials not set.")
    elif not POLY_CSV_DIR or not os.path.exists(POLY_CSV_PATH):
        logger.error(f"CSV file not found at {POLY_CSV_PATH}.")
    else:
        asyncio.run(main())
