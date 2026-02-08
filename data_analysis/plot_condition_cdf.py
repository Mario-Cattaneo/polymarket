import os
import asyncio
import asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging
from datetime import datetime, timezone

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

# --- Oracle Aliases ---
ORACLE_ALIASES = {
    "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7": "MOOV2 Adapter",
    "0x58e1745bedda7312c4cddb72618923da1b90efde": "Centralized Adapter",
    "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296": "Negrisk Adapter",
}

# ========== USER: SET YOUR CONFIGURATION HERE ==========
# Filter by block number
START_BLOCK = 79172086  # Nov 18, 2025 (inclusive)
END_BLOCK = 79172086 + 200000    # Jan 5, 2026 00:00 UTC (inclusive)
BLOCK_BATCH_SIZE = 100000  # Process in batches of 100k blocks
# ========== END CONFIGURATION ==========

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
        return pd.DataFrame()

def load_market_data(csv_path):
    """Loads market data from CSV."""
    logger.info(f"Loading market data from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, usecols=['createdAt', 'conditionId'])
        df['conditionId'] = df['conditionId'].str.lower()
        df['createdAt_dt'] = pd.to_datetime(df['createdAt'], utc=True, format='ISO8601')
        df_unique = df.sort_values('createdAt_dt').drop_duplicates(subset='conditionId', keep='first')
        logger.info(f"Loaded {len(df_unique):,} unique markets from CSV.")
        return df_unique[['createdAt_dt', 'conditionId']]
    except FileNotFoundError:
        logger.error(f"CSV file not found at {csv_path}.")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading CSV: {e}")
        return pd.DataFrame()

# ----------------------------------------------------------------
# 3. CDF HELPERS
# ----------------------------------------------------------------

def prepare_cdf_data(df, time_column, label):
    """Prepares data for CDF plotting."""
    if df.empty:
        return pd.DataFrame(columns=['datetime', 'count', 'label'])
    
    if not pd.api.types.is_datetime64_any_dtype(df[time_column]):
        df['datetime'] = pd.to_datetime(df[time_column], unit='ms', utc=True)
    else:
        df['datetime'] = df[time_column]

    df_sorted = df.sort_values('datetime')
    df_sorted['count'] = range(1, len(df_sorted) + 1)
    df_sorted['label'] = label
    
    return df_sorted[['datetime', 'count', 'label']]

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
            df_batch['timestamp_dt'] = pd.to_datetime(df_batch['timestamp'], unit='ms', utc=True)
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
    df_conditions_dedup = df_conditions.sort_values('timestamp_dt').drop_duplicates(subset='condition_id', keep='first')
    logger.info(f"After deduplication: {len(df_conditions_dedup):,} unique condition IDs")
    
    await pool.close()

    # 4. Prepare CDF data
    logger.info("\n--- PREPARING CDF DATA ---")
    
    # CDF 1: CSV Market Creation Times
    cdf_csv = prepare_cdf_data(df_markets, 'createdAt_dt', 'Market Creation (CSV)')
    logger.info(f"CSV markets CDF: {len(cdf_csv):,} points")
    
    # CDF 2: Condition Preparation Times
    cdf_conditions = prepare_cdf_data(df_conditions_dedup, 'timestamp_dt', 'Condition Prepared (Events)')
    logger.info(f"Condition preparation CDF: {len(cdf_conditions):,} points")
    
    # CDF 3-5: Per-Oracle Condition Preparation Times
    cdf_by_oracle = {}
    for oracle_addr in ORACLE_ALIASES.keys():
        oracle_conditions = df_conditions_dedup[df_conditions_dedup['oracle'] == oracle_addr]
        if not oracle_conditions.empty:
            alias = ORACLE_ALIASES.get(oracle_addr, oracle_addr)
            cdf_oracle = prepare_cdf_data(oracle_conditions, 'timestamp_dt', f'{alias}')
            cdf_by_oracle[oracle_addr] = cdf_oracle
            logger.info(f"  {alias}: {len(cdf_oracle):,} events")

    # 5. Plot CDF
    logger.info("\n--- PLOTTING CDF ---")
    plt.figure(figsize=(14, 8))
    
    # Plot CSV market creation times
    plt.plot(cdf_csv['datetime'], cdf_csv['count'], 
             label=f"Market Creation (CSV) (n={len(cdf_csv):,})",
             color='red', linewidth=3, linestyle='-', zorder=1)
    
    # Plot combined condition preparation times
    plt.plot(cdf_conditions['datetime'], cdf_conditions['count'], 
             label=f"Condition Prepared (All Oracles) (n={len(cdf_conditions):,})",
             color='black', linewidth=2.5, linestyle='--', zorder=3)
    
    # Plot per-oracle condition preparation times
    oracle_colors = ['#1f77b4', '#ff7f0e', '#2ca02c']  # blue, orange, green
    for idx, (oracle_addr, cdf_oracle) in enumerate(cdf_by_oracle.items()):
        alias = ORACLE_ALIASES.get(oracle_addr, oracle_addr)
        plt.plot(cdf_oracle['datetime'], cdf_oracle['count'],
                label=f"{alias} (n={len(cdf_oracle):,})",
                color=oracle_colors[idx % len(oracle_colors)],
                linewidth=1.5, linestyle='-.', zorder=2)
    
    # Styling
    plt.title('Cumulative Arrival of Condition IDs (CDF)', fontsize=16, fontweight='bold')
    plt.xlabel('Time (UTC)', fontsize=12)
    plt.ylabel('Cumulative Count of Unique Conditions', fontsize=12)
    plt.grid(True, which='both', linestyle='--', alpha=0.6)
    plt.legend(loc='upper left', fontsize=10)
    
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gcf().autofmt_xdate()
    
    output_file = "condition_ids_cdf.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    logger.info(f"CDF Plot saved to {output_file}")
    plt.show()

if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials not set.")
    elif not POLY_CSV_DIR or not os.path.exists(POLY_CSV_PATH):
        logger.error(f"CSV file not found at {POLY_CSV_PATH}.")
    else:
        asyncio.run(main())
