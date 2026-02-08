import os
import asyncio
import asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging
import time
from datetime import datetime, timezone, timedelta

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
END_BLOCK = 81225971  # Jan 5, 2026 00:00 UTC (inclusive)
BLOCK_BATCH_SIZE = 100000  # Process in batches of 100k blocks

# Start time (Nov 18, 2025 07:00:03 UTC) - known from database
START_TIME_DT = datetime(2025, 11, 18, 7, 0, 3, tzinfo=timezone.utc)

# Block time (seconds per block)
SECONDS_PER_BLOCK = 2

# ========== END CONFIGURATION ==========

# ----------------------------------------------------------------
# 2. DATA FETCHING & LOADING
# ----------------------------------------------------------------

async def fetch_data(pool, query, params=[]):
    """Fetches data with detailed timing logging."""
    try:
        async with pool.acquire() as connection:
            sql_start = time.time()
            rows = await connection.fetch(query, *params)
            sql_elapsed = time.time() - sql_start
            
            pd_start = time.time()
            df = pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()
            pd_elapsed = time.time() - pd_start
            
            if rows:
                logger.info(f"Query fetched {len(rows):,} rows (SQL: {sql_elapsed:.2f}s, Pandas: {pd_elapsed:.3f}s)")
            else:
                logger.warning(f"Query returned 0 rows")
            return df
    except Exception as e:
        logger.error(f"SQL Error: {e}")
        return pd.DataFrame()

def load_market_data(csv_path):
    """Loads market data from CSV."""
    logger.info(f"Loading market data from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, usecols=['createdAt', 'startDate', 'acceptingOrdersTimestamp', 'conditionId'])
        df['conditionId'] = df['conditionId'].str.lower()
        df['createdAt_dt'] = pd.to_datetime(df['createdAt'], utc=True, format='ISO8601')
        df['startDate_dt'] = pd.to_datetime(df['startDate'], utc=True, format='ISO8601', errors='coerce')
        df['acceptingOrdersTimestamp_dt'] = pd.to_datetime(df['acceptingOrdersTimestamp'], utc=True, format='ISO8601', errors='coerce')
        df_unique = df.sort_values('createdAt_dt').drop_duplicates(subset='conditionId', keep='first')
        logger.info(f"Loaded {len(df_unique):,} unique markets from CSV.")
        return df_unique[['createdAt_dt', 'startDate_dt', 'acceptingOrdersTimestamp_dt', 'conditionId']]
    except Exception as e:
        logger.error(f"Error loading CSV: {e}")
        return pd.DataFrame()

def prepare_cdf_data(df, time_column, label):
    """Prepares data for CDF plotting."""
    if df.empty:
        return pd.DataFrame(columns=['datetime', 'count', 'label'])
    
    df_sorted = df.sort_values(time_column)
    df_sorted = df_sorted.copy()
    df_sorted['count'] = range(1, len(df_sorted) + 1)
    df_sorted['label'] = label
    
    return df_sorted[[time_column, 'count', 'label']].rename(columns={time_column: 'datetime'})

# ----------------------------------------------------------------
# 3. MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    # Calculate END_BLOCK timestamp
    blocks_elapsed = END_BLOCK - START_BLOCK
    seconds_elapsed = blocks_elapsed * SECONDS_PER_BLOCK
    end_time_dt = START_TIME_DT + timedelta(seconds=seconds_elapsed)
    
    logger.info(f"Block range: {START_BLOCK:,} to {END_BLOCK:,}")
    logger.info(f"Time range: {START_TIME_DT.isoformat()} to {end_time_dt.isoformat()}")
    
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
        batch_start_time = time.time()
        logger.info(f"Batch {batch_num}: blocks {current_block:,} to {batch_end:,}")
        
        df_batch = await fetch_data(pool, q_conditions, [ADDR['conditional_tokens'], current_block, batch_end])
        
        if not df_batch.empty:
            df_batch['condition_id'] = df_batch['condition_id'].str.lower()
            df_batch['timestamp_dt'] = pd.to_datetime(df_batch['timestamp'], unit='ms', utc=True)
            df_conditions_list.append(df_batch)
            batch_elapsed = time.time() - batch_start_time
            logger.info(f"  {len(df_batch):,} events (batch total: {batch_elapsed:.2f}s)")
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

    # 4. Filter to time range
    logger.info(f"\n--- FILTERING TO TIME RANGE ---")
    
    # Filter markets
    df_markets_filtered = df_markets[
        (df_markets['createdAt_dt'] >= START_TIME_DT) & 
        (df_markets['createdAt_dt'] <= end_time_dt)
    ].copy()
    logger.info(f"Markets in time range: {len(df_markets_filtered):,}")
    
    # Filter conditions
    df_conditions_filtered = df_conditions_dedup[
        (df_conditions_dedup['timestamp_dt'] >= START_TIME_DT) & 
        (df_conditions_dedup['timestamp_dt'] <= end_time_dt)
    ].copy()
    logger.info(f"Conditions in time range: {len(df_conditions_filtered):,}")
    

    
    # Filter markets by acceptingOrdersTimestamp
    df_markets_with_accepting = df_markets[
        (df_markets['acceptingOrdersTimestamp_dt'].notna()) &
        (df_markets['acceptingOrdersTimestamp_dt'] >= START_TIME_DT) & 
        (df_markets['acceptingOrdersTimestamp_dt'] <= end_time_dt)
    ].copy()
    logger.info(f"Markets with acceptingOrdersTimestamp in range: {len(df_markets_with_accepting):,}")

    # 5. Prepare CDF data
    logger.info(f"\n--- PREPARING CDF DATA ---")
    
    cdf_data = {}
    
    # 1. Market Creation (CSV)
    cdf_data['createdAt'] = prepare_cdf_data(df_markets_filtered, 'createdAt_dt', f'market object creation (n={len(df_markets_filtered):,})')
    
    # 2. Combined Condition Prepared
    cdf_data['conditions_all'] = prepare_cdf_data(df_conditions_filtered, 'timestamp_dt', f'All ConditionsPrepared (n={len(df_conditions_filtered):,})')
    
    # 3. Market acceptingOrdersTimestamp
    cdf_data['acceptingOrders'] = prepare_cdf_data(df_markets_with_accepting, 'acceptingOrdersTimestamp_dt', f'market object acceptingOrders (n={len(df_markets_with_accepting):,})')
    
    # 4-6. Per-oracle conditions
    oracle_mapping = {
        "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7": "MOOV2 Adapter",
        "0x58e1745bedda7312c4cddb72618923da1b90efde": "Centralized Adapter",
        "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296": "Negrisk Adapter",
    }
    for oracle_addr, oracle_name in oracle_mapping.items():
        oracle_conditions = df_conditions_filtered[df_conditions_filtered['oracle'] == oracle_addr]
        if not oracle_conditions.empty:
            cdf_data[f'oracle_{oracle_addr}'] = prepare_cdf_data(
                oracle_conditions, 'timestamp_dt', f'{oracle_name} (n={len(oracle_conditions):,})'
            )
            logger.info(f"  {oracle_name}: {len(oracle_conditions):,} events")
        else:
            logger.info(f"  {oracle_name}: 0 events")

    # 6. Create single plot with all 7 lines
    logger.info(f"\n--- CREATING PLOT ---")
    
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # Line styles and colors for each CDF
    plot_specs = [
        ('createdAt', 'red', '-', 3),
        ('conditions_all', 'black', '--', 2.5),
        ('acceptingOrders', 'purple', ':', 2),
        (f'oracle_0x65070be91477460d8a7aeeb94ef92fe056c2f2a7', '#1f77b4', '-.', 1.5),
        (f'oracle_0x58e1745bedda7312c4cddb72618923da1b90efde', '#ff7f0e', '-.', 1.5),
        (f'oracle_0xd91e80cf2e7be2e162c6513ced06f1dd0da35296', '#2ca02c', '-.', 1.5),
    ]
    
    for cdf_key, color, linestyle, linewidth in plot_specs:
        if cdf_key not in cdf_data or cdf_data[cdf_key].empty:
            logger.warning(f"No data for {cdf_key}")
            continue
        
        cdf = cdf_data[cdf_key]
        ax.plot(cdf['datetime'], cdf['count'], 
               label=cdf['label'].iloc[0],
               color=color, linewidth=linewidth, linestyle=linestyle, zorder=3 if linestyle == '-' else 2)
    
    ax.set_title('Cumulative Arrival of Conditions', fontsize=20, fontweight='bold')
    ax.set_xlabel('Time (UTC)', fontsize=16, fontweight='bold')
    ax.set_ylabel('Cumulative Count of Conditions', fontsize=16, fontweight='bold')
    ax.grid(True, which='both', linestyle='--', alpha=0.6)
    ax.legend(loc='upper left', fontsize=16, framealpha=0.95)
    
    # Increase tick label font sizes
    ax.tick_params(axis='x', labelsize=16)
    ax.tick_params(axis='y', labelsize=16)
    
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate(rotation=30, ha='right')
    
    output_file = "condition_ids_cdf.png"
    fig.savefig(output_file, dpi=300, bbox_inches='tight')
    logger.info(f"Plot saved: {output_file}")
    plt.show()

if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials not set.")
    elif not POLY_CSV_DIR or not os.path.exists(POLY_CSV_PATH):
        logger.error(f"CSV file not found at {POLY_CSV_PATH}.")
    else:
        asyncio.run(main())
