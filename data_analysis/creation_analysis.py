import os
import asyncio
import asyncpg
import pandas as pd
import logging
from pathlib import Path
import time

# ----------------------------------------------------------------
# 1. SETUP & CONFIGURATION
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

# --- Database Credentials ---
# NOTE: These environment variables must be set before running the script
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- File Paths ---
CSV_DIR = os.getenv('POLY_CSV', '.')
GAMMA_MARKETS_FILE = Path(CSV_DIR) / 'gamma_markets.csv'

# --- Table & Contract Config ---
FORK_MARKET_TABLE = "markets_3"
ADDR = {
    'negrisk_exchange': "0xc5d563a36ae78145c45a50134d48a1215220f80a".lower(),
    'ctfe_exchange': "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e".lower(),
    'conditional_tokens': "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower(),
}

# --- Time Filtering Config ---
FILTER_BY_TIME = True
START_TIME = 1766146619712  # Dec-19-2025
END_TIME = 1766673099000    # Dec-25-2025

# ----------------------------------------------------------------
# 2. HELPER FUNCTIONS
# ----------------------------------------------------------------

async def fetch_data(pool, query, params=[]):
    try:
        async with pool.acquire() as connection:
            start = time.time()
            rows = await connection.fetch(query, *params)
            elapsed = time.time() - start
            if elapsed > 2.0:
                logger.info(f"Query fetched {len(rows):,} rows in {elapsed:.2f}s")
            return pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"SQL Error: {e}")
        return pd.DataFrame()

def analyze_and_print_stats(name, df, id_column):
    if df.empty or id_column not in df.columns:
        print(f"- {name:<30}: 0 total, 0 unique, 0 duplicates")
        return

    raw_series = df[id_column].dropna().astype(str).str.lower()
    total_count = len(raw_series)
    unique_count = raw_series.nunique()
    duplicate_count = total_count - unique_count

    print(f"- {name:<30}: {total_count:,} total, {unique_count:,} unique, {duplicate_count:,} duplicates")

    if "exchange" in name:
        value_counts = raw_series.value_counts()
        is_valid = not value_counts.empty and value_counts.eq(2).all()
        print(f"  - Verification (all have 1 duplicate): {'PASS' if is_valid else 'FAIL'}")

async def ensure_indexes(pool):
    """Creates necessary indexes for performance if they do not already exist."""
    logger.info("Ensuring critical indexes exist...")
    async with pool.acquire() as conn:
        # Index for markets_3 time filtering
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{FORK_MARKET_TABLE}_time_cycle 
            ON {FORK_MARKET_TABLE} (found_time_ms, exhaustion_cycle);
        """)
        
        # Indexes for event tables (critical for time and event_name filtering)
        event_tables = [
            'events_conditional_tokens', 'events_ctf_exchange', 
            'events_neg_risk_exchange', 'OOV2', 'events_managed_oracle'
        ]
        
        for table in event_tables:
            # Composite index for the most common WHERE clauses
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table}_filter_composite 
                ON {table} (event_name, contract_address, timestamp_ms);
            """)
            # Index for the timestamp filter (used by build_time_clause)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table}_timestamp 
                ON {table} (timestamp_ms);
            """)
    logger.info("Index check complete.")

async def fetch_request_prices(pool, time_clause, time_params):
    """
    Fetches RequestPrice events from OOV2 and events_managed_oracle, 
    extracts the market_id from the ancillaryData using a CTE and regex.
    """
    logger.info("Fetching RequestPrice events and extracting market_id using CTE...")
    
    # The market_id is embedded as a string in the ancillaryData (part of the 'data' column).
    query = f"""
        WITH RequestEvents AS (
            -- Combine RequestPrice events from both tables
            SELECT
                transaction_hash,
                block_number,
                timestamp_ms,
                contract_address,
                topics[2] AS uma_identifier,
                data
            FROM OOV2
            WHERE event_name = 'RequestPrice'
            UNION ALL
            SELECT
                transaction_hash,
                block_number,
                timestamp_ms,
                contract_address,
                topics[2] AS uma_identifier,
                data
            FROM events_managed_oracle
            WHERE event_name = 'RequestPrice'
        )
        SELECT
            t.transaction_hash,
            t.block_number,
            t.timestamp_ms,
            t.contract_address,
            t.uma_identifier,
            -- Extract the market_id (a string of digits) from the ancillaryData in the 'data' field
            (regexp_match(
                -- Decode the hex string (starting after '0x') to bytea, then convert to UTF-8 text
                decode(substring(t.data from 3), 'hex'),
                'market_id: (\d+)'
            ))[1] AS extracted_market_id
        FROM
            RequestEvents AS t
        WHERE
            -- Apply time filter here. The new indexes on timestamp_ms will speed up the CTE.
            t.timestamp_ms IS NOT NULL {time_clause.replace('timestamp_ms', 't.timestamp_ms')}
            -- Filter out rows where market_id extraction failed
            AND (regexp_match(
                decode(substring(t.data from 3), 'hex'),
                'market_id: (\d+)'
            ))[1] IS NOT NULL
    """
    
    return await fetch_data(pool, query, time_params)


# ----------------------------------------------------------------
# 3. MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Successfully connected to the database.")
    
    # --- CRITICAL PERFORMANCE STEP: Ensure Indexes ---
    await ensure_indexes(pool)

    # --- Step 0: Time Filter Setup ---
    def build_time_clause(column_name, current_params):
        if not FILTER_BY_TIME: return "", current_params
        clauses = []
        new_params = current_params.copy()
        if START_TIME:
            new_params.append(START_TIME)
            clauses.append(f"{column_name} >= ${len(new_params)}")
        if END_TIME:
            new_params.append(END_TIME)
            clauses.append(f"{column_name} <= ${len(new_params)}")
        return (" AND " + " AND ".join(clauses) if clauses else ""), new_params

    logger.info("Fetching all required data...")

    # Prepare time clause for all fetches
    t_all, p_all = build_time_clause("timestamp_ms", [])
    
    # 1. Fetch Markets (markets_3)
    logger.info(f"Fetching {FORK_MARKET_TABLE} (exhaustion_cycle > 0)...")
    markets_base = f"SELECT market_id, condition_id, question_id, message ->> 'questionID' as json_question_id FROM {FORK_MARKET_TABLE} WHERE exhaustion_cycle > 0"
    t_mk, p_mk = build_time_clause("found_time_ms", [])
    clob_df = await fetch_data(pool, markets_base + t_mk, p_mk)
    
    # 2. Fetch RequestPrice Events (NEW STEP)
    request_price_df = await fetch_request_prices(pool, t_all, p_all)
    
    # --- Step 1: Match RequestPrice with Orderbooks (NEW STEP) ---
    logger.info("Matching RequestPrice events with Orderbooks...")
    
    request_price_df.rename(columns={'extracted_market_id': 'market_id'}, inplace=True)
    
    matched_df = pd.merge(
        clob_df, 
        request_price_df, 
        on='market_id', 
        how='inner', 
        suffixes=('_market', '_request')
    )
    
    # --- Step 2: Analysis of Matched Data (NEW STEP) ---
    print("\n" + "="*80)
    print("REQUEST PRICE TO ORDERBOOK MATCHING ANALYSIS")
    print("="*80)
    
    total_requests = len(request_price_df)
    total_markets = len(clob_df)
    total_matches = len(matched_df)
    
    print(f"- Total RequestPrice Events Fetched: {total_requests:,}")
    print(f"- Total Orderbook Markets Fetched:   {total_markets:,}")
    print(f"- Total Matches (on market_id):      {total_matches:,}")
    
    if total_requests > 0:
        match_rate = (total_matches / total_requests) * 100
        print(f"- Match Rate (Requests -> Markets):  {match_rate:.2f}%")

    analyze_and_print_stats("Matched Markets (market_id)", matched_df, 'market_id')
    
    if not matched_df.empty:
        print("\nSample Matched Data (Market ID, Market Question ID, Request Tx Hash):")
        print(matched_df[['market_id', 'question_id', 'transaction_hash']].head())

    # 3. Fetch Ground Truth (CSV)
    try:
        logger.info(f"Loading CSV: {GAMMA_MARKETS_FILE}...")
        ground_orderbooks_df = pd.read_csv(GAMMA_MARKETS_FILE, usecols=['conditionId', 'questionID'])
        ground_orderbooks_df.rename(columns={'questionID': 'question_id', 'conditionId': 'condition_id'}, inplace=True)
        logger.info(f"Loaded {len(ground_orderbooks_df):,} records from CSV")
    except FileNotFoundError:
        logger.error("Ground truth file not found.")
        ground_orderbooks_df = pd.DataFrame()

    # 4. Fetch Condition Preparation
    logger.info("Fetching ConditionPreparation events...")
    cond_prep_base = """
        SELECT topics[2] as condition_id, '0x' || substring(topics[3] from 27) as oracle, topics[4] as question_id 
        FROM events_conditional_tokens 
        WHERE event_name = 'ConditionPreparation' 
        AND LOWER(contract_address) = $1
    """
    t_cp, p_cp = build_time_clause("timestamp_ms", [ADDR['conditional_tokens']])
    cond_prep_df = await fetch_data(pool, cond_prep_base + t_cp, p_cp)

    # 5. Fetch Exchange Events
    logger.info("Fetching Exchange TokenRegistered events...")
    async def fetch_exchange(table, addr):
        base = f"""
            SELECT topics[4] as condition_id 
            FROM {table} 
            WHERE event_name = 'TokenRegistered' 
            AND LOWER(contract_address) = $1
        """
        t, p = build_time_clause("timestamp_ms", [addr])
        return await fetch_data(pool, base + t, p)

    exchange_ctfe_df = await fetch_exchange('events_ctf_exchange', ADDR['ctfe_exchange'])
    exchange_negrisk_df = await fetch_exchange('events_neg_risk_exchange', ADDR['negrisk_exchange'])

    logger.info("Data fetched. Starting analysis...")

    # --- Step 3: Partition Data (Original Step 2) ---
    prep_partitions = {oracle: group for oracle, group in cond_prep_df.groupby('oracle')}
    exchange_partitions = {'ctfe_exchange': exchange_ctfe_df, 'negrisk_exchange': exchange_negrisk_df}

    # --- Step 4: Analysis (Original Step 3) ---
    print("\n" + "="*80)
    print("INITIAL ANALYSIS: UNIQUE AND DUPLICATE COUNTS")
    print("="*80)

    print("\n[A] Condition ID Counts")
    analyze_and_print_stats("Orderbook (DB)", clob_df, 'condition_id')
    analyze_and_print_stats("Ground Orderbook (CSV)", ground_orderbooks_df, 'condition_id')
    for name, df in exchange_partitions.items():
        analyze_and_print_stats(name, df, 'condition_id')
    for oracle, df in prep_partitions.items():
        analyze_and_print_stats(f"Prep (Oracle: {oracle[:10]}...)", df, 'condition_id')

    print("\n[B] Question ID Counts")
    analyze_and_print_stats("Orderbook (DB)", clob_df, 'question_id')
    analyze_and_print_stats("Ground Orderbook (CSV)", ground_orderbooks_df, 'question_id')
    for oracle, df in prep_partitions.items():
        analyze_and_print_stats(f"Prep (Oracle: {oracle[:10]}...)", df, 'question_id')

    # --- Step 5: Intersections (Original Step 4) ---
    logger.info("Calculating intersections (this may take a moment)...")
    
    # Helper to safely convert to set
    def to_set(series):
        return set(series.dropna().astype(str).str.lower())

    clob_q_ids = to_set(clob_df['question_id'])
    clob_c_ids = to_set(clob_df['condition_id'])
    ground_q_ids = to_set(ground_orderbooks_df['question_id'])
    ground_c_ids = to_set(ground_orderbooks_df['condition_id'])

    prep_q_id_sets = {oracle: to_set(df['question_id']) for oracle, df in prep_partitions.items()}
    prep_c_id_sets = {oracle: to_set(df['condition_id']) for oracle, df in prep_partitions.items()}
    exchange_c_id_sets = {name: to_set(df['condition_id']) for name, df in exchange_partitions.items()}

    print("\n" + "="*80)
    print("ORDERBOOK SOURCE COMPARISON")
    print("="*80)
    print(f"- DB Orderbook ∩ Ground (CSV) [Question IDs]: {len(clob_q_ids.intersection(ground_q_ids)):,}")
    print(f"- DB Orderbook ∩ Ground (CSV) [Condition IDs]: {len(clob_c_ids.intersection(ground_c_ids)):,}")

    orderbook_sources = {
        "Orderbook (DB)": (clob_q_ids, clob_c_ids),
        "Ground Orderbook (CSV)": (ground_q_ids, ground_c_ids),
    }

    for source_name, (q_ids, c_ids) in orderbook_sources.items():
        print("\n" + "="*80)
        print(f"INTERSECTION ANALYSIS FOR: {source_name.upper()}")
        print("="*80)

        print("\n[A] Shared Unique Question IDs (vs Preparation):")
        for oracle, q_id_set in prep_q_id_sets.items():
            print(f"- {source_name} ∩ Prep (Oracle: {oracle[:10]}...): {len(q_ids.intersection(q_id_set)):,} shared")

        print("\n[B] Shared Unique Condition IDs:")
        print(f"\n  [B.1] {source_name} ∩ ConditionPreparation Partitions:")
        for oracle, c_id_set in prep_c_id_sets.items():
            print(f"  - {source_name} ∩ Prep (Oracle: {oracle[:10]}...): {len(c_ids.intersection(c_id_set)):,} shared")
        
        print(f"\n  [B.2] {source_name} ∩ Exchange Partitions:")
        for name, c_id_set in exchange_c_id_sets.items():
            print(f"  - {source_name} ∩ {name}: {len(c_ids.intersection(c_id_set)):,} shared")
        
        if source_name == "Orderbook (DB)":
            print("\n  [B.3] Exchange ∩ Preparation Partitions:")
            for ex_name, ex_set in exchange_c_id_sets.items():
                for oracle, prep_set in prep_c_id_sets.items():
                    print(f"  - {ex_name} ∩ Prep (Oracle: {oracle[:10]}...): {len(ex_set.intersection(prep_set)):,} shared")

        print(f"\n  [B.4] Three-Way Intersections ({source_name} ∩ Exchange ∩ Preparation):")
        for ex_name, ex_set in exchange_c_id_sets.items():
            for oracle, prep_set in prep_c_id_sets.items():
                print(f"  - {source_name} ∩ {ex_name} ∩ Prep (Oracle: {oracle[:10]}...): {len(c_ids.intersection(ex_set).intersection(prep_set)):,} shared")

    await pool.close()
    logger.info("Analysis complete.")

if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials not set. Please set PG_SOCKET, POLY_PG_PORT, POLY_DB, POLY_DB_CLI, and POLY_DB_CLI_PASS.")
    else:
        asyncio.run(main())