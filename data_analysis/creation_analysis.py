import os
import asyncio
import asyncpg
import pandas as pd
import logging
from pathlib import Path

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

# --- File Paths ---

CSV_DIR = os.getenv('POLY_CSV', '.')
GAMMA_MARKETS_FILE = Path(CSV_DIR) / 'gamma_markets.csv'

# --- Table & Contract Config ---

FORK_MARKET_TABLE = "markets_5"
ADDR = {
    'negrisk_exchange': "0xc5d563a36ae78145c45a50134d48a1215220f80a".lower(),
    'ctfe_exchange': "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e".lower(),
    'conditional_tokens': "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower(),
}

# --- Time Filtering Config ---

FILTER_BY_TIME = True
START_TIME = None  # If None and FILTER_BY_TIME is True, defaults to MIN(found_time_ms) in markets_3
END_TIME = 1766673099000    # If None, no upper limit

# ----------------------------------------------------------------
# 2. HELPER FUNCTIONS
# ----------------------------------------------------------------

async def fetch_data(pool, query, params=[]):
    """Helper to fetch data and return a DataFrame."""
    try:
        async with pool.acquire() as connection:
            rows = await connection.fetch(query, *params)
            return pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"SQL Error: {e}. Please check your query and table schema.")
        return pd.DataFrame()

def analyze_and_print_stats(name, df, id_column):
    """Analyzes a DataFrame column for unique and duplicate counts and prints the results."""
    if df.empty or id_column not in df.columns:
        print(f"- {name:<30}: 0 total, 0 unique, 0 duplicates")
        return

    raw_list = df[id_column].dropna().astype(str).str.lower().tolist()
    unique_set = set(raw_list)
    total_count = len(raw_list)
    unique_count = len(unique_set)
    duplicate_count = total_count - unique_count

    print(f"- {name:<30}: {total_count:,} total, {unique_count:,} unique, {duplicate_count:,} duplicates")

    if "exchange" in name:
        value_counts = df[id_column].value_counts()
        is_valid = not value_counts.empty and value_counts.eq(2).all()
        print(f"  - Verification (all have 1 duplicate): {'PASS' if is_valid else 'FAIL'}")

# ----------------------------------------------------------------
# 3. MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    """Main function to connect, fetch, partition, analyze, and print results."""
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Successfully connected to the database.")

    # --- Step 0: Determine Time Range ---
    start_ms = START_TIME
    end_ms = END_TIME

    if FILTER_BY_TIME:
        if start_ms is None:
            logger.info(f"Fetching minimum found_time_ms from {FORK_MARKET_TABLE}...")
            async with pool.acquire() as conn:
                start_ms = await conn.fetchval(f"SELECT MIN(found_time_ms) FROM {FORK_MARKET_TABLE}")
            logger.info(f"Start time set to: {start_ms}")
        
        logger.info(f"Time Filter Active: Start={start_ms}, End={end_ms}")
    else:
        logger.info("Time Filter: OFF")

    def build_time_clause(column_name, current_params):
        """Helper to append time filtering SQL and update params."""
        if not FILTER_BY_TIME:
            return "", current_params
        
        clauses = []
        new_params = current_params.copy()
        
        if start_ms is not None:
            new_params.append(start_ms)
            clauses.append(f"{column_name} >= ${len(new_params)}")
        
        if end_ms is not None:
            new_params.append(end_ms)
            clauses.append(f"{column_name} <= ${len(new_params)}")
            
        if not clauses:
            return "", new_params
            
        return " AND " + " AND ".join(clauses), new_params

    # --- Step 1: Fetch All Data ---
    logger.info("Fetching all required data from database and CSV...")

    # 1. Fetch Markets (markets_3)
    # Uses 'found_time_ms' for filtering
    markets_base_query = f"SELECT condition_id, message ->> 'questionID' as question_id FROM {FORK_MARKET_TABLE} WHERE exhaustion_cycle != 0"
    markets_time_clause, markets_params = build_time_clause("found_time_ms", [])
    
    clob_df = await fetch_data(pool, markets_base_query + markets_time_clause, markets_params)
    
    # 2. Fetch Ground Truth (CSV) - NO TIME FILTER
    try:
        ground_orderbooks_df = pd.read_csv(GAMMA_MARKETS_FILE, usecols=['conditionId', 'questionID'])
        ground_orderbooks_df.rename(columns={'questionID': 'question_id', 'conditionId': 'condition_id'}, inplace=True)
        logger.info(f"Successfully loaded {len(ground_orderbooks_df):,} records from {GAMMA_MARKETS_FILE}")
    except FileNotFoundError:
        logger.error(f"Ground truth orderbook file not found at '{GAMMA_MARKETS_FILE}'. Skipping its analysis.")
        ground_orderbooks_df = pd.DataFrame()

    # 3. Fetch Condition Preparation Events
    # Uses 'timestamp_ms' for filtering
    cond_prep_base = "SELECT topics[2] as condition_id, '0x' || substring(topics[3] from 27) as oracle, topics[4] as question_id FROM events_conditional_tokens WHERE event_name = 'ConditionPreparation' AND LOWER(contract_address) = $1"
    cond_prep_time_clause, cond_prep_params = build_time_clause("timestamp_ms", [ADDR['conditional_tokens']])
    
    cond_prep_df = await fetch_data(pool, cond_prep_base + cond_prep_time_clause, cond_prep_params)

    # 4. Fetch Exchange Registration Events
    # Uses 'timestamp_ms' for filtering
    async def fetch_exchange_data(table_name, address):
        base_query = f"SELECT topics[4] as condition_id FROM {table_name} WHERE event_name = 'TokenRegistered' AND LOWER(contract_address) = $1"
        time_clause, params = build_time_clause("timestamp_ms", [address])
        return await fetch_data(pool, base_query + time_clause, params)

    exchange_ctfe_df = await fetch_exchange_data('events_ctf_exchange', ADDR['ctfe_exchange'])
    exchange_negrisk_df = await fetch_exchange_data('events_neg_risk_exchange', ADDR['negrisk_exchange'])

    logger.info("All data fetched. Starting analysis...")

    # --- Step 2: Partition Data ---
    prep_partitions = {oracle: group for oracle, group in cond_prep_df.groupby('oracle')}
    exchange_partitions = {'ctfe_exchange': exchange_ctfe_df, 'negrisk_exchange': exchange_negrisk_df}

    # --- Step 3: Initial Unique/Duplicate Analysis ---
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

    # --- Step 4: Create Unique Sets for Intersection Analysis ---
    clob_q_ids = set(clob_df['question_id'].dropna().astype(str).str.lower())
    clob_c_ids = set(clob_df['condition_id'].dropna().astype(str).str.lower())

    ground_q_ids = set(ground_orderbooks_df['question_id'].dropna().astype(str).str.lower())
    ground_c_ids = set(ground_orderbooks_df['condition_id'].dropna().astype(str).str.lower())

    prep_q_id_sets = {oracle: set(df['question_id'].dropna().astype(str).str.lower()) for oracle, df in prep_partitions.items()}
    prep_c_id_sets = {oracle: set(df['condition_id'].dropna().astype(str).str.lower()) for oracle, df in prep_partitions.items()}
    exchange_c_id_sets = {name: set(df['condition_id'].dropna().astype(str).str.lower()) for name, df in exchange_partitions.items()}

    # --- Step 5: Compare Orderbook Sources ---
    print("\n" + "="*80)
    print("ORDERBOOK SOURCE COMPARISON")
    print("="*80)
    print("\n--- Question ID Intersections ---")
    print(f"- DB Orderbook ∩ Ground (CSV): {len(clob_q_ids.intersection(ground_q_ids)):,}")

    print("\n--- Condition ID Intersections ---")
    print(f"- DB Orderbook ∩ Ground (CSV): {len(clob_c_ids.intersection(ground_c_ids)):,}")

    # --- Step 6: Run Intersection Analysis for each Orderbook Source ---
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
        
        # Note: This section is independent of the orderbook source, so we only need to print it once.
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
    logger.info("Analysis complete. Database connection closed.")

if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials are not set. Please set PG_SOCKET, POLY_PG_PORT, etc.")
    else:
        asyncio.run(main())