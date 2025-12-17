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
LABELS_FILE = Path(CSV_DIR) / 'labels.csv'

# --- Table & Contract Config ---
FORK_MARKET_TABLE = "markets_2"
ADDR = {
    'negrisk_exchange': "0xc5d563a36ae78145c45a50134d48a1215220f80a".lower(),
    'ctfe_exchange': "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e".lower(),
    'conditional_tokens': "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower(),
    'neg_uma': "0x2f5e3684cb1f318ec51b00edba38d79ac2c0aa9d".lower(),
    'moo_uma': "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7".lower(),
    'negrisk_adapt': "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296".lower()
}

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

def load_labels(file_path):
    """Loads labels from the specified CSV file."""
    if not file_path.exists():
        logger.error(f"Labels file not found at '{file_path}'. Cannot perform semantic analysis.")
        return None
    df = pd.read_csv(file_path)
    labels = df['representative_label'].tolist()
    logger.info(f"Loaded {len(labels)} labels for semantic analysis.")
    return labels

# ----------------------------------------------------------------
# 3. SEMANTIC ANALYSIS FUNCTION
# ----------------------------------------------------------------

async def analyze_semantics(pool, labels):
    """Fetches semantic data and prints statistical summaries."""
    print("\n" + "="*60)
    print("SEMANTIC ANALYSIS SUMMARY")
    print("="*60)

    semantic_categories = {
        "CLOB but not CTF Questions": "semantic_clob_not_ctf",
        "Oracles but not CLOB Questions": "semantic_oracles_not_clob",
        "Shared Bridge (CLOB and CTF) Questions": "semantic_shared_bridge"
    }

    for category_name, table_name in semantic_categories.items():
        # Check if the table exists before querying
        table_exists_query = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1);"
        exists = await pool.fetchval(table_exists_query, table_name)
        
        if not exists:
            logger.warning(f"Semantic table '{table_name}' not found. Skipping analysis for '{category_name}'.")
            continue

        label_cols = ", ".join([f'"{f"label_{i}"}"' for i in range(len(labels))])
        query = f"SELECT {label_cols} FROM {table_name};"
        
        df = await fetch_data(pool, query)

        if df.empty:
            logger.warning(f"No data found in semantic table '{table_name}'. Skipping.")
            continue

        print(f"\n--- Analysis for '{category_name}' ({len(df):,} questions) ---")

        # 1. Mean Vector Calculation
        mean_vector = df.mean()
        print("\nMean Label Vector:")
        for i, label in enumerate(labels):
            col_name = f"label_{i}"
            print(f"  - {label:<12}: {mean_vector[col_name]:.4f}")

        # 2. Percentiles Calculation
        percentiles = df.quantile([0.01, 0.30, 0.60, 0.99])
        print("\nLabel Percentiles:")
        for i, label in enumerate(labels):
            col_name = f"label_{i}"
            p_values = percentiles[col_name]
            print(f"  {label}:")
            print(f"    - 1st percentile:  {p_values[0.01]:.4f}")
            print(f"    - 30th percentile: {p_values[0.30]:.4f}")
            print(f"    - 60th percentile: {p_values[0.60]:.4f}")
            print(f"    - 99th percentile: {p_values[0.99]:.4f}")

# ----------------------------------------------------------------
# 4. MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    """Main function to connect, fetch, analyze, and print results."""
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Successfully connected to the database.")

    # --- Step 1: Get Time Range ---
    time_range_query = f"SELECT MIN(found_time_ms) as min_ts, MAX(found_time_ms) as max_ts FROM {FORK_MARKET_TABLE} WHERE exhaustion_cycle != 0"
    time_range = await pool.fetchrow(time_range_query)
    min_ts, max_ts = time_range['min_ts'], time_range['max_ts']
    logger.info(f"Time range for Fork 2 established: {pd.to_datetime(min_ts, unit='ms', utc=True)} to {pd.to_datetime(max_ts, unit='ms', utc=True)}")

    # --- Step 2: Fetch All Data ---
    cond_prep_query = "SELECT topics[2] as condition_id, topics[3] as oracle_topic, topics[4] as question_id FROM events_conditional_tokens WHERE event_name = 'ConditionPreparation' AND LOWER(contract_address) = $1 AND timestamp_ms BETWEEN $2 AND $3"
    ctf_df = await fetch_data(pool, cond_prep_query, [ADDR['conditional_tokens'], min_ts, max_ts])
    
    token_reg_query = "SELECT topics[4] as condition_id FROM {table} WHERE event_name = 'TokenRegistered' AND LOWER(contract_address) = $1 AND timestamp_ms BETWEEN $2 AND $3"
    negrisk_ctfe_df = await fetch_data(pool, token_reg_query.format(table='events_neg_risk_exchange'), [ADDR['negrisk_exchange'], min_ts, max_ts])
    base_ctfe_df = await fetch_data(pool, token_reg_query.format(table='events_ctf_exchange'), [ADDR['ctfe_exchange'], min_ts, max_ts])
    
    negrisk_question_query = "SELECT topics[3] as question_id FROM events_neg_risk_adapter WHERE event_name = 'QuestionPrepared' AND LOWER(contract_address) = $1 AND timestamp_ms BETWEEN $2 AND $3"
    negrisk_adapter_df = await fetch_data(pool, negrisk_question_query, [ADDR['negrisk_adapt'], min_ts, max_ts])
    
    moo_question_query = "SELECT topics[2] as question_id FROM events_uma_adapter WHERE event_name = 'QuestionInitialized' AND LOWER(contract_address) = $1 AND timestamp_ms BETWEEN $2 AND $3"
    moo_uma_df = await fetch_data(pool, moo_question_query, [ADDR['moo_uma'], min_ts, max_ts])
    neg_uma_df = await fetch_data(pool, moo_question_query, [ADDR['neg_uma'], min_ts, max_ts])
    
    markets_query = f"SELECT condition_id, message ->> 'questionID' as question_id FROM {FORK_MARKET_TABLE} WHERE exhaustion_cycle != 0"
    clob_df = await fetch_data(pool, markets_query)
    
    logger.info("All data fetched. Starting analysis...")

    # --- Step 3: Create Normalized Sets ---
    def get_lists_and_sets(series):
        if series.empty: return [], set()
        raw_list = series.dropna().astype(str).str.lower().str.strip().tolist()
        unique_set = set(raw_list)
        return raw_list, unique_set

    def get_paired_sets(df, col1, col2):
        if df.empty: return set()
        temp_df = df[[col1, col2]].dropna()
        return set(zip(
            temp_df[col1].astype(str).str.lower().str.strip(),
            temp_df[col2].astype(str).str.lower().str.strip()
        ))

    clob_raw_conditions, clob_conditions = get_lists_and_sets(clob_df['condition_id'])
    clob_raw_questions, clob_questions = get_lists_and_sets(clob_df['question_id'])
    clob_pairs = get_paired_sets(clob_df, 'condition_id', 'question_id')

    ctf_raw_conditions, ctf_conditions = get_lists_and_sets(ctf_df['condition_id'])
    ctf_raw_questions, ctf_questions = get_lists_and_sets(ctf_df['question_id'])
    ctf_pairs = get_paired_sets(ctf_df, 'condition_id', 'question_id')

    negrisk_ctfe_raw_conditions, negrisk_ctfe_conditions = get_lists_and_sets(negrisk_ctfe_df['condition_id'])
    base_ctfe_raw_conditions, base_ctfe_conditions = get_lists_and_sets(base_ctfe_df['condition_id'])
    negrisk_uma_raw_questions, negrisk_uma_questions = get_lists_and_sets(neg_uma_df['question_id'])
    negrisk_adapter_raw_questions, negrisk_adapter_questions = get_lists_and_sets(negrisk_adapter_df['question_id'])
    moo_uma_raw_questions, moo_uma_questions = get_lists_and_sets(moo_uma_df['question_id'])
    
    # --- Step 4: Perform and Print Analysis ---
    print("\n" + "="*60)
    print("CLEAN ANALYTICAL RESULTS")
    print("="*60)

    # A) Duplicates and Uniques
    print("\n--- [A] Unique and Duplicate Counts ---")
    print("--- Bridge Sets ---")
    print(f"clob (orderbook): {len(clob_conditions):,} unique conditions ({len(clob_raw_conditions) - len(clob_conditions):,} duplicates)")
    print(f"clob (orderbook): {len(clob_questions):,} unique questions ({len(clob_raw_questions) - len(clob_questions):,} duplicates)")
    print(f"ctf (on-chain):   {len(ctf_conditions):,} unique conditions ({len(ctf_raw_conditions) - len(ctf_conditions):,} duplicates)")
    print(f"ctf (on-chain):   {len(ctf_questions):,} unique questions ({len(ctf_raw_questions) - len(ctf_questions):,} duplicates)")
    print("--- Non-Bridge Sets ---")
    print(f"base_ctfe:        {len(base_ctfe_conditions):,} unique conditions ({len(base_ctfe_raw_conditions) - len(base_ctfe_conditions):,} duplicates)")
    print(f"negrisk_ctfe:     {len(negrisk_ctfe_conditions):,} unique conditions ({len(negrisk_ctfe_raw_conditions) - len(negrisk_ctfe_conditions):,} duplicates)")
    print(f"negrisk_uma:      {len(negrisk_uma_questions):,} unique questions ({len(negrisk_uma_raw_questions) - len(negrisk_uma_questions):,} duplicates)")
    print(f"negrisk_adapter:  {len(negrisk_adapter_questions):,} unique questions ({len(negrisk_adapter_raw_questions) - len(negrisk_adapter_questions):,} duplicates)")
    print(f"moo_uma:          {len(moo_uma_questions):,} unique questions ({len(moo_uma_raw_questions) - len(moo_uma_questions):,} duplicates)")

    # B) CTF Oracles
    ctf_df['oracle_address'] = '0x' + ctf_df['oracle_topic'].str[26:]
    oracle_counts = ctf_df['oracle_address'].value_counts()
    print(f"\n--- [B] Oracles in 'ctf' (ConditionPreparation) ---")
    print(f"Found {len(oracle_counts)} unique oracles. Counts per address:")
    for address, count in oracle_counts.items():
        print(f"- {address}: {count:,} events")

    # C) CTFE Token Sets
    combined_ctfe = negrisk_ctfe_conditions.union(base_ctfe_conditions)
    shared_ctfe = negrisk_ctfe_conditions.intersection(base_ctfe_conditions)
    print("\n--- [C] CTFE Token Sets (by condition_id) ---")
    print(f"1) base_ctfe (TokenRegistered): {len(base_ctfe_conditions):,}")
    print(f"2) negrisk_ctfe (TokenRegistered): {len(negrisk_ctfe_conditions):,}")
    print(f"3) shared_ctfe (negrisk_ctfe ^ base_ctfe): {len(shared_ctfe):,}")

    # D) Oracle Question Sets
    combined_main_oracles = moo_uma_questions.union(negrisk_adapter_questions)
    shared_main_oracles = moo_uma_questions.intersection(negrisk_adapter_questions)
    print("\n--- [D] Main Oracle Question Sets ---")
    print(f"1) combined_oracles (moo_uma U negrisk_adapter): {len(combined_main_oracles):,}")
    print(f"2) shared_oracles (moo_uma ^ negrisk_adapter): {len(shared_main_oracles):,}")

    # E) NegRisk Question Sets
    combined_negrisk = negrisk_adapter_questions.union(negrisk_uma_questions)
    shared_negrisk = negrisk_adapter_questions.intersection(negrisk_uma_questions)
    print("\n--- [E] NegRisk Question Sets ---")
    print(f"1) combined_negrisk (negrisk_adapter U negrisk_uma): {len(combined_negrisk):,}")
    print(f"2) shared_negrisk (negrisk_adapter ^ negrisk_uma): {len(shared_negrisk):,}")

    # E.2) Oracle Cross-Reference (NEW SECTION)
    print("\n--- [E.2] Oracle Cross-Reference (Moo vs NegRisk) ---")
    moo_vs_neg_adapt = moo_uma_questions.intersection(negrisk_adapter_questions)
    moo_vs_neg_uma = moo_uma_questions.intersection(negrisk_uma_questions)
    print(f"1) Shared Questions (Moo Oracle ^ NegRisk UMA Adapter): {len(moo_vs_neg_adapt):,}")
    print(f"2) Shared Questions (Moo Oracle ^ NegRisk UMA Oracle):  {len(moo_vs_neg_uma):,}")

    # F) Bridge Sets (clob vs ctf)
    shared_condition_bridges = clob_conditions.intersection(ctf_conditions)
    shared_question_bridges = clob_questions.intersection(ctf_questions)
    shared_pairs = clob_pairs.intersection(ctf_pairs)
    print("\n--- [F] Bridge Sets (clob vs ctf) ---")
    print("--- (condition, question) PAIRS ---")
    print(f"1) shared (clob ^ ctf): {len(shared_pairs):,}")
    print(f"2) only in orderbook (clob \\ ctf): {len(clob_pairs - ctf_pairs):,}")
    print(f"3) only on-chain (ctf \\ clob): {len(ctf_pairs - clob_pairs):,}")
    print("--- CONDITIONS ---")
    print(f"4) shared (clob ^ ctf): {len(shared_condition_bridges):,}")
    print(f"5) only in orderbook (clob \\ ctf): {len(clob_conditions - ctf_conditions):,}")
    print(f"6) only on-chain (ctf \\ clob): {len(ctf_conditions - ctf_conditions):,}")
    print("--- QUESTIONS ---")
    print(f"7) shared (clob ^ ctf): {len(shared_question_bridges):,}")
    print(f"8) only in orderbook (clob \\ ctf): {len(clob_questions - ctf_questions):,}")
    print(f"9) only on-chain (ctf \\ clob): {len(ctf_questions - ctf_questions):,}")

    # G) No Bridge
    print("\n--- [G] Items Not Found in Bridges ---")
    print("--- base_ctfe (conditions) ---")
    print(f"  \\ shared_condition_bridges: {len(base_ctfe_conditions - shared_condition_bridges):,}")
    print(f"  \\ clob_conditions: {len(base_ctfe_conditions - clob_conditions):,}")
    print(f"  \\ ctf_conditions: {len(base_ctfe_conditions - ctf_conditions):,}")
    print("--- negrisk_ctfe (conditions) ---")
    print(f"  \\ shared_condition_bridges: {len(negrisk_ctfe_conditions - shared_condition_bridges):,}")
    print(f"  \\ clob_conditions: {len(negrisk_ctfe_conditions - clob_conditions):,}")
    print(f"  \\ ctf_conditions: {len(negrisk_ctfe_conditions - ctf_conditions):,}")
    print("--- negrisk_uma (questions) ---")
    print(f"  \\ shared_question_bridges: {len(negrisk_uma_questions - shared_question_bridges):,}")
    print(f"  \\ clob_questions: {len(negrisk_uma_questions - clob_questions):,}")
    print(f"  \\ ctf_questions: {len(negrisk_uma_questions - ctf_questions):,}")
    print("--- negrisk_adapter (questions) ---")
    print(f"  \\ shared_question_bridges: {len(negrisk_adapter_questions - shared_question_bridges):,}")
    print(f"  \\ clob_questions: {len(negrisk_adapter_questions - clob_questions):,}")
    print(f"  \\ ctf_questions: {len(negrisk_adapter_questions - ctf_questions):,}")
    print("--- moo_uma (questions) ---")
    print(f"  \\ shared_question_bridges: {len(moo_uma_questions - shared_question_bridges):,}")
    print(f"  \\ clob_questions: {len(moo_uma_questions - clob_questions):,}")
    print(f"  \\ ctf_questions: {len(moo_uma_questions - ctf_questions):,}")

    # H) Unregistered / Missing
    ctf_only_conditions = ctf_conditions - clob_conditions
    clob_only_conditions = clob_conditions - ctf_conditions
    ctf_only_questions = ctf_questions - clob_questions
    clob_only_questions = clob_questions - ctf_questions
    print("\n--- [H] Unregistered Conditions / Missing Oracles ---")
    print("--- Unregistered Conditions (Bridge conditions not in any token registration) ---")
    print(f"1) From intersection (ctf ^ clob): {len(shared_condition_bridges - combined_ctfe):,}")
    print(f"2) From only on-chain (ctf \\ clob): {len(ctf_only_conditions - combined_ctfe):,}")
    print(f"3) From only orderbook (clob \\ ctf): {len(clob_only_conditions - combined_ctfe):,}")
    print("--- Missing Oracles (Bridge questions not from a main oracle) ---")
    print(f"4) From intersection (ctf ^ clob): {len(shared_question_bridges - combined_main_oracles):,}")
    print(f"5) From only on-chain (ctf \\ clob): {len(ctf_only_questions - combined_main_oracles):,}")
    print(f"6) From only orderbook (clob \\ ctf): {len(clob_only_questions - combined_main_oracles):,}")

    # I) Off-chain vs On-chain
    print("\n--- [I] Off-chain vs On-chain Analysis ---")
    base_tokens_not_in_ctf = base_ctfe_conditions - ctf_conditions
    negrisk_tokens_not_in_ctf = negrisk_ctfe_conditions - ctf_conditions
    print(f"1) Orderbooks for 'base_ctfe' tokens not prepared on-chain: {len(clob_conditions.intersection(base_tokens_not_in_ctf)):,}")
    print(f"2) Orderbooks for 'negrisk_ctfe' tokens not prepared on-chain: {len(clob_conditions.intersection(negrisk_tokens_not_in_ctf)):,}")
    conditions_not_in_tokens = ctf_conditions - combined_ctfe
    print(f"3) Conditions prepared on-chain but not registered as tokens: {len(conditions_not_in_tokens):,}")
    oracles_not_in_ctf = combined_main_oracles - ctf_questions
    print(f"4) Orderbooks for questions from oracles but not prepared on-chain: {len(clob_questions.intersection(oracles_not_in_ctf)):,}")
    ctf_not_in_oracles = ctf_questions - combined_main_oracles
    print(f"5) Conditions prepared on-chain for questions not from main oracles: {len(ctf_not_in_oracles):,}")
    onchain_not_in_clob = ctf_conditions - clob_conditions
    onchain_not_in_clob_base = onchain_not_in_clob.intersection(base_ctfe_conditions)
    onchain_not_in_clob_negrisk = onchain_not_in_clob.intersection(negrisk_ctfe_conditions)
    questions_for_base_subset = set(ctf_df[ctf_df['condition_id'].isin(onchain_not_in_clob_base)]['question_id'])
    questions_for_negrisk_subset = set(ctf_df[ctf_df['condition_id'].isin(onchain_not_in_clob_negrisk)]['question_id'])
    base_with_matching_oracle = len(questions_for_base_subset.intersection(combined_main_oracles))
    negrisk_with_matching_oracle = len(questions_for_negrisk_subset.intersection(combined_main_oracles))
    print(f"6) Conditions on-chain but not in orderbook ('base_ctfe' tokens): {len(onchain_not_in_clob_base):,}")
    print(f"   - Of these, count with a matching oracle entry: {base_with_matching_oracle:,}")
    print(f"7) Conditions on-chain but not in orderbook ('negrisk_ctfe' tokens): {len(onchain_not_in_clob_negrisk):,}")
    print(f"   - Of these, count with a matching oracle entry: {negrisk_with_matching_oracle:,}")

    # --- [J] NEW: Semantic Analysis Section ---
    labels = load_labels(LABELS_FILE)
    if labels:
        await analyze_semantics(pool, labels)

    await pool.close()
    logger.info("Database connection closed.")


if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials are not set.")
    else:
        asyncio.run(main())