import os
import asyncio
import asyncpg
import pandas as pd
import logging

# ----------------------------------------------------------------
# PREREQUISITE:
# pip install pandas "asyncpg<0.29.0"
# ----------------------------------------------------------------

# --- Set your database credentials as environment variables ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- Configuration ---
FORK_MARKET_TABLE = "markets_2"

# --- Contract Addresses (lowercase for reliable matching) ---
ADDR = {
    'negrisk_exchange': "0xc5d563a36ae78145c45a50134d48a1215220f80a".lower(),
    'ctfe_exchange': "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e".lower(),
    'conditional_tokens': "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower(),
    'neg_uma': "0x2f5e3684cb1f318ec51b00edba38d79ac2c0aa9d".lower(),
    'moo_uma': "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7".lower(),
    'negrisk_adapt': "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296".lower()
}

# --- Setup basic logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()


async def fetch_data(pool, query, params=[]):
    """Helper to fetch data and return a DataFrame."""
    try:
        rows = await pool.fetch(query, *params)
        return pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"SQL Error: {e}. Please check your query and table schema.")
        return pd.DataFrame()


async def main():
    """Main function to connect, fetch, analyze, and print results."""
    # Use the specified timestamp range
    min_ts = 1764053596065
    max_ts = 1765267123725
    
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Successfully connected to the database.")

    logger.info(f"Using specified time range: {pd.to_datetime(min_ts, unit='ms', utc=True)} to {pd.to_datetime(max_ts, unit='ms', utc=True)}")

    # --- Step 2: Fetch All Data ---
    cond_prep_query = "SELECT topics[2] as condition_id, topics[3] as oracle_topic, topics[4] as question_id FROM events_conditional_tokens WHERE event_name = 'ConditionPreparation' AND LOWER(contract_address) = $1 AND timestamp_ms BETWEEN $2 AND $3"
    ctf_df = await fetch_data(pool, cond_prep_query, [ADDR['conditional_tokens'], min_ts, max_ts])
    logger.info(f"Fetched {len(ctf_df)} 'ctf' (ConditionPreparation) events.")

    token_reg_query = "SELECT topics[4] as condition_id FROM {table} WHERE event_name = 'TokenRegistered' AND LOWER(contract_address) = $1 AND timestamp_ms BETWEEN $2 AND $3"
    negrisk_ctfe_df = await fetch_data(pool, token_reg_query.format(table='events_neg_risk_exchange'), [ADDR['negrisk_exchange'], min_ts, max_ts])
    logger.info(f"Fetched {len(negrisk_ctfe_df)} 'negrisk_ctfe' (TokenRegistered) events.")
    base_ctfe_df = await fetch_data(pool, token_reg_query.format(table='events_ctf_exchange'), [ADDR['ctfe_exchange'], min_ts, max_ts])
    logger.info(f"Fetched {len(base_ctfe_df)} 'base_ctfe' (TokenRegistered) events.")

    negrisk_question_query = "SELECT topics[3] as question_id FROM events_neg_risk_adapter WHERE event_name = 'QuestionPrepared' AND LOWER(contract_address) = $1 AND timestamp_ms BETWEEN $2 AND $3"
    negrisk_adapter_df = await fetch_data(pool, negrisk_question_query, [ADDR['negrisk_adapt'], min_ts, max_ts])
    logger.info(f"Fetched {len(negrisk_adapter_df)} 'negrisk_adapter' (QuestionPrepared) events.")

    moo_question_query = "SELECT topics[2] as question_id FROM events_uma_adapter WHERE event_name = 'QuestionInitialized' AND LOWER(contract_address) = $1 AND timestamp_ms BETWEEN $2 AND $3"
    moo_uma_df = await fetch_data(pool, moo_question_query, [ADDR['moo_uma'], min_ts, max_ts])
    logger.info(f"Fetched {len(moo_uma_df)} 'moo_uma' (QuestionInitialized) events.")
    neg_uma_df = await fetch_data(pool, moo_question_query, [ADDR['neg_uma'], min_ts, max_ts])
    logger.info(f"Fetched {len(neg_uma_df)} 'negrisk_uma' (QuestionInitialized) events.")

    markets_query = f"SELECT condition_id, message ->> 'questionID' as question_id FROM {FORK_MARKET_TABLE} WHERE found_time_ms BETWEEN $1 AND $2 AND exhaustion_cycle != 0"
    clob_df = await fetch_data(pool, markets_query, [min_ts, max_ts])
    logger.info(f"Fetched {len(clob_df)} 'clob' (orderbook) entries.")

    await pool.close()
    logger.info("Database connection closed. Starting analysis...")

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

    # F) Bridge Sets (clob vs ctf) - MODIFIED
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
    print(f"6) only on-chain (ctf \\ clob): {len(ctf_conditions - clob_conditions):,}")
    print("--- QUESTIONS ---")
    print(f"7) shared (clob ^ ctf): {len(shared_question_bridges):,}")
    print(f"8) only in orderbook (clob \\ ctf): {len(clob_questions - ctf_questions):,}")
    print(f"9) only on-chain (ctf \\ clob): {len(ctf_questions - clob_questions):,}")

    # G) No Bridge
    combined_condition_bridges = clob_conditions.union(ctf_conditions)
    combined_question_bridges = clob_questions.union(ctf_questions)
    print("\n--- [G] Items Not Found in Bridges ---")
    print("--- base_ctfe (conditions) ---")
    print(f"  \\ combined_condition_bridges: {len(base_ctfe_conditions - combined_condition_bridges):,}")
    print(f"  \\ clob_conditions: {len(base_ctfe_conditions - clob_conditions):,}")
    print(f"  \\ ctf_conditions: {len(base_ctfe_conditions - ctf_conditions):,}")
    print("--- negrisk_ctfe (conditions) ---")
    print(f"  \\ combined_condition_bridges: {len(negrisk_ctfe_conditions - combined_condition_bridges):,}")
    print(f"  \\ clob_conditions: {len(negrisk_ctfe_conditions - clob_conditions):,}")
    print(f"  \\ ctf_conditions: {len(negrisk_ctfe_conditions - ctf_conditions):,}")
    print("--- negrisk_uma (questions) ---")
    print(f"  \\ combined_question_bridges: {len(negrisk_uma_questions - combined_question_bridges):,}")
    print(f"  \\ clob_questions: {len(negrisk_uma_questions - clob_questions):,}")
    print(f"  \\ ctf_questions: {len(negrisk_uma_questions - ctf_questions):,}")
    print("--- negrisk_adapter (questions) ---")
    print(f"  \\ combined_question_bridges: {len(negrisk_adapter_questions - combined_question_bridges):,}")
    print(f"  \\ clob_questions: {len(negrisk_adapter_questions - clob_questions):,}")
    print(f"  \\ ctf_questions: {len(negrisk_adapter_questions - ctf_questions):,}")
    print("--- moo_uma (questions) ---")
    print(f"  \\ combined_question_bridges: {len(moo_uma_questions - combined_question_bridges):,}")
    print(f"  \\ clob_questions: {len(moo_uma_questions - clob_questions):,}")
    print(f"  \\ ctf_questions: {len(moo_uma_questions - ctf_questions):,}")

    # H) Unregistered / Missing - MODIFIED
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

    # I) Off-chain vs On-chain - MODIFIED
    print("\n--- [I] Off-chain vs On-chain Analysis ---")
    # 1. Orderbooks for registered tokens not prepared on-chain
    base_tokens_not_in_ctf = base_ctfe_conditions - ctf_conditions
    negrisk_tokens_not_in_ctf = negrisk_ctfe_conditions - ctf_conditions
    print(f"1) Orderbooks for 'base_ctfe' tokens not prepared on-chain: {len(clob_conditions.intersection(base_tokens_not_in_ctf)):,}")
    print(f"2) Orderbooks for 'negrisk_ctfe' tokens not prepared on-chain: {len(clob_conditions.intersection(negrisk_tokens_not_in_ctf)):,}")

    # 2. Conditions prepared on-chain but not registered
    conditions_not_in_tokens = ctf_conditions - combined_ctfe
    print(f"3) Conditions prepared on-chain but not registered as tokens: {len(conditions_not_in_tokens):,}")

    # 3. Orderbooks for oracle questions not prepared on-chain
    oracles_not_in_ctf = combined_main_oracles - ctf_questions
    print(f"4) Orderbooks for questions from oracles but not prepared on-chain: {len(clob_questions.intersection(oracles_not_in_ctf)):,}")

    # 4. Conditions prepared for questions not from main oracles
    ctf_not_in_oracles = ctf_questions - combined_main_oracles
    print(f"5) Conditions prepared on-chain for questions not from main oracles: {len(ctf_not_in_oracles):,}")

    # 5. NEW: For conditions prepared on-chain but NOT in orderbooks, how many have a matching oracle?
    onchain_not_in_clob = ctf_conditions - clob_conditions
    # Filter these by registration type
    onchain_not_in_clob_base = onchain_not_in_clob.intersection(base_ctfe_conditions)
    onchain_not_in_clob_negrisk = onchain_not_in_clob.intersection(negrisk_ctfe_conditions)
    # Get the question IDs for these subsets from the original ctf dataframe
    questions_for_base_subset = set(ctf_df[ctf_df['condition_id'].isin(onchain_not_in_clob_base)]['question_id'])
    questions_for_negrisk_subset = set(ctf_df[ctf_df['condition_id'].isin(onchain_not_in_clob_negrisk)]['question_id'])
    # Check how many of those question IDs are in the main oracle set
    base_with_matching_oracle = len(questions_for_base_subset.intersection(combined_main_oracles))
    negrisk_with_matching_oracle = len(questions_for_negrisk_subset.intersection(combined_main_oracles))
    print(f"6) Conditions on-chain but not in orderbook ('base_ctfe' tokens): {len(onchain_not_in_clob_base):,}")
    print(f"   - Of these, count with a matching oracle entry: {base_with_matching_oracle:,}")
    print(f"7) Conditions on-chain but not in orderbook ('negrisk_ctfe' tokens): {len(onchain_not_in_clob_negrisk):,}")
    print(f"   - Of these, count with a matching oracle entry: {negrisk_with_matching_oracle:,}")

    print("\n" + "="*60)


if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials are not set. Please set PG_SOCKET, POLY_PG_PORT, POLY_DB, POLY_DB_CLI, and POLY_DB_CLI_PASS environment variables.")
    else:
        asyncio.run(main())