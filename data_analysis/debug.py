import os
import asyncio
import asyncpg
import logging

# --- Setup: Same as before ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")
FORK_MARKET_TABLE = "markets_2"
ADDR = {
    'conditional_tokens': "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower(),
    'ctfe_exchange': "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e".lower(),
    'uma_adapter': "0x65070BE91477460D8A7AeEb94ef92fe056C2f2A7".lower() # MOO UMA
}
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

async def main():
    """Fetches and prints a small sample of raw IDs for debugging."""
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Connected to DB. Fetching samples...")

    # --- Get Time Range ---
    time_range_query = f"SELECT MIN(found_time_ms) as min_ts, MAX(found_time_ms) as max_ts FROM {FORK_MARKET_TABLE} WHERE exhaustion_cycle != 0"
    time_range = await pool.fetchrow(time_range_query)
    min_ts, max_ts = time_range['min_ts'], time_range['max_ts']

    # --- 1. Sample ConditionPreparation IDs ---
    cp_query = "SELECT topics[1] as condition_id, topics[3] as question_id FROM events_conditional_tokens WHERE event_name = 'ConditionPreparation' AND LOWER(contract_address) = $1 AND timestamp_ms BETWEEN $2 AND $3 LIMIT 5"
    cp_rows = await pool.fetch(cp_query, ADDR['conditional_tokens'], min_ts, max_ts)
    
    print("\n--- [1] Sample ConditionPreparation IDs ---")
    for row in cp_rows:
        print(f"  ConditionID: {row['condition_id']} (Type: {type(row['condition_id'])})")
        print(f"  QuestionID:  {row['question_id']} (Type: {type(row['question_id'])})")

    # --- 2. Sample TokenRegistered conditionId ---
    tr_query = "SELECT topics[3] as condition_id FROM events_ctf_exchange WHERE event_name = 'TokenRegistered' AND LOWER(contract_address) = $1 AND timestamp_ms BETWEEN $2 AND $3 LIMIT 5"
    tr_rows = await pool.fetch(tr_query, ADDR['ctfe_exchange'], min_ts, max_ts)

    print("\n--- [2] Sample TokenRegistered conditionId ---")
    for row in tr_rows:
        print(f"  ConditionID: {row['condition_id']} (Type: {type(row['condition_id'])})")

    # --- 3. Sample QuestionInitialized questionId ---
    qi_query = "SELECT topics[1] as question_id FROM events_uma_adapter WHERE event_name = 'QuestionInitialized' AND LOWER(contract_address) = $1 AND timestamp_ms BETWEEN $2 AND $3 LIMIT 5"
    qi_rows = await pool.fetch(qi_query, ADDR['uma_adapter'], min_ts, max_ts)

    print("\n--- [3] Sample QuestionInitialized questionId ---")
    for row in qi_rows:
        print(f"  QuestionID: {row['question_id']} (Type: {type(row['question_id'])})")

    print("\nPlease share this output. It will reveal the exact reason the matching is failing.")
    await pool.close()

if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials are not set.")
    else:
        asyncio.run(main())