import os
import asyncio
import asyncpg
import pandas as pd
import logging
import re
from pathlib import Path

# --- NLP Imports ---
from sentence_transformers import SentenceTransformer
from sentence_transformers.util import cos_sim
import torch

# ----------------------------------------------------------------
# 1. SETUP & CONFIGURATION
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
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
CONTRACT_CONFIG = {
    'conditional_tokens': {'address': '0x4d97dcd97ec945f40cf65f87097ace5ea0476045'},
    'negrisk_adapt': {'table': 'events_neg_risk_adapter', 'address': '0xd91e80cf2e7be2e162c6513ced06f1dd0da35296'},
    'moo_uma': {'table': 'events_uma_adapter', 'address': '0x65070be91477460d8a7aeeb94ef92fe056c2f2a7'}
}

# --- NLP Model ---
NLP_MODEL_NAME = 'all-MiniLM-L6-v2'

# ----------------------------------------------------------------
# 2. DATA FETCHING & DECODING
# ----------------------------------------------------------------

async def get_question_ids_from_sources(pool):
    """Fetches all unique question IDs from the relevant sources."""
    logger.info("Fetching unique question IDs from all sources...")
    
    clob_q = f"SELECT DISTINCT message ->> 'questionID' as question_id FROM {FORK_MARKET_TABLE} WHERE message ->> 'questionID' IS NOT NULL"
    clob_rows = await pool.fetch(clob_q)
    clob_ids = {r['question_id'].lower() for r in clob_rows}
    logger.info(f"Found {len(clob_ids):,} unique question IDs in CLOB.")

    ctf_q = f"SELECT DISTINCT topics[4] as question_id FROM events_conditional_tokens WHERE event_name = 'ConditionPreparation' AND LOWER(contract_address) = $1"
    ctf_rows = await pool.fetch(ctf_q, CONTRACT_CONFIG['conditional_tokens']['address'])
    ctf_ids = {r['question_id'].lower() for r in ctf_rows}
    logger.info(f"Found {len(ctf_ids):,} unique question IDs in CTF.")

    moo_q = f"SELECT DISTINCT topics[2] as question_id FROM {CONTRACT_CONFIG['moo_uma']['table']} WHERE event_name = 'QuestionInitialized' AND LOWER(contract_address) = $1"
    moo_rows = await pool.fetch(moo_q, CONTRACT_CONFIG['moo_uma']['address'])
    moo_ids = {r['question_id'].lower() for r in moo_rows}
    logger.info(f"Found {len(moo_ids):,} unique question IDs from Moo UMA.")

    neg_q = f"SELECT DISTINCT topics[3] as question_id FROM {CONTRACT_CONFIG['negrisk_adapt']['table']} WHERE event_name = 'QuestionPrepared' AND LOWER(contract_address) = $1"
    neg_rows = await pool.fetch(neg_q, CONTRACT_CONFIG['negrisk_adapt']['address'])
    neg_ids = {r['question_id'].lower() for r in neg_rows}
    logger.info(f"Found {len(neg_ids):,} unique question IDs from Neg Risk Adapter.")
    
    oracle_ids = moo_ids.union(neg_ids)
    logger.info(f"Found {len(oracle_ids):,} total unique question IDs from oracles.")

    return clob_ids, ctf_ids, oracle_ids, moo_ids, neg_ids

def decode_hex_data(hex_string: str) -> str:
    """Decodes a hex string from contract data into a UTF-8 string."""
    if not hex_string or not isinstance(hex_string, str):
        return ""
    hex_string = hex_string.replace('0x', '')
    try:
        byte_data = bytes.fromhex(hex_string)
        return byte_data.decode('utf-8', errors='ignore')
    except (ValueError, TypeError):
        return ""

def parse_neg_risk_question(decoded_text: str) -> str:
    """Extracts the question text from Neg Risk Adapter's ancillary data."""
    match = re.search(r'question:\s*(.*?)\?', decoded_text, re.IGNORECASE)
    return match.group(1).strip() + "?" if match else ""

def parse_moo_uma_question(decoded_text: str) -> str:
    """Extracts the question title from Moo UMA's ancillary data."""
    match = re.search(r'q:\s*title:\s*(.*?)(?:,\s*description:|$)', decoded_text, re.IGNORECASE)
    return match.group(1).strip() if match else ""

async def fetch_question_texts(pool, question_ids, source):
    """Fetches and parses question texts for a given set of IDs and source."""
    if not question_ids:
        return {}
    
    texts = {}
    if source == 'clob':
        query = f"SELECT DISTINCT ON (message ->> 'questionID') message ->> 'questionID' as question_id, message ->> 'question' as text FROM {FORK_MARKET_TABLE} WHERE (message ->> 'questionID') = ANY($1::text[])"
        rows = await pool.fetch(query, list(question_ids))
        texts = {r['question_id'].lower(): r['text'] for r in rows if r['text']}
    
    elif source == 'moo_uma':
        query = f"SELECT topics[2] as question_id, data FROM {CONTRACT_CONFIG['moo_uma']['table']} WHERE event_name = 'QuestionInitialized' AND LOWER(contract_address) = $1 AND topics[2] = ANY($2::text[])"
        rows = await pool.fetch(query, CONTRACT_CONFIG['moo_uma']['address'], list(question_ids))
        for r in rows:
            decoded = decode_hex_data(r['data'])
            parsed_text = parse_moo_uma_question(decoded)
            if parsed_text:
                texts[r['question_id'].lower()] = parsed_text

    elif source == 'negrisk_adapt':
        query = f"SELECT topics[3] as question_id, data FROM {CONTRACT_CONFIG['negrisk_adapt']['table']} WHERE event_name = 'QuestionPrepared' AND LOWER(contract_address) = $1 AND topics[3] = ANY($2::text[])"
        rows = await pool.fetch(query, CONTRACT_CONFIG['negrisk_adapt']['address'], list(question_ids))
        for r in rows:
            decoded = decode_hex_data(r['data'])
            parsed_text = parse_neg_risk_question(decoded)
            if parsed_text:
                texts[r['question_id'].lower()] = parsed_text
                
    return texts

# ----------------------------------------------------------------
# 3. SEMANTIC ANALYSIS CORE
# ----------------------------------------------------------------

def load_labels(file_path):
    """Loads labels from the specified CSV file."""
    if not file_path.exists():
        logger.error(f"Labels file not found at '{file_path}'. Please check the POLY_CSV env var.")
        return None
    df = pd.read_csv(file_path)
    labels = df['representative_label'].tolist()
    logger.info(f"Successfully loaded {len(labels)} labels: {labels}")
    return labels

def analyze_questions(questions, labels, model):
    """
    Performs semantic analysis on a list of questions against a list of labels.
    Returns a list of dictionaries with the question and its scores.
    """
    if not questions or not labels:
        return []

    logger.info(f"Creating semantic embeddings for {len(labels)} labels...")
    label_embeddings = model.encode(labels, convert_to_tensor=True)

    logger.info(f"Creating semantic embeddings for {len(questions)} questions...")
    question_embeddings = model.encode(questions, convert_to_tensor=True, show_progress_bar=True)

    logger.info("Calculating cosine similarity scores...")
    cosine_scores = cos_sim(question_embeddings, label_embeddings)

    results = []
    for i, question in enumerate(questions):
        scores = torch.clamp(cosine_scores[i], 0, 1).tolist()
        results.append({'question_text': question, 'scores': scores})
        
    return results

# ----------------------------------------------------------------
# 4. DATABASE OUTPUT (CORRECTED)
# ----------------------------------------------------------------

async def create_analysis_table(pool, table_name, labels):
    """Creates a table for semantic results if it doesn't exist."""
    label_columns = ", ".join([f'"{f"label_{i}"}" REAL' for i in range(len(labels))])
    # Use IF NOT EXISTS to make the script idempotent and safe to re-run.
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        question_text TEXT UNIQUE,
        {label_columns},
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    """
    logger.info(f"Ensuring table '{table_name}' exists...")
    await pool.execute(query)
    logger.info(f"Table '{table_name}' is ready.")

async def insert_analysis_results(pool, table_name, results, labels):
    """
    De-duplicates results and inserts them into the specified table,
    gracefully skipping any records that already exist.
    """
    if not results:
        logger.warning(f"No results to insert into '{table_name}'.")
        return

    column_names = ['question_text'] + [f'label_{i}' for i in range(len(labels))]
    
    # --- FIX: De-duplicate data in-memory before hitting the database ---
    df = pd.DataFrame(results)
    scores_df = pd.DataFrame(df['scores'].tolist(), columns=[f'label_{i}' for i in range(len(labels))])
    df = pd.concat([df['question_text'], scores_df], axis=1)
    
    original_count = len(df)
    df.drop_duplicates(subset=['question_text'], keep='first', inplace=True)
    deduped_count = len(df)
    if original_count > deduped_count:
        logger.info(f"Removed {original_count - deduped_count} duplicate question texts before insertion.")
        
    records_to_insert = [tuple(x) for x in df.to_numpy()]
    # --- END FIX ---

    if not records_to_insert:
        logger.warning(f"No new, unique records to insert into '{table_name}'.")
        return

    async with pool.acquire() as connection:
        async with connection.transaction():
            # Use a temporary table and ON CONFLICT DO NOTHING for a robust, idempotent insert
            temp_table_name = f"temp_{table_name}"
            await connection.execute(f"CREATE TEMP TABLE {temp_table_name} (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP;")
            
            await connection.copy_records_to_table(temp_table_name, records=records_to_insert, columns=column_names)
            
            insert_query = f"""
            INSERT INTO {table_name} ({', '.join(f'"{c}"' for c in column_names)})
            SELECT {', '.join(f'"{c}"' for c in column_names)} FROM {temp_table_name}
            ON CONFLICT (question_text) DO NOTHING;
            """
            status = await connection.execute(insert_query)
            
    inserted_count = int(status.split(' ')[-1]) if status and 'INSERT' in status else 0
    logger.info(f"Successfully inserted {inserted_count} new records into '{table_name}'. Skipped {len(records_to_insert) - inserted_count} pre-existing records.")

# ----------------------------------------------------------------
# 5. MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials are not set. Exiting.")
        return

    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Successfully connected to the database.")

    labels = load_labels(LABELS_FILE)
    if not labels:
        await pool.close()
        return
        
    logger.info(f"Loading NLP model '{NLP_MODEL_NAME}'... (This may take a moment on first run)")
    model = SentenceTransformer(NLP_MODEL_NAME)
    logger.info("NLP model loaded successfully.")

    clob_ids, ctf_ids, oracle_ids, moo_ids, neg_ids = await get_question_ids_from_sources(pool)
    
    groups = {
        "clob_not_ctf": {
            "ids": clob_ids - ctf_ids,
            "table_name": "semantic_clob_not_ctf",
            "sources": {'clob'} 
        },
        "oracles_not_clob": {
            "ids": oracle_ids - clob_ids,
            "table_name": "semantic_oracles_not_clob",
            "sources": {'moo_uma', 'negrisk_adapt'}
        },
        "shared_bridge": {
            "ids": clob_ids.intersection(ctf_ids),
            "table_name": "semantic_shared_bridge",
            "sources": {'clob'}
        }
    }

    for name, group in groups.items():
        logger.info(f"\n{'='*20} PROCESSING GROUP: {name} ({len(group['ids'])} questions) {'='*20}")
        
        all_texts = {}
        for source in group['sources']:
            source_ids = group['ids']
            if source == 'moo_uma':
                source_ids = group['ids'].intersection(moo_ids)
            elif source == 'negrisk_adapt':
                source_ids = group['ids'].intersection(neg_ids)
            
            logger.info(f"Fetching texts from '{source}' for {len(source_ids)} IDs...")
            texts = await fetch_question_texts(pool, source_ids, source)
            all_texts.update(texts)
        
        question_list = list(all_texts.values())
        if not question_list:
            logger.warning(f"No question texts found for group '{name}'. Skipping.")
            continue
            
        analysis_results = analyze_questions(question_list, labels, model)
        
        await create_analysis_table(pool, group['table_name'], labels)
        await insert_analysis_results(pool, group['table_name'], analysis_results, labels)

    await pool.close()
    logger.info("\nSemantic analysis complete. All data has been stored in the database.")


if __name__ == "__main__":
    asyncio.run(main())