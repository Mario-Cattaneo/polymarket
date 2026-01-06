import os
import asyncio
import asyncpg
import pandas as pd
import logging
from sentence_transformers import SentenceTransformer, util
import torch

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

# --- CSV Path Configuration ---
POLY_CSV_DIR = os.getenv("POLY_CSV")
POLY_CSV_PATH = os.path.join(POLY_CSV_DIR, "gamma_markets.csv") if POLY_CSV_DIR else "gamma_markets.csv"

# --- Contract and Oracle Addresses ---
ADDR = {
    'conditional_tokens': "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower(),
}

ORACLE_ALIASES = {
    "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7": "MOOV2 Adapter",
    "0x58e1745bedda7312c4cddb72618923da1b90efde": "Centralized Adapter",
    "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296": "Negrisk UmaCtfAdapter",
}

# --- Market Categorization Labels ---
# Using more descriptive phrases improves embedding quality.
DESCRIPTIVE_LABELS = [
    'Sports competitions and outcomes', 
    'Recurring scheduled events', 
    'Political elections and government policy', 
    'Culture, entertainment, movies, and awards', 
    'Financial markets, stocks, and indices', 
    'Esports and competitive video gaming', 
    'Cryptocurrency prices and blockchain technology', 
    'Technology industry and innovations', 
    'World affairs and international relations', 
    'Economic indicators and performance', 
    'Weather, climate, and natural phenomena', 
    'Business news and corporate earnings'
]
# We'll map back to shorter names for the final report.
LABEL_MAPPING = {
    'Sports competitions and outcomes': 'Sports',
    'Recurring scheduled events': 'Recurring',
    'Political elections and government policy': 'Politics',
    'Culture, entertainment, movies, and awards': 'Culture',
    'Financial markets, stocks, and indices': 'Finance',
    'Esports and competitive video gaming': 'Esports',
    'Cryptocurrency prices and blockchain technology': 'Crypto',
    'Technology industry and innovations': 'Tech',
    'World affairs and international relations': 'World',
    'Economic indicators and performance': 'Economy',
    'Weather, climate, and natural phenomena': 'Weather',
    'Business news and corporate earnings': 'Business'
}


# --- NLP Model Configuration ---
MODEL_NAME = 'all-MiniLM-L6-v2'
SIMILARITY_THRESHOLD = 0.4  # Lowered threshold to be less strict and capture more relevant labels.
BATCH_SIZE = 256

# ----------------------------------------------------------------
# 2. DATA FETCHING & LOADING
# ----------------------------------------------------------------

async def fetch_data(pool, query, params=[]):
    """Fetches data from the database."""
    try:
        async with pool.acquire() as connection:
            rows = await connection.fetch(query, *params)
            return pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()
    except Exception as e:
        logger.error(f"SQL Error: {e}")
        return pd.DataFrame()

def load_market_data_with_text(csv_path):
    """Loads and prepares market data from CSV."""
    logger.info(f"Loading market data from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, usecols=['conditionId', 'description', 'question'])
        df['conditionId'] = df['conditionId'].str.lower()
        df.dropna(subset=['description', 'question'], inplace=True)
        df['description'] = df['description'].astype(str)
        df['question'] = df['question'].astype(str)
        df['combined_text'] = "Market Question: " + df['question'] + "; Market Description: " + df['description']
        df_unique = df.drop_duplicates(subset='conditionId', keep='first')
        logger.info(f"Loaded {len(df_unique):,} unique markets with text from CSV.")
        return df_unique
    except FileNotFoundError:
        logger.error(f"CSV file not found at {csv_path}. Please ensure POLY_CSV is set correctly.")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading/processing CSV: {e}")
        return pd.DataFrame()

# ----------------------------------------------------------------
# 3. MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    """Main function to fetch, match, classify with Sentence Transformers, and report."""
    
    # 1. Load and Match Data
    df_markets = load_market_data_with_text(POLY_CSV_PATH)
    if df_markets.empty: return

    try:
        pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
        logger.info("Successfully connected to the database.")
    except Exception as e:
        logger.error(f"Database connection failed: {e}"); return

    logger.info("Fetching all ConditionPreparation events...")
    query_oracles = """
        SELECT topics[2] as condition_id, '0x' || substring(topics[3] from 27) as oracle, timestamp_ms
        FROM events_conditional_tokens 
        WHERE event_name = 'ConditionPreparation' AND LOWER(contract_address) = $1
    """
    df_oracles = await fetch_data(pool, query_oracles, [ADDR['conditional_tokens']])
    await pool.close()

    if df_oracles.empty: logger.error("No ConditionPreparation events found."); return
        
    df_oracles['condition_id'] = df_oracles['condition_id'].str.lower()
    df_oracles_unique = df_oracles.sort_values('timestamp_ms').drop_duplicates(subset='condition_id', keep='first')
    
    logger.info("Matching markets with their preparing oracles...")
    df_matched = pd.merge(
        df_markets, df_oracles_unique[['condition_id', 'oracle']],
        left_on='conditionId', right_on='condition_id', how='inner'
    )
    df_final_matched = df_matched[df_matched['oracle'].isin(ORACLE_ALIASES.keys())].copy()
    
    total_matched_count = len(df_final_matched)
    if total_matched_count == 0:
        logger.warning("No markets were found prepared by the specified oracles."); return
    logger.info(f"Found {total_matched_count:,} markets matched with the target oracles.")

    # 2. Initialize Sentence Transformer Model
    logger.info(f"Initializing Sentence Transformer model '{MODEL_NAME}'...")
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    model = SentenceTransformer(MODEL_NAME, device=device)
    logger.info(f"Model loaded successfully on device: {device}")

    # 3. Generate Embeddings
    logger.info("Generating embeddings for market labels...")
    label_embeddings = model.encode(DESCRIPTIVE_LABELS, convert_to_tensor=True, show_progress_bar=False)

    logger.info(f"Generating embeddings for {total_matched_count:,} markets...")
    market_texts = df_final_matched['combined_text'].tolist()
    market_embeddings = model.encode(market_texts, convert_to_tensor=True, show_progress_bar=True, batch_size=BATCH_SIZE)

    # 4. Compute Cosine Similarity
    logger.info("Calculating cosine similarity between markets and labels...")
    cosine_scores = util.cos_sim(market_embeddings, label_embeddings)

    # 5. Assign Labels
    logger.info(f"Assigning labels with a similarity threshold of {SIMILARITY_THRESHOLD}...")
    assigned_labels = []
    for i in range(len(market_texts)):
        market_scores = cosine_scores[i]
        confident_labels = [DESCRIPTIVE_LABELS[j] for j, score in enumerate(market_scores) if score > SIMILARITY_THRESHOLD]
        assigned_labels.append(confident_labels)
    
    df_final_matched['labels'] = assigned_labels
    logger.info("Classification complete.")

    # 6. Aggregate and Display Results
    print("\n" + "="*70)
    print("      Oracle Market Category Representation (Sentence Transformer)")
    print("="*70)
    print(f"\nAnalysis based on {total_matched_count:,} matched markets (Similarity > {SIMILARITY_THRESHOLD}).\n")

    for oracle_addr, alias in ORACLE_ALIASES.items():
        print(f"--- Oracle: {alias} ---")
        
        oracle_df = df_final_matched[df_final_matched['oracle'] == oracle_addr]
        oracle_market_count = len(oracle_df)

        if oracle_market_count == 0:
            print(f"  No matched markets found for this oracle."); print("-" * 50 + "\n"); continue
        
        # Map the long labels back to short names before counting
        oracle_df['short_labels'] = oracle_df['labels'].apply(lambda lst: [LABEL_MAPPING[l] for l in lst])
        
        oracle_labels = oracle_df.explode('short_labels')['short_labels'].dropna()
        label_counts = oracle_labels.value_counts().to_dict()

        print(f"  Total Matched Markets: {oracle_market_count:,}\n")

        if not label_counts:
            print("  No markets with assigned labels found for this oracle."); print("-" * 50 + "\n"); continue

        sorted_labels = sorted(label_counts.items(), key=lambda item: item[1], reverse=True)
        
        print("  Market Categories (by count of labels assigned):")
        for label, count in sorted_labels:
            print(f"    - {label:<30} | {count:>6,} assignments")
        print("-" * 50 + "\n")


if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS, POLY_CSV_DIR]):
        logger.error("One or more required environment variables are not set.")
    else:
        asyncio.run(main())