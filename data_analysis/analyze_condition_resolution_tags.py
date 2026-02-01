import os
import asyncio
import asyncpg
import pandas as pd
import logging
import re
import ast
from collections import Counter
from hexbytes import HexBytes
from eth_abi import decode as abi_decode

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

# --- CSV Configuration ---
POLY_CSV_DIR = os.getenv("POLY_CSV")
POLY_CSV_PATH = os.path.join(POLY_CSV_DIR, "gamma_markets.csv") if POLY_CSV_DIR else "gamma_markets.csv"

# --- Conditional Tokens Address ---
ADDR = {
    'conditional_tokens': "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower(),
}

# --- ConditionResolution Event Definition ---
CONDITION_RESOLUTION_EVENT = {
    "indexed_types": ["bytes32", "address", "bytes32", "uint256"],
    "data_types": ["uint256[]"],
    "arg_names": ["conditionId", "oracle", "questionId", "outcomeSlotCount", "payoutNumerators"]
}

# --- Top Tags Configuration ---
TOP_TAGS_COUNT = 10

# ----------------------------------------------------------------
# 2. HELPER FUNCTIONS
# ----------------------------------------------------------------

def parse_tags(tag_str):
    """Safely parses a string representation of a list into a Python list."""
    if pd.isna(tag_str) or not isinstance(tag_str, str):
        return []
    try:
        tags_list = ast.literal_eval(tag_str)
        return tags_list if isinstance(tags_list, list) else []
    except (ValueError, SyntaxError):
        return []

def payout_to_tuple(payout_numerators):
    """Convert payout numerators to a tuple for categorization."""
    if isinstance(payout_numerators, (list, tuple)):
        return tuple(payout_numerators)
    return None

def categorize_payout(payout_tuple):
    """Categorize payout into known patterns or 'Other'."""
    if payout_tuple == (1, 1):
        return "(1,1)"
    elif payout_tuple == (1, 0):
        return "(1,0)"
    elif payout_tuple == (0, 1):
        return "(0,1)"
    else:
        return "Other"

def analyze_tags_for_group(df_group, title):
    """Analyzes and prints top tags for a group of markets."""
    if df_group.empty or 'tags' not in df_group.columns:
        logger.info(f"{title}: No data or tags column")
        return
    
    all_tags = []
    for tags_list in df_group['tags']:
        if isinstance(tags_list, list):
            all_tags.extend(tags_list)
    
    if all_tags:
        tag_counts = Counter(all_tags)
        top_tags = tag_counts.most_common(TOP_TAGS_COUNT)
        
        total_tags = len(all_tags)
        logger.info(f"\n{title}")
        logger.info(f"Markets: {len(df_group):,} | Total Tags: {total_tags:,}")
        for rank, (tag, count) in enumerate(top_tags, 1):
            percentage = (count / total_tags) * 100
            logger.info(f"  {rank:2d}. {tag}: {count:,} ({percentage:.2f}%)")
    else:
        logger.info(f"{title}: No tags found")

# ----------------------------------------------------------------
# 3. CORE LOGIC
# ----------------------------------------------------------------

async def main():
    """Main function to fetch, decode, and analyze ConditionResolution events."""
    
    # --- Validate Environment Variables ---
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS, POLY_CSV_DIR]):
        logger.error("One or more required environment variables are not set.")
        return

    if not os.path.exists(POLY_CSV_PATH):
        logger.error(f"CSV file not found at the specified path: {POLY_CSV_PATH}")
        return

    # --- Load Market Data from CSV ---
    logger.info(f"Loading market data from {POLY_CSV_PATH}...")
    try:
        df_markets = pd.read_csv(POLY_CSV_PATH, usecols=['conditionId', 'tags'])
        df_markets['conditionId'] = df_markets['conditionId'].str.lower()
        df_markets['tags'] = df_markets['tags'].apply(parse_tags)
        logger.info(f"Loaded {len(df_markets):,} markets into memory.")
    except Exception as e:
        logger.error(f"Error loading market data: {e}")
        return

    # --- Connect to Database ---
    logger.info("Connecting to the database...")
    pool = None
    try:
        pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
        logger.info("Database connection successful.")

        # --- Fetch All ConditionResolution Events ---
        async with pool.acquire() as conn:
            logger.info("Fetching all ConditionResolution events...")
            records = await conn.fetch(
                "SELECT data, topics FROM events_conditional_tokens WHERE event_name = 'ConditionResolution' AND LOWER(contract_address) = $1",
                ADDR['conditional_tokens']
            )
            
        logger.info(f"Fetched {len(records):,} ConditionResolution events.")

        # --- Decode Events ---
        logger.info("Decoding ConditionResolution events...")
        decoded_events = []
        
        for idx, record in enumerate(records):
            try:
                topics = record.get('topics', [])
                data_hex = record.get('data', '0x')
                
                # Decode indexed arguments
                decoded_indexed_args = []
                if len(topics) > 1:
                    for i in range(len(CONDITION_RESOLUTION_EVENT['indexed_types'])):
                        if i + 1 < len(topics):
                            decoded_indexed_args.append(
                                abi_decode([CONDITION_RESOLUTION_EVENT['indexed_types'][i]], HexBytes(topics[i + 1]))[0]
                            )
                
                # Decode data arguments
                data_bytes = HexBytes(data_hex)
                decoded_data_args = abi_decode(CONDITION_RESOLUTION_EVENT['data_types'], data_bytes)
                
                # Reconstruct full argument list
                all_args = decoded_indexed_args + list(decoded_data_args)
                
                # Create a dictionary of decoded arguments
                args = dict(zip(CONDITION_RESOLUTION_EVENT['arg_names'], all_args))
                
                # Extract condition_id and normalize
                condition_id = args.get('conditionId')
                if isinstance(condition_id, bytes):
                    condition_id = '0x' + condition_id.hex()
                condition_id = condition_id.lower() if condition_id else None
                
                # Extract payout numerators
                payout_numerators = args.get('payoutNumerators')
                if isinstance(payout_numerators, (list, tuple)):
                    payout_numerators = tuple(payout_numerators)
                
                decoded_events.append({
                    'condition_id': condition_id,
                    'payout_numerators': payout_numerators,
                    'oracle': args.get('oracle', 'N/A'),
                    'question_id': args.get('questionId', 'N/A')
                })
                
            except Exception as e:
                logger.debug(f"Failed to decode event {idx}: {e}")
                continue

        logger.info(f"Successfully decoded {len(decoded_events):,} events.")

        # --- Match with Markets and Categorize ---
        logger.info("Matching events with markets by condition_id...")
        df_events = pd.DataFrame(decoded_events)
        
        # Merge with market data
        df_merged = pd.merge(
            df_events,
            df_markets,
            left_on='condition_id',
            right_on='conditionId',
            how='inner'
        )
        
        matched_count = len(df_merged)
        logger.info(f"Matched {matched_count:,} ConditionResolution events with markets.")
        
        if matched_count == 0:
            logger.warning("No matches found. Exiting.")
            return

        # --- Categorize by Payout Pattern ---
        df_merged['payout_category'] = df_merged['payout_numerators'].apply(categorize_payout)

        # --- Print Statistics ---
        print("\n" + "="*80)
        print("ConditionResolution Event Analysis - Payout Pattern Distribution")
        print("="*80)
        
        # Count by category
        category_counts = df_merged['payout_category'].value_counts().sort_index()
        total_events = len(df_merged)
        
        print(f"\nTotal Matched Events: {total_events:,}\n")
        print("--- Count Distribution ---")
        for category in ["(1,1)", "(1,0)", "(0,1)", "Other"]:
            count = category_counts.get(category, 0)
            percentage = (count / total_events * 100) if total_events > 0 else 0
            print(f"{category}: {count:,} ({percentage:.2f}%)")
        
        print("\n" + "="*80)
        print("Top Tags Analysis by Payout Pattern")
        print("="*80)
        
        # Analyze tags for each category
        for category in ["(1,1)", "(1,0)", "(0,1)", "Other"]:
            df_category = df_merged[df_merged['payout_category'] == category]
            if not df_category.empty:
                analyze_tags_for_group(df_category, f"\n--- {category} ---")

        print("\n" + "="*80 + "\n")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if pool:
            await pool.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials not set.")
    elif not POLY_CSV_DIR:
        logger.error("CSV directory not set.")
    elif not os.path.exists(POLY_CSV_PATH):
        logger.error(f"CSV file not found at {POLY_CSV_PATH}.")
    else:
        asyncio.run(main())
