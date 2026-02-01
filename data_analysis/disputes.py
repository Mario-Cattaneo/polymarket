import os
import asyncio
import asyncpg
import pandas as pd
import logging
import re
from collections import Counter
import ast
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

# --- OOV2 Event ABIs and Argument Types ---
# We only need DisputePrice for this script, but keeping ProposePrice for context
OOV2_EVENT_DEFINITIONS = {
    "ProposePrice": {
        "signature": "ProposePrice(address,address,bytes32,uint256,bytes,int256,uint256,address)",
        "indexed_types": ["address", "address"],
        "data_types": ["bytes32", "uint256", "bytes", "int256", "uint256", "address"],
        "arg_names": ["requester", "proposer", "identifier", "timestamp", "ancillaryData", "proposedPrice", "reward", "currency"]
    },
    "DisputePrice": {
        "signature": "DisputePrice(address,address,address,bytes32,uint256,bytes,int256)",
        "indexed_types": ["address", "address", "address"],
        "data_types": ["bytes32", "uint256", "bytes", "int256"],
        "arg_names": ["requester", "proposer", "disputer", "identifier", "timestamp", "ancillaryData", "proposedPrice"]
    }
}

# ----------------------------------------------------------------
# 2. HELPER FUNCTIONS
# ----------------------------------------------------------------

def decode_ancillary_data(data_bytes: bytes) -> str:
    """Decodes ancillary data bytes into a readable string, handling potential errors."""
    try:
        # First, try decoding as UTF-8
        decoded_string = data_bytes.decode('utf-8')
        # Clean up non-printable characters
        return ''.join(char for char in decoded_string if char.isprintable())
    except UnicodeDecodeError:
        # If UTF-8 fails, return a placeholder indicating non-UTF8 bytes
        return f"<Non-UTF8 Bytes, len: {len(data_bytes)}>"
    except Exception:
        return "<Decoding Error>"

def extract_market_id(ancillary_str: str) -> str | None:
    """Extracts the market_id from the ancillary data string using regex."""
    # This regex looks for 'market_id:' followed by an optional space,
    # then captures the 0x-prefixed hex string inside quotes.
    match = re.search(r"market_id:\s*['\"](0x[a-fA-F0-9]+)['\"]", ancillary_str)
    if match:
        return match.group(1).lower()
    return None

def parse_tags(tag_str: str):
    """Safely parses a string representation of a list into a Python list."""
    if pd.isna(tag_str) or not isinstance(tag_str, str):
        return []
    try:
        # ast.literal_eval is a safe way to evaluate a string containing a Python literal
        tags_list = ast.literal_eval(tag_str)
        return tags_list if isinstance(tags_list, list) else []
    except (ValueError, SyntaxError):
        # Return an empty list if parsing fails
        return []

# ----------------------------------------------------------------
# 3. CORE LOGIC
# ----------------------------------------------------------------

async def main():
    """Main function to fetch, process, and analyze dispute events."""
    # --- Validate Environment Variables ---
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS, POLY_CSV_DIR]):
        logger.error("One or more required environment variables are not set.")
        logger.error("Please set: PG_SOCKET, POLY_PG_PORT, POLY_DB, POLY_DB_CLI, POLY_DB_CLI_PASS, POLY_CSV")
        return

    if not os.path.exists(POLY_CSV_PATH):
        logger.error(f"CSV file not found at the specified path: {POLY_CSV_PATH}")
        return

    pool = None
    try:
        # --- 1. Load Market Data from CSV ---
        logger.info(f"Loading market data from {POLY_CSV_PATH}...")
        df_markets = pd.read_csv(POLY_CSV_PATH, usecols=['id', 'tags'])
        df_markets['tags'] = df_markets['tags'].apply(parse_tags)
        # Create a dictionary for fast lookups: {id: [tags]}
        market_tags_map = df_markets.set_index('id')['tags'].to_dict()
        logger.info(f"Loaded {len(df_markets):,} markets into memory.")

        # --- 2. Connect to Database ---
        logger.info("Connecting to the database...")
        pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
        logger.info("Database connection successful.")

        # --- 3. Fetch Event Counts and Data ---
        async with pool.acquire() as conn:
            # Get total ProposePrice events
            logger.info("Fetching ProposePrice event count...")
            propose_price_count = await conn.fetchval("SELECT COUNT(*) FROM oov2 WHERE event_name = 'ProposePrice'")

            # Get all DisputePrice events to decode them
            logger.info("Fetching all DisputePrice events for analysis...")
            dispute_price_records = await conn.fetch("SELECT data FROM oov2 WHERE event_name = 'DisputePrice'")
            dispute_price_count = len(dispute_price_records)

        if dispute_price_count == 0:
            logger.warning("No DisputePrice events found. Exiting.")
            return

        # --- 4. Decode Events and Match Tags ---
        logger.info(f"Decoding {dispute_price_count:,} DisputePrice events and matching with market tags...")
        all_disputed_tags = []
        dispute_definition = OOV2_EVENT_DEFINITIONS['DisputePrice']
        
        for record in dispute_price_records:
            try:
                data_bytes = HexBytes(record['data'])
                decoded_data_args = abi_decode(dispute_definition['data_types'], data_bytes)
                
                # Create a dictionary of decoded arguments by name
                args = dict(zip(dispute_definition['arg_names'], decoded_data_args))
                
                # The ancillaryData is in the 'data' part, not 'indexed'
                ancillary_bytes = args.get('ancillaryData')
                if ancillary_bytes:
                    ancillary_str = decode_ancillary_data(ancillary_bytes)
                    market_id = extract_market_id(ancillary_str)
                    
                    if market_id:
                        # Look up the market_id in our map and get the tags
                        tags = market_tags_map.get(market_id)
                        if tags:
                            all_disputed_tags.extend(tags)

            except Exception as e:
                logger.warning(f"Could not decode a DisputePrice event: {e}")

        # --- 5. Analyze and Print Results ---
        logger.info("Analysis complete. Compiling results...")
        
        print("\n" + "="*60)
        print("              UMA Dispute Event Analysis")
        print("="*60 + "\n")

        print(f"Total 'ProposePrice' Events: {propose_price_count:,}")
        print(f"Total 'DisputePrice' Events: {dispute_price_count:,}\n")
        
        if not all_disputed_tags:
            print("No tags could be matched to the dispute events.")
            print("="*60)
            return

        # Count the frequency of each tag
        tag_counts = Counter(all_disputed_tags)
        top_10_tags = tag_counts.most_common(10)

        print("--- Top 10 Tags in Disputed Markets ---\n")
        print(f"{'Rank':<5} {'Tag':<20} {'Percentage of Disputes'}")
        print(f"{'-'*4:<5} {'-'*19:<20} {'-'*24}")

        for i, (tag, count) in enumerate(top_10_tags, 1):
            # Percentage is relative to the total number of dispute events
            percentage = (count / dispute_price_count) * 100
            print(f"{i:<5} {tag:<20} {percentage:.2f}%")
        
        print("\n" + "="*60)

    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}")
    finally:
        if pool:
            await pool.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    asyncio.run(main())