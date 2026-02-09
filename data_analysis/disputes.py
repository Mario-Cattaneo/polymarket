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

# --- Block Range Filtering ---
START_BLOCK_NR = 79172086  # Set to a specific block number to start filtering
END_BLOCK_NR = 81225971    # Set to a specific block number to end filtering
BLOCK_BATCH_SIZE = 100000  # Process in batches of N blocks

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

def extract_market_id(ancillary_str: str) -> int | None:
    """Extracts the market_id from the ancillary data string using regex."""
    # This regex looks for 'market_id:' followed by an optional space,
    # then captures a numeric value (can be quoted or unquoted).
    try:
        # Try to match numeric market_id (may be quoted or unquoted)
        match = re.search(r"market_id:\s*['\"]?(\d+)['\"]?", ancillary_str)
        if match:
            return int(match.group(1))
    except (ValueError, AttributeError) as e:
        logger.debug(f"Failed to extract market_id: {e}")
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

def analyze_tags(tags_list, title, total_count=None):
    """Analyzes and prints top tags from a list of tags."""
    if not tags_list:
        logger.info(f"{title}: No tags found")
        return
    
    from collections import Counter
    tag_counts = Counter(tags_list)
    top_tags = tag_counts.most_common(10)
    
    total_tags = len(tags_list)
    logger.info(f"\n{title} (n={total_tags:,} total tags)")
    
    for rank, (tag, count) in enumerate(top_tags, 1):
        if total_count:
            # Percentage relative to total events/disputes
            percentage = (count / total_count) * 100
            logger.info(f"  {rank}. {tag}: {count:,} ({percentage:.2f}%)")
        else:
            # Percentage relative to total tags
            percentage = (count / total_tags) * 100
            logger.info(f"  {rank}. {tag}: {count:,} ({percentage:.2f}%)")

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

        # --- 3. Fetch Events in Batches with Block Range Filtering ---
        logger.info(f"Fetching events in batches of {BLOCK_BATCH_SIZE:,} blocks from {START_BLOCK_NR:,} to {END_BLOCK_NR:,}...")
        
        all_dispute_price_records_oov2 = []
        all_dispute_price_records_moov2 = []
        all_propose_price_records_oov2 = []
        all_propose_price_records_moov2 = []
        
        propose_price_count_oov2 = 0
        propose_price_count_moov2 = 0
        dispute_price_count = 0
        dispute_price_count_moov2 = 0
        
        current_block = START_BLOCK_NR
        batch_num = 0
        
        while current_block <= END_BLOCK_NR:
            batch_num += 1
            batch_end = min(current_block + BLOCK_BATCH_SIZE - 1, END_BLOCK_NR)
            logger.info(f"Batch {batch_num}: blocks {current_block:,} to {batch_end:,}")
            
            async with pool.acquire() as conn:
                # Get ProposePrice event counts for this batch
                batch_propose_count_oov2 = await conn.fetchval(
                    f"SELECT COUNT(*) FROM oov2 WHERE event_name = 'ProposePrice' AND block_number >= {current_block} AND block_number <= {batch_end}"
                )
                batch_propose_count_moov2 = await conn.fetchval(
                    f"SELECT COUNT(*) FROM events_managed_oracle WHERE event_name = 'ProposePrice' AND block_number >= {current_block} AND block_number <= {batch_end}"
                )
                
                propose_price_count_oov2 += batch_propose_count_oov2
                propose_price_count_moov2 += batch_propose_count_moov2
                
                # Fetch DisputePrice events for this batch
                batch_dispute_records_oov2 = await conn.fetch(
                    f"SELECT data, topics FROM oov2 WHERE event_name = 'DisputePrice' AND block_number >= {current_block} AND block_number <= {batch_end}"
                )
                all_dispute_price_records_oov2.extend(batch_dispute_records_oov2)
                
                batch_dispute_count_oov2 = len(batch_dispute_records_oov2)
                dispute_price_count += batch_dispute_count_oov2
                
                # Fetch DisputePrice events from events_managed_oracle for this batch
                batch_dispute_records_moov2 = await conn.fetch(
                    f"SELECT data, topics FROM events_managed_oracle WHERE event_name = 'DisputePrice' AND block_number >= {current_block} AND block_number <= {batch_end}"
                )
                all_dispute_price_records_moov2.extend(batch_dispute_records_moov2)
                
                batch_dispute_count_moov2 = len(batch_dispute_records_moov2)
                dispute_price_count_moov2 += batch_dispute_count_moov2
                
                # Fetch ProposePrice events for this batch
                batch_propose_records_oov2 = await conn.fetch(
                    f"SELECT data, topics FROM oov2 WHERE event_name = 'ProposePrice' AND block_number >= {current_block} AND block_number <= {batch_end}"
                )
                all_propose_price_records_oov2.extend(batch_propose_records_oov2)
                
                batch_propose_records_moov2 = await conn.fetch(
                    f"SELECT data, topics FROM events_managed_oracle WHERE event_name = 'ProposePrice' AND block_number >= {current_block} AND block_number <= {batch_end}"
                )
                all_propose_price_records_moov2.extend(batch_propose_records_moov2)
                
                logger.info(f"  Batch {batch_num}: {batch_dispute_count_oov2:,} DisputePrice (oov2), {batch_dispute_count_moov2:,} DisputePrice (moov2), {batch_propose_count_oov2 + batch_propose_count_moov2:,} ProposePrice events")
            
            current_block = batch_end + 1
        
        # Reassign for consistency with downstream code
        dispute_price_records = all_dispute_price_records_oov2
        dispute_price_records_moov2 = all_dispute_price_records_moov2
        propose_price_records = all_propose_price_records_oov2
        propose_price_records_moov2 = all_propose_price_records_moov2

        if dispute_price_count == 0 and dispute_price_count_moov2 == 0:
            logger.warning("No DisputePrice events found. Exiting.")
            return

        # --- 4. Decode Events and Match Tags ---
        logger.info(f"Processing events and matching with market tags...")
        
        # Collections for each event type
        all_disputed_tags = []
        # matched disputes per source
        matched_disputes_oov2 = 0
        matched_disputes_moov2 = 0
        
        # Counters for matched ProposePrice events
        matched_proposes_oov2 = 0
        matched_proposes_moov2 = 0
        
        dispute_definition = OOV2_EVENT_DEFINITIONS['DisputePrice']
        propose_definition = OOV2_EVENT_DEFINITIONS['ProposePrice']
        
        # DEBUG: Show sample market IDs from CSV
        sample_market_ids = list(market_tags_map.keys())[:5]
        logger.info(f"DEBUG: Sample market IDs from CSV: {sample_market_ids}")
        logger.info(f"DEBUG: Total unique market IDs in CSV: {len(market_tags_map):,}")
        
        # --- Process OOV2 DisputePrice events ---
        logger.info(f"Processing {dispute_price_count:,} DisputePrice events from oov2...")
        for idx, record in enumerate(dispute_price_records):
            try:
                data_bytes = HexBytes(record['data'])
                topics = record.get('topics', [])
                
                decoded_data_args = abi_decode(dispute_definition['data_types'], data_bytes)
                
                # Decode indexed arguments from topics (skip topic[0] which is the event signature)
                decoded_indexed_args = []
                if len(topics) > 1:
                    for i in range(len(dispute_definition['indexed_types'])):
                        if i + 1 < len(topics):
                            decoded_indexed_args.append(
                                abi_decode([dispute_definition['indexed_types'][i]], HexBytes(topics[i + 1]))[0]
                            )
                
                # Reconstruct full argument list: indexed first, then data
                all_args = decoded_indexed_args + list(decoded_data_args)
                
                # Create a dictionary of decoded arguments by name
                args = dict(zip(dispute_definition['arg_names'], all_args))
                
                # The ancillaryData should now be properly decoded
                ancillary_bytes = args.get('ancillaryData')
                
                if ancillary_bytes is not None and len(ancillary_bytes) > 0:
                    ancillary_str = decode_ancillary_data(ancillary_bytes)
                    market_id = extract_market_id(ancillary_str)
                    
                    if market_id:
                        tags = market_tags_map.get(market_id)
                        if tags:
                            all_disputed_tags.extend(tags)
                            matched_disputes_oov2 += 1

            except Exception as e:
                logger.debug(f"Could not decode a DisputePrice event {idx}: {e}")
        
        # --- Process events_managed_oracle DisputePrice events ---
        logger.info(f"Processing {dispute_price_count_moov2:,} DisputePrice events from events_managed_oracle...")
        for idx, record in enumerate(dispute_price_records_moov2):
            try:
                data_bytes = HexBytes(record['data'])
                topics = record.get('topics', [])
                
                decoded_data_args = abi_decode(dispute_definition['data_types'], data_bytes)
                
                # Decode indexed arguments from topics
                decoded_indexed_args = []
                if len(topics) > 1:
                    for i in range(len(dispute_definition['indexed_types'])):
                        if i + 1 < len(topics):
                            decoded_indexed_args.append(
                                abi_decode([dispute_definition['indexed_types'][i]], HexBytes(topics[i + 1]))[0]
                            )
                
                # Reconstruct full argument list
                all_args = decoded_indexed_args + list(decoded_data_args)
                args = dict(zip(dispute_definition['arg_names'], all_args))
                
                ancillary_bytes = args.get('ancillaryData')
                
                if ancillary_bytes is not None and len(ancillary_bytes) > 0:
                    ancillary_str = decode_ancillary_data(ancillary_bytes)
                    market_id = extract_market_id(ancillary_str)
                    
                    if market_id:
                        tags = market_tags_map.get(market_id)
                        if tags:
                            all_disputed_tags.extend(tags)
                            matched_disputes_moov2 += 1

            except Exception as e:
                logger.debug(f"Could not decode a DisputePrice event from events_managed_oracle {idx}: {e}")

        # --- Process ProposePrice events from oov2 ---
        logger.info(f"Processing {len(propose_price_records):,} ProposePrice events from oov2...")
        for idx, record in enumerate(propose_price_records):
            try:
                data_bytes = HexBytes(record['data'])
                topics = record.get('topics', [])

                decoded_data_args = abi_decode(propose_definition['data_types'], data_bytes)

                # Decode indexed arguments from topics
                decoded_indexed_args = []
                if len(topics) > 1:
                    for i in range(len(propose_definition['indexed_types'])):
                        if i + 1 < len(topics):
                            decoded_indexed_args.append(
                                abi_decode([propose_definition['indexed_types'][i]], HexBytes(topics[i + 1]))[0]
                            )

                all_args = decoded_indexed_args + list(decoded_data_args)
                args = dict(zip(propose_definition['arg_names'], all_args))

                ancillary_bytes = args.get('ancillaryData')
                if ancillary_bytes is not None and len(ancillary_bytes) > 0:
                    ancillary_str = decode_ancillary_data(ancillary_bytes)
                    market_id = extract_market_id(ancillary_str)
                    if market_id:
                        tags = market_tags_map.get(market_id)
                        if tags:
                            matched_proposes_oov2 += 1

            except Exception as e:
                logger.debug(f"Could not decode a ProposePrice event from oov2 {idx}: {e}")

        # --- Process ProposePrice events from events_managed_oracle (moov2) ---
        logger.info(f"Processing {len(propose_price_records_moov2):,} ProposePrice events from events_managed_oracle...")
        for idx, record in enumerate(propose_price_records_moov2):
            try:
                data_bytes = HexBytes(record['data'])
                topics = record.get('topics', [])

                decoded_data_args = abi_decode(propose_definition['data_types'], data_bytes)

                # Decode indexed arguments from topics
                decoded_indexed_args = []
                if len(topics) > 1:
                    for i in range(len(propose_definition['indexed_types'])):
                        if i + 1 < len(topics):
                            decoded_indexed_args.append(
                                abi_decode([propose_definition['indexed_types'][i]], HexBytes(topics[i + 1]))[0]
                            )

                all_args = decoded_indexed_args + list(decoded_data_args)
                args = dict(zip(propose_definition['arg_names'], all_args))

                ancillary_bytes = args.get('ancillaryData')
                if ancillary_bytes is not None and len(ancillary_bytes) > 0:
                    ancillary_str = decode_ancillary_data(ancillary_bytes)
                    market_id = extract_market_id(ancillary_str)
                    if market_id:
                        tags = market_tags_map.get(market_id)
                        if tags:
                            matched_proposes_moov2 += 1

            except Exception as e:
                logger.debug(f"Could not decode a ProposePrice event from events_managed_oracle {idx}: {e}")

        # --- 5. Analyze and Print Results ---
        logger.info("Analysis complete. Compiling results...")
        
        print("\n" + "="*70)
        print("                 UMA Event Analysis (OOV2 + MOOV2)")
        print("="*70 + "\n")

        # Event counts with percentages
        print("--- Event Counts ---")
        print(f"OOV2 ProposePrice Events: {propose_price_count_oov2:,}")
        print(f"MOOV2 ProposePrice Events: {propose_price_count_moov2:,}")
        print(f"Total ProposePrice Events: {propose_price_count_oov2 + propose_price_count_moov2:,}")
        print(f"OOV2 ProposePrice Events Matched to Markets: {matched_proposes_oov2:,} ({(matched_proposes_oov2/propose_price_count_oov2*100 if propose_price_count_oov2>0 else 0):.2f}%)")
        print(f"MOOV2 ProposePrice Events Matched to Markets: {matched_proposes_moov2:,} ({(matched_proposes_moov2/propose_price_count_moov2*100 if propose_price_count_moov2>0 else 0):.2f}%)")
        print(f"Total ProposePrice Events Matched to Markets: {matched_proposes_oov2 + matched_proposes_moov2:,} ({((matched_proposes_oov2 + matched_proposes_moov2)/(propose_price_count_oov2 + propose_price_count_moov2)*100 if (propose_price_count_oov2 + propose_price_count_moov2)>0 else 0):.2f}%)")
        print()
        print(f"OOV2 DisputePrice Events: {dispute_price_count:,}")
        print(f"MOOV2 DisputePrice Events: {dispute_price_count_moov2:,}")
        total_matched_disputes = matched_disputes_oov2 + matched_disputes_moov2
        print(f"Total DisputePrice Events: {dispute_price_count + dispute_price_count_moov2:,}")
        print(f"OOV2 DisputePrice Events Matched to Markets: {matched_disputes_oov2:,} ({(matched_disputes_oov2/dispute_price_count*100 if dispute_price_count>0 else 0):.2f}%)")
        print(f"MOOV2 DisputePrice Events Matched to Markets: {matched_disputes_moov2:,} ({(matched_disputes_moov2/dispute_price_count_moov2*100 if dispute_price_count_moov2>0 else 0):.2f}%)")
        print(f"Total DisputePrice Events Matched to Markets: {total_matched_disputes:,} ({(total_matched_disputes/(dispute_price_count + dispute_price_count_moov2)*100 if (dispute_price_count + dispute_price_count_moov2)>0 else 0):.2f}%)")
        print()
        
        if not all_disputed_tags:
            print("No tags could be matched to the dispute events.")
            print("="*70)
            return

        # Analyze and print top 10 tags
        analyze_tags(all_disputed_tags, "--- Top 10 Tags in Matched Disputed Markets ---", total_count=total_matched_disputes)
        
        print("\n" + "="*70)

    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}")
    finally:
        if pool:
            await pool.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    asyncio.run(main())