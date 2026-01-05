import asyncio
import asyncpg
import os
import logging
import re
import pandas as pd
from typing import Dict, List, Tuple
from hexbytes import HexBytes
from eth_abi import decode as abi_decode

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Database Configuration ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- Configuration ---
POLY_CSV_DIR = os.getenv("POLY_CSV")
POLY_CSV_PATH = os.path.join(POLY_CSV_DIR, "gamma_markets.csv") if POLY_CSV_DIR else "gamma_markets.csv"

# RequestPrice Event Definition
REQUEST_PRICE_EVENT = {
    "signature": "RequestPrice(address,bytes32,uint256,bytes,address,uint256,uint256)",
    "indexed_types": ["address"],
    "data_types": ["bytes32", "uint256", "bytes", "address", "uint256", "uint256"],
    "arg_names": ["requester", "identifier", "timestamp", "ancillaryData", "currency", "reward", "finalFee"]
}

# ----------------------------------------------------------------
# 1. HELPER FUNCTIONS
# ----------------------------------------------------------------

def decode_argument(arg_type: str, value) -> str:
    """Decodes a single argument into a more readable format."""
    if isinstance(value, bytes):
        if "bytes32" in arg_type:
            try:
                return value.strip(b'\x00').decode('utf-8')
            except UnicodeDecodeError:
                return value.hex()
        else:
            try:
                decoded_string = value.decode('utf-8')
                return ''.join(char for char in decoded_string if char.isprintable())
            except UnicodeDecodeError:
                return f"<Non-UTF8 Bytes, len: {len(value)}>"
    return str(value)

def extract_market_id(ancillary_data_str: str) -> int:
    """Extract market_id from ancillary data string."""
    try:
        # Look for "market_id: <number>"
        pattern = r'market_id:\s*(\d+)'
        match = re.search(pattern, ancillary_data_str)
        if match:
            return int(match.group(1))
    except Exception as e:
        logger.debug(f"Failed to extract market_id: {e}")
    return None

# ----------------------------------------------------------------
# 2. DATABASE FUNCTIONS
# ----------------------------------------------------------------

async def fetch_request_price_events(pool, table_name: str) -> List[Dict]:
    """Fetch and decode RequestPrice events from a table."""
    query = f'''
        SELECT block_number, timestamp_ms, data, topics
        FROM "{table_name}"
        WHERE event_name = 'RequestPrice'
        ORDER BY block_number
    '''
    
    all_events = []
    
    try:
        async with pool.acquire() as conn:
            records = await conn.fetch(query)
            logger.info(f"Found {len(records):,} RequestPrice events in {table_name}")
            
            for record in records:
                try:
                    block_number = record['block_number']
                    block_timestamp = record['timestamp_ms']
                    topics = record['topics']
                    data_hex = record['data']
                    
                    if not topics or len(topics) < 2:
                        continue
                    
                    indexed_args_raw = [HexBytes(t) for t in topics[1:]]
                    data_bytes = HexBytes(data_hex)
                    
                    decoded_data_args = abi_decode(REQUEST_PRICE_EVENT['data_types'], data_bytes)
                    decoded_indexed_args = [
                        abi_decode([dtype], val)[0] 
                        for dtype, val in zip(REQUEST_PRICE_EVENT['indexed_types'], indexed_args_raw)
                    ]
                    
                    # Reconstruct full argument list
                    all_args = []
                    indexed_args_iter = iter(decoded_indexed_args)
                    data_args_iter = iter(decoded_data_args)
                    
                    for i, arg_name in enumerate(REQUEST_PRICE_EVENT['arg_names']):
                        if i < len(REQUEST_PRICE_EVENT['indexed_types']):
                            all_args.append(next(indexed_args_iter))
                        else:
                            all_args.append(next(data_args_iter))
                    
                    # Extract event data
                    event_data = {}
                    all_types = REQUEST_PRICE_EVENT['indexed_types'] + REQUEST_PRICE_EVENT['data_types']
                    for name, type_str, value in zip(REQUEST_PRICE_EVENT['arg_names'], all_types, all_args):
                        event_data[name] = value
                    
                    # Decode ancillary data
                    ancillary_data_str = decode_argument('bytes', event_data.get('ancillaryData', b''))
                    
                    # Extract market_id
                    market_id = extract_market_id(ancillary_data_str)
                    
                    all_events.append({
                        'block_number': block_number,
                        'block_timestamp': block_timestamp,
                        'event_timestamp': event_data.get('timestamp', 0),
                        'market_id': market_id,
                        'ancillary_data': ancillary_data_str,
                    })
                
                except Exception as e:
                    logger.debug(f"Failed to decode event: {e}")
                    continue
    
    except Exception as e:
        logger.error(f"Error querying {table_name}: {e}")
    
    return all_events

def load_market_ids(csv_path: str) -> set:
    """Load all market IDs from CSV."""
    logger.info(f"Loading market IDs from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, usecols=['id'], low_memory=False)
        market_ids = set(df['id'].astype(int).unique())
        logger.info(f"Loaded {len(market_ids):,} unique market IDs from CSV.")
        return market_ids
    except Exception as e:
        logger.error(f"Error loading market IDs: {e}")
        return set()

# ----------------------------------------------------------------
# 3. MAIN ANALYSIS
# ----------------------------------------------------------------

async def main():
    logger.info("Starting RequestPrice Market ID Matching Analysis...")
    
    # Load market IDs from CSV
    market_ids_csv = load_market_ids(POLY_CSV_PATH)
    if not market_ids_csv:
        logger.error("Failed to load market IDs")
        return
    
    # Database Connection
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Connected to database.")
    
    try:
        # Process each table
        results = {}
        
        for table_name in ['oov2', 'events_managed_oracle']:
            logger.info(f"\n{'='*70}")
            logger.info(f"Processing {table_name} table...")
            logger.info(f"{'='*70}")
            
            # Fetch RequestPrice events
            request_price_events = await fetch_request_price_events(pool, table_name)
            
            total_events = len(request_price_events)
            logger.info(f"Total RequestPrice events in {table_name}: {total_events:,}")
            
            # Count matches
            matched_events = 0
            matched_with_market_id = 0
            unmatched_market_ids = {}
            
            for event in request_price_events:
                market_id = event['market_id']
                
                if market_id is not None:
                    matched_with_market_id += 1
                    
                    if market_id in market_ids_csv:
                        matched_events += 1
                    else:
                        # Track unmatched market_ids
                        unmatched_market_ids[market_id] = unmatched_market_ids.get(market_id, 0) + 1
            
            match_rate = (matched_events / total_events * 100) if total_events > 0 else 0
            extraction_rate = (matched_with_market_id / total_events * 100) if total_events > 0 else 0
            
            logger.info(f"\n--- Matching Results for {table_name} ---")
            logger.info(f"Total RequestPrice events: {total_events:,}")
            logger.info(f"Events with extracted market_id: {matched_with_market_id:,} ({extraction_rate:.2f}%)")
            logger.info(f"Events matched with CSV market_ids: {matched_events:,} ({match_rate:.2f}%)")
            
            if unmatched_market_ids:
                logger.info(f"Unmatched market_ids found: {len(unmatched_market_ids):,}")
                # Show top 10 unmatched
                top_unmatched = sorted(unmatched_market_ids.items(), key=lambda x: x[1], reverse=True)[:10]
                logger.info("Top 10 unmatched market_ids:")
                for mid, count in top_unmatched:
                    logger.info(f"  market_id {mid}: {count:,} events")
            
            results[table_name] = {
                'total': total_events,
                'extracted': matched_with_market_id,
                'matched': matched_events,
                'extraction_rate': extraction_rate,
                'match_rate': match_rate,
                'unmatched_ids': len(unmatched_market_ids)
            }
        
        # Summary
        logger.info(f"\n{'='*70}")
        logger.info("SUMMARY")
        logger.info(f"{'='*70}")
        
        total_all = sum(r['total'] for r in results.values())
        total_matched = sum(r['matched'] for r in results.values())
        overall_rate = (total_matched / total_all * 100) if total_all > 0 else 0
        
        for table_name, result in results.items():
            alias = "MOOV2" if table_name == "events_managed_oracle" else "OOV2"
            logger.info(f"\n{alias} ({table_name}):")
            logger.info(f"  Total RequestPrice events: {result['total']:,}")
            logger.info(f"  Matched with CSV: {result['matched']:,} ({result['match_rate']:.2f}%)")
        
        logger.info(f"\nGRAND TOTAL:")
        logger.info(f"  Total RequestPrice events (both tables): {total_all:,}")
        logger.info(f"  Total matched with CSV: {total_matched:,} ({overall_rate:.2f}%)")
        
        logger.info("\nAnalysis complete!")
    
    finally:
        await pool.close()

if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials not set.")
    elif not POLY_CSV_DIR:
        logger.error("CSV directory not set.")
    elif not os.path.exists(POLY_CSV_PATH):
        logger.error(f"CSV file not found at {POLY_CSV_PATH}.")
    else:
        asyncio.run(main())
