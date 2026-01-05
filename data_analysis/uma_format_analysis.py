import asyncio
import asyncpg
import os
import logging
import re
from typing import Dict, List, Tuple
from collections import defaultdict
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
DB_CONFIG = {
    "socket_env": "PG_SOCKET",
    "port_env": "POLY_PG_PORT",
    "name_env": "POLY_DB",
    "user_env": "POLY_DB_CLI",
    "pass_env": "POLY_DB_CLI_PASS"
}

# --- OOV2 Event ABIs and Argument Types ---
OOV2_EVENT_DEFINITIONS = {
    "RequestPrice": {
        "signature": "RequestPrice(address,bytes32,uint256,bytes,address,uint256,uint256)",
        "indexed_types": ["address"],
        "data_types": ["bytes32", "uint256", "bytes", "address", "uint256", "uint256"],
        "arg_names": ["requester", "identifier", "timestamp", "ancillaryData", "currency", "reward", "finalFee"]
    },
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
    },
    "Settle": {
        "signature": "Settle(address,address,address,bytes32,uint256,bytes,int256,uint256)",
        "indexed_types": ["address", "address", "address"],
        "data_types": ["bytes32", "uint256", "bytes", "int256", "uint256"],
        "arg_names": ["requester", "proposer", "disputer", "identifier", "timestamp", "ancillaryData", "price", "payout"]
    }
}

# --- Helper Functions ---
async def get_db_pool(db_config: dict, pool_size: int):
    """Creates an asyncpg database connection pool."""
    try:
        for key in db_config.values():
            if key not in os.environ:
                logger.error(f"Missing required environment variable: {key}")
                return None
        pool = await asyncpg.create_pool(
            host=os.environ.get(db_config['socket_env']),
            port=os.environ.get(db_config['port_env']),
            database=os.environ.get(db_config['name_env']),
            user=os.environ.get(db_config['user_env']),
            password=os.environ.get(db_config['pass_env']),
            min_size=pool_size,
            max_size=pool_size
        )
        return pool
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        raise e

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

def extract_ancillary_keys(ancillary_data_str: str) -> List[str]:
    """
    Extract all keys from ancillary data string.
    Uses a whitelist of known keys plus generic extraction to handle variable format.
    The ancillary data is a string with key:value pairs but values can contain colons.
    """
    # Known standard keys that should appear in UMA data
    known_keys = {'q', 'title', 'description', 'market_id', 'res_data', 
                  'p1', 'p2', 'p3', 'initializer'}
    
    found_keys = []
    
    # Check for each known key
    for key in known_keys:
        # Look for the key as a standalone word followed by colon
        # Use word boundaries and require colon immediately or after optional space
        pattern = r'\b' + re.escape(key) + r'\s*:'
        if re.search(pattern, ancillary_data_str):
            found_keys.append(key)
    
    # Also look for any other keys that might exist (generic pattern)
    # Only match if they're NOT in the description/text portion
    # Keys appear as isolated identifiers before colons, typically after punctuation or start
    # Exclude matches that appear within longer words or common text patterns
    pattern_generic = r'(?:^|[.,\s])([a-z][a-z0-9_]*)\s*:(?=\s*[0-9x\-a-f])'
    generic_matches = re.findall(pattern_generic, ancillary_data_str, re.IGNORECASE)
    
    # Filter out known ones (already added) and likely false positives
    false_positives = {'https', 'here', 'at', 'is', 'if', 'see', 'will', 'be', 'the', 'this', 'or'}
    for key in generic_matches:
        if key not in known_keys and key not in false_positives and key not in found_keys:
            found_keys.append(key)
    
    return found_keys

async def fetch_and_analyze_uma_format():
    """
    Fetches events from oov2 and events_managed_oracle tables,
    analyzes identifier and ancillary data format.
    """
    logger.info("Starting UMA format analysis script.")
    logger.info("Initializing database connection pool...")
    db_pool = await get_db_pool(DB_CONFIG, pool_size=2)

    if not db_pool:
        logger.error("Could not create database pool.")
        return

    # Statistics dictionaries
    identifier_counts = defaultdict(int)
    ancillary_keys_counts = defaultdict(int)
    total_events = 0
    sample_ancillary_data = None
    sample_event_with_missing_keys = None

    try:
        # Analyze oov2 table
        logger.info("=" * 70)
        logger.info("Analyzing OOV2 table...")
        logger.info("=" * 70)
        
        async with db_pool.acquire() as conn:
            # Get total count
            total_oov2 = await conn.fetchval('SELECT COUNT(*) FROM "oov2"')
            logger.info(f"Total events in oov2: {total_oov2}")
            
            # Fetch all events with identifier and ancillary data
            query = '''
                SELECT topics, data, event_name 
                FROM "oov2"
            '''
            records = await conn.fetch(query)
            
            for record in records:
                event_name = record.get('event_name')
                
                if event_name not in OOV2_EVENT_DEFINITIONS:
                    continue
                
                definition = OOV2_EVENT_DEFINITIONS[event_name]
                topics = record.get('topics', [])
                data_hex = record.get('data', '0x')
                
                try:
                    indexed_args_raw = [HexBytes(t) for t in topics[1:]]
                    data_bytes = HexBytes(data_hex)
                    
                    decoded_data_args = abi_decode(definition['data_types'], data_bytes)
                    decoded_indexed_args = [
                        abi_decode([dtype], val)[0] for dtype, val in zip(definition['indexed_types'], indexed_args_raw)
                    ]
                    
                    # Reconstruct arguments
                    all_args = []
                    indexed_args_iter = iter(decoded_indexed_args)
                    data_args_iter = iter(decoded_data_args)
                    
                    for i, arg_name in enumerate(definition['arg_names']):
                        if i < len(definition['indexed_types']):
                            all_args.append(next(indexed_args_iter))
                        else:
                            all_args.append(next(data_args_iter))
                    
                    # Process identifier and ancillary data
                    all_types = definition['indexed_types'] + definition['data_types']
                    for name, type_str, value in zip(definition['arg_names'], all_types, all_args):
                        if name == 'identifier':
                            identifier_str = decode_argument(type_str, value)
                            identifier_counts[identifier_str] += 1
                            total_events += 1
                        elif name == 'ancillaryData':
                            ancillary_str = decode_argument(type_str, value)
                            keys = extract_ancillary_keys(ancillary_str)
                            
                            # Store a sample and check for missing keys
                            if sample_ancillary_data is None:
                                sample_ancillary_data = ancillary_str
                            
                            # Check if this event is missing any expected keys
                            expected_keys = {'q', 'title', 'description', 'market_id', 'res_data', 'p1', 'p2', 'p3', 'initializer'}
                            found_keys = set(keys)
                            missing = expected_keys - found_keys
                            if missing and sample_event_with_missing_keys is None:
                                sample_event_with_missing_keys = (ancillary_str, missing, found_keys)
                            
                            for key in keys:
                                ancillary_keys_counts[key] += 1
                
                except Exception as e:
                    logger.warning(f"Failed to decode event {event_name}: {e}")

        # Analyze events_managed_oracle table
        logger.info("=" * 70)
        logger.info("Analyzing events_managed_oracle table...")
        logger.info("=" * 70)
        
        async with db_pool.acquire() as conn:
            total_emo = await conn.fetchval('SELECT COUNT(*) FROM "events_managed_oracle"')
            logger.info(f"Total events in events_managed_oracle: {total_emo}")
            
            # Fetch events with topics and data (same structure as oov2)
            query = '''
                SELECT topics, data, event_name 
                FROM "events_managed_oracle"
            '''
            
            try:
                records = await conn.fetch(query)
                
                for record in records:
                    event_name = record.get('event_name')
                    
                    if event_name not in OOV2_EVENT_DEFINITIONS:
                        continue
                    
                    definition = OOV2_EVENT_DEFINITIONS[event_name]
                    topics = record.get('topics', [])
                    data_hex = record.get('data', '0x')
                    
                    try:
                        indexed_args_raw = [HexBytes(t) for t in topics[1:]]
                        data_bytes = HexBytes(data_hex)
                        
                        decoded_data_args = abi_decode(definition['data_types'], data_bytes)
                        decoded_indexed_args = [
                            abi_decode([dtype], val)[0] for dtype, val in zip(definition['indexed_types'], indexed_args_raw)
                        ]
                        
                        # Reconstruct arguments
                        all_args = []
                        indexed_args_iter = iter(decoded_indexed_args)
                        data_args_iter = iter(decoded_data_args)
                        
                        for i, arg_name in enumerate(definition['arg_names']):
                            if i < len(definition['indexed_types']):
                                all_args.append(next(indexed_args_iter))
                            else:
                                all_args.append(next(data_args_iter))
                        
                        # Process identifier and ancillary data
                        all_types = definition['indexed_types'] + definition['data_types']
                        for name, type_str, value in zip(definition['arg_names'], all_types, all_args):
                            if name == 'identifier':
                                identifier_str = decode_argument(type_str, value)
                                identifier_counts[identifier_str] += 1
                                total_events += 1
                            elif name == 'ancillaryData':
                                ancillary_str = decode_argument(type_str, value)
                                keys = extract_ancillary_keys(ancillary_str)
                                
                                # Store a sample and check for missing keys
                                if sample_ancillary_data is None:
                                    sample_ancillary_data = ancillary_str
                                
                                # Check if this event is missing any expected keys
                                expected_keys = {'q', 'title', 'description', 'market_id', 'res_data', 'p1', 'p2', 'p3', 'initializer'}
                                found_keys = set(keys)
                                missing = expected_keys - found_keys
                                if missing and sample_event_with_missing_keys is None:
                                    sample_event_with_missing_keys = (ancillary_str, missing, found_keys)
                                
                                for key in keys:
                                    ancillary_keys_counts[key] += 1
                    
                    except Exception as e:
                        logger.warning(f"Failed to decode event {event_name} from events_managed_oracle: {e}")
            
            except Exception as e:
                logger.warning(f"Could not query events_managed_oracle: {e}")

    finally:
        if db_pool:
            await db_pool.close()
            logger.info("Database connection pool closed.")

    # Print results
    logger.info("=" * 70)
    logger.info("ANALYSIS RESULTS")
    logger.info("=" * 70)
    
    print("\n" + "=" * 70)
    print("IDENTIFIER ANALYSIS")
    print("=" * 70)
    print(f"Total events analyzed: {total_events}\n")
    
    if identifier_counts:
        # Sort by count descending
        sorted_identifiers = sorted(identifier_counts.items(), key=lambda x: x[1], reverse=True)
        
        for identifier, count in sorted_identifiers:
            percentage = (count / total_events * 100) if total_events > 0 else 0
            print(f"  {identifier}: {count} times ({percentage:.2f}%)")
    else:
        print("  No identifiers found.")
    
    print("\n" + "=" * 70)
    print("ANCILLARY DATA KEYS ANALYSIS")
    print("=" * 70)
    
    if ancillary_keys_counts:
        # Sort by count descending
        sorted_keys = sorted(ancillary_keys_counts.items(), key=lambda x: x[1], reverse=True)
        
        print()
        
        for key, count in sorted_keys:
            percentage = (count / total_events * 100) if total_events > 0 else 0
            print(f"  '{key}': {count} times ({percentage:.2f}% of events)")
    else:
        print("  No ancillary data keys found.")
    
    print("\n" + "=" * 70)
    print("SAMPLE ANCILLARY DATA")
    print("=" * 70)
    if sample_ancillary_data:
        print(f"\nFirst sample ancillary data:\n{sample_ancillary_data[:1000]}...\n")
    
    if sample_event_with_missing_keys:
        data, missing_keys, found_keys = sample_event_with_missing_keys
        print(f"\nSample with missing keys from {{'q', 'title', 'description', 'market_id', 'res_data', 'p1', 'p2', 'p3', 'initializer'}}:")
        print(f"Missing: {missing_keys}")
        print(f"Found: {found_keys}")
        print(f"Data sample:\n{data[:1500]}...\n")
    
    print("=" * 70)


if __name__ == "__main__":
    try:
        asyncio.run(fetch_and_analyze_uma_format())
    except Exception as e:
        logger.critical(f"Script failed with an unhandled exception: {e}")
