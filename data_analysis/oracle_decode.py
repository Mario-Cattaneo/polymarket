import asyncio
import asyncpg
import os
import logging
from typing import Dict, List, Any
from hexbytes import HexBytes

# web3 is only used for its utility functions, no node connection is needed.
from eth_abi import decode as abi_decode
from web3 import Web3

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
# Based on the signatures from the initial prompt and analysis of indexed topics.
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

def decode_argument(arg_type: str, value: Any) -> Any:
    """Decodes a single argument into a more readable format."""
    if isinstance(value, bytes):
        # For bytes32, try to decode as utf-8, otherwise show hex
        if "bytes32" in arg_type:
            try:
                return value.strip(b'\x00').decode('utf-8')
            except UnicodeDecodeError:
                return Web3.to_hex(value)
        # For dynamic bytes, try to decode, otherwise show a snippet
        else:
            try:
                decoded_string = value.decode('utf-8')
                # Sanitize the string from null characters and non-printable chars
                return ''.join(char for char in decoded_string if char.isprintable())
            except UnicodeDecodeError:
                return f"<Non-UTF8 Bytes, len: {len(value)}>"

    if arg_type == "address" and isinstance(value, str):
        return Web3.to_checksum_address(value)
        
    return value

# --- Main Logic ---
async def fetch_and_decode_oov2_events():
    """
    Connects to the database, fetches one of each OOV2 event,
    and prints the decoded, human-readable arguments.
    """
    logger.info("Initializing database connection pool...")
    db_pool = await get_db_pool(DB_CONFIG, pool_size=2)

    if not db_pool:
        logger.error("Could not create database pool.")
        return

    table_name = "oov2"

    try:
        for event_name, definition in OOV2_EVENT_DEFINITIONS.items():
            print("-" * 60)
            logger.info(f"Querying for one '{event_name}' event...")

            async with db_pool.acquire() as conn:
                query = f'SELECT topics, data FROM "{table_name}" WHERE event_name = $1 LIMIT 1'
                record = await conn.fetchrow(query, event_name)

                if not record:
                    logger.warning(f"No event found for '{event_name}'. Skipping.")
                    continue

                logger.info(f"Event found. Decoding arguments for: {definition['signature']}")
                print(f"\nDECODED Arguments for {event_name}:")

                topics = record.get('topics', [])
                data_hex = record.get('data', '0x')
                
                # The first topic is the event signature hash, the rest are indexed args
                indexed_args_raw = [HexBytes(t) for t in topics[1:]]
                data_bytes = HexBytes(data_hex)

                try:
                    # Decode the non-indexed arguments from the 'data' field
                    decoded_data_args = abi_decode(definition['data_types'], data_bytes)
                    
                    # Decode the indexed arguments from the 'topics'
                    decoded_indexed_args = [
                        abi_decode([dtype], val)[0] for dtype, val in zip(definition['indexed_types'], indexed_args_raw)
                    ]

                    # Combine all arguments in the correct order
                    all_args = []
                    indexed_args_iter = iter(decoded_indexed_args)
                    data_args_iter = iter(decoded_data_args)
                    
                    # Reconstruct the argument list based on the original signature order
                    # by checking which ones were indexed.
                    for i, arg_name in enumerate(definition['arg_names']):
                        # This logic assumes indexed arguments always come first in the signature
                        if i < len(definition['indexed_types']):
                            all_args.append(next(indexed_args_iter))
                        else:
                            all_args.append(next(data_args_iter))

                    # Print the final, readable results
                    all_types = definition['indexed_types'] + definition['data_types']
                    for name, type, value in zip(definition['arg_names'], all_types, all_args):
                        readable_value = decode_argument(type, value)
                        print(f"  - {name} ({type}): {readable_value}")

                except Exception as e:
                    logger.error(f"Failed to decode event {event_name}: {e}")
            print()

    finally:
        if db_pool:
            await db_pool.close()
            logger.info("Database connection pool closed.")


if __name__ == "__main__":
    logger.info("Starting OOV2 event argument decoding script.")
    try:
        asyncio.run(fetch_and_decode_oov2_events())
    except Exception as e:
        logger.critical(f"Script failed with an unhandled exception: {e}")