#!/usr/bin/env python3
"""
Script to query and print OrderFilled and OrdersMatched events from events_ctf_exchange
with their semantic argument names.
"""

import os
import json
import asyncio
import asyncpg
import logging

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger()

# --- CONFIGURATION ---
DB_CONFIG = {
    "socket_env": "PG_SOCKET",
    "port_env": "POLY_PG_PORT",
    "name_env": "POLY_DB",
    "user_env": "POLY_DB_CLI",
    "pass_env": "POLY_DB_CLI_PASS"
}

SEMANTIC_SCHEMA_FILE = "semantic_event_args.json"
EVENT_ARGS_FILE = "event_args.json"
TABLE_NAME = "events_ctf_exchange"
EVENTS_TO_QUERY = ["OrderFilled", "OrdersMatched"]
LIMIT = 10  # Number of events to fetch per event type


async def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        for key, env_var in DB_CONFIG.items():
            if env_var not in os.environ:
                logger.error(f"Missing environment variable: {env_var}")
                return None
        
        conn = await asyncpg.connect(
            host=os.getenv(DB_CONFIG['socket_env']),
            port=os.getenv(DB_CONFIG['port_env']),
            database=os.getenv(DB_CONFIG['name_env']),
            user=os.getenv(DB_CONFIG['user_env']),
            password=os.getenv(DB_CONFIG['pass_env'])
        )
        logger.info("✅ Database connection established")
        return conn
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return None


def parse_event_arg(value_bytes, arg_type):
    """Parse event argument based on its type."""
    try:
        if isinstance(value_bytes, bytes):
            hex_str = value_bytes.hex()
        else:
            hex_str = value_bytes.lower().replace('0x', '')
        
        if arg_type == 'bytes32':
            return f"0x{hex_str}"
        elif arg_type == 'address':
            # Address is the last 40 hex chars (20 bytes)
            return f"0x{hex_str[-40:]}"
        elif arg_type == 'uint256':
            return int(hex_str, 16)
        else:
            return f"0x{hex_str}"
    except Exception as e:
        logger.error(f"Error parsing arg: {e}")
        return None


def extract_arg_from_data(data_hex, slot):
    """Extract a 32-byte value from the data field at a specific slot."""
    try:
        if isinstance(data_hex, bytes):
            data_hex = data_hex.hex()
        else:
            data_hex = data_hex.lower().replace('0x', '')
        
        # Each slot is 64 hex chars (32 bytes)
        start = slot * 64
        end = start + 64
        return data_hex[start:end]
    except Exception as e:
        logger.error(f"Error extracting from data: {e}")
        return None


async def query_and_print_events(conn, semantic_schema, event_args_schema):
    """Query events and print them with semantic argument names."""
    
    for event_name in EVENTS_TO_QUERY:
        logger.info(f"\n{'='*80}")
        logger.info(f"Querying {event_name} events from {TABLE_NAME} (limit: {LIMIT})")
        logger.info(f"{'='*80}")
        
        # Get the semantic schema for this event
        semantic_event_schema = semantic_schema.get("events_ctf_exchange", {}).get(event_name, {})
        event_location_schema = event_args_schema.get("events_ctf_exchange", {}).get(event_name, {})
        
        if not semantic_event_schema:
            logger.warning(f"No semantic schema found for {event_name}")
            continue
        
        # Query the events
        query = f"""
            SELECT block_number, transaction_hash, topics, data, timestamp_ms
            FROM {TABLE_NAME}
            WHERE event_name = $1
            ORDER BY block_number DESC, log_index DESC
            LIMIT $2
        """
        
        try:
            records = await conn.fetch(query, event_name, LIMIT)
            logger.info(f"Found {len(records)} {event_name} events\n")
            
            for idx, record in enumerate(records, 1):
                print(f"\n--- Event #{idx} ---")
                print(f"Block Number: {record['block_number']}")
                print(f"Transaction Hash: {record['transaction_hash']}")
                print(f"Timestamp: {record['timestamp_ms']} ms")
                print(f"\nEvent Arguments:")
                
                # Parse all arguments according to semantic schema
                for arg_index_str, arg_info in sorted(semantic_event_schema.items(), key=lambda x: int(x[0])):
                    arg_index = int(arg_index_str)
                    arg_name = arg_info.get('name', f'arg{arg_index}')
                    arg_type = arg_info.get('type', 'unknown')
                    
                    value = None
                    
                    # Event signature is in topics[0]
                    # First indexed param is in topics[1], second in topics[2], third in topics[3]
                    # Non-indexed params are in data field
                    
                    if event_name == "OrderFilled":
                        # OrderFilled has 3 indexed params: orderHash, maker, taker (in topics[1,2,3])
                        # And 5 non-indexed params: makerAssetId, takerAssetId, making, taking, fee (in data)
                        if arg_index == 0:  # orderHash
                            if len(record['topics']) > 1:
                                value = parse_event_arg(record['topics'][1], arg_type)
                        elif arg_index == 1:  # maker
                            if len(record['topics']) > 2:
                                value = parse_event_arg(record['topics'][2], arg_type)
                        elif arg_index == 2:  # taker
                            if len(record['topics']) > 3:
                                value = parse_event_arg(record['topics'][3], arg_type)
                        elif arg_index >= 3:  # makerAssetId onwards (in data)
                            if record['data']:
                                data_slot = arg_index - 3
                                hex_value = extract_arg_from_data(record['data'], data_slot)
                                if hex_value:
                                    value = parse_event_arg(hex_value, arg_type)
                    
                    elif event_name == "OrdersMatched":
                        # OrdersMatched has 2 indexed params: orderHash, maker (in topics[1,2])
                        # And 4 non-indexed params: makerAssetId, takerAssetId, making, taking (in data)
                        if arg_index == 0:  # orderHash
                            if len(record['topics']) > 1:
                                value = parse_event_arg(record['topics'][1], arg_type)
                        elif arg_index == 1:  # maker
                            if len(record['topics']) > 2:
                                value = parse_event_arg(record['topics'][2], arg_type)
                        elif arg_index >= 2:  # makerAssetId onwards (in data)
                            if record['data']:
                                data_slot = arg_index - 2
                                hex_value = extract_arg_from_data(record['data'], data_slot)
                                if hex_value:
                                    value = parse_event_arg(hex_value, arg_type)
                    
                    if value is not None:
                        print(f"  [{arg_index}] {arg_name} ({arg_type}): {value}")
                    else:
                        print(f"  [{arg_index}] {arg_name} ({arg_type}): <not found>")
                
                print()
        
        except Exception as e:
            logger.error(f"Error querying {event_name}: {e}")
            import traceback
            traceback.print_exc()


async def main():
    """Main function."""
    # Load semantic schema
    try:
        with open(SEMANTIC_SCHEMA_FILE, 'r') as f:
            semantic_schema = json.load(f)
        logger.info(f"✅ Loaded semantic schema from {SEMANTIC_SCHEMA_FILE}")
    except FileNotFoundError:
        logger.error(f"❌ Semantic schema file not found: {SEMANTIC_SCHEMA_FILE}")
        return
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON in semantic schema: {e}")
        return
    
    # Load event args schema
    try:
        with open(EVENT_ARGS_FILE, 'r') as f:
            event_args_schema = json.load(f)
        logger.info(f"✅ Loaded event args schema from {EVENT_ARGS_FILE}")
    except FileNotFoundError:
        logger.error(f"❌ Event args file not found: {EVENT_ARGS_FILE}")
        return
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON in event args: {e}")
        return
    
    # Connect to database
    conn = await get_db_connection()
    if not conn:
        return
    
    try:
        # Query and print events
        await query_and_print_events(conn, semantic_schema, event_args_schema)
    finally:
        await conn.close()
        logger.info("\n✅ Database connection closed")


if __name__ == "__main__":
    asyncio.run(main())
