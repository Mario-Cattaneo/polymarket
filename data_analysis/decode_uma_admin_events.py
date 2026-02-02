import asyncio
import asyncpg
import os
import logging
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

# --- MOO V2 Adapter Address ---
MOO_UMA_ADAPTER_ADDRESS = "0x65070BE91477460D8A7AeEb94ef92fe056C2f2A7"

# --- Event Definitions ---
EVENTS = {
    "QuestionPaused": {
        "signature": "QuestionPaused(bytes32)",
        "indexed_types": ["bytes32"],  # questionId (indexed)
        "data_types": [],  # no data args
        "arg_names": ["questionId"]
    },
    "QuestionFlagged": {
        "signature": "QuestionFlagged(bytes32)",
        "indexed_types": ["bytes32"],  # questionId (indexed)
        "data_types": [],
        "arg_names": ["questionId"]
    },
    "QuestionUnpaused": {
        "signature": "QuestionUnpaused(bytes32)",
        "indexed_types": ["bytes32"],  # questionId (indexed)
        "data_types": [],
        "arg_names": ["questionId"]
    },
    "QuestionUnflagged": {
        "signature": "QuestionUnflagged(bytes32)",
        "indexed_types": ["bytes32"],  # questionId (indexed)
        "data_types": [],
        "arg_names": ["questionId"]
    },
    "QuestionReset": {
        "signature": "QuestionReset(bytes32)",
        "indexed_types": ["bytes32"],  # questionId (indexed)
        "data_types": [],
        "arg_names": ["questionId"]
    },
}

async def fetch_and_decode_events():
    """Fetch and decode admin question events from MOO V2 Adapter."""
    try:
        pool = await asyncpg.create_pool(
            host=PG_HOST,
            port=PG_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        logger.info("Connected to database.")
        
        # Check which events exist and their counts
        async with pool.acquire() as conn:
            logger.info(f"\n--- Event Counts in MOO V2 Adapter ({MOO_UMA_ADAPTER_ADDRESS}) ---")
            
            event_counts = {}
            for event_name in EVENTS.keys():
                result = await conn.fetchval(
                    """
                    SELECT COUNT(*) FROM events_uma_adapter
                    WHERE event_name = $1
                    AND LOWER(contract_address) = LOWER($2)
                    """,
                    event_name,
                    MOO_UMA_ADAPTER_ADDRESS
                )
                event_counts[event_name] = result or 0
                logger.info(f"{event_name}: {event_counts[event_name]:,} events")
            
            # Find which events have data
            events_with_data = [e for e, count in event_counts.items() if count > 0]
            
            if not events_with_data:
                logger.warning("\nNo admin events found in this adapter.")
                await pool.close()
                return
            
            # Decode first 10 events of each type
            logger.info(f"\n{'=' * 100}")
            
            for event_name in events_with_data:
                logger.info(f"\n--- First 10 {event_name} Events ---\n")
                
                query = """
                    SELECT data, topics, block_number, transaction_hash
                    FROM events_uma_adapter
                    WHERE event_name = $1
                    AND LOWER(contract_address) = LOWER($2)
                    ORDER BY block_number ASC
                    LIMIT 10
                """
                records = await conn.fetch(query, event_name, MOO_UMA_ADAPTER_ADDRESS)
                
                event_def = EVENTS[event_name]
                decoded_count = 0
                
                for idx, record in enumerate(records):
                    try:
                        topics = record['topics']
                        block = record['block_number']
                        tx_hash = record['transaction_hash']
                        
                        # Decode indexed argument (questionId from topics[1])
                        questionId = None
                        if len(topics) > 1:
                            questionId = abi_decode(
                                event_def['indexed_types'],
                                HexBytes(topics[1])
                            )[0]
                        
                        if questionId:
                            logger.info(f"Block {block}, TX {tx_hash[:10]}...: questionId = {questionId.hex()}")
                            decoded_count += 1
                    
                    except Exception as e:
                        logger.debug(f"Error decoding event {idx}: {e}")
                        continue
                
                logger.info(f"Decoded {decoded_count}/{len(records)} events\n")
        
        await pool.close()
        logger.info("Analysis complete!")
    
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials not set.")
        logger.error("Please set: PG_SOCKET, POLY_PG_PORT, POLY_DB, POLY_DB_CLI, POLY_DB_CLI_PASS")
    else:
        asyncio.run(fetch_and_decode_events())