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

# --- Event Definition ---
# Based on: "ConditionResolution": { "0": { "name": "conditionId", "type": "bytes32" }, "1": { "name": "oracle", "type": "address" }, "2": { "name": "questionId", "type": "bytes32" }, "3": { "name": "outcomeSlotCount", "type": "uint256" }, "4": { "name": "payoutNumerators", "type": "uint256[]" } }
CONDITION_RESOLUTION_EVENT = {
    "signature": "ConditionResolution(bytes32,address,bytes32,uint256,uint256[])",
    # Indexed arguments appear in topics
    "indexed_types": ["bytes32", "address", "bytes32"],  # conditionId, oracle, questionId
    # Non-indexed arguments appear in data
    "data_types": ["uint256", "uint256[]"],  # outcomeSlotCount, payoutNumerators
    "arg_names": ["conditionId", "oracle", "questionId", "outcomeSlotCount", "payoutNumerators"]
}

async def fetch_and_decode_condition_resolution():
    """Fetch and analyze all ConditionResolution events by payout pattern."""
    try:
        pool = await asyncpg.create_pool(
            host=PG_HOST,
            port=PG_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        logger.info("Connected to database.")
        
        async with pool.acquire() as conn:
            # Fetch all ConditionResolution events
            logger.info("Fetching ConditionResolution events...")
            query = """
                SELECT data, topics, block_number, transaction_hash
                FROM events_conditional_tokens
                WHERE event_name = 'ConditionResolution'
            """
            records = await conn.fetch(query)
            
            if not records:
                logger.warning("No ConditionResolution events found.")
                await pool.close()
                return
            
            logger.info(f"Found {len(records):,} ConditionResolution events")
            
            # Categorize by payout patterns
            payout_counts = {
                "(1, 1)": 0,
                "(1, 0)": 0,
                "(0, 1)": 0,
                "other": 0
            }
            other_samples = []
            
            for idx, record in enumerate(records):
                try:
                    data_hex = record['data']
                    topics = record['topics']
                    
                    # Decode indexed arguments from topics
                    decoded_indexed_args = []
                    for i in range(len(CONDITION_RESOLUTION_EVENT['indexed_types'])):
                        if i + 1 < len(topics):
                            topic = topics[i + 1]
                            arg_type = CONDITION_RESOLUTION_EVENT['indexed_types'][i]
                            decoded_value = abi_decode([arg_type], HexBytes(topic))[0]
                            decoded_indexed_args.append(decoded_value)
                    
                    # Decode non-indexed arguments from data
                    decoded_data_args = []
                    if len(data_hex) > 2:
                        data_bytes = HexBytes(data_hex)
                        decoded_data_args = abi_decode(CONDITION_RESOLUTION_EVENT['data_types'], data_bytes)
                    
                    # Get payoutNumerators (last argument)
                    if len(decoded_data_args) >= 2:
                        payout_numerators = tuple(decoded_data_args[1])
                        
                        # Categorize
                        if payout_numerators == (1, 1):
                            payout_counts["(1, 1)"] += 1
                        elif payout_numerators == (1, 0):
                            payout_counts["(1, 0)"] += 1
                        elif payout_numerators == (0, 1):
                            payout_counts["(0, 1)"] += 1
                        else:
                            payout_counts["other"] += 1
                            if len(other_samples) < 5:
                                other_samples.append({
                                    "block": record['block_number'],
                                    "payout": payout_numerators
                                })
                
                except Exception as e:
                    logger.debug(f"Error processing event {idx}: {e}")
                    continue
            
            # Print summary
            total_events = len(records)
            logger.info(f"\n{'='*70}")
            logger.info("ConditionResolution Event Analysis - Payout Patterns")
            logger.info(f"{'='*70}")
            
            logger.info(f"\nTotal Events: {total_events:,}\n")
            
            for pattern in ["(1, 1)", "(1, 0)", "(0, 1)", "other"]:
                count = payout_counts[pattern]
                percentage = (count / total_events) * 100 if total_events > 0 else 0
                logger.info(f"{pattern:10} : {count:8,} ({percentage:6.2f}%)")
            
            # Print samples of "other" payouts
            if other_samples:
                logger.info(f"\n--- Samples of 'Other' Payout Patterns (up to 5) ---")
                for i, sample in enumerate(other_samples, 1):
                    logger.info(f"Sample {i}: Block {sample['block']} - Payout: {sample['payout']}")
            
            logger.info(f"\n{'='*70}")
        
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
        asyncio.run(fetch_and_decode_condition_resolution())