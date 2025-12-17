import os
import asyncio
import asyncpg
import logging
from datetime import datetime, timezone

# --- Setup: Using your connection guide ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

EVENTS_TABLE = "events_managed_oracle"

# --- Logger Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

def decode_ancillary_data(data_hex: str) -> str:
    """Decodes the ancillaryData from the raw event data hex string."""
    if not data_hex or len(data_hex) < 194:
        return "[ancillaryData not present]"
    try:
        offset_hex = data_hex[2 + 128 : 2 + 192]
        offset = int(offset_hex, 16)
        dynamic_data_start_pos = 2 + (offset * 2)
        length_hex = data_hex[dynamic_data_start_pos : dynamic_data_start_pos + 64]
        length = int(length_hex, 16)
        data_start_pos = dynamic_data_start_pos + 64
        ancillary_data_hex = data_hex[data_start_pos : data_start_pos + (length * 2)]
        return bytes.fromhex(ancillary_data_hex).decode('utf-8', errors='replace')
    except (ValueError, IndexError) as e:
        return f"[Decoding Error: {e}]"

def format_topics(event_name: str, topics: list) -> str:
    """Decodes indexed topics into a human-readable string."""
    formatted = []
    try:
        if event_name == 'RequestPrice':
            requester = f"0x{topics[1][-40:]}"
            formatted.append(f"requester: {requester}")
        elif event_name == 'ProposePrice':
            requester = f"0x{topics[1][-40:]}"
            proposer = f"0x{topics[2][-40:]}"
            formatted.append(f"requester: {requester}")
            formatted.append(f"proposer: {proposer}")
        elif event_name in ['DisputePrice', 'Settle']:
            requester = f"0x{topics[1][-40:]}"
            proposer = f"0x{topics[2][-40:]}"
            disputer = f"0x{topics[3][-40:]}"
            formatted.append(f"requester: {requester}")
            formatted.append(f"proposer: {proposer}")
            if disputer != '0x' + '0'*40:
                formatted.append(f"disputer: {disputer}")
    except IndexError:
        return "[Topic decoding error]"
    return ", ".join(formatted)

async def find_and_print_true_lifecycle(pool, start_event):
    """
    Given a starting event, this function finds the true, isolated lifecycle
    by matching on the unique ancillaryData.
    """
    # THE TRUE KEY: The ancillaryData string itself.
    ancillary_data_to_match = decode_ancillary_data(start_event['data'])
    
    if "[Decoding Error" in ancillary_data_to_match or not ancillary_data_to_match:
        logger.warning(f"Could not decode ancillaryData for starting event {start_event['transaction_hash']}. Skipping.")
        return

    logger.info(f"Tracing True Lifecycle for Ancillary Data: \"{ancillary_data_to_match[:70]}...\"")
    logger.info(f"Found starting '{start_event['event_name']}' event in Tx: {start_event['transaction_hash']}")

    # We must fetch all potential events and filter in Python, as we can't query by ancillaryData directly.
    # We use the (requester, identifier, timestamp) as a pre-filter to narrow down the search.
    requester_topic = start_event['topics'][1]
    identifier_hex = start_event['data'][2:66]
    timestamp_hex = start_event['data'][66:130]

    pre_filter_query = f"""
        SELECT event_name, block_number, transaction_hash, timestamp_ms, data, topics
        FROM {EVENTS_TABLE}
        WHERE
            topics[2] = $1 AND
            substring(data from 3 for 64) = $2 AND
            substring(data from 67 for 64) = $3 AND
            event_name IN ('RequestPrice', 'ProposePrice', 'DisputePrice', 'Settle')
        ORDER BY
            block_number, log_index;
    """
    candidate_events = await pool.fetch(pre_filter_query, requester_topic, identifier_hex, timestamp_hex)

    # Now, filter the candidates to find the true lifecycle
    true_lifecycle_events = []
    for event in candidate_events:
        if decode_ancillary_data(event['data']) == ancillary_data_to_match:
            true_lifecycle_events.append(event)

    if not true_lifecycle_events:
        logger.warning("  Could not find any matching lifecycle events.")
        return

    for event in true_lifecycle_events:
        event_time = datetime.fromtimestamp(event['timestamp_ms'] / 1000, tz=timezone.utc)
        decoded_topics = format_topics(event['event_name'], event['topics'])
        
        logger.info(f"  -> {event['event_name']:<15} | Block: {event['block_number']} | Time: {event_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"     Tx: {event['transaction_hash']}")
        logger.info(f"     Topics: [ {decoded_topics} ]")


async def main():
    pool = None
    try:
        pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
        logger.info("Successfully connected to the database.")

        logger.info("\n=============== FINDING 2 SETTLED LIFECYCLES ===============")
        find_settled_query = f"SELECT * FROM {EVENTS_TABLE} WHERE event_name = 'Settle' ORDER BY RANDOM() LIMIT 2;"
        settled_starts = await pool.fetch(find_settled_query)
        if settled_starts:
            for i, start_event in enumerate(settled_starts):
                logger.info(f"\n--- Settled Lifecycle #{i+1} ---")
                await find_and_print_true_lifecycle(pool, start_event)
        else:
            logger.warning("Could not find any 'Settle' events to start from.")

        logger.info("\n=============== FINDING 1 DISPUTED LIFECYCLE ===============")
        find_disputed_query = f"SELECT * FROM {EVENTS_TABLE} WHERE event_name = 'DisputePrice' ORDER BY RANDOM() LIMIT 1;"
        disputed_start = await pool.fetchrow(find_disputed_query)
        if disputed_start:
            logger.info("\n--- Disputed Lifecycle #1 ---")
            await find_and_print_true_lifecycle(pool, disputed_start)
        else:
            logger.warning("Could not find any 'DisputePrice' events to start from.")

    except (Exception, asyncpg.PostgresError) as error:
        logger.error(f"An error occurred: {error}", exc_info=True)
    finally:
        if pool:
            await pool.close()
            logger.info("\nDatabase connection pool closed.")

if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials are not fully set in environment variables.")
    else:
        asyncio.run(main())