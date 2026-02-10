#!/usr/bin/env python3
"""
Builds derivative 'settlements_house' and 'CFT_no_exchange_house' tables
by filtering for specific market condition and asset IDs.

This script processes the main 'settlements' and 'CFT_no_exchange' tables,
decodes the event data within them to find matches, and stores the relevant
rows in the new tables. It is designed to be resumable and processes data
in batches based on block numbers.
"""

import os
import asyncio
import asyncpg
import logging
import json
from collections import defaultdict
from eth_abi import decode
from typing import Dict, Any, List, Set

# ----------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

# --- Database Credentials ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- Processing Configuration ---
# Batch size in BLOCKS for processing and rewind overlap
BLOCK_BATCH_SIZE = 10_000

# Hardcoded start block number if the target tables are empty
HARDCODED_START_BLOCK = 81372608

# Buffer start number as requested
BUFFER_START_NR = 7

# --- Target Market Data ---
# Condition IDs for Democratic and Republican 2026 House control markets
TARGET_CONDITION_IDS: Set[str] = {
    "0xd5d9fc47718bd553592d126b1fa5e87183d27f3936975b0c04cc0f2dec1f1bb4",
    "0x4e4f77e7dbf4cab666e9a1943674d7ae66348e862df03ea6f44b11eb95731928",
}

# Asset IDs for the outcomes of the target markets
TARGET_ASSET_IDS: Set[str] = {
    # Democratic Party Market
    "83247781037352156539108067944461291821683755894607244160607042790356561625563",
    "33156410999665902694791064431724433042010245771106314074312009703157423879038",
    # Republican Party Market
    "65139230827417363158752884968303867495725894165574887635816574090175320800482",
    "17371217118862125782438074585166210555214661810823929795910191856905580975576",
}

# ----------------------------------------------------------------
# EVENT DECODING LOGIC
# ----------------------------------------------------------------

def decode_orders_matched(data: str, topics: List[str]) -> Dict[str, Any]:
    """Decodes data for an OrdersMatched event."""
    try:
        data_hex = bytes.fromhex(data[2:])
        decoded = decode(['uint256', 'uint256', 'uint256', 'uint256'], data_hex)
        return {
            'makerAssetId': str(decoded[0]),
            'takerAssetId': str(decoded[1]),
        }
    except Exception:
        return {}

def decode_order_filled(data: str, topics: List[str]) -> Dict[str, Any]:
    """Decodes data for an OrderFilled event."""
    try:
        data_hex = bytes.fromhex(data[2:])
        decoded = decode(['uint256', 'uint256', 'uint256', 'uint256', 'uint256'], data_hex)
        return {
            'makerAssetId': str(decoded[0]),
            'takerAssetId': str(decoded[1]),
        }
    except Exception:
        return {}

def decode_position_event(data: str, topics: List[str]) -> Dict[str, Any]:
    """Decodes data for PositionSplit or PositionsMerge events."""
    try:
        # The conditionId is the 4th topic (index 3)
        if len(topics) > 3:
            return {'conditionId': topics[3]}
        return {}
    except Exception:
        return {}

# ----------------------------------------------------------------
# DATABASE FUNCTIONS
# ----------------------------------------------------------------

async def connect_db():
    """Creates and returns a database connection pool."""
    return await asyncpg.create_pool(
        host=PG_HOST, port=int(PG_PORT), database=DB_NAME, user=DB_USER, password=DB_PASS, min_size=1, max_size=10
    )

async def create_tables(conn):
    """Creates the target tables if they do not exist, ensuring unique constraints."""
    # --- SETTLEMENTS_HOUSE TABLE ---
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS settlements_house (
            id BIGINT,
            block_number BIGINT NOT NULL,
            transaction_hash TEXT NOT NULL,
            trade JSONB NOT NULL,
            negrisk_adapter_events JSONB,
            type VARCHAR(50) NOT NULL,
            exchange VARCHAR(20) NOT NULL,
            PRIMARY KEY (id),
            CONSTRAINT uk_settlements_house_tx_hash UNIQUE (transaction_hash)
        );
        CREATE INDEX IF NOT EXISTS idx_settlements_house_block ON settlements_house(block_number);
    """)

    # --- CFT_NO_EXCHANGE_HOUSE TABLE ---
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS CFT_no_exchange_house (
            id BIGINT,
            block_number BIGINT NOT NULL,
            transaction_hash TEXT NOT NULL,
            cft_events JSONB,
            negrisk_adapter_events JSONB,
            type VARCHAR(50) NOT NULL,
            PRIMARY KEY (id),
            CONSTRAINT uk_cft_house_tx_hash UNIQUE (transaction_hash)
        );
        CREATE INDEX IF NOT EXISTS idx_cft_house_block ON CFT_no_exchange_house(block_number);
    """)
    logger.info(" 'house' tables ready with unique constraints")

async def get_global_bounds(conn):
    """Gets the min and max block number from the source tables."""
    query = """
        SELECT MIN(min_b), MAX(max_b) FROM (
            SELECT MIN(block_number) as min_b, MAX(block_number) as max_b FROM settlements
            UNION ALL
            SELECT MIN(block_number) as min_b, MAX(block_number) as max_b FROM CFT_no_exchange
        ) t
    """
    row = await conn.fetchrow(query)
    return row[0], row[1]

async def get_safe_start_block(conn, global_min):
    """
    Determines the safe starting block for processing.
    If target tables are empty, uses the hardcoded start block.
    Otherwise, resumes from the last processed block with a safety overlap.
    """
    row_s = await conn.fetchrow("SELECT MAX(block_number) FROM settlements_house")
    max_s = row_s[0] if row_s[0] is not None else 0
    
    row_c = await conn.fetchrow("SELECT MAX(block_number) FROM CFT_no_exchange_house")
    max_c = row_c[0] if row_c[0] is not None else 0
    
    current_max = max(max_s, max_c)
    
    # If no data has been collected yet, start from the hardcoded block number.
    if current_max == 0:
        logger.info(f" No data in target tables. Starting from hardcoded block: {HARDCODED_START_BLOCK}")
        return HARDCODED_START_BLOCK

    # If resuming, rewind by one batch size to handle potential partial writes from a crash.
    safe_start = max(global_min, current_max - BLOCK_BATCH_SIZE)
    logger.info(f" Found Max Block: {current_max}. Resuming from safe block {safe_start}.")
    return safe_start

async def clean_overlap(conn, start_block):
    """Deletes overlapping data from target tables to prevent duplicates on resume."""
    logger.info(f" Cleaning overlap from block {start_block} in 'house' tables...")
    await conn.execute("DELETE FROM settlements_house WHERE block_number >= $1", start_block)
    await conn.execute("DELETE FROM CFT_no_exchange_house WHERE block_number >= $1", start_block)

async def fetch_source_data_in_range(conn, start_block, end_block):
    """Fetches records from the source tables within a given block range."""
    q_s = "SELECT * FROM settlements WHERE block_number >= $1 AND block_number < $2"
    q_c = "SELECT * FROM CFT_no_exchange WHERE block_number >= $1 AND block_number < $2"
    
    settlements = await conn.fetch(q_s, start_block, end_block)
    cfts = await conn.fetch(q_c, start_block, end_block)
    
    return settlements, cfts

async def process_batch(conn, settlement_rows, cft_rows):
    """
    Processes a batch of rows, decodes events to find matches,
    and prepares them for insertion into the 'house' tables.
    """
    insert_settlements = []
    insert_cft = []

    # --- Process Settlements ---
    for row in settlement_rows:
        is_match = False
        try:
            trade = json.loads(row['trade'])
            
            # Check OrdersMatched and OrderFilled events for target asset IDs
            trade_events = trade.get('orders_matched', []) + trade.get('orders_filled', [])
            for event in trade_events:
                if 'OrdersMatched' in event.get('topics', [''])[0]:
                    decoded = decode_orders_matched(event['data'], event['topics'])
                else:
                    decoded = decode_order_filled(event['data'], event['topics'])
                
                if decoded.get('makerAssetId') in TARGET_ASSET_IDS or decoded.get('takerAssetId') in TARGET_ASSET_IDS:
                    is_match = True
                    break
            
            if is_match:
                insert_settlements.append(tuple(row.values()))

        except (json.JSONDecodeError, KeyError, IndexError) as e:
            logger.warning(f"Skipping settlement row due to parsing error (tx: {row['transaction_hash']}): {e}")

    # --- Process CFTs ---
    for row in cft_rows:
        is_match = False
        try:
            # Check position events for target condition IDs
            cft_events = json.loads(row['cft_events'] or '[]')
            adapter_events = json.loads(row['negrisk_adapter_events'] or '[]')
            
            for event in cft_events + adapter_events:
                # Position events have 4 topics
                if len(event.get('topics', [])) == 4:
                    decoded = decode_position_event(event['data'], event['topics'])
                    if decoded.get('conditionId') in TARGET_CONDITION_IDS:
                        is_match = True
                        break
            
            if is_match:
                insert_cft.append(tuple(row.values()))

        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Skipping CFT row due to parsing error (tx: {row['transaction_hash']}): {e}")

    # --- Batch Insert with ON CONFLICT clause for robustness ---
    async with conn.transaction():
        if insert_settlements:
            await conn.executemany("""
                INSERT INTO settlements_house (id, block_number, transaction_hash, trade, negrisk_adapter_events, type, exchange)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (transaction_hash) DO NOTHING
            """, insert_settlements)
            
        if insert_cft:
            await conn.executemany("""
                INSERT INTO CFT_no_exchange_house (id, block_number, transaction_hash, cft_events, negrisk_adapter_events, type)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (transaction_hash) DO NOTHING
            """, insert_cft)
        
    return len(insert_settlements), len(insert_cft)

async def main():
    """Main execution function."""
    pool = await connect_db()
    try:
        async with pool.acquire() as conn:
            await create_tables(conn)
            
            min_blk, max_blk = await get_global_bounds(conn)
            if not min_blk: 
                logger.error("No source data found in 'settlements' or 'CFT_no_exchange' tables.")
                return

            start_blk = await get_safe_start_block(conn, min_blk)
            await clean_overlap(conn, start_blk)

            if start_blk >= max_blk:
                logger.info(" All blocks already processed.")
                return

            logger.info(f" Starting processing from Block {start_blk} to {max_blk}")

            curr = start_blk
            while curr < max_blk:
                next_blk = min(curr + BLOCK_BATCH_SIZE, max_blk + 1)
                logger.info(f"Batch: Block {curr} -> {next_blk-1}")
                
                settlements, cfts = await fetch_source_data_in_range(conn, curr, next_blk)
                if settlements or cfts:
                    s_count, c_count = await process_batch(conn, settlements, cfts)
                    logger.info(f"  -> Found Matches: {s_count} Settlements, {c_count} CFTs")
                else:
                    logger.info("  -> No source events in range")
                
                curr = next_blk
            
            logger.info(" Processing complete.")
                
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())