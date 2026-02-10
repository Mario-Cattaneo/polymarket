#!/usr/bin/env python3
"""
Build settlements table.
FINAL VERSION:
  - Iterates by BLOCK NUMBER.
  - Correctly handles standalone adapter events like 'PositionsConverted'.
  - Resumable with overlap protection.
"""

import os
import asyncio
import asyncpg
import logging
import json
from collections import defaultdict

# ----------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# Batch size in BLOCKS
BLOCK_BATCH_SIZE = 10_000

# Adapter events we care about
ADAPTER_EVENTS = {'PositionSplit', 'PositionsMerge', 'PositionsConverted'}

# ----------------------------------------------------------------
# DATABASE FUNCTIONS
# ----------------------------------------------------------------

async def connect_db():
    return await asyncpg.create_pool(
        host=PG_HOST, port=int(PG_PORT), database=DB_NAME, user=DB_USER, password=DB_PASS, min_size=1, max_size=10
    )

async def create_tables(conn):
    # --- SETTLEMENTS TABLE ---
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS settlements (
            id BIGSERIAL PRIMARY KEY,
            block_number BIGINT NOT NULL,
            transaction_hash TEXT NOT NULL,
            trade JSONB NOT NULL,
            negrisk_adapter_events JSONB,
            type VARCHAR(50) NOT NULL,
            exchange VARCHAR(20) NOT NULL,
            
            CONSTRAINT uk_settlements_tx_hash UNIQUE (transaction_hash)
        );
        
        CREATE INDEX IF NOT EXISTS idx_settlements_type_block ON settlements(type, block_number ASC);
        CREATE INDEX IF NOT EXISTS idx_settlements_block ON settlements(block_number);
        CREATE INDEX IF NOT EXISTS idx_settlements_exchange ON settlements(exchange);
    """)

    # --- CFT NO EXCHANGE TABLE ---
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS CFT_no_exchange (
            id BIGSERIAL PRIMARY KEY,
            block_number BIGINT NOT NULL,
            transaction_hash TEXT NOT NULL,
            cft_events JSONB, -- Can be NULL for adapter-only events
            negrisk_adapter_events JSONB,
            type VARCHAR(50) NOT NULL,
            
            CONSTRAINT uk_cft_tx_hash UNIQUE (transaction_hash)
        );
        
        CREATE INDEX IF NOT EXISTS idx_cft_type_block ON CFT_no_exchange(type, block_number ASC);
        CREATE INDEX IF NOT EXISTS idx_cft_block ON CFT_no_exchange(block_number);
    """)
    logger.info("Tables ready")

async def get_global_bounds(conn):
    query = """
        SELECT MIN(min_b), MAX(max_b) FROM (
            SELECT MIN(block_number) as min_b, MAX(block_number) as max_b FROM events_ctf_exchange
            UNION ALL
            SELECT MIN(block_number) as min_b, MAX(block_number) as max_b FROM events_neg_risk_exchange
            UNION ALL
            SELECT MIN(block_number) as min_b, MAX(block_number) as max_b FROM events_conditional_tokens
            UNION ALL
            SELECT MIN(block_number) as min_b, MAX(block_number) as max_b FROM events_neg_risk_adapter
        ) t
    """
    row = await conn.fetchrow(query)
    return row[0], row[1]

async def get_safe_start_block(conn, global_min):
    row_s = await conn.fetchrow("SELECT MAX(block_number) FROM settlements")
    max_s = row_s[0] if row_s[0] is not None else 0
    
    row_c = await conn.fetchrow("SELECT MAX(block_number) FROM CFT_no_exchange")
    max_c = row_c[0] if row_c[0] is not None else 0
    
    current_max = max(max_s, max_c)
    if current_max == 0: return global_min
    
    safe_start = max(global_min, current_max - BLOCK_BATCH_SIZE)
    logger.info(f"Found Max Block: {current_max}. Rewinding to {safe_start}.")
    return safe_start

async def clean_overlap(conn, start_block):
    """Delete overlapping events from BOTH tables to prevent duplicates on resume."""
    logger.info(f"Cleaning overlap from block {start_block}...")
    await conn.execute("DELETE FROM settlements WHERE block_number >= $1", start_block)
    await conn.execute("DELETE FROM CFT_no_exchange WHERE block_number >= $1", start_block)

async def fetch_events_in_block_range(conn, start_block, end_block):
    q_ctf = """
        SELECT transaction_hash, event_name, topics, data, block_number, 'base' as source
        FROM events_ctf_exchange WHERE block_number >= $1 AND block_number < $2
    """
    q_neg = """
        SELECT transaction_hash, event_name, topics, data, block_number, 'negrisk' as source
        FROM events_neg_risk_exchange WHERE block_number >= $1 AND block_number < $2
    """
    q_pos = """
        SELECT transaction_hash, event_name, topics, data, block_number, 'cft' as source
        FROM events_conditional_tokens WHERE block_number >= $1 AND block_number < $2
    """
    q_adap = """
        SELECT transaction_hash, event_name, topics, data, block_number, 'adapter' as source
        FROM events_neg_risk_adapter WHERE block_number >= $1 AND block_number < $2
    """
    
    ctf = await conn.fetch(q_ctf, start_block, end_block)
    neg = await conn.fetch(q_neg, start_block, end_block)
    pos = await conn.fetch(q_pos, start_block, end_block)
    adap = await conn.fetch(q_adap, start_block, end_block)
    
    return ctf + neg + pos + adap

def serialize_events(event_list):
    if not event_list: return None
    return json.dumps([
        {'event': x['event_name'], 'data': x['data'], 'topics': x['topics']} 
        for x in event_list
    ])

async def process_batch(conn, events):
    tx_groups = defaultdict(list)
    for e in events:
        tx_groups[e['transaction_hash']].append(e)
    
    insert_settlements = []
    insert_cft = []
    
    for tx_hash, group in tx_groups.items():
        orders_matched = []
        orders_filled = []
        cft_events = []
        adapter_events = []
        
        ref_block = group[0]['block_number']
        exchange = 'base'
        
        for e in group:
            source = e['source']
            name = e['event_name']
            
            if source == 'negrisk':
                exchange = 'negrisk'
            
            if source == 'adapter':
                if name in ADAPTER_EVENTS:
                    adapter_events.append(e)
            elif source == 'cft':
                if name in ['PositionSplit', 'PositionsMerge']:
                    cft_events.append(e)
            elif name == 'OrdersMatched':
                orders_matched.append(e)
            elif name == 'OrderFilled':
                orders_filled.append(e)
        
        has_trade = len(orders_matched) > 0
        has_cft = len(cft_events) > 0
        has_adapter = len(adapter_events) > 0
        
        # --- MATCHING LOGIC ---

        # CASE 1: SETTLEMENT (Trade exists)
        if has_trade:
            # Check for conversion FIRST - it has absolute priority
            has_convert = any(x['event_name'] == 'PositionsConverted' for x in adapter_events)
            
            if has_convert:
                sType = 'conversion'
            else:
                # Only check split/merge if no conversion
                has_split = any(x['event_name'] == 'PositionSplit' for x in cft_events)
                has_merge = any(x['event_name'] == 'PositionsMerge' for x in cft_events)
                
                if has_split:
                    sType = 'split'
                elif has_merge:
                    sType = 'merge'
                else:
                    sType = 'complementary'
            
            trade_obj = {
                'transaction_hash': tx_hash,
                'orders_matched': [{'data': x['data'], 'topics': x['topics']} for x in orders_matched],
                'orders_filled': [{'data': x['data'], 'topics': x['topics']} for x in orders_filled],
                'position_events': [{'data': x['data'], 'topics': x['topics']} for x in cft_events]
            }
            
            adap_json = serialize_events(adapter_events)
            
            insert_settlements.append((ref_block, tx_hash, json.dumps(trade_obj), adap_json, sType, exchange))

        # CASE 2: NO TRADE (CFT or Adapter exists)
        elif has_cft or has_adapter:
            cft_type = 'unknown'
            
            if has_adapter:
                # Prioritize Adapter event for naming
                if any(x['event_name'] == 'PositionsConverted' for x in adapter_events):
                    cft_type = 'negrisk_PositionsConverted'
                elif any(x['event_name'] == 'PositionSplit' for x in adapter_events):
                    cft_type = 'negrisk_PositionSplit'
                elif any(x['event_name'] == 'PositionsMerge' for x in adapter_events):
                    cft_type = 'negrisk_PositionsMerge'
                else:
                    cft_type = f"negrisk_{adapter_events[0]['event_name']}"
            
            elif has_cft:
                if any(x['event_name'] == 'PositionSplit' for x in cft_events):
                    cft_type = 'PositionSplit_alone'
                elif any(x['event_name'] == 'PositionsMerge' for x in cft_events):
                    cft_type = 'PositionsMerge_alone'
            
            cft_json = serialize_events(cft_events)
            adap_json = serialize_events(adapter_events)
            
            insert_cft.append((ref_block, tx_hash, cft_json, adap_json, cft_type))

    async with conn.transaction():
        if insert_settlements:
            await conn.executemany("""
                INSERT INTO settlements (block_number, transaction_hash, trade, negrisk_adapter_events, type, exchange)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (transaction_hash) DO NOTHING
            """, insert_settlements)
            
        if insert_cft:
            await conn.executemany("""
                INSERT INTO CFT_no_exchange (block_number, transaction_hash, cft_events, negrisk_adapter_events, type)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (transaction_hash) DO NOTHING
            """, insert_cft)
        
    return len(insert_settlements), len(insert_cft)

async def main():
    pool = await connect_db()
    try:
        async with pool.acquire() as conn:
            await create_tables(conn)
            
            min_blk, max_blk = await get_global_bounds(conn)
            if not min_blk: 
                logger.error("No source data found.")
                return

            start_blk = await get_safe_start_block(conn, min_blk)
            await clean_overlap(conn, start_blk)

            if start_blk > max_blk:
                logger.info("All blocks already processed.")
                return

            logger.info(f"Starting processing from Block {start_blk}")

            curr = start_blk
            while curr < max_blk:
                next_blk = curr + BLOCK_BATCH_SIZE
                logger.info(f"Batch: Block {curr} -> {next_blk}")
                
                events = await fetch_events_in_block_range(conn, curr, next_blk)
                if events:
                    s_count, c_count = await process_batch(conn, events)
                    logger.info(f"  -> Settlements: {s_count}, CFT: {c_count}")
                else:
                    logger.info("  -> No events in range")
                
                curr = next_blk
                
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())