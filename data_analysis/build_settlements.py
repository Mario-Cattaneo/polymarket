#!/usr/bin/env python3
"""
Build settlements table by matching OrdersFilled/OrdersMatched with PositionSplit/PositionsMerge events.

Strategy:
1. Fetch all events in 1-day time windows (indexed by timestamp)
2. Group by transaction_hash
3. For each tx_hash group:
   - Collect all OrdersFilled/OrdersMatched events (the "trade")
   - Check if PositionSplit or PositionsMerge exists in same tx_hash
   - Determine type: "complementary" if no split/merge, else "split" or "merge"
   - Determine exchange: "negrisk" or "base" (from contract_address)
4. Insert into settlements table as JSONB
"""

import os
import asyncio
import asyncpg
import pandas as pd
import logging
import json
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# ----------------------------------------------------------------
# SETUP & CONFIGURATION
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

# Database Credentials
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# Exchange Addresses
CTFE_ADDR = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e".lower()
NEGRISK_ADDR = "0x9e4a16ddf5a656a3fc1af9872b2c207024558bd0".lower()

# Time batch size (1 day in milliseconds)
BATCH_TIME_RANGE_MS = 12 * 60 * 60 * 1000

# ----------------------------------------------------------------
# DATABASE FUNCTIONS
# ----------------------------------------------------------------

async def connect_db():
    """Create asyncpg connection pool."""
    return await asyncpg.create_pool(
        host=PG_HOST,
        port=int(PG_PORT),
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        min_size=1,
        max_size=10,
    )

async def create_settlements_table(conn):
    """Create settlements table if it doesn't exist."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS settlements (
            id BIGSERIAL,
            timestamp_ms BIGINT NOT NULL,
            transaction_hash TEXT NOT NULL,
            trade JSONB NOT NULL,
            type VARCHAR(20) NOT NULL,
            exchange VARCHAR(20) NOT NULL
        );
        
        CREATE INDEX IF NOT EXISTS idx_settlements_timestamp ON settlements(timestamp_ms);
        CREATE INDEX IF NOT EXISTS idx_settlements_type ON settlements(type);
        CREATE INDEX IF NOT EXISTS idx_settlements_exchange ON settlements(exchange);
        CREATE INDEX IF NOT EXISTS idx_settlements_tx_hash ON settlements(transaction_hash);
    """)
    logger.info("✅ Settlements table ready")

async def get_last_covered_timestamp(conn):
    """Get the last covered timestamp to resume from."""
    row = await conn.fetchrow("SELECT MAX(timestamp_ms) as max_ts FROM settlements")
    return row['max_ts'] if row['max_ts'] else None

async def create_cft_no_exchange_table(conn):
    """Create CFT_no_exchange table to store unmatched position events."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS CFT_no_exchange (
            id BIGSERIAL,
            timestamp_ms BIGINT NOT NULL,
            transaction_hash TEXT NOT NULL,
            event_name VARCHAR(50) NOT NULL,
            event_data JSONB NOT NULL
        );
        
        CREATE INDEX IF NOT EXISTS idx_cft_no_exchange_timestamp ON CFT_no_exchange(timestamp_ms);
        CREATE INDEX IF NOT EXISTS idx_cft_no_exchange_event ON CFT_no_exchange(event_name);
        CREATE INDEX IF NOT EXISTS idx_cft_no_exchange_tx_hash ON CFT_no_exchange(transaction_hash);
    """)
    logger.info("✅ CFT_no_exchange table ready")

async def get_min_max_timestamps(conn):
    """Get min/max timestamps from all involved tables (NO event filtering!)."""
    query = """
        SELECT 
            MIN(min_ts) as global_min,
            MAX(max_ts) as global_max
        FROM (
            SELECT MIN(timestamp_ms) as min_ts, MAX(timestamp_ms) as max_ts FROM events_ctf_exchange
            UNION ALL
            SELECT MIN(timestamp_ms) as min_ts, MAX(timestamp_ms) as max_ts FROM events_neg_risk_exchange
            UNION ALL
            SELECT MIN(timestamp_ms) as min_ts, MAX(timestamp_ms) as max_ts FROM events_conditional_tokens
        ) t
    """
    row = await conn.fetchrow(query)
    return row['global_min'], row['global_max']

async def fetch_events_in_time_range(conn, min_ts_ms, max_ts_ms):
    """
    Fetch all relevant events in the time range, sorted by timestamp then tx_hash.
    Since events with same tx_hash share the same timestamp, this clusters matching events.
    """
    
    # Fetch ALL events from CTF exchange
    trade_query_ctf = """
        SELECT 
            transaction_hash,
            event_name,
            topics,
            data,
            timestamp_ms,
            LOWER(contract_address) as contract
        FROM events_ctf_exchange
        WHERE timestamp_ms BETWEEN $1 AND $2
        ORDER BY timestamp_ms ASC, transaction_hash ASC
    """
    
    # Fetch ALL events from NegRisk exchange
    trade_query_negrisk = """
        SELECT 
            transaction_hash,
            event_name,
            topics,
            data,
            timestamp_ms,
            LOWER(contract_address) as contract
        FROM events_neg_risk_exchange
        WHERE timestamp_ms BETWEEN $1 AND $2
        ORDER BY timestamp_ms ASC, transaction_hash ASC
    """
    
    # Fetch ALL events from conditional tokens
    position_query = """
        SELECT 
            transaction_hash,
            event_name,
            topics,
            data,
            timestamp_ms
        FROM events_conditional_tokens
        WHERE timestamp_ms BETWEEN $1 AND $2
        ORDER BY timestamp_ms ASC, transaction_hash ASC
    """
    
    # Fetch all events
    ctf_rows = await conn.fetch(trade_query_ctf, min_ts_ms, max_ts_ms)
    logger.info(f"  Fetched {len(ctf_rows):,} events from events_ctf_exchange")
    
    negrisk_rows = await conn.fetch(trade_query_negrisk, min_ts_ms, max_ts_ms)
    logger.info(f"  Fetched {len(negrisk_rows):,} events from events_neg_risk_exchange")
    
    position_rows = await conn.fetch(position_query, min_ts_ms, max_ts_ms)
    logger.info(f"  Fetched {len(position_rows):,} events from events_conditional_tokens")
    
    # Filter trade events by name locally
    trades = []
    for row in ctf_rows + negrisk_rows:
        if row['event_name'] in ('OrderFilled', 'OrdersMatched'):
            trades.append(dict(row))
    
    # Filter position events by name locally
    position_events = []
    for row in position_rows:
        if row['event_name'] in ('PositionSplit', 'PositionsMerge'):
            position_events.append(dict(row))
    
    logger.info(f"  Filtered to: {len(trades):,} trade events, {len(position_events):,} position events")
    
    # Group by timestamp
    timestamp_map = defaultdict(lambda: {'trades': [], 'positions': []})
    
    for trade in trades:
        timestamp_map[trade['timestamp_ms']]['trades'].append(trade)
    
    for pos in position_events:
        timestamp_map[pos['timestamp_ms']]['positions'].append(pos)
    
    # Both lists are already sorted by (timestamp_ms, transaction_hash) from the query
    return dict(timestamp_map)

async def process_and_insert_settlements(conn, timestamp_map):
    """
    Process events per timestamp using dictionaries for efficient matching.
    
    Algorithm:
    1. For each timestamp, build dicts keyed by tx_hash:
       - orders_matched[exchange][tx_hash] = [events]
       - orders_filled[exchange][tx_hash] = [events]
       - position_split[tx_hash] = [events]
       - position_merge[tx_hash] = [events]
    2. Iterate through orders_matched, fetch matching orders_filled and position events
    3. Create settlements, remove matched entries from dicts
    4. Insert leftover position events to CFT_no_exchange
    5. Warn about unmatched orders_filled
    """
    insert_count = 0
    unmatched_count = 0
    
    for timestamp_ms in sorted(timestamp_map.keys()):
        trades = timestamp_map[timestamp_ms]['trades']
        position_events = timestamp_map[timestamp_ms]['positions']
        
        # Build dictionaries keyed by tx_hash
        orders_matched = defaultdict(list)  # tx_hash -> [events]
        orders_filled = defaultdict(list)
        position_split = defaultdict(list)
        position_merge = defaultdict(list)
        
        # Separate by exchange for orders_matched/filled
        orders_matched_by_exchange = {'negrisk': defaultdict(list), 'base': defaultdict(list)}
        orders_filled_by_exchange = {'negrisk': defaultdict(list), 'base': defaultdict(list)}
        
        for trade in trades:
            if trade['event_name'] == 'OrdersMatched':
                exchange = 'negrisk' if trade['contract'].lower() == NEGRISK_ADDR else 'base'
                orders_matched_by_exchange[exchange][trade['transaction_hash']].append(trade)
            elif trade['event_name'] == 'OrderFilled':
                exchange = 'negrisk' if trade['contract'].lower() == NEGRISK_ADDR else 'base'
                orders_filled_by_exchange[exchange][trade['transaction_hash']].append(trade)
        
        for pos in position_events:
            if pos['event_name'] == 'PositionSplit':
                position_split[pos['transaction_hash']].append(pos)
            elif pos['event_name'] == 'PositionsMerge':
                position_merge[pos['transaction_hash']].append(pos)
        
        # Process each exchange
        for exchange in ['negrisk', 'base']:
            om_dict = orders_matched_by_exchange[exchange]
            of_dict = orders_filled_by_exchange[exchange]
            
            for tx_hash, om_events in om_dict.items():
                # Get matching orders_filled and position events
                of_events = of_dict.pop(tx_hash, [])
                split_events = position_split.pop(tx_hash, [])
                merge_events = position_merge.pop(tx_hash, [])
                
                # Determine settlement type
                settlement_type = 'complementary'
                if split_events:
                    settlement_type = 'split'
                elif merge_events:
                    settlement_type = 'merge'
                
                # Collect all position events
                all_pos_events = split_events + merge_events
                
                # Create settlement record with only data and topics
                trade_jsonb = json.dumps({
                    'transaction_hash': tx_hash,
                    'orders_matched': [{'data': om['data'], 'topics': om['topics']} for om in om_events],
                    'orders_filled': [{'data': of['data'], 'topics': of['topics']} for of in of_events],
                    'position_events': [{'data': pe['data'], 'topics': pe['topics']} for pe in all_pos_events]
                })
                
                # Determine exchange
                exchange = 'negrisk' if om_events[0]['contract'].lower() == NEGRISK_ADDR else 'base'
                
                # Insert into settlements table
                await conn.execute(
                    """
                    INSERT INTO settlements (timestamp_ms, transaction_hash, trade, type, exchange)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    timestamp_ms, tx_hash, trade_jsonb, settlement_type, exchange
                )
                
                insert_count += 1
        
        # Insert leftover position events (those with no matching orders_matched)
        for tx_hash, events in position_split.items():
            for event in events:
                unmatched_jsonb = json.dumps(dict(event))
                await conn.execute(
                    """
                    INSERT INTO CFT_no_exchange (timestamp_ms, transaction_hash, event_name, event_data)
                    VALUES ($1, $2, $3, $4)
                    """,
                    timestamp_ms,
                    tx_hash,
                    'PositionSplit',
                    unmatched_jsonb
                )
                unmatched_count += 1
        
        for tx_hash, events in position_merge.items():
            for event in events:
                unmatched_jsonb = json.dumps(dict(event))
                await conn.execute(
                    """
                    INSERT INTO CFT_no_exchange (timestamp_ms, transaction_hash, event_name, event_data)
                    VALUES ($1, $2, $3, $4)
                    """,
                    timestamp_ms,
                    tx_hash,
                    'PositionsMerge',
                    unmatched_jsonb
                )
                unmatched_count += 1
        
        # Warn about unmatched orders_filled
        unmatched_filled = 0
        for exchange in ['negrisk', 'base']:
            unmatched_filled += sum(len(events) for events in orders_filled_by_exchange[exchange].values())
        
        if unmatched_filled > 0:
            logger.warning(f"    ⚠️ Found {unmatched_filled} unmatched OrdersFilled at timestamp {timestamp_ms}")
    
    return insert_count, unmatched_count

# ----------------------------------------------------------------
# MAIN LOOP
# ----------------------------------------------------------------

async def main():
    pool = await connect_db()
    
    try:
        async with pool.acquire() as conn:
            # Create tables
            await create_settlements_table(conn)
            await create_cft_no_exchange_table(conn)
            
            # Get time range
            logger.info("\n" + "=" * 80)
            logger.info("Finding time range across all tables...")
            logger.info("=" * 80)
            
            min_ts_ms, max_ts_ms = await get_min_max_timestamps(conn)
            
            if min_ts_ms is None or max_ts_ms is None:
                logger.error("No data found in tables")
                return
            
            # Check if we have already covered some range
            last_ts = await get_last_covered_timestamp(conn)
            if last_ts:
                logger.info(f"Last covered timestamp: {last_ts}")
                min_ts_ms = last_ts
            
            logger.info(f"Processing from {min_ts_ms} to {max_ts_ms}")
            logger.info(f"Range span: {(max_ts_ms - min_ts_ms) / (24 * 60 * 60 * 1000):.1f} days")
            
            # Process in time batches
            current_ts = min_ts_ms
            batch_num = 1
            total_inserted = 0
            total_unmatched = 0
            
            while current_ts < max_ts_ms:
                batch_end_ts = current_ts + BATCH_TIME_RANGE_MS
                
                # Cap at max timestamp
                batch_end_ts = min(batch_end_ts, max_ts_ms)
                
                logger.info(f"\n" + "=" * 80)
                logger.info(f"Batch {batch_num}: {current_ts} - {batch_end_ts}")
                logger.info("=" * 80)
                
                # Fetch events in this time range (returns dict of timestamp -> (trades, positions))
                timestamp_map = await fetch_events_in_time_range(conn, current_ts, batch_end_ts)
                
                # Process and insert
                inserted, unmatched = await process_and_insert_settlements(conn, timestamp_map)
                logger.info(f"  Inserted {inserted:,} settlement records, {unmatched:,} unmatched position events")
                
                total_inserted += inserted
                total_unmatched += unmatched
                
                # Move to next batch
                current_ts = batch_end_ts
                batch_num += 1
            
            logger.info(f"\n" + "=" * 80)
            logger.info(f"✅ COMPLETE: Inserted {total_inserted:,} settlement records, {total_unmatched:,} unmatched position events")
            logger.info("=" * 80)
    
    finally:
        await pool.close()

# ----------------------------------------------------------------
# ENTRY POINT
# ----------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(main())
