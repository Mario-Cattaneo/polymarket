#!/usr/bin/env python3
"""
Build house_settlements table by filtering settlements and CFT_no_exchange events.
Matches settlements with specific asset IDs and condition IDs.
"""

import os
import asyncio
import asyncpg
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Set, Any
from eth_abi import decode

# ================================================================
# CONFIGURATION
# ================================================================

# Asset IDs to filter for (Exchange/Settlements)
ASSET_IDS = [
    "65139230827417363158752884968303867495725894165574887635816574090175320800482", 
    "17371217118862125782438074585166210555214661810823929795910191856905580975576",
    "83247781037352156539108067944461291821683755894607244160607042790356561625563", 
    "33156410999665902694791064431724433042010245771106314074312009703157423879038"
]

# Condition IDs to filter for (CFT Events)
CONDITION_IDS = [
    "0xd5d9fc47718bd553592d126b1fa5e87183d27f3936975b0c04cc0f2dec1f1bb4", 
    "0x4e4f77e7dbf4cab666e9a1943674d7ae66348e862df03ea6f44b11eb95731928"
]

# Default Start Date: January 8th, 2026 00:00 UTC
DEFAULT_START_DT = datetime(2026, 1, 8, 0, 0, 0, tzinfo=timezone.utc)
DEFAULT_START_MS = int(DEFAULT_START_DT.timestamp() * 1000)

T_BATCH = 6 * 60 * 60 * 1000  # 6 hours in milliseconds

# Database Credentials
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ================================================================
# DECODING LOGIC
# ================================================================

EVENT_SIGS = {
    'OrderFilled': '0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6',
    'OrdersMatched': '0x63bf4d16b7fa898ef4c4b2b6d90fd201e9c56313b65638af6088d149d2ce956c',
    'PositionSplit': '0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298',
    'PositionsMerge': '0x6f13ca62553fcc2bcd2372180a43949c1e4cebba603901ede2f4e14f36b282ca',
}

def get_asset_ids_from_data(event_name: str, data_hex: str) -> Set[str]:
    """Decodes binary data to extract asset IDs."""
    found_assets = set()
    try:
        data_bytes = bytes.fromhex(data_hex[2:] if data_hex.startswith('0x') else data_hex)
        
        if event_name == 'OrderFilled':
            # [makerAssetId, takerAssetId, making, taking, fee]
            decoded = decode(['uint256', 'uint256', 'uint256', 'uint256', 'uint256'], data_bytes)
            found_assets.add(str(decoded[0])) # makerAssetId
            found_assets.add(str(decoded[1])) # takerAssetId
            
        elif event_name == 'OrdersMatched':
            # [makerAssetId, takerAssetId, making, taking]
            decoded = decode(['uint256', 'uint256', 'uint256', 'uint256'], data_bytes)
            found_assets.add(str(decoded[0])) # makerAssetId
            found_assets.add(str(decoded[1])) # takerAssetId
            
    except Exception as e:
        # logger.debug(f"Failed to decode {event_name}: {e}")
        pass
        
    return found_assets

def extract_assets_from_settlement(trade_data: Dict) -> Set[str]:
    """Parses a settlement trade object to find all involved Asset IDs."""
    assets = set()
    
    # Helper to process list of events
    def process_events(event_list, event_type_override=None):
        if not event_list: return
        for item in event_list:
            if isinstance(item, str):
                try:
                    item = json.loads(item)
                except: continue
            
            topics = item.get('topics', [])
            if isinstance(topics, str): topics = json.loads(topics)
            
            data = item.get('data', '0x')
            
            # Identify event
            sig = topics[0] if topics else None
            event_name = event_type_override
            
            if not event_name and sig:
                for name, known_sig in EVENT_SIGS.items():
                    if sig.lower() == known_sig.lower():
                        event_name = name
                        break
            
            if event_name in ['OrderFilled', 'OrdersMatched']:
                assets.update(get_asset_ids_from_data(event_name, data))

    # Check standard fields in the trade JSON
    process_events(trade_data.get('orders_matched', []), 'OrdersMatched')
    process_events(trade_data.get('orders_filled', []), 'OrderFilled')
    
    return assets

def extract_condition_from_cft(event_data: Dict) -> Set[str]:
    """Extracts condition IDs from CFT event topics."""
    conditions = set()
    
    topics = event_data.get('topics', [])
    if isinstance(topics, str):
        try:
            topics = json.loads(topics)
        except: return conditions

    # PositionSplit and PositionsMerge: Condition ID is usually indexed at topic 2
    # Event Sig: topic[0], Stakeholder: topic[1], ConditionId: topic[2]
    if len(topics) > 2:
        conditions.add(topics[2].lower())
        
    return conditions

# ================================================================
# DATABASE LOGIC
# ================================================================

async def create_tables(conn):
    """Create the house_settlements table."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS house_settlements (
            id BIGSERIAL PRIMARY KEY,
            source_table VARCHAR(50) NOT NULL, -- 'settlements' or 'CFT_no_exchange'
            source_id BIGINT NOT NULL,         -- ID from the source table
            asset_id TEXT NOT NULL,            -- The ID that triggered the match (Asset ID or Condition ID)
            timestamp_ms BIGINT NOT NULL,
            transaction_hash TEXT NOT NULL,
            payload JSONB NOT NULL,            -- The full data
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    
    # Index for fast resuming and querying
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_house_settlements_source_ts 
        ON house_settlements (source_table, timestamp_ms);
    """)
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_house_settlements_asset 
        ON house_settlements (asset_id);
    """)
    logger.info("Table `house_settlements` verified.")

async def get_resume_timestamp(conn, source_table: str) -> int:
    """Get the last timestamp processed for a specific source table."""
    val = await conn.fetchval("""
        SELECT MAX(timestamp_ms) 
        FROM house_settlements 
        WHERE source_table = $1
    """, source_table)
    
    if val:
        return val + 1 # Start from next ms
    return DEFAULT_START_MS

async def process_settlements(conn, start_ts: int, end_ts: int):
    """Fetch, filter, and insert settlements."""
    rows = await conn.fetch("""
        SELECT id, timestamp_ms, transaction_hash, type, exchange, trade
        FROM settlements
        WHERE timestamp_ms >= $1 AND timestamp_ms < $2
        ORDER BY timestamp_ms ASC
    """, start_ts, end_ts)
    
    count = 0
    for row in rows:
        try:
            trade_json = row['trade']
            if isinstance(trade_json, str):
                trade_json = json.loads(trade_json)
            
            # Extract assets involved in this settlement
            involved_assets = extract_assets_from_settlement(trade_json)
            
            # Check intersection with our target list
            matches = [aid for aid in involved_assets if aid in ASSET_IDS]
            
            for match_id in matches:
                await conn.execute("""
                    INSERT INTO house_settlements 
                    (source_table, source_id, asset_id, timestamp_ms, transaction_hash, payload)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """, 'settlements', row['id'], match_id, row['timestamp_ms'], row['transaction_hash'], json.dumps(dict(row)))
                count += 1
                
        except Exception as e:
            logger.error(f"Error processing settlement {row['id']}: {e}")
            continue
            
    return count

async def process_cft(conn, start_ts: int, end_ts: int):
    """Fetch, filter, and insert CFT events."""
    rows = await conn.fetch("""
        SELECT id, timestamp_ms, transaction_hash, event_name, event_data
        FROM CFT_no_exchange
        WHERE timestamp_ms >= $1 AND timestamp_ms < $2
        ORDER BY timestamp_ms ASC
    """, start_ts, end_ts)
    
    count = 0
    for row in rows:
        try:
            event_data = row['event_data']
            if isinstance(event_data, str):
                event_data = json.loads(event_data)
            
            # Extract conditions involved
            involved_conditions = extract_condition_from_cft(event_data)
            
            # Check intersection (normalize to lower case for comparison)
            matches = [cid for cid in involved_conditions if cid.lower() in [x.lower() for x in CONDITION_IDS]]
            
            for match_id in matches:
                await conn.execute("""
                    INSERT INTO house_settlements 
                    (source_table, source_id, asset_id, timestamp_ms, transaction_hash, payload)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """, 'CFT_no_exchange', row['id'], match_id, row['timestamp_ms'], row['transaction_hash'], json.dumps(dict(row)))
                count += 1
                
        except Exception as e:
            logger.error(f"Error processing CFT event {row['id']}: {e}")
            continue
            
    return count

async def main():
    if not PG_HOST:
        logger.error("Database credentials not set in environment variables.")
        return

    pool = await asyncpg.create_pool(
        host=PG_HOST,
        port=PG_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    try:
        async with pool.acquire() as conn:
            await create_tables(conn)
            
            # Get resume points
            next_settlement_ts = await get_resume_timestamp(conn, 'settlements')
            next_cft_ts = await get_resume_timestamp(conn, 'CFT_no_exchange')
            
            logger.info(f"Resuming Settlements from: {datetime.fromtimestamp(next_settlement_ts/1000, tz=timezone.utc)}")
            logger.info(f"Resuming CFT Events from:  {datetime.fromtimestamp(next_cft_ts/1000, tz=timezone.utc)}")
            
            # Get global max to know when to stop (optional, or just run until now)
            max_ts_row = await conn.fetchrow("""
                SELECT GREATEST(
                    (SELECT MAX(timestamp_ms) FROM settlements),
                    (SELECT MAX(timestamp_ms) FROM CFT_no_exchange)
                ) as max_ts
            """)
            global_max = max_ts_row['max_ts'] or DEFAULT_START_MS
            
            # Loop until caught up
            while next_settlement_ts < global_max or next_cft_ts < global_max:
                
                # 1. Process Settlements Batch
                if next_settlement_ts < global_max:
                    end_batch = next_settlement_ts + T_BATCH
                    logger.info(f"Processing Settlements: {datetime.fromtimestamp(next_settlement_ts/1000, tz=timezone.utc)} -> {datetime.fromtimestamp(end_batch/1000, tz=timezone.utc)}")
                    
                    inserted = await process_settlements(conn, next_settlement_ts, end_batch)
                    if inserted > 0:
                        logger.info(f"  -> Found {inserted} matching settlements")
                    
                    next_settlement_ts = end_batch

                # 2. Process CFT Batch
                if next_cft_ts < global_max:
                    end_batch = next_cft_ts + T_BATCH
                    logger.info(f"Processing CFT Events:  {datetime.fromtimestamp(next_cft_ts/1000, tz=timezone.utc)} -> {datetime.fromtimestamp(end_batch/1000, tz=timezone.utc)}")
                    
                    inserted = await process_cft(conn, next_cft_ts, end_batch)
                    if inserted > 0:
                        logger.info(f"  -> Found {inserted} matching CFT events")
                    
                    next_cft_ts = end_batch

            logger.info("Sync complete.")

    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())