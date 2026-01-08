#!/usr/bin/env python3
"""
Script to find TokenRegistered events matching specific asset IDs.
Searches both CTF and Neg Risk exchanges for token registration events.
"""

import os
import asyncio
import asyncpg
import json
import logging
from datetime import datetime

# --- CONFIGURATION ---
# December 28, 2025 as milliseconds timestamp
SEARCH_START_MS = int(datetime(2025, 12, 28, 0, 0, 0).timestamp() * 1000)

# Target asset IDs (the ones that matched in OrdersMatched/OrdersFilled events)
TARGET_ASSET_IDS = [
    "4031132846844196883695870747593975059183828438525834177960621871803786384542",
    "41404836665749362782624319574687607394745081822202452725583623357802975579587"
]

# Database Config
DB_CONFIG = {
    "host": os.getenv("PG_SOCKET"),
    "port": os.getenv("POLY_PG_PORT"),
    "database": os.getenv("POLY_DB"),
    "user": os.getenv("POLY_DB_CLI"),
    "password": os.getenv("POLY_DB_CLI_PASS")
}

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger()

async def get_db_pool():
    """Creates a connection pool."""
    try:
        pool = await asyncpg.create_pool(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            min_size=1,
            max_size=5
        )
        return pool
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None

def decode_token_registered(topics: list) -> dict:
    """
    Decode TokenRegistered event.
    Topics: [token0, token1, conditionId]
    All are indexed, stored as bytes32 in topics.
    """
    try:
        # Topics are hex strings, convert to int
        token0 = int(topics[1], 16) if len(topics) > 1 else None
        token1 = int(topics[2], 16) if len(topics) > 2 else None
        condition_id = topics[3] if len(topics) > 3 else None
        
        return {
            "token0": token0,
            "token1": token1,
            "conditionId": condition_id,
        }
    except Exception as e:
        logger.warning(f"Failed to decode TokenRegistered: {e}")
        return {}

async def get_token_registered_events(pool, start_ms):
    """Fetches TokenRegistered events from both exchanges after given timestamp."""
    query = """
        SELECT transaction_hash, event_name, topics, data, timestamp_ms, 'ctf' as source
        FROM events_ctf_exchange
        WHERE event_name = 'TokenRegistered'
          AND timestamp_ms >= $1
        UNION ALL
        SELECT transaction_hash, event_name, topics, data, timestamp_ms, 'neg_risk' as source
        FROM events_neg_risk_exchange
        WHERE event_name = 'TokenRegistered'
          AND timestamp_ms >= $1
        ORDER BY timestamp_ms ASC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, start_ms)
        return rows

async def main():
    pool = await get_db_pool()
    if not pool:
        return

    try:
        logger.info("="*80)
        logger.info("TOKEN REGISTRATION SEARCH")
        logger.info("="*80)
        
        logger.info(f"\n🔍 Search Parameters:")
        logger.info(f"   Start timestamp (MS): {SEARCH_START_MS} ({datetime.fromtimestamp(SEARCH_START_MS/1000)})")
        logger.info(f"   Target Asset IDs:")
        for aid in TARGET_ASSET_IDS:
            logger.info(f"     - {aid}")
        
        # Fetch events
        logger.info(f"\n📊 Fetching TokenRegistered events...")
        events = await get_token_registered_events(pool, SEARCH_START_MS)
        
        if not events:
            logger.warning("No TokenRegistered events found after this timestamp.")
            return
        
        logger.info(f"✅ Found {len(events)} TokenRegistered events total")
        
        # Show sample decoded event
        logger.info(f"\n--- SAMPLE DECODED EVENT ---")
        if events:
            sample = events[0]
            topics = sample['topics'] if isinstance(sample['topics'], list) else json.loads(sample['topics'].strip('{}').replace("'", '"'))
            decoded = decode_token_registered(topics)
            logger.info(f"Event from {sample['source']} exchange:")
            logger.info(f"  Transaction: {sample['transaction_hash']}")
            logger.info(f"  Timestamp (MS): {sample['timestamp_ms']}")
            logger.info(f"  Decoded: {json.dumps(decoded, indent=2)}")
        
        # Convert target asset IDs to integers for comparison
        target_asset_ids_set = set(int(aid) for aid in TARGET_ASSET_IDS)
        
        # Decode all events and find matches
        logger.info(f"\n--- MATCHING EVENTS ---")
        matched_events = []
        
        for event in events:
            topics = event['topics'] if isinstance(event['topics'], list) else json.loads(event['topics'].strip('{}').replace("'", '"'))
            decoded = decode_token_registered(topics)
            
            if decoded:
                token0 = decoded.get('token0')
                token1 = decoded.get('token1')
                
                # Check if either token matches our target asset IDs
                if token0 in target_asset_ids_set or token1 in target_asset_ids_set:
                    matched_events.append({
                        'event': event,
                        'decoded': decoded,
                        'source': event['source']
                    })
        
        logger.info(f"\n✅ Found {len(matched_events)} matching TokenRegistered events!")
        
        if matched_events:
            # Group by source
            by_source = {}
            for match in matched_events:
                source = match['source']
                if source not in by_source:
                    by_source[source] = []
                by_source[source].append(match)
            
            for source in ['ctf', 'neg_risk']:
                if source in by_source:
                    logger.info(f"\n📋 {source.upper()} Exchange: {len(by_source[source])} matches")
                    for i, match in enumerate(by_source[source][:5], 1):  # Show first 5
                        decoded = match['decoded']
                        event = match['event']
                        match_type = []
                        if int(decoded.get('token0', 0)) in target_asset_ids_set:
                            match_type.append("token0")
                        if int(decoded.get('token1', 0)) in target_asset_ids_set:
                            match_type.append("token1")
                        
                        logger.info(f"  [{i}] Token0: {decoded.get('token0')}")
                        logger.info(f"      Token1: {decoded.get('token1')}")
                        logger.info(f"      ConditionId: {decoded.get('conditionId')}")
                        logger.info(f"      Matched fields: {', '.join(match_type)}")
                        logger.info(f"      TX: {event['transaction_hash']}")
                        logger.info(f"      Timestamp (MS): {event['timestamp_ms']}")
                        logger.info("")
        else:
            logger.warning("⚠️  No TokenRegistered events found matching the target asset IDs")
        
        logger.info("="*80)
        
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
