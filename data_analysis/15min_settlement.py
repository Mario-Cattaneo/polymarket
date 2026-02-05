#!/usr/bin/env python3
"""
Script to correlate Polymarket Last Trades with CTF Exchange Events.

1. Finds the Polymarket market for a specific UTC time (Bitcoin Up/Down 15m).
2. Fetches 'poly_last_trade_price' for that market's assets.
3. Determines the time window (Min/Max Server Time +/- Buffer).
4. Fetches 'OrdersMatched' and 'OrderFilled' from 'events_ctf_exchange' in that window.
"""

import os
import asyncio
import asyncpg
import json
import logging
from datetime import datetime, timedelta
import re

# --- CONFIGURATION ---
# The UTC time to target (Definable variable)
TARGET_UTC_TIME = "2026-01-08 00:30:00"

# Buffer time to add/subtract from the min/max trade times (in milliseconds)
BUFFER_TIME_MS = 2000  # e.g., 2 seconds

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
            password=DB_CONFIG["password"]
        )
        return pool
    except Exception as e:
        logger.error(f"Failed to connect to DB: {e}")
        return None

# --- HELPER FUNCTIONS ---

def utc_to_et(utc_dt):
    """Convert UTC datetime to ET (UTC-5)."""
    return utc_dt - timedelta(hours=5)

def format_et_time(et_dt):
    """Format ET datetime as HH:MMAM/PM."""
    hour = et_dt.hour
    minute = et_dt.minute
    ampm = "AM" if hour < 12 else "PM"
    if hour > 12:
        hour -= 12
    elif hour == 0:
        hour = 12
    return f"{hour}:{minute:02d}{ampm}"

def construct_polymarket_question_pattern(et_start_dt):
    """Construct regex pattern for Polymarket question matching this 15-min window."""
    month_name = et_start_dt.strftime("%B")
    day = et_start_dt.day
    start_time = format_et_time(et_start_dt)
    end_time = format_et_time(et_start_dt + timedelta(minutes=15))
    
    # Pattern: "Bitcoin Up or Down - January 6, 1:00PM-1:15PM ET"
    # Note: We use regex escaping for the times to handle special chars if any
    pattern = f"Bitcoin Up or Down\\s*-\\s*{month_name}\\s+{day},?\\s+{re.escape(start_time)}-{re.escape(end_time)}\\s+ET"
    return pattern

def extract_uint256_from_data(data_hex, slot):
    """Extract a uint256 value from the data field at a specific slot (from Script 1)."""
    try:
        if isinstance(data_hex, bytes):
            data_hex = data_hex.hex()
        else:
            data_hex = data_hex.lower().replace('0x', '')
        
        start = slot * 64
        end = start + 64
        hex_value = data_hex[start:end]
        if not hex_value: return 0
        return int(hex_value, 16)
    except Exception:
        return 0

# --- MAIN LOGIC ---

async def get_market_and_assets(pool, utc_time_str):
    """Finds the market ID and Asset IDs for the given UTC time."""
    try:
        utc_dt = datetime.strptime(utc_time_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        logger.error("Invalid UTC time format. Use YYYY-MM-DD HH:MM:SS")
        return None, None

    et_dt = utc_to_et(utc_dt)
    
    # Construct query parameters for the specific Bitcoin Up/Down market
    # Using the ILIKE pattern matching from your example
    search_pattern = f"%Bitcoin Up or Down%{et_dt.strftime('%B').lower()}%{et_dt.day}%{format_et_time(et_dt)}%{format_et_time(et_dt + timedelta(minutes=15))}%"
    
    logger.info(f"Searching for market matching: {search_pattern}")

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT market_id, message FROM poly_markets WHERE message->>'question' ILIKE $1 LIMIT 1",
            search_pattern
        )
        
        if not row:
            logger.warning(" No matching Polymarket found.")
            return None, None

        market_id = row['market_id']
        message = json.loads(row['message'])
        question = message.get('question', 'Unknown Question')
        
        # Extract Asset IDs
        clob_token_ids_raw = message.get('clobTokenIds', '[]')
        try:
            asset_ids = json.loads(clob_token_ids_raw)
        except (json.JSONDecodeError, TypeError):
            asset_ids = []

        logger.info(f"✅ Found Market: {market_id}")
        logger.info(f"   Question: {question}")
        logger.info(f"   Asset IDs: {asset_ids}")
        
        return market_id, asset_ids

async def get_last_trades(pool, asset_ids):
    """Fetches last trades for the assets, ordered by server_time."""
    if not asset_ids:
        return []

    query = """
        SELECT asset_id, 
               message->>'price' as price, 
               message->>'size' as size, 
               server_time_us, 
               found_time_us
        FROM poly_last_trade_price 
        WHERE asset_id = ANY($1) 
        ORDER BY server_time_us ASC
    """
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, asset_ids)
        return rows

async def get_ctf_events(pool, start_ms, end_ms):
    """Fetches OrdersMatched and OrderFilled events from both CTF and Neg Risk exchanges."""
    query = """
        SELECT transaction_hash, event_name, topics, data, timestamp_ms
        FROM events_ctf_exchange
        WHERE event_name IN ('OrdersMatched', 'OrderFilled')
          AND timestamp_ms BETWEEN $1 AND $2
        UNION ALL
        SELECT transaction_hash, event_name, topics, data, timestamp_ms
        FROM events_neg_risk_exchange
        WHERE event_name IN ('OrdersMatched', 'OrderFilled')
          AND timestamp_ms BETWEEN $1 AND $2
        ORDER BY timestamp_ms ASC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, start_ms, end_ms)
        return rows

def decode_event_data(data_hex: str) -> dict:
    """Decode hex data into uint256 values."""
    # Remove 0x prefix
    data_hex = data_hex[2:] if data_hex.startswith('0x') else data_hex
    
    # Pad to multiple of 64 (32 bytes = 256 bits)
    if len(data_hex) % 64 != 0:
        data_hex = data_hex.ljust((len(data_hex) // 64 + 1) * 64, '0')
    
    values = []
    for i in range(0, len(data_hex), 64):
        chunk = data_hex[i:i+64]
        value = int(chunk, 16)
        values.append(value)
    
    return values

def decode_order_filled(topics: list, data_hex: str) -> dict:
    """
    Decode OrderFilled event.
    Topics: [hash, maker, taker]
    Data: [makerAssetId, takerAssetId, making, taking, fee]
    """
    try:
        # Topics
        order_hash = topics[1] if len(topics) > 1 else None
        maker = "0x" + topics[2][-40:] if len(topics) > 2 else None  # Last 40 chars of address
        taker = "0x" + topics[3][-40:] if len(topics) > 3 else None
        
        # Data (non-indexed: makerAssetId, takerAssetId, making, taking, fee)
        data_values = decode_event_data(data_hex)
        
        return {
            "orderHash": order_hash,
            "maker": maker,
            "taker": taker,
            "makerAssetId": data_values[0] if len(data_values) > 0 else None,
            "takerAssetId": data_values[1] if len(data_values) > 1 else None,
            "making": data_values[2] if len(data_values) > 2 else None,
            "taking": data_values[3] if len(data_values) > 3 else None,
            "fee": data_values[4] if len(data_values) > 4 else None,
        }
    except Exception as e:
        logger.warning(f"Failed to decode OrderFilled: {e}")
        return {}

def decode_orders_matched(topics: list, data_hex: str) -> dict:
    """
    Decode OrdersMatched event.
    Topics: [hash, maker]
    Data: [makerAssetId, takerAssetId, making, taking]
    """
    try:
        # Topics
        order_hash = topics[1] if len(topics) > 1 else None
        maker = "0x" + topics[2][-40:] if len(topics) > 2 else None
        
        # Data (non-indexed: makerAssetId, takerAssetId, making, taking)
        data_values = decode_event_data(data_hex)
        
        return {
            "orderHash": order_hash,
            "maker": maker,
            "makerAssetId": data_values[0] if len(data_values) > 0 else None,
            "takerAssetId": data_values[1] if len(data_values) > 1 else None,
            "making": data_values[2] if len(data_values) > 2 else None,
            "taking": data_values[3] if len(data_values) > 3 else None,
        }
    except Exception as e:
        logger.warning(f"Failed to decode OrdersMatched: {e}")
        return {}

async def main():
    pool = await get_db_pool()
    if not pool:
        return

    try:
        # 1. Get Market and Assets
        market_id, asset_ids = await get_market_and_assets(pool, TARGET_UTC_TIME)
        if not asset_ids:
            return

        # 2. Get Last Trades
        logger.info("Fetching last trades...")
        trades = await get_last_trades(pool, asset_ids)
        
        if not trades:
            logger.warning("No trades found for these assets.")
            return

        logger.info(f"Found {len(trades)} trades.")

        # 3. Calculate Time Range
        # Note: poly_last_trade_price uses microseconds (us), events_ctf_exchange uses milliseconds (ms)
        min_server_us = min(t['server_time_us'] for t in trades)
        max_server_us = max(t['server_time_us'] for t in trades)
        
        # Convert to MS for the CTF table comparison
        min_server_ms = min_server_us // 1000
        max_server_ms = max_server_us // 1000

        # Apply Buffer
        search_start_ms = min_server_ms - BUFFER_TIME_MS
        search_end_ms = max_server_ms + BUFFER_TIME_MS

        logger.info("-" * 40)
        logger.info(f"Trade Time Range (US): {min_server_us} - {max_server_us}")
        logger.info(f"Trade Time Range (MS): {min_server_ms} - {max_server_ms}")
        logger.info(f"Search Window (+/- {BUFFER_TIME_MS}ms): {search_start_ms} - {search_end_ms}")
        logger.info("-" * 40)

        # 4. Find CTF Events in Range
        logger.info("Searching for CTF Exchange events in calculated window...")
        events = await get_ctf_events(pool, search_start_ms, search_end_ms)

        if not events:
            logger.warning("No OrdersMatched or OrderFilled events found in this window.")
        else:
            logger.info(f"Found {len(events)} events in the window.")
            
            # Decode and sample events
            logger.info("\n--- DECODING SAMPLE EVENTS ---")
            
            orders_matched_events = [e for e in events if e['event_name'] == 'OrdersMatched']
            order_filled_events = [e for e in events if e['event_name'] == 'OrderFilled']
            
            if orders_matched_events:
                logger.info(f"\nOrdersMatched Events: {len(orders_matched_events)} total")
                sample = orders_matched_events[0]
                topics = sample['topics'] if isinstance(sample['topics'], list) else json.loads(sample['topics'].strip('{}').replace("'", '"'))
                decoded = decode_orders_matched(topics, sample['data'])
                logger.info(f"   Sample decoded: {json.dumps(decoded, indent=2)}")
            
            if order_filled_events:
                logger.info(f"\n OrderFilled Events: {len(order_filled_events)} total")
                sample = order_filled_events[0]
                topics = sample['topics'] if isinstance(sample['topics'], list) else json.loads(sample['topics'].strip('{}').replace("'", '"'))
                decoded = decode_order_filled(topics, sample['data'])
                logger.info(f"   Sample decoded: {json.dumps(decoded, indent=2)}")
            
            # --- ANALYSIS BY ASSET ID ---
            logger.info("\n" + "="*80)
            logger.info("ANALYSIS BY ASSET ID")
            logger.info("="*80)
            
            # First, show what asset IDs are actually in the events
            logger.info(f"\nDEBUG: Unique asset IDs found in OrdersMatched events:")
            unique_matched_asset_ids = set()
            for event in orders_matched_events:
                topics = event['topics'] if isinstance(event['topics'], list) else json.loads(event['topics'].strip('{}').replace("'", '"'))
                decoded = decode_orders_matched(topics, event['data'])
                if decoded:
                    if decoded.get('makerAssetId'):
                        unique_matched_asset_ids.add(decoded.get('makerAssetId'))
                    if decoded.get('takerAssetId'):
                        unique_matched_asset_ids.add(decoded.get('takerAssetId'))
            
            logger.info(f"   Found {len(unique_matched_asset_ids)} unique asset IDs in OrdersMatched")
            for aid in sorted(list(unique_matched_asset_ids))[:10]:  # Show first 10
                logger.info(f"     - {aid}")
            
            logger.info(f"\n DEBUG: Unique asset IDs found in OrdersFilled events:")
            unique_filled_asset_ids = set()
            for event in order_filled_events:
                topics = event['topics'] if isinstance(event['topics'], list) else json.loads(event['topics'].strip('{}').replace("'", '"'))
                decoded = decode_order_filled(topics, event['data'])
                if decoded:
                    if decoded.get('makerAssetId'):
                        unique_filled_asset_ids.add(decoded.get('makerAssetId'))
                    if decoded.get('takerAssetId'):
                        unique_filled_asset_ids.add(decoded.get('takerAssetId'))
            
            logger.info(f"   Found {len(unique_filled_asset_ids)} unique asset IDs in OrdersFilled")
            for aid in sorted(list(unique_filled_asset_ids))[:10]:  # Show first 10
                logger.info(f"     - {aid}")
            
            logger.info(f"\n DEBUG: Polymarket asset IDs to match:")
            for aid in asset_ids:
                logger.info(f"   - {aid}")
            
            # Additional debug asset IDs to check
            debug_asset_ids = [
                "4031132846844196883695870747593975059183828438525834177960621871803786384542",
                "41404836665749362782624319574687607394745081822202452725583623357802975579587"
            ]
            logger.info(f"\n DEBUG: Additional asset IDs to check:")
            for aid in debug_asset_ids:
                logger.info(f"   - {aid}")
            
            # Check for matches
            market_asset_ids_set = set(int(aid) for aid in asset_ids)
            debug_asset_ids_set = set(int(aid) for aid in debug_asset_ids)
            all_check_ids = market_asset_ids_set | debug_asset_ids_set
            
            matched_in_events = all_check_ids & (unique_matched_asset_ids | unique_filled_asset_ids)
            logger.info(f"\n   Matches found: {matched_in_events if matched_in_events else 'NONE - ASSET IDS DO NOT MATCH!'}")
            
            # Analyze BOTH Polymarket asset IDs and matched debug asset IDs
            analysis_asset_ids = asset_ids + (debug_asset_ids if matched_in_events else [])
            
            for asset_id in analysis_asset_ids:
                asset_id_int = int(asset_id)
                logger.info(f"\n Asset ID: {asset_id}")
                
                # Count last trades for this asset
                asset_trades = [t for t in trades if int(t['asset_id']) == asset_id_int]
                num_trades = len(asset_trades)
                logger.info(f"   Last Trades: {num_trades}")
                
                # Decode all events and count matches
                decoded_matched = []
                decoded_filled = []
                
                for event in orders_matched_events:
                    topics = event['topics'] if isinstance(event['topics'], list) else json.loads(event['topics'].strip('{}').replace("'", '"'))
                    decoded = decode_orders_matched(topics, event['data'])
                    if decoded:
                        decoded_matched.append(decoded)
                
                for event in order_filled_events:
                    topics = event['topics'] if isinstance(event['topics'], list) else json.loads(event['topics'].strip('{}').replace("'", '"'))
                    decoded = decode_order_filled(topics, event['data'])
                    if decoded:
                        decoded_filled.append(decoded)
                
                # Count events where this asset is involved
                matched_with_asset = [e for e in decoded_matched if (e.get('makerAssetId') == asset_id_int or e.get('takerAssetId') == asset_id_int)]
                filled_with_asset = [e for e in decoded_filled if (e.get('makerAssetId') == asset_id_int or e.get('takerAssetId') == asset_id_int)]
                
                logger.info(f"   OrdersMatched with this asset: {len(matched_with_asset)}")
                logger.info(f"   OrdersFilled with this asset: {len(filled_with_asset)}")
                
                # Count trades that got matched (have a corresponding event)
                matched_trades = 0
                for trade in asset_trades:
                    trade_time_ms = trade['server_time_us'] // 1000
                    # Check if there's an event within a reasonable time window
                    nearby_events = [e for e in decoded_filled 
                                   if (e.get('makerAssetId') == asset_id_int or e.get('takerAssetId') == asset_id_int)
                                   and abs(trade_time_ms - (e.get('timestamp_ms', 0))) < 5000]  # 5 second window
                    if nearby_events:
                        matched_trades += 1
                
                match_pct = (matched_trades / num_trades * 100) if num_trades > 0 else 0
                logger.info(f"   Trades matched to events: {matched_trades}/{num_trades} ({match_pct:.1f}%)")
            
            print("\n" + "="*80)
            print(f"{'TIMESTAMP':<25} | {'EVENT TYPE':<15} | {'MAKING/ASSET':<15} | {'TX HASH'}")
            print("="*80)
            

    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())