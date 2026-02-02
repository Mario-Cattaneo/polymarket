"""
Diagnostic script to find and validate market ticker and asset IDs for 15-minute intervals.
Tests multiple random 15-min intervals to verify market discovery works.
"""

import os
import asyncio
import asyncpg
import json
import logging
import random
from datetime import datetime, timedelta, timezone
import re

# --- Logging Setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Configuration ---
PG_HOST = os.getenv("PG_SOCKET", "localhost")
PG_PORT = os.getenv("POLY_PG_PORT", 5432)
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- Helper Functions ---

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
    
    pattern = f"Bitcoin Up or Down\\s*-\\s*{month_name}\\s+{day},?\\s+{re.escape(start_time)}-{re.escape(end_time)}\\s+ET"
    return pattern

def construct_kalshi_ticker_pattern(et_start_dt):
    """Construct Kalshi ticker for this 15-min window (uses end time)."""
    et_end_dt = et_start_dt + timedelta(minutes=15)
    
    year_short = et_end_dt.strftime("%y")
    month_abbr = et_end_dt.strftime("%b").upper()
    day = et_end_dt.day
    hour = et_end_dt.hour
    minute = et_end_dt.minute
    
    ticker_start = f"KXBTC15M-{year_short}{month_abbr}{day:02d}{hour:02d}{minute:02d}"
    return ticker_start

def round_to_15min(dt):
    """Round datetime to nearest 15-minute boundary."""
    minute = dt.minute
    if minute < 15:
        new_minute = 0
    elif minute < 30:
        new_minute = 15
    elif minute < 45:
        new_minute = 30
    else:
        new_minute = 45
    return dt.replace(minute=new_minute, second=0, microsecond=0)

# --- Database Connection ---

async def connect_db():
    """Creates a connection pool to the database."""
    return await asyncpg.create_pool(
        host=PG_HOST,
        port=int(PG_PORT),
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        min_size=1,
        max_size=5
    )

# --- Market Discovery Functions ---

async def find_polymarket_asset_id(pool, utc_dt):
    """
    Find Polymarket asset_id for 15-min cycle starting at utc_dt.
    Returns (asset_id, market_id, question) or (None, None, None) if not found.
    """
    et_dt = utc_to_et(utc_dt)
    start_time_str = format_et_time(et_dt)
    end_time_str = format_et_time(et_dt + timedelta(minutes=15))
    month_name = et_dt.strftime("%B")
    day = et_dt.day
    
    search_pattern = f"%Bitcoin Up or Down%{month_name}%{day}%{start_time_str}%{end_time_str}%ET"
    
    logger.debug(f"[POLY] Searching for pattern: {search_pattern}")
    logger.debug(f"[POLY] UTC interval: {utc_dt} - {utc_dt + timedelta(minutes=15)}")
    logger.debug(f"[POLY] ET interval: {et_dt} - {et_dt + timedelta(minutes=15)}")
    logger.debug(f"[POLY] Start time: {start_time_str}, End time: {end_time_str}")
    
    async with pool.acquire() as conn:
        # Search BOTH tables
        markets = await conn.fetch("""
            SELECT market_id, message FROM poly_markets WHERE message->>'question' ILIKE $1
            UNION
            SELECT market_id, message FROM poly_new_market WHERE message->>'question' ILIKE $1
            LIMIT 5
        """, search_pattern)
        
        logger.debug(f"[POLY] Found {len(markets)} markets")
        
        if markets:
            for i, market in enumerate(markets):
                logger.debug(f"[POLY]   Market {i}: {market['market_id']}")
                try:
                    msg = json.loads(market['message'])
                    question = msg.get('question', '')
                    logger.debug(f"[POLY]     Question: {question}")
                except:
                    pass
        
        if not markets:
            logger.warning(f"[POLY] No market found for {utc_dt}")
            return None, None, None
        
        # Use first market
        market_msg = json.loads(markets[0]['message'])
        question = market_msg.get('question', '')
        market_id = markets[0]['market_id']
        
        clob_token_ids_raw = market_msg.get('clobTokenIds', '[]')
        
        try:
            asset_ids = json.loads(clob_token_ids_raw)
        except (json.JSONDecodeError, TypeError):
            asset_ids = []
        
        if not asset_ids:
            logger.warning(f"[POLY] No asset IDs found in market {market_id}")
            return None, market_id, question
        
        asset_id = asset_ids[0]
        logger.info(f"[POLY] Found asset_id: {asset_id} for market {market_id}")
        return asset_id, market_id, question

async def find_kalshi_ticker(pool, utc_dt):
    """
    Find Kalshi market ticker for 15-min cycle starting at utc_dt.
    Returns (ticker, records_count) or (None, 0) if not found.
    """
    et_dt = utc_to_et(utc_dt)
    ticker_pattern = construct_kalshi_ticker_pattern(et_dt)
    
    logger.debug(f"[KALSHI] Searching for ticker pattern: {ticker_pattern}%")
    logger.debug(f"[KALSHI] UTC interval: {utc_dt} - {utc_dt + timedelta(minutes=15)}")
    logger.debug(f"[KALSHI] ET interval: {et_dt} - {et_dt + timedelta(minutes=15)}")
    
    async with pool.acquire() as conn:
        # Find markets matching pattern
        markets = await conn.fetch(
            "SELECT market_ticker FROM kalshi_markets_3 WHERE market_ticker LIKE $1 LIMIT 5",
            ticker_pattern + '%'
        )
        
        logger.debug(f"[KALSHI] Found {len(markets)} markets matching pattern")
        
        if markets:
            for i, market in enumerate(markets):
                logger.debug(f"[KALSHI]   Market {i}: {market['market_ticker']}")
        
        if not markets:
            logger.warning(f"[KALSHI] No market found for pattern {ticker_pattern}%")
            return None, 0
        
        ticker = markets[0]['market_ticker']
        logger.info(f"[KALSHI] Found ticker: {ticker}")
        
        # Count records for this ticker
        cycle_start_us = int(utc_dt.timestamp() * 1_000_000)
        cycle_end_us = int((utc_dt + timedelta(minutes=15)).timestamp() * 1_000_000)
        
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM kalshi_orderbook_updates_3 WHERE market_ticker = $1 AND server_time_us >= $2 AND server_time_us < $3",
            ticker, cycle_start_us, cycle_end_us
        )
        
        logger.info(f"[KALSHI] Records in time window [{cycle_start_us}, {cycle_end_us}): {count}")
        
        if count > 0:
            # Get min/max timestamps
            result = await conn.fetchrow(
                "SELECT MIN(server_time_us) as min_ts, MAX(server_time_us) as max_ts FROM kalshi_orderbook_updates_3 WHERE market_ticker = $1 AND server_time_us >= $2 AND server_time_us < $3",
                ticker, cycle_start_us, cycle_end_us
            )
            if result and result['min_ts']:
                min_dt = datetime.fromtimestamp(result['min_ts'] / 1_000_000, tz=timezone.utc)
                max_dt = datetime.fromtimestamp(result['max_ts'] / 1_000_000, tz=timezone.utc)
                logger.info(f"[KALSHI] Time range of records: {min_dt} - {max_dt}")
        
        return ticker, count

async def find_polymarket_data_count(pool, asset_id, utc_dt):
    """Check if Polymarket has data in this time window."""
    cycle_start_us = int(utc_dt.timestamp() * 1_000_000)
    cycle_end_us = int((utc_dt + timedelta(minutes=15)).timestamp() * 1_000_000)
    
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM poly_book_state WHERE asset_id = $1 AND server_time_us >= $2 AND server_time_us < $3",
            asset_id, cycle_start_us, cycle_end_us
        )
    
    logger.info(f"[POLY] Records in time window [{cycle_start_us}, {cycle_end_us}): {count}")
    
    if count > 0 and asset_id:
        async with pool.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT MIN(server_time_us) as min_ts, MAX(server_time_us) as max_ts FROM poly_book_state WHERE asset_id = $1 AND server_time_us >= $2 AND server_time_us < $3",
                asset_id, cycle_start_us, cycle_end_us
            )
            if result and result['min_ts']:
                min_dt = datetime.fromtimestamp(result['min_ts'] / 1_000_000, tz=timezone.utc)
                max_dt = datetime.fromtimestamp(result['max_ts'] / 1_000_000, tz=timezone.utc)
                logger.info(f"[POLY] Time range of records: {min_dt} - {max_dt}")
    
    return count

async def test_cycle(pool, utc_dt, cycle_num):
    """Test market discovery for a single 15-minute cycle."""
    print(f"\n{'='*80}")
    print(f"TEST {cycle_num}: UTC {utc_dt}")
    print(f"{'='*80}")
    
    # Find Polymarket
    logger.info("--- Finding Polymarket ---")
    poly_asset_id, poly_market_id, poly_question = await find_polymarket_asset_id(pool, utc_dt)
    
    if poly_asset_id:
        poly_data_count = await find_polymarket_data_count(pool, poly_asset_id, utc_dt)
        poly_status = "✓ HAS DATA" if poly_data_count > 0 else "✗ NO DATA"
    else:
        poly_data_count = 0
        poly_status = "✗ NOT FOUND"
    
    # Find Kalshi
    logger.info("\n--- Finding Kalshi ---")
    kalshi_ticker, kalshi_data_count = await find_kalshi_ticker(pool, utc_dt)
    kalshi_status = "✓ HAS DATA" if kalshi_data_count > 0 else ("✗ NOT FOUND" if not kalshi_ticker else "✗ NO DATA")
    
    # Summary
    print(f"\n--- RESULTS ---")
    print(f"Polymarket: {poly_status}")
    if poly_asset_id:
        print(f"  Asset ID: {poly_asset_id}")
        print(f"  Market ID: {poly_market_id}")
        print(f"  Question: {poly_question}")
        print(f"  Records: {poly_data_count}")
    
    print(f"Kalshi: {kalshi_status}")
    if kalshi_ticker:
        print(f"  Ticker: {kalshi_ticker}")
        print(f"  Records: {kalshi_data_count}")
    
    both_ready = poly_data_count > 0 and kalshi_data_count > 0
    print(f"\nBoth have data: {'✓ YES' if both_ready else '✗ NO'}")
    
    return both_ready, (poly_asset_id, poly_market_id, poly_question, poly_data_count), (kalshi_ticker, kalshi_data_count)

async def find_time_range_bounds(pool):
    """Find earliest and latest timestamps in both tables."""
    logger.info("\n=== Finding Data Time Range Bounds ===")
    
    async with pool.acquire() as conn:
        # Polymarket bounds
        poly_result = await conn.fetchrow("""
            SELECT MIN(server_time_us) as earliest, MAX(server_time_us) as latest 
            FROM poly_new_market
        """)
        
        # Kalshi bounds
        kalshi_result = await conn.fetchrow("""
            SELECT MIN(server_time_us) as earliest, MAX(server_time_us) as latest 
            FROM kalshi_orderbook_updates_3
        """)
    
    poly_earliest = datetime.fromtimestamp(poly_result['earliest'] / 1_000_000, tz=timezone.utc) if poly_result['earliest'] else None
    poly_latest = datetime.fromtimestamp(poly_result['latest'] / 1_000_000, tz=timezone.utc) if poly_result['latest'] else None
    
    kalshi_earliest = datetime.fromtimestamp(kalshi_result['earliest'] / 1_000_000, tz=timezone.utc) if kalshi_result['earliest'] else None
    kalshi_latest = datetime.fromtimestamp(kalshi_result['latest'] / 1_000_000, tz=timezone.utc) if kalshi_result['latest'] else None
    
    logger.info(f"Polymarket (poly_new_market): {poly_earliest} - {poly_latest}")
    logger.info(f"Kalshi (orderbook_updates): {kalshi_earliest} - {kalshi_latest}")
    
    # Find overlap
    if poly_earliest and kalshi_earliest:
        overlap_start = max(poly_earliest, kalshi_earliest)
        overlap_end = min(poly_latest, kalshi_latest) if poly_latest and kalshi_latest else None
        
        if overlap_end and overlap_start < overlap_end:
            logger.info(f"Overlap period: {overlap_start} - {overlap_end}")
            return overlap_start, overlap_end
        else:
            logger.warning("No overlap between Polymarket and Kalshi time ranges!")
            return None, None
    
    return None, None

# --- Main ---

async def main():
    """Run diagnostic tests on random 15-minute cycles."""
    logger.info("Starting market discovery diagnostic...")
    
    pool = await connect_db()
    
    try:
        # Find time range
        overlap_start, overlap_end = await find_time_range_bounds(pool)
        
        if not overlap_start or not overlap_end:
            logger.error("Cannot find overlapping time range!")
            return
        
        # Test at several points in the overlap range
        num_tests = 5
        results = []
        
        # Generate random times in overlap range
        time_span = (overlap_end - overlap_start).total_seconds()
        test_times = []
        
        for i in range(num_tests):
            # Random timestamp in overlap
            random_seconds = random.uniform(0, time_span)
            random_dt = overlap_start + timedelta(seconds=random_seconds)
            # Round to 15-min boundary
            rounded_dt = round_to_15min(random_dt)
            test_times.append(rounded_dt)
        
        # Sort them
        test_times.sort()
        
        logger.info(f"\n\nTesting {num_tests} random 15-minute intervals in overlap period:")
        
        for i, test_dt in enumerate(test_times, 1):
            both_ready, poly_info, kalshi_info = await test_cycle(pool, test_dt, i)
            results.append((test_dt, both_ready, poly_info, kalshi_info))
        
        # Summary
        print(f"\n\n{'='*80}")
        print("FINAL SUMMARY")
        print(f"{'='*80}")
        
        ready_count = sum(1 for _, ready, _, _ in results if ready)
        print(f"Cycles with both markets and data: {ready_count}/{num_tests}")
        print()
        
        for test_dt, both_ready, poly_info, kalshi_info in results:
            status = "✓" if both_ready else "✗"
            poly_asset_id, _, _, poly_count = poly_info
            kalshi_ticker, kalshi_count = kalshi_info
            
            print(f"{status} {test_dt}: Poly({poly_count} records) | Kalshi({kalshi_count} records)")
            if poly_asset_id:
                print(f"    Asset: {poly_asset_id} | Ticker: {kalshi_ticker}")
    
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
