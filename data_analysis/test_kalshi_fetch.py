#!/usr/bin/env python3
"""
Test script for Kalshi orderbook data fetching and filtering.

Tests the new fetch_kalshi_orderbook_updates function with random 15-minute intervals
to verify that:
1. Ticker is correctly constructed from UTC timestamp
2. Orderbook updates are fetched for the correct time window
3. Filters correctly to matching tickers only
"""

import os
import asyncio
import asyncpg
import json
import logging
from datetime import datetime, timedelta, timezone
import random

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
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

# --- Database Connection ---

async def connect_db():
    """Creates a connection pool to the database."""
    return await asyncpg.create_pool(
        host=PG_HOST,
        port=PG_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        min_size=1,
        max_size=5,
        command_timeout=60
    )

# --- Import from heatmap script ---
import sys
sys.path.insert(0, '/home/mario_cattaneo/polymarket/data_analysis')
from heatmap_15min_cycles import (
    fetch_kalshi_orderbook_updates,
    get_kalshi_ticker,
    construct_kalshi_ticker_pattern,
    utc_to_et,
    round_to_15min,
)

# --- Test Functions ---

async def test_kalshi_fetch(pool, utc_timestamp, test_num):
    """Test fetching Kalshi orderbook updates for a given UTC timestamp."""
    logger.info(f"\n{'='*80}")
    logger.info(f"TEST {test_num}: UTC {utc_timestamp.isoformat()}")
    logger.info(f"{'='*80}")
    
    # Convert to ET and show the interval
    et_dt = utc_to_et(utc_timestamp)
    et_end = et_dt + timedelta(minutes=15)
    logger.info(f"ET interval: {et_dt.strftime('%B %d, %I:%M%p')} - {et_end.strftime('%I:%M%p')} ET")
    
    # Get ticker
    ticker = await get_kalshi_ticker(pool, utc_timestamp)
    if ticker is None:
        logger.warning(f"No Kalshi market found for this interval")
        return
    
    logger.info(f"Market ticker: {ticker}")
    
    # Fetch orderbook updates
    records = await fetch_kalshi_orderbook_updates(pool, utc_timestamp, ticker)
    
    if not records:
        logger.warning(f"No orderbook updates found for this interval")
        return
    
    # Analyze the records
    first_time_us = records[0]['server_time_us']
    last_time_us = records[-1]['server_time_us']
    
    first_time = datetime.fromtimestamp(first_time_us / 1_000_000, tz=timezone.utc)
    last_time = datetime.fromtimestamp(last_time_us / 1_000_000, tz=timezone.utc)
    
    logger.info(f"Total updates: {len(records)}")
    logger.info(f"Time range: {first_time.isoformat()} to {last_time.isoformat()}")
    
    # Show sample message types
    message_types = {}
    for record in records[:10]:  # Analyze first 10
        try:
            wrapper = json.loads(record['message'])
            msg_type = wrapper.get('type', 'unknown')
            message_types[msg_type] = message_types.get(msg_type, 0) + 1
        except:
            pass
    
    logger.info(f"Sample message types: {message_types}")
    
    # Show first and last update details
    try:
        first_msg = json.loads(records[0]['message'])
        logger.info(f"First update type: {first_msg.get('type', 'unknown')}")
        
        last_msg = json.loads(records[-1]['message'])
        logger.info(f"Last update type: {last_msg.get('type', 'unknown')}")
    except Exception as e:
        logger.warning(f"Error parsing messages: {e}")

async def main():
    """Run tests with random 15-minute intervals."""
    logger.info("="*80)
    logger.info("KALSHI ORDERBOOK FETCH TEST")
    logger.info("="*80)
    
    pool = await connect_db()
    
    try:
        # Find data range
        async with pool.acquire() as conn:
            min_time_us = await conn.fetchval("SELECT MIN(server_time_us) FROM kalshi_orderbook_updates_3")
            max_time_us = await conn.fetchval("SELECT MAX(server_time_us) FROM kalshi_orderbook_updates_3")
        
        if not min_time_us or not max_time_us:
            logger.error("No data in kalshi_orderbook_updates_3 table")
            return
        
        min_time = datetime.fromtimestamp(min_time_us / 1_000_000, tz=timezone.utc)
        max_time = datetime.fromtimestamp(max_time_us / 1_000_000, tz=timezone.utc)
        
        logger.info(f"\nData range in kalshi_orderbook_updates_3:")
        logger.info(f"  Earliest: {min_time.isoformat()}")
        logger.info(f"  Latest:   {max_time.isoformat()}")
        
        # Generate 5 random 15-minute intervals
        num_tests = 5
        logger.info(f"\nGenerating {num_tests} random 15-minute test intervals...\n")
        
        test_intervals = []
        for _ in range(num_tests):
            # Random timestamp between min and max
            random_us = random.randint(min_time_us, max_time_us - 15*60*1_000_000)
            random_dt = datetime.fromtimestamp(random_us / 1_000_000, tz=timezone.utc)
            # Round to 15-minute boundary
            random_dt = round_to_15min(random_dt)
            test_intervals.append(random_dt)
        
        # Run tests
        for i, utc_dt in enumerate(test_intervals, 1):
            await test_kalshi_fetch(pool, utc_dt, i)
        
        logger.info(f"\n{'='*80}")
        logger.info("ALL TESTS COMPLETE")
        logger.info(f"{'='*80}\n")
    
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
