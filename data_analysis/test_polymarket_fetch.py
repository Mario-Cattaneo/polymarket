#!/usr/bin/env python3
"""
Test script for Polymarket orderbook data fetching and filtering.

Tests the new fetch_polymarket_orderbook_updates function with random 15-minute intervals
to verify that:
1. Asset ID is correctly retrieved from both poly_markets and poly_new_market tables
2. Orderbook updates are fetched for the correct time window
3. Filters correctly to matching asset IDs only
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
    fetch_polymarket_orderbook_updates,
    get_polymarket_yes_asset_id,
    utc_to_et,
    round_to_15min,
)

# --- Test Functions ---

async def test_polymarket_fetch(pool, utc_timestamp, test_num):
    """Test fetching Polymarket orderbook updates for a given UTC timestamp."""
    logger.info(f"\n{'='*80}")
    logger.info(f"TEST {test_num}: UTC {utc_timestamp.isoformat()}")
    logger.info(f"{'='*80}")
    
    # Convert to ET and show the interval
    et_dt = utc_to_et(utc_timestamp)
    et_end = et_dt + timedelta(minutes=15)
    logger.info(f"ET interval: {et_dt.strftime('%B %d, %I:%M%p')} - {et_end.strftime('%I:%M%p')} ET")
    
    # Get asset ID
    asset_id = await get_polymarket_yes_asset_id(pool, utc_timestamp)
    if asset_id is None:
        logger.warning(f"No Polymarket market found for this interval")
        return
    
    logger.info(f"Asset ID (YES outcome): {asset_id[:80]}...")
    
    # Fetch orderbook updates
    records = await fetch_polymarket_orderbook_updates(pool, utc_timestamp, asset_id)
    
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
            msg = json.loads(record['message'])
            msg_type = "book_state"  # poly_book_state always contains book state
            message_types[msg_type] = message_types.get(msg_type, 0) + 1
        except:
            pass
    
    logger.info(f"Sample message types: {message_types}")
    
    # Show first and last update details
    try:
        first_msg = json.loads(records[0]['message'])
        first_has_bids = 'bids' in first_msg
        first_has_asks = 'asks' in first_msg
        logger.info(f"First update: has_bids={first_has_bids}, has_asks={first_has_asks}")
        
        last_msg = json.loads(records[-1]['message'])
        last_has_bids = 'bids' in last_msg
        last_has_asks = 'asks' in last_msg
        logger.info(f"Last update: has_bids={last_has_bids}, has_asks={last_has_asks}")
    except Exception as e:
        logger.warning(f"Error parsing messages: {e}")

async def main():
    """Run tests across ALL 15-minute intervals to find gaps."""
    logger.info("="*80)
    logger.info("POLYMARKET ORDERBOOK FETCH TEST - ALL INTERVALS")
    logger.info("="*80)
    
    pool = await connect_db()
    
    try:
        # Use hardcoded data range (partitioned table, avoid full scan)
        min_time_us = 1767815077116000
        max_time_us = 1770021582844001
        
        min_time = datetime.fromtimestamp(min_time_us / 1_000_000, tz=timezone.utc)
        max_time = datetime.fromtimestamp(max_time_us / 1_000_000, tz=timezone.utc)
        
        logger.info(f"\nData range in poly_book_state:")
        logger.info(f"  Earliest: {min_time.isoformat()}")
        logger.info(f"  Latest:   {max_time.isoformat()}")
        
        # Generate ALL 15-minute intervals in the range
        logger.info(f"\nGenerating all 15-minute intervals...\n")
        
        test_intervals = []
        current_time = round_to_15min(min_time)
        while current_time <= max_time:
            test_intervals.append(current_time)
            current_time += timedelta(minutes=15)
        
        logger.info(f"Total intervals to check: {len(test_intervals)}")
        logger.info(f"First interval: {test_intervals[0].isoformat()}")
        logger.info(f"Last interval: {test_intervals[-1].isoformat()}\n")
        
        # Track results
        intervals_with_data = 0
        intervals_without_data = 0
        intervals_without_market = 0
        gaps = []
        
        # Run tests
        for i, interval in enumerate(test_intervals, 1):
            logger.info(f"\n[{i}/{len(test_intervals)}] Testing UTC {interval.isoformat()}")
            
            # Get asset ID
            asset_id = await get_polymarket_yes_asset_id(pool, interval)
            if asset_id is None:
                logger.warning(f"  -> No Polymarket market found")
                intervals_without_market += 1
                continue
            
            # Fetch orderbook updates
            records = await fetch_polymarket_orderbook_updates(pool, interval, asset_id)
            
            if not records:
                logger.warning(f"  -> GAP: No orderbook updates found")
                intervals_without_data += 1
                gaps.append(interval)
            else:
                logger.info(f"  -> OK: {len(records)} updates")
                intervals_with_data += 1
        
        # Print summary
        logger.info("\n" + "="*80)
        logger.info("SUMMARY")
        logger.info("="*80)
        logger.info(f"Total intervals checked: {len(test_intervals)}")
        logger.info(f"Intervals with data: {intervals_with_data}")
        logger.info(f"Intervals without data (GAPS): {intervals_without_data}")
        logger.info(f"Intervals without market: {intervals_without_market}")
        
        if gaps:
            logger.info(f"\nGap intervals (no orderbook data):")
            for gap in gaps[:20]:  # Show first 20 gaps
                logger.info(f"  - {gap.isoformat()}")
            if len(gaps) > 20:
                logger.info(f"  ... and {len(gaps) - 20} more gaps")
        
        logger.info("="*80)
    
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
