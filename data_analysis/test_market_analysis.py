#!/usr/bin/env python3
"""Test script for market analysis functions."""

import asyncio
import os
import logging
from datetime import datetime, timezone
from heatmap_15min_cycles import (
    analyze_poly_markets_coverage,
    get_polymarket_yes_asset_id,
    connect_db,
    round_to_15min
)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

async def main():
    """Run market analysis tests."""
    
    logger.info("=" * 80)
    logger.info("MARKET COVERAGE ANALYSIS")
    logger.info("=" * 80)
    
    # Step 1: Analyze coverage
    first, last, gaps = await analyze_poly_markets_coverage()
    
    if first and last:
        logger.info(f"\nCoverage Summary:")
        logger.info(f"  First market: {first['question']}")
        logger.info(f"  Last market:  {last['question']}")
        logger.info(f"  Total gaps found: {len(gaps)}")
    
    logger.info("\n" + "=" * 80)
    logger.info("TEST ASSET ID EXTRACTION")
    logger.info("=" * 80)
    
    # Step 2: Test asset ID extraction for specific times
    pool = await connect_db()
    
    try:
        # Test a few different times
        test_times_utc = [
            datetime(2026, 1, 8, 0, 30, tzinfo=timezone.utc),  # Jan 8, 00:30 UTC = 7:30 PM ET (Jan 7)
            datetime(2026, 1, 9, 22, 45, tzinfo=timezone.utc),  # Jan 9, 22:45 UTC 
            datetime(2026, 1, 10, 14, 0, tzinfo=timezone.utc),  # Jan 10, 14:00 UTC
        ]
        
        for utc_time in test_times_utc:
            # Round to 15-min boundary
            utc_rounded = round_to_15min(utc_time)
            
            # Get asset ID
            asset_id = await get_polymarket_yes_asset_id(pool, utc_rounded)
            
            if asset_id:
                logger.info(f"\nUTC {utc_rounded.isoformat()}")
                logger.info(f"  Asset ID (YES outcome): {asset_id[:30]}...")
            else:
                logger.warning(f"\nUTC {utc_rounded.isoformat()}: No market found")
    
    finally:
        await pool.close()
    
    logger.info("\n" + "=" * 80)
    logger.info("TESTS COMPLETE")
    logger.info("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
