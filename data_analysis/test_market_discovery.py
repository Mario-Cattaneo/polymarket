#!/usr/bin/env python3
"""Test market discovery functions for Bitcoin 15-min cycles.

Tests:
1. get_polymarket_coverage_info() - comprehensive market coverage analysis
2. get_polymarket_yes_asset_id() - get YES asset ID for a specific UTC timestamp
"""

import asyncio
import os
import logging
from datetime import datetime, timezone, timedelta
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Add data_analysis to path
sys.path.insert(0, os.path.dirname(__file__))

from heatmap_15min_cycles import (
    connect_db,
    get_polymarket_coverage_info,
    get_polymarket_yes_asset_id,
    utc_to_et
)

async def main():
    """Run market discovery tests."""
    pool = await connect_db()
    
    try:
        logger.info("=" * 80)
        logger.info("MARKET COVERAGE ANALYSIS")
        logger.info("=" * 80)
        
        # Test 1: Get comprehensive coverage info
        coverage_info = await get_polymarket_coverage_info(pool)
        
        if coverage_info:
            first_utc = coverage_info['first_utc']
            last_utc = coverage_info['last_utc']
            
            logger.info(f"First market: {coverage_info['first_question']}")
            logger.info(f"  UTC: {first_utc.isoformat()}")
            logger.info(f"  ET:  {utc_to_et(first_utc).isoformat()}")
            
            logger.info(f"\nLast market: {coverage_info['last_question']}")
            logger.info(f"  UTC: {last_utc.isoformat()}")
            logger.info(f"  ET:  {utc_to_et(last_utc).isoformat()}")
            
            if coverage_info['gaps']:
                logger.warning(f"\nFound {len(coverage_info['gaps'])} gaps in market coverage:")
                for gap in coverage_info['gaps'][:5]:  # Show first 5
                    logger.warning(f"  Gap: {gap['gap_minutes']}min instead of 15min")
                    logger.warning(f"    After: {gap['prev_question'][:60]}...")
                    logger.warning(f"    Next:  {gap['next_question'][:60]}...")
            else:
                logger.info("\nNo gaps detected - continuous 15-minute market coverage")
            
            logger.info(f"\n" + "=" * 80)
            logger.info("ASSET ID EXTRACTION TEST")
            logger.info("=" * 80)
            
            # Test 2: Extract YES asset IDs for specific UTC timestamps
            test_cases = [
                datetime(2026, 1, 8, 0, 30, tzinfo=timezone.utc),   # Should exist
                datetime(2026, 1, 9, 22, 45, tzinfo=timezone.utc),  # Should exist
                datetime(2026, 1, 10, 14, 0, tzinfo=timezone.utc),  # Should exist
                datetime(2026, 1, 7, 12, 0, tzinfo=timezone.utc),   # Before first market (gap)
            ]
            
            for utc_ts in test_cases:
                logger.info(f"\nUTC: {utc_ts.isoformat()}")
                logger.info(f"ET:  {utc_to_et(utc_ts).isoformat()}")
                
                asset_id = await get_polymarket_yes_asset_id(pool, utc_ts)
                if asset_id:
                    logger.info(f"  Asset ID (YES outcome): {asset_id[:40]}...")
                else:
                    logger.warning(f"  Asset ID: NOT FOUND (gap period or market not available)")
            
            logger.info(f"\n" + "=" * 80)
            logger.info("TESTS COMPLETE")
            logger.info("=" * 80)
        
        else:
            logger.warning("No market coverage information available")
    
    finally:
        await pool.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    asyncio.run(main())
