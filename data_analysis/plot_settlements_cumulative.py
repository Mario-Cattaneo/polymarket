#!/usr/bin/env python3
"""
Plot cumulative count over time for 5 settlement categories:
1. Complementary trade (settlements with type='complementary')
2. Merge trade (settlements with type='merge')
3. Split trade (settlements with type='split')
4. Split alone (PositionSplit in CFT_no_exchange with no matching trade)
5. Merge alone (PositionsMerge in CFT_no_exchange with no matching trade)

Samples 100k records per category using equal buckets to avoid too many points.
"""

import os
import asyncio
import asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

# Database Credentials
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

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

async def get_settlement_data(conn, settlement_type):
    """
    Fetch settlement data for a given type, sampled to ~100k records but scaled to real total.
    Returns (timestamps, cumulative_counts, total_count)
    """
    # Get total count and time range
    row = await conn.fetchrow(
        "SELECT COUNT(*) as total FROM settlements WHERE type = $1",
        settlement_type
    )
    total_count = row['total']
    
    if total_count == 0:
        logger.info(f"  No records found")
        return [], [], 0
    
    # Determine sampling rate to get ~100k samples
    sample_rate = max(1, total_count // 100000)
    logger.info(f"  Total: {total_count:,}, sampling rate: 1/{sample_rate}")
    
    # Fetch every Nth row ordered by timestamp
    query = """
        SELECT timestamp_ms FROM (
            SELECT 
                timestamp_ms,
                ROW_NUMBER() OVER (ORDER BY timestamp_ms ASC) as rn
            FROM settlements
            WHERE type = $1
        ) sub
        WHERE rn % $2 = 1
        ORDER BY timestamp_ms ASC
    """
    
    rows = await conn.fetch(query, settlement_type, sample_rate)
    timestamps = [row['timestamp_ms'] for row in rows]
    
    # Create cumulative count by multiplying sampled count by sample_rate
    cumulative = [i * sample_rate for i in range(1, len(timestamps) + 1)]
    # Ensure last value is exactly the total count
    if cumulative:
        cumulative[-1] = total_count
    
    logger.info(f"  Sampled {len(timestamps):,} records, scaled to total {total_count:,}")
    return timestamps, cumulative, total_count

async def get_cft_no_exchange_data(conn, event_name):
    """
    Fetch CFT_no_exchange data for a given event type, sampled to ~100k records but scaled to real total.
    Returns (timestamps, cumulative_counts, total_count)
    """
    # Get total count
    row = await conn.fetchrow(
        "SELECT COUNT(*) as total FROM CFT_no_exchange WHERE event_name = $1",
        event_name
    )
    total_count = row['total']
    
    if total_count == 0:
        logger.info(f"  No records found")
        return [], [], 0
    
    # Determine sampling rate to get ~100k samples
    sample_rate = max(1, total_count // 100000)
    logger.info(f"  Total: {total_count:,}, sampling rate: 1/{sample_rate}")
    
    # Fetch every Nth row ordered by timestamp
    query = """
        SELECT timestamp_ms FROM (
            SELECT 
                timestamp_ms,
                ROW_NUMBER() OVER (ORDER BY timestamp_ms ASC) as rn
            FROM CFT_no_exchange
            WHERE event_name = $1
        ) sub
        WHERE rn % $2 = 1
        ORDER BY timestamp_ms ASC
    """
    
    rows = await conn.fetch(query, event_name, sample_rate)
    timestamps = [row['timestamp_ms'] for row in rows]
    
    # Create cumulative count by multiplying sampled count by sample_rate
    cumulative = [i * sample_rate for i in range(1, len(timestamps) + 1)]
    # Ensure last value is exactly the total count
    if cumulative:
        cumulative[-1] = total_count
    
    logger.info(f"  Sampled {len(timestamps):,} records, scaled to total {total_count:,}")
    return timestamps, cumulative, total_count

def compute_cumulative(timestamps):
    """Convert timestamps to cumulative count."""
    if not timestamps:
        return [], []
    
    sorted_ts = sorted(timestamps)
    return sorted_ts, list(range(1, len(sorted_ts) + 1))

def convert_timestamp_to_datetime(ts_ms):
    """Convert millisecond timestamp to datetime."""
    return datetime.fromtimestamp(ts_ms / 1000.0)

async def main():
    pool = await connect_db()
    
    try:
        async with pool.acquire() as conn:
            logger.info("\n" + "=" * 80)
            logger.info("Fetching data for 5 settlement categories")
            logger.info("=" * 80 + "\n")
            
            # Fetch data for each category
            logger.info("1. Complementary trades (settlements.type='complementary')")
            comp_ts, comp_cum, comp_total = await get_settlement_data(conn, 'complementary')
            
            logger.info("\n2. Merge trades (settlements.type='merge')")
            merge_trade_ts, merge_trade_cum, merge_trade_total = await get_settlement_data(conn, 'merge')
            
            logger.info("\n3. Split trades (settlements.type='split')")
            split_trade_ts, split_trade_cum, split_trade_total = await get_settlement_data(conn, 'split')
            
            logger.info("\n4. Split alone (CFT_no_exchange.event_name='PositionSplit')")
            split_alone_ts, split_alone_cum, split_alone_total = await get_cft_no_exchange_data(conn, 'PositionSplit')
            
            logger.info("\n5. Merge alone (CFT_no_exchange.event_name='PositionsMerge')")
            merge_alone_ts, merge_alone_cum, merge_alone_total = await get_cft_no_exchange_data(conn, 'PositionsMerge')
            
            # Create single plot with all 5 categories
            logger.info("\n" + "=" * 80)
            logger.info("Creating cumulative plot")
            logger.info("=" * 80)
            
            fig, ax = plt.subplots(figsize=(14, 8))
            
            # Convert timestamps to datetime
            if comp_ts:
                ax.plot([convert_timestamp_to_datetime(ts) for ts in comp_ts], comp_cum, 
                       color='blue', linewidth=2, label=f'Complementary (n={comp_total:,})', alpha=0.8)
            
            if merge_trade_ts:
                ax.plot([convert_timestamp_to_datetime(ts) for ts in merge_trade_ts], merge_trade_cum,
                       color='green', linewidth=2, label=f'Merge Trade (n={merge_trade_total:,})', alpha=0.8)
            
            if split_trade_ts:
                ax.plot([convert_timestamp_to_datetime(ts) for ts in split_trade_ts], split_trade_cum,
                       color='red', linewidth=2, label=f'Split Trade (n={split_trade_total:,})', alpha=0.8)
            
            if split_alone_ts:
                ax.plot([convert_timestamp_to_datetime(ts) for ts in split_alone_ts], split_alone_cum,
                       color='orange', linewidth=2, label=f'Split Alone (n={split_alone_total:,})', alpha=0.8)
            
            if merge_alone_ts:
                ax.plot([convert_timestamp_to_datetime(ts) for ts in merge_alone_ts], merge_alone_cum,
                       color='purple', linewidth=2, label=f'Merge Alone (n={merge_alone_total:,})', alpha=0.8)
            
            ax.set_title('Cumulative Count Over Time - All Settlement Categories', fontsize=14, fontweight='bold')
            ax.set_xlabel('Date', fontsize=12)
            ax.set_ylabel('Cumulative Count', fontsize=12)
            ax.legend(fontsize=10, loc='upper left')
            ax.grid(True, alpha=0.3)
            ax.tick_params(axis='x', rotation=45)
            
            plt.tight_layout()
            
            # Save figure
            output_path = '/home/mario_cattaneo/polymarket/data_analysis/plots/cumulative_settlements.png'
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            plt.savefig(output_path, dpi=150, bbox_inches='tight')
            logger.info(f"\n✅ Plot saved to {output_path}")
            
            # Print summary
            logger.info("\n" + "=" * 80)
            logger.info("SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Complementary trades:  {comp_total:>12,}")
            logger.info(f"Merge trades:          {merge_trade_total:>12,}")
            logger.info(f"Split trades:          {split_trade_total:>12,}")
            logger.info(f"Split alone:           {split_alone_total:>12,}")
            logger.info(f"Merge alone:           {merge_alone_total:>12,}")
            logger.info(f"{'TOTAL':30} {sum([comp_total, merge_trade_total, split_trade_total, split_alone_total, merge_alone_total]):>12,}")
    
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
