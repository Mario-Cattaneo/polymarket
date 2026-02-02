import os
import asyncio
import asyncpg
import json
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

PG_HOST = os.getenv("PG_SOCKET", "localhost")
PG_PORT = os.getenv("POLY_PG_PORT", 5432)
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

async def connect_db():
    return await asyncpg.create_pool(
        host=PG_HOST,
        port=int(PG_PORT),
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        min_size=1,
        max_size=5
    )

async def main():
    pool = await connect_db()
    
    try:
        logger.info("=== POLYMARKET ANALYSIS ===")
        
        async with pool.acquire() as conn:
            # Get all Bitcoin markets
            markets = await conn.fetch("SELECT market_id, message FROM poly_markets WHERE message->>'question' ILIKE '%Bitcoin%' ORDER BY market_id")
            logger.info(f"Total Bitcoin markets: {len(markets)}")
            
            for market in markets[:10]:  # Show first 10
                msg = json.loads(market['message'])
                question = msg.get('question', '')
                logger.info(f"  Market {market['market_id']}: {question}")
        
        logger.info("\n=== KALSHI ANALYSIS ===")
        
        async with pool.acquire() as conn:
            # Get unique Kalshi tickers
            tickers = await conn.fetch("SELECT DISTINCT market_ticker FROM kalshi_markets_3 WHERE market_ticker LIKE '%KXBTC%' ORDER BY market_ticker")
            logger.info(f"Total KXBTC market tickers: {len(tickers)}")
            
            for ticker_row in tickers[:10]:  # Show first 10
                ticker = ticker_row['market_ticker']
                logger.info(f"  Ticker: {ticker}")
        
        logger.info("\n=== DATA TIME RANGES ===")
        
        async with pool.acquire() as conn:
            # Polymarket data range
            poly_range = await conn.fetchrow("SELECT MIN(server_time_us) as min_time, MAX(server_time_us) as max_time FROM poly_book_state")
            if poly_range and poly_range['min_time']:
                min_dt = datetime.fromtimestamp(poly_range['min_time'] / 1_000_000, tz=timezone.utc)
                max_dt = datetime.fromtimestamp(poly_range['max_time'] / 1_000_000, tz=timezone.utc)
                logger.info(f"Polymarket data: {min_dt} to {max_dt}")
            
            # Kalshi data range
            kalshi_range = await conn.fetchrow("SELECT MIN(server_time_us) as min_time, MAX(server_time_us) as max_time FROM kalshi_orderbook_updates_3")
            if kalshi_range and kalshi_range['min_time']:
                min_dt = datetime.fromtimestamp(kalshi_range['min_time'] / 1_000_000, tz=timezone.utc)
                max_dt = datetime.fromtimestamp(kalshi_range['max_time'] / 1_000_000, tz=timezone.utc)
                logger.info(f"Kalshi data: {min_dt} to {max_dt}")
        
        logger.info("\n=== POLYMARKET ASSET_IDs ===")
        
        async with pool.acquire() as conn:
            # Get sample asset_ids with their data ranges
            assets = await conn.fetch("SELECT DISTINCT asset_id FROM poly_book_state LIMIT 10")
            logger.info(f"Sample asset_ids:")
            
            for asset_row in assets:
                asset_id = asset_row['asset_id']
                range_data = await conn.fetchrow("SELECT MIN(server_time_us) as min_time, MAX(server_time_us) as max_time, COUNT(*) as count FROM poly_book_state WHERE asset_id = $1", asset_id)
                
                if range_data and range_data['min_time']:
                    min_dt = datetime.fromtimestamp(range_data['min_time'] / 1_000_000, tz=timezone.utc)
                    max_dt = datetime.fromtimestamp(range_data['max_time'] / 1_000_000, tz=timezone.utc)
                    logger.info(f"  {asset_id}: {min_dt} to {max_dt} ({range_data['count']} records)")
        
        logger.info("\n=== KALSHI TICKER DATA RANGES ===")
        
        async with pool.acquire() as conn:
            # Get data ranges for each Kalshi ticker
            tickers = await conn.fetch("SELECT DISTINCT market_ticker FROM kalshi_orderbook_updates_3 LIMIT 5")
            logger.info(f"Sample ticker data ranges:")
            
            for ticker_row in tickers:
                ticker = ticker_row['market_ticker']
                range_data = await conn.fetchrow("SELECT MIN(server_time_us) as min_time, MAX(server_time_us) as max_time, COUNT(*) as count FROM kalshi_orderbook_updates_3 WHERE market_ticker = $1", ticker)
                
                if range_data and range_data['min_time']:
                    min_dt = datetime.fromtimestamp(range_data['min_time'] / 1_000_000, tz=timezone.utc)
                    max_dt = datetime.fromtimestamp(range_data['max_time'] / 1_000_000, tz=timezone.utc)
                    logger.info(f"  {ticker}: {min_dt} to {max_dt} ({range_data['count']} records)")
        
        logger.info("\n=== MATCHING ANALYSIS ===")
        
        async with pool.acquire() as conn:
            # Get the overall data range
            poly_range = await conn.fetchrow("SELECT MIN(server_time_us) as min_time, MAX(server_time_us) as max_time FROM poly_book_state")
            kalshi_range = await conn.fetchrow("SELECT MIN(server_time_us) as min_time, MAX(server_time_us) as max_time FROM kalshi_orderbook_updates_3")
            
            if poly_range['min_time'] and kalshi_range['min_time']:
                poly_min = datetime.fromtimestamp(poly_range['min_time'] / 1_000_000, tz=timezone.utc)
                poly_max = datetime.fromtimestamp(poly_range['max_time'] / 1_000_000, tz=timezone.utc)
                kalshi_min = datetime.fromtimestamp(kalshi_range['min_time'] / 1_000_000, tz=timezone.utc)
                kalshi_max = datetime.fromtimestamp(kalshi_range['max_time'] / 1_000_000, tz=timezone.utc)
                
                logger.info(f"Polymarket range: {poly_min} to {poly_max}")
                logger.info(f"Kalshi range: {kalshi_min} to {kalshi_max}")
                
                # Find overlapping date
                overlap_start = max(poly_min, kalshi_min)
                overlap_end = min(poly_max, kalshi_max)
                
                if overlap_start < overlap_end:
                    logger.info(f"\n✓ Overlapping period: {overlap_start} to {overlap_end}")
                    
                    # Find 15-min cycles in overlap
                    logger.info("\nFinding 15-minute cycles in overlapping period...")
                    current = overlap_start.replace(minute=(overlap_start.minute // 15) * 15, second=0, microsecond=0)
                    cycles = []
                    while current <= overlap_end:
                        cycles.append(current)
                        current += timedelta(minutes=15)
                    
                    logger.info(f"  Found {len(cycles)} potential cycles")
                    for i, cycle in enumerate(cycles[:5]):
                        logger.info(f"    Cycle {i+1}: {cycle}")
                else:
                    logger.warning("\n✗ NO OVERLAPPING PERIOD between Polymarket and Kalshi data!")
    
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
