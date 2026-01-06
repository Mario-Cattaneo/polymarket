#!/usr/bin/env python3
"""
Query market data from Kalshi and Polymarket databases for a given 15-minute UTC start time.
"""

import os
import asyncio
import asyncpg
from datetime import datetime, timedelta
import pytz

# --- Environment Variable Setup ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")


def utc_to_et(utc_time):
    """Convert UTC datetime to ET (Eastern Time)."""
    if isinstance(utc_time, str):
        utc_time = datetime.fromisoformat(utc_time)
    
    utc_zone = pytz.UTC
    et_zone = pytz.timezone('America/New_York')
    
    if utc_time.tzinfo is None:
        utc_time = utc_zone.localize(utc_time)
    
    et_time = utc_time.astimezone(et_zone)
    return et_time


def format_kalshi_ticker(utc_start_time):
    """
    Format Kalshi market ticker from UTC start time.
    Kalshi ticker format: KXBTC15M-26JAN061215-15
    The time in ticker is the END time, so add 15 minutes to start time.
    """
    et_start = utc_to_et(utc_start_time)
    et_end = et_start + timedelta(minutes=15)
    
    # Format: DDMMMHHMMSS-MM where MMM is uppercase month abbreviation
    day = et_end.strftime('%d')
    month = et_end.strftime('%b').upper()
    year = et_end.strftime('%y')
    hour = et_end.strftime('%H')
    minute = et_end.strftime('%M')
    
    ticker = f"KXBTC15M-{year}{month}{day}{hour}{minute}-{minute}"
    return ticker


def format_polymarket_question(utc_start_time):
    """
    Format Polymarket question pattern from UTC start time.
    Format: "Bitcoin Up or Down - January 6, 12:00PM-12:15PM ET"
    """
    et_start = utc_to_et(utc_start_time)
    et_end = et_start + timedelta(minutes=15)
    
    # Month name, day
    month_day = et_start.strftime('%B %-d')  # e.g., "January 6"
    
    # Start time: 12:00PM format
    start_hour = et_start.hour
    start_min = et_start.minute
    start_ampm = 'AM' if start_hour < 12 else 'PM'
    if start_hour == 0:
        start_hour = 12
    elif start_hour > 12:
        start_hour -= 12
    start_time = f"{start_hour}:{start_min:02d}{start_ampm}"
    
    # End time
    end_hour = et_end.hour
    end_min = et_end.minute
    end_ampm = 'AM' if end_hour < 12 else 'PM'
    if end_hour == 0:
        end_hour = 12
    elif end_hour > 12:
        end_hour -= 12
    end_time = f"{end_hour}:{end_min:02d}{end_ampm}"
    
    question = f"Bitcoin Up or Down - {month_day}, {start_time}-{end_time} ET"
    return question


async def get_market_data(utc_start_time):
    """
    Retrieve all market data for a given UTC start time.
    
    Args:
        utc_start_time: datetime object or ISO format string for UTC start time
        
    Returns:
        dict with keys: kalshi_trades, kalshi_orderbook, polymarket_book, 
                       polymarket_price_change, polymarket_tick_size, 
                       polymarket_last_trade_price
    """
    # Format identifiers
    kalshi_ticker = format_kalshi_ticker(utc_start_time)
    poly_question = format_polymarket_question(utc_start_time)
    
    print(f"Querying for UTC start time: {utc_start_time}")
    print(f"Kalshi ticker: {kalshi_ticker}")
    print(f"Polymarket question: {poly_question}")
    print()
    
    # Connect to database
    pool = await asyncpg.create_pool(
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        host=PG_HOST,
        port=PG_PORT
    )
    
    try:
        # Query Kalshi trades
        kalshi_trades = await pool.fetch("""
            SELECT * FROM kalshi_trades_3
            WHERE market_ticker = $1
            ORDER BY server_time_us
        """, kalshi_ticker)
        
        # Query Kalshi orderbook updates
        kalshi_orderbook = await pool.fetch("""
            SELECT * FROM kalshi_orderbook_updates_3
            WHERE market_ticker = $1
            ORDER BY server_time_us
        """, kalshi_ticker)
        
        # Get Polymarket market_id from poly_markets
        poly_market = await pool.fetchrow("""
            SELECT market_id, message->>'conditionId' as condition_id
            FROM poly_markets
            WHERE message->>'question' = $1
        """, poly_question)
        
        if not poly_market:
            print(f"Warning: No Polymarket market found for question: {poly_question}")
            return {
                'kalshi_ticker': kalshi_ticker,
                'polymarket_question': poly_question,
                'kalshi_trades': kalshi_trades,
                'kalshi_orderbook': kalshi_orderbook,
                'polymarket_book': [],
                'polymarket_price_change': [],
                'polymarket_tick_size': [],
                'polymarket_last_trade_price': []
            }
        
        market_id = poly_market['market_id']
        condition_id = poly_market['condition_id']
        
        print(f"Found Polymarket market_id: {market_id}, condition_id: {condition_id}")
        print()
        
        # Query Polymarket book
        poly_book = await pool.fetch("""
            SELECT * FROM poly_book
            WHERE market_address = $1
            ORDER BY found_time_ms
        """, condition_id)
        
        # Query Polymarket price_change
        poly_price_change = await pool.fetch("""
            SELECT * FROM poly_price_change
            WHERE market_address = $1
            ORDER BY found_time_ms
        """, condition_id)
        
        # Query Polymarket tick_size_change
        poly_tick_size = await pool.fetch("""
            SELECT * FROM poly_tick_size_change
            WHERE market_address = $1
            ORDER BY found_time_ms
        """, condition_id)
        
        # Query Polymarket last_trade_price
        poly_last_trade = await pool.fetch("""
            SELECT * FROM poly_last_trade_price
            WHERE market_address = $1
            ORDER BY found_time_ms
        """, condition_id)
        
        return {
            'kalshi_ticker': kalshi_ticker,
            'polymarket_question': poly_question,
            'polymarket_market_id': market_id,
            'polymarket_condition_id': condition_id,
            'kalshi_trades': kalshi_trades,
            'kalshi_orderbook': kalshi_orderbook,
            'polymarket_book': poly_book,
            'polymarket_price_change': poly_price_change,
            'polymarket_tick_size': poly_tick_size,
            'polymarket_last_trade_price': poly_last_trade
        }
    
    finally:
        await pool.close()


async def print_market_summary(utc_start_time):
    """Print a summary of market data for a given UTC start time."""
    data = await get_market_data(utc_start_time)
    
    print("=" * 80)
    print("MARKET DATA SUMMARY")
    print("=" * 80)
    print(f"Kalshi Ticker: {data['kalshi_ticker']}")
    print(f"Polymarket Question: {data['polymarket_question']}")
    if 'polymarket_market_id' in data:
        print(f"Polymarket Market ID: {data['polymarket_market_id']}")
        print(f"Polymarket Condition ID: {data['polymarket_condition_id']}")
    print()
    
    print(f"Kalshi Trades: {len(data['kalshi_trades'])} rows")
    print(f"Kalshi Orderbook Updates: {len(data['kalshi_orderbook'])} rows")
    print(f"  - Snapshots: {sum(1 for r in data['kalshi_orderbook'] if r['message'].get('type') == 'orderbook_snapshot')}")
    print(f"  - Deltas: {sum(1 for r in data['kalshi_orderbook'] if r['message'].get('type') == 'orderbook_delta')}")
    print()
    
    print(f"Polymarket Book: {len(data['polymarket_book'])} rows")
    print(f"Polymarket Price Change: {len(data['polymarket_price_change'])} rows")
    print(f"Polymarket Tick Size Change: {len(data['polymarket_tick_size'])} rows")
    print(f"Polymarket Last Trade Price: {len(data['polymarket_last_trade_price'])} rows")
    print("=" * 80)
    
    return data


# Example usage
if __name__ == "__main__":
    # Example: Query for January 6, 2026 12:00 PM ET (17:00 UTC)
    utc_start = datetime(2026, 1, 6, 17, 0, 0)
    
    # Or use current time rounded to last 15-minute interval
    # now = datetime.utcnow()
    # minutes = (now.minute // 15) * 15
    # utc_start = now.replace(minute=minutes, second=0, microsecond=0)
    
    result = asyncio.run(print_market_summary(utc_start))
