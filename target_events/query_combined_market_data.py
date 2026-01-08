import os
import asyncio
import asyncpg
import json
from datetime import datetime, timedelta
import re

# --- Environment Variable Setup ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

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
    pattern = f"Bitcoin Up or Down\\s*-\\s*{month_name}\\s+{day},?\\s+{re.escape(start_time)}-{re.escape(end_time)}\\s+ET"
    return pattern

def construct_kalshi_ticker_pattern(et_start_dt):
    """Construct Kalshi ticker for this 15-min window (uses end time)."""
    # Format: KXBTC15M-YYMONYDHHMM-MM
    # et_start_dt is the START time, we need the END time for the ticker
    et_end_dt = et_start_dt + timedelta(minutes=15)
    
    year_short = et_end_dt.strftime("%y")
    month_abbr = et_end_dt.strftime("%b").upper()
    day = et_end_dt.day
    hour = et_end_dt.hour
    minute = et_end_dt.minute
    
    ticker_start = f"KXBTC15M-{year_short}{month_abbr}{day:02d}{hour:02d}{minute:02d}"
    return ticker_start

async def get_combined_market_data(pool, utc_time_str):
    """
    Fetch combined Polymarket and Kalshi data for a given UTC time.
    
    Args:
        pool: asyncpg connection pool
        utc_time_str: UTC time string in format "YYYY-MM-DD HH:MM:SS"
    
    Returns:
        Dictionary with polymarket and kalshi data, or None if not found
    """
    try:
        utc_dt = datetime.strptime(utc_time_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        print(f"Invalid UTC time format. Use: YYYY-MM-DD HH:MM:SS")
        return None
    
    poly_data = await get_polymarket_data(pool, utc_time_str)
    kalshi_data = await get_kalshi_data(pool, utc_time_str)
    
    return {
        'utc_time': utc_time_str,
        'polymarket': poly_data,
        'kalshi': kalshi_data
    }

async def get_polymarket_data(pool, utc_time_str):
    try:
        utc_dt = datetime.strptime(utc_time_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        print(f"Invalid UTC time format. Use: YYYY-MM-DD HH:MM:SS")
        return None
    
    et_dt = utc_to_et(utc_dt)
    question_pattern = construct_polymarket_question_pattern(et_dt)
    
    print(f"\n=== POLYMARKET DATA ===")
    print(f"UTC Start Time: {utc_time_str}")
    print(f"ET Start Time: {et_dt.strftime('%B %d, %I:%M%p')} ET")
    print(f"Matching Question Pattern: {question_pattern}\n")
    
    # Find the market with matching question
    async with pool.acquire() as conn:
        markets = await conn.fetch(
            "SELECT market_id, message FROM poly_markets WHERE message->>'question' ILIKE $1 LIMIT 1",
            f"%Bitcoin Up or Down%{et_dt.strftime('%B').lower()}%{et_dt.day}%{format_et_time(et_dt)}%{format_et_time(et_dt + timedelta(minutes=15))}%"
        )
        
        if not markets:
            print("❌ No matching Polymarket found")
            return None
        
        market = markets[0]
        market_id = market['market_id']
        market_msg = json.loads(market['message'])
        
        question = market_msg.get('question', '')
        print(f"✓ Found Market {market_id}: {question}")
        
        # Get asset_ids from clobTokenIds field (it's a JSON string)
        clob_token_ids_raw = market_msg.get('clobTokenIds', '[]')
        # Parse the JSON string to get the list
        try:
            asset_ids = json.loads(clob_token_ids_raw)
        except (json.JSONDecodeError, TypeError):
            asset_ids = []
        if not asset_ids:
            print("❌ No asset IDs found in market")
            return None
        
        # Get the condition ID (market address) from the market object
        market_address = market_msg.get('market', '')
        
        print(f"Asset IDs: {', '.join(asset_ids[:2])}..." if len(asset_ids) > 2 else f"Asset IDs: {', '.join(asset_ids)}")
        
        # Fetch all related messages for these asset_ids
        results = {
            'market_id': market_id,
            'market_address': market_address,
            'question': question,
            'asset_ids': asset_ids,
            'price_change': [],
            'tick_size_change': [],
            'last_trade_price': [],
            'book': []
        }
        
        # Query each table
        # Price changes are stored by asset_id (each price_change item becomes a row)
        for asset_id in asset_ids:
            price_changes = await conn.fetch(
                "SELECT found_time_us, server_time_us, message FROM poly_price_change WHERE asset_id = $1 ORDER BY server_time_us ASC",
                asset_id
            )
            results['price_change'].extend([
                {'asset_id': asset_id, 'found_time_us': row['found_time_us'], 'server_time_us': row['server_time_us'], 'data': json.loads(row['message'])}
                for row in price_changes
            ])
            # Tick size changes
            tick_changes = await conn.fetch(
                "SELECT found_time_us, server_time_us, message FROM poly_tick_size_change WHERE asset_id = $1 ORDER BY server_time_us ASC",
                asset_id
            )
            results['tick_size_change'].extend([
                {'asset_id': asset_id, 'found_time_us': row['found_time_us'], 'server_time_us': row['server_time_us'], 'data': json.loads(row['message'])}
                for row in tick_changes
            ])
            
            # Last trade prices
            trades = await conn.fetch(
                "SELECT found_time_us, server_time_us, message FROM poly_last_trade_price WHERE asset_id = $1 ORDER BY server_time_us ASC",
                asset_id
            )
            results['last_trade_price'].extend([
                {'asset_id': asset_id, 'found_time_us': row['found_time_us'], 'server_time_us': row['server_time_us'], 'data': json.loads(row['message'])}
                for row in trades
            ])
            
            # Books
            books = await conn.fetch(
                "SELECT found_time_us, server_time_us, message FROM poly_book WHERE asset_id = $1 ORDER BY server_time_us ASC",
                asset_id
            )
            results['book'].extend([
                {'asset_id': asset_id, 'found_time_us': row['found_time_us'], 'server_time_us': row['server_time_us'], 'data': json.loads(row['message'])}
                for row in books
            ])
        
        return results

async def get_kalshi_data(pool, utc_time_str):
    """
    Fetch Kalshi data for a given UTC time.
    utc_time_str format: "2026-01-07 11:00:00"
    """
    try:
        utc_dt = datetime.strptime(utc_time_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        print(f"Invalid UTC time format. Use: YYYY-MM-DD HH:MM:SS")
        return None
    
    et_dt = utc_to_et(utc_dt)
    ticker_pattern = construct_kalshi_ticker_pattern(et_dt)
    
    print(f"\n=== KALSHI DATA ===")
    print(f"UTC Start Time: {utc_time_str}")
    print(f"ET Start Time: {et_dt.strftime('%B %d, %I:%M%p')} ET")
    print(f"Matching Ticker Pattern: {ticker_pattern}*\n")
    
    # Find markets matching the ticker pattern
    async with pool.acquire() as conn:
        markets = await conn.fetch(
            "SELECT market_ticker FROM kalshi_markets_3 WHERE market_ticker LIKE $1 LIMIT 1",
            ticker_pattern + '%'
        )
        
        if not markets:
            print("❌ No matching Kalshi markets found")
            return None
        
        market_ticker = markets[0]['market_ticker']
        print(f"✓ Found Market Ticker: {market_ticker}")
        
        results = {
            'market_ticker': market_ticker,
            'orderbook_updates': [],
            'trades': []
        }
        
        # Fetch all related messages
        orderbook_updates = await conn.fetch(
            "SELECT found_time_us, server_time_us, message FROM kalshi_orderbook_updates_3 WHERE market_ticker = $1 ORDER BY server_time_us ASC",
            market_ticker
        )
        results['orderbook_updates'] = [
            {'found_time_us': row['found_time_us'], 'server_time_us': row['server_time_us'], 'data': json.loads(row['message'])}
            for row in orderbook_updates
        ]
        
        trades = await conn.fetch(
            "SELECT found_time_us, server_time_us, message FROM kalshi_trades_3 WHERE market_ticker = $1 ORDER BY server_time_us ASC",
            market_ticker
        )
        results['trades'] = [
            {'found_time_us': row['found_time_us'], 'server_time_us': row['server_time_us'], 'data': json.loads(row['message'])}
            for row in trades
        ]
        
        return results
async def print_summary(poly_data, kalshi_data):
    """Print a summary of the retrieved data."""
    print("\n" + "="*80)
    print("DATA SUMMARY")
    print("="*80)
    
    if poly_data:
        print(f"\n📊 POLYMARKET: {poly_data['market_id']}")
        print(f"   Question: {poly_data['question']}")
        print(f"   Price Changes: {len(poly_data['price_change'])}")
        print(f"   Tick Size Changes: {len(poly_data['tick_size_change'])}")
        print(f"   Last Trade Prices: {len(poly_data['last_trade_price'])}")
        print(f"   Books: {len(poly_data['book'])}")
    
    if kalshi_data:
        print(f"\n📊 KALSHI: {kalshi_data['market_ticker']}")
        print(f"   Orderbook Updates: {len(kalshi_data['orderbook_updates'])}")
        print(f"   Trades: {len(kalshi_data['trades'])}")
    
    print("\n" + "="*80)

async def main():
    """Main function to query and display market data."""
    import sys
    
    # Test with Jan 6 18:00 UTC
    test_utc_time = "2026-01-06 20:30:00"
    
    pool = await connect_db()
    try:
        print(f"Testing with UTC time: {test_utc_time}\n")
        combined_data = await get_combined_market_data(pool, test_utc_time)
        
        if not combined_data:
            print("❌ Failed to retrieve data")
            return
        
        # Print counts for each message type
        print("\n" + "="*80)
        print("MESSAGE COUNTS")
        print("="*80)
        
        poly_data = combined_data['polymarket']
        kalshi_data = combined_data['kalshi']
        
        if poly_data:
            print(f"\n🔷 POLYMARKET ({poly_data['market_id']})")
            print(f"   Market: {poly_data['question']}")
            print(f"   Price Changes:     {len(poly_data['price_change']):6d}")
            print(f"   Tick Size Changes: {len(poly_data['tick_size_change']):6d}")
            print(f"   Last Trade Prices: {len(poly_data['last_trade_price']):6d}")
            print(f"   Books:             {len(poly_data['book']):6d}")
            total_poly = sum([
                len(poly_data['price_change']),
                len(poly_data['tick_size_change']),
                len(poly_data['last_trade_price']),
                len(poly_data['book'])
            ])
            print(f"   TOTAL:             {total_poly:6d}")
        else:
            print(f"\n🔷 POLYMARKET: No data found")
        
        if kalshi_data:
            print(f"\n🔸 KALSHI ({kalshi_data['market_ticker']})")
            print(f"   Orderbook Updates: {len(kalshi_data['orderbook_updates']):6d}")
            print(f"   Trades:            {len(kalshi_data['trades']):6d}")
            total_kalshi = sum([
                len(kalshi_data['orderbook_updates']),
                len(kalshi_data['trades'])
            ])
            print(f"   TOTAL:             {total_kalshi:6d}")
        else:
            print(f"\n🔸 KALSHI: No data found")
        
        print("\n" + "="*80)
        
        # Save detailed data to JSON
        if poly_data or kalshi_data:
            output_file = f"market_data_{test_utc_time.replace(' ', '_').replace(':', '')}.json"
            with open(output_file, 'w') as f:
                json.dump(combined_data, f, indent=2, default=str)
            print(f"\n✓ Detailed data saved to {output_file}")
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
