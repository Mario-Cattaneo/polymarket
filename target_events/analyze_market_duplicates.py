import os
import asyncio
import asyncpg
import json
from datetime import datetime, timedelta
import re
from collections import defaultdict

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

async def get_polymarket_market(pool, utc_time_str):
    """
    Find Polymarket market at given UTC time.
    Returns market_id, market_address, and asset_ids.
    """
    try:
        utc_dt = datetime.strptime(utc_time_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        print(f"Invalid UTC time format. Use: YYYY-MM-DD HH:MM:SS")
        return None
    
    et_dt = utc_to_et(utc_dt)
    
    print(f"\n=== FINDING POLYMARKET ===")
    print(f"UTC Start Time: {utc_time_str}")
    print(f"ET Start Time: {et_dt.strftime('%B %d, %I:%M%p')} ET\n")
    
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
        print(f"✓ Found Market {market_id}")
        print(f"  Question: {question}")
        
        # Get asset_ids from clobTokenIds field
        clob_token_ids_raw = market_msg.get('clobTokenIds', '[]')
        try:
            asset_ids = json.loads(clob_token_ids_raw)
        except (json.JSONDecodeError, TypeError):
            asset_ids = []
        
        if not asset_ids:
            print("❌ No asset IDs found in market")
            return None
        
        market_address = market_msg.get('market', '')
        
        print(f"  Number of Asset IDs: {len(asset_ids)}")
        print(f"  Asset IDs: {', '.join(asset_ids[:2])}..." if len(asset_ids) > 2 else f"  Asset IDs: {', '.join(asset_ids)}")
        
        return {
            'market_id': market_id,
            'market_address': market_address,
            'question': question,
            'asset_ids': asset_ids
        }

async def count_duplicates_last_trade_price(conn, asset_id):
    """
    Count total rows, ambiguous rows (same server_time_us, price, side),
    and fully duplicate rows (transaction hash is the same).
    Uses SQL aggregation for speed.
    """
    # Total count
    total_count = await conn.fetchval(
        "SELECT COUNT(*) FROM poly_last_trade_price WHERE asset_id = $1",
        asset_id
    )
    
    # Count ambiguous rows (same server_time_us, price, side)
    ambiguous_result = await conn.fetch("""
        SELECT server_time_us, message->>'price' as price, message->>'side' as side, COUNT(*) as cnt
        FROM poly_last_trade_price
        WHERE asset_id = $1
        GROUP BY server_time_us, message->>'price', message->>'side'
        HAVING COUNT(*) > 1
    """, asset_id)
    
    total_ambiguous_rows = sum(row['cnt'] for row in ambiguous_result)
    ambiguous_tuples = len(ambiguous_result)
    
    # Count fully duplicate rows (same transaction_hash)
    fully_duplicate_result = await conn.fetch("""
        SELECT message->>'transaction_hash' as tx_hash, COUNT(*) as cnt
        FROM poly_last_trade_price
        WHERE asset_id = $1 AND message->>'transaction_hash' IS NOT NULL
        GROUP BY message->>'transaction_hash'
        HAVING COUNT(*) > 1
    """, asset_id)
    
    total_fully_duplicate_rows = sum(row['cnt'] for row in fully_duplicate_result)
    fully_duplicate_hashes = len(fully_duplicate_result)
    
    return {
        'total_rows': total_count,
        'unique_ambiguous_tuples': ambiguous_tuples,
        'ambiguous_tuples': ambiguous_tuples,
        'total_ambiguous_rows': total_ambiguous_rows,
        'fully_duplicate_tx_hashes': fully_duplicate_hashes,
        'total_fully_duplicate_rows': total_fully_duplicate_rows,
    }

async def count_duplicates_price_change(conn, asset_id):
    """
    Count total rows, ambiguous rows (same server_time_us, price, side),
    and fully duplicate rows (same quantity as well).
    Uses SQL aggregation for speed.
    """
    # Total count
    total_count = await conn.fetchval(
        "SELECT COUNT(*) FROM poly_price_change WHERE asset_id = $1",
        asset_id
    )
    
    # Count ambiguous rows (same server_time_us, price, side)
    ambiguous_result = await conn.fetch("""
        SELECT server_time_us, message->>'price' as price, message->>'side' as side, COUNT(*) as cnt
        FROM poly_price_change
        WHERE asset_id = $1
        GROUP BY server_time_us, message->>'price', message->>'side'
        HAVING COUNT(*) > 1
    """, asset_id)
    
    total_ambiguous_rows = sum(row['cnt'] for row in ambiguous_result)
    ambiguous_tuples = len(ambiguous_result)
    
    # Count fully duplicate rows (same server_time_us, price, side, size)
    fully_duplicate_result = await conn.fetch("""
        SELECT server_time_us, message->>'price' as price, message->>'side' as side, 
               message->>'size' as size, COUNT(*) as cnt
        FROM poly_price_change
        WHERE asset_id = $1
        GROUP BY server_time_us, message->>'price', message->>'side', message->>'size'
        HAVING COUNT(*) > 1
    """, asset_id)
    
    total_fully_duplicate_rows = sum(row['cnt'] for row in fully_duplicate_result)
    fully_duplicate_tuples = len(fully_duplicate_result)
    
    return {
        'total_rows': total_count,
        'unique_ambiguous_tuples': ambiguous_tuples,
        'ambiguous_tuples': ambiguous_tuples,
        'total_ambiguous_rows': total_ambiguous_rows,
        'fully_duplicate_tuples': fully_duplicate_tuples,
        'total_fully_duplicate_rows': total_fully_duplicate_rows,
    }

async def count_combined_book_price_change(conn, asset_id):
    """
    Combine book and price_change messages and count:
    - Total combined rows
    - Ambiguous rows (same server_time_us between book and price_change)
    """
    books = await conn.fetch(
        "SELECT 'book' as msg_type, server_time_us FROM poly_book WHERE asset_id = $1 ORDER BY server_time_us ASC",
        asset_id
    )
    
    price_changes = await conn.fetch(
        "SELECT 'price_change' as msg_type, server_time_us FROM poly_price_change WHERE asset_id = $1 ORDER BY server_time_us ASC",
        asset_id
    )
    
    total_book_rows = len(books)
    total_pc_rows = len(price_changes)
    total_combined_rows = total_book_rows + total_pc_rows
    
    # Track timestamps and their sources
    timestamp_sources = defaultdict(list)
    
    for row in books:
        timestamp_sources[row['server_time_us']].append('book')
    
    for row in price_changes:
        timestamp_sources[row['server_time_us']].append('price_change')
    
    # Find ambiguous timestamps (where both book and price_change exist)
    ambiguous_timestamps = {}
    for ts, sources in timestamp_sources.items():
        if 'book' in sources and 'price_change' in sources:
            ambiguous_timestamps[ts] = {
                'book_count': sources.count('book'),
                'price_change_count': sources.count('price_change')
            }
    
    total_ambiguous_book_rows = sum(v['book_count'] for v in ambiguous_timestamps.values())
    total_ambiguous_pc_rows = sum(v['price_change_count'] for v in ambiguous_timestamps.values())
    total_ambiguous_combined_rows = total_ambiguous_book_rows + total_ambiguous_pc_rows
    
    return {
        'total_book_rows': total_book_rows,
        'total_price_change_rows': total_pc_rows,
        'total_combined_rows': total_combined_rows,
        'ambiguous_timestamps': len(ambiguous_timestamps),
        'total_ambiguous_book_rows': total_ambiguous_book_rows,
        'total_ambiguous_pc_rows': total_ambiguous_pc_rows,
        'total_ambiguous_combined_rows': total_ambiguous_combined_rows,
        'ambiguous_timestamp_distribution': dict(ambiguous_timestamps)
    }

async def count_duplicates_book(conn, asset_id):
    """
    Count total rows, ambiguous rows (same server_time_us),
    and fully duplicate rows (same server_time_us AND same bids and asks strings).
    Uses SQL aggregation for speed.
    """
    # Total count
    total_count = await conn.fetchval(
        "SELECT COUNT(*) FROM poly_book WHERE asset_id = $1",
        asset_id
    )
    
    # Count ambiguous rows (same server_time_us, regardless of bids/asks)
    ambiguous_result = await conn.fetch("""
        SELECT server_time_us, COUNT(*) as cnt
        FROM poly_book
        WHERE asset_id = $1
        GROUP BY server_time_us
        HAVING COUNT(*) > 1
    """, asset_id)
    
    total_ambiguous_rows = sum(row['cnt'] for row in ambiguous_result)
    ambiguous_times = len(ambiguous_result)
    
    # Count fully duplicate rows (same server_time_us AND same bids/asks)
    fully_duplicate_result = await conn.fetch("""
        SELECT server_time_us, COUNT(*) as cnt
        FROM poly_book
        WHERE asset_id = $1
        GROUP BY server_time_us, 
                 (message->'bids')::TEXT,
                 (message->'asks')::TEXT
        HAVING COUNT(*) > 1
    """, asset_id)
    
    total_fully_duplicate_rows = sum(row['cnt'] for row in fully_duplicate_result)
    fully_duplicate_tuples = len(fully_duplicate_result)
    
    return {
        'total_rows': total_count,
        'ambiguous_times': ambiguous_times,
        'total_ambiguous_rows': total_ambiguous_rows,
        'fully_duplicate_tuples': fully_duplicate_tuples,
        'total_fully_duplicate_rows': total_fully_duplicate_rows,
    }

async def analyze_market_data(pool, market_data):
    """
    Analyze data for both asset IDs using fast SQL queries.
    """
    print(f"\n=== ANALYZING MARKET DATA ===\n")
    
    async with pool.acquire() as conn:
        for idx, asset_id in enumerate(market_data['asset_ids']):
            print(f"\n--- Asset ID {idx+1}: {asset_id} ---")
            
            # Quick counts
            ltp_count = await conn.fetchval(
                "SELECT COUNT(*) FROM poly_last_trade_price WHERE asset_id = $1",
                asset_id
            )
            pc_count = await conn.fetchval(
                "SELECT COUNT(*) FROM poly_price_change WHERE asset_id = $1",
                asset_id
            )
            book_count = await conn.fetchval(
                "SELECT COUNT(*) FROM poly_book WHERE asset_id = $1",
                asset_id
            )
            
            print(f"  LTP: {ltp_count}, PC: {pc_count}, Books: {book_count}")
            
            # Diagnostic: Check if message asset_id matches column asset_id
            pc_mismatch = await conn.fetchval(
                "SELECT COUNT(*) FROM poly_price_change WHERE asset_id = $1 AND message->>'asset_id' != $1",
                asset_id
            )
            ltp_mismatch = await conn.fetchval(
                "SELECT COUNT(*) FROM poly_last_trade_price WHERE asset_id = $1 AND message->>'asset_id' != $1",
                asset_id
            )
            book_mismatch = await conn.fetchval(
                "SELECT COUNT(*) FROM poly_book WHERE asset_id = $1 AND message->>'asset_id' != $1",
                asset_id
            )
            
            if pc_mismatch or ltp_mismatch or book_mismatch:
                print(f"  ⚠️  ASSET_ID MISMATCH DETECTED:")
                if pc_mismatch:
                    print(f"    PC: {pc_mismatch} rows have column asset_id != message asset_id")
                if ltp_mismatch:
                    print(f"    LTP: {ltp_mismatch} rows have column asset_id != message asset_id")
                if book_mismatch:
                    print(f"    Book: {book_mismatch} rows have column asset_id != message asset_id")
            else:
                print(f"  ✓ All asset_ids match (column == message)")
            
            # Last Trade Prices
            if ltp_count > 0:
                print(f"\nLast Trade Prices:")
                ltp_stats = await count_duplicates_last_trade_price(conn, asset_id)
                print(f"  Total Rows: {ltp_stats['total_rows']}")
                print(f"  Ambiguous tuples (same server_time_us, price, side): {ltp_stats['ambiguous_tuples']}")
                print(f"  Total rows in ambiguous tuples: {ltp_stats['total_ambiguous_rows']}")
                print(f"  Fully duplicate tx_hashes: {ltp_stats['fully_duplicate_tx_hashes']}")
                print(f"  Total fully duplicate rows: {ltp_stats['total_fully_duplicate_rows']}")
            
            # Price Changes
            if pc_count > 0:
                print(f"\nPrice Changes:")
                pc_stats = await count_duplicates_price_change(conn, asset_id)
                print(f"  Total Rows: {pc_stats['total_rows']}")
                print(f"  Ambiguous tuples (same server_time_us, price, side): {pc_stats['ambiguous_tuples']}")
                print(f"  Total rows in ambiguous tuples: {pc_stats['total_ambiguous_rows']}")
                print(f"  Fully duplicate tuples (same size too): {pc_stats['fully_duplicate_tuples']}")
                print(f"  Total fully duplicate rows: {pc_stats['total_fully_duplicate_rows']}")
            
            # Books
            if book_count > 0:
                print(f"\nBooks:")
                book_stats = await count_duplicates_book(conn, asset_id)
                print(f"  Total Rows: {book_stats['total_rows']}")
                print(f"  Ambiguous timestamps (same server_time_us): {book_stats['ambiguous_times']}")
                print(f"  Total rows at ambiguous timestamps: {book_stats['total_ambiguous_rows']}")
                print(f"  Fully duplicate tuples (same bids/asks): {book_stats['fully_duplicate_tuples']}")
                print(f"  Total fully duplicate rows: {book_stats['total_fully_duplicate_rows']}")
            
            # Combined Book + Price Change Analysis
            if pc_count > 0 and book_count > 0:
                print(f"\nCombined Books + Price Changes:")
                combined_stats = await count_combined_book_price_change(conn, asset_id)
                print(f"  Total Combined Rows: {combined_stats['total_combined_rows']}")
                print(f"  Ambiguous timestamps (shared): {combined_stats['ambiguous_timestamps']}")
                print(f"  Total rows at ambiguous timestamps: {combined_stats['total_ambiguous_combined_rows']}")


async def main():
    pool = await connect_db()
    try:
        # Get UTC time from user
        utc_time_str = input("Enter UTC time (YYYY-MM-DD HH:MM:SS): ").strip()
        
        # Get market data
        market_data = await get_polymarket_market(pool, utc_time_str)
        
        if not market_data:
            print("\n❌ Could not find market data")
            return
        
        # Analyze the market data
        await analyze_market_data(pool, market_data)
        
        print("\n=== ANALYSIS COMPLETE ===\n")
        
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
