import asyncio
import asyncpg
import json
import os
from datetime import datetime, timedelta

POLY_DEM_ASSET_ID = "83247781037352156539108067944461291821683755894607244160607042790356561625563"
POLY_REP_ASSET_ID = "65139230827417363158752884968303867495725894165574887635816574090175320800482"

PG_HOST = os.getenv("PG_SOCKET", "localhost")
PG_PORT = os.getenv("POLY_PG_PORT", 5432)
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")
DB_NAME = os.getenv("POLY_DB")

async def fetch_raw_messages(pool, asset_id, num_samples=5):
    query = """
        SELECT asset_id, message, server_time_us 
        FROM poly_book_state_house
        WHERE asset_id = $1
        ORDER BY server_time_us ASC
        LIMIT $2
    """
    
    records = await pool.fetch(query, asset_id, num_samples)
    
    print(f"\n{'='*80}")
    print(f"RAW MESSAGES FOR asset_id: {asset_id[:20]}...")
    print(f"{'='*80}")
    
    for i, record in enumerate(records, 1):
        msg = record['message']
        db_asset_id = record['asset_id']
        print(f"\n--- Message {i} ---")
        print(f"Timestamp: {record['server_time_us']}")
        
        # Parse JSON if it's a string
        if isinstance(msg, str):
            msg = json.loads(msg)
        
        msg_asset_id = msg.get('asset_id', 'N/A')
        print(f"DB Asset ID:      {db_asset_id}")
        print(f"Message Asset ID: {msg_asset_id}")
        if str(db_asset_id) != str(msg_asset_id):
            print(f"⚠️  MISMATCH!")
        
        # Extract bids and asks
        bids = msg.get('bids', [])
        asks = msg.get('asks', [])
        
        bid_prices = [float(b['price']) for b in bids if 'price' in b]
        ask_prices = [float(a['price']) for a in asks if 'price' in a]
        
        max_bid = max(bid_prices) if bid_prices else None
        min_ask = min(ask_prices) if ask_prices else None
        
        print(f"Bids: {bid_prices[:5]}{'...' if len(bid_prices) > 5 else ''}")
        print(f"Max Bid: {max_bid}")
        print(f"Asks: {ask_prices[:5]}{'...' if len(ask_prices) > 5 else ''}")
        print(f"Min Ask: {min_ask}")
        print(f"Last Trade Price: {msg.get('last_trade_price', 'N/A')}")

async def main():
    pool = await asyncpg.create_pool(
        host=PG_HOST,
        port=PG_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        min_size=1,
        max_size=5,
        ssl=True,
    )

    try:
        await fetch_raw_messages(pool, POLY_DEM_ASSET_ID, 5)
        await fetch_raw_messages(pool, POLY_REP_ASSET_ID, 5)
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
