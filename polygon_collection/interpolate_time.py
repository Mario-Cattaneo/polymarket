import os
import asyncio
import asyncpg
import json
import math # Added for rounding
from datetime import datetime, timezone
from web3 import Web3
import aiohttp
from pathlib import Path

# --- CONFIGURATION ---
# DB Config
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# Infura Config (for block timestamps)
INFURA_KEY = os.getenv('INFURA_KEY_2')
if not INFURA_KEY:
    print("❌ Error: INFURA_KEY not set.")
    exit(1)
RPC_URL = f"https://polygon-mainnet.infura.io/v3/{INFURA_KEY}"

# Persistence File
PERSISTENCE_FILE = Path("last_block_data.json")
BATCH_SIZE = 5000

# --- HELPER FUNCTIONS ---

def load_last_block_data():
    """Loads the last processed block number and timestamp (as BIGINT) from a file."""
    if PERSISTENCE_FILE.exists():
        with open(PERSISTENCE_FILE, 'r') as f:
            data = json.load(f)
            # last_timestamp is stored as an integer (Unix seconds)
            return data
    return None

def save_last_block_data(block_nr, timestamp_unix):
    """Saves the latest processed block number and timestamp (as BIGINT) to a file."""
    data = {
        'last_block_nr': block_nr,
        'last_timestamp': timestamp_unix # Stored as integer
    }
    with open(PERSISTENCE_FILE, 'w') as f:
        json.dump(data, f, indent=4)
    
    dt = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc)
    print(f"💾 Persisted new last block: {block_nr} at {dt.isoformat()} ({timestamp_unix})")

async def get_block_time(session, block_num):
    """Fetches the actual timestamp for a given block number from the RPC and returns it as Unix seconds (BIGINT)."""
    payload = {"jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": [hex(block_num), False], "id": 1}
    try:
        async with session.post(RPC_URL, json=payload, timeout=10) as resp:
            data = await resp.json()
            if 'result' in data and data['result']:
                # The result is a hex string of the Unix timestamp in seconds
                ts = int(data['result']['timestamp'], 16)
                return ts # Return as integer Unix seconds
    except Exception as e:
        print(f"❌ Error fetching block {block_num}: {e}")
        return None

# --- MAIN LOGIC ---

async def update_interpolated_times(conn, session):
    """
    Determines the interpolation range, fetches events, calculates interpolated
    time (BIGINT), and updates the database.
    """
    last_data = load_last_block_data()
    
    # 1. Determine Start Point (Block A)
    if last_data:
        start_block_nr = last_data['last_block_nr']
        start_timestamp = last_data['last_timestamp']
        dt = datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
        print(f"▶️ Resuming from persisted block: {start_block_nr} ({dt.isoformat()})")
    else:
        # Initial run: Find the lowest block in the table
        min_query = "SELECT MIN(block_number) as min_blk FROM polygon_events WHERE interpolated_time IS NULL"
        row = await conn.fetchrow(min_query)
        start_block_nr = row['min_blk'] if row and row['min_blk'] else None
        
        if not start_block_nr:
            print("✅ No new events found to process.")
            return
            
        start_timestamp = await get_block_time(session, start_block_nr)
        if not start_timestamp:
            print(f"❌ Could not fetch timestamp for initial block {start_block_nr}. Aborting.")
            return
        dt = datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
        print(f"🆕 Initial run. Starting interpolation from MIN block: {start_block_nr} ({dt.isoformat()})")

    # 2. Determine End Point (Block B)
    max_query = "SELECT MAX(block_number) as max_blk FROM polygon_events"
    row = await conn.fetchrow(max_query)
    end_block_nr = row['max_blk'] if row and row['max_blk'] else None
    
    if not end_block_nr or end_block_nr <= start_block_nr:
        print("✅ No new blocks to process or max block is the same as start block.")
        return

    end_timestamp = await get_block_time(session, end_block_nr)
    if not end_timestamp:
        print(f"❌ Could not fetch timestamp for MAX block {end_block_nr}. Aborting.")
        return
        
    dt = datetime.fromtimestamp(end_timestamp, tz=timezone.utc)
    print(f"🏁 Interpolating up to MAX block: {end_block_nr} ({dt.isoformat()})")

    # 3. Interpolation Parameters
    total_block_diff = end_block_nr - start_block_nr
    total_time_diff_seconds = end_timestamp - start_timestamp
    
    if total_block_diff <= 0:
        print("⚠️ Block difference is zero or negative. Cannot interpolate.")
        return

    # 4. Fetch Events to Update
    fetch_query = f"""
        SELECT transaction_hash, log_index, block_number
        FROM polygon_events
        WHERE interpolated_time IS NULL 
          AND block_number >= $1 
          AND block_number <= $2
        ORDER BY block_number ASC
    """
    events_to_update = await conn.fetch(fetch_query, start_block_nr, end_block_nr)
    
    if not events_to_update:
        print("✅ No new events found between the start and end blocks to update.")
        save_last_block_data(end_block_nr, end_timestamp)
        return

    print(f"🔄 Found {len(events_to_update)} events to interpolate and update.")

    # 5. Perform Interpolation and Batch Update
    update_query = """
        UPDATE polygon_events
        SET interpolated_time = $1
        WHERE transaction_hash = $2 AND log_index = $3
    """
    
    interpolated_updates = []
    for row in events_to_update:
        block_diff = row['block_number'] - start_block_nr
        
        # Linear interpolation calculation
        time_offset_seconds = (block_diff / total_block_diff) * total_time_diff_seconds
        
        # Calculate the final Unix timestamp and round to the nearest integer (BIGINT)
        interpolated_ts = start_timestamp + round(time_offset_seconds)
        
        # Prepare the update tuple: (interpolated_time, transaction_hash, log_index)
        interpolated_updates.append((interpolated_ts, row['transaction_hash'], row['log_index']))

    # Execute updates in batches
    updated_count = 0
    for i in range(0, len(interpolated_updates), BATCH_SIZE):
        batch = interpolated_updates[i:i + BATCH_SIZE]
        # Use executemany for efficient batch updates
        await conn.executemany(update_query, batch)
        updated_count += len(batch)
        print(f"   ... Updated {updated_count}/{len(events_to_update)} events.")

    print(f"✅ Successfully updated {updated_count} events.")
    
    # 6. Persist the new last block data
    save_last_block_data(end_block_nr, end_timestamp)


async def main():
    conn = await asyncpg.connect(
        user=DB_USER, password=DB_PASS,
        database=DB_NAME, host=PG_HOST, port=PG_PORT
    )
    
    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        await update_interpolated_times(conn, session)
        
    await conn.close()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())