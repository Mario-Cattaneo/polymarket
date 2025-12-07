import os
import asyncio
import asyncpg
import json
from datetime import datetime, timezone
import time
from web3 import Web3
import aiohttp
from pathlib import Path
import sys

# --- CONFIGURATION ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")
INFURA_KEY = os.getenv('INFURA_KEY_2')
if not INFURA_KEY:
    print("--- ERROR: INFURA_KEY not set. ---", file=sys.stderr)
    sys.exit(1)
RPC_URL = f"https://polygon-mainnet.infura.io/v3/{INFURA_KEY}"
PERSISTENCE_FILE = Path("last_block_data.json")

# --- TUNING ---
# CRITICAL: Number of blocks to process in a single batch (MINIMAL SIZE)
BLOCK_CHUNK_SIZE = 1000 

# --- HELPER FUNCTIONS (Unchanged) ---
def print_flush(message):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    print(f"{timestamp} {message}")
    sys.stdout.flush()

def load_last_block_data():
    if PERSISTENCE_FILE.exists():
        with open(PERSISTENCE_FILE, 'r') as f:
            data = json.load(f)
            return data.get('next_start_block_nr')
    return None

def save_next_start_block(next_start_block_nr):
    data = {
        'next_start_block_nr': next_start_block_nr,
        'updated_at': datetime.now(timezone.utc).isoformat()
    }
    with open(PERSISTENCE_FILE, 'w') as f:
        json.dump(data, f, indent=4)
    print_flush(f"--- PERSISTENCE: Saved next start block: {next_start_block_nr} ---")

async def get_block_time(session, block_num):
    payload = {"jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": [hex(block_num), False], "id": 1}
    try:
        async with session.post(RPC_URL, json=payload, timeout=10) as resp:
            data = await resp.json()
            if 'result' in data and data['result']:
                ts = int(data['result']['timestamp'], 16)
                return ts
    except Exception as e:
        print_flush(f"--- ERROR: RPC fetch failed for block {block_num}: {e} ---")
        return None

# --- MAIN LOGIC ---

async def update_interpolated_times(conn, session):
    next_start_block_from_file = load_last_block_data()
    
    # 1. Determine Global Interpolation Range (Block A and Block B)
    min_max_query = "SELECT MIN(block_number) as min_blk, MAX(block_number) as max_blk FROM polygon_events"
    row = await conn.fetchrow(min_max_query)
    
    global_min_block_nr = row['min_blk']
    global_max_block_nr = row['max_blk']
    
    if not global_min_block_nr:
        print_flush("--- SUCCESS: No events found in the table. Exiting. ---")
        return

    global_min_timestamp = await get_block_time(session, global_min_block_nr)
    global_max_timestamp = await get_block_time(session, global_max_block_nr)
    
    if not global_min_timestamp or not global_max_timestamp:
        print_flush("--- FATAL ERROR: Could not fetch timestamps for min/max blocks. Aborting. ---")
        return

    total_block_diff = global_max_block_nr - global_min_block_nr
    total_time_diff_seconds = global_max_timestamp - global_min_timestamp
    if total_block_diff <= 0:
        print_flush("--- WARNING: Block difference is zero or negative. Cannot interpolate. ---")
        return
        
    dt_min = datetime.fromtimestamp(global_min_timestamp, tz=timezone.utc)
    dt_max = datetime.fromtimestamp(global_max_timestamp, tz=timezone.utc)
    print_flush(f"--- INFO: Global Range: {global_min_block_nr} ({dt_min.isoformat()}) -> {global_max_block_nr} ({dt_max.isoformat()})")
    print_flush(f"--- INFO: Total Block Diff: {total_block_diff}, Total Time Diff: {total_time_diff_seconds}s")

    # 2. Determine Start Block for Processing
    if next_start_block_from_file and next_start_block_from_file > global_min_block_nr:
        current_chunk_start_block = next_start_block_from_file
        print_flush(f"--- RESUME: Resuming from block: {current_chunk_start_block} ---")
    else:
        current_chunk_start_block = global_min_block_nr
        print_flush(f"--- START: Starting from MIN block: {current_chunk_start_block} ---")

    # 3. Block-Partitioned SQL Update Loop
    
    # The interpolation formula is constructed using the dynamic parameters
    update_query = f"""
        UPDATE public.polygon_events
        SET interpolated_time = {global_min_timestamp} + ROUND(
            (block_number - {global_min_block_nr}) * ({total_time_diff_seconds}::NUMERIC / {total_block_diff}::NUMERIC)
        )
        WHERE block_number >= $1 
          AND block_number <= $2;
    """
    
    while current_chunk_start_block <= global_max_block_nr:
        chunk_end_block = min(current_chunk_start_block + BLOCK_CHUNK_SIZE - 1, global_max_block_nr)
        
        if chunk_end_block < current_chunk_start_block:
            break
            
        start_time = time.time()
        
        print_flush(f"--- CHUNK: Processing {current_chunk_start_block} to {chunk_end_block} ---")
        
        try:
            # CRITICAL: Use an explicit transaction block for the UPDATE
            async with conn.transaction():
                # Execute the chunked update, passing only the chunk boundaries
                status = await conn.execute(
                    update_query, 
                    current_chunk_start_block, # $1: chunk_start_block
                    chunk_end_block            # $2: chunk_end_block
                )
            
            # The transaction is now committed, guaranteeing the update is permanent.
            
            # Extract the number of updated rows in this chunk
            updated_in_chunk = int(status.split()[-1])
            
            elapsed_time = time.time() - start_time
            
            # Calculate block progress percentage
            block_progress_pct = (chunk_end_block - global_min_block_nr) / total_block_diff * 100
            
            print_flush(f"--- BATCH SUCCESS: Updated {updated_in_chunk:,} events in {elapsed_time:.2f}s.")
            print_flush(f"--- PROGRESS: Block {chunk_end_block}/{global_max_block_nr} ({block_progress_pct:.2f}%) ---")
            
            # 4. Persist the progress: Save the start block of the *next* chunk
            next_start_block = chunk_end_block + 1
            save_next_start_block(next_start_block)
            
            current_chunk_start_block = next_start_block
            
        except Exception as e:
            print_flush(f"--- FATAL ERROR: SQL UPDATE failed in chunk {current_chunk_start_block}-{chunk_end_block}: {e} ---")
            print_flush("--- ABORT: Resume will start from the last successfully saved block. ---")
            return

    print_flush(f"\n--- SUCCESS: Interpolation Complete. ---")
    
    # Final persistence update to the block *after* the max block
    save_next_start_block(global_max_block_nr + 1)


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
    
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print_flush("--- INTERRUPTED: Script stopped by user. ---")
    except Exception as e:
        print_flush(f"--- FATAL ERROR: An unhandled error occurred: {e} ---")