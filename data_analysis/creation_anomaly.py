import os
import asyncio
import asyncpg
import aiohttp
from datetime import datetime, timezone
from web3 import Web3 # Used for hex conversion utility

# --- CONFIGURATION ---
# DB Config
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# Infura Config (for block number lookup)
INFURA_KEY = os.getenv('INFURA_KEY_2') # Assuming INFURA_KEY_2 is used for RPC
if not INFURA_KEY:
    # Use a placeholder URL if key is missing, but the script will fail if RPC is needed
    RPC_URL = "http://placeholder.url"
else:
    RPC_URL = f"https://polygon-mainnet.infura.io/v3/{INFURA_KEY}"

# Contract Addresses (from LogHarvester)
_CTF_EXCHANGE = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
_NEG_RISK_CTF = "0xc5d563a36ae78145c45a50134d48a1215220f80a"

# Time Range (11-25 18:00 to 11-26 00:00 UTC)
START_TIME_STR = "2025-11-25 18:00:00"
END_TIME_STR = "2025-11-26 00:00:00"

# --- HELPER FUNCTIONS ---

def hex_to_decimal_string(hex_str):
    """Converts a 32-byte hex string (0x...) to its decimal string representation."""
    if not hex_str or not hex_str.startswith('0x'):
        return None
    return str(int(hex_str, 16))

def parse_token_registered_topics(topics):
    """
    Parses the topics array for a TokenRegistered event and converts tokens
    to decimal strings for comparison with markets_2.asset_id.
    """
    if len(topics) < 4:
        return None, None, None
    
    token_0_dec = hex_to_decimal_string(topics[1])
    token_1_dec = hex_to_decimal_string(topics[2])
    condition_id_hex = topics[3]
    
    return token_0_dec, token_1_dec, condition_id_hex

async def get_block_number_by_timestamp(conn, timestamp_dt):
    """
    Queries the DB to find the block number whose interpolated_time is closest 
    to the target timestamp.
    """
    
    # Convert datetime to Unix timestamp (BIGINT)
    timestamp_unix = int(timestamp_dt.timestamp())
    
    # 1. Try to use the accurate interpolated_time column
    query_interpolated = """
        SELECT block_number
        FROM polygon_events
        WHERE interpolated_time IS NOT NULL AND interpolated_time >= $1
        ORDER BY interpolated_time ASC
        LIMIT 1
    """
    
    block_nr = await conn.fetchval(query_interpolated, timestamp_unix)
    
    if block_nr:
        return block_nr
    
    # 2. Fallback to the less accurate created_at column
    query_created_at = """
        SELECT block_number
        FROM polygon_events
        WHERE created_at >= to_timestamp($1)
        ORDER BY created_at ASC
        LIMIT 1
    """
    
    block_nr = await conn.fetchval(query_created_at, timestamp_unix)
    
    if block_nr:
        return block_nr
    
    # 3. Final Fallback: Return the max block number if the time is in the future
    return await conn.fetchval("SELECT MAX(block_number) FROM polygon_events")


# --- MAIN LOGIC ---

async def analyze_token_matches():
    conn = await asyncpg.connect(
        user=DB_USER, password=DB_PASS,
        database=DB_NAME, host=PG_HOST, port=PG_PORT
    )
    
    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        
        start_dt = datetime.strptime(START_TIME_STR, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(END_TIME_STR, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

        print(f"🔍 Analyzing TokenRegistered events from {start_dt.isoformat()} to {end_dt.isoformat()}")

        # 1. Find Block Numbers for the Time Range
        start_block = await get_block_number_by_timestamp(conn, start_dt)
        end_block = await get_block_number_by_timestamp(conn, end_dt)
        
        if not start_block or not end_block:
            print("❌ Could not determine block range. Aborting.")
            await conn.close()
            return

        # Ensure end_block is greater than start_block (or equal)
        if end_block < start_block:
            print(f"⚠️ Warning: End block ({end_block}) is before start block ({start_block}). Using max block for end.")
            end_block = await conn.fetchval("SELECT MAX(block_number) FROM polygon_events")
            
        print(f"--- Block Range: {start_block} to {end_block} ---")

        # 2. Fetch TokenRegistered Events in Block Range
        # This query is now fast because it uses the indexed block_number column
        query_events = f"""
            SELECT contract_address, topics, transaction_hash, log_index
            FROM polygon_events
            WHERE event_name = 'TokenRegistered'
              AND contract_address = ANY($1::text[])
              AND block_number >= $2
              AND block_number <= $3
            ORDER BY block_number ASC
        """
        
        target_addresses = [_CTF_EXCHANGE, _NEG_RISK_CTF]
        event_rows = await conn.fetch(query_events, target_addresses, start_block, end_block)
        
        # Initial Counts
        total_events = len(event_rows)
        neg_risk_count = sum(1 for r in event_rows if r['contract_address'] == _NEG_RISK_CTF)
        ctf_exchange_count = sum(1 for r in event_rows if r['contract_address'] == _CTF_EXCHANGE)
        
        print(f"\n--- Initial Event Counts ({total_events} total) ---")
        print(f"Neg Risk CTF ({_NEG_RISK_CTF}): {neg_risk_count}")
        print(f"CTF Exchange ({_CTF_EXCHANGE}): {ctf_exchange_count}")
        print("--------------------------------------------------")

        # 3. Fetch all relevant markets data from markets_2
        query_markets = """
            SELECT condition_id, asset_id
            FROM markets_2
            WHERE condition_id IS NOT NULL AND asset_id IS NOT NULL
        """
        market_rows = await conn.fetch(query_markets)
        
        # Create a dictionary for fast lookup: condition_id (Hex) -> set of asset_ids (Decimal String)
        market_lookup = {}
        for r in market_rows:
            cond_id = r['condition_id'].lower()
            asset_id = r['asset_id']
            if cond_id not in market_lookup:
                market_lookup[cond_id] = set()
            market_lookup[cond_id].add(asset_id)

        # 4. Perform Matching and Categorization (Logic Unchanged)
        
        two_market_rows_match = {0: [], 1: [], 2: []}
        one_market_row_match = []
        unmatched_condition_id = []
        
        for event in event_rows:
            token_0_dec, token_1_dec, condition_id_event_hex = parse_token_registered_topics(event['topics'])
            
            if not condition_id_event_hex or not token_0_dec or not token_1_dec:
                unmatched_condition_id.append(event)
                continue
                
            condition_id_event_lower = condition_id_event_hex.lower()
            
            if condition_id_event_lower in market_lookup:
                market_asset_ids = market_lookup[condition_id_event_lower]
                market_rows_count = len(market_asset_ids)
                event_tokens_dec = {token_0_dec, token_1_dec}
                
                if market_rows_count == 2:
                    match_count = 0
                    for token_dec in event_tokens_dec:
                        if token_dec in market_asset_ids:
                            match_count += 1
                    two_market_rows_match[match_count].append(event)
                    
                elif market_rows_count == 1:
                    market_asset_id = list(market_asset_ids)[0]
                    if market_asset_id in event_tokens_dec:
                        one_market_row_match.append(event)
                    else:
                        unmatched_condition_id.append(event)
                
                else:
                    unmatched_condition_id.append(event)
            
            else:
                unmatched_condition_id.append(event)

        # 5. Print Results (Logic Unchanged)
        
        print("\n--- Matching Analysis Results ---")
        print(f"Total TokenRegistered Events Analyzed: {total_events}")
        print("-----------------------------------")
        
        total_two_match = sum(len(v) for v in two_market_rows_match.values())
        print(f"Events with 2 Market Rows (by condition_id): {total_two_match}")
        print(f"  - 2/2 Token/Asset ID Matches: {len(two_market_rows_match[2])}")
        print(f"  - 1/2 Token/Asset ID Matches: {len(two_market_rows_match[1])}")
        print(f"  - 0/2 Token/Asset ID Matches: {len(two_market_rows_match[0])}")
        
        print(f"\nEvents with 1 Market Row (by condition_id) AND Token Match: {len(one_market_row_match)}")
        
        print(f"\nEvents with Unmatched Condition ID (or other mismatch): {len(unmatched_condition_id)}")
        
        total_matched_sum = total_two_match + len(one_market_row_match) + len(unmatched_condition_id)
        print(f"\nVerification Sum: {total_matched_sum} (Should equal {total_events})")
        
        await conn.close()

if __name__ == "__main__":
    if not all([PG_HOST, PG_PORT, DB_NAME, DB_USER, DB_PASS]):
        print("Error: Required database environment variables not fully set.")
        exit(1)
        
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(analyze_token_matches())