import os
import time
import csv
import requests
from datetime import datetime, timezone
from web3 import Web3
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1. CONFIGURATION
# ----------------
RPC_URL = os.getenv('QUICK_NODE_URL')
if not RPC_URL:
    print("❌ Error: QUICK_NODE_URL environment variable not set.")
    exit()

print(f"🔌 Using QuickNode: {RPC_URL[:15]}...{RPC_URL[-5:]}")

START_BLOCK = 79172085
OUTPUT_FILE = "raw_polymarket_events.csv"

# QUICKNODE FREE TIER LIMITS
# They strictly limit range to 5 blocks.
BATCH_SIZE = 5 
MAX_WORKERS = 10 # Parallel workers to speed up the tiny batches

# Contracts
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
CONDITIONAL_TOKENS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"

# 2. SETUP
# --------
w3 = Web3()

def ensure_prefix(hex_str):
    if not hex_str: return None
    if not hex_str.startswith('0x'): return '0x' + hex_str
    return hex_str

# Topics
TOPIC_TOKEN_REG = ensure_prefix(w3.keccak(text="TokenRegistered(uint256,uint256,bytes32)").hex())
TOPIC_COND_PREP = ensure_prefix(w3.keccak(text="ConditionPreparation(bytes32,address,bytes32,uint256)").hex())
TOPIC_COND_RES = ensure_prefix(w3.keccak(text="ConditionResolution(bytes32,address,bytes32,uint256,uint256[])").hex())

EVENT_MAP = {
    TOPIC_TOKEN_REG: "TokenRegistered",
    TOPIC_COND_PREP: "ConditionPreparation",
    TOPIC_COND_RES: "ConditionResolution"
}

CONTRACT_MAP = {
    CTF_EXCHANGE.lower(): "CTF Exchange",
    NEG_RISK.lower(): "NegRisk Adapter",
    CONDITIONAL_TOKENS.lower(): "Conditional Tokens"
}

ADDRESS_LIST = [CTF_EXCHANGE.lower(), NEG_RISK.lower(), CONDITIONAL_TOKENS.lower()]
TOPIC_LIST = [[TOPIC_TOKEN_REG, TOPIC_COND_PREP, TOPIC_COND_RES]]

# 3. FETCH LOGIC
# --------------
def fetch_chunk(args):
    start, end = args
    payload = {
        "jsonrpc": "2.0", "method": "eth_getLogs", "id": 1,
        "params": [{
            "fromBlock": hex(start), 
            "toBlock": hex(end),
            "address": ADDRESS_LIST, 
            "topics": TOPIC_LIST
        }]
    }
    
    retries = 0
    while retries < 5:
        try:
            response = requests.post(RPC_URL, json=payload, timeout=10)
            
            # Handle Rate Limits (429)
            if response.status_code == 429:
                time.sleep(2 * (retries + 1))
                retries += 1
                continue
                
            data = response.json()
            
            if 'error' in data:
                # If we somehow still hit a limit, wait and retry
                time.sleep(1)
                retries += 1
                continue
            
            return data.get('result', [])
        except:
            time.sleep(1)
            retries += 1
            
    return []

# 4. MAIN EXECUTION
# -----------------
def main():
    # Get Boundaries
    print("⏳ Fetching time boundaries...")
    try:
        # Start Time
        p1 = {"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[hex(START_BLOCK), False],"id":1}
        r1 = requests.post(RPC_URL, json=p1).json()
        start_ts = int(r1['result']['timestamp'], 16)
        
        # End Time
        p2 = {"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}
        r2 = requests.post(RPC_URL, json=p2).json()
        latest_block = int(r2['result'], 16)
        
        p3 = {"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[hex(latest_block), False],"id":1}
        r3 = requests.post(RPC_URL, json=p3).json()
        end_ts = int(r3['result']['timestamp'], 16)
    except Exception as e:
        print(f"❌ Failed to fetch boundaries: {e}")
        return

    print(f"   Start: {datetime.fromtimestamp(start_ts, tz=timezone.utc)}")
    print(f"   End:   {datetime.fromtimestamp(end_ts, tz=timezone.utc)}")

    # Calculate Slope for Interpolation
    total_blocks = latest_block - START_BLOCK
    total_time = end_ts - start_ts
    avg_block_time = total_time / total_blocks
    print(f"   Avg Block Time: {avg_block_time:.4f}s")

    # Prepare Chunks (Size 5)
    chunks = []
    curr = START_BLOCK
    while curr <= latest_block:
        end = min(curr + BATCH_SIZE, latest_block)
        chunks.append((curr, end))
        curr = end + 1
        
    print(f"📦 Prepared {len(chunks)} micro-batches (Size: {BATCH_SIZE})")

    print(f"💾 Writing to {OUTPUT_FILE}...")
    
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'block_number', 'block_timestamp_utc', 'timestamp_unix',
            'transaction_hash', 'log_index', 'contract_name',
            'contract_address', 'event_name', 'topic0', 
            'topic1', 'topic2', 'topic3', 'data'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Parallel Execution
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_chunk = {executor.submit(fetch_chunk, chunk): chunk for chunk in chunks}
            
            for future in tqdm(as_completed(future_to_chunk), total=len(chunks), desc="Scanning"):
                logs = future.result()
                
                if logs:
                    rows = []
                    for log in logs:
                        blk = int(log['blockNumber'], 16)
                        
                        # Timestamp Logic
                        ts_hex = log.get('blockTimestamp')
                        if ts_hex:
                            ts_unix = int(ts_hex, 16)
                        else:
                            # Interpolation Fallback
                            block_diff = blk - START_BLOCK
                            ts_unix = int(start_ts + (block_diff * avg_block_time))
                        
                        ts_utc = datetime.fromtimestamp(ts_unix, tz=timezone.utc).isoformat()
                        
                        addr = log['address'].lower()
                        topic0 = ensure_prefix(log['topics'][0] if isinstance(log['topics'][0], str) else log['topics'][0].hex())
                        topics = log['topics']
                        
                        rows.append({
                            'block_number': blk,
                            'block_timestamp_utc': ts_utc,
                            'timestamp_unix': ts_unix,
                            'transaction_hash': log['transactionHash'],
                            'log_index': int(log['logIndex'], 16),
                            'contract_name': CONTRACT_MAP.get(addr, "Unknown"),
                            'contract_address': addr,
                            'event_name': EVENT_MAP.get(topic0, "Unknown"),
                            'topic0': topic0,
                            'topic1': topics[1] if len(topics) > 1 else "",
                            'topic2': topics[2] if len(topics) > 2 else "",
                            'topic3': topics[3] if len(topics) > 3 else "",
                            'data': log['data']
                        })
                    writer.writerows(rows)

    print(f"\n✅ Done. Data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()