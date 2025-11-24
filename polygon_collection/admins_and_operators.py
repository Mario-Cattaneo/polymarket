import os
import time
import json
import csv
import requests
from web3 import Web3
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1. CONFIGURATION
# ----------------
INFURA_KEY = os.getenv('INFURA_KEY')
RPC_URL = f"https://polygon-mainnet.infura.io/v3/{INFURA_KEY}"

# Contract: CTF Exchange
CONTRACT_ADDRESS = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
DEPLOYER_ADDRESS = "0x81fd0E5E7372ED171f421A7C33a4b263Ea9DCc25"

# Deployment Block
START_BLOCK = 15949236 
OUTPUT_CSV = "admins_and_operators.csv"

# 2. SETUP
# --------
w3 = Web3()
TARGET_ADDR = CONTRACT_ADDRESS.lower()

def ensure_prefix(hex_str):
    """Guarantees 0x prefix for Infura compatibility"""
    if not hex_str.startswith('0x'): return '0x' + hex_str
    return hex_str

# Define Event Signatures
EVENTS = {
    "NewAdmin": ensure_prefix(w3.keccak(text="NewAdmin(address,address)").hex()),
    "RemovedAdmin": ensure_prefix(w3.keccak(text="RemovedAdmin(address,address)").hex()),
    "NewOperator": ensure_prefix(w3.keccak(text="NewOperator(address,address)").hex()),
    "RemovedOperator": ensure_prefix(w3.keccak(text="RemovedOperator(address,address)").hex()),
    "ProxyFactoryUpdated": ensure_prefix(w3.keccak(text="ProxyFactoryUpdated(address,address)").hex()),
    "SafeFactoryUpdated": ensure_prefix(w3.keccak(text="SafeFactoryUpdated(address,address)").hex())
}

TOPIC_LIST = list(EVENTS.values())
TOPIC_TO_NAME = {v: k for k, v in EVENTS.items()}

# 3. FETCHING LOGIC (Raw RPC)
# ---------------------------
def fetch_logs_chunk(args):
    start, end = args
    payload = {
        "jsonrpc": "2.0", "method": "eth_getLogs", "id": 1,
        "params": [{
            "fromBlock": hex(start), 
            "toBlock": hex(end),
            "address": [TARGET_ADDR],
            "topics": [TOPIC_LIST] # Filter for ANY of the admin events
        }]
    }
    
    retries = 0
    while retries < 5:
        try:
            response = requests.post(RPC_URL, json=payload, timeout=20)
            data = response.json()
            
            if 'error' in data:
                # If limit exceeded, signal to split
                if "-32005" in str(data['error']):
                    return "SPLIT"
                # If rate limit, wait
                if "429" in str(data['error']):
                    time.sleep(2 * (retries + 1))
                    retries += 1
                    continue
                return []
            
            return data.get('result', [])
        except:
            time.sleep(1)
            retries += 1
    return []

def fetch_recursive(start, end):
    """Handles splitting chunks if they are too large"""
    result = fetch_logs_chunk((start, end))
    if result == "SPLIT":
        mid = (start + end) // 2
        if mid == start: return []
        return fetch_recursive(start, mid) + fetch_recursive(mid + 1, end)
    return result

def decode_address(topic_str):
    if not topic_str: return "None"
    clean = topic_str.replace("0x", "")
    return w3.to_checksum_address("0x" + clean[-40:])

# 4. MAIN EXECUTION
# -----------------
def main():
    # Get Latest Block
    try:
        latest_payload = {"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}
        latest_block = int(requests.post(RPC_URL, json=latest_payload).json()['result'], 16)
    except:
        latest_block = 65000000

    print(f"🚀 Generating Admin CSV: {START_BLOCK} -> {latest_block}")
    
    # Use large chunks (5M) because these events are rare
    CHUNK_SIZE = 5000000 
    chunks = []
    curr = START_BLOCK
    while curr <= latest_block:
        end = min(curr + CHUNK_SIZE, latest_block)
        chunks.append((curr, end))
        curr = end + 1

    all_logs = []
    
    print(f"Fetching logs in {len(chunks)} chunks...")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_chunk = {executor.submit(fetch_recursive, c[0], c[1]): c for c in chunks}
        
        for future in tqdm(as_completed(future_to_chunk), total=len(chunks), desc="Scanning"):
            all_logs.extend(future.result())

    print(f"✅ Found {len(all_logs)} events. Processing...")

    # Sort Chronologically (Block -> Log Index)
    all_logs.sort(key=lambda x: (int(x['blockNumber'], 16), int(x['logIndex'], 16)))

    # --- STATE RECONSTRUCTION ---
    # Initial State: Deployer is the only Admin
    admins = {w3.to_checksum_address(DEPLOYER_ADDRESS)}
    operators = set()
    
    print(f"Writing to {OUTPUT_CSV}...")
    
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['block_number', 'event', 'target', 'actor', 'admins_snapshot', 'operators_snapshot']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Write Initial State Row
        writer.writerow({
            'block_number': START_BLOCK,
            'event': 'INITIALIZATION',
            'target': DEPLOYER_ADDRESS,
            'actor': 'System',
            'admins_snapshot': json.dumps(list(admins)),
            'operators_snapshot': json.dumps(list(operators))
        })

        for log in all_logs:
            blk = int(log['blockNumber'], 16)
            
            # Identify Event
            topic0 = ensure_prefix(log['topics'][0] if isinstance(log['topics'][0], str) else log['topics'][0].hex())
            event_name = TOPIC_TO_NAME.get(topic0, "Unknown")
            
            # Decode Params
            target = decode_address(log['topics'][1]) if len(log['topics']) > 1 else "None"
            actor = decode_address(log['topics'][2]) if len(log['topics']) > 2 else "None"
            
            # Update State
            if event_name == "NewAdmin":
                admins.add(target)
            elif event_name == "RemovedAdmin":
                if target in admins: admins.remove(target)
            elif event_name == "NewOperator":
                operators.add(target)
            elif event_name == "RemovedOperator":
                if target in operators: operators.remove(target)
            
            # Write Row
            # We save the snapshot of admins/operators AFTER the change
            writer.writerow({
                'block_number': blk,
                'event': event_name,
                'target': target,
                'actor': actor,
                'admins_snapshot': json.dumps(sorted(list(admins))),
                'operators_snapshot': json.dumps(sorted(list(operators)))
            })

    print("✅ CSV Generation Complete.")
    print(f"Final Admin Count: {len(admins)}")
    print(f"Final Operator Count: {len(operators)}")

if __name__ == "__main__":
    main()