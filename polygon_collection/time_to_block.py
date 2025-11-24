from web3 import Web3
# UPDATED IMPORT for Web3.py v7+
from web3.middleware import ExtraDataToPOAMiddleware 
import os
from datetime import datetime, timezone

# Note: Changed inner quotes to single quotes to avoid syntax errors
w3 = Web3(Web3.HTTPProvider(f"https://polygon-mainnet.infura.io/v3/{os.getenv('INFURA_KEY')}"))

# UPDATED INJECTION for Web3.py v7+
w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

def get_block_by_timestamp(target_timestamp):
    """
    Finds the block number closest to the given timestamp using binary search.
    """
    print("Fetching latest block...")
    # Get current bounds
    latest_block = w3.eth.get_block('latest')
    high = latest_block['number']
    low = 0
    
    print(f"Latest Block: {high}. Starting binary search...")

    # Binary Search
    while low <= high:
        mid = (low + high) // 2
        # Get the block info (we only need the timestamp)
        mid_block = w3.eth.get_block(mid)
        mid_timestamp = mid_block['timestamp']
        
        if mid_timestamp < target_timestamp:
            low = mid + 1
        elif mid_timestamp > target_timestamp:
            high = mid - 1
        else:
            return mid # Exact match found
            
    # If no exact match, 'low' is the closest block *after* the timestamp
    return low

# Define target time
start_dt = datetime(2025, 11, 18, 7, 0, 0, tzinfo=timezone.utc)
start_ts = int(start_dt.timestamp())

print(f"Target Timestamp: {start_ts}")

# Find the block
start_block = get_block_by_timestamp(start_ts)

print(f"Start Block: {start_block}")