#!/usr/bin/env python3
"""
Analyze trading volumes from all settlements.
Similar to volume_analysis.py but uses the complete 'settlements' table.
"""

import os
import asyncio
import asyncpg
import logging
import json
import pandas as pd
import matplotlib.pyplot as plt
from decimal import Decimal, getcontext
from eth_abi import decode

# ----------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
getcontext().prec = 36

# --- Database Credentials ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- Timestamp Interpolation Constants ---
REF_BLOCK_NUMBER = 81372608
REF_UNIX_TIMESTAMP = 1767864474
SECONDS_PER_BLOCK = 2
# --- Block Range Configuration ---
START_BLOCK_NR = None  # None = use database minimum
END_BLOCK_NR = 79172085 + 50000  # None = use database maximum
# --- Constants ---
USDC_SCALAR = Decimal("1_000_000")
SHARE_SCALAR = Decimal("1_000_000")

# ----------------------------------------------------------------
# HELPER & DATA PROCESSING FUNCTIONS
# ----------------------------------------------------------------

def block_to_timestamp(block_number: int) -> int:
    return ((block_number - REF_BLOCK_NUMBER) * SECONDS_PER_BLOCK) + REF_UNIX_TIMESTAMP

async def get_block_bounds(conn) -> tuple:
    """Get min and max block numbers from settlements table."""
    row = await conn.fetchrow("SELECT MIN(block_number) as min_b, MAX(block_number) as max_b FROM settlements")
    return row['min_b'], row['max_b']

async def fetch_batch(conn, start_block: int, end_block: int) -> list:
    """Fetch settlements in a block range."""
    return await conn.fetch(
        "SELECT block_number, transaction_hash, type, trade FROM settlements WHERE block_number >= $1 AND block_number < $2 ORDER BY block_number ASC",
        start_block, end_block
    )

def decode_order_filled(data: str, topics: list) -> dict:
    """Decode OrderFilled event."""
    try:
        data_hex = bytes.fromhex(data[2:] if data.startswith('0x') else data)
        decoded = decode(['uint256', 'uint256', 'uint256', 'uint256', 'uint256'], data_hex)
        
        maker = '0x' + topics[2][-40:] if len(topics) > 2 else None
        taker = '0x' + topics[3][-40:] if len(topics) > 3 else None
        maker_asset_id = int(decoded[0])
        taker_asset_id = int(decoded[1])
        making = int(decoded[2])
        taking = int(decoded[3])
        fee = int(decoded[4])
        
        return {
            'maker': maker,
            'taker': taker,
            'makerAssetId': maker_asset_id,
            'takerAssetId': taker_asset_id,
            'making': making,
            'taking': taking,
            'fee': fee
        }
    except Exception as e:
        logger.warning(f"Failed to decode OrderFilled: {e}")
        return {}

async def process_batch_volumes(settlements: list) -> dict:
    """
    Process a batch of settlements to extract volume data.
    Returns dict with cumulative volumes by type and timestamps.
    """
    EXCHANGE_ADDRESS = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
    
    result = {
        'timestamps': [],
        'split': [],
        'merge': [],
        'complementary': [],
        'count': 0,
        'volumes': {'SPLIT': Decimal(0), 'MERGE': Decimal(0), 'COMPLEMENTARY': Decimal(0)}
    }
    
    for row in settlements:
        try:
            trade = json.loads(row['trade'])
            
            if not trade.get('orders_filled') or len(trade.get('orders_filled', [])) < 2:
                continue
            
            # Decode all OrderFilled events
            all_fills = []
            exchange_fill = None
            for idx, fill in enumerate(trade.get('orders_filled', [])):
                decoded_fill = decode_order_filled(fill['data'], fill['topics'])
                if decoded_fill:
                    is_exchange_fill = (decoded_fill['taker'].lower() == EXCHANGE_ADDRESS.lower())
                    decoded_fill['is_exchange'] = is_exchange_fill
                    all_fills.append(decoded_fill)
                    
                    if is_exchange_fill:
                        exchange_fill = decoded_fill
            
            if not exchange_fill:
                continue
            
            # Determine taker direction
            if exchange_fill['makerAssetId'] == 0:
                taker_is_buying = True
            else:
                taker_is_buying = False
            
            # Log exchange fill info
            exchange_making = Decimal(exchange_fill['making'])
            exchange_taking = Decimal(exchange_fill['taking'])
            exchange_usdc_volume = min(exchange_making, exchange_taking) / USDC_SCALAR
            
            # Process maker fills
            maker_fills = [f for f in all_fills if not f['is_exchange']]
            
            if len(maker_fills) < 1:
                continue
            
            # Categorize maker fills and calculate volumes
            for fill in maker_fills:
                maker_is_buying = (fill['makerAssetId'] == 0)
                
                making = Decimal(fill['making'])
                taking = Decimal(fill['taking'])
                
                # Determine match type
                if maker_is_buying == taker_is_buying:
                    if taker_is_buying:
                        match_type = 'SPLIT'
                    else:
                        match_type = 'MERGE'
                else:
                    match_type = 'COMPLEMENTARY'
                
                # Calculate volume
                if match_type == 'COMPLEMENTARY':
                    volume = min(making, taking) / USDC_SCALAR
                else:
                    volume = max(making, taking) / USDC_SCALAR
                
                result['volumes'][match_type] += volume
            
            timestamp = block_to_timestamp(row['block_number'])
            result['timestamps'].append(timestamp)
            result['split'].append(float(result['volumes']['SPLIT']))
            result['merge'].append(float(result['volumes']['MERGE']))
            result['complementary'].append(float(result['volumes']['COMPLEMENTARY']))
            result['count'] += 1

        except Exception as e:
            continue
    
    return result

def plot_volumes(timestamps: list, split: list, merge: list, complementary: list):
    """Create volume analysis plot."""
    if not timestamps:
        logger.warning("No volume data to plot.")
        return
    
    df = pd.DataFrame({
        'timestamp': pd.to_datetime(timestamps, unit='s'),
        'split': split,
        'merge': merge,
        'complementary': complementary
    })
    
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(16, 9))
    
    ax.plot(df['timestamp'], df['split'], color='steelblue', linewidth=2.5, label='SPLIT')
    ax.plot(df['timestamp'], df['merge'], color='crimson', linewidth=2.5, label='MERGE')
    ax.plot(df['timestamp'], df['complementary'], color='darkgreen', linewidth=2.5, label='COMPLEMENTARY')
    
    total = [s + m + c for s, m, c in zip(df['split'], df['merge'], df['complementary'])]
    ax.plot(df['timestamp'], total, color='black', linewidth=2, linestyle='--', label='Total')
    
    ax.set_xlabel('Timestamp', fontsize=14)
    ax.set_ylabel('Cumulative Trading Volume (USDC)', fontsize=14)
    ax.set_title('Cumulative Trading Volume From All Settlements by Match Type', fontsize=16)
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    filename = 'volume_all_matchtype.png'
    plt.savefig(filename, dpi=150)
    logger.info(f"Plot saved to {filename}")
    plt.close()

# ----------------------------------------------------------------
# MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    conn = None
    try:
        conn = await asyncpg.connect(host=PG_HOST, port=int(PG_PORT), database=DB_NAME, user=DB_USER, password=DB_PASS)
        
        min_blk, max_blk = await get_block_bounds(conn)
        if not min_blk:
            logger.error("No settlement records found.")
            return
        
        # Apply configured block range limits
        start_blk = START_BLOCK_NR if START_BLOCK_NR is not None else min_blk
        end_blk = END_BLOCK_NR if END_BLOCK_NR is not None else max_blk
        
        if start_blk > end_blk:
            logger.error(f"Invalid block range: {start_blk} > {end_blk}")
            return
        
        logger.info(f"Processing settlements from block {start_blk} to {end_blk}")
        
        # Batch processing
        all_timestamps = []
        all_split = []
        all_merge = []
        all_complementary = []
        total_settlements = 0
        cumulative_volumes = {'SPLIT': Decimal(0), 'MERGE': Decimal(0), 'COMPLEMENTARY': Decimal(0)}
        
        BLOCK_BATCH_SIZE = 50_000
        curr_blk = start_blk
        
        while curr_blk < end_blk:
            next_blk = min(curr_blk + BLOCK_BATCH_SIZE, end_blk)
            
            settlements = await fetch_batch(conn, curr_blk, next_blk)
            if settlements:
                batch_result = await process_batch_volumes(settlements)
                
                # Offset volumes for this batch to previous cumulative
                adjusted_split = [float(cumulative_volumes['SPLIT']) + val for val in batch_result['split']]
                adjusted_merge = [float(cumulative_volumes['MERGE']) + val for val in batch_result['merge']]
                adjusted_complementary = [float(cumulative_volumes['COMPLEMENTARY']) + val for val in batch_result['complementary']]
                
                all_timestamps.extend(batch_result['timestamps'])
                all_split.extend(adjusted_split)
                all_merge.extend(adjusted_merge)
                all_complementary.extend(adjusted_complementary)
                
                cumulative_volumes['SPLIT'] += batch_result['volumes']['SPLIT']
                cumulative_volumes['MERGE'] += batch_result['volumes']['MERGE']
                cumulative_volumes['COMPLEMENTARY'] += batch_result['volumes']['COMPLEMENTARY']
                
                total_settlements += batch_result['count']
                
                batch_total = batch_result['volumes']['SPLIT'] + batch_result['volumes']['MERGE'] + batch_result['volumes']['COMPLEMENTARY']
                logger.info(f"  Blocks {curr_blk:,}-{next_blk:,}: {batch_result['count']} settlements, batch volume: ${batch_total:.2f}")
            
            curr_blk = next_blk
        
        logger.info(f"\nAnalyzed {total_settlements} settlements")
        logger.info(f"Cumulative volume - SPLIT: ${cumulative_volumes['SPLIT']:.2f}, MERGE: ${cumulative_volumes['MERGE']:.2f}, COMPLEMENTARY: ${cumulative_volumes['COMPLEMENTARY']:.2f}")
        logger.info(f"   Total: ${sum(cumulative_volumes.values()):.2f}")
        
        # Plot
        plot_volumes(all_timestamps, all_split, all_merge, all_complementary)
        
    finally:
        if conn:
            await conn.close()

if __name__ == "__main__":
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST, PG_PORT]):
        logger.error("One or more required database environment variables are not set.")
    else:
        asyncio.run(main())
