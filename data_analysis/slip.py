#!/usr/bin/env python3
"""
Calculate and plot cumulative slippage from all settlements.
Similar to house_slip.py but uses the complete 'settlements' table.
"""

import os
import asyncio
import asyncpg
import logging
import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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
START_BLOCK_NR = 79172085 # None = use database minimum
END_BLOCK_NR = 81225971 # None = use database maximum
BLOCK_BATCH_SIZE = 50_000  # Process in batches of N blocks
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

async def process_batch_slippage(settlements: list) -> tuple:
    """
    Calculate slippage from a batch of settlements.
    Returns (timestamps, cumulative_values, count, cumulative, cumulative_taker_usdc_inflow).
    """
    EXCHANGE_ADDRESS = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
    
    timestamps = []
    cumulative_values = []
    cumulative_taker_usdc_values = []
    current_cumulative = Decimal(0)
    current_taker_usdc = Decimal(0)
    count = 0
    
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
                logger.debug(f"No exchange fill found in {row['transaction_hash']}")
                continue
            
            # Determine taker direction
            if exchange_fill['makerAssetId'] == 0:
                taker_is_buying = True
            else:
                taker_is_buying = False
            
            # Get taker USDC amount and tokens from exchange fill
            exchange_making = Decimal(exchange_fill['making'])
            exchange_taking = Decimal(exchange_fill['taking'])
            
            # taker_usdc_amount = min(making, taking)
            # taker_tokens = max(making, taking)
            taker_usdc_amount = min(exchange_making, exchange_taking) / USDC_SCALAR
            taker_tokens = max(exchange_making, exchange_taking) / SHARE_SCALAR
            
            # Process maker fills
            maker_fills = [f for f in all_fills if not f['is_exchange']]
            
            if len(maker_fills) < 1:
                continue
            
            # Calculate prices for all maker fills
            maker_prices = []
            for fill in maker_fills:
                making_raw = Decimal(fill['making'])
                taking_raw = Decimal(fill['taking'])
                
                # Determine maker side from makerAssetId
                maker_is_buying = (fill['makerAssetId'] == 0)
                
                # price_per_share = min/max
                price_per_share = min(making_raw, taking_raw) / max(making_raw, taking_raw)
                
                # If maker is on same side as taker, flip price
                if maker_is_buying == taker_is_buying:
                    price_per_share = Decimal(1) - price_per_share
                
                maker_prices.append(price_per_share)
            
            # Only compute slippage if we have at least 1 maker fill
            if len(maker_prices) >= 1:
                # best_price = minimum price (buying at min price is best)
                best_price_per_share = min(maker_prices)
                
                # slippage = taker_usdc_amount - best_price_per_share * taker_tokens
                expected_cost = best_price_per_share * taker_tokens
                tx_slippage = taker_usdc_amount - expected_cost
                
                current_cumulative += tx_slippage
                timestamp = block_to_timestamp(row['block_number'])
                
                # Track taker USDC inflow
                current_taker_usdc += taker_usdc_amount
                
                timestamps.append(timestamp)
                cumulative_values.append(float(current_cumulative))
                cumulative_taker_usdc_values.append(float(current_taker_usdc))
                count += 1

        except Exception as e:
            continue
    
    return timestamps, cumulative_values, cumulative_taker_usdc_values, count, current_cumulative, current_taker_usdc

def plot_slippage(slippage_data: dict):
    """Create cumulative slippage plot."""
    if not slippage_data['timestamps'] or not slippage_data['cumulative_slippage']:
        logger.warning("No slippage data to plot.")
        return
    
    import pandas as pd
    
    # Convert timestamps to datetime
    df = pd.DataFrame({
        'timestamp': pd.to_datetime(slippage_data['timestamps'], unit='s'),
        'cumulative_slippage': slippage_data['cumulative_slippage']
    })
    
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(16, 9))
    
    ax.plot(df['timestamp'], df['cumulative_slippage'], 
           color='steelblue', linewidth=2.5, label='All Settlements')
    
    ax.fill_between(df['timestamp'], df['cumulative_slippage'], 
                    alpha=0.3, color='steelblue')
    
    ax.set_xlabel('Timestamp', fontsize=14)
    ax.set_ylabel('Cumulative Slippage (USDC)', fontsize=14)
    ax.set_title('Cumulative Slippage from All Settlements', fontsize=16)
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    filename = 'slip_cumulative.png'
    plt.savefig(filename, dpi=150)
    logger.info(f"✅ Plot saved to {filename}")
    plt.close()

def plot_usdc_to_slippage_ratio(ratio_data: dict):
    """Create cumulative taker USDC inflow divided by cumulative slippage plot."""
    if not ratio_data['timestamps'] or not ratio_data['ratio']:
        logger.warning("No ratio data to plot.")
        return
    
    import pandas as pd
    
    # Convert timestamps to datetime
    df = pd.DataFrame({
        'timestamp': pd.to_datetime(ratio_data['timestamps'], unit='s'),
        'ratio': ratio_data['ratio']
    })
    
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(16, 9))
    
    ax.plot(df['timestamp'], df['ratio'], 
           color='darkgreen', linewidth=2.5, label='Slippage / Taker USDC')
    
    ax.fill_between(df['timestamp'], df['ratio'], 
                    alpha=0.3, color='darkgreen')
    
    ax.set_xlabel('Time (UTC)', fontsize=22, fontweight='bold')
    ax.set_ylabel('Ratio (log scale)', fontsize=22, fontweight='bold')
    ax.set_title('Cumulative Slippage Divided by Cumulative Taker USDC', fontsize=26, fontweight='bold')
    ax.set_yscale('log')
    ax.set_ylim(0.001, 1)
    ax.legend(fontsize=24, loc='upper right', framealpha=0.95)
    ax.grid(True, which='both', linestyle='--', alpha=0.6)
    
    # Format x-axis dates
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate(rotation=30, ha='right')
    
    # Increase tick label font sizes
    ax.tick_params(axis='x', labelsize=20)
    ax.tick_params(axis='y', labelsize=20)
    
    # Add left margin for y-axis label space
    fig.subplots_adjust(left=0.15)
    plt.tight_layout()
    
    filename = 'slip_slippage_to_usdc_ratio.png'
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    logger.info(f"✅ Plot saved to {filename}")
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
        all_cumulative = []
        all_cumulative_taker_usdc = []
        total_settlements = 0
        total_cumulative = Decimal(0)
        total_taker_usdc = Decimal(0)
        
        curr_blk = start_blk
        
        while curr_blk < end_blk:
            next_blk = min(curr_blk + BLOCK_BATCH_SIZE, end_blk)
            
            settlements = await fetch_batch(conn, curr_blk, next_blk)
            if settlements:
                timestamps, cumulative_vals, cumulative_usdc_vals, count, batch_cumulative, batch_usdc = await process_batch_slippage(settlements)
                
                # Offset cumulative values for this batch to previous total
                adjusted_cumulative = [float(total_cumulative) + val for val in cumulative_vals]
                adjusted_cumulative_usdc = [float(total_taker_usdc) + val for val in cumulative_usdc_vals]
                
                all_timestamps.extend(timestamps)
                all_cumulative.extend(adjusted_cumulative)
                all_cumulative_taker_usdc.extend(adjusted_cumulative_usdc)
                total_settlements += count
                total_cumulative += batch_cumulative
                total_taker_usdc += batch_usdc
                
                logger.info(f"  Blocks {curr_blk:,}-{next_blk:,}: {count} settlements, batch cumulative: ${batch_cumulative:.4f}")
            
            curr_blk = next_blk
        
        logger.info(f"\nProcessed {total_settlements} settlements with valid slippage data.")
        logger.info(f"Total cumulative slippage: ${total_cumulative:.4f}")
        logger.info(f"Total taker USDC inflow: ${total_taker_usdc:.2f}")
        
        # Plot slippage
        slippage_data = {'timestamps': all_timestamps, 'cumulative_slippage': all_cumulative}
        plot_slippage(slippage_data)
        
        # Calculate and plot ratio (slippage / usdc, protecting against division by zero)
        ratio_values = []
        for usdc, slippage in zip(all_cumulative_taker_usdc, all_cumulative):
            if usdc > 0:
                ratio_values.append(slippage / usdc)
            else:
                ratio_values.append(0)
        
        ratio_data = {'timestamps': all_timestamps, 'ratio': ratio_values}
        plot_usdc_to_slippage_ratio(ratio_data)
        
    finally:
        if conn:
            await conn.close()

if __name__ == "__main__":
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST, PG_PORT]):
        logger.error("One or more required database environment variables are not set.")
    else:
        asyncio.run(main())
