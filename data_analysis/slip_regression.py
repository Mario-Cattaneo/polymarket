#!/usr/bin/env python3
"""
Linear regression analysis: slippage = a * inflow + b
Performs regression on discrete settlement pairs to understand the relationship
between taker USDC inflow and realized slippage.
"""

import os
import asyncio
import asyncpg
import logging
import json
import matplotlib.pyplot as plt
import numpy as np
from decimal import Decimal, getcontext
from eth_abi import decode
from scipy import stats

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
START_BLOCK_NR = 79172085  # None = use database minimum
END_BLOCK_NR = 81225971     # None = use database maximum
BLOCK_BATCH_SIZE = 200_000  # Process in batches of N blocks

# --- Regression Filter Range (Unix timestamps) ---
# Set these to filter which settlements to include in regression
# Leave as None to include all data
# Example: Block 79172085 → timestamp 1763463428, Block 79372085 → timestamp 1763863428
REGRESSION_START_TIMESTAMP = ((START_BLOCK_NR - REF_BLOCK_NUMBER) * SECONDS_PER_BLOCK) + REF_UNIX_TIMESTAMP + 24 * 60 * 60  # Set to first block's timestamp + offset to skip early artifacts
REGRESSION_END_TIMESTAMP = None    # None = no upper limit

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
        logger.debug(f"Failed to decode OrderFilled: {e}")
        return {}

async def process_batch_for_regression(settlements: list) -> tuple:
    """
    Extract discrete (inflow, slippage) pairs from settlements.
    Returns (timestamps, inflows, slippages) - three parallel lists of individual measurements.
    """
    EXCHANGE_ADDRESS = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
    
    timestamps = []
    inflows = []
    slippages = []
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
                continue
            
            # Determine taker direction
            if exchange_fill['makerAssetId'] == 0:
                taker_is_buying = True
            else:
                taker_is_buying = False
            
            # Process maker fills
            maker_fills = [f for f in all_fills if not f['is_exchange']]
            
            if len(maker_fills) < 1:
                continue
            
            # Calculate prices for all maker fills
            fills_info = []
            for fill in maker_fills:
                maker_is_buying = (fill['makerAssetId'] == 0)
                
                making_raw = Decimal(fill['making'])
                taking_raw = Decimal(fill['taking'])
                
                # Price is always min/max to keep in [0,1]
                price_per_token = min(making_raw, taking_raw) / max(making_raw, taking_raw)
                
                # Determine amounts based on asset IDs
                if fill['takerAssetId'] == 0:
                    amount_shares = making_raw / SHARE_SCALAR
                    amount_usdc = taking_raw / USDC_SCALAR
                else:
                    amount_shares = taking_raw / SHARE_SCALAR
                    amount_usdc = making_raw / USDC_SCALAR
                
                # If maker is on same side as taker, convert price to taker's perspective
                if maker_is_buying == taker_is_buying:
                    price_per_token = Decimal(1) - price_per_token
                
                fills_info.append({
                    'price_per_token': price_per_token,
                    'amount_shares': amount_shares,
                    'amount_usdc': amount_usdc
                })
            
            # Only compute slippage if we have at least 2 maker fills
            if len(fills_info) >= 2:
                prices = [f['price_per_token'] for f in fills_info]
                if taker_is_buying:
                    p_best = min(prices)
                else:
                    p_best = max(prices)

                # Calculate slippage
                exchange_making = Decimal(exchange_fill['making'])
                exchange_taking = Decimal(exchange_fill['taking'])
                amount_shares = max(exchange_making, exchange_taking) / SHARE_SCALAR
                actual_cost = min(exchange_making, exchange_taking) / USDC_SCALAR
                expected_cost = p_best * amount_shares
                tx_slippage = abs(expected_cost - actual_cost)
                
                # Taker USDC inflow
                exchange_usdc_volume = min(exchange_making, exchange_taking) / USDC_SCALAR
                
                timestamp = block_to_timestamp(row['block_number'])
                
                # Store discrete measurement
                timestamps.append(timestamp)
                inflows.append(float(exchange_usdc_volume))
                slippages.append(float(tx_slippage))
                count += 1

        except Exception as e:
            continue
    
    return timestamps, inflows, slippages, count

def filter_data_by_timestamp(timestamps: list, inflows: list, slippages: list) -> tuple:
    """Filter data based on REGRESSION_START_TIMESTAMP and REGRESSION_END_TIMESTAMP."""
    filtered_ts = []
    filtered_inf = []
    filtered_slip = []
    
    for ts, inf, slip in zip(timestamps, inflows, slippages):
        if REGRESSION_START_TIMESTAMP is not None and ts < REGRESSION_START_TIMESTAMP:
            continue
        if REGRESSION_END_TIMESTAMP is not None and ts > REGRESSION_END_TIMESTAMP:
            continue
        filtered_ts.append(ts)
        filtered_inf.append(inf)
        filtered_slip.append(slip)
    
    return filtered_ts, filtered_inf, filtered_slip

def perform_regression(inflows: list, slippages: list) -> dict:
    """
    Perform linear regression: slippage = slope * inflow + intercept
    Returns dict with regression results and statistics.
    """
    X = np.array(inflows)
    y = np.array(slippages)
    
    # Using scipy.stats.linregress for comprehensive statistics
    slope, intercept, r_value, p_value, std_err = stats.linregress(X, y)
    
    # Calculate additional metrics
    y_pred = slope * X + intercept
    residuals = y - y_pred
    rmse = np.sqrt(np.mean(residuals ** 2))
    
    # R-squared
    r_squared = r_value ** 2
    
    return {
        'slope': slope,
        'intercept': intercept,
        'r_value': r_value,
        'r_squared': r_squared,
        'p_value': p_value,
        'std_err': std_err,
        'rmse': rmse,
        'n_samples': len(X),
        'X': X,
        'y': y,
        'y_pred': y_pred
    }

def plot_regression(regression_results: dict):
    """Create scatter plot with regression line."""
    X = regression_results['X']
    y = regression_results['y']
    y_pred = regression_results['y_pred']
    slope = regression_results['slope']
    intercept = regression_results['intercept']
    r_squared = regression_results['r_squared']
    
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(14, 9))
    
    # Scatter plot
    ax.scatter(X, y, alpha=0.5, s=20, color='steelblue', label='Individual settlements')
    
    # Regression line
    ax.plot(X, y_pred, 'r-', linewidth=2.5, label=f'Linear fit: slippage = {slope:.6f} * inflow + {intercept:.6f}')
    
    ax.set_xlabel('Taker USDC Inflow', fontsize=14)
    ax.set_ylabel('Slippage (USDC)', fontsize=14)
    ax.set_title(f'Linear Regression: Slippage vs Inflow (R² = {r_squared:.4f})', fontsize=16)
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    filename = 'slip_regression.png'
    plt.savefig(filename, dpi=150)
    logger.info(f"✅ Regression plot saved to {filename}")
    plt.close()

def log_regression_summary(regression_results: dict):
    """Log comprehensive regression statistics."""
    logger.info("\n" + "="*70)
    logger.info("LINEAR REGRESSION RESULTS: slippage = slope * inflow + intercept")
    logger.info("="*70)
    logger.info(f"Slope (β):              {regression_results['slope']:.8f}")
    logger.info(f"Intercept (b):          {regression_results['intercept']:.8f}")
    logger.info(f"R² (variance explained): {regression_results['r_squared']:.6f}")
    logger.info(f"R (correlation):        {regression_results['r_value']:.6f}")
    logger.info(f"P-value:                {regression_results['p_value']:.2e}")
    logger.info(f"Std Error (slope):      {regression_results['std_err']:.8f}")
    logger.info(f"RMSE:                   {regression_results['rmse']:.8f}")
    logger.info(f"N samples:              {regression_results['n_samples']}")
    logger.info("="*70)
    
    # Interpretation
    if regression_results['p_value'] < 0.001:
        significance = "highly significant (p < 0.001)"
    elif regression_results['p_value'] < 0.01:
        significance = "very significant (p < 0.01)"
    elif regression_results['p_value'] < 0.05:
        significance = "significant (p < 0.05)"
    else:
        significance = "not significant (p >= 0.05)"
    
    logger.info(f"\nInterpretation:")
    logger.info(f"  - Relationship is {significance}")
    logger.info(f"  - For every $1 of inflow, slippage increases by ${regression_results['slope']:.6f}")
    logger.info(f"  - Model explains {regression_results['r_squared']*100:.2f}% of variance in slippage")
    logger.info("="*70 + "\n")

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
        
        # Batch processing - collect discrete measures
        all_timestamps = []
        all_inflows = []
        all_slippages = []
        total_processed = 0
        
        curr_blk = start_blk
        
        while curr_blk < end_blk:
            next_blk = min(curr_blk + BLOCK_BATCH_SIZE, end_blk)
            
            settlements = await fetch_batch(conn, curr_blk, next_blk)
            if settlements:
                timestamps, inflows, slippages, count = await process_batch_for_regression(settlements)
                
                all_timestamps.extend(timestamps)
                all_inflows.extend(inflows)
                all_slippages.extend(slippages)
                total_processed += count
                
                logger.info(f"  Blocks {curr_blk:,}-{next_blk:,}: {count} valid settlements extracted")
            
            curr_blk = next_blk
        
        logger.info(f"\nTotal settlements with valid slippage data: {total_processed}")
        
        # Filter data by timestamp range for regression
        if REGRESSION_START_TIMESTAMP or REGRESSION_END_TIMESTAMP:
            filtered_ts, filtered_inflows, filtered_slippages = filter_data_by_timestamp(
                all_timestamps, all_inflows, all_slippages
            )
            logger.info(f"After timestamp filtering: {len(filtered_inflows)} samples")
        else:
            filtered_inflows = all_inflows
            filtered_slippages = all_slippages
        
        if len(filtered_inflows) < 2:
            logger.error("Not enough samples for regression after filtering")
            return
        
        # Perform regression
        regression_results = perform_regression(filtered_inflows, filtered_slippages)
        log_regression_summary(regression_results)
        
        # Plot
        plot_regression(regression_results)
        
    finally:
        if conn:
            await conn.close()

if __name__ == "__main__":
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST, PG_PORT]):
        logger.error("One or more required database environment variables are not set.")
    else:
        asyncio.run(main())
