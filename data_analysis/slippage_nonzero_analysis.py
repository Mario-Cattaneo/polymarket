#!/usr/bin/env python3
"""
Analyze correlation between order size and slippage for settlements with NON-ZERO slippage only.
Includes probability analysis, binned distributions, and breakdown by settlement type.
"""

import os
import asyncio
import asyncpg
import logging
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from decimal import Decimal, getcontext
from eth_abi import decode
from scipy.stats import pearsonr, spearmanr

# ----------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
getcontext().prec = 36

PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

REF_BLOCK_NUMBER = 81372608
REF_UNIX_TIMESTAMP = 1767864474
SECONDS_PER_BLOCK = 2

# --- Block Range Configuration ---
START_BLOCK_NR = None  # None = use database minimum
END_BLOCK_NR = 79172085 + 50000  # None = use database maximum

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
        return {}

async def process_batch_metrics(records: list) -> tuple:
    """
    Process a batch to extract metrics for both zero and non-zero slippage settlements.
    Returns (all_metrics, nonzero_metrics, zero_slippage_count).
    """
    metrics = []
    nonzero_metrics = []
    zero_count = 0
    EXCHANGE_ADDRESS = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

    for row in records:
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

            # Get order size
            exchange_making = Decimal(exchange_fill['making'])
            exchange_taking = Decimal(exchange_fill['taking'])
            order_size = max(exchange_making, exchange_taking) / SHARE_SCALAR

            # Process maker fills to calculate slippage
            maker_fills = [f for f in all_fills if not f['is_exchange']]
            
            if len(maker_fills) < 2:
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
            
            # Compute slippage
            prices = [f['price_per_token'] for f in fills_info]
            if taker_is_buying:
                p_best = min(prices)
            else:
                p_best = max(prices)

            amount_shares = max(exchange_making, exchange_taking) / SHARE_SCALAR
            actual_cost = min(exchange_making, exchange_taking) / USDC_SCALAR
            expected_cost = p_best * amount_shares
            tx_slippage = abs(expected_cost - actual_cost)
            
            metric = {
                'timestamp': pd.to_datetime(block_to_timestamp(row['block_number']), unit='s'),
                'transaction_hash': row['transaction_hash'],
                'order_size': float(order_size),
                'slippage': float(tx_slippage),
                'taker_direction': 'BUYING' if taker_is_buying else 'SELLING',
                'settlement_type': row['type'],
                'num_makers': len(maker_fills),
                'price_variance': float(max(prices) - min(prices))
            }
            
            metrics.append(metric)
            
            if tx_slippage > Decimal("0.0001"):  # Non-zero threshold
                nonzero_metrics.append(metric)
            else:
                zero_count += 1

        except Exception as e:
            continue
    
    return metrics, nonzero_metrics, zero_count

# ----------------------------------------------------------------
# ANALYSIS FUNCTIONS
# ----------------------------------------------------------------

def analyze_nonzero_correlation(df: pd.DataFrame):
    """Compute correlation statistics for non-zero slippage settlements."""
    
    if df.empty or len(df) < 3:
        logger.warning("Not enough non-zero slippage data for analysis.")
        return
    
    logger.info("\n" + "="*80)
    logger.info("CORRELATION ANALYSIS: Order Size vs Slippage (NON-ZERO SLIPPAGE ONLY)")
    logger.info("="*80)
    
    # Overall
    if len(df) >= 3:
        pearson_r, pearson_p = pearsonr(df['order_size'], df['slippage'])
        spearman_r, spearman_p = spearmanr(df['order_size'], df['slippage'])
        
        logger.info(f"\nOVERALL (n={len(df)} non-zero slippage settlements):")
        logger.info(f"  Pearson correlation: {pearson_r:.4f} (p-value: {pearson_p:.6f})")
        logger.info(f"  Spearman correlation: {spearman_r:.4f} (p-value: {spearman_p:.6f})")
        logger.info(f"  Mean order size: ${df['order_size'].mean():.2f}")
        logger.info(f"  Median order size: ${df['order_size'].median():.2f}")
        logger.info(f"  Mean slippage: ${df['slippage'].mean():.4f}")
        logger.info(f"  Median slippage: ${df['slippage'].median():.4f}")
    
    # By settlement type
    logger.info("\nBY SETTLEMENT TYPE:")
    for stype in df['settlement_type'].unique():
        subset = df[df['settlement_type'] == stype]
        if len(subset) >= 3:
            pearson_r, pearson_p = pearsonr(subset['order_size'], subset['slippage'])
            spearman_r, spearman_p = spearmanr(subset['order_size'], subset['slippage'])
            
            logger.info(f"\n  {stype.upper()} (n={len(subset)}):")
            logger.info(f"    Pearson: {pearson_r:.4f}, Spearman: {spearman_r:.4f}")
            logger.info(f"    Mean slippage: ${subset['slippage'].mean():.4f}")
            logger.info(f"    Mean order size: ${subset['order_size'].mean():.2f}")

def analyze_probability(df_all: pd.DataFrame):
    """Analyze probability of inclurring slippage by order size."""
    logger.info("\n" + "="*80)
    logger.info("PROBABILITY ANALYSIS: Chance of Non-Zero Slippage by Order Size")
    logger.info("="*80)
    
    # Create size bins
    n_bins = 10
    binned = df_all.groupby(pd.cut(df_all['order_size'], bins=n_bins))
    
    logger.info("")
    for bin_label, group in binned:
        if len(group) > 0:
            nonzero = (group['slippage'] > 0.0001).sum()
            prob = nonzero / len(group) * 100
            avg_slip = group['slippage'].mean()
            
            logger.info(f"  Size {bin_label.left:>10.0f}-{bin_label.right:>10.0f}: " +
                       f"n={len(group):>5} | non-zero slippage: {nonzero:>4} ({prob:>5.1f}%) | " +
                       f"avg slippage: ${avg_slip:>8.4f}")

def plot_nonzero_analysis(df_nonzero: pd.DataFrame):
    """Create analysis plots for non-zero slippage."""
    if df_nonzero.empty:
        logger.warning("No non-zero slippage data to plot.")
        return

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # Plot 1: Scatter with regression (non-zero only)
    ax = axes[0, 0]
    colors = {'split': 'steelblue', 'merge': 'crimson', 'complementary': 'darkgreen', 'conversion': 'orange'}
    
    for stype in df_nonzero['settlement_type'].unique():
        subset = df_nonzero[df_nonzero['settlement_type'] == stype]
        ax.scatter(subset['order_size'], subset['slippage'], 
                  label=stype, color=colors.get(stype, 'gray'), alpha=0.6, s=80)
    
    if len(df_nonzero) >= 3:
        z = np.polyfit(df_nonzero['order_size'], df_nonzero['slippage'], 1)
        p = np.poly1d(z)
        x_line = np.linspace(df_nonzero['order_size'].min(), df_nonzero['order_size'].max(), 100)
        ax.plot(x_line, p(x_line), color='black', linestyle='--', linewidth=2, alpha=0.8)
    
    ax.set_xlabel('Order Size (Shares)', fontsize=12)
    ax.set_ylabel('Slippage (USDC)', fontsize=12)
    ax.set_title('Order Size vs Slippage (Non-Zero Only)', fontsize=14)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    
    # Plot 2: Binned mean slippage
    ax = axes[0, 1]
    binned = df_nonzero.groupby(pd.cut(df_nonzero['order_size'], bins=8))
    bin_stats = []
    for bin_label, group in binned:
        if len(group) > 0:
            bin_stats.append({
                'bin_center': (bin_label.left + bin_label.right) / 2,
                'mean_slippage': group['slippage'].mean(),
                'p75_slippage': group['slippage'].quantile(0.75),
                'p95_slippage': group['slippage'].quantile(0.95),
                'count': len(group)
            })
    
    if bin_stats:
        bin_df = pd.DataFrame(bin_stats)
        ax.errorbar(bin_df['bin_center'], bin_df['mean_slippage'], 
                   yerr=bin_df['p75_slippage'] - bin_df['mean_slippage'],
                   label='Mean ± p75', color='steelblue', marker='o', linestyle='-', linewidth=2, capsize=5)
        ax.scatter(bin_df['bin_center'], bin_df['p95_slippage'], 
                  label='p95', color='red', marker='^', s=100, alpha=0.7)
    
    ax.set_xlabel('Order Size Range (Shares)', fontsize=12)
    ax.set_ylabel('Slippage (USDC)', fontsize=12)
    ax.set_title('Slippage Distribution by Size (Non-Zero Only)', fontsize=14)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    
    # Plot 3: Probability of non-zero slippage
    ax = axes[1, 0]
    prob_data = []
    binned = df_nonzero.groupby(pd.cut(df_nonzero['order_size'], bins=8))
    for bin_label, group in binned:
        if len(group) > 0:
            prob_data.append({
                'bin_center': (bin_label.left + bin_label.right) / 2,
                'count': len(group)
            })
    
    if prob_data:
        prob_df = pd.DataFrame(prob_data)
        ax.bar(prob_df['bin_center'], prob_df['count'], width=prob_df['bin_center'].iloc[1] - prob_df['bin_center'].iloc[0] if len(prob_df) > 1 else 1,
              color='steelblue', alpha=0.7)
    
    ax.set_xlabel('Order Size Range (Shares)', fontsize=12)
    ax.set_ylabel('Count of Non-Zero Slippage Settlements', fontsize=12)
    ax.set_title('Settlement Count with Non-Zero Slippage', fontsize=14)
    ax.grid(True, alpha=0.3, axis='y')
    
    # Plot 4: By settlement type
    ax = axes[1, 1]
    type_stats = df_nonzero.groupby('settlement_type').agg({
        'slippage': ['mean', 'median', 'count'],
        'order_size': 'mean'
    }).round(4)
    
    type_data = []
    for stype in df_nonzero['settlement_type'].unique():
        subset = df_nonzero[df_nonzero['settlement_type'] == stype]
        type_data.append({
            'type': stype,
            'mean_slip': subset['slippage'].mean(),
            'count': len(subset)
        })
    
    if type_data:
        type_df = pd.DataFrame(type_data)
        bars = ax.bar(range(len(type_df)), type_df['mean_slip'], color='steelblue', alpha=0.7)
        ax.set_xticks(range(len(type_df)))
        ax.set_xticklabels(type_df['type'], rotation=45)
        ax.set_ylabel('Mean Slippage (USDC)', fontsize=12)
        ax.set_title('Mean Slippage by Settlement Type', fontsize=14)
        
        # Add count labels on bars
        for i, (bar, row) in enumerate(zip(bars, type_df.itertuples())):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'n={row.count}', ha='center', va='bottom', fontsize=10)
        
        ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    filename = 'slippage_nonzero_analysis.png'
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
        
        start_blk = START_BLOCK_NR if START_BLOCK_NR is not None else min_blk
        end_blk = END_BLOCK_NR if END_BLOCK_NR is not None else max_blk
        
        if start_blk > end_blk:
            logger.error(f"Invalid block range: {start_blk} > {end_blk}")
            return
        
        logger.info(f"Processing settlements from block {start_blk} to {end_blk}")
        
        # Batch processing
        all_metrics = []
        nonzero_metrics = []
        total_zero_slip = 0
        BLOCK_BATCH_SIZE = 50_000
        curr_blk = start_blk
        
        while curr_blk < end_blk:
            next_blk = min(curr_blk + BLOCK_BATCH_SIZE, end_blk)
            
            settlements = await fetch_batch(conn, curr_blk, next_blk)
            if settlements:
                batch_all, batch_nonzero, batch_zero = await process_batch_metrics(settlements)
                all_metrics.extend(batch_all)
                nonzero_metrics.extend(batch_nonzero)
                total_zero_slip += batch_zero
                
                logger.info(f"  Blocks {curr_blk:,}-{next_blk:,}: {len(batch_all)} total, {len(batch_nonzero)} non-zero slippage")
            
            curr_blk = next_blk
        
        if not all_metrics:
            logger.warning("No valid settlements found.")
            return
        
        df_all = pd.DataFrame(all_metrics)
        df_nonzero = pd.DataFrame(nonzero_metrics)
        
        logger.info(f"\nAnalyzed {len(df_all)} total settlements")
        logger.info(f"  Zero slippage: {total_zero_slip + len(df_nonzero) - len(df_nonzero)} settlements")
        logger.info(f"  Non-zero slippage: {len(df_nonzero)} settlements ({len(df_nonzero)/len(df_all)*100:.1f}%)")
        
        # Analysis
        analyze_probability(df_all)
        analyze_nonzero_correlation(df_nonzero)
        
        # Plotting
        logger.info("\nGenerating visualizations...")
        plot_nonzero_analysis(df_nonzero)
        
    finally:
        if conn:
            await conn.close()

if __name__ == "__main__":
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST, PG_PORT]):
        logger.error("Missing database environment variables")
    else:
        asyncio.run(main())
