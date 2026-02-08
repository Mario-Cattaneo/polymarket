#!/usr/bin/env python3
"""
Analyze correlation between order size and slippage.

Order size = max(making, taking) from exchange fill / SHARE_SCALAR
Slippage = computed from settlement data using the same logic as house_slip.py
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

# --- Constants ---
USDC_SCALAR = Decimal("1_000_000")
SHARE_SCALAR = Decimal("1_000_000")

# --- Taker Intention Asset IDs ---
DEMOCRATS_YES = 83247781037352156539108067944461291821683755894607244160607042790356561625563
DEMOCRATS_NO = 33156410999665902694791064431724433042010245771106314074312009703157423879038
REPUBLICANS_YES = 65139230827417363158752884968303867495725894165574887635816574090175320800482
REPUBLICANS_NO = 17371217118862125782438074585166210555214661810823929795910191856905580975576

DEMOCRATS_ASSETS = {DEMOCRATS_YES, DEMOCRATS_NO}
REPUBLICANS_ASSETS = {REPUBLICANS_YES, REPUBLICANS_NO}

# ----------------------------------------------------------------
# HELPER & DATA PROCESSING FUNCTIONS
# ----------------------------------------------------------------

def block_to_timestamp(block_number: int) -> int:
    return ((block_number - REF_BLOCK_NUMBER) * SECONDS_PER_BLOCK) + REF_UNIX_TIMESTAMP

def determine_taker_intention(asset_id: int, taker_is_buying: bool) -> str:
    """Determine taker's intention based on which asset they're trading and their direction."""
    if asset_id in DEMOCRATS_ASSETS:
        return "Democrats" if taker_is_buying else "Republicans"
    elif asset_id in REPUBLICANS_ASSETS:
        return "Republicans" if taker_is_buying else "Democrats"
    else:
        return "Unknown"

async def fetch_data(query: str) -> list:
    conn = None
    try:
        conn = await asyncpg.connect(host=PG_HOST, port=int(PG_PORT), database=DB_NAME, user=DB_USER, password=DB_PASS)
        logger.info(f"Executing query...")
        records = await conn.fetch(query)
        logger.info(f"Fetched {len(records)} records.")
        return records
    except Exception as e:
        logger.error(f"Database error: {e}")
        return []
    finally:
        if conn:
            await conn.close()

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

def calculate_slippage_metrics(records: list) -> pd.DataFrame:
    """
    Process settlement records to extract order size and slippage.
    """
    metrics = []
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
            exchange_usdc_volume = min(exchange_making, exchange_taking) / USDC_SCALAR
            
            # Determine taker intention
            non_zero_asset_id = exchange_fill['makerAssetId'] if exchange_fill['takerAssetId'] == 0 else exchange_fill['takerAssetId']
            taker_intention = determine_taker_intention(non_zero_asset_id, taker_is_buying)

            # Process maker fills to calculate slippage
            maker_fills = [f for f in all_fills if not f['is_exchange']]
            
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
                amount_shares = max(exchange_making, exchange_taking) / SHARE_SCALAR
                actual_cost = min(exchange_making, exchange_taking) / USDC_SCALAR
                expected_cost = p_best * amount_shares
                tx_slippage = abs(expected_cost - actual_cost)
                
                metrics.append({
                    'timestamp': pd.to_datetime(block_to_timestamp(row['block_number']), unit='s'),
                    'transaction_hash': row['transaction_hash'],
                    'order_size': float(order_size),
                    'slippage': float(tx_slippage),
                    'taker_intention': taker_intention,
                    'taker_direction': 'BUYING' if taker_is_buying else 'SELLING'
                })

        except Exception as e:
            logger.warning(f"Skipping settlement {row.get('transaction_hash', 'unknown')} due to error: {e}")
            continue
    
    return pd.DataFrame(metrics)

# ----------------------------------------------------------------
# ANALYSIS FUNCTIONS
# ----------------------------------------------------------------

def analyze_correlation(df: pd.DataFrame):
    """Compute correlation statistics overall and by intention."""
    
    if df.empty or len(df) < 3:
        logger.warning("Not enough data for correlation analysis.")
        return
    
    logger.info("\n" + "="*80)
    logger.info("CORRELATION ANALYSIS: Order Size vs Slippage")
    logger.info("="*80)
    
    # Overall correlation
    if len(df) >= 3:
        pearson_r, pearson_p = pearsonr(df['order_size'], df['slippage'])
        spearman_r, spearman_p = spearmanr(df['order_size'], df['slippage'])
        
        logger.info(f"\nOVERALL (n={len(df)} settlements):")
        logger.info(f"  Pearson correlation: {pearson_r:.4f} (p-value: {pearson_p:.6f})")
        logger.info(f"  Spearman correlation: {spearman_r:.4f} (p-value: {spearman_p:.6f})")
        logger.info(f"  Mean order size: ${df['order_size'].mean():.2f}")
        logger.info(f"  Mean slippage: ${df['slippage'].mean():.4f}")
    
    # By taker intention
    for intention in df['taker_intention'].unique():
        subset = df[df['taker_intention'] == intention]
        if len(subset) >= 3:
            pearson_r, pearson_p = pearsonr(subset['order_size'], subset['slippage'])
            spearman_r, spearman_p = spearmanr(subset['order_size'], subset['slippage'])
            
            logger.info(f"\n{intention.upper()} INTENTION (n={len(subset)} settlements):")
            logger.info(f"  Pearson correlation: {pearson_r:.4f} (p-value: {pearson_p:.6f})")
            logger.info(f"  Spearman correlation: {spearman_r:.4f} (p-value: {spearman_p:.6f})")
            logger.info(f"  Mean order size: ${subset['order_size'].mean():.2f}")
            logger.info(f"  Mean slippage: ${subset['slippage'].mean():.4f}")

def plot_scatter_with_regression(df: pd.DataFrame):
    """Create scatter plot with regression lines."""
    if df.empty:
        logger.warning("No data to plot.")
        return

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(14, 9))
    
    colors = {'Democrats': 'steelblue', 'Republicans': 'crimson'}
    
    for intention in df['taker_intention'].unique():
        subset = df[df['taker_intention'] == intention]
        
        # Scatter plot
        ax.scatter(subset['order_size'], subset['slippage'], 
                  label=intention, color=colors.get(intention, 'gray'), 
                  alpha=0.6, s=100)
        
        # Regression line
        if len(subset) >= 3:
            z = np.polyfit(subset['order_size'], subset['slippage'], 1)
            p = np.poly1d(z)
            x_line = np.linspace(subset['order_size'].min(), subset['order_size'].max(), 100)
            ax.plot(x_line, p(x_line), color=colors.get(intention, 'gray'), 
                   linestyle='--', linewidth=2, alpha=0.8)
    
    ax.set_xlabel('Order Size (Shares)', fontsize=14)
    ax.set_ylabel('Slippage (USDC)', fontsize=14)
    ax.set_title('Order Size vs Slippage by Taker Intention', fontsize=16)
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    
    filename = 'slippage_order_size_scatter.png'
    plt.savefig(filename, dpi=150)
    logger.info(f"✅ Scatter plot saved to {filename}")
    plt.close()

def plot_binned_analysis(df: pd.DataFrame, n_bins=10):
    """Create binned analysis plot."""
    if df.empty:
        logger.warning("No data to plot.")
        return

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    colors = {'Democrats': 'steelblue', 'Republicans': 'crimson'}
    
    for intention in df['taker_intention'].unique():
        subset = df[df['taker_intention'] == intention]
        
        # Bin by order size
        binned = subset.groupby(pd.cut(subset['order_size'], bins=n_bins))
        bin_stats = []
        for bin_label, group in binned:
            if len(group) > 0:
                bin_stats.append({
                    'bin_center': (bin_label.left + bin_label.right) / 2,
                    'mean_slippage': group['slippage'].mean(),
                    'std_slippage': group['slippage'].std(),
                    'count': len(group)
                })
        
        if bin_stats:
            bin_df = pd.DataFrame(bin_stats)
            
            # Plot 1: Mean slippage per bin
            axes[0].errorbar(bin_df['bin_center'], bin_df['mean_slippage'], 
                            yerr=bin_df['std_slippage'], 
                            label=intention, color=colors.get(intention, 'gray'),
                            marker='o', linestyle='-', linewidth=2, capsize=5)
            
            # Plot 2: Count of settlements per bin
            axes[1].bar([x + (0.2 if intention == 'Republicans' else -0.2) for x in bin_df['bin_center']], 
                       bin_df['count'], width=0.35, label=intention,
                       color=colors.get(intention, 'gray'), alpha=0.7)
    
    axes[0].set_xlabel('Order Size Range (Shares)', fontsize=12)
    axes[0].set_ylabel('Mean Slippage (USDC)', fontsize=12)
    axes[0].set_title('Mean Slippage by Order Size Bin', fontsize=14)
    axes[0].legend(fontsize=10)
    axes[0].grid(True, alpha=0.3)
    
    axes[1].set_xlabel('Order Size Range (Shares)', fontsize=12)
    axes[1].set_ylabel('Number of Settlements', fontsize=12)
    axes[1].set_title('Settlement Count by Order Size Bin', fontsize=14)
    axes[1].legend(fontsize=10)
    axes[1].grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    
    filename = 'slippage_order_size_binned.png'
    plt.savefig(filename, dpi=150)
    logger.info(f"✅ Binned analysis plot saved to {filename}")
    plt.close()

# ----------------------------------------------------------------
# MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    logger.info("Fetching all settlement data...")
    settlement_records = await fetch_data("SELECT block_number, transaction_hash, type, trade FROM settlements_house")
    
    logger.info("Calculating order size and slippage metrics...")
    df_metrics = calculate_slippage_metrics(settlement_records)
    
    if df_metrics.empty:
        logger.warning("No valid settlements found for analysis.")
        return
    
    logger.info(f"Analyzed {len(df_metrics)} settlements with complete slippage data.")
    
    # Analysis
    analyze_correlation(df_metrics)
    
    # Plotting
    logger.info("\nGenerating visualizations...")
    plot_scatter_with_regression(df_metrics)
    plot_binned_analysis(df_metrics, n_bins=8)

if __name__ == "__main__":
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST, PG_PORT]):
        logger.error("One or more required database environment variables are not set.")
    else:
        asyncio.run(main())
