import os
import shutil
import asyncio
import asyncpg
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import json
import logging
from datetime import datetime, timedelta
from heatmap_15min_cycles import (
    fetch_polymarket_orderbook_updates,
    get_polymarket_yes_asset_id,
    fetch_kalshi_orderbook_updates,
    get_kalshi_ticker
)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Database config
PG_HOST = os.getenv("PG_SOCKET", "localhost")
PG_PORT = int(os.getenv("POLY_PG_PORT", 5432))
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# Configuration
K_CYCLES = 1000  # Number of 15-min intervals to analyze (None = all)
RANGE_DEN = 100  # Price range buckets (0.01 each)
DOMAIN_DEN = 15 * 60  # Time buckets (1 second each)

PLOT_DIR = "btc_arbitrage_plots"

def setup_plot_dir():
    """Delete and recreate plot directory."""
    if os.path.exists(PLOT_DIR):
        shutil.rmtree(PLOT_DIR)
    os.makedirs(PLOT_DIR, exist_ok=True)
    logger.info(f"Created {PLOT_DIR}\n")

def extract_prices(records):
    """Extract YES/NO prices from Polymarket records."""
    timestamps = []
    yes_prices = []
    no_prices = []
    
    for record in records:
        try:
            msg = json.loads(record['message'])
            bids = msg.get('bids', [])
            asks = msg.get('asks', [])
            
            if bids and asks:
                yes = max(float(b['price']) for b in bids)
                no = 1.0 - min(float(a['price']) for a in asks)
                
                timestamps.append(pd.to_datetime(record['server_time_us'], unit='us'))
                yes_prices.append(yes)
                no_prices.append(no)
        except:
            pass
    
    return timestamps, yes_prices, no_prices

def extract_kalshi_prices(records):
    """Extract YES/NO prices from Kalshi records."""
    timestamps = []
    yes_prices = []
    no_prices = []
    yes_book = {}
    no_book = {}
    
    for record in records:
        try:
            wrapper = json.loads(record['message'])
            msg_type = wrapper.get('type')
            msg = wrapper.get('msg', {})
            
            if msg_type == 'orderbook_snapshot':
                yes_book = {int(i[0]): i[1] for i in msg.get('yes', [])}
                no_book = {int(i[0]): i[1] for i in msg.get('no', [])}
            elif msg_type == 'orderbook_delta':
                side = msg.get('side')
                price = int(msg.get('price', 0))
                delta = msg.get('delta', 0)
                
                if side == 'yes':
                    yes_book[price] = yes_book.get(price, 0) + delta
                    if yes_book[price] <= 0 and price in yes_book:
                        del yes_book[price]
                else:
                    no_book[price] = no_book.get(price, 0) + delta
                    if no_book[price] <= 0 and price in no_book:
                        del no_book[price]
            
            if yes_book and no_book:
                best_yes = max(yes_book.keys()) / 100.0
                best_no = max(no_book.keys()) / 100.0
                
                timestamps.append(pd.to_datetime(record['server_time_us'], unit='us'))
                yes_prices.append(best_yes)
                no_prices.append(best_no)
        except:
            pass
    
    return timestamps, yes_prices, no_prices

def align_prices(poly_ts, poly_yes, poly_no, kalshi_ts, kalshi_yes, kalshi_no):
    """Align two time series by forward-filling missing values."""
    df_poly = pd.DataFrame({
        'poly_yes': poly_yes,
        'poly_no': poly_no
    }, index=poly_ts)
    
    df_kalshi = pd.DataFrame({
        'kalshi_yes': kalshi_yes,
        'kalshi_no': kalshi_no
    }, index=kalshi_ts)
    
    # Remove duplicate indices (keep last occurrence)
    df_poly = df_poly[~df_poly.index.duplicated(keep='last')]
    df_kalshi = df_kalshi[~df_kalshi.index.duplicated(keep='last')]
    
    df_all = pd.concat([df_poly, df_kalshi], axis=1)
    df_all.sort_index(inplace=True)
    df_all.ffill(inplace=True)
    df_all.dropna(inplace=True)
    
    return df_all

def calculate_arbitrage(df):
    """Calculate arbitrage metrics."""
    df['poly_internal'] = 1.0 - (df['poly_yes'] + df['poly_no'])
    df['kalshi_internal'] = 1.0 - (df['kalshi_yes'] + df['kalshi_no'])
    df['mirror_yes'] = np.abs(df['poly_yes'] - df['kalshi_yes'])
    df['mirror_no'] = np.abs(df['poly_no'] - df['kalshi_no'])
    return df

def plot_interval_comparison(df, interval_num, interval_dt):
    """Plot arbitrage metrics for a single interval."""
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    
    # Internal arbitrage - Polymarket
    axes[0, 0].plot(df.index, df['poly_internal'], 'g-', linewidth=1, label='Polymarket Internal')
    axes[0, 0].axhline(0, color='black', linestyle='--', alpha=0.5)
    axes[0, 0].set_title('Polymarket Internal Arbitrage (1 - Yes - No)')
    axes[0, 0].set_ylabel('Arbitrage %')
    axes[0, 0].grid(True, alpha=0.3)
    axes[0, 0].legend()
    
    # Internal arbitrage - Kalshi
    axes[0, 1].plot(df.index, df['kalshi_internal'], 'b-', linewidth=1, label='Kalshi Internal')
    axes[0, 1].axhline(0, color='black', linestyle='--', alpha=0.5)
    axes[0, 1].set_title('Kalshi Internal Arbitrage (1 - Yes - No)')
    axes[0, 1].set_ylabel('Arbitrage %')
    axes[0, 1].grid(True, alpha=0.3)
    axes[0, 1].legend()
    
    # Mirror arbitrage - YES
    axes[1, 0].plot(df.index, df['mirror_yes'], 'purple', linewidth=1, label='Mirror YES')
    axes[1, 0].set_title('Mirror Arbitrage - YES (|Poly - Kalshi|)')
    axes[1, 0].set_ylabel('Price Difference')
    axes[1, 0].grid(True, alpha=0.3)
    axes[1, 0].legend()
    
    # Mirror arbitrage - NO
    axes[1, 1].plot(df.index, df['mirror_no'], 'orange', linewidth=1, label='Mirror NO')
    axes[1, 1].set_title('Mirror Arbitrage - NO (|Poly - Kalshi|)')
    axes[1, 1].set_ylabel('Price Difference')
    axes[1, 1].grid(True, alpha=0.3)
    axes[1, 1].legend()
    
    for ax in axes.flat:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        ax.tick_params(axis='x', rotation=45)
    
    fig.suptitle(f'Interval {interval_num}: {interval_dt.strftime("%Y-%m-%d %H:%M UTC")}', fontsize=14, fontweight='bold')
    fig.tight_layout()
    
    filename = os.path.join(PLOT_DIR, f"interval_{interval_num:02d}_{interval_dt.strftime('%Y%m%d_%H%M')}.png")
    plt.savefig(filename, dpi=100)
    plt.close()
    logger.info(f"Saved interval plot: {filename}")

def create_heatmap(all_data, metric_name, title, filename):
    """Create heatmap of arbitrage metric across cycles."""
    # Initialize heatmap grid
    # Y-axis: price buckets (RANGE_DEN buckets of size 1/RANGE_DEN)
    # X-axis: time buckets (DOMAIN_DEN buckets)
    heatmap = np.zeros((RANGE_DEN, DOMAIN_DEN))
    
    for cycle_data in all_data:
        if cycle_data is None or len(cycle_data) == 0:
            continue
        
        df = cycle_data
        
        # Get start time for this cycle
        cycle_start = df.index[0]
        cycle_end = df.index[-1]
        cycle_duration_s = (cycle_end - cycle_start).total_seconds()
        
        for idx, (timestamp, row) in enumerate(df.iterrows()):
            value = row[metric_name]
            
            # Skip NaN
            if np.isnan(value):
                continue
            
            # Clamp value to [0, 1]
            value_clamped = np.clip(value, 0, 1)
            
            # Get time offset from cycle start
            time_offset_s = (timestamp - cycle_start).total_seconds()
            
            # Map to bucket indices
            price_bucket = int(value_clamped * RANGE_DEN)
            price_bucket = min(price_bucket, RANGE_DEN - 1)
            
            time_bucket = int((time_offset_s / cycle_duration_s) * DOMAIN_DEN)
            time_bucket = min(time_bucket, DOMAIN_DEN - 1)
            
            heatmap[price_bucket, time_bucket] += 1
    
    # Plot heatmap
    fig, ax = plt.subplots(figsize=(16, 10))
    
    im = ax.imshow(heatmap, aspect='auto', origin='lower', cmap='YlOrRd', interpolation='nearest')
    
    # Labels
    ax.set_xlabel('Time (seconds into cycle)')
    ax.set_ylabel('Value (buckets of 1/RANGE_DEN)')
    ax.set_title(title)
    
    # Set tick labels
    ax.set_xticks(np.linspace(0, DOMAIN_DEN-1, 10))
    ax.set_xticklabels([f'{int(i * 15*60 / DOMAIN_DEN)}' for i in np.linspace(0, DOMAIN_DEN-1, 10)])
    
    ax.set_yticks(np.linspace(0, RANGE_DEN-1, 11))
    ax.set_yticklabels([f'{i/RANGE_DEN:.2f}' for i in np.linspace(0, RANGE_DEN, 11)])
    
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('Observation Count')
    
    plt.tight_layout()
    
    filepath = os.path.join(PLOT_DIR, filename)
    plt.savefig(filepath, dpi=100)
    plt.close()
    logger.info(f"Saved heatmap: {filepath}")

async def main():
    """Main analysis function."""
    setup_plot_dir()
    
    pool = await asyncpg.create_pool(
        host=PG_HOST, port=PG_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    
    try:
        # Get data range - iterate from actual data start through end
        min_ts = 1767815077116000  # Jan 7, 2026 19:44:37 UTC (actual data start)
        max_ts = 1770021582844001  # Feb 2, 2026 08:39:42 UTC
        
        min_dt = pd.to_datetime(min_ts, unit='us')
        max_dt = pd.to_datetime(max_ts, unit='us')
        logger.info(f"Data range: {min_dt} to {max_dt}\n")
        
        # Process intervals starting from first, moving forward
        # Only COUNT toward K_CYCLES those intervals where BOTH Polymarket AND Kalshi have data
        logger.info("Processing intervals...\n")
        
        current_us = min_ts
        total_checked = 0  # Total intervals examined
        cycle_count = 0    # Intervals with data from BOTH exchanges
        cycle_data_list = []
        
        while current_us < max_ts and (K_CYCLES is None or cycle_count < K_CYCLES):
            total_checked += 1
            current_dt = pd.to_datetime(current_us, unit='us')
            current_dt = current_dt.floor('15min')  # Ensure 15-min boundary
            
            logger.info(f"{'='*80}")
            logger.info(f"Interval {total_checked} (CYCLE {cycle_count + 1}): {current_dt.strftime('%Y-%m-%d %H:%M UTC')}")
            logger.info(f"{'='*80}\n")
            
            # Get Polymarket data
            asset_id = await get_polymarket_yes_asset_id(pool, current_dt)
            if asset_id is None:
                logger.warning("No Polymarket market - Skipping\n")
                current_us += 15 * 60 * 1_000_000
                continue
            
            poly_records = await fetch_polymarket_orderbook_updates(pool, current_dt, asset_id)
            if not poly_records:
                logger.warning("No Polymarket records - Skipping\n")
                current_us += 15 * 60 * 1_000_000
                continue
            
            poly_ts, poly_yes, poly_no = extract_prices(poly_records)
            logger.info(f"[POLY] {len(poly_ts)} price points")
            
            if not poly_ts:
                logger.warning("No Polymarket prices - Skipping\n")
                current_us += 15 * 60 * 1_000_000
                continue
            
            # Get Kalshi data
            kalshi_ticker = await get_kalshi_ticker(pool, current_dt)
            if not kalshi_ticker:
                logger.warning("No Kalshi market - Skipping\n")
                current_us += 15 * 60 * 1_000_000
                continue
            
            kalshi_records = await fetch_kalshi_orderbook_updates(pool, current_dt, kalshi_ticker)
            if not kalshi_records:
                logger.warning("No Kalshi records - Skipping\n")
                current_us += 15 * 60 * 1_000_000
                continue
            
            kalshi_ts, kalshi_yes, kalshi_no = extract_kalshi_prices(kalshi_records)
            logger.info(f"[KALSHI] {len(kalshi_ts)} price points")
            
            if not kalshi_ts:
                logger.warning("No Kalshi prices - Skipping\n")
                current_us += 15 * 60 * 1_000_000
                continue
            
            # BOTH sources have data - increment cycle count
            cycle_count += 1
            
            # Align and calculate arbitrage
            df = align_prices(poly_ts, poly_yes, poly_no, kalshi_ts, kalshi_yes, kalshi_no)
            df = calculate_arbitrage(df)
            logger.info(f"Aligned: {len(df)} synchronized data points\n")
            
            cycle_data_list.append(df)
            
            # Plot individual interval (only first 5 with data)
            if cycle_count <= 10:
                plot_interval_comparison(df, cycle_count, current_dt)
            
            current_us += 15 * 60 * 1_000_000
        
        # Create heatmaps from all cycles
        logger.info(f"\n{'='*80}")
        logger.info("Creating heatmaps...")
        logger.info(f"{'='*80}\n")
        
        if cycle_data_list:
            create_heatmap(cycle_data_list, 'poly_internal', 
                          f'Polymarket Internal Arbitrage Heatmap ({len(cycle_data_list)} cycles)',
                          'heatmap_poly_internal.png')
            
            create_heatmap(cycle_data_list, 'kalshi_internal', 
                          f'Kalshi Internal Arbitrage Heatmap ({len(cycle_data_list)} cycles)',
                          'heatmap_kalshi_internal.png')
            
            create_heatmap(cycle_data_list, 'mirror_yes', 
                          f'Mirror YES Arbitrage Heatmap ({len(cycle_data_list)} cycles)',
                          'heatmap_mirror_yes.png')
            
            create_heatmap(cycle_data_list, 'mirror_no', 
                          f'Mirror NO Arbitrage Heatmap ({len(cycle_data_list)} cycles)',
                          'heatmap_mirror_no.png')
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Complete: Analyzed {len(cycle_data_list)} cycles")
        logger.info(f"{'='*80}\n")
    
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
