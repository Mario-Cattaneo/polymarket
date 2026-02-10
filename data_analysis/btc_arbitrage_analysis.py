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
    # Initialize heatmap grid and statistics
    # Y-axis: price buckets (RANGE_DEN buckets of size 1/RANGE_DEN)
    # X-axis: time buckets (DOMAIN_DEN buckets)
    heatmap = np.zeros((RANGE_DEN, DOMAIN_DEN))
    time_bucket_stats = {}  # Store values per time bucket for mean/std calculation
    
    for i in range(DOMAIN_DEN):
        time_bucket_stats[i] = []
    
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
            time_bucket_stats[time_bucket].append(value_clamped)
    
    # Calculate expected value and std for each time bucket
    expected_values = np.zeros(DOMAIN_DEN)
    std_values = np.zeros(DOMAIN_DEN)
    
    for i in range(DOMAIN_DEN):
        if time_bucket_stats[i]:
            expected_values[i] = np.mean(time_bucket_stats[i])
            std_values[i] = np.std(time_bucket_stats[i])
        else:
            expected_values[i] = np.nan
            std_values[i] = np.nan
    
    # Find max value in heatmap for dynamic range adjustment
    max_value = np.nanmax(heatmap)
    max_y_idx = np.where((heatmap > 0).any(axis=1))[0]
    if len(max_y_idx) > 0:
        max_y_idx = max_y_idx[-1] + 1
    else:
        max_y_idx = RANGE_DEN
    
    # Plot heatmap with statistics - ENHANCED VERSION
    fig = plt.figure(figsize=(18, 10))
    gs = fig.add_gridspec(3, 1, height_ratios=[3, 1.5, 1.5], hspace=0.35)
    
    # Font sizes
    title_fontsize = 22
    label_fontsize = 16
    tick_fontsize = 14
    legend_fontsize = 14
    
    # Main heatmap
    ax_hm = fig.add_subplot(gs[0])
    heatmap_trimmed = heatmap[:max_y_idx, :]
    
    im = ax_hm.imshow(heatmap_trimmed, aspect='auto', origin='lower', cmap='YlOrRd', interpolation='nearest')
    
    # Labels
    bucket_width = 1.0 / RANGE_DEN
    time_window_s = 15 * 60
    time_bucket_width_s = time_window_s / DOMAIN_DEN
    
    ax_hm.set_ylabel('Value Bucket', fontsize=label_fontsize, fontweight='bold')
    ax_hm.set_title(title, fontsize=title_fontsize, fontweight='bold', pad=20)
    
    # Set tick labels with larger fonts
    ax_hm.set_xticks(np.linspace(0, DOMAIN_DEN-1, 10))
    ax_hm.set_xticklabels([f'{int(i * time_window_s / DOMAIN_DEN)}' for i in np.linspace(0, DOMAIN_DEN-1, 10)],
                           fontsize=tick_fontsize)
    ax_hm.tick_params(axis='y', labelsize=tick_fontsize)
    
    # Adjust y-ticks to match trimmed heatmap
    n_ticks = min(11, max_y_idx + 1)
    ax_hm.set_yticks(np.linspace(0, max_y_idx-1, n_ticks))
    ax_hm.set_yticklabels([f'{i*bucket_width:.3f}' for i in np.linspace(0, max_y_idx, n_ticks)],
                           fontsize=tick_fontsize)
    
    cbar = plt.colorbar(im, ax=ax_hm)
    cbar.set_label('Observation Count', fontsize=label_fontsize, fontweight='bold')
    cbar.ax.tick_params(labelsize=tick_fontsize)
    
    # Expected value plot
    ax_ev = fig.add_subplot(gs[1])
    valid_idx = ~np.isnan(expected_values)
    ax_ev.plot(np.where(valid_idx)[0], expected_values[valid_idx], 'b-', linewidth=2.5, label='Expected Value')
    ax_ev.fill_between(np.where(valid_idx)[0], 
                        expected_values[valid_idx] - std_values[valid_idx],
                        expected_values[valid_idx] + std_values[valid_idx],
                        alpha=0.3, color='blue', label='±1 Std Dev')
    ax_ev.set_ylabel('Expected Value', fontsize=label_fontsize, fontweight='bold')
    ax_ev.set_xlim(0, DOMAIN_DEN-1)
    ax_ev.grid(True, alpha=0.3, linewidth=0.8)
    
    # Legend centered above the plot
    ax_ev.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=2,
                fontsize=legend_fontsize, frameon=True, fancybox=True, shadow=True)
    
    ax_ev.set_xticks(np.linspace(0, DOMAIN_DEN-1, 10))
    ax_ev.set_xticklabels([f'{int(i * time_window_s / DOMAIN_DEN)}' for i in np.linspace(0, DOMAIN_DEN-1, 10)],
                           fontsize=tick_fontsize)
    ax_ev.tick_params(axis='y', labelsize=tick_fontsize)
    
    # Standard deviation plot
    ax_std = fig.add_subplot(gs[2])
    ax_std.plot(np.where(valid_idx)[0], std_values[valid_idx], 'r-', linewidth=2.5, label='Standard Deviation')
    ax_std.set_ylabel('Std Dev', fontsize=label_fontsize, fontweight='bold')
    ax_std.set_xlabel('Time (seconds into cycle)', fontsize=label_fontsize, fontweight='bold')
    ax_std.set_xlim(0, DOMAIN_DEN-1)
    ax_std.grid(True, alpha=0.3, linewidth=0.8)
    
    # Legend centered above the plot
    ax_std.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15),
                 fontsize=legend_fontsize, frameon=True, fancybox=True, shadow=True)
    
    ax_std.set_xticks(np.linspace(0, DOMAIN_DEN-1, 10))
    ax_std.set_xticklabels([f'{int(i * time_window_s / DOMAIN_DEN)}' for i in np.linspace(0, DOMAIN_DEN-1, 10)],
                            fontsize=tick_fontsize)
    ax_std.tick_params(axis='y', labelsize=tick_fontsize)
    
    filepath = os.path.join(PLOT_DIR, filename)
    plt.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    logger.info(f"Saved heatmap: {filepath}")

def compute_heatmap_stats(all_data, metric_name):
    """Compute median and double percentile bands for a metric across all cycles.
    
    Inner band: 25th-75th percentile (middle third, ~68% of normal dist)
    Outer band: 1st-99th percentile (almost all data)
    """
    time_bucket_stats = {i: [] for i in range(DOMAIN_DEN)}
    
    for cycle_data in all_data:
        if cycle_data is None or len(cycle_data) == 0:
            continue
        
        df = cycle_data
        cycle_start = df.index[0]
        cycle_end = df.index[-1]
        cycle_duration_s = (cycle_end - cycle_start).total_seconds()
        
        for timestamp, row in df.iterrows():
            value = row[metric_name]
            
            if np.isnan(value):
                continue
            
            value_clamped = np.clip(value, 0, 1)
            time_offset_s = (timestamp - cycle_start).total_seconds()
            time_bucket = int((time_offset_s / cycle_duration_s) * DOMAIN_DEN)
            time_bucket = min(time_bucket, DOMAIN_DEN - 1)
            
            time_bucket_stats[time_bucket].append(value_clamped)
    
    median_values = np.zeros(DOMAIN_DEN)
    inner_lower = np.zeros(DOMAIN_DEN)  # 25th percentile
    inner_upper = np.zeros(DOMAIN_DEN)  # 75th percentile
    outer_lower = np.zeros(DOMAIN_DEN)  # 1st percentile
    outer_upper = np.zeros(DOMAIN_DEN)  # 99th percentile
    
    for i in range(DOMAIN_DEN):
        if time_bucket_stats[i]:
            median_values[i] = np.median(time_bucket_stats[i])
            inner_lower[i] = np.percentile(time_bucket_stats[i], 25)
            inner_upper[i] = np.percentile(time_bucket_stats[i], 75)
            outer_lower[i] = np.percentile(time_bucket_stats[i], 1)
            outer_upper[i] = np.percentile(time_bucket_stats[i], 99)
        else:
            median_values[i] = np.nan
            inner_lower[i] = np.nan
            inner_upper[i] = np.nan
            outer_lower[i] = np.nan
            outer_upper[i] = np.nan
    
    return median_values, inner_lower, inner_upper, outer_lower, outer_upper

def plot_expected_value(all_data, metric_name, title, filename):
    """Create individual expected value plot with double percentile bands."""
    
    median_values, inner_lower, inner_upper, outer_lower, outer_upper = compute_heatmap_stats(all_data, metric_name)
    valid_idx = ~np.isnan(median_values)
    
    # Create figure
    fig, ax = plt.subplots(figsize=(18, 8))
    
    # Font sizes - all enlarged
    title_fontsize = 26
    label_fontsize = 18
    tick_fontsize = 16
    legend_fontsize = 16
    
    time_window_s = 15 * 60
    
    # Plot median and percentile bands
    x_data = np.where(valid_idx)[0]
    y_median = median_values[valid_idx]
    y_inner_lower = inner_lower[valid_idx]
    y_inner_upper = inner_upper[valid_idx]
    y_outer_lower = outer_lower[valid_idx]
    y_outer_upper = outer_upper[valid_idx]
    
    # Plot outer band first (behind inner)
    ax.fill_between(x_data, y_outer_lower, y_outer_upper, 
                    alpha=0.15, color='blue', label='1st-99th Percentile')
    
    # Plot inner band
    ax.fill_between(x_data, y_inner_lower, y_inner_upper, 
                    alpha=0.35, color='blue', label='25th-75th Percentile')
    
    # Plot median on top
    ax.plot(x_data, y_median, color='blue', linewidth=3.5, label='Median')
    
    ax.set_title(title, fontsize=title_fontsize, fontweight='bold', pad=25)
    ax.set_ylabel('Price Discrepancy', fontsize=label_fontsize, fontweight='bold')
    ax.set_xlabel('Time (seconds into cycle)', fontsize=label_fontsize, fontweight='bold')
    
    ax.set_xlim(0, DOMAIN_DEN-1)
    ax.set_ylim(bottom=0)  # Constrain to non-negative
    ax.grid(True, alpha=0.3, linewidth=0.8)
    
    # Legend below title, centered
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05), ncol=3,
             fontsize=legend_fontsize, frameon=True, fancybox=True, shadow=True)
    
    # X-axis ticks
    ax.set_xticks(np.linspace(0, DOMAIN_DEN-1, 10))
    ax.set_xticklabels([f'{int(i * time_window_s / DOMAIN_DEN)}' for i in np.linspace(0, DOMAIN_DEN-1, 10)],
                       fontsize=tick_fontsize)
    ax.tick_params(axis='y', labelsize=tick_fontsize)
    
    fig.tight_layout()
    
    filepath = os.path.join(PLOT_DIR, filename)
    plt.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    logger.info(f"Saved expected value plot: {filepath}")

def plot_stacked_expected_values(all_data, num_cycles):
    """Create 4 vertically stacked expected value plots in one image with double percentile bands."""
    
    metrics = [
        ('poly_internal', 'Polymarket Internal Price Discrepancy: |1 - (Price YES + Price NO)|'),
        ('kalshi_internal', 'Kalshi Internal Price Discrepancy: |1 - (Price YES + Price NO)|'),
        ('mirror_yes', 'Mirror YES Price Discrepancy: |Price YES Polymarket - Price YES Kalshi|'),
        ('mirror_no', 'Mirror NO Price Discrepancy: |Price NO Polymarket - Price NO Kalshi|')
    ]
    
    fig, axes = plt.subplots(4, 1, figsize=(22, 20))
    fig.suptitle(f'Price Discrepancy Distributions over {num_cycles} cycles', 
                 fontsize=32, fontweight='bold', y=0.995)
    
    # Font sizes - all enlarged for better readability
    label_fontsize = 24
    tick_fontsize = 24
    legend_fontsize = 22
    subplot_title_fontsize = 24
    
    time_window_s = 15 * 60
    
    for idx, (metric_name, title) in enumerate(metrics):
        ax = axes[idx]
        
        median_values, inner_lower, inner_upper, outer_lower, outer_upper = compute_heatmap_stats(all_data, metric_name)
        valid_idx = ~np.isnan(median_values)
        
        x_data = np.where(valid_idx)[0]
        y_median = median_values[valid_idx]
        y_inner_lower = inner_lower[valid_idx]
        y_inner_upper = inner_upper[valid_idx]
        y_outer_lower = outer_lower[valid_idx]
        y_outer_upper = outer_upper[valid_idx]
        
        # Plot outer band first (behind inner)
        ax.fill_between(x_data, y_outer_lower, y_outer_upper, 
                        alpha=0.15, color='blue', label='1st-99th Percentile')
        
        # Plot inner band
        ax.fill_between(x_data, y_inner_lower, y_inner_upper, 
                        alpha=0.35, color='blue', label='25th-75th Percentile')
        
        # Plot median on top
        ax.plot(x_data, y_median, color='blue', linewidth=3.5, label='Median')
        
        ax.set_title(title, fontsize=subplot_title_fontsize, fontweight='bold', pad=15)
        if idx == 3:  # Only label x-axis on bottom plot
            ax.set_xlabel('Time (seconds into cycle)', fontsize=label_fontsize, fontweight='bold')
        
        ax.set_xlim(0, DOMAIN_DEN-1)
        ax.set_ylim(bottom=0)  # Constrain to non-negative
        ax.grid(True, alpha=0.3, linewidth=0.8)
        
        # Legend centered above plot
        ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.08), ncol=3,
                 fontsize=legend_fontsize, frameon=True, fancybox=True, shadow=True)
        
        # X-axis ticks
        ax.set_xticks(np.linspace(0, DOMAIN_DEN-1, 10))
        ax.set_xticklabels([f'{int(i * time_window_s / DOMAIN_DEN)}' for i in np.linspace(0, DOMAIN_DEN-1, 10)],
                           fontsize=tick_fontsize)
        ax.tick_params(axis='y', labelsize=tick_fontsize)
    
    fig.tight_layout()
    
    filepath = os.path.join(PLOT_DIR, 'stacked_expected_values.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    logger.info(f"Saved stacked expected values plot: {filepath}")

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
            
            # Create individual expected value plots
            logger.info(f"\n{'='*80}")
            logger.info("Creating expected value plots...")
            logger.info(f"{'='*80}\n")
            
            plot_expected_value(cycle_data_list, 'poly_internal',
                              'Polymarket Internal Price Discrepancy 1 - (Price YES + Price NO)',
                              'expected_value_poly_internal.png')
            
            plot_expected_value(cycle_data_list, 'kalshi_internal',
                              'Kalshi Internal Price Discrepancy 1 - (Price YES + Price NO)',
                              'expected_value_kalshi_internal.png')
            
            plot_expected_value(cycle_data_list, 'mirror_yes',
                              'Mirror YES Price Discrepancy: |Price YES Polymarket - Price YES Kalshi|',
                              'expected_value_mirror_yes.png')
            
            plot_expected_value(cycle_data_list, 'mirror_no',
                              'Mirror NO Price Discrepancy: |Price NO Polymarket - Price NO Kalshi|',
                              'expected_value_mirror_no.png')
            
            # Create stacked expected value plot
            logger.info(f"\n{'='*80}")
            logger.info("Creating stacked expected values plot...")
            logger.info(f"{'='*80}\n")
            
            plot_stacked_expected_values(cycle_data_list, len(cycle_data_list))
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Complete: Analyzed {len(cycle_data_list)} cycles")
        logger.info(f"{'='*80}\n")
    
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
