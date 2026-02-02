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
import random
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

PLOT_DIR = "test_btc_plots"

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

def plot_comparison(poly_data, kalshi_data, poly_title, poly_ticker, kalshi_ticker, interval_start, plot_num):
    """Plot YES and NO prices for both exchanges."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    poly_ts, poly_yes, poly_no = poly_data
    kalshi_ts, kalshi_yes, kalshi_no = kalshi_data
    
    # YES prices
    if poly_ts:
        ax1.plot(poly_ts, poly_yes, 'g-o', label=f'Polymarket {poly_ticker}', linewidth=2, markersize=4, alpha=0.7)
    if kalshi_ts:
        ax1.plot(kalshi_ts, kalshi_yes, 'b-s', label=f'Kalshi {kalshi_ticker}', linewidth=2, markersize=4, alpha=0.7)
    ax1.set_ylabel('YES Price', fontsize=12)
    ax1.set_title(f'{poly_title} - YES Price', fontsize=14, fontweight='bold')
    ax1.legend(loc='best')
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim([0, 1])
    
    # NO prices
    if poly_ts:
        ax2.plot(poly_ts, poly_no, 'r-o', label=f'Polymarket {poly_ticker}', linewidth=2, markersize=4, alpha=0.7)
    if kalshi_ts:
        ax2.plot(kalshi_ts, kalshi_no, 'orange', linestyle='-', marker='s', label=f'Kalshi {kalshi_ticker}', linewidth=2, markersize=4, alpha=0.7)
    ax2.set_ylabel('NO Price', fontsize=12)
    ax2.set_xlabel('Time (UTC)', fontsize=12)
    ax2.set_title(f'{poly_title} - NO Price', fontsize=14, fontweight='bold')
    ax2.legend(loc='best')
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim([0, 1])
    
    # Format x-axis
    for ax in [ax1, ax2]:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    fig.suptitle(f'{interval_start.strftime("%Y-%m-%d %H:%M UTC")} (15min)', fontsize=14, fontweight='bold')
    
    filename = os.path.join(PLOT_DIR, f"plot_{plot_num:02d}_{interval_start.strftime('%Y%m%d_%H%M')}.png")
    plt.tight_layout()
    plt.savefig(filename, dpi=100)
    plt.close()
    
    logger.info(f"Saved: {filename}")

async def main():
    """Main test function."""
    setup_plot_dir()
    
    logger.info("Connecting to database...")
    pool = await asyncpg.create_pool(
        host=PG_HOST, port=PG_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    logger.info("Connected!\n")
    
    try:
        # Use hardcoded data range (MIN/MAX queries timeout on large table)
        logger.info("Using hardcoded data range...")
        min_ts = 1767815077116000  # 2026-01-07
        max_ts = 1770021582844001  # 2026-02-02
        
        min_dt = pd.to_datetime(min_ts, unit='us')
        max_dt = pd.to_datetime(max_ts, unit='us')
        logger.info(f"Data range: {min_dt} to {max_dt}\n")
        
        # Generate 3 random 15-min intervals
        logger.info("Generating random test intervals...")
        test_intervals = []
        for _ in range(3):
            random_us = np.random.randint(min_ts, max_ts - 15*60*1_000_000)
            dt = pd.to_datetime(random_us, unit='us')
            dt = dt.floor('15min')
            test_intervals.append(dt)
        
        test_intervals = sorted(set(test_intervals))
        logger.info(f"Selected {len(test_intervals)} intervals\n")
        
        successful = 0
        
        for i, interval in enumerate(test_intervals, 1):
            logger.info(f"\n{'='*80}")
            logger.info(f"TEST {i}: {interval.strftime('%Y-%m-%d %H:%M UTC')}")
            logger.info(f"{'='*80}\n")
            
            # Get Polymarket asset
            logger.info("[POLY] Fetching Bitcoin market...")
            asset_id = await get_polymarket_yes_asset_id(pool, interval)
            if asset_id is None:
                logger.warning("[POLY] No market found - Skipping\n")
                continue
            logger.info(f"[POLY] Asset ID: {asset_id[:40]}...\n")
            
            # Fetch Polymarket orderbook
            logger.info(f"[POLY] Fetching orderbook {interval.isoformat()} to +15min...")
            poly_records = await fetch_polymarket_orderbook_updates(pool, interval, asset_id)
            logger.info(f"[POLY] Got {len(poly_records)} records\n")
            if not poly_records:
                logger.warning("[POLY] No orderbook data - Skipping\n")
                continue
            
            poly_ts, poly_yes, poly_no = extract_prices(poly_records)
            logger.info(f"[POLY] Extracted {len(poly_ts)} price points\n")
            
            if not poly_ts:
                logger.warning("[POLY] No prices extracted - Skipping\n")
                continue
            
            # Use generic labels
            poly_title = "Bitcoin"
            poly_ticker = "POLY"
            
            # Get Kalshi ticker
            logger.info("[KALSHI] Fetching Bitcoin market...")
            kalshi_ticker = await get_kalshi_ticker(pool, interval)
            if not kalshi_ticker:
                logger.warning("[KALSHI] No market found - Skipping\n")
                continue
            logger.info(f"[KALSHI] Ticker: {kalshi_ticker}\n")
            
            # Fetch Kalshi orderbook
            logger.info(f"[KALSHI] Fetching orderbook {interval.isoformat()} to +15min...")
            kalshi_records = await fetch_kalshi_orderbook_updates(pool, interval, kalshi_ticker)
            logger.info(f"[KALSHI] Got {len(kalshi_records)} records\n")
            if not kalshi_records:
                logger.warning("[KALSHI] No orderbook data - Skipping\n")
                continue
            
            kalshi_ts, kalshi_yes, kalshi_no = extract_kalshi_prices(kalshi_records)
            logger.info(f"[KALSHI] Extracted {len(kalshi_ts)} price points\n")
            
            if not kalshi_ts:
                logger.warning("[KALSHI] No prices extracted - Skipping\n")
                continue
            
            # Plot
            logger.info("Plotting...")
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
            
            ax1.plot(poly_ts, poly_yes, 'g-o', label=f'Polymarket {poly_ticker}', linewidth=2, markersize=4, alpha=0.7)
            ax1.plot(kalshi_ts, kalshi_yes, 'b-s', label=f'Kalshi {kalshi_ticker}', linewidth=2, markersize=4, alpha=0.7)
            ax1.set_ylabel('YES Price')
            ax1.set_title(f'{poly_title} - YES Price')
            ax1.legend()
            ax1.grid(True, alpha=0.3)
            ax1.set_ylim([0, 1])
            
            ax2.plot(poly_ts, poly_no, 'r-o', label=f'Polymarket {poly_ticker}', linewidth=2, markersize=4, alpha=0.7)
            ax2.plot(kalshi_ts, kalshi_no, 'orange', linestyle='-', marker='s', label=f'Kalshi {kalshi_ticker}', linewidth=2, markersize=4, alpha=0.7)
            ax2.set_ylabel('NO Price')
            ax2.set_xlabel('Time (UTC)')
            ax2.set_title(f'{poly_title} - NO Price')
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            ax2.set_ylim([0, 1])
            
            for ax in [ax1, ax2]:
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
                plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
            
            fig.suptitle(f'{interval.strftime("%Y-%m-%d %H:%M UTC")} (15min)')
            
            filename = os.path.join(PLOT_DIR, f"plot_{i:02d}_{interval.strftime('%Y%m%d_%H%M')}.png")
            plt.tight_layout()
            plt.savefig(filename, dpi=100)
            plt.close()
            
            logger.info(f"[PLOT] Saved: {filename}\n")
            successful += 1
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Complete: {successful}/{len(test_intervals)} plots generated")
        logger.info(f"{'='*80}\n")
    
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())