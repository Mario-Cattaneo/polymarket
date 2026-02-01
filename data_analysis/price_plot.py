import os
import asyncio
import asyncpg
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import json
import time
import logging
from scipy.stats import skew, kurtosis

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Configuration ---
PG_HOST = os.getenv("PG_SOCKET", "localhost")
PG_PORT = os.getenv("POLY_PG_PORT", 5432)
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- ASSET IDs (Original Mapping, as requested by the user) ---
POLY_DEM_ASSET_ID = '83247781037352156539108067944461291821683755894607244160607042790356561625563'
POLY_REP_ASSET_ID = '65139230827417363158752884968303867495725894165574887635816574090175320800482'
KALSHI_DEM_TICKER = 'CONTROLH-2026-D'

PLOT_DIR = "price_plots"
os.makedirs(PLOT_DIR, exist_ok=True)

# --- Data Parsing & Fetching ---

def parse_polymarket_book(message):
    """
    Parses the JSON orderbook.
    Yes Price = Best Bid (Max Bid)
    No Price  = 1 - Best Ask (inverted ask price)
    """
    try:
        bids = message.get('bids', [])
        asks = message.get('asks', [])
        
        best_bid = max(float(b['price']) for b in bids) if bids else np.nan
        best_ask = min(float(a['price']) for a in asks) if asks else np.nan
        
        # The price to BUY the asset (Yes Price) is the Best Bid
        yes_price = best_bid
        # The No Price is 1 - Best Ask (inverted ask price)
        no_price = 1.0 - best_ask if not np.isnan(best_ask) else np.nan
        
        return yes_price, no_price
    except:
        return np.nan, np.nan

async def get_polymarket_data(pool, asset_id, prefix, time_range=None, time_batch_hours=24):
    """
    Fetch polymarket data in time-based batches.
    
    Args:
        time_range: tuple of (start, end) timestamps to fetch. If None or (None, None), fetches all data.
        time_batch_hours: hours per batch (default 24 = 1 day)
    """
    logger.info(f"Starting fetch for {prefix} (asset_id: {asset_id})")
    logger.info(f"Time batch size: {time_batch_hours} hours")
    
    # Handle None or (None, None) for time_range - query all data
    if time_range is None or time_range == (None, None):
        fetch_all = True
        time_range = (pd.Timestamp('1970-01-01'), pd.Timestamp('2099-12-31'))  # Dummy range for logging
    else:
        fetch_all = False
        if time_range[0] is None or time_range[1] is None:
            # If only one is None, use extreme dates for full range
            start = time_range[0] if time_range[0] is not None else pd.Timestamp('1970-01-01')
            end = time_range[1] if time_range[1] is not None else pd.Timestamp('2099-12-31')
            time_range = (start, end)
    
    logger.info(f"Fetching {'ALL data' if fetch_all else 'data in range'} for {prefix}")
    
    timestamps, yes_p, no_p = [], [], []
    batch_num = 0
    total_rows = 0
    matching_rows = 0
    asset_filtered = 0
    last_y, last_n = None, None
    samples_logged = 0
    first_batch_asset_ids = set()
    
    start_time = time.time()
    
    # Convert to microseconds for database queries
    batch_start_us = int(time_range[0].timestamp() * 1_000_000)
    batch_end_us = int(time_range[1].timestamp() * 1_000_000)
    time_batch_us = time_batch_hours * 3600 * 1_000_000
    
    current_batch_start_us = batch_start_us
    
    async with pool.acquire() as conn:
        while current_batch_start_us < batch_end_us:
            batch_fetch_start = time.time()
            current_batch_end_us = min(current_batch_start_us + time_batch_us, batch_end_us)
            
            # Fetch all records in this time window
            records = await conn.fetch("""
                SELECT asset_id, outcome, server_time_us, message
                FROM poly_book_state_house
                WHERE server_time_us >= $1 AND server_time_us < $2
                ORDER BY server_time_us ASC
            """, current_batch_start_us, current_batch_end_us)
            
            batch_fetch_time = time.time() - batch_fetch_start
            
            if not records:
                logger.info(f"{prefix}: Batch {batch_num} (time window {current_batch_start_us}-{current_batch_end_us}) returned 0 rows.")
                current_batch_start_us = current_batch_end_us
                batch_num += 1
                continue
            
            batch_num += 1
            total_rows += len(records)
            batch_matches = 0
            
            # Collect unique asset_ids from first batch for inspection
            if batch_num == 1:
                for r in records[:20]:
                    first_batch_asset_ids.add(str(r['asset_id']))
            
            for row in records:
                try:
                    msg = json.loads(row['message'])
                    
                    # Filter by asset_id INSIDE the message
                    msg_asset_id = msg.get('asset_id')
                    if msg_asset_id is None or str(msg_asset_id) != str(asset_id):
                        asset_filtered += 1
                        continue
                    
                    # Verify bids and asks exist and are well-formed
                    bids = msg.get('bids', [])
                    asks = msg.get('asks', [])
                    
                    if not bids or not asks:
                        continue
                    
                    # Yes price = max bid price, No price = 1 - min ask price
                    bid_prices = [float(b['price']) for b in bids]
                    ask_prices = [float(a['price']) for a in asks]
                    
                    y = max(bid_prices)
                    n = 1.0 - min(ask_prices)
                    

                    # Only store if prices changed
                    if last_y is None or y != last_y or n != last_n:
                        timestamps.append(pd.to_datetime(row['server_time_us'], unit='us'))
                        yes_p.append(y)
                        no_p.append(n)
                        last_y, last_n = y, n
                        matching_rows += 1
                        batch_matches += 1
                except Exception as e:
                    logger.debug(f"Error parsing orderbook for {asset_id}: {e}")
                    continue
            
            batch_start_ts = pd.to_datetime(current_batch_start_us, unit='us')
            batch_end_ts = pd.to_datetime(current_batch_end_us, unit='us')
            logger.info(f"{prefix}: Batch {batch_num} | Time: {batch_start_ts} to {batch_end_ts} | Fetched: {len(records)} rows | Asset matched: {len(records) - asset_filtered} | Price changes: {batch_matches} | Fetch time: {batch_fetch_time:.2f}s")
            
            current_batch_start_us = current_batch_end_us
    
    total_time = time.time() - start_time
    logger.info(f"{prefix}: Total batches: {batch_num} | Total rows scanned: {total_rows} | Asset filtered: {asset_filtered} | Effective data points: {matching_rows} | Total time: {total_time:.2f}s")
    
    if batch_num > 0:
        logger.info(f"{prefix}: First batch unique asset_ids found: {first_batch_asset_ids}")
    
    df = pd.DataFrame({'timestamp': timestamps, f'{prefix}_yes': yes_p, f'{prefix}_no': no_p})
    df.set_index('timestamp', inplace=True)
    # Remove duplicate timestamps, keeping the last occurrence
    df = df[~df.index.duplicated(keep='last')]
    logger.info(f"{prefix}: Final DataFrame size: {len(df)} rows (after deduplication)")
    if len(yes_p) > 0:
        logger.info(f"{prefix}: Yes price range: [{min(yes_p):.4f}, {max(yes_p):.4f}] | No price range: [{min(no_p):.4f}, {max(no_p):.4f}]")
    return df

async def get_kalshi_data(pool, ticker, prefix, time_range=None, time_batch_hours=24):
    """
    Fetch Kalshi data in time-based batches.
    
    Args:
        time_range: tuple of (start, end) timestamps to fetch. If None or (None, None), fetches all data.
        time_batch_hours: hours per batch (default 24 = 1 day)
    """
    logger.info(f"Starting fetch for {prefix} (ticker: {ticker})")
    logger.info(f"Time batch size: {time_batch_hours} hours")
    
    # Handle None or (None, None) for time_range - query all data
    if time_range is None or time_range == (None, None):
        fetch_all = True
        time_range = (pd.Timestamp('1970-01-01'), pd.Timestamp('2099-12-31'))  # Dummy range for logging
    else:
        fetch_all = False
        if time_range[0] is None or time_range[1] is None:
            # If only one is None, use extreme dates for full range
            start = time_range[0] if time_range[0] is not None else pd.Timestamp('1970-01-01')
            end = time_range[1] if time_range[1] is not None else pd.Timestamp('2099-12-31')
            time_range = (start, end)
    
    logger.info(f"Fetching {'ALL data' if fetch_all else 'data in range'} for {prefix}")
    
    timestamps, yes_p, no_p = [], [], []
    batch_num = 0
    total_rows = 0
    matching_rows = 0
    ticker_filtered = 0
    type_filtered = 0
    last_y, last_n = None, None
    samples_logged = 0
    
    start_time = time.time()
    
    # Convert to microseconds for database queries
    batch_start_us = int(time_range[0].timestamp() * 1_000_000)
    batch_end_us = int(time_range[1].timestamp() * 1_000_000)
    time_batch_us = time_batch_hours * 3600 * 1_000_000
    
    current_batch_start_us = batch_start_us
    
    async with pool.acquire() as conn:
        while current_batch_start_us < batch_end_us:
            batch_fetch_start = time.time()
            current_batch_end_us = min(current_batch_start_us + time_batch_us, batch_end_us)
            
            # Fetch all records in this time window
            records = await conn.fetch("""
                SELECT market_ticker, server_time_us, message
                FROM kalshi_orderbook_updates_house
                WHERE server_time_us >= $1 AND server_time_us < $2
                ORDER BY server_time_us ASC
            """, current_batch_start_us, current_batch_end_us)
            
            batch_fetch_time = time.time() - batch_fetch_start
            
            if not records:
                logger.info(f"{prefix}: Batch {batch_num} (time window {current_batch_start_us}-{current_batch_end_us}) returned 0 rows.")
                current_batch_start_us = current_batch_end_us
                batch_num += 1
                continue

            batch_num += 1
            total_rows += len(records)
            batch_matches = 0

            for row in records:
                # Filter by ticker and type locally
                if row['market_ticker'] != ticker:
                    ticker_filtered += 1
                    continue
                    
                try:
                    wrapper = json.loads(row['message'])
                    msg_type = wrapper.get('type')
                    
                    # Filter by type locally
                    if msg_type not in ('orderbook_snapshot', 'orderbook_delta'):
                        type_filtered += 1
                        continue
                    
                    msg = wrapper.get('msg')
                    ts = pd.to_datetime(row['server_time_us'], unit='us')

                    if msg_type == 'orderbook_snapshot':
                        yes_book = {i[0]: i[1] for i in msg.get('yes', [])}
                        no_book = {i[0]: i[1] for i in msg.get('no', [])}
                    elif msg_type == 'orderbook_delta':
                        side = msg.get('side')
                        price = msg.get('price')
                        delta = msg.get('delta')
                        target = yes_book if side == 'yes' else no_book
                        new_qty = target.get(price, 0) + delta
                        if new_qty <= 0:
                            if price in target: del target[price]
                        else:
                            target[price] = new_qty

                    best_yes = (max(yes_book.keys()) / 100.0) if yes_book else np.nan
                    best_no = (max(no_book.keys()) / 100.0) if no_book else np.nan

                    # Only store if prices changed from last recorded value
                    if last_y is None or best_yes != last_y or best_no != last_n:
                        timestamps.append(ts)
                        yes_p.append(best_yes)
                        no_p.append(best_no)
                        last_y, last_n = best_yes, best_no
                        matching_rows += 1
                        batch_matches += 1
                except Exception as e:
                    logger.debug(f"Error parsing orderbook for {ticker}: {e}")
                    continue
            
            batch_start_ts = pd.to_datetime(current_batch_start_us, unit='us')
            batch_end_ts = pd.to_datetime(current_batch_end_us, unit='us')
            logger.info(f"{prefix}: Batch {batch_num} | Time: {batch_start_ts} to {batch_end_ts} | Fetched: {len(records)} rows | Ticker matched: {len(records) - ticker_filtered} | Type matched: {len(records) - ticker_filtered - type_filtered} | Price changes: {batch_matches} | Fetch time: {batch_fetch_time:.2f}s")
            
            current_batch_start_us = current_batch_end_us
    
    total_time = time.time() - start_time
    logger.info(f"{prefix}: Total batches: {batch_num} | Total rows scanned: {total_rows} | Ticker filtered: {ticker_filtered} | Type filtered: {type_filtered} | Effective data points: {matching_rows} | Total time: {total_time:.2f}s")
    
    df = pd.DataFrame({'timestamp': timestamps, f'{prefix}_yes': yes_p, f'{prefix}_no': no_p})
    df.set_index('timestamp', inplace=True)
    # Remove duplicate timestamps, keeping the last occurrence
    df = df[~df.index.duplicated(keep='last')]
    logger.info(f"{prefix}: Final DataFrame size: {len(df)} rows (after deduplication)")
    if len(yes_p) > 0:
        logger.info(f"{prefix}: Yes price range: [{min(yes_p):.4f}, {max(yes_p):.4f}] | No price range: [{min(no_p):.4f}, {max(no_p):.4f}]")
    return df

# --- Analysis & Plotting ---

def downsample_for_plotting(df, max_points=50000):
    """Downsample dataframe to max_points for plotting to avoid rendering issues."""
    if len(df) <= max_points:
        return df
    
    factor = len(df) // max_points
    logger.info(f"Downsampling from {len(df)} to ~{len(df) // factor} points (factor: {factor})")
    return df.iloc[::factor]

def calculate_stats(series, name):
    clean = series.dropna()
    if clean.empty:
        print(f"--- {name}: No Data ---")
        return

    mu = clean.mean()
    var = clean.var()
    sk = skew(clean)
    ku = kurtosis(clean)
    pcts = np.percentile(clean, [0.1, 10, 33, 66, 90, 99.9])

    print(f"\n--- Statistics: {name} ---")
    print(f"Mean:     {mu:.6f}")
    print(f"Variance: {var:.6f}")
    print(f"Skewness: {sk:.6f}")
    print(f"Kurtosis: {ku:.6f}")
    print(f"Percentiles [0.1, 10, 33, 66, 90, 99.9]:")
    print(f"  {pcts}")

def plot_market_raw(df, yes_col, no_col, title, filename, time_range=None):
    """Plots Yes and No prices for a single market, extended to full time range if provided."""
    # If time_range provided and not full range, ensure data covers full range by forward-filling
    if time_range is not None and time_range != (None, None) and len(df) > 0:
        start_time, end_time = time_range
        last_idx = df.index[-1]
        
        # Only extend if last data point is before end of range
        if last_idx < end_time:
            # Add final point at end_time with same values as last point
            final_row = df.iloc[-1:].copy()
            final_row.index = [end_time]
            df = pd.concat([df, final_row])
    
    # Downsample for plotting to avoid rendering issues
    df_plot = downsample_for_plotting(df, max_points=50000)
    logger.info(f"Plotting {title}: {len(df_plot)} points (downsampled from {len(df)})")
    
    plt.figure(figsize=(12, 6))
    plt.plot(df_plot.index, df_plot[yes_col], drawstyle='steps-post', color='green', label='Yes', linewidth=1.5)
    plt.plot(df_plot.index, df_plot[no_col], drawstyle='steps-post', color='red', label='No', linewidth=1.5, linestyle='--')
    
    # We add a subtle fill to clearly show the spread for debugging, 
    # but the primary plot lines are the steps
    plt.fill_between(df_plot.index, df_plot[yes_col], df_plot[no_col], color='gray', alpha=0.1)
    
    plt.title(title, fontsize=14)
    plt.legend(loc='upper left')
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.ylim(-0.05, 1.05)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    path = os.path.join(PLOT_DIR, filename)
    plt.savefig(path)
    plt.close()
    print(f"Saved plot: {path}")

def plot_difference(df, col_name, title, filename):
    """Plots a difference series."""
    # Downsample for plotting to avoid rendering issues
    df_plot = downsample_for_plotting(df, max_points=50000)
    logger.info(f"Plotting {title}: {len(df_plot)} points (downsampled from {len(df)})")
    
    plt.figure(figsize=(12, 6))
    plt.plot(df_plot.index, df_plot[col_name], drawstyle='steps-post', color='purple', linewidth=1.5)
    plt.axhline(0, color='black', linewidth=1, linestyle='-')
    
    plt.title(title, fontsize=14)
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    path = os.path.join(PLOT_DIR, filename)
    plt.savefig(path)
    plt.close()
    print(f"Saved plot: {path}")

async def main(time_range=None, debug=False):
    """
    Main function to fetch and plot market data.
    
    Args:
        time_range: tuple of (start_datetime, end_datetime) for filtering data.
                   If None or (None, None), fetches all available data.
        debug: if True, sample asset IDs in database before fetching
    """
    # Default: Jan 21 to Jan 22, 2026 (1 day)
    if time_range is None:
        time_range = (
            pd.Timestamp('2026-01-21 00:00:00'),
            pd.Timestamp('2026-01-22 00:00:00')
        )
    
    # Log time range
    if time_range == (None, None):
        logger.info("Using time range: ALL DATA")
    else:
        logger.info(f"Using time range: {time_range[0]} to {time_range[1]}")
    
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST]):
        print("Error: DB credentials missing.")
        return

    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    print("DB Connected.")
    
    # Debug: check what asset IDs exist in the time range
    if debug:
        async with pool.acquire() as conn:
            batch_start_us = int(time_range[0].timestamp() * 1_000_000)
            batch_end_us = int(time_range[1].timestamp() * 1_000_000)
            sample_records = await conn.fetch("""
                SELECT DISTINCT asset_id FROM poly_book_state_house
                WHERE server_time_us >= $1 AND server_time_us < $2
                LIMIT 20
            """, batch_start_us, batch_end_us)
            logger.info(f"DEBUG: Sample asset_ids in database for time range:")
            for r in sample_records:
                logger.info(f"  - {r['asset_id']}")
            logger.info(f"Expected DEM asset_id: {POLY_DEM_ASSET_ID}")
            logger.info(f"Expected REP asset_id: {POLY_REP_ASSET_ID}")

    # 1. Fetch Data
    print("Fetching data...")
    t1 = get_polymarket_data(pool, POLY_DEM_ASSET_ID, 'poly_dem', time_range=time_range)
    t2 = get_polymarket_data(pool, POLY_REP_ASSET_ID, 'poly_rep', time_range=time_range)
    t3 = get_kalshi_data(pool, KALSHI_DEM_TICKER, 'kalshi_dem', time_range=time_range)
    
    df_pd, df_pr, df_kd = await asyncio.gather(t1, t2, t3)
    await pool.close()

    # 2. Plot Raw Markets (3 Plots)
    plot_market_raw(df_pd, 'poly_dem_yes', 'poly_dem_no', 'Polymarket Democrats (Raw)', '1_market_poly_dem.png', time_range=time_range)
    plot_market_raw(df_pr, 'poly_rep_yes', 'poly_rep_no', 'Polymarket Republicans (Raw)', '2_market_poly_rep.png', time_range=time_range)
    plot_market_raw(df_kd, 'kalshi_dem_yes', 'kalshi_dem_no', 'Kalshi Democrats (Raw)', '3_market_kalshi_dem.png', time_range=time_range)

    # 3. Align Data (State of the World)
    print("Aligning timelines...")
    df_all = pd.concat([df_pd, df_pr, df_kd], axis=1)
    df_all.sort_index(inplace=True)
    df_all.ffill(inplace=True)
    df_all.dropna(inplace=True)
    logger.info(f"After alignment: {len(df_all)} rows")

    # 4. Calculate Price Differences (7 Plots)
    
    # A) Internal Arbitrage: 1 - (yes + no) for each market
    df_all['diff_internal_poly_dem'] = 1.0 - (df_all['poly_dem_yes'] + df_all['poly_dem_no'])
    df_all['diff_internal_poly_rep'] = 1.0 - (df_all['poly_rep_yes'] + df_all['poly_rep_no'])
    df_all['diff_internal_kalshi_dem'] = 1.0 - (df_all['kalshi_dem_yes'] + df_all['kalshi_dem_no'])
    
    # B) Poly Complementary Arbitrage
    df_all['diff_complementary_no'] = 1.0 - (df_all['poly_dem_no'] + df_all['poly_rep_no'])
    df_all['diff_complementary_yes'] = 1.0 - (df_all['poly_dem_yes'] + df_all['poly_rep_yes'])
    
    # C) Mirror Arbitrage (Poly vs Kalshi) - absolute difference
    df_all['diff_mirror_yes'] = abs(df_all['poly_dem_yes'] - df_all['kalshi_dem_yes'])
    df_all['diff_mirror_no'] = abs(df_all['poly_dem_no'] - df_all['kalshi_dem_no'])

    # 5. Stats & Plots for Differences
    diffs = [
        ('diff_internal_poly_dem', 'Internal Arb: Poly Dem (1 - Yes - No)', '4_diff_internal_poly_dem.png'),
        ('diff_internal_poly_rep', 'Internal Arb: Poly Rep (1 - Yes - No)', '5_diff_internal_poly_rep.png'),
        ('diff_internal_kalshi_dem', 'Internal Arb: Kalshi Dem (1 - Yes - No)', '6_diff_internal_kalshi_dem.png'),
        ('diff_complementary_no', 'Poly Complementary Arb: |No Dem + No Rep - 1|', '7_diff_complementary_no.png'),
        ('diff_complementary_yes', 'Poly Complementary Arb: |Yes Dem + Yes Rep - 1|', '8_diff_complementary_yes.png'),
        ('diff_mirror_yes', 'Mirror Arb: |Yes Poly Dem - Yes Kalshi|', '9_diff_mirror_yes.png'),
        ('diff_mirror_no', 'Mirror Arb: |No Poly Dem - No Kalshi|', '10_diff_mirror_no.png')
    ]

    for col, title, fname in diffs:
        calculate_stats(df_all[col], title)
        plot_difference(df_all, col, title, fname)

if __name__ == "__main__":
    # To use full range, pass: time_range=(None, None) or modify the condition
    # To use custom range, pass: time_range=(start_datetime, end_datetime)
    # Default is Jan 21-22, 2026
    # Set debug=True to see what asset_ids are actually in the database
    asyncio.run(main(time_range=(None, None), debug=False))