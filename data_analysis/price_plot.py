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
    Parses the JSON orderbook EXACTLY as requested (No 1-P inversion).
    Yes Price = Best Bid (Max Bid)
    No Price  = Best Ask (Min Ask)
    """
    try:
        bids = message.get('bids', [])
        asks = message.get('asks', [])
        
        best_bid = max(float(b['price']) for b in bids) if bids else np.nan
        best_ask = min(float(a['price']) for a in asks) if asks else np.nan
        
        # The price to BUY the asset (Yes Price) is the Best Bid
        yes_price = best_bid
        # The price to SELL the asset (No Price / Price to Buy 'No') is the Best Ask
        no_price = best_ask
        
        return yes_price, no_price
    except:
        return np.nan, np.nan

async def get_polymarket_data(pool, asset_id, prefix):
    logger.info(f"Starting fetch for {prefix} (asset_id: {asset_id})")
    logger.info(f"Asset ID type: {type(asset_id)}, value length: {len(str(asset_id))}")
    
    timestamps, yes_p, no_p = [], [], []
    batch_size = 100000
    batch_num = 0
    total_rows = 0
    matching_rows = 0
    asset_filtered = 0
    last_y, last_n = None, None
    last_time_us = 0
    samples_logged = 0
    first_batch_asset_ids = set()
    
    start_time = time.time()
    
    async with pool.acquire() as conn:
        while True:
            batch_start = time.time()
            # Use keyset pagination (WHERE server_time_us > last_time_us) instead of OFFSET
            records = await conn.fetch("""
                SELECT asset_id, outcome, server_time_us, message
                FROM poly_book_state_house
                WHERE server_time_us > $1
                ORDER BY server_time_us ASC
                LIMIT $2;
            """, last_time_us, batch_size)
            
            batch_fetch_time = time.time() - batch_start
            
            if not records:
                logger.info(f"{prefix}: Batch {batch_num} returned 0 rows. Fetch complete.")
                break
            
            batch_num += 1
            total_rows += len(records)
            batch_matches = 0
            
            # Collect unique asset_ids from this batch for inspection
            if batch_num == 1:
                for r in records[:20]:
                    first_batch_asset_ids.add(str(r['asset_id']))
            
            for row in records:
                try:
                    msg = json.loads(row['message'])
                    
                    # Filter by asset_id INSIDE the message, not the database column
                    msg_asset_id = msg.get('asset_id')
                    if msg_asset_id is None or str(msg_asset_id) != str(asset_id):
                        asset_filtered += 1
                        continue
                    
                    # Verify bids and asks exist and are well-formed
                    bids = msg.get('bids', [])
                    asks = msg.get('asks', [])
                    
                    if not bids or not asks:
                        continue
                    
                    # Yes price = max bid price (highest price someone will pay to buy)
                    bid_prices = [float(b['price']) for b in bids]
                    ask_prices = [float(a['price']) for a in asks]
                    
                    y = max(bid_prices)
                    n = min(ask_prices)
                    
                    # Log first 5 samples with FULL message structure
                    if samples_logged < 5:
                        msg_asset_id = msg.get('asset_id', 'N/A')
                        logger.info(f"{prefix} SAMPLE {samples_logged + 1}: DB asset_id={row['asset_id']} | JSON asset_id={msg_asset_id} | outcome={row['outcome']}")
                        logger.info(f"{prefix} SAMPLE {samples_logged + 1} Prices: yes={y:.4f}, no={n:.4f}")
                        samples_logged += 1
                    
                    # Only store if prices changed from last recorded value
                    if last_y is None or y != last_y or n != last_n:
                        timestamps.append(pd.to_datetime(row['server_time_us'], unit='us'))
                        yes_p.append(y)
                        no_p.append(n)
                        last_y, last_n = y, n
                        matching_rows += 1
                        batch_matches += 1
                    last_time_us = row['server_time_us']
                except Exception as e:
                    logger.debug(f"Error parsing orderbook for {asset_id}: {e}")
                    continue
            
            logger.info(f"{prefix}: Batch {batch_num} | Fetched: {len(records)} rows | Asset matched: {len(records) - asset_filtered} | Price changes: {batch_matches} | Time: {batch_fetch_time:.2f}s")
    
    total_time = time.time() - start_time
    logger.info(f"{prefix}: Total rows scanned: {total_rows} | Asset filtered: {asset_filtered} | Effective data points: {matching_rows} | Total time: {total_time:.2f}s")
    
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

async def get_kalshi_data(pool, ticker, prefix):
    logger.info(f"Starting fetch for {prefix} (ticker: {ticker})")
    
    timestamps, yes_p, no_p = [], [], []
    yes_book, no_book = {}, {}
    batch_size = 100000
    batch_num = 0
    total_rows = 0
    matching_rows = 0
    ticker_filtered = 0
    type_filtered = 0
    last_y, last_n = None, None
    last_time_us = 0
    samples_logged = 0
    
    start_time = time.time()
    
    async with pool.acquire() as conn:
        while True:
            batch_start = time.time()
            # Use keyset pagination (WHERE server_time_us > last_time_us) instead of OFFSET
            records = await conn.fetch("""
                SELECT market_ticker, server_time_us, message
                FROM kalshi_orderbook_updates_house
                WHERE server_time_us > $1
                ORDER BY server_time_us ASC
                LIMIT $2;
            """, last_time_us, batch_size)
            
            batch_fetch_time = time.time() - batch_start
            
            if not records:
                logger.info(f"{prefix}: Batch {batch_num} returned 0 rows. Fetch complete.")
                break

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

                    # Log first 10 samples for verification
                    if samples_logged < 10:
                        logger.info(f"{prefix} SAMPLE {samples_logged + 1}: msg_type={msg_type}, yes_book_keys={len(yes_book)}, no_book_keys={len(no_book)}, y={best_yes}, n={best_no}")
                        samples_logged += 1

                    # Only store if prices changed from last recorded value
                    if last_y is None or best_yes != last_y or best_no != last_n:
                        timestamps.append(ts)
                        yes_p.append(best_yes)
                        no_p.append(best_no)
                        last_y, last_n = best_yes, best_no
                        matching_rows += 1
                        batch_matches += 1
                    last_time_us = row['server_time_us']
                except Exception as e:
                    logger.debug(f"Error parsing orderbook for {ticker}: {e}")
                    continue
            
            logger.info(f"{prefix}: Batch {batch_num} | Fetched: {len(records)} rows | Ticker matched: {len(records) - ticker_filtered} | Type matched: {len(records) - ticker_filtered - type_filtered} | Price changes: {batch_matches} | Time: {batch_fetch_time:.2f}s")
    
    total_time = time.time() - start_time
    logger.info(f"{prefix}: Total rows scanned: {total_rows} | Ticker filtered: {ticker_filtered} | Type filtered: {type_filtered} | Effective data points: {matching_rows} | Total time: {total_time:.2f}s")
    
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

def plot_market_raw(df, yes_col, no_col, title, filename):
    """Plots Yes (Bid) and No (Ask) for a single market (No Fill)."""
    # Downsample for plotting to avoid rendering issues
    df_plot = downsample_for_plotting(df, max_points=50000)
    logger.info(f"Plotting {title}: {len(df_plot)} points (downsampled from {len(df)})")
    
    plt.figure(figsize=(12, 6))
    plt.plot(df_plot.index, df_plot[yes_col], drawstyle='steps-post', color='green', label='Yes (Bid)', linewidth=1.5)
    plt.plot(df_plot.index, df_plot[no_col], drawstyle='steps-post', color='red', label='No (Ask)', linewidth=1.5, linestyle='--')
    
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

async def main():
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST]):
        print("Error: DB credentials missing.")
        return

    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    print("DB Connected.")

    # 1. Fetch Data
    print("Fetching data...")
    t1 = get_polymarket_data(pool, POLY_DEM_ASSET_ID, 'poly_dem')
    t2 = get_polymarket_data(pool, POLY_REP_ASSET_ID, 'poly_rep')
    t3 = get_kalshi_data(pool, KALSHI_DEM_TICKER, 'kalshi_dem')
    
    df_pd, df_pr, df_kd = await asyncio.gather(t1, t2, t3)
    await pool.close()

    # 2. Plot Raw Markets (3 Plots)
    plot_market_raw(df_pd, 'poly_dem_yes', 'poly_dem_no', 'Polymarket Democrats (Raw)', '1_market_poly_dem.png')
    plot_market_raw(df_pr, 'poly_rep_yes', 'poly_rep_no', 'Polymarket Republicans (Raw)', '2_market_poly_rep.png')
    plot_market_raw(df_kd, 'kalshi_dem_yes', 'kalshi_dem_no', 'Kalshi Democrats (Raw)', '3_market_kalshi_dem.png')

    # 3. Align Data (State of the World)
    print("Aligning timelines...")
    df_all = pd.concat([df_pd, df_pr, df_kd], axis=1)
    df_all.sort_index(inplace=True)
    df_all.ffill(inplace=True)
    df_all.dropna(inplace=True)

    # 4. Calculate Differences (5 Plots)
    
    # Internal Differences (Spread: Yes - No)
    df_all['diff_internal_poly_dem'] = df_all['poly_dem_yes'] - df_all['poly_dem_no']
    df_all['diff_internal_poly_rep'] = df_all['poly_rep_yes'] - df_all['poly_rep_no']
    df_all['diff_internal_kalshi_dem'] = df_all['kalshi_dem_yes'] - df_all['kalshi_dem_no']
    
    # Mirror Difference (Arbitrage: Poly Dem Yes - Kalshi Dem Yes)
    df_all['diff_mirror_dem'] = df_all['poly_dem_yes'] - df_all['kalshi_dem_yes']
    
    # Complementary Difference (Poly Dem Yes - Poly Rep No)
    df_all['diff_complementary'] = df_all['poly_dem_yes'] - df_all['poly_rep_no']

    # 5. Stats & Plots for Differences
    diffs = [
        ('diff_internal_poly_dem', 'Internal Spread: Poly Dem (Yes - No)', '4_diff_internal_poly_dem.png'),
        ('diff_internal_poly_rep', 'Internal Spread: Poly Rep (Yes - No)', '5_diff_internal_poly_rep.png'),
        ('diff_internal_kalshi_dem', 'Internal Spread: Kalshi Dem (Yes - No)', '6_diff_internal_kalshi_dem.png'),
        ('diff_mirror_dem', 'Mirror Arb: Poly Dem Yes - Kalshi Dem Yes', '7_diff_mirror_dem.png'),
        ('diff_complementary', 'Complementary: Poly Dem Yes - Poly Rep No', '8_diff_complementary.png')
    ]

    for col, title, fname in diffs:
        calculate_stats(df_all[col], title)
        plot_difference(df_all, col, title, fname)

if __name__ == "__main__":
    asyncio.run(main())