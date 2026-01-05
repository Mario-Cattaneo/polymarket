import os
import asyncio
import asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging
import time
from datetime import datetime, timezone, timedelta
import numpy as np
from hexbytes import HexBytes
from eth_abi import decode as abi_decode

# ----------------------------------------------------------------
# 1. SETUP & CONFIGURATION
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

# --- Database Credentials ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- Configuration ---
POLY_CSV_DIR = os.getenv("POLY_CSV")
POLY_CSV_PATH = os.path.join(POLY_CSV_DIR, "gamma_markets.csv") if POLY_CSV_DIR else "gamma_markets.csv"

# RequestPrice Event Definition
REQUEST_PRICE_EVENT = {
    "signature": "RequestPrice(address,bytes32,uint256,bytes,address,uint256,uint256)",
    "indexed_types": ["address"],
    "data_types": ["bytes32", "uint256", "bytes", "address", "uint256", "uint256"],
    "arg_names": ["requester", "identifier", "timestamp", "ancillaryData", "currency", "reward", "finalFee"]
}

# ----------------------------------------------------------------
# 2. DATA FETCHING & LOADING
# ----------------------------------------------------------------

def decode_argument(arg_type, value):
    """Decodes individual arguments based on their type."""
    if arg_type == "bytes32":
        return HexBytes(value).hex()
    elif arg_type == "uint256":
        return int(value)
    elif arg_type == "bytes":
        return HexBytes(value).hex()
    elif arg_type == "address":
        return "0x" + HexBytes(value)[-20:].hex()
    return value

async def fetch_request_price_events(pool):
    """Fetch and decode RequestPrice events from both tables."""
    logger.info("Fetching RequestPrice events from both oov2 and events_managed_oracle...")
    
    all_events = []
    
    for table_name in ['oov2', 'events_managed_oracle']:
        query = f'''
            SELECT block_number, timestamp_ms, data, topics
            FROM "{table_name}"
            WHERE event_name = 'RequestPrice'
            ORDER BY block_number
        '''
        
        try:
            async with pool.acquire() as conn:
                start = time.time()
                records = await conn.fetch(query)
                elapsed = time.time() - start
                logger.info(f"Found {len(records):,} RequestPrice events in {table_name} ({elapsed:.2f}s)")
                
                for record in records:
                    try:
                        block_number = record['block_number']
                        block_timestamp = record['timestamp_ms']
                        topics = record['topics']
                        data_hex = record['data']
                        
                        if not topics or len(topics) < 2:
                            continue
                        
                        indexed_args_raw = [HexBytes(t) for t in topics[1:]]
                        data_bytes = HexBytes(data_hex)
                        
                        decoded_data_args = abi_decode(REQUEST_PRICE_EVENT['data_types'], data_bytes)
                        decoded_indexed_args = [
                            abi_decode([dtype], val)[0] 
                            for dtype, val in zip(REQUEST_PRICE_EVENT['indexed_types'], indexed_args_raw)
                        ]
                        
                        # Reconstruct full argument list
                        all_args = []
                        indexed_args_iter = iter(decoded_indexed_args)
                        data_args_iter = iter(decoded_data_args)
                        
                        for i, arg_name in enumerate(REQUEST_PRICE_EVENT['arg_names']):
                            if i < len(REQUEST_PRICE_EVENT['indexed_types']):
                                all_args.append(next(indexed_args_iter))
                            else:
                                all_args.append(next(data_args_iter))
                        
                        # Extract event data
                        event_data = {}
                        all_types = REQUEST_PRICE_EVENT['indexed_types'] + REQUEST_PRICE_EVENT['data_types']
                        for name, type_str, value in zip(REQUEST_PRICE_EVENT['arg_names'], all_types, all_args):
                            event_data[name] = value
                        
                        all_events.append({
                            'table': table_name,
                            'block_number': block_number,
                            'block_timestamp': block_timestamp,
                            'event_timestamp': event_data.get('timestamp', 0),
                        })
                    
                    except Exception as e:
                        logger.debug(f"Failed to decode event from {table_name}: {e}")
                        continue
        
        except Exception as e:
            logger.error(f"Error querying {table_name}: {e}")
    
    return all_events

def load_market_data(csv_path):
    """Loads and processes market data from the CSV."""
    logger.info(f"Loading market data from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, low_memory=False, usecols=['id', 'createdAt', 'acceptingOrdersTimestamp', 'startDate'])
        df['id'] = df['id'].astype(str)
        
        # Parse timestamps with mixed format support
        df['createdAt_dt'] = pd.to_datetime(df['createdAt'], format='mixed', utc=True)
        df['acceptingOrdersTimestamp_dt'] = pd.to_datetime(df['acceptingOrdersTimestamp'], format='mixed', utc=True, errors='coerce')
        df['startDate_dt'] = pd.to_datetime(df['startDate'], format='mixed', utc=True, errors='coerce')
        
        logger.info(f"Loaded {len(df):,} markets from CSV.")
        return df
    
    except FileNotFoundError:
        logger.error(f"CSV file not found at {csv_path}.")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading CSV: {e}")
        return pd.DataFrame()

# ----------------------------------------------------------------
# 3. ANALYSIS & PLOTTING HELPERS
# ----------------------------------------------------------------

def format_timedelta(seconds):
    """Helper to format seconds into a readable timedelta string."""
    if pd.isna(seconds): return "N/A"
    td = timedelta(seconds=abs(seconds))
    sign = "-" if seconds < 0 else ""
    hours, remainder = divmod(td.seconds, 3600)
    minutes, seconds_part = divmod(remainder, 60)
    return f"{sign}{hours:02d}:{minutes:02d}:{seconds_part:02d}"

def prepare_cdf_data(df, time_column, label):
    """Prepares data for CDF plotting - returns datetime, count, label."""
    if df.empty:
        return pd.DataFrame(columns=['datetime', 'count', 'label'])
    
    if not pd.api.types.is_datetime64_any_dtype(df[time_column]):
        df['datetime'] = pd.to_datetime(df[time_column], unit='ms', utc=True)
    else:
        df['datetime'] = df[time_column]

    df_sorted = df.sort_values('datetime')
    df_sorted['count'] = range(1, len(df_sorted) + 1)
    df_sorted['label'] = label
    
    return df_sorted[['datetime', 'count', 'label']]

def analyze_timing_custom(df_matched_all, event_ts_col, market_ts_col, timestamp_name):
    """Analyzes timing difference between RequestPrice event (T1) and market timestamp (T2)."""
    if df_matched_all.empty:
        logger.info(f"No matched data for {timestamp_name} timing analysis.")
        return None, None, None
    
    df_matched_all = df_matched_all.copy()
    df_matched_all['event_ts_dt'] = pd.to_datetime(df_matched_all[event_ts_col], unit='s', utc=True)
    
    # Calculate difference: Delta T = T2 - T1 (Market - RequestPrice Event)
    df_matched_all['time_diff'] = df_matched_all[market_ts_col] - df_matched_all['event_ts_dt']
    df_matched_all['time_diff_sec'] = df_matched_all['time_diff'].dt.total_seconds()
    
    total_count = len(df_matched_all)
    
    # Calculate Probabilities
    count_t2_gt_t1 = (df_matched_all['time_diff_sec'] > 0).sum()
    count_t2_eq_t1 = (df_matched_all['time_diff_sec'] == 0).sum()
    count_t2_lt_t1 = (df_matched_all['time_diff_sec'] < 0).sum()
    
    p_t2_gt_t1 = (count_t2_gt_t1 / total_count) * 100
    p_t2_eq_t1 = (count_t2_eq_t1 / total_count) * 100
    p_t2_lt_t1 = (count_t2_lt_t1 / total_count) * 100
    
    avg_diff_sec = df_matched_all['time_diff_sec'].mean()
    std_diff_sec = df_matched_all['time_diff_sec'].std()
    min_diff_sec = df_matched_all['time_diff_sec'].min()
    max_diff_sec = df_matched_all['time_diff_sec'].max()
    
    logger.info(f"\n--- Timing Analysis ({timestamp_name}: Market T2 - RequestPrice T1) ---")
    print(f"Total Matched Events: {total_count:,}")
    print(f"\n--- Probability Analysis ---")
    print(f"P(T2 > T1) [Market AFTER RequestPrice]: {p_t2_gt_t1:.2f}% ({count_t2_gt_t1:,} events)")
    print(f"P(T2 = T1) [Market AT RequestPrice]: {p_t2_eq_t1:.2f}% ({count_t2_eq_t1:,} events)")
    print(f"P(T2 < T1) [Market BEFORE RequestPrice]: {p_t2_lt_t1:.2f}% ({count_t2_lt_t1:,} events)")
    
    print(f"\n--- Range, Mean and Standard Deviation ---")
    print(f"Min Difference: {format_timedelta(min_diff_sec)} ({min_diff_sec:.2f} seconds)")
    print(f"Max Difference: {format_timedelta(max_diff_sec)} ({max_diff_sec:.2f} seconds)")
    print(f"E(T2 - T1) [Average Delay]: {format_timedelta(avg_diff_sec)} ({avg_diff_sec:.2f} seconds)")
    print(f"StdDev(T2 - T1): {format_timedelta(std_diff_sec)} ({std_diff_sec:.2f} seconds)")
    logger.info("------------------------------------------------------------------")
    
    summary_text = (
        f"Total Events: {total_count:,}\n"
        f"P(T2 > T1): {p_t2_gt_t1:.2f}%\n"
        f"P(T2 = T1): {p_t2_eq_t1:.0f}%\n"
        f"P(T2 < T1): {p_t2_lt_t1:.2f}%\n"
        f"Avg Delay: {format_timedelta(avg_diff_sec)}\n"
        f"StdDev: {format_timedelta(std_diff_sec)}"
    )
    
    return df_matched_all['time_diff_sec'], summary_text, df_matched_all

def plot_pmf_overlaid(df_data, title, x_label, y_label, output_filename, lower_bound=None, upper_bound=None, bin_width_sec=60):
    """Creates stacked PMF histogram showing all events combined."""
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    all_time_diffs = df_data['time_diff_sec'].values
    if len(all_time_diffs) == 0:
        logger.warning("No data to plot")
        return fig
    
    if lower_bound is None or upper_bound is None:
        lb = all_time_diffs.min() if lower_bound is None else lower_bound
        ub = all_time_diffs.max() if upper_bound is None else upper_bound
    else:
        lb, ub = lower_bound, upper_bound
    
    data_range = ub - lb
    num_bins = max(20, int(np.ceil(data_range / bin_width_sec)))
    bins = np.linspace(lb, ub, num_bins + 1)
    
    # Plot histogram
    counts, _ = np.histogram(all_time_diffs, bins=bins)
    pmf = counts / len(all_time_diffs)
    
    bin_centers = (bins[:-1] + bins[1:]) / 2
    bin_width = bins[1] - bins[0]
    
    ax.bar(bin_centers, pmf, width=bin_width * 0.92, 
           color='#1f77b4', alpha=0.85, edgecolor='black', linewidth=0.5)
    
    # Reference lines
    ax.axvline(0, color='black', linestyle='-', linewidth=2.5, label='Zero Delay', alpha=0.8, zorder=10)
    mean_delay = df_data['time_diff_sec'].mean()
    ax.axvline(mean_delay, color='red', linestyle='--', linewidth=2, label=f'Mean: {mean_delay:.1f}s', alpha=0.8, zorder=10)
    
    # Styling
    ax.set_yscale('log')
    ax.set_xlabel(x_label, fontsize=13, fontweight='bold')
    ax.set_ylabel(y_label, fontsize=13, fontweight='bold')
    ax.set_title(title, fontsize=15, fontweight='bold', pad=20)
    ax.grid(True, which='major', alpha=0.25, linestyle='--', axis='y', zorder=0)
    ax.grid(True, which='minor', alpha=0.1, linestyle=':', axis='y', zorder=0)
    
    ax.legend(fontsize=11, loc='upper right', framealpha=0.98, fancybox=True, shadow=True)
    ax.set_xlim(lb, ub)
    
    ax.set_facecolor('#f8f9fa')
    fig.patch.set_facecolor('white')
    
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300, bbox_inches='tight', facecolor='white')
    logger.info(f"PMF Plot saved to {output_filename}")
    return fig

# ----------------------------------------------------------------
# 4. MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    # 1. Load CSV Data
    df_markets = load_market_data(POLY_CSV_PATH)
    if df_markets.empty:
        return
    
    logger.info(f"Total Markets in CSV: {len(df_markets):,}")
    
    # 2. Database Connection
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Connected to database.")
    
    try:
        # 3. Fetch RequestPrice events
        request_price_events = await fetch_request_price_events(pool)
        
        if not request_price_events:
            logger.error("No RequestPrice events found")
            return
        
        df_events = pd.DataFrame(request_price_events)
        logger.info(f"Total RequestPrice events: {len(df_events):,}")
        logger.info(f"Block number range: {df_events['block_number'].min()} to {df_events['block_number'].max()}")
        logger.info(f"Event timestamp range: {df_events['event_timestamp'].min()} to {df_events['event_timestamp'].max()}")
        
        # 4. Convert timestamps to datetime
        df_events['block_timestamp_dt'] = pd.to_datetime(df_events['block_timestamp'], unit='ms', utc=True)
        df_events['event_timestamp_dt'] = pd.to_datetime(df_events['event_timestamp'], unit='s', utc=True)
        
        # 5. Determine time range from RequestPrice events
        start_dt = df_events['event_timestamp_dt'].min()
        end_dt = df_events['event_timestamp_dt'].max()
        
        logger.info(f"Time range from RequestPrice: {start_dt.strftime('%Y-%m-%d %H:%M:%S')} to {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 6. Prepare CDF Data for RequestPrice (3 panels)
        plot_data = {}
        
        # Panel 1: RequestPrice Block Timestamp
        plot_data['RequestPrice Block Timestamp'] = prepare_cdf_data(
            df_events, 
            'block_timestamp_dt', 
            'RequestPrice Block Timestamp'
        )
        
        # Panel 2: RequestPrice Event Timestamp (argument)
        plot_data['RequestPrice Event Timestamp'] = prepare_cdf_data(
            df_events, 
            'event_timestamp_dt', 
            'RequestPrice Event Timestamp'
        )
        
        # Panel 3: Market timestamps (filtered by time range)
        plot_data_all = {}
        
        if 'createdAt_dt' in df_markets.columns:
            df_mkts = df_markets[['createdAt_dt']].copy()
            df_mkts = df_mkts[df_mkts['createdAt_dt'].notna()]
            df_mkts_filtered = df_mkts[(df_mkts['createdAt_dt'] >= start_dt) & (df_mkts['createdAt_dt'] <= end_dt)]
            if len(df_mkts_filtered) > 0:
                plot_data_all['Market createdAt'] = prepare_cdf_data(df_mkts_filtered, 'createdAt_dt', 'Market createdAt')
                logger.info(f"Market createdAt in time range: {len(df_mkts_filtered):,}")
        
        if 'acceptingOrdersTimestamp_dt' in df_markets.columns:
            df_mkts = df_markets[['acceptingOrdersTimestamp_dt']].copy()
            df_mkts = df_mkts[df_mkts['acceptingOrdersTimestamp_dt'].notna()]
            df_mkts_filtered = df_mkts[(df_mkts['acceptingOrdersTimestamp_dt'] >= start_dt) & (df_mkts['acceptingOrdersTimestamp_dt'] <= end_dt)]
            if len(df_mkts_filtered) > 0:
                plot_data_all['Market acceptingOrdersTimestamp'] = prepare_cdf_data(df_mkts_filtered, 'acceptingOrdersTimestamp_dt', 'Market acceptingOrdersTimestamp')
                logger.info(f"Market acceptingOrdersTimestamp in time range: {len(df_mkts_filtered):,}")
        
        if 'startDate_dt' in df_markets.columns:
            df_mkts = df_markets[['startDate_dt']].copy()
            df_mkts = df_mkts[df_mkts['startDate_dt'].notna()]
            df_mkts_filtered = df_mkts[(df_mkts['startDate_dt'] >= start_dt) & (df_mkts['startDate_dt'] <= end_dt)]
            if len(df_mkts_filtered) > 0:
                plot_data_all['Market startDate'] = prepare_cdf_data(df_mkts_filtered, 'startDate_dt', 'Market startDate')
                logger.info(f"Market startDate in time range: {len(df_mkts_filtered):,}")
        
        # 7. Plot CDF - single combined plot
        logger.info("Generating CDF Plot...")
        plt.figure(figsize=(14, 8))
        
        styles = {
            'RequestPrice Block Timestamp': {'color': '#1f77b4', 'linewidth': 2.5, 'linestyle': '-', 'zorder': 3},
            'RequestPrice Event Timestamp': {'color': '#ff7f0e', 'linewidth': 2.5, 'linestyle': '--', 'zorder': 3},
            'Market createdAt': {'color': '#2ca02c', 'linewidth': 2, 'linestyle': '-.', 'zorder': 2},
            'Market startDate': {'color': '#d62728', 'linewidth': 2, 'linestyle': ':', 'zorder': 2},
            'Market acceptingOrdersTimestamp': {'color': '#9467bd', 'linewidth': 2, 'linestyle': (0, (5, 5)), 'zorder': 2},
        }
        
        # Plot RequestPrice timestamps
        for label, df_cdf in plot_data.items():
            if not df_cdf.empty:
                style = styles.get(label, {'color': 'gray', 'linewidth': 1.5})
                plot_label = f"{label} (n={len(df_cdf):,})"
                plt.plot(df_cdf['datetime'], df_cdf['count'], label=plot_label, **style)
        
        # Plot market timestamps
        for label, df_cdf in plot_data_all.items():
            if not df_cdf.empty:
                style = styles.get(label, {'color': 'gray', 'linewidth': 1.5})
                plot_label = f"{label} (n={len(df_cdf):,})"
                plt.plot(df_cdf['datetime'], df_cdf['count'], label=plot_label, **style)
        
        plt.title('Cumulative Arrival of RequestPrice and Market Events (CDF)', fontsize=16, fontweight='bold')
        plt.xlabel('Time (UTC)', fontsize=12)
        plt.ylabel('Cumulative Count', fontsize=12)
        plt.grid(True, which='both', linestyle='--', alpha=0.6)
        plt.legend(loc='upper left', fontsize=10)
        
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
        plt.gcf().autofmt_xdate()
        
        output_file_cdf = "request_price_timestamp_cdf.png"
        plt.savefig(output_file_cdf, dpi=300, bbox_inches='tight')
        logger.info(f"CDF Plot saved to {output_file_cdf}")
        plt.close()
        
        logger.info("Analysis complete!")
    
    finally:
        await pool.close()

if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials not set. Please set PG_SOCKET, POLY_DB, POLY_DB_CLI, and POLY_DB_CLI_PASS.")
    elif not POLY_CSV_DIR:
        logger.error("CSV directory not set. Please set the POLY_CSV environment variable.")
    elif not os.path.exists(POLY_CSV_PATH):
        logger.error(f"CSV file not found at {POLY_CSV_PATH}. Please ensure POLY_CSV is the correct directory path.")
    else:
        asyncio.run(main())
