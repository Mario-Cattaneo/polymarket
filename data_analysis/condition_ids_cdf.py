import os
import asyncio
import asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging
import time
from datetime import datetime, timezone, timedelta

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
# CORRECTED: Use POLY_CSV environment variable for the directory path
POLY_CSV_DIR = os.getenv("POLY_CSV")
POLY_CSV_PATH = os.path.join(POLY_CSV_DIR, "gamma_markets.csv") if POLY_CSV_DIR else "gamma_markets.csv"

ADDR = {
    'conditional_tokens': "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower(),
}

# --- Dynamic Time Filtering (Will be set in main()) ---
FILTER_BY_TIME = False
START_TIME = None
END_TIME = None

# ----------------------------------------------------------------
# 2. DATA FETCHING & LOADING
# ----------------------------------------------------------------

async def fetch_data(pool, query, params=[]):
    """Fetches data with timing logging."""
    try:
        async with pool.acquire() as connection:
            start = time.time()
            rows = await connection.fetch(query, *params)
            elapsed = time.time() - start
            if elapsed > 1.0:
                logger.info(f"Query fetched {len(rows):,} rows in {elapsed:.2f}s")
            return pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()
    except Exception as e:
        logger.error(f"SQL Error: {e}")
        return pd.DataFrame()

async def fetch_time_range(pool):
    """Fetches the min and max timestamp_ms from ConditionPreparation events."""
    logger.info("Fetching min/max timestamps from ConditionPreparation events...")
    query = f"""
        SELECT MIN(timestamp_ms) as min_time, MAX(timestamp_ms) as max_time
        FROM events_conditional_tokens 
        WHERE event_name = 'ConditionPreparation' 
        AND LOWER(contract_address) = $1
    """
    df = await fetch_data(pool, query, [ADDR['conditional_tokens']])
    
    if df.empty or df['min_time'].isnull().all():
        logger.error("Could not determine time range from ConditionPreparation events.")
        return None, None
        
    min_time = df['min_time'].iloc[0]
    max_time = df['max_time'].iloc[0]
    
    min_dt = datetime.fromtimestamp(min_time / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    max_dt = datetime.fromtimestamp(max_time / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"Time Range determined: {min_dt} to {max_dt}")
    return min_time, max_time

def load_market_data(csv_path):
    """Loads and processes market data from the CSV, using only createdAt and conditionId."""
    logger.info(f"Loading market data from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, usecols=['createdAt', 'conditionId'])
        df['conditionId'] = df['conditionId'].str.lower()
        
        # FIX: Use format='ISO8601' to correctly parse timestamps with or without milliseconds/Z
        df['createdAt_dt'] = pd.to_datetime(df['createdAt'], utc=True, format='ISO8601')
        
        df_unique = df.sort_values('createdAt_dt').drop_duplicates(subset='conditionId', keep='first')
        logger.info(f"Loaded {len(df_unique):,} unique markets from CSV.")
        return df_unique[['createdAt_dt', 'conditionId']]
    except FileNotFoundError:
        logger.error(f"CSV file not found at {csv_path}. Please ensure POLY_CSV environment variable is set correctly.")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading/processing CSV: {e}")
        return pd.DataFrame()

# ----------------------------------------------------------------
# 3. ANALYSIS & PLOTTING HELPERS
# ----------------------------------------------------------------

def format_timedelta(seconds):
    """Helper to format seconds into a readable timedelta string."""
    if pd.isna(seconds): return "N/A"
    td = timedelta(seconds=abs(seconds))
    sign = "-" if seconds < 0 else ""
    # Format as H:MM:SS
    hours, remainder = divmod(td.seconds, 3600)
    minutes, seconds_part = divmod(remainder, 60)
    return f"{sign}{hours:02d}:{minutes:02d}:{seconds_part:02d}"

def prepare_cdf_data(df, time_column, label):
    """Prepares data for CDF plotting."""
    if df.empty: return pd.DataFrame(columns=['datetime', 'count', 'label'])
    
    if not pd.api.types.is_datetime64_any_dtype(df[time_column]):
        df['datetime'] = pd.to_datetime(df[time_column], unit='ms', utc=True)
    else:
        df['datetime'] = df[time_column]

    df_sorted = df.sort_values('datetime')
    df_sorted['count'] = range(1, len(df_sorted) + 1)
    df_sorted['label'] = label
    
    return df_sorted[['datetime', 'count', 'label']]

def analyze_timing(df_matched_all):
    """Calculates and prints the timing difference statistics (T2 - T1)."""
    
    if df_matched_all.empty:
        logger.info("No matched data for timing analysis.")
        return None
        
    # T1 is createdAt_dt (datetime), T2 is timestamp (ms)
    df_matched_all['timestamp_dt'] = pd.to_datetime(df_matched_all['timestamp'], unit='ms', utc=True)
    
    # Calculate difference: Delta T = T2 - T1 (Event - Market)
    df_matched_all['time_diff'] = df_matched_all['timestamp_dt'] - df_matched_all['createdAt_dt']
    df_matched_all['time_diff_sec'] = df_matched_all['time_diff'].dt.total_seconds()
    
    total_count = len(df_matched_all)
    
    # Calculate Probabilities
    count_t2_gt_t1 = (df_matched_all['time_diff_sec'] > 0).sum()
    count_t2_eq_t1 = (df_matched_all['time_diff_sec'] == 0).sum()
    count_t2_lt_t1 = (df_matched_all['time_diff_sec'] < 0).sum()
    
    p_t2_gt_t1 = (count_t2_gt_t1 / total_count) * 100
    p_t2_eq_t1 = (count_t2_eq_t1 / total_count) * 100
    p_t2_lt_t1 = (count_t2_lt_t1 / total_count) * 100
    
    # Calculate Mean, Std Dev, Min, and Max
    avg_diff_sec = df_matched_all['time_diff_sec'].mean()
    std_diff_sec = df_matched_all['time_diff_sec'].std()
    min_diff_sec = df_matched_all['time_diff_sec'].min()
    max_diff_sec = df_matched_all['time_diff_sec'].max()
    
    logger.info("\n--- Timing Analysis (Event Preparation T2 - Market Creation T1) ---")
    print(f"Total Matched Events: {total_count:,}")
    print("\n--- Probability Analysis ---")
    print(f"P(T2 > T1) [Event AFTER Market]: {p_t2_gt_t1:.2f}% ({count_t2_gt_t1:,} events)")
    print(f"P(T2 = T1) [Event AT Market]: {p_t2_eq_t1:.2f}% ({count_t2_eq_t1:,} events)")
    print(f"P(T2 < T1) [Event BEFORE Market]: {p_t2_lt_t1:.2f}% ({count_t2_lt_t1:,} events)")
    
    print("\n--- Range, Mean and Standard Deviation ---")
    print(f"Min Difference: {format_timedelta(min_diff_sec)} ({min_diff_sec:.2f} seconds)")
    print(f"Max Difference: {format_timedelta(max_diff_sec)} ({max_diff_sec:.2f} seconds)")
    print(f"E(T2 - T1) [Average Delay]: {format_timedelta(avg_diff_sec)} ({avg_diff_sec:.2f} seconds)")
    print(f"StdDev(T2 - T1) [Standard Deviation]: {format_timedelta(std_diff_sec)} ({std_diff_sec:.2f} seconds)")
    
    logger.info("------------------------------------------------------------------")
    
    return df_matched_all['time_diff_sec'] # Return the series for plotting


def analyze_and_print_stats(df_matched_all):
    """Calculates and prints the required statistics."""
    
    df_filtered = df_matched_all[df_matched_all['oracle'].notna()]
    total_matched_events = len(df_filtered)
    
    if total_matched_events == 0:
        logger.info("No matched events to analyze.")
        return []
        
    logger.info("\n--- Oracle Event Analysis ---")
    logger.info(f"Total Matched ConditionPreparation Events: {total_matched_events:,}")
    
    oracle_groups = df_filtered.groupby('oracle')
    event_counts = oracle_groups.size().rename('event_count')
    
    # Calculate percentages
    stats_df = pd.DataFrame(event_counts)
    stats_df['event_percent'] = (stats_df['event_count'] / total_matched_events) * 100
    
    # The count of events that matched a market is the event_count itself due to the inner join
    stats_df['market_match_count'] = stats_df['event_count']
    # The percent of an oracle's events that matched a market is 100% due to the inner join
    stats_df['market_match_percent'] = 100.0 
    
    # Print results
    for oracle, row in stats_df.iterrows():
        # Print the full oracle address
        print(f"\nOracle: {oracle}")
        print(f"  Total Events: {row['event_count']:,} ({row['event_percent']:.2f}%)")
        print(f"  Market Matches: {row['market_match_count']:,} ({row['market_match_percent']:.2f}%)")

    logger.info("\n-----------------------------\n")
    return stats_df.index.tolist()

# ----------------------------------------------------------------
# 4. MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    global START_TIME, END_TIME, FILTER_BY_TIME
    
    # 1. Load CSV Data
    df_markets = load_market_data(POLY_CSV_PATH)
    if df_markets.empty:
        return

    # 2. Database Connection
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Connected to database.")

    # 3. Determine Dynamic Time Range
    START_TIME, END_TIME = await fetch_time_range(pool)
    if START_TIME is None:
        await pool.close()
        return
    FILTER_BY_TIME = True # Enable filtering now that the range is set

    # Convert ms timestamps to datetime objects for filtering the CSV data
    start_dt = pd.to_datetime(START_TIME, unit='ms', utc=True)
    end_dt = pd.to_datetime(END_TIME, unit='ms', utc=True)

    # Filter CSV markets by the event time range (createdAt)
    df_markets_filtered = df_markets[
        (df_markets['createdAt_dt'] >= start_dt) & 
        (df_markets['createdAt_dt'] <= end_dt)
    ].copy()
    total_markets_in_range = len(df_markets_filtered)
    logger.info(f"Filtered CSV markets by event time range: {total_markets_in_range:,} markets remaining.")


    def build_time_clause(column_name, current_params):
        """Builds the WHERE clause using the dynamically set START_TIME/END_TIME."""
        if not FILTER_BY_TIME: return "", current_params
        clauses = []
        new_params = current_params.copy()
        if START_TIME:
            new_params.append(START_TIME)
            clauses.append(f"{column_name} >= ${len(new_params)}")
        if END_TIME:
            new_params.append(END_TIME)
            clauses.append(f"{column_name} <= ${len(new_params)}")
        return (" AND " + " AND ".join(clauses) if clauses else ""), new_params

    # 4. Fetch CTF ConditionPreparation events - TIME FILTER APPLIED HERE
    logger.info("Fetching CTF ConditionPreparation data (time-filtered)...")
    q_ctf = """
        SELECT topics[2] as condition_id, timestamp_ms as timestamp, '0x' || substring(topics[3] from 27) as oracle 
        FROM events_conditional_tokens 
        WHERE event_name = 'ConditionPreparation' 
        AND LOWER(contract_address) = $1
    """
    t_ctf, p_ctf = build_time_clause("timestamp_ms", [ADDR['conditional_tokens']])
    df_ctf = await fetch_data(pool, q_ctf + t_ctf, p_ctf)
    df_ctf['condition_id'] = df_ctf['condition_id'].str.lower()
    df_ctf_unique = df_ctf.sort_values('timestamp').drop_duplicates(subset='condition_id', keep='first')

    await pool.close()
    logger.info("Data fetching complete. Processing...")

    # 5. Data Matching and Filtering

    # Inner merge: Time-filtered CSV Markets (createdAt) with time-filtered CTF Preps (oracle, timestamp)
    df_matched_all = pd.merge(
        df_markets_filtered, # <-- Use the time-filtered CSV data
        df_ctf_unique, 
        left_on='conditionId', 
        right_on='condition_id', 
        how='inner'
    )
    matched_markets_count = len(df_matched_all)
    logger.info(f"Matched {matched_markets_count:,} markets with time-filtered ConditionPreparation events.")
    
    # Calculate Unmatched Markets
    unmatched_markets_count = total_markets_in_range - matched_markets_count
    unmatched_percent = (unmatched_markets_count / total_markets_in_range) * 100 if total_markets_in_range > 0 else 0.0
    
    logger.info(f"\n--- Unmatched Market Analysis ---")
    logger.info(f"Markets in Time Range: {total_markets_in_range:,}")
    logger.info(f"Unmatched Markets (No ConditionPreparation Event): {unmatched_markets_count:,} ({unmatched_percent:.2f}%)")
    logger.info(f"---------------------------------\n")

    # 6. Timing Analysis (T2 - T1)
    time_diff_series = analyze_timing(df_matched_all)

    # 7. Analyze and Print Oracle Statistics
    filtered_oracles = analyze_and_print_stats(df_matched_all)
    
    if df_matched_all.empty:
        logger.warning("No data remaining after matching. Cannot generate plots.")
        return

    # 8. Prepare CDF Data
    plot_data = {}
    
    # CDF 1: createdAt (from CSV) for all matched markets
    plot_data['createdAt (CSV)'] = prepare_cdf_data(
        df_matched_all, 
        'createdAt_dt', 
        'Market Creation (CSV)'
    )
    
    # CDF 2: Combined Matched Events (from CTF timestamp)
    plot_data['Combined Oracles'] = prepare_cdf_data(
        df_matched_all, 
        'timestamp', 
        'Combined Oracles (CTF)'
    )
    
    # CDF 3: Individual Oracle Events (from CTF timestamp)
    for oracle in filtered_oracles:
        # Use the full oracle address for the label
        plot_data[f'Oracle {oracle}'] = prepare_cdf_data(
            df_matched_all[df_matched_all['oracle'] == oracle], 
            'timestamp', 
            f'Oracle {oracle}'
        )

    # 9. Plotting
    logger.info("Generating CDF Plot...")
    
    # Plot 1: CDF of Market/Event Arrivals
    plt.figure(figsize=(14, 8))
    
    styles = {
        'createdAt (CSV)':      {'color': 'red', 'linewidth': 3, 'linestyle': '-', 'zorder': 1}, # Yellow, solid, behind
        'Combined Oracles':     {'color': 'black',  'linewidth': 2.5, 'linestyle': '--', 'zorder': 3}, # Black, dashed, in front
    }
    oracle_colors = ['brown', 'blue', 'green', 'orange', 'brown', 'pink', 'cyan', 'magenta']

    oracle_idx = 0
    for label, df in plot_data.items():
        if df.empty:
            logger.warning(f"No data for {label}")
            continue
            
        style = styles.get(label, {})
        if 'Oracle' in label and label not in styles:
            style = {'color': oracle_colors[oracle_idx % len(oracle_colors)], 'linewidth': 1.5, 'linestyle': '-.', 'zorder': 2}
            oracle_idx += 1

        # Use the full label from the prepared data (which contains the full oracle address)
        plt.plot(df['datetime'], df['count'], label=f"{df['label'].iloc[0]} (n={len(df):,})", **style)

    plt.title('Cumulative Arrival of Unique Condition IDs (CDF)', fontsize=16)
    plt.xlabel('Time (UTC)', fontsize=12)
    plt.ylabel('Cumulative Count of Unique Conditions', fontsize=12)
    plt.grid(True, which='both', linestyle='--', alpha=0.6)
    plt.legend(loc='upper left', fontsize=10)
    
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gcf().autofmt_xdate()

    output_file_cdf = "oracle_market_analysis_cdf.png"
    plt.savefig(output_file_cdf, dpi=300, bbox_inches='tight')
    logger.info(f"CDF Plot saved to {output_file_cdf}")
    
    # Plot 2: Histogram of Time Difference (T2 - T1)
    if time_diff_series is not None and not time_diff_series.empty:
        plt.figure(figsize=(14, 8))
        
        # Focusing on the central 99% of the data to exclude extreme outliers for a clearer view
        lower_bound = time_diff_series.quantile(0.005)
        upper_bound = time_diff_series.quantile(0.995)
        
        plt.hist(
            time_diff_series, 
            bins=100, 
            range=(lower_bound, upper_bound), 
            edgecolor='black', 
            color='skyblue'
        )
        
        plt.axvline(time_diff_series.mean(), color='red', linestyle='dashed', linewidth=2, label=f'Mean: {format_timedelta(time_diff_series.mean())}')
        plt.axvline(0, color='black', linestyle='-', linewidth=1, label='T2 = T1 (Zero Delay)')
        
        plt.title(f'Distribution of Event Preparation Delay (T2 - T1) - Central 99%', fontsize=16)
        plt.xlabel('Time Difference (Seconds)', fontsize=12)
        plt.ylabel('Frequency (Count)', fontsize=12)
        plt.legend(loc='upper right')
        plt.grid(True, which='both', linestyle='--', alpha=0.6)

        output_file_hist = "timing_difference_histogram.png"
        plt.savefig(output_file_hist, dpi=300, bbox_inches='tight')
        logger.info(f"Timing Histogram saved to {output_file_hist}")
        
    plt.show()

if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials not set. Please set PG_SOCKET, POLY_DB, POLY_DB_CLI, and POLY_DB_CLI_PASS.")
    elif not POLY_CSV_DIR:
        logger.error("CSV directory not set. Please set the POLY_CSV environment variable.")
    elif not os.path.exists(POLY_CSV_PATH):
        logger.error(f"CSV file not found at {POLY_CSV_PATH}. Please ensure POLY_CSV is the correct directory path.")
    else:
        asyncio.run(main())