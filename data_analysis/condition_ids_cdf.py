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
from scipy import stats

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

# --- Oracle Aliases for Plot Legend ---
ORACLE_ALIASES = {
    "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7": "MOOV2 Adapter",
    "0x58e1745bedda7312c4cddb72618923da1b90efde": "Centralized Adapter",
    "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296": "Negrisk UmaCtfAdapter",
}

# --- Dynamic Time Filtering (Will be set in main()) ---
FILTER_BY_TIME = False
START_TIME = None
END_TIME = None

# --- PMF Configuration ---
# Define the desired bin width (step size) in seconds for the PMF plot
PMF_BIN_WIDTH_SEC = 60 # 10 minute step size

# --- Percentiles Configuration ---
# Define percentiles to compute and display in timing analysis
PERCENTILES = [0.001, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 0.999]

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
    """Loads and processes market data from the CSV, including createdAt, startDate, acceptingOrdersTimestamp, and conditionId."""
    logger.info(f"Loading market data from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, usecols=['createdAt', 'startDate', 'acceptingOrdersTimestamp', 'conditionId'])
        df['conditionId'] = df['conditionId'].str.lower()
        
        # FIX: Use format='ISO8601' to correctly parse timestamps with or without milliseconds/Z
        df['createdAt_dt'] = pd.to_datetime(df['createdAt'], utc=True, format='ISO8601')
        df['startDate_dt'] = pd.to_datetime(df['startDate'], utc=True, format='ISO8601', errors='coerce')
        df['acceptingOrdersTimestamp_dt'] = pd.to_datetime(df['acceptingOrdersTimestamp'], utc=True, format='ISO8601', errors='coerce')
        
        df_unique = df.sort_values('createdAt_dt').drop_duplicates(subset='conditionId', keep='first')
        logger.info(f"Loaded {len(df_unique):,} unique markets from CSV.")
        return df_unique[['createdAt_dt', 'startDate_dt', 'acceptingOrdersTimestamp_dt', 'conditionId']]
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

def analyze_timing_custom(df_matched_all, timestamp_col, timestamp_name):
    """Analyzes timing difference between Event Preparation (T2) and a custom timestamp (T1)."""
    if df_matched_all.empty:
        logger.info("No matched data for timing analysis.")
        return None, None
        
    # T1 is the custom timestamp, T2 is timestamp (ms)
    df_matched_all = df_matched_all.copy()
    df_matched_all['timestamp_dt'] = pd.to_datetime(df_matched_all['timestamp'], unit='ms', utc=True)
    
    # Calculate difference: Delta T = T2 - T1 (Event - Timestamp)
    df_matched_all['time_diff'] = df_matched_all['timestamp_dt'] - df_matched_all[timestamp_col]
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
    
    # Calculate Skewness (normalized third moment - measures asymmetry)
    skewness = stats.skew(df_matched_all['time_diff_sec'].dropna())
    
    # Calculate Kurtosis (excess kurtosis - measures heavy tailness)
    kurtosis = stats.kurtosis(df_matched_all['time_diff_sec'].dropna(), fisher=True)
    
    logger.info(f"\n--- Timing Analysis (Event Preparation T2 - {timestamp_name} T1) ---")
    print(f"Total Matched Events: {total_count:,}")
    print("\n--- Probability Analysis ---")
    print(f"P(T2 > T1) [Event AFTER {timestamp_name}]: {p_t2_gt_t1:.2f}% ({count_t2_gt_t1:,} events)")
    print(f"P(T2 = T1) [Event AT {timestamp_name}]: {p_t2_eq_t1:.2f}% ({count_t2_eq_t1:,} events)")
    print(f"P(T2 < T1) [Event BEFORE {timestamp_name}]: {p_t2_lt_t1:.2f}% ({count_t2_lt_t1:,} events)")
    
    print(f"\n--- Range, Mean, Standard Deviation, and Distribution Shape ---")
    print(f"Min Difference: {format_timedelta(min_diff_sec)} ({min_diff_sec:.2f} seconds)")
    print(f"Max Difference: {format_timedelta(max_diff_sec)} ({max_diff_sec:.2f} seconds)")
    print(f"E(T2 - T1) [Average Delay]: {format_timedelta(avg_diff_sec)} ({avg_diff_sec:.2f} seconds)")
    print(f"StdDev(T2 - T1) [Standard Deviation]: {format_timedelta(std_diff_sec)} ({std_diff_sec:.2f} seconds)")
    print(f"Skewness (Normalized 3rd Moment - Asymmetry): {skewness:.4f}")
    print(f"Kurtosis Excess (Heavy Tailness): {kurtosis:.4f}")
    
    # Calculate and print percentiles
    print(f"\n--- Percentiles ---")
    for p in PERCENTILES:
        percentile_val = df_matched_all['time_diff_sec'].quantile(p)
        percentile_label = f"{p*100:.1f}th"
        print(f"{percentile_label} Percentile: {format_timedelta(percentile_val)} ({percentile_val:.2f} seconds)")
    
    # Per-oracle statistics
    print(f"\n--- Per-Oracle Timing Statistics ({timestamp_name}) ---")
    for oracle_addr, alias in ORACLE_ALIASES.items():
        oracle_data = df_matched_all[df_matched_all['oracle'] == oracle_addr]
        if not oracle_data.empty:
            oracle_mean = oracle_data['time_diff_sec'].mean()
            oracle_std = oracle_data['time_diff_sec'].std()
            oracle_skewness = stats.skew(oracle_data['time_diff_sec'].dropna())
            oracle_kurtosis = stats.kurtosis(oracle_data['time_diff_sec'].dropna(), fisher=True)
            oracle_count = len(oracle_data)
            print(f"\n{alias} ({oracle_count:,} events):")
            print(f"  E(T2 - T1): {format_timedelta(oracle_mean)} ({oracle_mean:.2f} seconds)")
            print(f"  StdDev(T2 - T1): {format_timedelta(oracle_std)} ({oracle_std:.2f} seconds)")
            print(f"  Skewness: {oracle_skewness:.4f}")
            print(f"  Kurtosis Excess: {oracle_kurtosis:.4f}")
            print(f"  Percentiles:")
            for p in PERCENTILES:
                percentile_val = oracle_data['time_diff_sec'].quantile(p)
                percentile_label = f"{p*100:.1f}th"
                print(f"    {percentile_label}: {format_timedelta(percentile_val)} ({percentile_val:.2f} seconds)")
    
    logger.info("------------------------------------------------------------------")

    # Create the summary string for the plot legend
    summary_text = (
        f"Total Events: {total_count:,}\n"
        f"P(T2 > T1): {p_t2_gt_t1:.2f}%\n"
        f"P(T2 = T1): {p_t2_eq_t1:.0f}%\n"
        f"P(T2 < T1): {p_t2_lt_t1:.2f}%\n"
        f"Avg Delay: {format_timedelta(avg_diff_sec)}\n"
        f"StdDev: {format_timedelta(std_diff_sec)}"
    )
    
    return df_matched_all['time_diff_sec'], summary_text, df_matched_all

def analyze_timing(df_matched_all):
    """Calculates and prints the timing difference statistics (T2 - T1) and returns the summary string."""
    if df_matched_all.empty:
        logger.info("No matched data for timing analysis.")
        return None, None, None
    return analyze_timing_custom(df_matched_all, 'createdAt_dt', 'createdAt') # Return the series and the summary

def plot_pmf_clean(df_data, title, x_label, y_label, output_filename, lower_bound=None, upper_bound=None, bin_width_sec=600):
    """
    Creates cleaner PMF plots with multiple visualization options.
    
    Options:
    1. Separate subplots (one per oracle)
    2. Side-by-side KDE plots
    3. Individual line plots with no overlaps
    """
    oracle_colors = {
        '0x65070be91477460d8a7aeeb94ef92fe056c2f2a7': '#001f3f',  # MOOV2 - navy
        '0x58e1745bedda7312c4cddb72618923da1b90efde': '#dc143c',  # Centralized - crimson
        '0xd91e80cf2e7be2e162c6513ced06f1dd0da35296': '#ff8c00'   # Negrisk - dark orange
    }
    
    # Option 1: Separate Subplots (Recommended for clarity)
    fig, axes = plt.subplots(1, 3, figsize=(18, 5), sharey=True)
    fig.suptitle(title, fontsize=16, fontweight='bold', y=1.02)
    
    for idx, (oracle_addr, alias) in enumerate(sorted(ORACLE_ALIASES.items())):
        ax = axes[idx]
        oracle_time_diffs = df_data[df_data['oracle'] == oracle_addr]['time_diff_sec'].values
        
        if len(oracle_time_diffs) == 0:
            ax.text(0.5, 0.5, 'No data', ha='center', va='center', transform=ax.transAxes)
            ax.set_title(f'{alias}\n(n=0)', fontsize=12, fontweight='bold')
            continue
        
        # Use provided bounds or calculate from data
        if lower_bound is None or upper_bound is None:
            lb = oracle_time_diffs.min() if lower_bound is None else lower_bound
            ub = oracle_time_diffs.max() if upper_bound is None else upper_bound
        else:
            lb, ub = lower_bound, upper_bound
        
        data_range = ub - lb
        num_bins = max(10, int(np.ceil(data_range / bin_width_sec)))
        
        # Plot histogram with clean styling
        ax.hist(oracle_time_diffs, bins=num_bins, range=(lb, ub), density=True,
                color=oracle_colors.get(oracle_addr, 'skyblue'), alpha=0.75, edgecolor='black', linewidth=0.5)
        
        # Reference lines
        ax.axvline(oracle_time_diffs.mean(), color='red', linestyle='--', linewidth=2, label='Mean', alpha=0.8)
        ax.axvline(0, color='black', linestyle='-', linewidth=1, label='Zero Delay', alpha=0.6)
        
        # Styling
        ax.set_yscale('log')
        ax.set_xlabel(x_label, fontsize=11)
        if idx == 0:
            ax.set_ylabel(y_label, fontsize=11)
        ax.set_title(f'{alias}\n(n={len(oracle_time_diffs):,})', fontsize=12, fontweight='bold')
        ax.grid(True, which='both', alpha=0.3, linestyle='--')
        ax.legend(fontsize=9, loc='upper right')
        ax.set_xlim(lb, ub)
    
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    logger.info(f"PMF Plot (Separate Subplots) saved to {output_filename}")
    return fig

def plot_pmf_overlaid(df_data, title, x_label, y_label, output_filename, lower_bound=None, upper_bound=None, bin_width_sec=600):
    """
    Creates beautiful stacked PMF histogram showing contribution from each oracle.
    The total height shows the overall PMF, and each color segment shows oracle contribution.
    """
    oracle_colors = {
        '0x65070be91477460d8a7aeeb94ef92fe056c2f2a7': '#1f77b4',  # MOOV2 - blue
        '0x58e1745bedda7312c4cddb72618923da1b90efde': '#ff7f0e',  # Centralized - orange
        '0xd91e80cf2e7be2e162c6513ced06f1dd0da35296': '#2ca02c'   # Negrisk - green
    }
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Determine common bins for all oracles
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
    
    # Collect histograms for each oracle
    oracle_data_list = []
    total_count = df_data.shape[0]
    
    for oracle_addr, alias in sorted(ORACLE_ALIASES.items()):
        oracle_subset = df_data[df_data['oracle'] == oracle_addr]
        oracle_time_diffs = oracle_subset['time_diff_sec'].values
        
        if len(oracle_time_diffs) > 0:
            counts, _ = np.histogram(oracle_time_diffs, bins=bins)
            # Normalize by total count to get PMF
            pmf = counts / total_count
            oracle_data_list.append({
                'alias': alias,
                'oracle_addr': oracle_addr,
                'pmf': pmf,
                'count': len(oracle_time_diffs)
            })
    
    if not oracle_data_list:
        logger.warning("No oracle data to plot")
        return fig
    
    # Prepare data for stacked bars
    bin_centers = (bins[:-1] + bins[1:]) / 2
    bin_width = bins[1] - bins[0]
    
    # Stack the histograms
    bottom = np.zeros(len(bins) - 1)
    
    for oracle_data in oracle_data_list:
        pmf = oracle_data['pmf']
        alias = oracle_data['alias']
        oracle_addr = oracle_data['oracle_addr']
        count = oracle_data['count']
        
        ax.bar(bin_centers, pmf, width=bin_width * 0.92, bottom=bottom,
               label=f'{alias} (n={count:,})',
               color=oracle_colors.get(oracle_addr, 'skyblue'), 
               alpha=0.85, edgecolor='white', linewidth=0.3)
        
        bottom += pmf
    
    # Reference lines - enhanced styling
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
    
    # Legend
    ax.legend(fontsize=11, loc='upper right', framealpha=0.98, fancybox=True, shadow=True)
    ax.set_xlim(lb, ub)
    
    # Set background
    ax.set_facecolor('#f8f9fa')
    fig.patch.set_facecolor('white')
    
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300, bbox_inches='tight', facecolor='white')
    logger.info(f"PMF Plot (Stacked) saved to {output_filename}")
    return fig


def analyze_and_print_stats(df_matched_all, total_prepared_events, oracle_totals_dict):
    """Calculates and prints the required statistics."""
    
    df_filtered = df_matched_all[df_matched_all['oracle'].notna()].copy()
    total_matched_events = len(df_filtered)
    
    if total_matched_events == 0:
        logger.info("No matched events to analyze.")
        return []
    
    # Only include oracles that have at least one match
    oracles_with_matches = set(df_filtered['oracle'].unique())
    
    # Calculate totals only from oracles with matches
    total_all_prepared = sum(oracle_totals_dict.get(oracle, 0) for oracle in oracles_with_matches)
    
    logger.info("\n--- Oracle Event Analysis (MATCHED vs TOTAL) ---")
    logger.info(f"Total Matched ConditionPreparation Events: {total_matched_events:,}\n")
    
    oracle_groups = df_filtered.groupby('oracle')
    event_counts = oracle_groups.size().rename('event_count')
    
    # Print results with percentages
    for oracle in sorted(event_counts.index):
        alias = ORACLE_ALIASES.get(oracle, oracle)
        matched_count = event_counts[oracle]
        
        # Get total count for this oracle from all prepared events
        total_for_oracle = oracle_totals_dict.get(oracle, 0)
        
        # Percentage of total prepared for this oracle (only from oracles with matches)
        pct_of_total_prepared = (total_for_oracle / total_all_prepared * 100) if total_all_prepared > 0 else 0
        
        # Percentage of total matched for this oracle
        pct_of_total_matched = (matched_count / total_matched_events * 100) if total_matched_events > 0 else 0
        
        # Percentage of this oracle's prepared that were matched
        pct_matched_for_oracle = (matched_count / total_for_oracle * 100) if total_for_oracle > 0 else 0
        
        logger.info(f"Oracle: {alias} ({oracle})")
        logger.info(f"  Total Prepared Events: {total_for_oracle:,} ({pct_of_total_prepared:.2f}% of all prepared)")
        logger.info(f"  Matched Events: {matched_count:,} ({pct_of_total_matched:.2f}% of all matched, {pct_matched_for_oracle:.2f}% of this oracle's prepared)")
        logger.info("-----------------------------")
    
    # Print totals
    logger.info(f"\nTOTALS:")
    logger.info(f"  Total Prepared Events (all oracles with matches): {total_all_prepared:,}")
    logger.info(f"  Total Matched Events (all oracles): {total_matched_events:,}")
    logger.info("-----------------------------\n")
    
    return event_counts.index.tolist()

# ----------------------------------------------------------------
# 4. MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    global START_TIME, END_TIME, FILTER_BY_TIME
    
    # 1. Load CSV Data (ALL markets, no time filter yet)
    df_markets = load_market_data(POLY_CSV_PATH)
    if df_markets.empty:
        return
    
    logger.info(f"Total Markets in CSV: {len(df_markets):,}")

    # 2. Database Connection
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Connected to database.")

    # 3. Determine Dynamic Time Range
    START_TIME, END_TIME = await fetch_time_range(pool)
    if START_TIME is None:
        await pool.close()
        return
    FILTER_BY_TIME = True # Enable filtering now that the range is set

    # Convert ms timestamps to datetime objects for later filtering
    start_dt = pd.to_datetime(START_TIME, unit='ms', utc=True)
    end_dt = pd.to_datetime(END_TIME, unit='ms', utc=True)

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

    # 4. Fetch CTF ConditionPreparation events (ALL, no time filter yet)
    logger.info("Fetching CTF ConditionPreparation data (all events)...")
    q_ctf = """
        SELECT topics[2] as condition_id, timestamp_ms as timestamp, '0x' || substring(topics[3] from 27) as oracle 
        FROM events_conditional_tokens 
        WHERE event_name = 'ConditionPreparation' 
        AND LOWER(contract_address) = $1
    """
    df_ctf = await fetch_data(pool, q_ctf, [ADDR['conditional_tokens']])
    df_ctf['condition_id'] = df_ctf['condition_id'].str.lower()
    df_ctf['timestamp'] = pd.to_datetime(df_ctf['timestamp'], unit='ms', utc=True)
    
    logger.info(f"Fetched {len(df_ctf):,} total ConditionPreparation events (before deduplication).")
    df_ctf_unique = df_ctf.sort_values('timestamp').drop_duplicates(subset='condition_id', keep='first')
    
    # Apply time filter to CTF data for CDF plotting
    df_ctf_filtered = df_ctf_unique[
        (df_ctf_unique['timestamp'] >= start_dt) & 
        (df_ctf_unique['timestamp'] <= end_dt)
    ].copy()
    logger.info(f"After time filtering: {len(df_ctf_filtered):,} ConditionPreparation events in time range.")

    # Log ALL prepared events BEFORE matching (from df_ctf_unique - all deduplicated CTF events)
    logger.info("\n--- Total Prepared ConditionPreparation Events (ALL, before matching) ---")
    total_all_prepared = len(df_ctf_unique)
    logger.info(f"Total Prepared Events (all oracles, deduped): {total_all_prepared:,}")
    
    if not df_ctf_unique.empty:
        oracle_totals_all = df_ctf_unique.groupby('oracle').size()
        for oracle_addr in sorted(ORACLE_ALIASES.keys()):
            count = oracle_totals_all.get(oracle_addr, 0)
            alias = ORACLE_ALIASES.get(oracle_addr, oracle_addr)
            pct = (count / total_all_prepared * 100) if total_all_prepared > 0 else 0
            logger.info(f"  {alias}: {count:,} events ({pct:.2f}%)")
        logger.info("--- End Total Prepared Events ---\n")

    await pool.close()
    logger.info("Data fetching complete. Processing...")

    # 5. Find oracles with at least one matching condition_id
    logger.info("Finding oracles with matching condition_ids...")
    
    # Get set of condition_ids from markets
    market_condition_ids = set(df_markets['conditionId'].unique())
    
    # Get set of condition_ids from CTF events
    ctf_condition_ids = set(df_ctf_unique['condition_id'].unique())
    
    # Find matching condition_ids
    matching_condition_ids = market_condition_ids & ctf_condition_ids
    logger.info(f"Found {len(matching_condition_ids):,} matching condition_ids")
    
    # Get oracles that have at least one matching condition_id
    df_ctf_with_matches = df_ctf_unique[df_ctf_unique['condition_id'].isin(matching_condition_ids)]
    oracles_with_matches = set(df_ctf_with_matches['oracle'].unique())
    logger.info(f"Found {len(oracles_with_matches):,} oracles with matching condition_ids")
    
    # 6. Get ALL events from oracles with matches (this is set A)
    logger.info("Fetching ALL events from matching oracles (set A)...")
    pool_for_all_events = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    
    q_all_events = f"""
        SELECT topics[2] as condition_id, timestamp_ms as timestamp, '0x' || substring(topics[3] from 27) as oracle 
        FROM events_conditional_tokens 
        WHERE event_name = 'ConditionPreparation' 
        AND LOWER(contract_address) = $1
        AND '0x' || substring(topics[3] from 27) = ANY($2::text[])
    """
    oracle_addresses = list(oracles_with_matches)
    df_all_events_A = await fetch_data(pool_for_all_events, q_all_events, [ADDR['conditional_tokens'], oracle_addresses])
    df_all_events_A['condition_id'] = df_all_events_A['condition_id'].str.lower()
    df_all_events_A['timestamp_dt'] = pd.to_datetime(df_all_events_A['timestamp'], unit='ms', utc=True)
    
    # Deduplicate by condition_id
    df_all_events_A = df_all_events_A.sort_values('timestamp_dt').drop_duplicates(subset='condition_id', keep='first')
    logger.info(f"Set A (all events from matching oracles): {len(df_all_events_A):,} unique condition_ids")
    
    # 7. Create filtered subsets: B, C, D
    df_B = df_all_events_A[
        (df_all_events_A['timestamp_dt'] >= start_dt) & 
        (df_all_events_A['timestamp_dt'] <= end_dt)
    ].copy()
    logger.info(f"Set B (createdAt/timestamp in range): {len(df_B):,} events")
    
    # For C, we need markets with startDate in range, then find matching events
    df_markets_with_start_in_range = df_markets[
        (df_markets['startDate_dt'].notna()) & 
        (df_markets['startDate_dt'] >= start_dt) & 
        (df_markets['startDate_dt'] <= end_dt)
    ].copy()
    condition_ids_C = set(df_markets_with_start_in_range['conditionId'].unique())
    df_C = df_all_events_A[df_all_events_A['condition_id'].isin(condition_ids_C)].copy()
    logger.info(f"Set C (markets with startDate in range): {len(df_C):,} events")
    
    # For D, we need markets with acceptingOrdersTimestamp in range
    df_markets_with_accepting_in_range = df_markets[
        (df_markets['acceptingOrdersTimestamp_dt'].notna()) & 
        (df_markets['acceptingOrdersTimestamp_dt'] >= start_dt) & 
        (df_markets['acceptingOrdersTimestamp_dt'] <= end_dt)
    ].copy()
    condition_ids_D = set(df_markets_with_accepting_in_range['conditionId'].unique())
    df_D = df_all_events_A[df_all_events_A['condition_id'].isin(condition_ids_D)].copy()
    logger.info(f"Set D (markets with acceptingOrdersTimestamp in range): {len(df_D):,} events")
    
    await pool_for_all_events.close()
    
    # 8. Data Matching for time analysis (ALL data, no time filter - match by condition_id only)
    logger.info("Matching CSV markets with ConditionPreparation events (for time analysis)...")
    df_matched_all = pd.merge(
        df_markets,  # <-- Use ALL CSV data, no time filter
        df_all_events_A,  # <-- Use ALL events from matching oracles
        left_on='conditionId', 
        right_on='condition_id', 
        how='inner'
    )
    logger.info(f"Matched {len(df_matched_all):,} markets with ConditionPreparation events (all time).")
    
    # 9. For statistics, get matched markets in time range
    df_matched_filtered = df_matched_all[
        (df_matched_all['createdAt_dt'] >= start_dt) & 
        (df_matched_all['createdAt_dt'] <= end_dt)
    ].copy()
    
    total_markets_in_range = len(df_matched_filtered)
    logger.info(f"After applying time range filter: {total_markets_in_range:,} markets in the time window.")
    
    # Log total ConditionPreparation events (before dedup) from matched dataset
    logger.info("\n--- ConditionPreparation Events from All Matches (before deduplication) ---")
    df_ctf_from_matches = df_matched_all[['oracle', 'timestamp', 'condition_id']].drop_duplicates(subset=['condition_id'])
    total_all_events = len(df_matched_all)  # total match events before filtering by time
    logger.info(f"Total ConditionPreparation events (all matches): {total_all_events:,}")
    
    if not df_matched_all.empty:
        oracle_totals = df_matched_all.groupby('oracle').size()
        for oracle_addr in sorted(ORACLE_ALIASES.keys()):
            count = oracle_totals.get(oracle_addr, 0)
            alias = ORACLE_ALIASES.get(oracle_addr, oracle_addr)
            pct = (count / total_all_events * 100) if total_all_events > 0 else 0
            logger.info(f"  {alias}: {count:,} events ({pct:.2f}%)")
        logger.info("--- End Total Events ---\n")
    
    if df_matched_filtered.empty:
        logger.warning("No data remaining after matching and time filtering. Cannot generate plots.")
        return
    
    # 7. Unmatched Markets Analysis (within time range)
    df_markets_in_range = df_markets[
        (df_markets['createdAt_dt'] >= start_dt) & 
        (df_markets['createdAt_dt'] <= end_dt)
    ].copy()
    total_markets_in_range = len(df_markets_in_range)
    matched_markets_count = len(df_matched_filtered)
    unmatched_markets_count = total_markets_in_range - matched_markets_count
    unmatched_percent = (unmatched_markets_count / total_markets_in_range) * 100 if total_markets_in_range > 0 else 0.0
    
    logger.info(f"\n--- Unmatched Market Analysis (within time range) ---")
    logger.info(f"Markets in Time Range: {total_markets_in_range:,}")
    logger.info(f"Matched: {matched_markets_count:,}")
    logger.info(f"Unmatched Markets (No ConditionPreparation Event): {unmatched_markets_count:,} ({unmatched_percent:.2f}%)")
    logger.info(f"---------------------------------\n")

    # 8. Timing Analysis (T2 - T1) - use filtered data
    time_diff_series, timing_summary_text, df_with_timing = analyze_timing(df_matched_filtered)

    # 8b. Timing Analysis for startDate (T2 - startDate)
    df_matched_filtered_with_start = df_matched_filtered[df_matched_filtered['startDate_dt'].notna()].copy()
    start_date_count = len(df_matched_filtered_with_start)
    if start_date_count > 0:
        logger.info(f"\n--- startDate Timing Analysis ---")
        logger.info(f"Markets with startDate (in time range): {start_date_count:,}")
        start_date_diff_series, start_date_summary_text, df_with_start_timing = analyze_timing_custom(df_matched_filtered_with_start, 'startDate_dt', 'startDate')
    else:
        logger.info(f"\n--- startDate Timing Analysis ---")
        logger.info(f"Markets with startDate (in time range): 0")
        start_date_diff_series, start_date_summary_text, df_with_start_timing = None, None, None

    # 8c. Timing Analysis for acceptingOrdersTimestamp (T2 - acceptingOrdersTimestamp)
    df_matched_filtered_with_accepting = df_matched_filtered[df_matched_filtered['acceptingOrdersTimestamp_dt'].notna()].copy()
    accepting_orders_count = len(df_matched_filtered_with_accepting)
    if accepting_orders_count > 0:
        logger.info(f"\n--- acceptingOrdersTimestamp Timing Analysis ---")
        logger.info(f"Markets with acceptingOrdersTimestamp (in time range): {accepting_orders_count:,}")
        accepting_orders_diff_series, accepting_orders_summary_text, df_with_accepting_timing = analyze_timing_custom(df_matched_filtered_with_accepting, 'acceptingOrdersTimestamp_dt', 'acceptingOrdersTimestamp')
    else:
        logger.info(f"\n--- acceptingOrdersTimestamp Timing Analysis ---")
        logger.info(f"Markets with acceptingOrdersTimestamp (in time range): 0")
        accepting_orders_diff_series, accepting_orders_summary_text, df_with_accepting_timing = None, None, None

    # 9. Analyze and Print Oracle Statistics - use filtered data
    # Get oracle totals from ALL matches (before time filter)
    oracle_totals_dict = df_matched_all.groupby('oracle').size().to_dict()
    filtered_oracles = analyze_and_print_stats(df_matched_filtered, total_all_events, oracle_totals_dict)

    # 10. Prepare CDF Data - use combined prepared events, market startDate, and market accepting orders
    plot_data = {}
    
    # CDF 1: createdAt (from CSV) for matched markets in time range
    plot_data['createdAt (CSV)'] = prepare_cdf_data(
        df_matched_filtered, 
        'createdAt_dt', 
        'Market Creation (CSV)'
    )
    
    # CDF 2: Combined Prepared Events - ALL events from oracles with matches in time range
    plot_data['Combined Prepared Events'] = prepare_cdf_data(
        df_B, 
        'timestamp_dt', 
        'Combined Prepared Events'
    )
    
    # CDF 3: Market startDate - events where corresponding markets have startDate in range
    plot_data['Market startDate'] = prepare_cdf_data(
        df_C, 
        'timestamp_dt', 
        'Market startDate'
    )
    
    # CDF 4: Market Accepting Orders - events where corresponding markets have acceptingOrdersTimestamp in range
    plot_data['Market Accepting Orders'] = prepare_cdf_data(
        df_D, 
        'timestamp_dt', 
        'Market Accepting Orders'
    )
    
    # CDF 5: Individual Oracle Events (from Combined Prepared Events)
    for oracle_addr in ORACLE_ALIASES.keys():
        # Use events from this oracle in the time range
        oracle_events = df_B[df_B['oracle'] == oracle_addr]
        if not oracle_events.empty:
            alias = ORACLE_ALIASES.get(oracle_addr, oracle_addr)
            plot_data[f'Oracle {oracle_addr}'] = prepare_cdf_data(
                oracle_events, 
                'timestamp_dt', 
                f'Oracle {alias}'
            )

    # 11. Detect Spike Point in Centralized Adapter
    centralized_adapter = "0x58e1745bedda7312c4cddb72618923da1b90efde"
    spike_point_time = None
    
    centralized_events = df_B[df_B['oracle'] == centralized_adapter].sort_values('timestamp_dt').copy()
    if len(centralized_events) > 300:
        logger.info("\n--- Detecting Centralized Adapter Spike Point ---")
        
        # Process in non-overlapping windows of 300 events
        window_size = 300
        avg_time_diffs = []
        window_starts = []
        
        for i in range(0, len(centralized_events) - window_size + 1, window_size):
            window = centralized_events.iloc[i:i + window_size]
            
            # Calculate time differences between consecutive events
            time_diffs = window['timestamp_dt'].diff().dt.total_seconds().dropna()
            avg_diff = time_diffs.mean()
            
            avg_time_diffs.append(avg_diff)
            window_starts.append(window['timestamp_dt'].iloc[0])
        
        avg_time_diffs = np.array(avg_time_diffs)
        
        logger.info(f"Total non-overlapping windows: {len(avg_time_diffs)}")
        logger.info("Window average time differences (seconds):")
        for idx, (ts, diff) in enumerate(zip(window_starts, avg_time_diffs)):
            logger.info(f"  Window {idx} (start: {ts.strftime('%Y-%m-%d %H:%M:%S')}): {diff:.4f} sec avg")
        
        # Find first significant drop in avg time difference
        spike_idx = -1
        if len(avg_time_diffs) > 1:
            baseline_diff = avg_time_diffs[0]
            
            for i in range(1, len(avg_time_diffs)):
                current_diff = avg_time_diffs[i]
                drop_ratio = baseline_diff / max(current_diff, 0.001)  # ratio of drop
                
                if drop_ratio > 2.0:  # At least 2x reduction in time between events
                    spike_idx = i
                    spike_point_time = window_starts[spike_idx]
                    
                    spike_point_formatted = spike_point_time.strftime('%Y-%m-%d %H:%M:%S UTC')
                    logger.info(f"\nSpike Point Detected at: {spike_point_formatted}")
                    logger.info(f"Baseline avg time diff (window 0): {baseline_diff:.4f} sec")
                    logger.info(f"Spike avg time diff (window {spike_idx}): {current_diff:.4f} sec")
                    logger.info(f"Reduction factor: {drop_ratio:.2f}x")
                    print(f"\n*** SPIKE POINT: {spike_point_formatted} ***")
                    print(f"*** Baseline: {baseline_diff:.4f} sec → Spike: {current_diff:.4f} sec ({drop_ratio:.2f}x faster) ***\n")
                    break
        
        if spike_idx < 0:
            logger.info("Could not detect significant jump in event rate (>2.0x acceleration)")
    else:
        logger.info("Insufficient Centralized Adapter events to detect spike")

    # 12. Plotting
    logger.info("Generating CDF Plot...")
    
    # Plot 1: CDF of Event/Market Arrivals
    plt.figure(figsize=(14, 8))
    
    styles = {
        'createdAt (CSV)':              {'color': 'red', 'linewidth': 3, 'linestyle': '-', 'zorder': 1},
        'Combined Prepared Events':     {'color': 'black', 'linewidth': 2.5, 'linestyle': '--', 'zorder': 3},
        'Market startDate':             {'color': 'blue', 'linewidth': 2, 'linestyle': '-.', 'zorder': 2},
        'Market Accepting Orders':      {'color': 'purple', 'linewidth': 2, 'linestyle': ':', 'zorder': 2},
    }
    oracle_colors = ['brown', 'orange', 'cyan', 'magenta', 'gray']

    oracle_idx = 0
    for key, df in plot_data.items():
        if df.empty:
            logger.warning(f"No data for {key}")
            continue
            
        style = styles.get(key, {})
        
        # Check if it's an individual oracle plot
        if key.startswith('Oracle 0x') and key not in styles:
            # Extract the full oracle address from the key
            full_address = key.split(' ')[1] 
            
            # Get alias or use full address if not found
            alias = ORACLE_ALIASES.get(full_address, full_address)
            
            # Construct the final label using the alias
            plot_label = f"Oracle {alias} (n={len(df):,})"
            
            # Set style for individual oracle
            style = {'color': oracle_colors[oracle_idx % len(oracle_colors)], 'linewidth': 1.5, 'linestyle': '-.', 'zorder': 2}
            oracle_idx += 1
        else:
            # For named sets
            plot_label = f"{df['label'].iloc[0]} (n={len(df):,})"

        plt.plot(df['datetime'], df['count'], label=plot_label, **style)

    # Add vertical line at spike point if detected
    if spike_point_time is not None:
        plt.axvline(spike_point_time, color='red', linestyle='--', linewidth=2, alpha=0.7, label=f'Spike Point: {spike_point_time.strftime("%Y-%m-%d %H:%M:%S")}', zorder=4)

    plt.title('Cumulative Arrival of Unique Condition IDs (CDF)', fontsize=16)
    plt.xlabel('Time (UTC)', fontsize=12)
    plt.ylabel('Cumulative Count of Unique Conditions', fontsize=12)
    plt.grid(True, which='both', linestyle='--', alpha=0.6)
    plt.legend(loc='upper left', fontsize=9)
    
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gcf().autofmt_xdate()

    output_file_cdf = "oracle_market_analysis_cdf.png"
    plt.savefig(output_file_cdf, dpi=300, bbox_inches='tight')
    logger.info(f"CDF Plot saved to {output_file_cdf}")
    
    # Plot 2: PMF Plot of Time Difference (T2 - T1) - Stacked by Oracle
    if time_diff_series is not None and not time_diff_series.empty:
        lower_bound = time_diff_series.quantile(0.000)
        upper_bound = time_diff_series.quantile(0.95)
        
        plot_pmf_overlaid(
            df_with_timing,
            f'PMF of Event Preparation Delay (prepared - createdAt)\n0th-95th Percentile',
            'Time Difference (Seconds)',
            'Probability / Mass (log scale)',
            'timing_difference_pmf.png',
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            bin_width_sec=PMF_BIN_WIDTH_SEC
        )
        plt.close()
    
    # Plot 3: PMF for startDate (T2 - startDate)
    if start_date_diff_series is not None and not start_date_diff_series.empty and df_with_start_timing is not None:
        lower_bound = start_date_diff_series.quantile(0.0001)
        upper_bound = start_date_diff_series.quantile(1.0)
        
        plot_pmf_overlaid(
            df_with_start_timing,
            'PMF of Event Preparation Delay (prepared - startDate)\n0.01th-100th Percentile',
            'Time Difference (Seconds)',
            'Probability / Mass (log scale)',
            'timing_difference_startDate_pmf.png',
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            bin_width_sec=60
        )
        plt.close()
    
    # Plot 4: PMF for acceptingOrdersTimestamp (T2 - acceptingOrdersTimestamp)
    if accepting_orders_diff_series is not None and not accepting_orders_diff_series.empty and df_with_accepting_timing is not None:
        lower_bound = accepting_orders_diff_series.min()
        upper_bound = accepting_orders_diff_series.max()
        
        plot_pmf_overlaid(
            df_with_accepting_timing,
            'PMF of Event Preparation Delay (prepared - acceptingOrdersTimestamp)\nFull Range',
            'Time Difference (Seconds)',
            'Probability / Mass (log scale)',
            'timing_difference_acceptingOrders_pmf.png',
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            bin_width_sec=60
        )
        plt.close()
        
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