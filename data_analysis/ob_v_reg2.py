import asyncio
import asyncpg
import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# --- Configuration ---
DB_CONFIG = {
    "socket_env": "PG_SOCKET", "port_env": "POLY_PG_PORT", "name_env": "POLY_DB",
    "user_env": "POLY_DB_CLI", "pass_env": "POLY_DB_CLI_PASS"
}
TABLES_OF_INTEREST = {"base_ctfe": "events_ctf_exchange", "nr_ctfe": "events_neg_risk_exchange"}
START_RANGE_STR = "Nov-18-2025 07:00:01 AM UTC"
END_RANGE_STR = "Dec-29-2025 02:44:39 PM UTC"

def load_and_clean_csv(csv_path: str):
    """Loads and robustly cleans the CSV for analysis."""
    logger.info(f"Attempting to load CSV from: {csv_path}")
    if not os.path.exists(csv_path):
        logger.critical(f"CSV file not found: {csv_path}"); return None
    
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        logger.info(f"Loaded {len(df):,} rows. Now cleaning and validating...")
    except Exception as e:
        logger.critical(f"Failed to read CSV: {e}"); return None

    df['conditionId'] = df['conditionId'].astype(str).str.lower().apply(lambda x: f'0x{x}' if not x.startswith('0x') else x)
    df['createdAt_dt'] = pd.to_datetime(df['createdAt'], errors='coerce', utc=True)
    
    for attr in ['volumeNum', 'spread', 'bestAsk', 'lastTradePrice']:
        if attr in df.columns:
            df[f'{attr}_cleaned'] = pd.to_numeric(df[attr], errors='coerce')
        else:
            df[f'{attr}_cleaned'] = np.nan
    return df

async def fetch_all_registered_ids(pool):
    """Fetches all unique condition IDs from the CTFE tables."""
    logger.info("Fetching all registered condition IDs from database...")
    all_ids = set()
    for table_name in TABLES_OF_INTEREST.values():
        events = await fetch_token_registered_events(pool, table_name)
        all_ids.update(e['condition_id'] for e in events)
    logger.info(f"Found {len(all_ids):,} total unique registered condition IDs.")
    return all_ids

def print_population_breakdown(df, T, attr_col, condition_desc, condition_mask):
    """
    Prints detailed population breakdown for a specific attribute condition.
    Shows counts for createdAt < T, createdAt >= T, matched/unmatched splits.
    """
    print(f"\n  Population Breakdown for: {condition_desc}")
    
    # Filter by condition
    condition_df = df[condition_mask]
    before_T = condition_df[condition_df['createdAt_dt'] < T]
    at_or_after_T = condition_df[condition_df['createdAt_dt'] >= T]
    
    # Overall counts
    total_condition = len(condition_df)
    matched_condition = condition_df['is_matched'].sum()
    unmatched_condition = total_condition - matched_condition
    
    print(f"    Overall (all time): {total_condition:,} rows")
    print(f"      - Matched: {matched_condition:,}")
    print(f"      - Unmatched: {unmatched_condition:,}")
    
    # Before T
    total_before = len(before_T)
    matched_before = before_T['is_matched'].sum()
    unmatched_before = total_before - matched_before
    
    print(f"    Before T (createdAt < T): {total_before:,} rows")
    print(f"      - Matched: {matched_before:,}")
    print(f"      - Unmatched: {unmatched_before:,}")
    
    # At or after T
    total_after = len(at_or_after_T)
    matched_after = at_or_after_T['is_matched'].sum()
    unmatched_after = total_after - matched_after
    
    print(f"    At or After T (createdAt >= T): {total_after:,} rows")
    print(f"      - Matched: {matched_after:,}")
    print(f"      - Unmatched: {unmatched_after:,}")
    
    return total_after, matched_after, unmatched_after

def find_optimal_epsilon(df, attribute_col, condition_type, objective='maximize', verbose=False):
    """
    Iterates through a range of epsilons to find the one that optimizes
    P(M=unmatched | attribute condition).
    """
    best_epsilon = None
    best_prob = -1.0 if objective == 'maximize' else 2.0
    best_counts = (0, 0, 0)  # (unmatched, matched, total)
    
    # Search over a logarithmic scale for efficiency and precision across magnitudes.
    epsilon_candidates = np.logspace(-9, -1, 100)

    for epsilon in epsilon_candidates:
        if condition_type == 'low': 
            population_df = df[df[attribute_col] < epsilon]
        elif condition_type == 'high': 
            population_df = df[df[attribute_col] >= (1.0 - epsilon)]
        elif condition_type == 'not_low': 
            population_df = df[df[attribute_col] >= epsilon]
        elif condition_type == 'not_high': 
            population_df = df[df[attribute_col] < (1.0 - epsilon)]
        else: 
            continue

        if population_df.empty: 
            continue

        unmatched_count = (~population_df['is_matched']).sum()
        matched_count = population_df['is_matched'].sum()
        total_count = len(population_df)
        current_prob = unmatched_count / total_count

        if objective == 'maximize' and current_prob > best_prob:
            best_prob = current_prob
            best_epsilon = epsilon
            best_counts = (unmatched_count, matched_count, total_count)
        elif objective == 'minimize' and current_prob < best_prob:
            best_prob = current_prob
            best_epsilon = epsilon
            best_counts = (unmatched_count, matched_count, total_count)
            
    return best_epsilon, best_prob, best_counts

def print_population_breakdown(df, T, attr_col, condition_desc, condition_mask):
    """
    Prints detailed population breakdown for a specific attribute condition.
    Shows counts for createdAt < T, createdAt >= T, matched/unmatched splits.
    """
    print(f"\n  Population Breakdown for: {condition_desc}")
    
    # Filter by condition
    condition_df = df[condition_mask]
    before_T = condition_df[condition_df['createdAt_dt'] < T]
    at_or_after_T = condition_df[condition_df['createdAt_dt'] >= T]
    
    # Overall counts
    total_condition = len(condition_df)
    matched_condition = condition_df['is_matched'].sum()
    unmatched_condition = total_condition - matched_condition
    
    print(f"    Overall (all time): {total_condition:,} rows")
    print(f"      - Matched: {matched_condition:,}")
    print(f"      - Unmatched: {unmatched_condition:,}")
    
    # Before T
    total_before = len(before_T)
    matched_before = before_T['is_matched'].sum()
    unmatched_before = total_before - matched_before
    
    print(f"    Before T (createdAt < T): {total_before:,} rows")
    print(f"      - Matched: {matched_before:,}")
    print(f"      - Unmatched: {unmatched_before:,}")
    
    # At or after T
    total_after = len(at_or_after_T)
    matched_after = at_or_after_T['is_matched'].sum()
    unmatched_after = total_after - matched_after
    
    print(f"    At or After T (createdAt >= T): {total_after:,} rows")
    print(f"      - Matched: {matched_after:,}")
    print(f"      - Unmatched: {unmatched_after:,}")
    
    return total_after, matched_after, unmatched_after

async def main():
    csv_path = os.path.join(os.environ.get('POLY_CSV', ''), 'gamma_markets.csv')
    df = load_and_clean_csv(csv_path)
    if df is None: return

    pool = await get_db_pool(DB_CONFIG)
    if not pool: return
    try:
        registered_ids = await fetch_all_registered_ids(pool)
    finally:
        await pool.close()
        logger.info("Database connection closed.")

    df['is_matched'] = df['conditionId'].isin(registered_ids)
    start_dt = pd.to_datetime(START_RANGE_STR, utc=True)
    end_dt = pd.to_datetime(END_RANGE_STR, utc=True)
    
    # --- Full dataset within time range ---
    analysis_df = df[df['createdAt_dt'].between(start_dt, end_dt)].copy()
    
    # --- Analyze unmatched row distribution over time ---
    unmatched_markets = analysis_df[~analysis_df['is_matched']].copy()
    
    if unmatched_markets.empty:
        T = pd.Timestamp.now(tz='UTC')
        print("WARNING: No unmatched rows found in range!")
    else:
        # Sort unmatched markets by creation time
        unmatched_sorted = unmatched_markets.sort_values('createdAt_dt').reset_index(drop=True)
        total_unmatched = len(unmatched_sorted)
        
        print("\n" + "="*100)
        print(" " * 30 + "UNMATCHED MARKETS TIME DISTRIBUTION ANALYSIS")
        print("="*100)
        print(f"\nTotal unmatched markets in range: {total_unmatched:,}")
        print(f"First unmatched market: {unmatched_sorted['createdAt_dt'].iloc[0].strftime('%Y-%m-%d %H:%M:%S %Z')}")
        print(f"Last unmatched market: {unmatched_sorted['createdAt_dt'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S %Z')}")
        
        # Find the window of N consecutive unmatched markets with smallest time span
        print("\n" + "-"*100)
        print("Finding Sharp Increase Point (highest density window of unmatched markets):")
        print("-"*100)
        
        window_size = 100  # Look for 100 consecutive unmatched markets
        
        if len(unmatched_sorted) >= window_size:
            min_time_span = None
            best_window_start = 0
            
            # Slide a window through the data to find the densest cluster
            print(f"\nSearching for window of {window_size} consecutive unmatched markets with minimum time span...")
            
            for i in range(len(unmatched_sorted) - window_size + 1):
                window_start_time = unmatched_sorted['createdAt_dt'].iloc[i]
                window_end_time = unmatched_sorted['createdAt_dt'].iloc[i + window_size - 1]
                time_span = window_end_time - window_start_time
                
                if min_time_span is None or time_span < min_time_span:
                    min_time_span = time_span
                    best_window_start = i
            
            T = unmatched_sorted['createdAt_dt'].iloc[best_window_start]
            window_end_time = unmatched_sorted['createdAt_dt'].iloc[best_window_start + window_size - 1]
            
            print(f"\nDensest window found:")
            print(f"  Starting at market #{best_window_start} (index in sorted unmatched)")
            print(f"  Time range: {T.strftime('%Y-%m-%d %H:%M:%S %Z')} to {window_end_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"  Time span: {min_time_span.total_seconds()/3600:.2f} hours for {window_size} markets")
            print(f"  Rate: {window_size / (min_time_span.total_seconds()/3600):.2f} markets/hour")
            print(f"\n  Setting T (cutoff time): {T.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            
            # Show statistics before and after
            unmatched_before_cutoff = best_window_start
            unmatched_after_cutoff = total_unmatched - best_window_start
            
            matched_before = analysis_df[(analysis_df['createdAt_dt'] < T) & (analysis_df['is_matched'])]
            total_before = len(matched_before) + unmatched_before_cutoff
            
            print(f"\n  Markets before T:")
            print(f"    - Unmatched: {unmatched_before_cutoff:,} ({100*unmatched_before_cutoff/total_unmatched:.2f}% of all unmatched)")
            print(f"    - Matched: {len(matched_before):,}")
            print(f"    - Total: {total_before:,}")
            print(f"    - % Unmatched before T: {100*unmatched_before_cutoff/total_before:.2f}%")
            print(f"  Markets at/after T:")
            print(f"    - Unmatched: {unmatched_after_cutoff:,} ({100*unmatched_after_cutoff/total_unmatched:.2f}% of all unmatched)")
        else:
            # Fallback for small datasets
            T = unmatched_markets['createdAt_dt'].min()
            print(f"\nInsufficient unmatched markets ({len(unmatched_sorted)}) for window analysis (need >= {window_size}).")
            print(f"Using first unmatched row: {T.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    print("\n" + "="*100)
    print(" " * 35 + "INITIAL POPULATION STATISTICS")
    print("="*100)
    
    # Total population statistics
    total_rows = len(analysis_df)
    total_matched = analysis_df['is_matched'].sum()
    total_unmatched = total_rows - total_matched
    
    print(f"\nAnalysis Time Range: {start_dt.strftime('%Y-%m-%d %H:%M:%S %Z')} to {end_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"\nT (Time of First Unmatched Row): {T.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"\nTotal Population in Time Range: {total_rows:,} rows")
    print(f"  - Matched: {total_matched:,} ({100*total_matched/total_rows:.2f}%)")
    print(f"  - Unmatched: {total_unmatched:,} ({100*total_unmatched/total_rows:.2f}%)")
    
    # Before T statistics
    before_T = analysis_df[analysis_df['createdAt_dt'] < T]
    before_T_total = len(before_T)
    before_T_matched = before_T['is_matched'].sum()
    before_T_unmatched = before_T_total - before_T_matched
    
    print(f"\nPopulation Before T (createdAt < T): {before_T_total:,} rows")
    print(f"  - Matched: {before_T_matched:,} ({100*before_T_matched/before_T_total if before_T_total > 0 else 0:.2f}%)")
    print(f"  - Unmatched: {before_T_unmatched:,} ({100*before_T_unmatched/before_T_total if before_T_total > 0 else 0:.2f}%)")
    
    # At or after T statistics
    at_or_after_T = analysis_df[analysis_df['createdAt_dt'] >= T]
    after_T_total = len(at_or_after_T)
    after_T_matched = at_or_after_T['is_matched'].sum()
    after_T_unmatched = after_T_total - after_T_matched
    
    print(f"\nPopulation At or After T (createdAt >= T): {after_T_total:,} rows")
    print(f"  - Matched: {after_T_matched:,} ({100*after_T_matched/after_T_total if after_T_total > 0 else 0:.2f}%)")
    print(f"  - Unmatched: {after_T_unmatched:,} ({100*after_T_unmatched/after_T_total if after_T_total > 0 else 0:.2f}%)")
    
    print("="*100)
    
    # --- CONDITIONAL PROBABILITY ANALYSIS FOR EACH ATTRIBUTE ---
    attributes = [
        ('volumeNum', 'low'),   # volume < eps* (low values interesting)
        ('spread', 'high'),      # spread >= 1.0 - eps* (high values interesting, range [0,1])
        ('bestAsk', 'high'),      # bestAsk >= 1.0 - eps* (high values interesting, range [0,1])
        ('lastTradePrice', 'mixed')  # Special handling: missing, <= 0, > 0
    ]
    
    for attr, direction in attributes:
        attr_col = f"{attr}_cleaned"
        
        print("\n\n" + "="*100)
        print(f" " * 40 + f"ANALYSIS FOR: {attr.upper()}")
        print("="*100)
        
        # Special handling for last_trade_price
        if direction == 'mixed':
            print(f"\n[SPECIAL ANALYSIS: last_trade_price with missing/non-positive/positive categories]")
            
            # First, let's check if the column exists and diagnose the data
            print("\n" + "-"*100)
            print("DIAGNOSTIC: Checking last_trade_price column")
            print("-"*100)
            
            if attr_col not in analysis_df.columns:
                print(f"ERROR: Column '{attr_col}' not found in dataframe!")
                print(f"Available columns: {[c for c in analysis_df.columns if 'trade' in c.lower() or 'price' in c.lower()]}")
                continue
            
            print(f"Column exists: {attr_col}")
            print(f"\nData type: {analysis_df[attr_col].dtype}")
            print(f"\nStatistics for entire analysis_df:")
            print(f"  Total rows: {len(analysis_df):,}")
            print(f"  NaN values: {analysis_df[attr_col].isna().sum():,}")
            print(f"  Non-NaN values: {analysis_df[attr_col].notna().sum():,}")
            
            # Before T statistics
            before_T_df = analysis_df[analysis_df['createdAt_dt'] < T]
            print(f"\nBefore T (createdAt < {T.strftime('%Y-%m-%d %H:%M:%S')}):")
            print(f"  Total rows: {len(before_T_df):,}")
            print(f"  NaN values: {before_T_df[attr_col].isna().sum():,}")
            print(f"  Non-NaN values: {before_T_df[attr_col].notna().sum():,}")
            non_nan_before = before_T_df[before_T_df[attr_col].notna()]
            if len(non_nan_before) > 0:
                print(f"  Sample non-NaN values (before T): {non_nan_before[attr_col].head(5).tolist()}")
                print(f"  Min: {non_nan_before[attr_col].min()}, Max: {non_nan_before[attr_col].max()}")
            
            # After T statistics
            print(f"\nAfter T (createdAt >= {T.strftime('%Y-%m-%d %H:%M:%S')}):")
            print(f"  Total rows: {len(at_or_after_T):,}")
            print(f"  NaN values: {at_or_after_T[attr_col].isna().sum():,}")
            print(f"  Non-NaN values: {at_or_after_T[attr_col].notna().sum():,}")
            non_nan_after = at_or_after_T[at_or_after_T[attr_col].notna()]
            if len(non_nan_after) > 0:
                print(f"  Sample non-NaN values (after T): {non_nan_after[attr_col].head(5).tolist()}")
                print(f"  Min: {non_nan_after[attr_col].min()}, Max: {non_nan_after[attr_col].max()}")
            
            # Categories: missing (NaN), <= 0, > 0
            print("\n" + "-"*100)
            print("ANALYSIS BY CATEGORY")
            print("-"*100)
            
            # Before T breakdown
            missing_before = before_T_df[attr_col].isna().sum()
            nonpositive_before = ((before_T_df[attr_col] <= 0) & (before_T_df[attr_col].notna())).sum()
            positive_before = ((before_T_df[attr_col] > 0) & (before_T_df[attr_col].notna())).sum()
            
            print(f"\nBefore T breakdown:")
            print(f"  - Missing (NaN): {missing_before:,}")
            print(f"  - Non-positive (<= 0): {nonpositive_before:,}")
            print(f"  - Positive (> 0): {positive_before:,}")
            print(f"  - Total: {missing_before + nonpositive_before + positive_before:,}")
            
            # After T breakdown
            missing_after = at_or_after_T[attr_col].isna().sum()
            nonpositive_after = ((at_or_after_T[attr_col] <= 0) & (at_or_after_T[attr_col].notna())).sum()
            positive_after = ((at_or_after_T[attr_col] > 0) & (at_or_after_T[attr_col].notna())).sum()
            
            print(f"\nAfter T breakdown:")
            print(f"  - Missing (NaN): {missing_after:,}")
            print(f"  - Non-positive (<= 0): {nonpositive_after:,}")
            print(f"  - Positive (> 0): {positive_after:,}")
            print(f"  - Total: {missing_after + nonpositive_after + positive_after:,}")
            
            # ==================== MISSING VALUES ANALYSIS ====================
            print("\n" + "-"*100)
            print("1. P(M=unmatched | createdAt >= T, last_trade_price missing)")
            print("-"*100)
            
            missing_df = at_or_after_T[at_or_after_T[attr_col].isna()]
            if len(missing_df) > 0:
                unmatched_missing = (~missing_df['is_matched']).sum()
                matched_missing = missing_df['is_matched'].sum()
                total_missing = len(missing_df)
                prob_unmatched_missing = unmatched_missing / total_missing
                
                print(f"  Population with missing last_trade_price (createdAt >= T): {total_missing:,}")
                print(f"    - Matched: {matched_missing:,}")
                print(f"    - Unmatched: {unmatched_missing:,}")
                print(f"  P(M=unmatched | createdAt >= T, last_trade_price missing) = {prob_unmatched_missing:.6f} ({prob_unmatched_missing*100:.2f}%)")
            else:
                print("  No rows with missing last_trade_price after T")
            
            # Also show before T for comparison
            missing_df_before = before_T_df[before_T_df[attr_col].isna()]
            if len(missing_df_before) > 0:
                unmatched_missing_before = (~missing_df_before['is_matched']).sum()
                matched_missing_before = missing_df_before['is_matched'].sum()
                total_missing_before = len(missing_df_before)
                prob_unmatched_missing_before = unmatched_missing_before / total_missing_before
                
                print(f"\n  For comparison - Before T:")
                print(f"  Population with missing last_trade_price (createdAt < T): {total_missing_before:,}")
                print(f"    - Matched: {matched_missing_before:,}")
                print(f"    - Unmatched: {unmatched_missing_before:,}")
                print(f"  P(M=unmatched | createdAt < T, last_trade_price missing) = {prob_unmatched_missing_before:.6f} ({prob_unmatched_missing_before*100:.2f}%)")
            
            # ==================== NON-POSITIVE VALUES EPSILON OPTIMIZATION ====================
            print("\n" + "-"*100)
            print("2. Find eps* to maximize P(M=unmatched | createdAt >= T, last_trade_price <= eps*)")
            print("-"*100)
            
            df_after_T_nonmissing = at_or_after_T[at_or_after_T[attr_col].notna()].copy()
            
            if len(df_after_T_nonmissing) > 0:
                eps_star_ltp, prob_star_ltp, counts_star_ltp = find_optimal_epsilon(
                    df_after_T_nonmissing, attr_col, 'low', 'maximize'
                )
                
                if eps_star_ltp is not None:
                    print(f"  Optimal eps* = {eps_star_ltp:.9f}")
                    cond_star_ltp_desc = f"last_trade_price <= {eps_star_ltp:.9f}"
                    print(f"  Condition: {cond_star_ltp_desc}")
                    print(f"  Population meeting condition (createdAt >= T, non-missing): {counts_star_ltp[2]:,}")
                    print(f"    - Matched: {counts_star_ltp[1]:,}")
                    print(f"    - Unmatched: {counts_star_ltp[0]:,}")
                    print(f"  P(M=unmatched | createdAt >= T, {cond_star_ltp_desc}) = {prob_star_ltp:.6f} ({prob_star_ltp*100:.2f}%)")
                else:
                    print("  No optimal epsilon found.")
                    eps_star_ltp = None
            else:
                print("  No non-missing last_trade_price data after T")
                eps_star_ltp = None
            
            # ==================== POSITIVE VALUES EPSILON OPTIMIZATION ====================
            print("\n" + "-"*100)
            print("3. Find eps** to minimize P(M=unmatched | createdAt >= T, last_trade_price >= eps**)")
            print("-"*100)
            
            if len(df_after_T_nonmissing) > 0:
                eps_star_star_ltp, prob_star_star_ltp, counts_star_star_ltp = find_optimal_epsilon(
                    df_after_T_nonmissing, attr_col, 'high', 'minimize'
                )
                
                if eps_star_star_ltp is not None:
                    print(f"  Optimal eps** = {eps_star_star_ltp:.9f}")
                    cond_star_star_ltp_desc = f"lastTradePrice >= {eps_star_star_ltp:.9f}"
                    print(f"  Condition: {cond_star_star_ltp_desc}")
                    print(f"  Population meeting condition (createdAt >= T, non-missing): {counts_star_star_ltp[2]:,}")
                    print(f"    - Matched: {counts_star_star_ltp[1]:,}")
                    print(f"    - Unmatched: {counts_star_star_ltp[0]:,}")
                    print(f"  P(M=unmatched | createdAt >= T, {cond_star_star_ltp_desc}) = {prob_star_star_ltp:.6f} ({prob_star_star_ltp*100:.2f}%)")
                else:
                    print("  No optimal epsilon found.")
                    eps_star_star_ltp = None
            else:
                print("  No non-missing last_trade_price data after T")
                eps_star_star_ltp = None
            
            # ==================== COMBINED CONDITION: MISSING OR NON-POSITIVE ====================
            print("\n" + "-"*100)
            print("4. P(last_trade_price missing OR last_trade_price <= eps* | createdAt >= T, M=unmatched)")
            print("-"*100)
            
            unmatched_after_T_df = at_or_after_T[~at_or_after_T['is_matched']]
            if len(unmatched_after_T_df) > 0:
                missing_unmatched = unmatched_after_T_df[attr_col].isna().sum()
                
                if eps_star_ltp is not None:
                    nonpositive_unmatched = (unmatched_after_T_df[attr_col] <= eps_star_ltp).sum()
                    combined_unmatched = unmatched_after_T_df[
                        (unmatched_after_T_df[attr_col].isna()) | 
                        (unmatched_after_T_df[attr_col] <= eps_star_ltp)
                    ]
                    combined_count = len(combined_unmatched)
                else:
                    nonpositive_unmatched = 0
                    combined_count = missing_unmatched
                
                prob_combined_given_unmatched = combined_count / len(unmatched_after_T_df)
                
                print(f"  Among unmatched rows (createdAt >= T): {len(unmatched_after_T_df):,}")
                print(f"    - With missing last_trade_price: {missing_unmatched:,}")
                if eps_star_ltp is not None:
                    print(f"    - With last_trade_price <= {eps_star_ltp:.9f}: {nonpositive_unmatched:,}")
                    print(f"    - Combined (missing OR <= eps*): {combined_count:,}")
                print(f"  P(missing OR <= eps* | createdAt >= T, M=unmatched) = {prob_combined_given_unmatched:.6f} ({prob_combined_given_unmatched*100:.2f}%)")
            else:
                print("  No unmatched rows after T")
        
        else:
            # Original handling for continuous attributes
            # Filter to rows after T with valid attribute values
            df_after_T = at_or_after_T[at_or_after_T[attr_col].notna()].copy()
            
            if len(df_after_T) == 0:
                print(f"\nWARNING: No valid {attr} data for rows with createdAt >= T")
                continue
            
            print(f"\nRows with createdAt >= T and valid {attr}: {len(df_after_T):,}")
            unmatched_after_T_count = (~df_after_T['is_matched']).sum()
            matched_after_T_count = df_after_T['is_matched'].sum()
            print(f"  - Matched: {matched_after_T_count:,}")
            print(f"  - Unmatched: {unmatched_after_T_count:,}")
            
            # ==================== EPSILON OPTIMIZATION ====================
            print("\n" + "-"*100)
            print("EPSILON OPTIMIZATION")
            print("-"*100)
            
            # Find eps* (maximize P(M=unmatched | condition))
            if direction == 'low':
                eps_star, prob_star, counts_star = find_optimal_epsilon(
                    df_after_T, attr_col, 'low', 'maximize'
                )
                cond_star_desc = f"{attr} < {eps_star:.9f}" if eps_star is not None else "N/A"
            else:  # high
                eps_star, prob_star, counts_star = find_optimal_epsilon(
                    df_after_T, attr_col, 'high', 'maximize'
                )
                cond_star_desc = f"{attr} >= {1.0 - eps_star:.9f}" if eps_star is not None else "N/A"
            
            print(f"\n[MAXIMIZATION] Find eps* to maximize P(M=unmatched | createdAt >= T, condition)")
            if eps_star is not None:
                print(f"  Optimal eps* = {eps_star:.9f}")
                print(f"  Condition: {cond_star_desc}")
                print(f"  Population meeting condition (createdAt >= T): {counts_star[2]:,}")
                print(f"    - Matched: {counts_star[1]:,}")
                print(f"    - Unmatched: {counts_star[0]:,}")
                print(f"  P(M=unmatched | createdAt >= T, {cond_star_desc}) = {prob_star:.6f} ({prob_star*100:.2f}%)")
            else:
                print("  No optimal epsilon found.")
            
            # Find eps** (minimize P(M=unmatched | opposite condition))
            if direction == 'low':
                eps_star_star, prob_star_star, counts_star_star = find_optimal_epsilon(
                    df_after_T, attr_col, 'not_low', 'minimize'
                )
                cond_star_star_desc = f"{attr} >= {eps_star_star:.9f}" if eps_star_star is not None else "N/A"
            else:  # high
                eps_star_star, prob_star_star, counts_star_star = find_optimal_epsilon(
                    df_after_T, attr_col, 'not_high', 'minimize'
                )
                cond_star_star_desc = f"{attr} < {eps_star_star:.9f}" if eps_star_star is not None else "N/A"
            
            print(f"\n[MINIMIZATION] Find eps** to minimize P(M=unmatched | createdAt >= T, opposite condition)")
            if eps_star_star is not None:
                print(f"  Optimal eps** = {eps_star_star:.9f}")
                print(f"  Condition: {cond_star_star_desc}")
                print(f"  Population meeting condition (createdAt >= T): {counts_star_star[2]:,}")
                print(f"    - Matched: {counts_star_star[1]:,}")
                print(f"    - Unmatched: {counts_star_star[0]:,}")
                print(f"  P(M=unmatched | createdAt >= T, {cond_star_star_desc}) = {prob_star_star:.6f} ({prob_star_star*100:.2f}%)")
            else:
                print("  No optimal epsilon found.")
            
            # ==================== DETAILED CONDITIONAL PROBABILITIES ====================
            print("\n" + "-"*100)
            print("DETAILED CONDITIONAL PROBABILITY ANALYSIS")
            print("-"*100)
            
            if eps_star is not None:
                # Create condition masks
                if direction == 'low':
                    mask_star = analysis_df[attr_col] < eps_star
                else:
                    mask_star = analysis_df[attr_col] >= (1.0 - eps_star)
                
                # P(M=unmatched | createdAt >= T, condition) - already computed above as prob_star
                print(f"\n1. P(M=unmatched | createdAt >= T, {cond_star_desc})")
                print(f"   = {prob_star:.6f} ({prob_star*100:.2f}%)")
                print(f"   [Already computed during optimization]")
                
                # Show detailed breakdown for this condition
                print_population_breakdown(analysis_df, T, attr_col, cond_star_desc, mask_star)
                
                # P(condition | createdAt >= T, M=unmatched)
                unmatched_after_T_df = df_after_T[~df_after_T['is_matched']]
                if len(unmatched_after_T_df) > 0:
                    if direction == 'low':
                        cond_met_unmatched = (unmatched_after_T_df[attr_col] < eps_star).sum()
                    else:
                        cond_met_unmatched = (unmatched_after_T_df[attr_col] >= (1.0 - eps_star)).sum()
                    
                    prob_cond_given_unmatched = cond_met_unmatched / len(unmatched_after_T_df)
                    
                    print(f"\n2. P({cond_star_desc} | createdAt >= T, M=unmatched)")
                    print(f"   = {prob_cond_given_unmatched:.6f} ({prob_cond_given_unmatched*100:.2f}%)")
                    print(f"   Calculation: {cond_met_unmatched:,} / {len(unmatched_after_T_df):,}")
                    print(f"   - Numerator: Unmatched rows (createdAt >= T) meeting {cond_star_desc}")
                    print(f"   - Denominator: All unmatched rows with createdAt >= T")
                else:
                    print(f"\n2. P({cond_star_desc} | createdAt >= T, M=unmatched): Cannot compute (no unmatched rows)")
            
            if eps_star_star is not None:
                # Create condition masks for opposite condition
                if direction == 'low':
                    mask_star_star = analysis_df[attr_col] >= eps_star_star
                else:
                    mask_star_star = analysis_df[attr_col] < (1.0 - eps_star_star)
                
                # P(M=unmatched | createdAt >= T, opposite condition) - already computed as prob_star_star
                print(f"\n3. P(M=unmatched | createdAt >= T, {cond_star_star_desc})")
                print(f"   = {prob_star_star:.6f} ({prob_star_star*100:.2f}%)")
                print(f"   [Already computed during optimization]")
                
                # Show detailed breakdown for opposite condition
                print_population_breakdown(analysis_df, T, attr_col, cond_star_star_desc, mask_star_star)
                
                # P(opposite condition | createdAt >= T, M=unmatched)
                unmatched_after_T_df = df_after_T[~df_after_T['is_matched']]
                if len(unmatched_after_T_df) > 0:
                    if direction == 'low':
                        cond_met_unmatched_opp = (unmatched_after_T_df[attr_col] >= eps_star_star).sum()
                    else:
                        cond_met_unmatched_opp = (unmatched_after_T_df[attr_col] < (1.0 - eps_star_star)).sum()
                    
                    prob_cond_opp_given_unmatched = cond_met_unmatched_opp / len(unmatched_after_T_df)
                    
                    print(f"\n4. P({cond_star_star_desc} | createdAt >= T, M=unmatched)")
                    print(f"   = {prob_cond_opp_given_unmatched:.6f} ({prob_cond_opp_given_unmatched*100:.2f}%)")
                    print(f"   Calculation: {cond_met_unmatched_opp:,} / {len(unmatched_after_T_df):,}")
                    print(f"   - Numerator: Unmatched rows (createdAt >= T) meeting {cond_star_star_desc}")
                    print(f"   - Denominator: All unmatched rows with createdAt >= T")
                else:
                    print(f"\n4. P({cond_star_star_desc} | createdAt >= T, M=unmatched): Cannot compute (no unmatched rows)")

    
    print("\n" + "="*100)
    print(" " * 40 + "ANALYSIS COMPLETE")
    print("="*100)

# Helper functions
async def get_db_pool(db_config: dict):
    try:
        for key in db_config.values():
            if key not in os.environ: logger.error(f"Missing env var: {key}"); return None
        return await asyncpg.create_pool(
            host=os.environ.get(db_config['socket_env']), port=os.environ.get(db_config['port_env']),
            database=os.environ.get(db_config['name_env']), user=os.environ.get(db_config['user_env']),
            password=os.environ.get(db_config['pass_env']),
        )
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}"); raise

async def fetch_token_registered_events(pool, table_name: str):
    query = f"SELECT topics FROM {table_name} WHERE event_name = 'TokenRegistered'"
    events = []
    async with pool.acquire() as conn:
        async with conn.transaction():
            async for record in conn.cursor(query):
                try:
                    raw_topic = record['topics'][3]
                    hex_string = raw_topic.hex() if isinstance(raw_topic, bytes) else raw_topic.lower().replace('0x', '')
                    if len(hex_string) == 64:
                        events.append({"condition_id": f"0x{hex_string}"})
                except: continue
    return events

if __name__ == "__main__":
    asyncio.run(main())