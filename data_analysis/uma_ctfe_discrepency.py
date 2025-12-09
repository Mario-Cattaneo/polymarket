import os
import asyncio
import asyncpg
import pandas as pd
import numpy as np
from scipy.stats import pearsonr, spearmanr, wasserstein_distance # Added wasserstein_distance
from dtw import dtw
import logging

# ----------------------------------------------------------------
# PREREQUISITE:
# pip install pandas "asyncpg<0.29.0" scipy numpy dtw-python
# ----------------------------------------------------------------

# --- Set your database credentials as environment variables ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- Configuration for the analysis ---
FORK_MARKET_TABLE = "markets_2"
CTFE_EXCHANGE_ADDRESS = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e".lower()
MOO_UMA_ADAPTER_ADDRESS = "0x65070BE91477460D8A7AeEb94ef92fe056C2f2A7".lower()

# --- Setup basic logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()


def analyze_similarity(df1, df2, name1, name2):
    """
    Computes and prints correlation, DTW, and Wasserstein distance for two event series.
    """
    logger.info(f"--- Analyzing Similarity: '{name1}' vs '{name2}' ---")

    if df1.empty or df2.empty:
        logger.error("One or both series are empty. Cannot perform analysis.")
        return

    # --- 1. Create Cumulative Distribution Functions (CDFs) ---
    cdf1 = df1.sort_values('time').reset_index(drop=True).rename_axis('count').reset_index(); cdf1['count'] += 1
    cdf2 = df2.sort_values('time').reset_index(drop=True).rename_axis('count').reset_index(); cdf2['count'] += 1

    # Resolve duplicate timestamps by taking the max count for each time
    cdf1 = cdf1.groupby('time').max().reset_index()
    cdf2 = cdf2.groupby('time').max().reset_index()

    # --- 2. Align Time Series on a common time index ---
    cdf1 = cdf1.set_index('time'); cdf2 = cdf2.set_index('time')
    combined_index = cdf1.index.union(cdf2.index)
    s1 = cdf1['count'].reindex(combined_index, method='ffill').fillna(0)
    s2 = cdf2['count'].reindex(combined_index, method='ffill').fillna(0)

    # --- 3. Calculate Correlation Coefficients ---
    pearson_corr, _ = pearsonr(s1, s2)
    spearman_corr, _ = spearmanr(s1, s2)

    print("\n--- Correlation Analysis (Direction) ---")
    print(f"Pearson Correlation: {pearson_corr:.4f}")
    print(f"Spearman Correlation:  {spearman_corr:.4f}")
    print("(Measures if the series trend together. Closer to +1 is a stronger relationship.)")

    # --- 4. Calculate Dynamic Time Warping (DTW) for Shape Similarity ---
    s1_norm = (s1 - s1.mean()) / s1.std(); s2_norm = (s2 - s2.mean()) / s2.std()
    dtw_result = dtw(s1_norm.to_numpy(), s2_norm.to_numpy())

    print("\n--- Shape Similarity (DTW) ---")
    print(f"Normalized DTW Distance: {dtw_result.normalizedDistance:.4f}")
    print("(Measures shape similarity. Closer to 0 means the shapes are more similar.)")

    # --- 5. Calculate Wasserstein Distance for Magnitude Separation ---
    # This directly measures the "area between the curves" of the two CDFs.
    w_dist = wasserstein_distance(s1, s2)

    print("\n--- Magnitude Separation (Wasserstein Distance) ---")
    print(f"Wasserstein Distance: {w_dist:,.2f}")
    print("(Measures the total 'area' or gap between the two functions. A larger value\n quantifies a greater overall separation, capturing the increasing vertical distance.)")
    print("\n" + "="*60 + "\n")


async def main():
    """
    Main function to connect to DB, fetch data, and run analysis.
    """
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Successfully connected to the database.")

    # --- Step 1: Get the authoritative time range for Fork 2 from the markets_2 table ---
    time_range_query = f"SELECT MIN(found_time_ms) as min_ts, MAX(found_time_ms) as max_ts FROM {FORK_MARKET_TABLE} WHERE exhaustion_cycle != 0"
    time_range = await pool.fetchrow(time_range_query)
    
    if not time_range or not time_range['min_ts']:
        logger.error(f"Could not determine time range from '{FORK_MARKET_TABLE}'. Exiting.")
        await pool.close()
        return
        
    min_ts, max_ts = time_range['min_ts'], time_range['max_ts']
    logger.info(f"Time range for Fork 2 established: {pd.to_datetime(min_ts, unit='ms', utc=True)} to {pd.to_datetime(max_ts, unit='ms', utc=True)}")

    # --- Step 2: Fetch 'CTFE Tokens Registered' events ---
    ctfe_query = "SELECT timestamp_ms FROM events_ctf_exchange WHERE event_name = 'TokenRegistered' AND LOWER(contract_address) = $1 AND timestamp_ms BETWEEN $2 AND $3"
    ctfe_rows = await pool.fetch(ctfe_query, CTFE_EXCHANGE_ADDRESS, min_ts, max_ts)
    ctfe_df = pd.DataFrame(ctfe_rows, columns=['timestamp_ms']); ctfe_df['time'] = pd.to_datetime(ctfe_df['timestamp_ms'], unit='ms', utc=True)
    logger.info(f"Fetched {len(ctfe_df)} events for 'CTFE Tokens Registered'.")

    # --- Step 3: Fetch 'MOO UMA QuestionInitialized' events ---
    moo_uma_query = "SELECT timestamp_ms FROM events_uma_adapter WHERE event_name = 'QuestionInitialized' AND LOWER(contract_address) = $1 AND timestamp_ms BETWEEN $2 AND $3"
    moo_uma_rows = await pool.fetch(moo_uma_query, MOO_UMA_ADAPTER_ADDRESS, min_ts, max_ts)
    moo_uma_df = pd.DataFrame(moo_uma_rows, columns=['timestamp_ms']); moo_uma_df['time'] = pd.to_datetime(moo_uma_df['timestamp_ms'], unit='ms', utc=True)
    
    doubled_moo_uma_df = pd.concat([moo_uma_df, moo_uma_df], ignore_index=True)
    logger.info(f"Fetched {len(moo_uma_df)} events for 'MOO UMA' (total {len(doubled_moo_uma_df)} for analysis).")

    # --- Step 4: Run the analysis ---
    analyze_similarity(
        df1=ctfe_df,
        df2=doubled_moo_uma_df,
        name1="CTFE Tokens Registered",
        name2="2 * MOO UMA (QuestionInitialized)"
    )

    await pool.close()
    logger.info("Database connection closed.")


if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials are not set. Please set PG_SOCKET, POLY_DB, POLY_DB_CLI, and POLY_DB_CLI_PASS environment variables.")
    else:
        asyncio.run(main())