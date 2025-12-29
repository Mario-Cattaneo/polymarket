import asyncio
import asyncpg
import os
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timezone
from collections import defaultdict

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Configuration ---
DB_CONFIG = {
    "socket_env": "PG_SOCKET",
    "port_env": "POLY_PG_PORT",
    "name_env": "POLY_DB",
    "user_env": "POLY_DB_CLI",
    "pass_env": "POLY_DB_CLI_PASS"
}

ORACLES_OF_INTEREST = {
    "0x58e1745bedda7312c4cddb72618923da1b90efde": "Prep Oracle @ 0x58e1",
    "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7": "Prep Oracle @ 0x6507",
    "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296": "Prep Oracle @ 0xd91e"
}

EXPECTED_COLUMNS = [
    'parent_event_id', 'market_id', 'token_id', 'condition_id', 'question', 'resolution_source',
    'end_date', 'category', 'start_date', 'fee', 'active', 'marketType', 'closed',
    'created_at', 'updated_at', 'closed_time', 'submitted_by', 'archived',
    'resolved_by', 'restricted', 'has_reviewed_end_dates', 'sentDiscord', 'uma_resolution_statuses'
]
DATE_COLUMNS = ['end_date', 'start_date', 'created_at', 'updated_at', 'closed_time']


# --- Helper Functions ---
async def get_db_pool(db_config: dict):
    """Creates an asyncpg database connection pool."""
    try:
        for key in db_config.values():
            if key not in os.environ:
                logger.error(f"Missing required environment variable: {key}")
                return None
        return await asyncpg.create_pool(
            host=os.environ.get(db_config['socket_env']),
            port=os.environ.get(db_config['port_env']),
            database=os.environ.get(db_config['name_env']),
            user=os.environ.get(db_config['user_env']),
            password=os.environ.get(db_config['pass_env']),
        )
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        raise

def load_and_analyze_csv(csv_path: str):
    """
    Loads the gamma_markets.csv, reports on data quality (including samples of
    malformed dates), and prepares the data for analysis using a robust parsing method.
    """
    logger.info(f"Attempting to load CSV from: {csv_path}")
    if not os.path.exists(csv_path):
        logger.critical(f"CSV file not found at path: {csv_path}. Cannot proceed.")
        return None, None, None

    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except Exception as e:
        logger.critical(f"Failed to read CSV file: {e}")
        return None, None, None

    original_rows = len(df)
    logger.info(f"Loaded {original_rows:,} rows from CSV. Now analyzing data quality...")
    
    quality_report = {col: {'missing': 0, 'malformed': 0} for col in EXPECTED_COLUMNS}
    
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            quality_report[col]['missing'] = original_rows
            logger.warning(f"Column '{col}' not found in CSV.")
            df[col] = pd.NA
            continue

        missing_count = df[col].isnull().sum()
        quality_report[col]['missing'] = missing_count

        if col in DATE_COLUMNS:
            # --- NEW: Robust Two-Pass Date Parsing ---
            # Pass 1: Attempt parsing with the strict and fast ISO 8601 format.
            converted_dates = pd.to_datetime(df[col], format='ISO8601', errors='coerce', utc=True)
            
            # Identify what failed the first pass.
            failed_mask = converted_dates.isnull() & df[col].notnull()

            # Pass 2: For the failures, try again with the more lenient inferred parser.
            if failed_mask.any():
                logger.info(f"Column '{col}': {failed_mask.sum():,} values were not in strict ISO8601 format. Retrying with lenient parser.")
                converted_dates[failed_mask] = pd.to_datetime(df[col][failed_mask], errors='coerce', utc=True)
            
            # --- Final check for malformed entries ---
            malformed_mask = converted_dates.isnull() & df[col].notnull()
            malformed_count = malformed_mask.sum()
            quality_report[col]['malformed'] = malformed_count
            
            if malformed_count > 0:
                logger.warning(f"Found {malformed_count:,} TRULY malformed date entries in column '{col}'.")
                samples = df.loc[malformed_mask, col].head(5).tolist()
                logger.warning(f"  -> First 5 samples: {samples}")

            df[f'{col}_dt'] = converted_dates

    unique_condition_ids = set(df['condition_id'].dropna().unique()) if 'condition_id' in df.columns else None
    return df, unique_condition_ids, quality_report

async def fetch_oracle_events(pool):
    """Fetches ConditionPreparation events from the database for the specified oracles."""
    logger.info("Fetching ConditionPreparation events from database...")
    query = "SELECT topics, timestamp_ms FROM events_conditional_tokens WHERE event_name = 'ConditionPreparation'"
    oracle_events = defaultdict(list)
    
    async with pool.acquire() as conn:
        async with conn.transaction():
            async for record in conn.cursor(query):
                try:
                    oracle_address = f"0x{record['topics'][2][-40:]}".lower()
                    if oracle_address in ORACLES_OF_INTEREST:
                        condition_id = record['topics'][1]
                        oracle_events[oracle_address].append({
                            "condition_id": condition_id,
                            "timestamp_ms": record['timestamp_ms']
                        })
                except (IndexError, TypeError):
                    continue
    logger.info("Finished fetching oracle events.")
    return oracle_events

def plot_combined_cdf(data_dict: dict, title: str, filename: str):
    """
    Generates and saves a single cumulative distribution function (CDF) plot
    for all data series, using distinct colors and line styles.
    """
    if not any(data_dict.values()):
        logger.warning(f"No data provided for plot '{title}'. Skipping generation.")
        return

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(18, 10))

    style_map = {
        "Total Prepared Conditions":          {"color": "black", "linestyle": "-", "linewidth": 3.0, "alpha": 0.9, "zorder": 10},
        "Prep Oracle @ 0x58e1":               {"color": "darkorange", "linestyle": "--", "linewidth": 2.0},
        "Prep Oracle @ 0x6507":               {"color": "indigo", "linestyle": ":", "linewidth": 2.0},
        "Prep Oracle @ 0xd91e":               {"color": "steelblue", "linestyle": "-.", "linewidth": 2.0},
        "Market Creation Time (`created_at`)":{"color": "forestgreen", "linestyle": "-", "linewidth": 2.5},
        "Market Start Time (`start_date`)":   {"color": "firebrick", "linestyle": "-", "linewidth": 2.5}
    }
    
    plot_order = [
        "Total Prepared Conditions", "Prep Oracle @ 0x58e1", "Prep Oracle @ 0x6507", 
        "Prep Oracle @ 0xd91e", "Market Creation Time (`created_at`)", "Market Start Time (`start_date`)"
    ]

    for label in plot_order:
        timestamps = data_dict.get(label)
        if not timestamps:
            continue
        
        valid_timestamps = [ts for ts in timestamps if pd.notna(ts)]
        if not valid_timestamps:
            continue

        sorted_dates = sorted(valid_timestamps)
        yvals = np.arange(1, len(sorted_dates) + 1)
        style = style_map.get(label, {})
        ax.plot(sorted_dates, yvals, label=f"{label} (Total: {len(sorted_dates):,})", **style)

    ax.set_title(title, fontsize=20, pad=20)
    ax.set_xlabel("Date", fontsize=15)
    ax.set_ylabel("Cumulative Count of Events", fontsize=15)
    ax.legend(fontsize=13, loc='upper left')
    ax.margins(x=0.01, y=0.02)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    fig.autofmt_xdate()
    ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))
    ax.tick_params(axis='both', which='major', labelsize=13)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)

    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    logger.info(f"Combined CDF plot saved to {filename}")
    plt.show()

async def main():
    """Main execution function."""
    csv_env_var = 'POLY_CSV'
    csv_dir = os.environ.get(csv_env_var)
    if not csv_dir:
        logger.critical(f"Environment variable ${csv_env_var} is not set. Exiting.")
        return
        
    csv_path = os.path.join(csv_dir, 'gamma_markets.csv')
    markets_df, csv_condition_ids, quality_report = load_and_analyze_csv(csv_path)
    
    if markets_df is None:
        return

    logger.info(f"\n--- CSV Analysis ---")
    if csv_condition_ids is not None:
        logger.info(f"1) Found {len(csv_condition_ids):,} unique condition IDs in the CSV.")
    else:
        logger.warning("1) Analysis skipped: 'condition_id' column not found or empty in the CSV.")

    db_pool = await get_db_pool(DB_CONFIG)
    if not db_pool:
        return
        
    all_oracle_timestamps_ms = []
    oracle_plot_data = defaultdict(list)
    try:
        oracle_events = await fetch_oracle_events(db_pool)
        logger.info(f"\n--- Oracle Matching Analysis ---")
        for addr, events in oracle_events.items():
            oracle_name = ORACLES_OF_INTEREST[addr]
            total_events = len(events)
            logger.info(f"Oracle '{oracle_name}':")
            logger.info(f"  - Total Prepared Condition Events from DB: {total_events:,}")
            if csv_condition_ids is not None:
                oracle_condition_ids = {e['condition_id'] for e in events}
                matched_count = len(oracle_condition_ids.intersection(csv_condition_ids))
                logger.info(f"  - Matched/Exist in CSV: {matched_count:,} ({matched_count/total_events:.2%} of total)")
            else:
                logger.warning("  - Matching skipped: 'condition_id' column not available from CSV.")
            for event in events:
                ts_ms = event['timestamp_ms']
                all_oracle_timestamps_ms.append(ts_ms)
                oracle_plot_data[oracle_name].append(datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc))
    finally:
        await db_pool.close()
        logger.info("Database connection closed.")

    if not all_oracle_timestamps_ms:
        logger.warning("No oracle events found in DB. Cannot determine time range or generate plots.")
    else:
        min_ts, max_ts = min(all_oracle_timestamps_ms), max(all_oracle_timestamps_ms)
        min_dt, max_dt = datetime.fromtimestamp(min_ts / 1000, tz=timezone.utc), datetime.fromtimestamp(max_ts / 1000, tz=timezone.utc)
        
        logger.info(f"\n--- Plotting Time Range (derived from oracle events) ---")
        logger.info(f"  - Start: {min_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"  - End:   {max_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")

        combined_plot_data = {}
        combined_plot_data.update(oracle_plot_data)

        total_oracle_timestamps = []
        for timestamps in oracle_plot_data.values():
            total_oracle_timestamps.extend(timestamps)
        combined_plot_data["Total Prepared Conditions"] = total_oracle_timestamps
        
        logger.info("Deduplicating market data by 'condition_id' for accurate plotting...")
        unique_markets_df = markets_df.drop_duplicates(subset=['condition_id']).copy()
        logger.info(f"Reduced to {len(unique_markets_df):,} unique markets for date analysis.")

        created_at_in_range = unique_markets_df[unique_markets_df['created_at_dt'].between(min_dt, max_dt)]['created_at_dt'].tolist()
        start_date_in_range = unique_markets_df[unique_markets_df['start_date_dt'].between(min_dt, max_dt)]['start_date_dt'].tolist()
        
        combined_plot_data["Market Creation Time (`created_at`)"] = created_at_in_range
        combined_plot_data["Market Start Time (`start_date`)"] = start_date_in_range
        
        plot_combined_cdf(
            combined_plot_data,
            f"Cumulative Event Counts: Oracles vs. Market Creation/Start\n(Analysis Range: {min_dt.date()} to {max_dt.date()})",
            "combined_events_cdf.png"
        )

    logger.info("\n" + "="*60)
    logger.info(" " * 18 + "CSV Data Quality Report")
    logger.info("="*60)
    logger.info(f"Based on {len(markets_df):,} rows from '{os.path.basename(csv_path)}'.\n")
    for col, issues in quality_report.items():
        missing_str = f"{issues['missing']:,}"
        malformed_str = f"{issues['malformed']:,}" if col in DATE_COLUMNS else "N/A"
        logger.info(f"Column: {col:<25} | Missing: {missing_str:<10} | Malformed (Dates): {malformed_str:<10}")
    logger.info("="*60)

if __name__ == "__main__":
    asyncio.run(main())