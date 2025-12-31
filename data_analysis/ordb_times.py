import os
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---

# 1. Attributes of interest for the CDF plot
ATTRIBUTES_OF_INTEREST = [
    'createdAt',
    'startDate',
    'updatedAt',
    'endDate',
    'closedTime',
    'acceptingOrdersTimestamp', 
    'umaEndDate'
]

# 2. Time range for the analysis (inclusive)
# --- CORRECTED FORMAT: Removed the '+' before UTC ---
START_RANGE_STR = "Nov-18-2025 07:00:01 AM UTC"
END_RANGE_STR = "Dec-29-2025 02:44:39 PM UTC"


def load_and_process_csv(csv_path: str):
    """
    Loads the specified CSV, robustly parses date columns, and generates a data quality report.
    """
    logger.info(f"Attempting to load CSV from: {csv_path}")
    if not os.path.exists(csv_path):
        logger.critical(f"CSV file not found at path: {csv_path}. Cannot proceed.")
        return None, None

    try:
        df = pd.read_csv(csv_path, low_memory=False)
        total_rows = len(df)
        logger.info(f"Loaded {total_rows:,} rows from CSV. Now analyzing data quality...")
    except Exception as e:
        logger.critical(f"Failed to read CSV file: {e}")
        return None, None

    quality_report = {
        attr: {'missing': 0, 'malformed': 0, 'samples': []} 
        for attr in ATTRIBUTES_OF_INTEREST
    }

    for attr in ATTRIBUTES_OF_INTEREST:
        dt_col = f"{attr}_dt"

        if attr not in df.columns:
            logger.warning(f"Column '{attr}' not found in CSV.")
            quality_report[attr]['missing'] = total_rows
            df[dt_col] = pd.NaT
            continue

        missing_count = df[attr].isnull().sum()
        quality_report[attr]['missing'] = missing_count

        converted_dates = pd.to_datetime(df[attr], format='ISO8601', errors='coerce', utc=True)
        failed_mask = converted_dates.isnull() & df[attr].notnull()

        if failed_mask.any():
            converted_dates[failed_mask] = pd.to_datetime(df[attr][failed_mask], errors='coerce', utc=True)
        
        malformed_mask = converted_dates.isnull() & df[attr].notnull()
        malformed_count = malformed_mask.sum()
        quality_report[attr]['malformed'] = malformed_count
        
        if malformed_count > 0:
            samples = df.loc[malformed_mask, attr].head(3).tolist()
            quality_report[attr]['samples'] = samples

        df[dt_col] = converted_dates
        
    return df, quality_report


def plot_combined_cdf(df: pd.DataFrame, start_range: datetime, end_range: datetime):
    """
    Generates and saves a single CDF plot for all specified attributes
    within a given time range, using distinct styles.
    """
    logger.info("Generating combined CDF plot...")
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(18, 10))

    style_map = {
        'createdAt':                {'color': 'forestgreen', 'linestyle': '-', 'linewidth': 2.5},
        'startDate':                {'color': 'firebrick', 'linestyle': '--', 'linewidth': 2.5},
        'updatedAt':                {'color': 'darkorange', 'linestyle': ':', 'linewidth': 2.5},
        'endDate':                  {'color': 'indigo', 'linestyle': '-.', 'linewidth': 2.0},
        'closedTime':               {'color': 'steelblue', 'linestyle': (0, (3, 5, 1, 5)), 'linewidth': 2.0},
        'acceptingOrdersTimestamp': {'color': 'grey', 'linestyle': '--', 'linewidth': 2.0},
        'umaEndDate':               {'color': 'purple', 'linestyle': ':', 'linewidth': 2.0}
    }

    for attr in ATTRIBUTES_OF_INTEREST:
        dt_col = f"{attr}_dt"
        
        if dt_col not in df.columns or df[dt_col].isnull().all():
            continue

        timestamps = df[dt_col].dropna()
        timestamps_in_range = timestamps[timestamps.between(start_range, end_range, inclusive='both')]

        if timestamps_in_range.empty:
            logger.info(f"No data for '{attr}' within the specified time range.")
            continue

        sorted_dates = sorted(timestamps_in_range)
        yvals = np.arange(1, len(sorted_dates) + 1)
        style = style_map.get(attr, {})
        
        ax.plot(sorted_dates, yvals, label=f"{attr} (Found: {len(sorted_dates):,})", **style)

    ax.set_title(f"Cumulative Event Counts Over Time\n(Range: {start_range.date()} to {end_range.date()})", fontsize=20, pad=20)
    ax.set_xlabel("Date", fontsize=15)
    ax.set_ylabel("Cumulative Count", fontsize=15)
    ax.legend(fontsize=13, loc='upper left')
    ax.margins(x=0.01, y=0.02)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    fig.autofmt_xdate()
    ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))
    ax.tick_params(axis='both', which='major', labelsize=13)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)

    plt.tight_layout()
    filename = "timestamp_attributes_cdf.png"
    plt.savefig(filename, dpi=150)
    logger.info(f"Combined CDF plot saved to {filename}")
    plt.show()


def print_quality_report(report: dict, total_rows: int):
    """Prints the final data quality report in a formatted table."""
    logger.info("\n" + "="*80)
    logger.info(" " * 25 + "CSV Data Quality Report")
    logger.info("="*80)
    logger.info(f"Based on {total_rows:,} total rows.\n")
    
    header = f"{'Attribute':<25} | {'Missing Count':<15} | {'Missing %':<10} | {'Malformed Count':<18} | {'Malformed %':<10}"
    logger.info(header)
    logger.info("-" * len(header))

    for attr, issues in report.items():
        missing_pct = (issues['missing'] / total_rows * 100) if total_rows > 0 else 0
        malformed_pct = (issues['malformed'] / total_rows * 100) if total_rows > 0 else 0
        
        logger.info(
            f"{attr:<25} | {issues['missing']:,<15} | {missing_pct:<9.2f}% | "
            f"{issues['malformed']:,<18} | {malformed_pct:<9.2f}%"
        )

    has_samples = any(issues['samples'] for issues in report.values())
    if has_samples:
        logger.info("\n--- Samples of Malformed Data ---")
        for attr, issues in report.items():
            if issues['samples']:
                logger.warning(f"  -> Samples for '{attr}': {issues['samples']}")
    
    logger.info("="*80)


def main():
    """Main execution function."""
    csv_env_var = 'POLY_CSV'
    csv_dir = os.environ.get(csv_env_var)
    if not csv_dir:
        logger.critical(f"Environment variable ${csv_env_var} is not set. Exiting.")
        return
        
    csv_path = os.path.join(csv_dir, 'gamma_markets.csv')
    
    markets_df, quality_report = load_and_process_csv(csv_path)
    
    if markets_df is None:
        return

    try:
        start_range = pd.to_datetime(START_RANGE_STR, utc=True)
        end_range = pd.to_datetime(END_RANGE_STR, utc=True)
        logger.info(f"Analysis time range set from {start_range} to {end_range}")
    except Exception as e:
        logger.critical(f"Could not parse the time range strings. Error: {e}")
        return

    plot_combined_cdf(markets_df, start_range, end_range)
    print_quality_report(quality_report, len(markets_df))


if __name__ == "__main__":
    main()