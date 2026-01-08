import asyncio
import asyncpg
import os
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.dates as mdates  # <-- FIX: Added the missing import
import json
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

ATTRIBUTES_TO_ANALYZE = {
    'outcomePrices': 'pmf',
    'has_lastTradePrice': 'pmf',
    'createdAt': 'cdf',
    'spread': 'cdf',
    'volumeNum': 'cdf',
    'bestAsk': 'cdf'
}

# Additional attributes to analyze with special categorization
ADDITIONAL_ATTRIBUTES = {
    'lastTradePrice_category_zero': 'pmf',  # Missing, = 0, > 0
    'lastTradePrice_category': 'pmf',  # Missing, <= 0, > 0
    'lastTradePrice_category_eps': 'pmf',  # Missing, <= 1e-7, > 1e-7
}

FILTER_CREATED_AT = '2025-12-17 20:43:53'

def get_plot_save_path(filename: str) -> str:
    """Constructs the full save path for a plot under $POLY_PLOTS/reg_vs_orderbook."""
    base_dir = os.environ.get('POLY_PLOTS', '.')
    output_dir = os.path.join(base_dir, 'reg_vs_orderbook')
    os.makedirs(output_dir, exist_ok=True)
    return os.path.join(output_dir, filename)

def load_and_clean_csv(csv_path: str):
    """Loads and robustly cleans the CSV, creating new features for analysis."""
    logger.info(f"Attempting to load CSV from: {csv_path}")
    if not os.path.exists(csv_path):
        logger.critical(f"CSV file not found: {csv_path}"); return None
    
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        logger.info(f"Loaded {len(df):,} rows. Now cleaning and validating...")
    except Exception as e:
        logger.critical(f"Failed to read CSV: {e}"); return None

    other_price_samples = []
    def map_outcome_prices(price_str):
        if pd.isna(price_str): return None
        try:
            prices = json.loads(price_str)
            if not isinstance(prices, list) or len(prices) != 2:
                other_price_samples.append(price_str); return "Other"
            p1, p2 = float(prices[0]), float(prices[1])
            sorted_prices = sorted([p1, p2])
            if sorted_prices == [0.0, 0.0]: return "[0, 0]"
            if sorted_prices == [0.0, 1.0]: return "[0, 1]"
            if sorted_prices == [0.5, 0.5]: return "[0.5, 0.5]"
            if sorted_prices == [1.0, 1.0]: return "[1, 1]"
            other_price_samples.append(price_str); return "Other"
        except: return None

    df['outcomePrices_cleaned'] = df['outcomePrices'].apply(map_outcome_prices)
    if other_price_samples:
        unique_others = sorted(list(set(other_price_samples)))
        logger.info(f"Found {len(unique_others)} unique price combinations categorized as 'Other'.")
        logger.info(f"  -> First 3 non-identical samples: {unique_others[:3]}")

    df['createdAt_dt'] = pd.to_datetime(df['createdAt'], errors='coerce', utc=True)

    if 'lastTradePrice' in df.columns:
        df['has_lastTradePrice_cleaned'] = df['lastTradePrice'].notna().astype(int)
        
        # Create categorized versions: Missing, = 0, > 0
        def categorize_trade_price_zero(price):
            if pd.isna(price):
                return 'Missing'
            price_val = float(price) if isinstance(price, (int, float, str)) else 0
            return '= 0' if price_val == 0 else '> 0'
        df['lastTradePrice_category_zero'] = df['lastTradePrice'].apply(categorize_trade_price_zero)
        
        # Create categorized versions: Missing, <= 0, > 0
        def categorize_trade_price(price):
            if pd.isna(price):
                return 'Missing'
            price_val = float(price) if isinstance(price, (int, float, str)) else 0
            return '<= 0' if price_val <= 0 else '> 0'
        df['lastTradePrice_category'] = df['lastTradePrice'].apply(categorize_trade_price)
        
        # Create categorized versions with epsilon: Missing, <= 1e-7, > 1e-7
        def categorize_trade_price_epsilon(price):
            if pd.isna(price):
                return 'Missing'
            price_val = float(price) if isinstance(price, (int, float, str)) else 0
            return '<= 1e-7' if price_val <= 1e-7 else '> 1e-7'
        df['lastTradePrice_category_eps'] = df['lastTradePrice'].apply(categorize_trade_price_epsilon)
    else:
        logger.warning("Column 'lastTradePrice' not found. Skipping its PMF analysis.")
        df['has_lastTradePrice_cleaned'] = 0
        df['lastTradePrice_category_zero'] = 'Missing'
        df['lastTradePrice_category'] = 'Missing'
        df['lastTradePrice_category_eps'] = 'Missing'

    for attr in ['spread', 'volumeNum', 'bestAsk']:
        if attr in df.columns:
            df[f'{attr}_cleaned'] = pd.to_numeric(df[attr], errors='coerce')
        else:
            df[f'{attr}_cleaned'] = np.nan
            
    df['conditionId'] = df['conditionId'].astype(str).str.lower().apply(lambda x: f'0x{x}' if not x.startswith('0x') else x)
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

def plot_pmf(df, attr, clean_attr, title_suffix=""):
    """Plots a Probability Mass Function, customized for different attributes."""
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Special handling for trade price categories
    if attr == 'has_lastTradePrice' and clean_attr in ['lastTradePrice_category_zero', 'lastTradePrice_category', 'lastTradePrice_category_eps']:
        counts = df.groupby(['is_matched', clean_attr]).size().unstack(fill_value=0)
        probs = counts.div(counts.sum(axis=1), axis=0)
        
        # Define category order
        if clean_attr == 'lastTradePrice_category_zero':
            category_order = ['Missing', '= 0', '> 0']
        elif clean_attr == 'lastTradePrice_category':
            category_order = ['Missing', '<= 0', '> 0']
        else:  # lastTradePrice_category_eps
            category_order = ['Missing', '<= 1e-7', '> 1e-7']
        
        probs = probs.reindex(columns=category_order, fill_value=0)
        probs.T.plot(kind='bar', ax=ax, width=0.6)
        ax.set_xlabel("Last Trade Price Status", fontsize=12)
        ax.tick_params(axis='x', rotation=45, labelsize=11)
    
    elif attr == 'outcomePrices':
        counts = df.groupby(['is_matched', clean_attr]).size().unstack(fill_value=0)
        probs = counts.div(counts.sum(axis=1), axis=0)
        category_order = ["[0, 0]", "[0, 1]", "[0.5, 0.5]", "[1, 1]", "Other"]
        probs = probs.reindex(columns=category_order, fill_value=0)
        probs.T.plot(kind='bar', ax=ax, width=0.8)
        ax.set_xlabel("OutcomePrices Category", fontsize=12)
        ax.tick_params(axis='x', rotation=0, labelsize=11)
    
    elif attr == 'has_lastTradePrice':
        counts = df.groupby(['is_matched', clean_attr]).size().unstack(fill_value=0)
        probs = counts.div(counts.sum(axis=1), axis=0)
        probs = probs.reindex(columns=[0, 1], fill_value=0)
        probs.T.plot(kind='bar', ax=ax, width=0.4)
        ax.set_xlabel("Attribute Status", fontsize=12)
        ax.set_xticklabels(['Missing', 'Present'], rotation=0)
    else:
        counts = df.groupby(['is_matched', clean_attr]).size().unstack(fill_value=0)
        probs = counts.div(counts.sum(axis=1), axis=0)
        probs.T.plot(kind='bar', ax=ax, width=0.6)

    ax.set_title(f"Probability of {attr} given Matched Status{title_suffix}", fontsize=16)
    ax.set_ylabel(f"P({attr} | Status)", fontsize=12)
    ax.legend(title='Status', labels=['Unmatched (M=0)', 'Matched (M=1)'])
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
    plt.tight_layout()
    
    # Generate filename - include clean_attr if it's one of the special trade price categories
    if clean_attr in ['lastTradePrice_category_zero', 'lastTradePrice_category', 'lastTradePrice_category_eps']:
        category_name = clean_attr.replace('lastTradePrice_', '')
        filename = f"pmf_has_lastTradePrice_{category_name}{title_suffix.lower().replace(' ', '_').replace('>', '').strip()}.png"
    else:
        filename = f"pmf_{attr}{title_suffix.lower().replace(' ', '_').replace('>', '').strip()}.png"
    
    save_path = get_plot_save_path(filename)
    plt.savefig(save_path, dpi=120)
    logger.info(f"PMF plot saved to {save_path}")
    plt.show()

def plot_cdf(df, attr, clean_attr, title_suffix=""):
    """Plots a Cumulative Distribution Function for a continuous attribute."""
    matched_data = df[df['is_matched']][clean_attr].dropna().sort_values()
    unmatched_data = df[~df['is_matched']][clean_attr].dropna().sort_values()
    fig, ax = plt.subplots(figsize=(12, 7))
    if not matched_data.empty:
        ax.plot(matched_data, np.arange(1, len(matched_data) + 1) / len(matched_data), 
                label=f'Matched (M=1) (n={len(matched_data):,})', color='green', linestyle='-')
    if not unmatched_data.empty:
        ax.plot(unmatched_data, np.arange(1, len(unmatched_data) + 1) / len(unmatched_data), 
                label=f'Unmatched (M=0) (n={len(unmatched_data):,})', color='red', linestyle='--')
    ax.set_title(f"CDF of {attr} for Matched vs. Unmatched Markets{title_suffix}", fontsize=16)
    ax.set_ylabel("Cumulative Probability", fontsize=12)
    ax.set_xlabel(f"{attr} Value", fontsize=12)
    
    if attr == 'createdAt':
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        fig.autofmt_xdate()

    ax.legend()
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout()
    filename = f"cdf_{attr}{title_suffix.lower().replace(' ', '_').replace('>', '').strip()}.png"
    save_path = get_plot_save_path(filename)
    plt.savefig(save_path, dpi=120)
    logger.info(f"CDF plot saved to {save_path}")
    plt.show()

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

    df['is_matched'] = df['conditionId'].isin(registered_ids)
    start_dt = pd.to_datetime(START_RANGE_STR, utc=True)
    end_dt = pd.to_datetime(END_RANGE_STR, utc=True)
    
    analysis_df = df[df['createdAt_dt'].between(start_dt, end_dt)].copy()
    logger.info(f"Filtered to {len(analysis_df):,} markets within the specified time range.")
    
    matched_count = analysis_df['is_matched'].sum()
    logger.info(f"Within this range: {matched_count:,} are Matched, {len(analysis_df) - matched_count:,} are Unmatched.")

    for attr, plot_type in ATTRIBUTES_TO_ANALYZE.items():
        if attr == 'createdAt':
            clean_attr = 'createdAt_dt'
        else:
            clean_attr = f"{attr}_cleaned"

        if clean_attr not in analysis_df.columns or analysis_df[clean_attr].notna().sum() == 0:
            logger.warning(f"Skipping plot for '{attr}' as there is no valid data in the time range.")
            continue
        if plot_type == 'pmf':
            plot_pmf(analysis_df, attr, clean_attr)
        elif plot_type == 'cdf':
            plot_cdf(analysis_df, attr, clean_attr)
    
    # Generate additional trade price category plots
    for attr, plot_type in ADDITIONAL_ATTRIBUTES.items():
        if attr not in analysis_df.columns or analysis_df[attr].notna().sum() == 0:
            logger.warning(f"Skipping plot for '{attr}' as there is no valid data.")
            continue
        if plot_type == 'pmf':
            plot_pmf(analysis_df, 'has_lastTradePrice', attr)
    
    # Generate filtered plots (createdAt > FILTER_CREATED_AT)
    logger.info(f"\n=== Generating filtered plots (createdAt > {FILTER_CREATED_AT}) ===")
    filter_created_at_dt = pd.to_datetime(FILTER_CREATED_AT, utc=True)
    
    # Filter within the same time window, but only after FILTER_CREATED_AT
    analysis_df_filtered = df[(df['createdAt_dt'] > filter_created_at_dt) & (df['createdAt_dt'] <= end_dt)].copy()
    logger.info(f"Filtered to {len(analysis_df_filtered):,} markets with createdAt > {FILTER_CREATED_AT} (within original time range).")
    
    if len(analysis_df_filtered) == 0:
        logger.warning("No data after applying createdAt filter. Skipping filtered plots.")
        return
    
    matched_count_filtered = analysis_df_filtered['is_matched'].sum()
    logger.info(f"In filtered set: {matched_count_filtered:,} are Matched, {len(analysis_df_filtered) - matched_count_filtered:,} are Unmatched.")
    
    for attr, plot_type in ATTRIBUTES_TO_ANALYZE.items():
        if attr == 'createdAt':
            clean_attr = 'createdAt_dt'
        else:
            clean_attr = f"{attr}_cleaned"

        if clean_attr not in analysis_df_filtered.columns or analysis_df_filtered[clean_attr].notna().sum() == 0:
            logger.warning(f"Skipping filtered plot for '{attr}' as there is no valid data.")
            continue
        
        title_suffix = f" (createdAt > {FILTER_CREATED_AT})"
        if plot_type == 'pmf':
            plot_pmf(analysis_df_filtered, attr, clean_attr, title_suffix)
        elif plot_type == 'cdf':
            plot_cdf(analysis_df_filtered, attr, clean_attr, title_suffix)
    
    # Generate additional trade price category plots (filtered)
    for attr, plot_type in ADDITIONAL_ATTRIBUTES.items():
        if attr not in analysis_df_filtered.columns or analysis_df_filtered[attr].notna().sum() == 0:
            logger.warning(f"Skipping filtered plot for '{attr}' as there is no valid data.")
            continue
        title_suffix = f" (createdAt > {FILTER_CREATED_AT})"
        if plot_type == 'pmf':
            plot_pmf(analysis_df_filtered, 'has_lastTradePrice', attr, title_suffix)

# Helper functions needed by this script
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