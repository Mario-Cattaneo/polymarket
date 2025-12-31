import asyncio
import asyncpg
import os
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timezone

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# --- Configuration ---
DB_CONFIG = {
    "socket_env": "PG_SOCKET", "port_env": "POLY_PG_PORT", "name_env": "POLY_DB",
    "user_env": "POLY_DB_CLI", "pass_env": "POLY_DB_CLI_PASS"
}
TABLES_OF_INTEREST = {"base_ctfe": "events_ctf_exchange", "nr_ctfe": "events_neg_risk_exchange"}

def get_plot_save_path(filename: str) -> str:
    """Constructs the full save path for a plot under $POLY_PLOTS/reg_vs_orderbook."""
    base_dir = os.environ.get('POLY_PLOTS', '.')
    output_dir = os.path.join(base_dir, 'reg_vs_orderbook')
    os.makedirs(output_dir, exist_ok=True)
    return os.path.join(output_dir, filename)

def load_and_prepare_csv(csv_path: str):
    """Loads the new gamma_markets.csv format, validates it, and prepares data."""
    logger.info(f"Attempting to load CSV from: {csv_path}")
    if not os.path.exists(csv_path):
        logger.critical(f"CSV file not found: {csv_path}."); return None, None
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        logger.info(f"Loaded {len(df):,} rows from CSV. Now preparing data...")
    except Exception as e:
        logger.critical(f"Failed to read CSV file: {e}"); return None, None
    required_cols = ['conditionId', 'createdAt']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logger.critical(f"CSV is missing required columns for this analysis: {missing}.")
        return None, None
    df.dropna(subset=['conditionId', 'createdAt'], inplace=True)
    df['conditionId'] = df['conditionId'].astype(str).str.lower().apply(lambda x: f'0x{x}' if not x.startswith('0x') else x)
    df['createdAt_dt'] = pd.to_datetime(df['createdAt'], errors='coerce', utc=True)
    df.dropna(subset=['createdAt_dt'], inplace=True)
    unique_condition_ids = set(df['conditionId'].unique())
    return df, unique_condition_ids

async def get_db_pool(db_config: dict):
    """Creates an asyncpg database connection pool."""
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
    """Fetches TokenRegistered events, correctly handling topic data types."""
    logger.info(f"Fetching TokenRegistered events from '{table_name}'...")
    query = f"SELECT topics, timestamp_ms FROM {table_name} WHERE event_name = 'TokenRegistered'"
    events = []
    async with pool.acquire() as conn:
        # --- FIX: Added the required transaction block for the cursor ---
        async with conn.transaction():
            async for record in conn.cursor(query):
                try:
                    raw_topic = record['topics'][3]
                    hex_string = raw_topic.hex() if isinstance(raw_topic, bytes) else raw_topic.lower().replace('0x', '')
                    if len(hex_string) == 64:
                        events.append({"condition_id": f"0x{hex_string}", "timestamp_ms": record['timestamp_ms']})
                except (IndexError, TypeError, AttributeError):
                    continue
    logger.info(f"Processed {len(events):,} events from '{table_name}'.")
    return events

def plot_registration_cdf(data_dict: dict, title: str, filename: str):
    """Generates and saves a single cumulative distribution function (CDF) plot."""
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(18, 10))
    style_map = {
        "Order Book Creations": {"color": "black", "linestyle": "-", "linewidth": 3.0, "zorder": 10},
        "Base CTFE Registrations": {"color": "dodgerblue", "linestyle": "--", "linewidth": 2.0},
        "NR CTFE Registrations": {"color": "darkorange", "linestyle": ":", "linewidth": 2.0},
        "Combined CTFE Registrations": {"color": "green", "linestyle": "-.", "linewidth": 2.5},
    }
    for label, timestamps in data_dict.items():
        if not timestamps: continue
        sorted_dates = sorted([ts for ts in timestamps if pd.notna(ts)])
        if not sorted_dates: continue
        yvals = np.arange(1, len(sorted_dates) + 1)
        ax.plot(sorted_dates, yvals, label=f"{label} (Total: {len(sorted_dates):,})", **style_map.get(label, {}))
    ax.set_title(title, fontsize=20, pad=20)
    ax.set_xlabel("Date", fontsize=15)
    ax.set_ylabel("Cumulative Count of Unique Markets", fontsize=15)
    ax.legend(fontsize=13, loc='upper left')
    ax.margins(x=0.01, y=0.02)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    fig.autofmt_xdate()
    ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))
    plt.tight_layout()
    save_path = get_plot_save_path(filename)
    plt.savefig(save_path, dpi=150)
    logger.info(f"CDF plot saved to {save_path}")
    plt.show()

async def main():
    csv_path = os.path.join(os.environ.get('POLY_CSV', ''), 'gamma_markets.csv')
    markets_df, csv_condition_ids = load_and_prepare_csv(csv_path)
    if markets_df is None: return

    db_pool = await get_db_pool(DB_CONFIG)
    if not db_pool: return
    
    all_ctfe_events = {}
    try:
        for key, table_name in TABLES_OF_INTEREST.items():
            all_ctfe_events[key] = await fetch_token_registered_events(db_pool, table_name)
    finally:
        await db_pool.close()
        logger.info("Database connection closed.")

    all_event_timestamps_ms = [evt['timestamp_ms'] for evts in all_ctfe_events.values() for evt in evts]
    if not all_event_timestamps_ms:
        logger.warning("No CTFE events found. Cannot generate plot."); return

    min_dt = datetime.fromtimestamp(min(all_event_timestamps_ms) / 1000, tz=timezone.utc)
    max_dt = datetime.fromtimestamp(max(all_event_timestamps_ms) / 1000, tz=timezone.utc)
    
    filtered_markets_df = markets_df[markets_df['createdAt_dt'].between(min_dt, max_dt)]
    order_book_timestamps = filtered_markets_df['createdAt_dt'].tolist()

    plot_data = {}
    all_ctfe_timestamps = []
    for key, events in all_ctfe_events.items():
        seen_ids = set()
        unique_timestamps = []
        for event in sorted(events, key=lambda x: x['timestamp_ms']):
            if event['condition_id'] not in seen_ids:
                unique_timestamps.append(datetime.fromtimestamp(event['timestamp_ms'] / 1000, tz=timezone.utc))
                seen_ids.add(event['condition_id'])
        plot_data[f"{key.replace('_', ' ').upper()} Registrations"] = unique_timestamps
        all_ctfe_timestamps.extend(unique_timestamps)

    combined_plot_data = {
        "Order Book Creations": order_book_timestamps,
        **plot_data,
        "Combined CTFE Registrations": sorted(all_ctfe_timestamps)
    }
    title = f"Cumulative Markets: Order Books vs. CTFE Registrations\n(Range: {min_dt.date()} to {max_dt.date()})"
    plot_registration_cdf(combined_plot_data, title, "ctfe_registrations_cdf.png")

if __name__ == "__main__":
    asyncio.run(main())