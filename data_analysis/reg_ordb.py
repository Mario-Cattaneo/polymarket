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

# Block range filtering
START_BLOCK_NR = 79172085  # Set to a specific block number to start filtering
END_BLOCK_NR = 81225971    # Set to a specific block number to end filtering
BLOCK_BATCH_SIZE = 100000  # Process in batches of N blocks

# TokenRegistered events table and contract addresses
TOKEN_REG_TABLE = "token_reg"
CTF_EXCHANGE_ADDRESS = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e".lower()
NEGRISK_EXCHANGE_ADDRESS = "0xC5d563A36AE78145C45a50134d48A1215220f80a".lower()

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

async def fetch_token_registered_events(pool, table_name: str, batch_start: int, batch_end: int, exchange_name: str, contract_address: str = None):
    """Fetches TokenRegistered events for a specific block range and optional contract address."""
    query = f"SELECT topics, timestamp_ms, contract_address FROM {table_name} WHERE event_name = 'TokenRegistered' AND block_number >= {batch_start} AND block_number <= {batch_end}"
    if contract_address:
        query += f" AND contract_address = '{contract_address.lower()}'"
    
    events = []
    async with pool.acquire() as conn:
        async with conn.transaction():
            async for record in conn.cursor(query):
                try:
                    raw_topic = record['topics'][3]
                    hex_string = raw_topic.hex() if isinstance(raw_topic, bytes) else raw_topic.lower().replace('0x', '')
                    if len(hex_string) == 64:
                        events.append({
                            "condition_id": f"0x{hex_string}", 
                            "timestamp_ms": record['timestamp_ms'],
                            "exchange": exchange_name
                        })
                except (IndexError, TypeError, AttributeError):
                    continue
    logger.info(f"Processed {len(events):,} events from '{table_name}' ({exchange_name}).")
    return events

def find_cutoff_point(order_book_timestamps, ctfe_timestamps):
    """
    Returns the hardcoded cutoff point from ob_v_reg2.py analysis.
    Cutoff: 2025-12-17 20:43:53 UTC (densest window of unmatched markets)
    """
    # Hardcoded cutoff from ob_v_reg2.py analysis
    cutoff_time = pd.Timestamp('2025-12-17 20:43:53', tz='UTC')
    logger.info(f"Using hardcoded cutoff point: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    return cutoff_time

def plot_registration_cdf(data_dict: dict, title: str, filename: str, order_book_timestamps=None, ctfe_timestamps=None):
    """Generates and saves a single cumulative distribution function (CDF) plot."""
    fig, ax = plt.subplots(figsize=(14, 7))
    style_map = {
        "market object creation": {"color": "#d62728", "linestyle": "-", "linewidth": 3, "zorder": 2},
        "All Exchange Registrations": {"color": "black", "linestyle": "--", "linewidth": 2.5, "zorder": 3},
        "CTF Exchange Registrations": {"color": "#1f77b4", "linestyle": "-.", "linewidth": 2.5, "zorder": 1},
        "Negrisk Exchange Registrations": {"color": "#ff7f0e", "linestyle": ":", "linewidth": 2.5, "zorder": 1},
    }
    for label, timestamps in data_dict.items():
        if not timestamps: continue
        sorted_dates = sorted([ts for ts in timestamps if pd.notna(ts)])
        if not sorted_dates: continue
        yvals = np.arange(1, len(sorted_dates) + 1)
        style = style_map.get(label, {"color": "gray", "linestyle": "--", "linewidth": 2})
        ax.plot(sorted_dates, yvals, label=f"{label} (n={len(sorted_dates):,})", **style)
    
    ax.set_title(title, fontsize=20, fontweight='bold')
    ax.set_xlabel("Time (UTC)", fontsize=16, fontweight='bold')
    ax.set_ylabel("Cumulative Count of Conditions", fontsize=16, fontweight='bold')
    ax.grid(True, which='both', linestyle='--', alpha=0.6)
    ax.legend(fontsize=16, loc='upper left', framealpha=0.95)
    
    # Increase tick label font sizes
    ax.tick_params(axis='x', labelsize=16)
    ax.tick_params(axis='y', labelsize=16)
    
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))
    fig.autofmt_xdate(rotation=30, ha='right')
    
    save_path = get_plot_save_path(filename)
    fig.savefig(save_path, dpi=300, bbox_inches='tight')
    logger.info(f"CDF plot saved to {save_path}")
    plt.show()

async def main():
    csv_path = os.path.join(os.environ.get('POLY_CSV', ''), 'gamma_markets.csv')
    markets_df, csv_condition_ids = load_and_prepare_csv(csv_path)
    if markets_df is None: return

    db_pool = await get_db_pool(DB_CONFIG)
    if not db_pool: return
    
    logger.info(f"\n{'='*80}")
    logger.info("TOKENREGISTERED EVENT FETCH (BATCHED BY EXCHANGE)")
    logger.info(f"{'='*80}")
    logger.info(f"Fetching TokenRegistered events in batches of {BLOCK_BATCH_SIZE:,} blocks from {START_BLOCK_NR:,} to {END_BLOCK_NR:,}...")
    
    ctf_exchange_events = []
    negrisk_exchange_events = []
    current_block = START_BLOCK_NR
    batch_num = 0
    
    try:
        while current_block <= END_BLOCK_NR:
            batch_num += 1
            batch_end = min(current_block + BLOCK_BATCH_SIZE - 1, END_BLOCK_NR)
            logger.info(f"Batch {batch_num}: blocks {current_block:,} to {batch_end:,}")
            
            # Fetch for CTF Exchange
            ctf_batch = await fetch_token_registered_events(
                db_pool, TOKEN_REG_TABLE, current_block, batch_end, 
                "ctf_exchange", CTF_EXCHANGE_ADDRESS
            )
            ctf_exchange_events.extend(ctf_batch)
            
            # Fetch for Negrisk Exchange
            negrisk_batch = await fetch_token_registered_events(
                db_pool, TOKEN_REG_TABLE, current_block, batch_end, 
                "neg_risk_exchange", NEGRISK_EXCHANGE_ADDRESS
            )
            negrisk_exchange_events.extend(negrisk_batch)
            
            logger.info(f"  CTF Exchange: {len(ctf_batch):,} events | Negrisk Exchange: {len(negrisk_batch):,} events")
            
            current_block = batch_end + 1
        
        logger.info(f"\nTotal CTF Exchange events: {len(ctf_exchange_events):,}")
        logger.info(f"Total Negrisk Exchange events: {len(negrisk_exchange_events):,}")
        logger.info(f"Total TokenRegistered events: {len(ctf_exchange_events) + len(negrisk_exchange_events):,}")
        logger.info(f"{'='*80}\n")
    finally:
        await db_pool.close()
        logger.info("Database connection closed.")

    # Combine all exchange events
    all_ctfe_events = ctf_exchange_events + negrisk_exchange_events
    
    all_event_timestamps_ms = [evt['timestamp_ms'] for evt in all_ctfe_events]
    if not all_event_timestamps_ms:
        logger.warning("No CTFE events found. Cannot generate plot."); return

    min_dt = datetime.fromtimestamp(min(all_event_timestamps_ms) / 1000, tz=timezone.utc)
    max_dt = datetime.fromtimestamp(max(all_event_timestamps_ms) / 1000, tz=timezone.utc)
    
    filtered_markets_df = markets_df[markets_df['createdAt_dt'].between(min_dt, max_dt)]
    order_book_timestamps = filtered_markets_df['createdAt_dt'].tolist()

    # Helper function to process events and create unique timestamp list
    def create_unique_timestamps(events):
        seen_ids = set()
        unique_timestamps = []
        for event in sorted(events, key=lambda x: x['timestamp_ms']):
            if event['condition_id'] not in seen_ids:
                unique_timestamps.append(datetime.fromtimestamp(event['timestamp_ms'] / 1000, tz=timezone.utc))
                seen_ids.add(event['condition_id'])
        return unique_timestamps

    # Process TokenRegistered events by exchange
    ctf_exchange_timestamps = create_unique_timestamps(ctf_exchange_events)
    negrisk_exchange_timestamps = create_unique_timestamps(negrisk_exchange_events)
    all_ctfe_timestamps = create_unique_timestamps(all_ctfe_events)

    combined_plot_data = {
        "market object creation": order_book_timestamps,
        "All Exchange Registrations": all_ctfe_timestamps,
        "CTF Exchange Registrations": ctf_exchange_timestamps,
        "Negrisk Exchange Registrations": negrisk_exchange_timestamps,
    }
    
    logger.info(f"\nOrder Book Markets (created): {len(order_book_timestamps):,}")
    logger.info(f"All Exchange Registrations (unique): {len(all_ctfe_timestamps):,}")
    logger.info(f"  - CTF Exchange: {len(ctf_exchange_timestamps):,}")
    logger.info(f"  - Negrisk Exchange: {len(negrisk_exchange_timestamps):,}")
    
    title = f"Cumulative Arrival of Conditions"
    plot_registration_cdf(combined_plot_data, title, "ctfe_registrations_cdf.png", order_book_timestamps, all_ctfe_timestamps)

if __name__ == "__main__":
    asyncio.run(main())