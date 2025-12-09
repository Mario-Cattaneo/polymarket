import os
import asyncio
import asyncpg
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging
from pathlib import Path

# ----------------------------------------------------------------
# 0. GLOBAL CONFIGURATION
# ----------------------------------------------------------------

PROCESS_FORK_1 = False # Set to False to only run for Fork 2
PROCESS_FORK_2 = True

# ----------------------------------------------------------------
# 1. SETUP
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

PG_HOST, PG_PORT, DB_NAME, DB_USER, DB_PASS = (os.getenv("PG_SOCKET"), os.getenv("POLY_PG_PORT"), os.getenv("POLY_DB"), os.getenv("POLY_DB_CLI"), os.getenv("POLY_DB_CLI_PASS"))
PLOTS_DIR = os.getenv('POLY_PLOTS', './plots')

MARKET_TABLES = {1: "markets", 2: "markets_2"}

CONTRACT_CONFIG = {
    'negrisk_exchange': {'table': 'events_neg_risk_exchange', 'address': '0xc5d563a36ae78145c45a50134d48a1215220f80a'},
    'ctfe_exchange': {'table': 'events_ctf_exchange', 'address': '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e'},
    'conditional_tokens': {'table': 'events_conditional_tokens', 'address': '0x4d97dcd97ec945f40cf65f87097ace5ea0476045'},
    'negrisk_op': {'table': 'events_neg_risk_operator', 'address': '0x71523d0f655b41e805cec45b17163f528b59b820'},
    'negrisk_adapt': {'table': 'events_neg_risk_adapter', 'address': '0xd91e80cf2e7be2e162c6513ced06f1dd0da35296'},
    'neg_uma': {'table': 'events_uma_adapter', 'address': '0x2F5e3684cb1F318ec51b00Edba38d79Ac2c0aA9d'},
    'ctfe_uma': {'table': 'events_uma_adapter', 'address': '0x6A9D222616C90FcA5754cd1333cFD9b7fb6a4F74'},
    'moo_uma': {'table': 'events_uma_adapter', 'address': '0x65070BE91477460D8A7AeEb94ef92fe056C2f2A7'}
}

# ----------------------------------------------------------------
# 2. DATA LOADING & PROCESSING (WITH DEBUG LOGGING)
# ----------------------------------------------------------------

async def calculate_fork_time_range(pool, market_table_name):
    """Calculates the precise time range using MIN/MAX of found_time_ms."""
    logger.info(f"Calculating precise time range from '{market_table_name}'...")
    query = f"SELECT MIN(found_time_ms) as min_ts, MAX(found_time_ms) as max_ts FROM {market_table_name} WHERE exhaustion_cycle != 0"
    row = await pool.fetchrow(query)
    if not row or not row['min_ts']: return None, None
    
    start_time = pd.to_datetime(row['min_ts'], unit='ms', utc=True)
    end_time = pd.to_datetime(row['max_ts'], unit='ms', utc=True)
    
    return start_time, end_time

async def load_filtered_event_data(pool, time_range):
    """Loads event data ONLY from within the specified time range."""
    logger.info("Loading filtered event data from database for the specific time range...")
    min_ts_ms = int(time_range[0].timestamp() * 1000)
    max_ts_ms = int(time_range[1].timestamp() * 1000)
    
    all_rows = []
    unique_tables = {config['table'] for config in CONTRACT_CONFIG.values()}
    
    for table in unique_tables:
        try:
            query = f"SELECT event_name, contract_address, timestamp_ms FROM {table} WHERE timestamp_ms BETWEEN $1 AND $2"
            rows = await pool.fetch(query, min_ts_ms, max_ts_ms)
            if rows:
                # --- DEBUG LOG ---
                logger.info(f"DEBUG: Fetched {len(rows)} rows from table '{table}'.")
                all_rows.extend([dict(r) for r in rows])
        except asyncpg.exceptions.UndefinedTableError:
            logger.error(f"Table '{table}' does not exist. Skipping.")
            continue
            
    if not all_rows: return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    df['time'] = pd.to_datetime(df['timestamp_ms'], unit='ms', utc=True)
    df['contract_address'] = df['contract_address'].str.lower()
    # --- DEBUG LOG ---
    logger.info(f"DEBUG: Total rows loaded into DataFrame: {len(df)}.")
    return df

async def get_market_data(pool, market_table_name):
    """Fetches found/closed times."""
    logger.info(f"Fetching market data from '{market_table_name}'...")
    query = f"SELECT found_time_ms, closed_time FROM {market_table_name} WHERE exhaustion_cycle != 0"
    rows = await pool.fetch(query)
    if not rows: return pd.DataFrame(columns=['time']), pd.DataFrame(columns=['time'])

    found_times = [r['found_time_ms'] for r in rows if r['found_time_ms']]
    found_df = pd.DataFrame(found_times, columns=['time'])
    found_df['time'] = pd.to_datetime(found_df['time'], unit='ms', utc=True)
    return found_df, pd.DataFrame() # Simplified for this debug

def create_cdf_df(df):
    if df.empty: return pd.DataFrame(columns=['time', 'count'])
    df_sorted = df.sort_values('time').reset_index(drop=True)
    df_sorted['count'] = df_sorted.index + 1
    return df_sorted

# ----------------------------------------------------------------
# 3. PLOTTING FUNCTIONS (Unchanged)
# ----------------------------------------------------------------

def plot_cdfs(data_dict, title, filename, time_range):
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.figure(figsize=(20, 14))
    palette = sns.color_palette("bright", len(data_dict))
    logger.info(f"Generating plot: {title}")
    for i, (label, df) in enumerate(data_dict.items()):
        cdf = create_cdf_df(df)
        if not cdf.empty:
            plt.plot(cdf['time'], cdf['count'], label=f"{label} (N={len(df)})", color=palette[i], linewidth=2.5)
    plt.title(title, fontsize=20, fontweight='bold')
    plt.xlabel("Date (UTC)", fontsize=14)
    plt.ylabel("Cumulative Count", fontsize=14)
    if time_range[0] and time_range[1]: plt.xlim(time_range)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45, ha="right", fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(fontsize=10)
    plt.tight_layout()
    plt.savefig(filename)
    logger.info(f"Saved plot to {filename}")
    plt.close()

# ----------------------------------------------------------------
# 4. PLOT GENERATION LOGIC (WITH DEBUG LOGGING)
# ----------------------------------------------------------------

def generate_fork_plots(fork_id, fork_events_df, market_data, time_range, output_dir):
    logger.info(f"--- Generating all plots for Fork {fork_id} ---")
    
    def get_events(event_name, contract_key):
        addr = CONTRACT_CONFIG[contract_key]['address'].lower()
        return fork_events_df[(fork_events_df['event_name'] == event_name) & (fork_events_df['contract_address'] == addr)]

    # --- PLOT 1: CREATION ---
    negrisk_tokens = get_events('TokenRegistered', 'negrisk_exchange')
    ctfe_tokens = get_events('TokenRegistered', 'ctfe_exchange')
    
    # --- DEBUG LOGS ---
    logger.info(f"DEBUG: Count for 'NegRisk Tokens Registered' before plotting: {len(negrisk_tokens)}")
    logger.info(f"DEBUG: Count for 'CTFE Tokens Registered' before plotting: {len(ctfe_tokens)}")
    
    cond_prepared = get_events('ConditionPreparation', 'conditional_tokens')
    uma_q_init = { 'neg': get_events('QuestionInitialized', 'neg_uma'), 'moo': get_events('QuestionInitialized', 'moo_uma') }
    
    creation_plot_data = {
        "NegRisk Tokens Registered": negrisk_tokens,
        "CTFE Tokens Registered": ctfe_tokens,
        "All Tokens Registered": pd.concat([negrisk_tokens, ctfe_tokens]),
        "2 * Conditions Prepared": pd.concat([cond_prepared, cond_prepared]),
        "Orderbooks Found (from DB)": market_data['found'],
        # ... other plot data ...
    }
    plot_cdfs(creation_plot_data, f"Fork {fork_id} CDF: Market Creation Events", output_dir / "plot_1_creation.png", time_range)

# ----------------------------------------------------------------
# 5. MAIN EXECUTION (WITH DEBUG LOGGING)
# ----------------------------------------------------------------

async def main():
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    forks_to_process = {}
    if PROCESS_FORK_2: forks_to_process[2] = MARKET_TABLES[2]
    
    for fork_id, market_table in forks_to_process.items():
        logger.info(f"===== Processing Fork {fork_id} (markets table: {market_table}) =====")
        
        # 1. Calculate the precise time range.
        time_range = await calculate_fork_time_range(pool, market_table)
        if not time_range[0]:
            logger.warning(f"Skipping Fork {fork_id}: No data found in '{market_table}'.")
            continue
        # --- DEBUG LOG ---
        logger.info(f"DEBUG: Calculated time range: {time_range[0]} to {time_range[1]}")

        # 2. Load ONLY the events from within that specific time range.
        fork_events_df = await load_filtered_event_data(pool, time_range)
        if fork_events_df.empty:
            logger.error(f"No event data found for Fork {fork_id} in the specified time range. Skipping.")
            continue

        # 3. Fetch other necessary data.
        market_data = {}
        market_data['found'], _ = await get_market_data(pool, market_table)
        
        output_dir = Path(PLOTS_DIR) / "lifetime" / str(fork_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 4. Generate plots using the correctly filtered data.
        generate_fork_plots(fork_id, fork_events_df, market_data, time_range, output_dir)

    await pool.close()
    logger.info("===== Plot generation complete. =====")

if __name__ == "__main__":
    asyncio.run(main())