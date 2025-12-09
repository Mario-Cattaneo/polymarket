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

PROCESS_FORK_1 = False # Set to False to only process Fork 2
PROCESS_FORK_2 = True

# ----------------------------------------------------------------
# 1. SETUP
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
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
# 2. DATA LOADING & PROCESSING (REWRITTEN FOR ACCURACY)
# ----------------------------------------------------------------

async def calculate_fork_time_range(pool, market_table_name):
    """Calculates the precise time range using MIN/MAX of found_time_ms."""
    logger.info(f"Calculating precise time range from '{market_table_name}'...")
    query = f"SELECT MIN(found_time_ms) as min_ts, MAX(found_time_ms) as max_ts FROM {market_table_name} WHERE exhaustion_cycle != 0"
    row = await pool.fetchrow(query)
    if not row or not row['min_ts']: return None, None
    return pd.to_datetime(row['min_ts'], unit='ms', utc=True), pd.to_datetime(row['max_ts'], unit='ms', utc=True)

async def get_events(pool, event_name, contract_key, time_range):
    """
    NEW AND CORRECT: Fetches a specific set of events with a targeted SQL query.
    This is the most efficient and reliable method.
    """
    config = CONTRACT_CONFIG[contract_key]
    table, address = config['table'], config['address'].lower()
    min_ts_ms = int(time_range[0].timestamp() * 1000)
    max_ts_ms = int(time_range[1].timestamp() * 1000)
    
    query = f"SELECT timestamp_ms FROM {table} WHERE event_name = $1 AND LOWER(contract_address) = $2 AND timestamp_ms BETWEEN $3 AND $4"
    
    logger.info(f"Fetching '{event_name}' events for '{contract_key}'...")
    rows = await pool.fetch(query, event_name, address, min_ts_ms, max_ts_ms)
    
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if not df.empty:
        df['time'] = pd.to_datetime(df['timestamp_ms'], unit='ms', utc=True)
    
    logger.info(f"-> Found {len(df)} events.")
    return df

async def get_market_data(pool, market_table_name):
    """Fetches found/closed times from the markets table."""
    logger.info(f"Fetching market data from '{market_table_name}'...")
    query = f"SELECT found_time_ms, closed_time FROM {market_table_name} WHERE exhaustion_cycle != 0"
    rows = await pool.fetch(query)
    if not rows: return pd.DataFrame(columns=['time']), pd.DataFrame(columns=['time'])

    found_times = [r['found_time_ms'] for r in rows if r['found_time_ms']]
    closed_times = [r['closed_time'] for r in rows if r['closed_time']]
    found_df = pd.DataFrame(found_times, columns=['time'])
    found_df['time'] = pd.to_datetime(found_df['time'], unit='ms', utc=True)
    
    def parse_closed_time(ts):
        try:
            ts = float(ts)
            if ts > 30000000000: ts /= 1000
            return pd.to_datetime(ts, unit='s', utc=True)
        except (ValueError, TypeError): return pd.NaT
    closed_df = pd.DataFrame(closed_times, columns=['ts_str'])
    closed_df['time'] = closed_df['ts_str'].apply(parse_closed_time)
    
    return found_df, closed_df.dropna()

def create_cdf_df(df):
    if df.empty: return pd.DataFrame(columns=['time', 'count'])
    df_sorted = df.sort_values('time').reset_index(drop=True)
    df_sorted['count'] = df_sorted.index + 1
    return df_sorted

def calculate_net_active_df(creation_df, resolution_df):
    creation = creation_df.copy()
    resolution = resolution_df.copy()
    if creation.empty and resolution.empty: return pd.DataFrame(columns=['time', 'net_active'])
    creation['value'] = 1
    resolution['value'] = -1
    combined = pd.concat([creation, resolution]).sort_values('time')
    combined['net_active'] = combined['value'].cumsum()
    return combined[['time', 'net_active']]

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

def plot_lines(data_dict, title, filename, time_range):
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.figure(figsize=(20, 14))
    palette = sns.color_palette("deep", len(data_dict))
    logger.info(f"Generating plot: {title}")
    for i, (label, df) in enumerate(data_dict.items()):
        if not df.empty:
            plt.plot(df['time'], df.iloc[:, 1], label=label, color=palette[i], linewidth=2.5)
    plt.title(title, fontsize=20, fontweight='bold')
    plt.xlabel("Date (UTC)", fontsize=14)
    plt.ylabel("Net Active Count", fontsize=14)
    if time_range[0] and time_range[1]: plt.xlim(time_range)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45, ha="right", fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(fontsize=11)
    plt.tight_layout()
    plt.savefig(filename)
    logger.info(f"Saved plot to {filename}")
    plt.close()

# ----------------------------------------------------------------
# 4. PLOT GENERATION LOGIC (REWRITTEN FOR ACCURACY)
# ----------------------------------------------------------------

async def generate_fork_plots(pool, fork_id, market_data, time_range, output_dir):
    logger.info(f"--- Generating all plots for Fork {fork_id} ---")

    # --- PLOT 1: CREATION ---
    negrisk_tokens = await get_events(pool, 'TokenRegistered', 'negrisk_exchange', time_range)
    ctfe_tokens = await get_events(pool, 'TokenRegistered', 'ctfe_exchange', time_range)
    cond_prepared = await get_events(pool, 'ConditionPreparation', 'conditional_tokens', time_range)
    uma_q_init = {
        'neg': await get_events(pool, 'QuestionInitialized', 'neg_uma', time_range),
        'ctfe': await get_events(pool, 'QuestionInitialized', 'ctfe_uma', time_range),
        'moo': await get_events(pool, 'QuestionInitialized', 'moo_uma', time_range)
    }
    
    creation_plot_data = {
        "NegRisk Tokens Registered": negrisk_tokens,
        "CTFE Tokens Registered": ctfe_tokens,
        "All Tokens Registered": pd.concat([negrisk_tokens, ctfe_tokens]),
        "2 * Conditions Prepared": pd.concat([cond_prepared, cond_prepared]),
        "Orderbooks Found (from DB)": market_data['found'],
        "NegRisk Op Question Prepared": await get_events(pool, 'QuestionPrepared', 'negrisk_op', time_range),
        "NegRisk Op Market Prepared": await get_events(pool, 'MarketPrepared', 'negrisk_op', time_range),
        "NegRisk Adapt Question Prepared": await get_events(pool, 'QuestionPrepared', 'negrisk_adapt', time_range),
        "NegRisk Adapt Market Prepared": await get_events(pool, 'MarketPrepared', 'negrisk_adapt', time_range),
        "2 * Neg UMA (QuestionInitialized)": pd.concat([uma_q_init['neg'], uma_q_init['neg']]),
        "2 * CTFE UMA (QuestionInitialized)": pd.concat([uma_q_init['ctfe'], uma_q_init['ctfe']]),
        "2 * MOO UMA (QuestionInitialized)": pd.concat([uma_q_init['moo'], uma_q_init['moo']]),
        "2 * (CTFE + MOO) UMA (QuestionInitialized)": pd.concat([uma_q_init['ctfe'], uma_q_init['ctfe'], uma_q_init['moo'], uma_q_init['moo']]),
        "2 * (All UMA) (QuestionInitialized)": pd.concat([uma_q_init['neg'], uma_q_init['neg'], uma_q_init['ctfe'], uma_q_init['ctfe'], uma_q_init['moo'], uma_q_init['moo']]),
    }
    plot_cdfs(creation_plot_data, f"Fork {fork_id} CDF: Market Creation Events", output_dir / "plot_1_creation.png", time_range)

    # --- PLOT 2: RESOLUTION ---
    uma_q_resolved = {
        'neg': await get_events(pool, 'QuestionResolved', 'neg_uma', time_range),
        'ctfe': await get_events(pool, 'QuestionResolved', 'ctfe_uma', time_range),
        'moo': await get_events(pool, 'QuestionResolved', 'moo_uma', time_range)
    }
    resolution_plot_data = {
        "Orderbooks Closed (from DB)": market_data['closed'],
        "NegRisk Op QuestionResolved": await get_events(pool, 'QuestionResolved', 'negrisk_op', time_range),
        "NegRisk Adapt OutcomeReported": await get_events(pool, 'OutcomeReported', 'negrisk_adapt', time_range),
        "Neg UMA QuestionManuallyResolved": await get_events(pool, 'QuestionManuallyResolved', 'neg_uma', time_range),
        "CTFE UMA QuestionManuallyResolved": await get_events(pool, 'QuestionManuallyResolved', 'ctfe_uma', time_range),
        "MOO UMA QuestionManuallyResolved": await get_events(pool, 'QuestionManuallyResolved', 'moo_uma', time_range),
        "2 * Neg UMA (QuestionResolved)": pd.concat([uma_q_resolved['neg'], uma_q_resolved['neg']]),
        "2 * CTFE UMA (QuestionResolved)": pd.concat([uma_q_resolved['ctfe'], uma_q_resolved['ctfe']]),
        "2 * MOO UMA (QuestionResolved)": pd.concat([uma_q_resolved['moo'], uma_q_resolved['moo']]),
        "2 * (CTFE + MOO) UMA (QuestionResolved)": pd.concat([uma_q_resolved['ctfe'], uma_q_resolved['ctfe'], uma_q_resolved['moo'], uma_q_resolved['moo']]),
        "2 * (All UMA) (QuestionResolved)": pd.concat([uma_q_resolved['neg'], uma_q_resolved['neg'], uma_q_resolved['ctfe'], uma_q_resolved['ctfe'], uma_q_resolved['moo'], uma_q_resolved['moo']]),
    }
    plot_cdfs(resolution_plot_data, f"Fork {fork_id} CDF: Market Resolution Events", output_dir / "plot_2_resolution.png", time_range)
    
    # --- PLOT 3: NET ACTIVE ---
    net_active_data = {
        "Net Active Orderbooks (DB)": calculate_net_active_df(market_data['found'], market_data['closed']),
        "2 * (Conditions Prepared - Resolved)": calculate_net_active_df(pd.concat([cond_prepared, cond_prepared]), await get_events(pool, 'ConditionResolution', 'conditional_tokens', time_range)),
        "NegRisk Tokens - Neg UMA Resolved": calculate_net_active_df(negrisk_tokens, uma_q_resolved['neg']),
        "CTFE Tokens - (MOO+CTFE) UMA Resolved": calculate_net_active_df(ctfe_tokens, pd.concat([uma_q_resolved['moo'], uma_q_resolved['ctfe']])),
        "All Tokens - All UMA Resolved": calculate_net_active_df(pd.concat([negrisk_tokens, ctfe_tokens]), pd.concat(uma_q_resolved.values())),
        "Neg UMA (Init - Resolved)": calculate_net_active_df(uma_q_init['neg'], uma_q_resolved['neg']),
        "CTFE & MOO UMA (Init - Resolved)": calculate_net_active_df(pd.concat([uma_q_init['ctfe'], uma_q_init['moo']]), pd.concat([uma_q_resolved['ctfe'], uma_q_resolved['moo']])),
        "All UMA (Init - Resolved)": calculate_net_active_df(pd.concat(uma_q_init.values()), pd.concat(uma_q_resolved.values())),
    }
    plot_lines(net_active_data, f"Fork {fork_id} Net Active Markets", output_dir / "plot_4_net_active.png", time_range)

# ----------------------------------------------------------------
# 5. MAIN EXECUTION (REWRITTEN FOR ACCURACY)
# ----------------------------------------------------------------

async def main():
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    forks_to_process = {}
    if PROCESS_FORK_1: forks_to_process[1] = MARKET_TABLES[1]
    if PROCESS_FORK_2: forks_to_process[2] = MARKET_TABLES[2]
    if not forks_to_process:
        logger.warning("Both PROCESS_FORK_1 and PROCESS_FORK_2 are False. Nothing to process.")
        await pool.close()
        return

    for fork_id, market_table in forks_to_process.items():
        logger.info(f"===== Processing Fork {fork_id} (markets table: {market_table}) =====")
        
        # 1. First, calculate the precise time range for the fork.
        time_range = await calculate_fork_time_range(pool, market_table)
        if not time_range[0]:
            logger.warning(f"Skipping Fork {fork_id}: No data found in '{market_table}'.")
            continue
        logger.info(f"Fork {fork_id} time range: {time_range[0]} to {time_range[1]}")

        # 2. Fetch other necessary data like market found/closed times.
        market_data = {}
        market_data['found'], market_data['closed'] = await get_market_data(pool, market_table)
        
        output_dir = Path(PLOTS_DIR) / "lifetime" / str(fork_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 3. Generate plots by passing the pool and time_range. Data is fetched inside.
        await generate_fork_plots(pool, fork_id, market_data, time_range, output_dir)

    await pool.close()
    logger.info("===== Plot generation complete. =====")

if __name__ == "__main__":
    asyncio.run(main())