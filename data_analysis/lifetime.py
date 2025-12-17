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
# 2. DATA LOADING & PROCESSING
# ----------------------------------------------------------------

async def calculate_fork_time_range(pool, market_table_name):
    logger.info(f"Calculating precise time range from '{market_table_name}'...")
    query = f"SELECT MIN(found_time_ms) as min_ts, MAX(found_time_ms) as max_ts FROM {market_table_name} WHERE exhaustion_cycle != 0"
    row = await pool.fetchrow(query)
    if not row or not row['min_ts']: return None, None
    return pd.to_datetime(row['min_ts'], unit='ms', utc=True), pd.to_datetime(row['max_ts'], unit='ms', utc=True)

async def get_events(pool, event_name, contract_key, time_range):
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

# NEW FUNCTION: To fetch events without filtering by contract address
async def get_events_no_address(pool, event_name, table_name, time_range):
    min_ts_ms = int(time_range[0].timestamp() * 1000)
    max_ts_ms = int(time_range[1].timestamp() * 1000)
    query = f"SELECT timestamp_ms FROM {table_name} WHERE event_name = $1 AND timestamp_ms BETWEEN $2 AND $3"
    logger.info(f"Fetching '{event_name}' events from table '{table_name}' (any address)...")
    rows = await pool.fetch(query, event_name, min_ts_ms, max_ts_ms)
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if not df.empty:
        df['time'] = pd.to_datetime(df['timestamp_ms'], unit='ms', utc=True)
    logger.info(f"-> Found {len(df)} events.")
    return df

async def get_events_with_topics(pool, event_name, contract_key, time_range, topic_map):
    config = CONTRACT_CONFIG[contract_key]
    table, address = config['table'], config['address'].lower()
    min_ts_ms = int(time_range[0].timestamp() * 1000)
    max_ts_ms = int(time_range[1].timestamp() * 1000)
    select_clause = ", ".join([f"topics[{idx}] as {name}" for name, idx in topic_map.items()])
    query = f"SELECT timestamp_ms, {select_clause} FROM {table} WHERE event_name = $1 AND LOWER(contract_address) = $2 AND timestamp_ms BETWEEN $3 AND $4"
    logger.info(f"Fetching '{event_name}' with topics for '{contract_key}'...")
    rows = await pool.fetch(query, event_name, address, min_ts_ms, max_ts_ms)
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if not df.empty:
        df['time'] = pd.to_datetime(df['timestamp_ms'], unit='ms', utc=True)
        for col in topic_map.keys():
            df[col] = df[col].str.lower()
    logger.info(f"-> Found {len(df)} events with details.")
    return df

async def get_market_data(pool, market_table_name, time_range):
    logger.info(f"Fetching market data from '{market_table_name}'...")
    min_ts_ms = int(time_range[0].timestamp() * 1000)
    max_ts_ms = int(time_range[1].timestamp() * 1000)
    query = f"SELECT found_time_ms, closed_time, condition_id, message ->> 'questionID' as question_id FROM {market_table_name} WHERE exhaustion_cycle != 0 AND found_time_ms BETWEEN $1 AND $2"
    rows = await pool.fetch(query, min_ts_ms, max_ts_ms)
    if not rows: return pd.DataFrame(), pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    df['condition_id'] = df['condition_id'].str.lower()
    df['question_id'] = df['question_id'].str.lower()
    found_df = df[['found_time_ms', 'condition_id', 'question_id']].copy()
    found_df.rename(columns={'found_time_ms': 'timestamp_ms'}, inplace=True)
    found_df['time'] = pd.to_datetime(found_df['timestamp_ms'], unit='ms', utc=True)
    closed_times = df['closed_time'].dropna().tolist()
    def parse_closed_time(ts):
        try:
            ts = float(ts)
            if ts > 30000000000: ts /= 1000
            return pd.to_datetime(ts, unit='s', utc=True)
        except (ValueError, TypeError): return pd.NaT
    closed_df = pd.DataFrame(closed_times, columns=['ts_str'])
    closed_df['time'] = closed_df['ts_str'].apply(parse_closed_time)
    return found_df.dropna(subset=['time']), closed_df.dropna(subset=['time'])

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
# 3. PLOTTING FUNCTIONS
# ----------------------------------------------------------------

def plot_cdfs(data_dict, title, filename, time_range):
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.figure(figsize=(20, 14))
    palette = sns.color_palette("bright", len(data_dict))
    logger.info(f"Generating plot: {title}")

    # Prepare data for sorting
    plot_items = []
    for label, data in data_dict.items():
        df, multiplier = (data, 1) if isinstance(data, pd.DataFrame) else data
        if not df.empty:
            final_count = len(df) * multiplier
            plot_items.append({'label': label, 'data': data, 'final_count': final_count})

    # Sort items by final count, descending, to order the legend
    sorted_plot_items = sorted(plot_items, key=lambda x: x['final_count'], reverse=True)

    # Plot sorted items
    for i, item in enumerate(sorted_plot_items):
        label, data, total_n = item['label'], item['data'], item['final_count']
        df, multiplier = (data, 1) if isinstance(data, pd.DataFrame) else data
        cdf = create_cdf_df(df)
        if not cdf.empty:
            cdf['count'] *= multiplier
            plt.plot(cdf['time'], cdf['count'], label=f"{label} (N={total_n})", color=palette[i], linewidth=2.5)

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
# 4. PLOT GENERATION LOGIC
# ----------------------------------------------------------------

async def generate_fork_plots(pool, fork_id, market_data, time_range, output_dir):
    logger.info(f"--- Generating all plots for Fork {fork_id} ---")

    # --- Data Fetching for ALL plots ---
    negrisk_tokens = await get_events(pool, 'TokenRegistered', 'negrisk_exchange', time_range)
    ctfe_tokens = await get_events(pool, 'TokenRegistered', 'ctfe_exchange', time_range)
    cond_prepared = await get_events(pool, 'ConditionPreparation', 'conditional_tokens', time_range)
    
    # Data for Plot 1 specifically
    # CORRECTED: Use the new function for RequestPrice
    moo_request_price = await get_events_no_address(pool, 'RequestPrice', 'events_managed_oracle', time_range)
    negrisk_adapt_q_prepared = await get_events(pool, 'QuestionPrepared', 'negrisk_adapt', time_range)
    negrisk_adapt_market_prepared = await get_events(pool, 'MarketPrepared', 'negrisk_adapt', time_range)
    negrisk_op_q_prepared = await get_events(pool, 'QuestionPrepared', 'negrisk_op', time_range)
    negrisk_op_market_prepared = await get_events(pool, 'MarketPrepared', 'negrisk_op', time_range)

    # Data for multiple plots (original structure)
    uma_q_init = {
        'neg': await get_events(pool, 'QuestionInitialized', 'neg_uma', time_range),
        'ctfe': await get_events(pool, 'QuestionInitialized', 'ctfe_uma', time_range),
        'moo': await get_events(pool, 'QuestionInitialized', 'moo_uma', time_range)
    }
    uma_q_resolved = {
        'neg': await get_events(pool, 'QuestionResolved', 'neg_uma', time_range),
        'ctfe': await get_events(pool, 'QuestionResolved', 'ctfe_uma', time_range),
        'moo': await get_events(pool, 'QuestionResolved', 'moo_uma', time_range)
    }

    # --- PLOT 1: CREATION (MODIFIED) ---
    creation_plot_data = {
        "A) NegRisk CTFE TokensRegistered": negrisk_tokens,
        "B) Base CTFE TokensRegistered": ctfe_tokens,
        "C) Combined CTFE TokensRegistered": pd.concat([negrisk_tokens, ctfe_tokens]),
        "D) 2 * CTF ConditionPreparation": (cond_prepared, 2),
        "E) Orderbooks Found (from DB)": market_data['found'],
        "F) 2 * MOO RequestPrice": (moo_request_price, 2),
        "G) 2 * MOO UMA Adapter QuestionInitialized": (uma_q_init['moo'], 2),
        "H) 2 * (NegRisk Adapter QuestionPrepared & MOO UMA QuestionInitialized)": (pd.concat([negrisk_adapt_q_prepared, uma_q_init['moo']]), 2),
        "I) 2 * NegRisk UMA Adapter QuestionInitialized": (uma_q_init['neg'], 2),
        "J) 2 * NegRisk Adapter MarketPrepared": (negrisk_adapt_market_prepared, 2),
        "K) 2 * NegRisk Operator QuestionPrepared": (negrisk_op_q_prepared, 2),
        "L) 2 * NegRisk Operator MarketPrepared": (negrisk_op_market_prepared, 2),
    }
    plot_cdfs(creation_plot_data, f"Fork {fork_id} CDF: Market Creation Events", output_dir / "plot_1_creation.png", time_range)

    # --- PLOT 1.1: CREATION ANALYSIS (UNCHANGED) ---
    logger.info("--- Generating Plot 1.1: Creation Discrepancy Analysis ---")
    ctf_details = await get_events_with_topics(pool, 'ConditionPreparation', 'conditional_tokens', time_range, {'condition_id': 2, 'question_id': 4})
    base_ctfe_details = await get_events_with_topics(pool, 'TokenRegistered', 'ctfe_exchange', time_range, {'condition_id': 4})
    negrisk_ctfe_details = await get_events_with_topics(pool, 'TokenRegistered', 'negrisk_exchange', time_range, {'condition_id': 4})
    moo_uma_details = await get_events_with_topics(pool, 'QuestionInitialized', 'moo_uma', time_range, {'question_id': 2})
    negrisk_adapter_details = await get_events_with_topics(pool, 'QuestionPrepared', 'negrisk_adapt', time_range, {'question_id': 3})
    
    combined_exchanges_details = pd.concat([base_ctfe_details, negrisk_ctfe_details])
    combined_oracles_details = pd.concat([moo_uma_details, negrisk_adapter_details])

    ctf_conditions = set(ctf_details['condition_id'].dropna())
    orderbook_conditions = set(market_data['found']['condition_id'].dropna())
    base_ctfe_conditions = set(base_ctfe_details['condition_id'].dropna())
    negrisk_ctfe_conditions = set(negrisk_ctfe_details['condition_id'].dropna())
    combined_exchanges_conditions = base_ctfe_conditions.union(negrisk_ctfe_conditions)
    
    orderbook_questions = set(market_data['found']['question_id'].dropna())
    moo_uma_questions = set(moo_uma_details['question_id'].dropna())
    negrisk_adapter_questions = set(negrisk_adapter_details['question_id'].dropna())
    combined_oracles_questions = moo_uma_questions.union(negrisk_adapter_questions)

    def filter_and_deduplicate(df, id_column, id_set):
        if df.empty or not id_set: return pd.DataFrame(columns=['time'])
        matching_events = df[df[id_column].isin(id_set)]
        first_occurrence = matching_events.sort_values('time').drop_duplicates(subset=[id_column], keep='first')
        return first_occurrence[['time']]

    plot_data_1_1 = {
        "2 * (ctf \\ orderbooks) conditions": (filter_and_deduplicate(ctf_details, 'condition_id', ctf_conditions - orderbook_conditions), 2),
        "2 * (combined exchanges \\ orderbooks) conditions": (filter_and_deduplicate(combined_exchanges_details, 'condition_id', combined_exchanges_conditions - orderbook_conditions), 2),
        "2 * (base ctfe \\ orderbooks) conditions": (filter_and_deduplicate(base_ctfe_details, 'condition_id', base_ctfe_conditions - orderbook_conditions), 2),
        "2 * (neg risk \\ orderbooks) conditions": (filter_and_deduplicate(negrisk_ctfe_details, 'condition_id', negrisk_ctfe_conditions - orderbook_conditions), 2),
        "2 * (combined exchanges \\ ctf) conditions": (filter_and_deduplicate(combined_exchanges_details, 'condition_id', combined_exchanges_conditions - ctf_conditions), 2),
        "2 * (base ctfe \\ ctf) conditions": (filter_and_deduplicate(base_ctfe_details, 'condition_id', base_ctfe_conditions - ctf_conditions), 2),
        "2 * (neg risk \\ ctf) conditions": (filter_and_deduplicate(negrisk_ctfe_details, 'condition_id', negrisk_ctfe_conditions - ctf_conditions), 2),
        "2 * (combined oracles \\ orderbooks) questions": (filter_and_deduplicate(combined_oracles_details, 'question_id', combined_oracles_questions - orderbook_questions), 2),
        "2 * (moo uma \\ orderbooks) questions": (filter_and_deduplicate(moo_uma_details, 'question_id', moo_uma_questions - orderbook_questions), 2),
        "2 * (neg risk adapter \\ orderbooks) questions": (filter_and_deduplicate(negrisk_adapter_details, 'question_id', negrisk_adapter_questions - orderbook_questions), 2),
    }
    plot_cdfs(plot_data_1_1, f"Fork {fork_id} CDF: Creation Discrepancy Analysis", output_dir / "creation_analysis1.1.png", time_range)

    # --- PLOT 2: RESOLUTION (UNCHANGED) ---
    resolution_plot_data = {
        "Orderbooks Closed (from DB)": market_data['closed'],
        "2 * (All UMA) (QuestionResolved)": (pd.concat(uma_q_resolved.values()), 2),
    }
    plot_cdfs(resolution_plot_data, f"Fork {fork_id} CDF: Market Resolution Events", output_dir / "plot_2_resolution.png", time_range)
    
    # --- PLOT 3: NET ACTIVE (UNCHANGED) ---
    net_active_data = {
        "Net Active Orderbooks (DB)": calculate_net_active_df(market_data['found'], market_data['closed']),
        "2 * (Conditions Prepared - Resolved)": calculate_net_active_df(pd.concat([cond_prepared]*2), await get_events(pool, 'ConditionResolution', 'conditional_tokens', time_range)),
        "All Tokens - All UMA Resolved": calculate_net_active_df(pd.concat([negrisk_tokens, ctfe_tokens]), pd.concat(uma_q_resolved.values())),
        "All UMA (Init - Resolved)": calculate_net_active_df(pd.concat(uma_q_init.values()), pd.concat(uma_q_resolved.values())),
    }
    plot_lines(net_active_data, f"Fork {fork_id} Net Active Markets", output_dir / "plot_4_net_active.png", time_range)

# ----------------------------------------------------------------
# 5. MAIN EXECUTION
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
        time_range = await calculate_fork_time_range(pool, market_table)
        if not time_range[0]:
            logger.warning(f"Skipping Fork {fork_id}: No data found in '{market_table}'.")
            continue
        logger.info(f"Fork {fork_id} time range: {time_range[0]} to {time_range[1]}")
        market_data = {}
        market_data['found'], market_data['closed'] = await get_market_data(pool, market_table, time_range)
        output_dir = Path(PLOTS_DIR) / "lifetime" / str(fork_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        await generate_fork_plots(pool, fork_id, market_data, time_range, output_dir)

    await pool.close()
    logger.info("===== Plot generation complete. =====")

if __name__ == "__main__":
    asyncio.run(main())