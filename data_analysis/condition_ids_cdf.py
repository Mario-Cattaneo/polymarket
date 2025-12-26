import os
import asyncio
import asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging
from datetime import datetime, timezone

# ----------------------------------------------------------------
# 1. SETUP & CONFIGURATION
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

# --- Database Credentials ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- Configuration ---
FORK_MARKET_TABLE = "markets_3"
ADDR = {
    'negrisk_exchange': "0xc5d563a36ae78145c45a50134d48a1215220f80a".lower(),
    'ctfe_exchange': "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e".lower(),
    'conditional_tokens': "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower(),
}

# The 3 Oracles of interest based on your logs
INTERESTING_ORACLES = [
    "0x58e1745b", # Oracle A
    "0x65070be9", # Oracle B
    "0xd91e80cf"  # Oracle C (NegRisk adapter usually)
]

# --- Time Filtering (Matches your logs) ---
FILTER_BY_TIME = True
START_TIME = 1766146619712  # From your logs
END_TIME = 1766673099000    # From your logs

# ----------------------------------------------------------------
# 2. DATA FETCHING
# ----------------------------------------------------------------

async def fetch_data(pool, query, params=[]):
    try:
        async with pool.acquire() as connection:
            rows = await connection.fetch(query, *params)
            return pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()
    except Exception as e:
        logger.error(f"SQL Error: {e}")
        return pd.DataFrame()

async def main():
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Connected to database.")

    # --- Helper for Time Clauses ---
    def build_time_clause(column_name, current_params):
        if not FILTER_BY_TIME: return "", current_params
        clauses = []
        new_params = current_params.copy()
        if START_TIME:
            new_params.append(START_TIME)
            clauses.append(f"{column_name} >= ${len(new_params)}")
        if END_TIME:
            new_params.append(END_TIME)
            clauses.append(f"{column_name} <= ${len(new_params)}")
        return (" AND " + " AND ".join(clauses) if clauses else ""), new_params

    # 1. Fetch Orderbook (Markets 3)
    logger.info("Fetching Orderbook data...")
    q_ob = f"SELECT condition_id, found_time_ms as timestamp FROM {FORK_MARKET_TABLE} WHERE exhaustion_cycle != 0"
    t_ob, p_ob = build_time_clause("found_time_ms", [])
    df_ob = await fetch_data(pool, q_ob + t_ob, p_ob)

    # 2. Fetch CTF (Condition Preparation)
    logger.info("Fetching CTF (ConditionPreparation) data...")
    q_ctf = "SELECT topics[2] as condition_id, timestamp_ms as timestamp, '0x' || substring(topics[3] from 27) as oracle FROM events_conditional_tokens WHERE event_name = 'ConditionPreparation' AND LOWER(contract_address) = $1"
    t_ctf, p_ctf = build_time_clause("timestamp_ms", [ADDR['conditional_tokens']])
    df_ctf = await fetch_data(pool, q_ctf + t_ctf, p_ctf)

    # 3. Fetch Exchanges
    logger.info("Fetching Exchange data...")
    async def fetch_ex(table, addr):
        q = f"SELECT topics[4] as condition_id, timestamp_ms as timestamp FROM {table} WHERE event_name = 'TokenRegistered' AND LOWER(contract_address) = $1"
        t, p = build_time_clause("timestamp_ms", [addr])
        return await fetch_data(pool, q + t, p)

    df_ctfe = await fetch_ex('events_ctf_exchange', ADDR['ctfe_exchange'])
    df_neg = await fetch_ex('events_neg_risk_exchange', ADDR['negrisk_exchange'])

    await pool.close()
    logger.info("Data fetching complete. Processing...")

    # ----------------------------------------------------------------
    # 3. DATA PROCESSING
    # ----------------------------------------------------------------

    def prepare_cdf_data(df, label):
        if df.empty:
            return pd.DataFrame(columns=['timestamp', 'count', 'label'])
        
        # 1. Keep only unique condition_ids (first occurrence by time)
        df_sorted = df.sort_values('timestamp')
        df_unique = df_sorted.drop_duplicates(subset='condition_id', keep='first').copy()
        
        # 2. Convert timestamp to datetime
        df_unique['datetime'] = pd.to_datetime(df_unique['timestamp'], unit='ms', utc=True)
        
        # 3. Create cumulative count
        df_unique['count'] = range(1, len(df_unique) + 1)
        df_unique['label'] = label
        
        return df_unique[['datetime', 'count', 'label']]

    # --- Prepare Groups ---
    
    # 1. Orderbook
    plot_data = {'Orderbook (DB)': prepare_cdf_data(df_ob, 'Orderbook')}

    # 2. CTF (All)
    plot_data['CTF (All Preps)'] = prepare_cdf_data(df_ctf, 'CTF (All)')

    # 3. The 3 Oracles
    for oracle_hash in INTERESTING_ORACLES:
        # Filter CTF data by oracle string (partial match)
        oracle_df = df_ctf[df_ctf['oracle'].str.startswith(oracle_hash)]
        plot_data[f'Oracle {oracle_hash[:6]}...'] = prepare_cdf_data(oracle_df, f'Oracle {oracle_hash[:6]}')

    # 4. CTF Exchange (Alone)
    plot_data['CTF Exchange'] = prepare_cdf_data(df_ctfe, 'CTF Exchange')

    # 5. NegRisk Exchange (Alone)
    plot_data['NegRisk Exchange'] = prepare_cdf_data(df_neg, 'NegRisk Exchange')

    # 6. Combined Exchanges
    df_combined = pd.concat([df_ctfe, df_neg])
    plot_data['Combined Exchanges'] = prepare_cdf_data(df_combined, 'Combined Exchanges')

    # ----------------------------------------------------------------
    # 4. PLOTTING
    # ----------------------------------------------------------------
    
    logger.info("Generating Plot...")
    plt.figure(figsize=(14, 8))
    
    # Define styles/colors for clarity
    styles = {
        'Orderbook (DB)':       {'color': 'black', 'linewidth': 2.5, 'linestyle': '-'},
        'CTF (All Preps)':      {'color': 'gray',  'linewidth': 2,   'linestyle': '--'},
        'Combined Exchanges':   {'color': 'blue',  'linewidth': 2.5, 'linestyle': '-'},
        'CTF Exchange':         {'color': 'cyan',  'linewidth': 1,   'linestyle': ':'},
        'NegRisk Exchange':     {'color': 'navy',  'linewidth': 1,   'linestyle': ':'},
    }
    # Default style for oracles
    oracle_colors = ['red', 'orange', 'purple']

    # Plot lines
    oracle_idx = 0
    for label, df in plot_data.items():
        if df.empty:
            logger.warning(f"No data for {label}")
            continue
            
        style = styles.get(label, {})
        
        # Assign colors to oracles dynamically
        if 'Oracle' in label:
            style = {'color': oracle_colors[oracle_idx % len(oracle_colors)], 'linewidth': 1.5, 'linestyle': '-.'}
            oracle_idx += 1

        plt.plot(df['datetime'], df['count'], label=f"{label} (n={len(df):,})", **style)

    # Formatting
    plt.title('Cumulative Arrival of Unique Condition IDs (CDF)', fontsize=16)
    plt.xlabel('Time (UTC)', fontsize=12)
    plt.ylabel('Cumulative Count of Unique Conditions', fontsize=12)
    plt.grid(True, which='both', linestyle='--', alpha=0.6)
    plt.legend(loc='upper left', fontsize=10)
    
    # Format X-Axis Date
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gcf().autofmt_xdate()

    # Save and Show
    output_file = "condition_ids_cdf.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    logger.info(f"Plot saved to {output_file}")
    plt.show()

if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials not set.")
    else:
        asyncio.run(main())