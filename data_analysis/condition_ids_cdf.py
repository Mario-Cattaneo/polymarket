import os
import asyncio
import asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging
import time
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

INTERESTING_ORACLES = [
    "0x58e1745b", # Oracle A
    "0x65070be9", # Oracle B
    "0xd91e80cf"  # Oracle C
]

# --- Time Filtering ---
FILTER_BY_TIME = True
START_TIME = 1766146619712  # Dec-19-2025
END_TIME = 1766673099000    # Dec-25-2025

# ----------------------------------------------------------------
# 2. DATA FETCHING
# ----------------------------------------------------------------

async def fetch_data(pool, query, params=[]):
    """Fetches data with timing logging."""
    try:
        async with pool.acquire() as connection:
            start = time.time()
            rows = await connection.fetch(query, *params)
            elapsed = time.time() - start
            if elapsed > 1.0:
                logger.info(f"Query fetched {len(rows):,} rows in {elapsed:.2f}s")
            return pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()
    except Exception as e:
        logger.error(f"SQL Error: {e}")
        return pd.DataFrame()

async def main():
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
    logger.info("Connected to database.")

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

    # 1. Fetch Orderbook
    # FILTER: exhaustion_cycle > 0
    logger.info("Fetching Orderbook data (exhaustion_cycle > 0)...")
    q_ob = f"SELECT condition_id, found_time_ms as timestamp FROM {FORK_MARKET_TABLE} WHERE exhaustion_cycle > 0"
    t_ob, p_ob = build_time_clause("found_time_ms", [])
    df_ob = await fetch_data(pool, q_ob + t_ob, p_ob)

    # 2. Fetch CTF
    # EXPLICIT FILTER: LOWER(contract_address) = $1
    logger.info("Fetching CTF data...")
    q_ctf = """
        SELECT topics[2] as condition_id, timestamp_ms as timestamp, '0x' || substring(topics[3] from 27) as oracle 
        FROM events_conditional_tokens 
        WHERE event_name = 'ConditionPreparation' 
        AND LOWER(contract_address) = $1
    """
    t_ctf, p_ctf = build_time_clause("timestamp_ms", [ADDR['conditional_tokens']])
    df_ctf = await fetch_data(pool, q_ctf + t_ctf, p_ctf)

    # 3. Fetch Exchanges
    # EXPLICIT FILTER: LOWER(contract_address) = $1
    logger.info("Fetching Exchange data...")
    async def fetch_ex(table, addr):
        q = f"""
            SELECT topics[4] as condition_id, timestamp_ms as timestamp 
            FROM {table} 
            WHERE event_name = 'TokenRegistered' 
            AND LOWER(contract_address) = $1
        """
        t, p = build_time_clause("timestamp_ms", [addr])
        return await fetch_data(pool, q + t, p)

    df_ctfe = await fetch_ex('events_ctf_exchange', ADDR['ctfe_exchange'])
    df_neg = await fetch_ex('events_neg_risk_exchange', ADDR['negrisk_exchange'])

    await pool.close()
    logger.info("Data fetching complete. Processing...")

    # ----------------------------------------------------------------
    # 3. DATA PROCESSING & PLOTTING
    # ----------------------------------------------------------------

    def prepare_cdf_data(df, label):
        if df.empty: return pd.DataFrame(columns=['datetime', 'count', 'label'])
        
        # Optimization: Sort by timestamp first
        df_sorted = df.sort_values('timestamp')
        
        # Drop duplicates keeping the first occurrence (earliest time)
        df_unique = df_sorted.drop_duplicates(subset='condition_id', keep='first').copy()
        
        # Convert to datetime
        df_unique['datetime'] = pd.to_datetime(df_unique['timestamp'], unit='ms', utc=True)
        
        # Create cumulative count (1 to N)
        df_unique['count'] = range(1, len(df_unique) + 1)
        df_unique['label'] = label
        
        return df_unique[['datetime', 'count', 'label']]

    # Prepare Data Groups
    plot_data = {'Orderbook (DB)': prepare_cdf_data(df_ob, 'Orderbook')}
    plot_data['CTF (All Preps)'] = prepare_cdf_data(df_ctf, 'CTF (All)')

    for oracle_hash in INTERESTING_ORACLES:
        oracle_df = df_ctf[df_ctf['oracle'].str.startswith(oracle_hash)]
        plot_data[f'Oracle {oracle_hash[:6]}...'] = prepare_cdf_data(oracle_df, f'Oracle {oracle_hash[:6]}')

    plot_data['CTF Exchange'] = prepare_cdf_data(df_ctfe, 'CTF Exchange')
    plot_data['NegRisk Exchange'] = prepare_cdf_data(df_neg, 'NegRisk Exchange')

    df_combined = pd.concat([df_ctfe, df_neg])
    plot_data['Combined Exchanges'] = prepare_cdf_data(df_combined, 'Combined Exchanges')

    # Plotting
    logger.info("Generating Plot...")
    plt.figure(figsize=(14, 8))
    
    styles = {
        'Orderbook (DB)':       {'color': 'black', 'linewidth': 2.5, 'linestyle': '-'},
        'CTF (All Preps)':      {'color': 'gray',  'linewidth': 2,   'linestyle': '--'},
        'Combined Exchanges':   {'color': 'blue',  'linewidth': 2.5, 'linestyle': '-'},
        'CTF Exchange':         {'color': 'cyan',  'linewidth': 1,   'linestyle': ':'},
        'NegRisk Exchange':     {'color': 'navy',  'linewidth': 1,   'linestyle': ':'},
    }
    oracle_colors = ['red', 'orange', 'purple']

    oracle_idx = 0
    for label, df in plot_data.items():
        if df.empty:
            logger.warning(f"No data for {label}")
            continue
            
        style = styles.get(label, {})
        if 'Oracle' in label:
            style = {'color': oracle_colors[oracle_idx % len(oracle_colors)], 'linewidth': 1.5, 'linestyle': '-.'}
            oracle_idx += 1

        plt.plot(df['datetime'], df['count'], label=f"{label} (n={len(df):,})", **style)

    plt.title('Cumulative Arrival of Unique Condition IDs (CDF)', fontsize=16)
    plt.xlabel('Time (UTC)', fontsize=12)
    plt.ylabel('Cumulative Count of Unique Conditions', fontsize=12)
    plt.grid(True, which='both', linestyle='--', alpha=0.6)
    plt.legend(loc='upper left', fontsize=10)
    
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gcf().autofmt_xdate()

    output_file = "condition_ids_cdf_optimized.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    logger.info(f"Plot saved to {output_file}")
    plt.show()

if __name__ == "__main__":
    if not all([PG_HOST, DB_NAME, DB_USER, DB_PASS]):
        logger.error("Database credentials not set.")
    else:
        asyncio.run(main())