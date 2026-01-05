import asyncio
import asyncpg
import os
import logging
import numpy as np
import pandas as pd
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

# --- Database Configuration ---
DB_CONFIG = {
    "socket_env": "PG_SOCKET",
    "port_env": "POLY_PG_PORT",
    "name_env": "POLY_DB",
    "user_env": "POLY_DB_CLI",
    "pass_env": "POLY_DB_CLI_PASS"
}

# --- CTF Contract Address ---
CTF_ADDRESS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower()

# --- Oracle Aliases (for display in plots) ---
ORACLE_ALIASES = {
    "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7": "MOOV2 Adapter",
    "0x58e1745bedda7312c4cddb72618923da1b90efde": "Centralized Adapter",
    "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296": "Negrisk UmaCtfAdapter",
}

# --- Oracles of Interest (Verified from your diagnostic output) ---
ORACLES_OF_INTEREST = {
    "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7": "MOOV2 Adapter",
    "0x58e1745bedda7312c4cddb72618923da1b90efde": "Centralized Adapter",
    "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296": "Negrisk UmaCtfAdapter"
}

# --- Dynamic Time Filtering (Will be set in main()) ---
FILTER_BY_TIME = False
START_TIME = None
END_TIME = None

# --- Helper Functions ---
async def get_db_pool(db_config: dict, pool_size: int):
    """Creates an asyncpg database connection pool."""
    try:
        for key in db_config.values():
            if key not in os.environ:
                logger.error(f"Missing required environment variable: {key}")
                return None
        
        pool = await asyncpg.create_pool(
            host=os.environ.get(db_config['socket_env']),
            port=os.environ.get(db_config['port_env']),
            database=os.environ.get(db_config['name_env']),
            user=os.environ.get(db_config['user_env']),
            password=os.environ.get(db_config['pass_env']),
            min_size=pool_size,
            max_size=pool_size
        )
        return pool
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        raise e

def prepare_cdf_data(timestamps_ms, label):
    """Prepares CDF data from a list of timestamps (in milliseconds)."""
    if not timestamps_ms:
        return pd.DataFrame(columns=['datetime', 'count', 'label'])
    
    # Sort timestamps
    sorted_ts = sorted(timestamps_ms)
    
    # Create cumulative count for ALL events (including duplicates)
    df = pd.DataFrame({
        'timestamp_ms': sorted_ts,
        'datetime': pd.to_datetime(sorted_ts, unit='ms', utc=True),
        'count': range(1, len(sorted_ts) + 1),
        'label': label
    })
    
    return df[['datetime', 'count', 'label']]

async def fetch_time_range(pool):
    """Fetches the min and max timestamp_ms from ConditionPreparation events."""
    logger.info("Fetching min/max timestamps from ConditionPreparation events...")
    query = f"""
        SELECT MIN(timestamp_ms) as min_time, MAX(timestamp_ms) as max_time
        FROM events_conditional_tokens 
        WHERE event_name = 'ConditionPreparation' 
        AND LOWER(contract_address) = $1
    """
    async with pool.acquire() as conn:
        result = await conn.fetchval(query, CTF_ADDRESS)
        row = await conn.fetchrow(query, CTF_ADDRESS)
    
    if not row or row['min_time'] is None:
        logger.error("Could not determine time range from ConditionPreparation events.")
        return None, None
        
    min_time = row['min_time']
    max_time = row['max_time']
    
    min_dt = datetime.fromtimestamp(min_time / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    max_dt = datetime.fromtimestamp(max_time / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"Time Range determined: {min_dt} to {max_dt}")
    return min_time, max_time


def plot_prepared_vs_requested_cdf(plot_data: dict):
    """Generates a single CDF plot comparing prepared conditions and requested prices."""
    
    # Extract data series
    oracle1_prepared = plot_data.get("MOOV2 Adapter", [])
    oracle2_prepared = plot_data.get("Centralized Adapter", [])
    oracle3_prepared = plot_data.get("Negrisk UmaCtfAdapter", [])
    
    moov2_requested = plot_data.get("MOOV2", [])
    oov2_requested = plot_data.get("OOV2", [])
    diff_data = plot_data.get("Difference (Prepared - Requested)", {})
    
    # Create simple figure with single axis
    fig, ax = plt.subplots(figsize=(16, 9))
    
    # Log actual data for verification
    logger.info(f"\nMOOV2 Adapter: {len(oracle1_prepared):,} events")
    logger.info(f"Centralized Adapter: {len(oracle2_prepared):,} events")
    logger.info(f"Negrisk UmaCtfAdapter: {len(oracle3_prepared):,} events")
    logger.info(f"MOOV2 Requested: {len(moov2_requested):,} events")
    logger.info(f"OOV2 Requested: {len(oov2_requested):,} events")
    
    # Plot requested prices FIRST (solid lines) - cool tones
    if moov2_requested:
        cdf_data = prepare_cdf_data(moov2_requested, "MOOV2 (Requested)")
        ax.plot(cdf_data['datetime'], cdf_data['count'], 
                label=f"MOOV2 Requested (n={len(moov2_requested):,})",
                color='#001f3f', linewidth=3, linestyle='-', zorder=1)
    
    if oov2_requested:
        cdf_data = prepare_cdf_data(oov2_requested, "OOV2 (Requested)")
        ax.plot(cdf_data['datetime'], cdf_data['count'], 
                label=f"OOV2 Requested (n={len(oov2_requested):,})",
                color='#00ced1', linewidth=3, linestyle='-', zorder=2)
    
    # Plot prepared oracle data (dashed lines) - warm tones
    if oracle1_prepared:
        cdf_data = prepare_cdf_data(oracle1_prepared, "MOOV2 Adapter (Prepared)")
        ax.plot(cdf_data['datetime'], cdf_data['count'], 
                label=f"MOOV2 Adapter (n={len(oracle1_prepared):,})",
                color='#ff6b35', linewidth=3.5, linestyle='--', zorder=5)
    
    if oracle3_prepared:
        cdf_data = prepare_cdf_data(oracle3_prepared, "Negrisk UmaCtfAdapter (Prepared)")
        ax.plot(cdf_data['datetime'], cdf_data['count'], 
                label=f"Negrisk UmaCtfAdapter (n={len(oracle3_prepared):,})",
                color='#ff8c00', linewidth=3.5, linestyle='--', zorder=6)
    
    if oracle2_prepared:
        cdf_data = prepare_cdf_data(oracle2_prepared, "Centralized Adapter (Prepared)")
        ax.plot(cdf_data['datetime'], cdf_data['count'], 
                label=f"Centralized Adapter (n={len(oracle2_prepared):,})",
                color='#dc143c', linewidth=3.5, linestyle='--', zorder=7)
    
    # Plot cumulative difference on same axis - vibrant purple
    if diff_data and diff_data.get('timestamps_ms'):
        timestamps = [datetime.fromtimestamp(ts / 1000, tz=timezone.utc) for ts in diff_data['timestamps_ms']]
        values = diff_data['values']
        
        ax.plot(timestamps, values, 
                label=f"Prepared - Requested (n={values[-1]:,})",
                color='#9370db', linewidth=3.5, linestyle='-', zorder=8, alpha=0.95)
    
    # Configure axis
    ax.set_title('CDF: Prepared Conditions vs Requested Prices', fontsize=16, pad=20)
    ax.set_xlabel('Time (UTC)', fontsize=12)
    ax.set_ylabel('Cumulative Count of Events', fontsize=12)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))
    ax.grid(True, which='both', linestyle='--', alpha=0.4, zorder=0)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    
    ax.legend(fontsize=11, loc='upper left', framealpha=0.95)
    
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig("cdf_prepared_vs_requested_combined.png", dpi=300, bbox_inches='tight')
    logger.info("Plot saved: cdf_prepared_vs_requested_combined.png\n")
    
    plt.show()


# --- Main Analysis Logic ---
async def analyze_and_plot_by_oracle():
    global START_TIME, END_TIME, FILTER_BY_TIME
    
    logger.info("Initializing database connection...")
    db_pool = await get_db_pool(DB_CONFIG, pool_size=3)
    if not db_pool:
        return

    # Fetch time range first
    START_TIME, END_TIME = await fetch_time_range(db_pool)
    if START_TIME is None:
        await db_pool.close()
        return
    FILTER_BY_TIME = True

    plot_data = defaultdict(list)
    try:
        async with db_pool.acquire() as conn1, db_pool.acquire() as conn2, db_pool.acquire() as conn3:
            # --- Fetch ConditionPreparation events (with time filtering) ---
            logger.info("Fetching and processing ConditionPreparation events...")
            cond_prep_query = """
                SELECT topics, timestamp_ms FROM events_conditional_tokens 
                WHERE event_name = 'ConditionPreparation' 
                AND LOWER(contract_address) = $1
                AND timestamp_ms >= $2 
                AND timestamp_ms <= $3
            """
            
            async with conn1.transaction():
                async for record in conn1.cursor(cond_prep_query, CTF_ADDRESS, START_TIME, END_TIME):
                    try:
                        oracle_topic = record['topics'][2]
                        oracle_address = f"0x{oracle_topic[-40:]}".lower()
                        
                        if oracle_address in ORACLES_OF_INTEREST:
                            plot_label = ORACLES_OF_INTEREST[oracle_address]
                            plot_data[plot_label].append(record['timestamp_ms'])
                    except (IndexError, TypeError):
                        continue
            
            for address, label in ORACLES_OF_INTEREST.items():
                logger.info(f"Found {len(plot_data[label]):,} events for {label}")

            # --- Fetch RequestPrice from OOV2 (with time filtering) ---
            logger.info("Fetching RequestPrice timestamps from OOV2...")
            oov2_query = """
                SELECT timestamp_ms FROM oov2 
                WHERE event_name = 'RequestPrice'
                AND timestamp_ms >= $1 
                AND timestamp_ms <= $2
            """
            oov2_records = await conn2.fetch(oov2_query, START_TIME, END_TIME)
            plot_data["OOV2"] = [r['timestamp_ms'] for r in oov2_records]
            logger.info(f"Fetched {len(plot_data['OOV2']):,} RequestPrice timestamps from OOV2.")

            # --- Fetch RequestPrice from events_managed_oracle (MOOV2) (with time filtering) ---
            logger.info("Fetching RequestPrice timestamps from events_managed_oracle (MOOV2)...")
            moov2_query = """
                SELECT timestamp_ms FROM events_managed_oracle 
                WHERE event_name = 'RequestPrice'
                AND timestamp_ms >= $1 
                AND timestamp_ms <= $2
            """
            moov2_records = await conn3.fetch(moov2_query, START_TIME, END_TIME)
            plot_data["MOOV2"] = [r['timestamp_ms'] for r in moov2_records]
            logger.info(f"Fetched {len(plot_data['MOOV2']):,} RequestPrice timestamps from MOOV2.")

    except Exception as e:
        logger.critical(f"An error occurred during database operations: {e}", exc_info=True)
    finally:
        if db_pool:
            await db_pool.close()
            logger.info("Database connection closed.")

    # --- Prepare combined and difference data series ---
    total_prepared_ts = []
    for label in ORACLES_OF_INTEREST.values():
        total_prepared_ts.extend(plot_data[label])
    
    total_requested_ts = plot_data.get("MOOV2", []) + plot_data.get("OOV2", [])
    
    if total_prepared_ts or total_requested_ts:
        # --- Calculate difference (Prepared - Requested) ---
        prepared_events = sorted([(ts, 1) for ts in total_prepared_ts])
        requested_events = sorted([(ts, -1) for ts in total_requested_ts])
        
        all_events = sorted(prepared_events + requested_events)
        
        diff_timestamps = []
        diff_values = []
        current_diff = 0
        
        if all_events:
            # Add a starting point at time zero
            diff_timestamps.append(all_events[0][0] - 1)
            diff_values.append(0)

            for ts, change in all_events:
                current_diff += change
                diff_timestamps.append(ts)
                diff_values.append(current_diff)
        
        plot_data["Difference (Prepared - Requested)"] = {
            "timestamps_ms": diff_timestamps,
            "values": diff_values
        }
        logger.info(f"Calculated 'Difference' series with {len(diff_timestamps)} data points.")

    # --- Plot the results ---
    if not any(plot_data.values()):
        logger.warning("No data found for any target event type. Cannot generate plots.")
        return
    
    # Log raw data counts before plotting
    logger.info("\n--- Raw Data Counts Before Plotting ---")
    for key, values in plot_data.items():
        if isinstance(values, list):
            logger.info(f"{key}: {len(values):,} events")
        elif isinstance(values, dict):
            logger.info(f"{key}: {len(values.get('timestamps_ms', [])):,} events")
    logger.info("--- End Raw Data Counts ---\n")
        
    plot_prepared_vs_requested_cdf(plot_data)


if __name__ == "__main__":
    asyncio.run(analyze_and_plot_by_oracle())