import asyncio
import asyncpg
import os
import logging
import numpy as np
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

# --- Oracles of Interest (Verified from your diagnostic output) ---
ORACLES_OF_INTEREST = {
    "0x58e1745bedda7312c4cddb72618923da1b90efde": "Prep Oracle @ 0x58e1",
    "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7": "Prep Oracle @ 0x6507",
    "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296": "Prep Oracle @ 0xd91e"
}

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

def plot_cumulative_counts(data_dict: dict, title: str, filename: str):
    """Generates and saves a cumulative count plot with varied line styles."""
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax1 = plt.subplots(figsize=(16, 9))

    # --- ENHANCED: Define unique styles for each line ---
    style_map = {
        "RequestPrice": {"color": "forestgreen", "linestyle": "-", "linewidth": 2.5},
        "Prep Oracle @ 0x58e1": {"color": "darkorange", "linestyle": "-", "linewidth": 2.5},
        "Prep Oracle @ 0x6507": {"color": "indigo", "linestyle": "-", "linewidth": 2.5},
        "Prep Oracle @ 0xd91e": {"color": "steelblue", "linestyle": "-", "linewidth": 2.5},
        "Total Prepared": {"color": "lightgreen", "linestyle": "-", "linewidth": 4, "alpha": 0.9},
        "Difference (Prepared - Requested)": {"color": "firebrick", "linestyle": "--", "linewidth": 2.5}
    }

    # Define the order for plotting to ensure logical layering
    plot_order = ["Total Prepared", "RequestPrice", "Prep Oracle @ 0x58e1", "Prep Oracle @ 0x6507", "Prep Oracle @ 0xd91e", "Difference (Prepared - Requested)"]
    
    for label in plot_order:
        data = data_dict.get(label)
        if not data:
            logger.warning(f"No data for '{label}', skipping plot.")
            continue

        style = style_map.get(label, {})
        
        if label == "Difference (Prepared - Requested)":
            timestamps = [datetime.fromtimestamp(ts / 1000, tz=timezone.utc) for ts in data['timestamps_ms']]
            values = data['values']
            ax1.plot(timestamps, values, label=label, **style)
        else:
            datetimes = [datetime.fromtimestamp(ts / 1000, tz=timezone.utc) for ts in data]
            sorted_data = np.sort(datetimes)
            yvals = np.arange(1, len(sorted_data) + 1)
            ax1.plot(sorted_data, yvals, label=f"{label} (Total: {len(sorted_data):,})", **style)

    ax1.set_title(title, fontsize=18, pad=20)
    ax1.set_xlabel("Date", fontsize=14)
    ax1.set_ylabel("Cumulative Count of Events", fontsize=14)
    ax1.legend(fontsize=12, loc='upper left')
    ax1.margins(x=0.01, y=0.02)

    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    fig.autofmt_xdate()
    
    ax1.get_yaxis().set_major_formatter(
        plt.FuncFormatter(lambda x, p: format(int(x), ','))
    )
    ax1.tick_params(axis='both', which='major', labelsize=12)

    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    logger.info(f"Cumulative count plot saved to {filename}")
    plt.show()

# --- Main Analysis Logic ---
async def analyze_and_plot_by_oracle():
    logger.info("Initializing database connection...")
    db_pool = await get_db_pool(DB_CONFIG, pool_size=2)
    if not db_pool:
        return

    plot_data = defaultdict(list)
    try:
        async with db_pool.acquire() as conn1, db_pool.acquire() as conn2:
            logger.info("Fetching and processing ConditionPreparation events...")
            cond_prep_query = "SELECT topics, timestamp_ms FROM events_conditional_tokens WHERE event_name = 'ConditionPreparation'"
            
            async with conn1.transaction():
                async for record in conn1.cursor(cond_prep_query):
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

            logger.info("Fetching RequestPrice timestamps...")
            req_price_query = """
                (SELECT timestamp_ms FROM oov2 WHERE event_name = 'RequestPrice')
                UNION ALL
                (SELECT timestamp_ms FROM events_managed_oracle WHERE event_name = 'RequestPrice')
            """
            req_price_records = await conn2.fetch(req_price_query)
            plot_data["RequestPrice"] = [r['timestamp_ms'] for r in req_price_records]
            logger.info(f"Fetched {len(plot_data['RequestPrice']):,} RequestPrice timestamps.")

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
    
    if total_prepared_ts:
        plot_data["Total Prepared"] = sorted(total_prepared_ts)
        logger.info(f"Calculated 'Total Prepared' with {len(plot_data['Total Prepared']):,} events.")

        # --- CORRECTED: Robust difference calculation ---
        prepared_events = sorted([(ts, 1) for ts in total_prepared_ts])
        requested_events = sorted([(ts, -1) for ts in plot_data["RequestPrice"]])
        
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
        logger.warning("No data found for any target event type. Cannot generate plot.")
        return
        
    plot_cumulative_counts(
        plot_data,
        "Cumulative Event Counts Over Time",
        "oracle_and_request_counts.png"
    )

if __name__ == "__main__":
    asyncio.run(analyze_and_plot_by_oracle())