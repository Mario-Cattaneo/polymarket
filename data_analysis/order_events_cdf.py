#!/usr/bin/env python3
"""
Script to analyze and plot CDFs for OrderFilled and OrdersMatched events.
Plots 4 CDFs:
1. OrdersMatched events (by making amount)
2. OrderFilled events (by making amount)
3. OrderFilled events WITHOUT matching OrdersMatched in same tx
4. OrderFilled events WITH matching OrdersMatched in same tx
"""

import os
import asyncio
import asyncpg
import logging
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger()

# --- CONFIGURATION ---
DB_CONFIG = {
    "socket_env": "PG_SOCKET",
    "port_env": "POLY_PG_PORT",
    "name_env": "POLY_DB",
    "user_env": "POLY_DB_CLI",
    "pass_env": "POLY_DB_CLI_PASS"
}

TABLE_NAME = "events_ctf_exchange"
SAMPLE_SIZE = 100000  # Sample size for each event type
PLOTS_DIR = os.getenv('POLY_PLOTS', './plots')
DAYS = 3  # Number of days for the window

async def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        for key, env_var in DB_CONFIG.items():
            if env_var not in os.environ:
                logger.error(f"Missing environment variable: {env_var}")
                return None
        
        conn = await asyncpg.connect(
            host=os.getenv(DB_CONFIG['socket_env']),
            port=os.getenv(DB_CONFIG['port_env']),
            database=os.getenv(DB_CONFIG['name_env']),
            user=os.getenv(DB_CONFIG['user_env']),
            password=os.getenv(DB_CONFIG['pass_env'])
        )
        logger.info("✅ Database connection established")
        return conn
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return None


def extract_uint256_from_data(data_hex, slot):
    """Extract a uint256 value from the data field at a specific slot."""
    try:
        if isinstance(data_hex, bytes):
            data_hex = data_hex.hex()
        else:
            data_hex = data_hex.lower().replace('0x', '')
        
        # Each slot is 64 hex chars (32 bytes)
        start = slot * 64
        end = start + 64
        hex_value = data_hex[start:end]
        return int(hex_value, 16)
    except Exception as e:
        logger.error(f"Error extracting uint256 from data: {e}")
        return None


async def get_time_range(conn):
    """Get the time range for events in the table."""
    query = f"""
        SELECT MIN(timestamp_ms) as min_ts, MAX(timestamp_ms) as max_ts
        FROM {TABLE_NAME}
        WHERE event_name IN ('OrderFilled', 'OrdersMatched')
    """
    try:
        result = await conn.fetchrow(query)
        min_ts = result['min_ts']
        max_ts = result['max_ts']
        
        if min_ts and max_ts:
            min_dt = datetime.fromtimestamp(min_ts / 1000.0)
            max_dt = datetime.fromtimestamp(max_ts / 1000.0)
            logger.info(f"Time range: {min_dt} to {max_dt}")
            return min_ts, max_ts
        return None, None
    except Exception as e:
        logger.error(f"Error getting time range: {e}")
        return None, None


async def sample_ordersmatched_events(conn, min_ts, max_ts):
    """Sample OrdersMatched events using TABLESAMPLE SYSTEM."""
    logger.info(f"Sampling {SAMPLE_SIZE} OrdersMatched events with TABLESAMPLE SYSTEM...")
    
    query = f"""
        SELECT transaction_hash, data, timestamp_ms
        FROM {TABLE_NAME} TABLESAMPLE SYSTEM (1)
        WHERE event_name = 'OrdersMatched'
            AND timestamp_ms BETWEEN $1 AND $2
        LIMIT $3
    """
    try:
        records = await conn.fetch(query, min_ts, max_ts, SAMPLE_SIZE)
        logger.info(f"Fetched {len(records)} OrdersMatched events")
        events = []
        for record in records:
            making = extract_uint256_from_data(record['data'], 2)
            if making is not None and making > 0:
                events.append({
                    'tx_hash': record['transaction_hash'],
                    'making': making,
                    'timestamp_ms': record['timestamp_ms']
                })
        logger.info(f"Parsed {len(events)} valid OrdersMatched events")
        return events
    except Exception as e:
        logger.error(f"Error sampling OrdersMatched: {e}")
        import traceback
        traceback.print_exc()
        return []


async def sample_orderfilled_events(conn, min_ts, max_ts):
    """Sample OrderFilled events using TABLESAMPLE SYSTEM."""
    logger.info(f"Sampling {SAMPLE_SIZE} OrderFilled events with TABLESAMPLE SYSTEM...")
    
    query = f"""
        SELECT transaction_hash, data, timestamp_ms
        FROM {TABLE_NAME} TABLESAMPLE SYSTEM (1)
        WHERE event_name = 'OrderFilled'
            AND timestamp_ms BETWEEN $1 AND $2
        LIMIT $3
    """
    try:
        records = await conn.fetch(query, min_ts, max_ts, SAMPLE_SIZE)
        logger.info(f"Fetched {len(records)} OrderFilled events")
        events = []
        for record in records:
            making = extract_uint256_from_data(record['data'], 2)
            if making is not None and making > 0:
                events.append({
                    'tx_hash': record['transaction_hash'],
                    'making': making,
                    'timestamp_ms': record['timestamp_ms']
                })
        logger.info(f"Parsed {len(events)} valid OrderFilled events")
        return events
    except Exception as e:
        logger.error(f"Error sampling OrderFilled: {e}")
        import traceback
        traceback.print_exc()
        return []


async def check_ordersmatched_in_transactions(conn, tx_hashes):
    """For a given set of transaction hashes, check which ones contain OrdersMatched events."""
    logger.info(f"Checking {len(tx_hashes)} transactions for OrdersMatched events...")
    
    if not tx_hashes:
        return set()
    
    query = f"""
        SELECT DISTINCT transaction_hash
        FROM {TABLE_NAME}
        WHERE event_name = 'OrdersMatched'
            AND transaction_hash = ANY($1)
    """
    
    try:
        records = await conn.fetch(query, list(tx_hashes))
        matched_tx_hashes = {record['transaction_hash'] for record in records}
        logger.info(f"Found {len(matched_tx_hashes)} transactions that also have OrdersMatched")
        return matched_tx_hashes
    except Exception as e:
        logger.error(f"Error checking OrdersMatched in transactions: {e}")
        return set()


def plot_cdfs(ordersmatched_data, orderfilled_data, orderfilled_no_match, orderfilled_with_match):
    """Plot CDFs for all four categories with absolute count on y-axis."""
    logger.info("Generating CDF plots...")
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Helper function to plot CDF
    def plot_cdf(data, label, color, linestyle='-'):
        if len(data) == 0:
            logger.warning(f"No data for {label}")
            return
        
        sorted_data = np.sort(data)
        y = np.arange(1, len(sorted_data) + 1)  # Absolute count
        ax.plot(sorted_data, y, label=f"{label} (n={len(data):,})", 
                color=color, linestyle=linestyle, linewidth=2, alpha=0.8)
    
    # Plot all CDFs
    plot_cdf(ordersmatched_data, "OrdersMatched", "blue", "-")
    plot_cdf(orderfilled_data, "OrderFilled (All)", "red", "-")
    plot_cdf(orderfilled_no_match, "OrderFilled (No Match)", "orange", "--")
    plot_cdf(orderfilled_with_match, "OrderFilled (With Match)", "green", "-.")
    
    ax.set_xlabel("Making Amount (wei)", fontsize=12)
    ax.set_ylabel("Cumulative Count", fontsize=12)
    ax.set_title("CDF of Making Amounts: OrderFilled vs OrdersMatched Events", fontsize=14, fontweight='bold')
    ax.set_xscale('log')
    ax.grid(True, alpha=0.3, which='both')
    ax.legend(fontsize=10, loc='lower right')
    
    # Add some statistics
    stats_text = []
    for data, name in [(ordersmatched_data, "OrdersMatched"),
                       (orderfilled_data, "OrderFilled (All)"),
                       (orderfilled_no_match, "OrderFilled (No Match)"),
                       (orderfilled_with_match, "OrderFilled (With Match)")]:
        if len(data) > 0:
            median = np.median(data)
            mean = np.mean(data)
            stats_text.append(f"{name}: median={median:.2e}, mean={mean:.2e}")
    
    textstr = '\n'.join(stats_text)
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    ax.text(0.02, 0.98, textstr, transform=ax.transAxes, fontsize=8,
            verticalalignment='top', bbox=props, family='monospace')
    
    plt.tight_layout()
    
    # Save plot
    os.makedirs(PLOTS_DIR, exist_ok=True)
    output_path = os.path.join(PLOTS_DIR, 'order_events_cdf.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    logger.info(f"✅ Plot saved to: {output_path}")
    
    plt.close()


def plot_cdfs_time(ordersmatched_events, orderfilled_events, orderfilled_no_match_events, orderfilled_with_match_events):
    """Plot time-based CDFs for all four categories."""
    import matplotlib.dates as mdates
    logger.info("Generating time-based CDF plots...")
    fig, ax = plt.subplots(figsize=(12, 8))
    def plot_time_cdf(events, label, color, linestyle='-'):
        if len(events) == 0:
            logger.warning(f"No data for {label}")
            return
        # Convert timestamps to datetime
        times = [datetime.fromtimestamp(e['timestamp_ms'] / 1000.0) for e in events]
        times_sorted = np.sort(times)
        y = np.arange(1, len(times_sorted) + 1)
        ax.plot(times_sorted, y, label=f"{label} (n={len(events):,})", color=color, linestyle=linestyle, linewidth=2, alpha=0.8)
    plot_time_cdf(ordersmatched_events, "OrdersMatched", "blue", "-")
    plot_time_cdf(orderfilled_events, "OrderFilled (All)", "red", "-")
    plot_time_cdf(orderfilled_no_match_events, "OrderFilled (No Match)", "orange", "--")
    plot_time_cdf(orderfilled_with_match_events, "OrderFilled (With Match)", "green", "-.")
    ax.set_xlabel("Time", fontsize=12)
    ax.set_ylabel("Cumulative Count", fontsize=12)
    ax.set_title("Time-based CDF of Events: OrderFilled vs OrdersMatched", fontsize=14, fontweight='bold')
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    fig.autofmt_xdate()
    ax.grid(True, alpha=0.3, which='both')
    ax.legend(fontsize=10, loc='upper left')
    plt.tight_layout()
    os.makedirs(PLOTS_DIR, exist_ok=True)
    output_path = os.path.join(PLOTS_DIR, 'order_events_time_cdf.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    logger.info(f"✅ Time-based CDF plot saved to: {output_path}")
    plt.close()


async def time_bucketed_sample(conn, event_name, min_ts, max_ts, sample_size, buckets=20):
    """Sample events uniformly across time buckets."""
    import math
    bucket_size = (max_ts - min_ts) // buckets
    per_bucket = sample_size // buckets
    sampled = []
    for i in range(buckets):
        start = min_ts + i * bucket_size
        end = start + bucket_size
        query = f"""
            SELECT transaction_hash, data, timestamp_ms
            FROM {TABLE_NAME}
            WHERE event_name = $1 AND timestamp_ms BETWEEN $2 AND $3
            ORDER BY RANDOM()
            LIMIT $4
        """
        try:
            records = await conn.fetch(query, event_name, start, end, per_bucket)
            for record in records:
                making = extract_uint256_from_data(record['data'], 2)
                if making is not None and making > 0:
                    sampled.append({
                        'tx_hash': record['transaction_hash'],
                        'making': making,
                        'timestamp_ms': record['timestamp_ms']
                    })
        except Exception as e:
            logger.error(f"Error sampling {event_name} in bucket {i}: {e}")
    logger.info(f"Sampled {len(sampled)} {event_name} events across {buckets} buckets")
    return sampled


async def get_events_in_window(conn, event_name, min_ts, max_ts):
    query = f"""
        SELECT transaction_hash, data, timestamp_ms
        FROM {TABLE_NAME}
        WHERE event_name = $1 AND timestamp_ms BETWEEN $2 AND $3
        ORDER BY timestamp_ms ASC
    """
    try:
        records = await conn.fetch(query, event_name, min_ts, max_ts)
        events = []
        for record in records:
            making = extract_uint256_from_data(record['data'], 2)
            if making is not None and making > 0:
                events.append({
                    'tx_hash': record['transaction_hash'],
                    'making': making,
                    'timestamp_ms': record['timestamp_ms']
                })
        logger.info(f"Fetched {len(events)} {event_name} events in window")
        return events
    except Exception as e:
        logger.error(f"Error fetching {event_name} events in window: {e}")
        return []


async def get_min_found_time_ms(conn):
    row = await conn.fetchrow("SELECT MIN(found_time_ms) AS min_time FROM markets_5")
    return row['min_time'] if row else None

async def get_max_time_ms(conn, event_name):
    row = await conn.fetchrow(f"SELECT MAX(timestamp_ms) AS max_time FROM {TABLE_NAME} WHERE event_name = $1", event_name)
    return row['max_time'] if row else None

async def get_buffer_last_trade_times(conn, start_time, end_time):
    query = """
        SELECT server_time_ms FROM buffer_last_trade_prices_2
        WHERE server_time_ms >= $1 AND server_time_ms <= $2
        ORDER BY server_time_ms ASC
    """
    rows = await conn.fetch(query, start_time, end_time)
    return [r['server_time_ms'] for r in rows]

async def get_event_times(conn, event_name, start_time, end_time):
    query = f"""
        SELECT timestamp_ms FROM {TABLE_NAME}
        WHERE event_name = $1 AND timestamp_ms >= $2 AND timestamp_ms <= $3
        ORDER BY timestamp_ms ASC
    """
    rows = await conn.fetch(query, event_name, start_time, end_time)
    return [r['timestamp_ms'] for r in rows]

def plot_cdfs_three(time_dict, start_time, end_time):
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from datetime import datetime
    fig, ax = plt.subplots(figsize=(12, 8))
    for label, times in time_dict.items():
        if not times:
            continue
        times_dt = [datetime.fromtimestamp(t/1000.0) for t in times]
        times_sorted = np.sort(times_dt)
        y = np.arange(1, len(times_sorted) + 1)
        ax.plot(times_sorted, y, label=f"{label} (n={len(times)})", linewidth=2)
    ax.set_xlabel("Time", fontsize=12)
    ax.set_ylabel("Cumulative Count", fontsize=12)
    ax.set_title(f"CDFs from {datetime.fromtimestamp(start_time/1000.0)} to {datetime.fromtimestamp(end_time/1000.0)}", fontsize=14, fontweight='bold')
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    fig.autofmt_xdate()
    ax.grid(True, alpha=0.3, which='both')
    ax.legend(fontsize=10, loc='upper left')
    plt.tight_layout()
    os.makedirs(PLOTS_DIR, exist_ok=True)
    output_path = os.path.join(PLOTS_DIR, f'cdf_three_sources.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Plot saved to: {output_path}")
    plt.close()

async def main():
    conn = await get_db_connection()
    if not conn:
        return
    try:
        min_found_time = await get_min_found_time_ms(conn)
        if min_found_time is None:
            print("No found_time_ms in markets_5!")
            return
        om_max = await get_max_time_ms(conn, 'OrdersMatched')
        of_max = await get_max_time_ms(conn, 'OrderFilled')
        end_time = min(om_max, of_max) if om_max and of_max else (om_max or of_max)
        print(f"Using time range: {min_found_time} to {end_time}")
        om_times = await get_event_times(conn, 'OrdersMatched', min_found_time, end_time)
        of_times = await get_event_times(conn, 'OrderFilled', min_found_time, end_time)
        blt_times = await get_buffer_last_trade_times(conn, min_found_time, end_time)
        plot_cdfs_three({
            'OrdersMatched': om_times,
            'OrderFilled': of_times,
            'BufferLastTrade': blt_times
        }, min_found_time, end_time)
    finally:
        await conn.close()
        print("Done.")

if __name__ == "__main__":
    asyncio.run(main())
