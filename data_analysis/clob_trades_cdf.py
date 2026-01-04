#!/usr/bin/env python3
"""
Script to analyze and plot the CDF of server_time_ms from buffer_last_trade_prices_2.
This version is optimized for speed by sampling from the entire table
without a time-based WHERE clause, using the fastest possible method.
"""

import os
import asyncio
import asyncpg
import logging
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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

BUFFER_TABLE = "buffer_last_trade_prices_2"
SAMPLE_SIZE = 100000  # Total sample size for the CDF
PLOTS_DIR = os.getenv('POLY_PLOTS', './plots')

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

async def sample_buffer_last_trade_times(conn):
    """
    Sample server_time_ms using a single, fastest possible TABLESAMPLE SYSTEM query
    without any WHERE clause.
    """
    logger.info(f"Sampling {SAMPLE_SIZE} times from the entire {BUFFER_TABLE} table...")
    
    # The fastest possible sampling query: TABLESAMPLE + LIMIT
    query = f"""
        SELECT server_time_ms
        FROM {BUFFER_TABLE} TABLESAMPLE SYSTEM (1)
        LIMIT $1
    """
    try:
        records = await conn.fetch(query, SAMPLE_SIZE)
        times = [record['server_time_ms'] for record in records]
        logger.info(f"Fetched and sampled {len(times)} server_time_ms records.")
        return times
    except Exception as e:
        logger.error(f"Error sampling {BUFFER_TABLE}: {e}")
        return []

def plot_cdf(times):
    """Plot the CDF of the sampled server_time_ms."""
    logger.info("Generating CDF plot...")
    
    if not times:
        logger.warning("No data to plot.")
        return

    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Convert timestamps (ms) to datetime objects
    times_dt = [datetime.fromtimestamp(t / 1000.0) for t in times]
    
    # Sort the times for CDF calculation
    times_sorted = np.sort(times_dt)
    
    # Y-axis is the cumulative count
    y = np.arange(1, len(times_sorted) + 1)
    
    ax.plot(times_sorted, y, label=f"{BUFFER_TABLE} (n={len(times):,})", 
            color="purple", linewidth=2, alpha=0.8)
    
    ax.set_xlabel("Time", fontsize=12)
    ax.set_ylabel("Cumulative Count", fontsize=12)
    
    # Determine the time range from the sampled data for the title
    min_dt = times_sorted[0]
    max_dt = times_sorted[-1]
    
    ax.set_title(f"CDF of {BUFFER_TABLE} Server Times (Fastest Sample)\nTime Range of Sample: {min_dt} to {max_dt}", 
                 fontsize=14, fontweight='bold')
    
    # Format x-axis as dates
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    fig.autofmt_xdate()
    
    ax.grid(True, alpha=0.3, which='both')
    ax.legend(fontsize=10, loc='upper left')
    
    plt.tight_layout()
    
    # Save plot
    os.makedirs(PLOTS_DIR, exist_ok=True)
    output_path = os.path.join(PLOTS_DIR, 'buffer_last_trade_time_cdf_fastest.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    logger.info(f"✅ Plot saved to: {output_path}")
    
    plt.close()

async def main():
    conn = await get_db_connection()
    if not conn:
        return
    
    try:
        # 1. Sample the times using the fastest TABLESAMPLE method
        sampled_times = await sample_buffer_last_trade_times(conn)

        # 2. Plot the CDF
        plot_cdf(sampled_times)

    finally:
        if conn:
            await conn.close()
            logger.info("Database connection closed.")
        logger.info("Script finished.")

if __name__ == "__main__":
    asyncio.run(main())