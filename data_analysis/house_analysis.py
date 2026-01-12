#!/usr/bin/env python3
"""
Analytics script to plot the cumulative growth of various market data points
over time on a single, highly readable figure with distinct patterns and totals in the legend.

This script generates one plot containing six lines:
1. Cumulative count of last trade prices.
2. Cumulative count of all matched settlements.
3. Cumulative count of 'complementary' type settlements.
4. Cumulative count of 'split' type settlements.
5. Cumulative count of 'merge' type settlements.
6. Cumulative count of CFT events without exchange interaction.
"""

import os
import asyncio
import asyncpg
import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ----------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

# --- Database Credentials ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- Timestamp Interpolation Constants ---
REF_BLOCK_NUMBER = 81372608
REF_UNIX_TIMESTAMP = 1767864474
SECONDS_PER_BLOCK = 2

# ----------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------

def block_to_timestamp(block_number: int) -> int:
    """
    Linearly interpolates a Unix timestamp from a block number.
    """
    return ((block_number - REF_BLOCK_NUMBER) * SECONDS_PER_BLOCK) + REF_UNIX_TIMESTAMP

async def fetch_data(query: str) -> list:
    """
    Connects to the database and executes a given query.
    """
    conn = None
    try:
        conn = await asyncpg.connect(
            host=PG_HOST, port=int(PG_PORT), database=DB_NAME, user=DB_USER, password=DB_PASS
        )
        logger.info(f"Executing query: {query[:100]}...")
        records = await conn.fetch(query)
        logger.info(f"Fetched {len(records)} records.")
        return records
    except Exception as e:
        logger.error(f"Database error: {e}")
        return []
    finally:
        if conn:
            await conn.close()

def plot_all_on_one_figure(dataframes: dict, title: str, ylabel: str, filename: str):
    """
    Generates and saves a multi-line cumulative plot with distinct styles and totals in the legend.
    """
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(16, 9))

    # Define a high-contrast color palette and specific styles with unique patterns
    color_palette = plt.get_cmap('tab10').colors
    styles = {
        'Last Trade Price':        {'color': color_palette[0], 'linestyle': '--', 'linewidth': 2.0, 'zorder': 10},
        'All Settlements':         {'color': color_palette[1], 'linestyle': '-',  'linewidth': 2.0, 'zorder': 5},
        'Complementary Settlements': {'color': color_palette[2], 'linestyle': ':',  'linewidth': 2.0, 'zorder': 4}, # Dotted
        'Split Settlements':       {'color': color_palette[3], 'linestyle': '-.', 'linewidth': 2.0, 'zorder': 3}, # Dash-dot
        'Merge Settlements':       {'color': color_palette[4], 'linestyle': (0, (3, 5, 1, 5)), 'linewidth': 2.0, 'zorder': 2}, # Custom dash
        'CFT No Exchange':         {'color': color_palette[5], 'linestyle': (0, (5, 10)), 'linewidth': 2.0, 'zorder': 1}, # Custom dash
    }

    # Plot each dataframe on the same axes
    for label, df in dataframes.items():
        if not df.empty:
            style = styles.get(label, {})
            # Get the final count for the legend
            total_count = df['cumulative_count'].iloc[-1]
            legend_label = f"{label} (Total: {total_count})"
            
            ax.plot(df['timestamp'], df['cumulative_count'], label=legend_label, **style)
        else:
            logger.warning(f"No data for '{label}', it will not be plotted.")

    # Formatting the plot
    ax.set_title(title, fontsize=18, pad=20)
    ax.set_xlabel("Date", fontsize=14)
    ax.set_ylabel(ylabel, fontsize=14)
    ax.legend(loc='best', fontsize='large') # Let matplotlib decide the best location
    
    # Format x-axis to display dates nicely
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    fig.autofmt_xdate()
    
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)

    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout()

    # Save the figure
    plt.savefig(filename, dpi=150) # Higher DPI for better quality
    logger.info(f"✅ Final combined plot saved to {filename}")
    plt.close()

# ----------------------------------------------------------------
# MAIN LOGIC
# ----------------------------------------------------------------

async def main():
    """
    Main function to orchestrate data fetching and plotting.
    """
    all_dataframes = {}
    max_event_timestamp = pd.Timestamp.min

    # --- Fetch and Process On-Chain Event Data First ---
    logger.info("\n--- Processing All On-Chain Events ---")
    all_settlements_records = await fetch_data("SELECT block_number, type FROM settlements_house")
    cft_records = await fetch_data("SELECT block_number FROM cft_no_exchange_house")

    if all_settlements_records:
        df_all_events = pd.DataFrame(all_settlements_records, columns=['block_number', 'type'])
        df_all_events['unix_ts'] = df_all_events['block_number'].apply(block_to_timestamp)
        df_all_events['timestamp'] = pd.to_datetime(df_all_events['unix_ts'], unit='s')
        df_all_events.sort_values('timestamp', inplace=True)

        # 1. All Settlements
        df_all_settlements = df_all_events.copy()
        df_all_settlements['cumulative_count'] = range(1, len(df_all_settlements) + 1)
        all_dataframes['All Settlements'] = df_all_settlements
        if not df_all_settlements.empty:
            max_event_timestamp = max(max_event_timestamp, df_all_settlements['timestamp'].max())

        # 2. Settlements by Type
        for settlement_type in ['complementary', 'split', 'merge']:
            df_subset = df_all_events[df_all_events['type'] == settlement_type].copy()
            if not df_subset.empty:
                df_subset['cumulative_count'] = range(1, len(df_subset) + 1)
                label = f"{settlement_type.capitalize()} Settlements"
                all_dataframes[label] = df_subset

    if cft_records:
        df_cft = pd.DataFrame(cft_records, columns=['block_number'])
        df_cft['unix_ts'] = df_cft['block_number'].apply(block_to_timestamp)
        df_cft['timestamp'] = pd.to_datetime(df_cft['unix_ts'], unit='s')
        df_cft.sort_values('timestamp', inplace=True)
        df_cft['cumulative_count'] = range(1, len(df_cft) + 1)
        all_dataframes['CFT No Exchange'] = df_cft
        if not df_cft.empty:
            max_event_timestamp = max(max_event_timestamp, df_cft['timestamp'].max())

    # --- Fetch, Truncate, and Process Last Trade Price Data ---
    logger.info("\n--- Processing Last Trade Price ---")
    trade_price_records = await fetch_data("SELECT server_time_us FROM poly_last_trade_price_house")
    if trade_price_records:
        df_trades = pd.DataFrame(trade_price_records, columns=['server_time_us'])
        df_trades['timestamp'] = pd.to_datetime(df_trades['server_time_us'], unit='us')
        
        if max_event_timestamp > pd.Timestamp.min:
            logger.info(f"Truncating Last Trade Price data to event end time: {max_event_timestamp}")
            df_trades = df_trades[df_trades['timestamp'] <= max_event_timestamp].copy()
        
        df_trades.sort_values('timestamp', inplace=True)
        df_trades['cumulative_count'] = range(1, len(df_trades) + 1)
        all_dataframes['Last Trade Price'] = df_trades

    # --- Generate the combined plot ---
    logger.info("\n--- Generating Final Combined Plot ---")
    if all_dataframes:
        plot_all_on_one_figure(
            all_dataframes,
            'Cumulative Event Counts Over Time',
            'Cumulative Count of Rows',
            'cumulative_analytics_final.png'
        )
    else:
        logger.warning("No data was fetched. No plot will be generated.")

if __name__ == "__main__":
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST, PG_PORT]):
        logger.error("One or more required database environment variables are not set.")
    else:
        asyncio.run(main())