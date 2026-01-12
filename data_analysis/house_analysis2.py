#!/usr/bin/env python3
"""
Analytics script to plot the cumulative volume of bought and sold shares for specific market outcomes.
This version uses a robust method to calculate cumulative volumes, ensuring logical consistency.

For each of the four target asset IDs, this script generates a plot with six lines:
- On-Chain Settlements: Cumulative Buys, Sells, and Combined Volume.
- Off-Chain Last Trades: Cumulative Buys, Sells, and Combined Volume (dashed lines).
"""

import os
import asyncio
import asyncpg
import logging
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from decimal import Decimal, getcontext
from eth_abi import decode

# ----------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
getcontext().prec = 28 # Set precision for Decimal

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

# --- Target Market & Asset Configuration ---
SHARE_SCALAR = Decimal("1_000_000")

TARGET_ASSETS = {
    "83247781037352156539108067944461291821683755894607244160607042790356561625563": "Democratic Party Control - Yes",
    "33156410999665902694791064431724433042010245771106314074312009703157423879038": "Democratic Party Control - No",
    "65139230827417363158752884968303867495725894165574887635816574090175320800482": "Republican Party Control - Yes",
    "17371217118862125782438074585166210555214661810823929795910191856905580975576": "Republican Party Control - No",
}

# ----------------------------------------------------------------
# HELPER & DATA PROCESSING FUNCTIONS
# ----------------------------------------------------------------

def block_to_timestamp(block_number: int) -> int:
    return ((block_number - REF_BLOCK_NUMBER) * SECONDS_PER_BLOCK) + REF_UNIX_TIMESTAMP

async def fetch_data(query: str) -> list:
    conn = None
    try:
        conn = await asyncpg.connect(host=PG_HOST, port=int(PG_PORT), database=DB_NAME, user=DB_USER, password=DB_PASS)
        logger.info(f"Executing query...")
        records = await conn.fetch(query)
        logger.info(f"Fetched {len(records)} records.")
        return records
    except Exception as e:
        logger.error(f"Database error: {e}")
        return []
    finally:
        if conn:
            await conn.close()

def process_settlements_data(records: list) -> pd.DataFrame:
    processed_trades = []
    for row in records:
        try:
            trade_data = json.loads(row['trade'])
            timestamp = pd.to_datetime(block_to_timestamp(row['block_number']), unit='s')
            for matched_order in trade_data.get('orders_matched', []):
                data_hex = bytes.fromhex(matched_order['data'][2:])
                decoded_data = decode(['uint256', 'uint256', 'uint256', 'uint256'], data_hex)
                maker_asset_id, taker_asset_id = str(decoded_data[0]), str(decoded_data[1])
                maker_amount, taker_amount = Decimal(decoded_data[2]), Decimal(decoded_data[3])
                if taker_asset_id in TARGET_ASSETS:
                    processed_trades.append({'timestamp': timestamp, 'asset_id': taker_asset_id, 'side': 'sell', 'amount': taker_amount / SHARE_SCALAR})
                if maker_asset_id in TARGET_ASSETS:
                    processed_trades.append({'timestamp': timestamp, 'asset_id': maker_asset_id, 'side': 'buy', 'amount': maker_amount / SHARE_SCALAR})
        except Exception as e:
            logger.warning(f"Skipping settlement row due to parsing error: {e}")
    return pd.DataFrame(processed_trades)

def process_last_trades_data(records: list) -> pd.DataFrame:
    processed_trades = []
    for row in records:
        try:
            msg = json.loads(row['message'])
            side = msg.get('side', '').lower()
            if side in ['buy', 'sell']:
                processed_trades.append({'timestamp': pd.to_datetime(row['server_time_us'], unit='us'), 'asset_id': row['asset_id'], 'side': side, 'amount': Decimal(msg.get('size', 0))})
        except Exception as e:
            logger.warning(f"Skipping last_trade row due to parsing error: {e}")
    return pd.DataFrame(processed_trades)

def prepare_plot_data(df: pd.DataFrame, asset_id: str) -> dict:
    """
    CORRECTED: Calculates cumulative volumes on a unified timeline to prevent logical errors.
    """
    if df.empty:
        return {}
    asset_df = df[df['asset_id'] == asset_id].sort_values('timestamp').copy()
    if asset_df.empty:
        return {}

    # Create separate columns for buy and sell amounts
    asset_df['buy_amount'] = asset_df.apply(lambda row: row['amount'] if row['side'] == 'buy' else 0, axis=1)
    asset_df['sell_amount'] = asset_df.apply(lambda row: row['amount'] if row['side'] == 'sell' else 0, axis=1)

    # Calculate cumulative sums on the unified dataframe
    asset_df['cumulative_buys'] = asset_df['buy_amount'].cumsum()
    asset_df['cumulative_sells'] = asset_df['sell_amount'].cumsum()
    asset_df['cumulative_combined'] = asset_df['amount'].cumsum()
    
    return asset_df

# ----------------------------------------------------------------
# PLOTTING FUNCTION
# ----------------------------------------------------------------

def plot_volume_graphs(asset_id: str, asset_name: str, settlements_df: pd.DataFrame, trades_df: pd.DataFrame):
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(16, 9))

    def get_legend_label(base_label: str, series: pd.Series) -> str:
        if series.empty:
            return f"{base_label} (Total: 0.00)"
        total = series.iloc[-1]
        return f"{base_label} (Total: {total:,.2f})"

    # --- Plotting Settlements Data (Solid Lines) ---
    if not settlements_df.empty:
        ax.plot(settlements_df['timestamp'], settlements_df['cumulative_buys'], color='green', linestyle='-', linewidth=1.5, zorder=5, label=get_legend_label('Settlements - Buys', settlements_df['cumulative_buys']))
        ax.plot(settlements_df['timestamp'], settlements_df['cumulative_sells'], color='red', linestyle='-', linewidth=1.5, zorder=5, label=get_legend_label('Settlements - Sells', settlements_df['cumulative_sells']))
        ax.plot(settlements_df['timestamp'], settlements_df['cumulative_combined'], color='black', linestyle='-', linewidth=2.0, zorder=4, label=get_legend_label('Settlements - Combined', settlements_df['cumulative_combined']))

    # --- Plotting Last Trades Data (Dashed Lines) ---
    if not trades_df.empty:
        ax.plot(trades_df['timestamp'], trades_df['cumulative_buys'], color='cyan', linestyle='--', linewidth=2.0, zorder=10, label=get_legend_label('Last Trades - Buys', trades_df['cumulative_buys']))
        ax.plot(trades_df['timestamp'], trades_df['cumulative_sells'], color='magenta', linestyle='--', linewidth=2.0, zorder=10, label=get_legend_label('Last Trades - Sells', trades_df['cumulative_sells']))
        ax.plot(trades_df['timestamp'], trades_df['cumulative_combined'], color='orange', linestyle='--', linewidth=2.5, zorder=9, label=get_legend_label('Last Trades - Combined', trades_df['cumulative_combined']))

    # --- Formatting ---
    ax.set_title(f'Cumulative Share Volume: {asset_name}', fontsize=18, pad=20)
    ax.set_xlabel('Date', fontsize=14)
    ax.set_ylabel('Cumulative Number of Shares', fontsize=14)
    ax.legend(loc='best', fontsize='large')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    fig.autofmt_xdate()
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout()

    filename = f'cumulative_volume_{asset_id[-10:]}.png'
    plt.savefig(filename, dpi=150)
    logger.info(f"✅ Plot saved to {filename}")
    plt.close()

# ----------------------------------------------------------------
# MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    settlement_records = await fetch_data("SELECT block_number, trade FROM settlements_house")
    last_trade_records = await fetch_data("SELECT asset_id, server_time_us, message FROM poly_last_trade_price_house")

    df_settlements = process_settlements_data(settlement_records)
    
    max_settlement_timestamp = pd.Timestamp.min
    if not df_settlements.empty:
        max_settlement_timestamp = df_settlements['timestamp'].max()
        logger.info(f"Time range will be synchronized to the last settlement event: {max_settlement_timestamp}")
    else:
        logger.warning("No settlement data found. The time range for last trades will not be truncated.")

    df_last_trades = process_last_trades_data(last_trade_records)
    if not df_last_trades.empty and max_settlement_timestamp > pd.Timestamp.min:
        df_last_trades = df_last_trades[df_last_trades['timestamp'] <= max_settlement_timestamp].copy()

    for asset_id, asset_name in TARGET_ASSETS.items():
        logger.info(f"\n--- Generating plot for: {asset_name} ---")
        
        settlements_plot_data = prepare_plot_data(df_settlements, asset_id)
        trades_plot_data = prepare_plot_data(df_last_trades, asset_id)
        
        plot_volume_graphs(asset_id, asset_name, settlements_plot_data, trades_plot_data)

if __name__ == "__main__":
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST, PG_PORT]):
        logger.error("One or more required database environment variables are not set.")
    else:
        asyncio.run(main())