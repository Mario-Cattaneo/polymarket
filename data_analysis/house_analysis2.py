#!/usr/bin/env python3
"""
Analytics script to plot the cumulative volume of bought and sold shares for specific market outcomes.
This version corrects the unit handling for off-chain trades and adds total counts to the legend.

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
# The amounts from ON-CHAIN settlements are divided by this scalar.
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
    """
    Processes raw settlement records to extract trade volumes for target assets.
    The logic centers on the OrdersMatched event, which summarizes the taker's action.
    """
    processed_trades = []
    for row in records:
        try:
            trade_data = json.loads(row['trade'])
            timestamp = pd.to_datetime(block_to_timestamp(row['block_number']), unit='s')
            
            for matched_order in trade_data.get('orders_matched', []):
                data_hex = bytes.fromhex(matched_order['data'][2:])
                decoded_data = decode(['uint256', 'uint256', 'uint256', 'uint256'], data_hex)
                maker_asset_id = str(decoded_data[0])
                taker_asset_id = str(decoded_data[1])
                maker_amount_filled = Decimal(decoded_data[2])
                taker_amount_filled = Decimal(decoded_data[3])

                if taker_asset_id in TARGET_ASSETS:
                    processed_trades.append({
                        'timestamp': timestamp, 'asset_id': taker_asset_id, 'side': 'sell',
                        'amount': taker_amount_filled / SHARE_SCALAR
                    })
                
                if maker_asset_id in TARGET_ASSETS:
                    processed_trades.append({
                        'timestamp': timestamp, 'asset_id': maker_asset_id, 'side': 'buy',
                        'amount': maker_amount_filled / SHARE_SCALAR
                    })
        except (json.JSONDecodeError, KeyError, IndexError, Exception) as e:
            logger.warning(f"Skipping settlement row due to parsing error: {e}")
            continue
            
    return pd.DataFrame(processed_trades)

def process_last_trades_data(records: list) -> pd.DataFrame:
    """Processes raw last_trade records to extract trade volumes."""
    processed_trades = []
    for row in records:
        try:
            msg = json.loads(row['message'])
            side = msg.get('side', '').lower()
            if side in ['buy', 'sell']:
                # CORRECTED: The 'size' from last_trade is already in the correct unit.
                # Do NOT divide by SHARE_SCALAR.
                amount = Decimal(msg.get('size', 0))
                processed_trades.append({
                    'timestamp': pd.to_datetime(row['server_time_us'], unit='us'),
                    'asset_id': row['asset_id'],
                    'side': side,
                    'amount': amount
                })
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Skipping last_trade row due to parsing error: {e}")
            continue
            
    return pd.DataFrame(processed_trades)

def prepare_plot_data(df: pd.DataFrame, asset_id: str) -> dict:
    """Filters data for an asset and calculates cumulative buys, sells, and combined volume."""
    if df.empty:
        return {'buy': pd.DataFrame(), 'sell': pd.DataFrame(), 'combined': pd.DataFrame()}

    asset_df = df[df['asset_id'] == asset_id].copy()
    if asset_df.empty:
        return {'buy': pd.DataFrame(), 'sell': pd.DataFrame(), 'combined': pd.DataFrame()}
        
    buys = asset_df[asset_df['side'] == 'buy'].sort_values('timestamp')
    buys['cumulative'] = buys['amount'].cumsum()
    
    sells = asset_df[asset_df['side'] == 'sell'].sort_values('timestamp')
    sells['cumulative'] = sells['amount'].cumsum()
    
    combined = asset_df.sort_values('timestamp')
    combined['cumulative'] = combined['amount'].cumsum()
    
    return {'buy': buys, 'sell': sells, 'combined': combined}

# ----------------------------------------------------------------
# PLOTTING FUNCTION
# ----------------------------------------------------------------

def plot_volume_graphs(asset_id: str, asset_name: str, settlements_data: dict, trades_data: dict):
    """Generates and saves a plot for a single asset ID with totals in the legend."""
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(16, 9))

    def get_legend_label(base_label: str, data: pd.DataFrame) -> str:
        if data.empty:
            return f"{base_label} (Total: 0.00)"
        total = data['cumulative'].iloc[-1]
        return f"{base_label} (Total: {total:,.2f})"

    # --- Plotting Settlements Data (Solid Lines) ---
    ax.plot(settlements_data['buy']['timestamp'], settlements_data['buy']['cumulative'], color='green', linestyle='-', linewidth=1.5, zorder=5, label=get_legend_label('Settlements - Buys', settlements_data['buy']))
    ax.plot(settlements_data['sell']['timestamp'], settlements_data['sell']['cumulative'], color='red', linestyle='-', linewidth=1.5, zorder=5, label=get_legend_label('Settlements - Sells', settlements_data['sell']))
    ax.plot(settlements_data['combined']['timestamp'], settlements_data['combined']['cumulative'], color='black', linestyle='-', linewidth=2.0, zorder=4, label=get_legend_label('Settlements - Combined', settlements_data['combined']))

    # --- Plotting Last Trades Data (Dashed Lines) ---
    ax.plot(trades_data['buy']['timestamp'], trades_data['buy']['cumulative'], color='cyan', linestyle='--', linewidth=2.0, zorder=10, label=get_legend_label('Last Trades - Buys', trades_data['buy']))
    ax.plot(trades_data['sell']['timestamp'], trades_data['sell']['cumulative'], color='magenta', linestyle='--', linewidth=2.0, zorder=10, label=get_legend_label('Last Trades - Sells', trades_data['sell']))
    ax.plot(trades_data['combined']['timestamp'], trades_data['combined']['cumulative'], color='orange', linestyle='--', linewidth=2.5, zorder=9, label=get_legend_label('Last Trades - Combined', trades_data['combined']))

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
    df_last_trades = process_last_trades_data(last_trade_records)

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