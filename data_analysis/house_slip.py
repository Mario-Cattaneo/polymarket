#!/usr/bin/env python3
"""
Analytics script to calculate and plot the cumulative slippage from on-chain settlements.

Slippage is defined as the value lost by a taker for accepting prices worse than the
best price available to them within a single atomic trade filled by multiple makers.

This script generates one plot with three lines:
- Cumulative slippage when makers are buying (taker is selling).
- Cumulative slippage when makers are selling (taker is buying).
- Combined total cumulative slippage.
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
getcontext().prec = 36 # Use high precision for calculations

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

# --- Constants ---
USDC_SCALAR = Decimal("1_000_000")
SHARE_SCALAR = Decimal("1_000_000")
COLLATERAL_ASSET_ID = "0"

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

def calculate_slippage_from_settlements(records: list) -> pd.DataFrame:
    """
    Processes settlement records to calculate slippage for each multi-fill transaction.
    """
    slippage_events = []

    for row in records:
        try:
            trade = json.loads(row['trade'])
            
            # A transaction needs OrdersMatched and multiple OrderFilled to have slippage
            if not trade.get('orders_matched') or len(trade.get('orders_filled', [])) < 2:
                continue

            # 1. Identify the Taker and their goal from OrdersMatched
            orders_matched = trade['orders_matched'][0]
            taker_address = '0x' + orders_matched['topics'][2][-40:]
            
            om_data_hex = bytes.fromhex(orders_matched['data'][2:])
            om_decoded = decode(['uint256', 'uint256', 'uint256', 'uint256'], om_data_hex)
            om_maker_asset, om_taker_asset = str(om_decoded[0]), str(om_decoded[1])

            taker_is_buying_shares = (om_maker_asset != COLLATERAL_ASSET_ID and om_taker_asset == COLLATERAL_ASSET_ID)
            taker_is_selling_shares = (om_taker_asset != COLLATERAL_ASSET_ID and om_maker_asset == COLLATERAL_ASSET_ID)

            if not (taker_is_buying_shares or taker_is_selling_shares):
                continue

            # 2. Isolate the fills against maker orders
            maker_fills = []
            for fill in trade.get('orders_filled', []):
                fill_taker = '0x' + fill['topics'][3][-40:]
                if fill_taker.lower() == taker_address.lower():
                    maker_fills.append(fill)
            
            # Slippage only occurs with 2 or more maker fills
            if len(maker_fills) < 2:
                continue

            # 3. Calculate price and quantity for each maker fill
            fill_details = []
            for fill in maker_fills:
                data_hex = bytes.fromhex(fill['data'][2:])
                decoded = decode(['uint256', 'uint256', 'uint256', 'uint256', 'uint256'], data_hex)
                maker_asset, taker_asset = str(decoded[0]), str(decoded[1])
                making, taking = Decimal(decoded[2]), Decimal(decoded[3])
                
                price, quantity = Decimal(0), Decimal(0)
                
                # Case A: Maker is selling shares (giving shares, taking USDC)
                if taker_asset == COLLATERAL_ASSET_ID:
                    price = (taking / USDC_SCALAR) / (making / SHARE_SCALAR)
                    quantity = making / SHARE_SCALAR
                # Case B: Maker is buying shares (giving USDC, taking shares)
                elif maker_asset == COLLATERAL_ASSET_ID:
                    price = (making / USDC_SCALAR) / (taking / SHARE_SCALAR)
                    quantity = taking / SHARE_SCALAR
                
                if price > 0 and quantity > 0:
                    fill_details.append({'price': price, 'quantity': quantity})

            if not fill_details:
                continue

            # 4. Find best price and calculate total slippage for the transaction
            prices = [d['price'] for d in fill_details]
            tx_slippage = Decimal(0)
            
            if taker_is_buying_shares: # Taker wants lowest price
                best_price = min(prices)
                for detail in fill_details:
                    tx_slippage += (detail['price'] - best_price) * detail['quantity']
                slippage_type = 'maker_selling' # Makers were selling to the taker
            
            else: # Taker is selling shares, wants highest price
                best_price = max(prices)
                for detail in fill_details:
                    tx_slippage += (best_price - detail['price']) * detail['quantity']
                slippage_type = 'maker_buying' # Makers were buying from the taker

            if tx_slippage > 0:
                slippage_events.append({
                    'timestamp': pd.to_datetime(block_to_timestamp(row['block_number']), unit='s'),
                    'maker_buying_slippage': tx_slippage if slippage_type == 'maker_buying' else Decimal(0),
                    'maker_selling_slippage': tx_slippage if slippage_type == 'maker_selling' else Decimal(0)
                })

        except Exception as e:
            logger.warning(f"Skipping settlement row {row['transaction_hash']} due to slippage calc error: {e}")
            continue
            
    return pd.DataFrame(slippage_events)

# ----------------------------------------------------------------
# PLOTTING FUNCTION
# ----------------------------------------------------------------

def plot_cumulative_slippage(df: pd.DataFrame):
    if df.empty:
        logger.warning("No slippage data to plot.")
        return

    df.sort_values('timestamp', inplace=True)
    df['cum_maker_buying'] = df['maker_buying_slippage'].cumsum()
    df['cum_maker_selling'] = df['maker_selling_slippage'].cumsum()
    df['cum_combined'] = df['cum_maker_buying'] + df['cum_maker_selling']

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(16, 9))

    def get_legend_label(base_label: str, series: pd.Series) -> str:
        if series.empty: return f"{base_label} (Total: $0.00)"
        total = series.iloc[-1]
        return f"{base_label} (Total: ${total:,.2f})"

    ax.plot(df['timestamp'], df['cum_maker_buying'], color='crimson', linestyle='-', linewidth=2, label=get_legend_label('Maker Buying Slippage', df['cum_maker_buying']))
    ax.plot(df['timestamp'], df['cum_maker_selling'], color='royalblue', linestyle='-', linewidth=2, label=get_legend_label('Maker Selling Slippage', df['cum_maker_selling']))
    ax.plot(df['timestamp'], df['cum_combined'], color='black', linestyle='--', linewidth=2.5, label=get_legend_label('Combined Slippage', df['cum_combined']))

    ax.set_title('Cumulative Slippage in On-Chain Settlements', fontsize=18, pad=20)
    ax.set_xlabel('Date', fontsize=14)
    ax.set_ylabel('Cumulative Slippage (in USDC)', fontsize=14)
    ax.legend(loc='best', fontsize='large')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    fig.autofmt_xdate()
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout()

    filename = 'cumulative_slippage.png'
    plt.savefig(filename, dpi=150)
    logger.info(f"✅ Slippage plot saved to {filename}")
    plt.close()

# ----------------------------------------------------------------
# MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    logger.info("Fetching all settlement data for slippage analysis...")
    settlement_records = await fetch_data("SELECT block_number, transaction_hash, trade FROM settlements_house")
    
    logger.info("Calculating slippage from settlement data...")
    df_slippage = calculate_slippage_from_settlements(settlement_records)
    
    logger.info("Generating cumulative slippage plot...")
    plot_cumulative_slippage(df_slippage)

if __name__ == "__main__":
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST, PG_PORT]):
        logger.error("One or more required database environment variables are not set.")
    else:
        asyncio.run(main())