#!/usr/bin/env python3
"""
Analytics script to analyze trading volume by match type and taker intention.

Generates two plots:
1. Cumulative trading volume by match type (SPLIT, MERGE, COMPLEMENTARY)
2. Cumulative trading volume by taker intention (Democrats, Republicans)
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
getcontext().prec = 36

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

# --- Taker Intention Asset IDs ---
DEMOCRATS_YES = 83247781037352156539108067944461291821683755894607244160607042790356561625563
DEMOCRATS_NO = 33156410999665902694791064431724433042010245771106314074312009703157423879038
REPUBLICANS_YES = 65139230827417363158752884968303867495725894165574887635816574090175320800482
REPUBLICANS_NO = 17371217118862125782438074585166210555214661810823929795910191856905580975576

DEMOCRATS_ASSETS = {DEMOCRATS_YES, DEMOCRATS_NO}
REPUBLICANS_ASSETS = {REPUBLICANS_YES, REPUBLICANS_NO}

# ----------------------------------------------------------------
# HELPER & DATA PROCESSING FUNCTIONS
# ----------------------------------------------------------------

def block_to_timestamp(block_number: int) -> int:
    return ((block_number - REF_BLOCK_NUMBER) * SECONDS_PER_BLOCK) + REF_UNIX_TIMESTAMP

def determine_taker_intention(asset_id: int, taker_is_buying: bool) -> str:
    """Determine taker's intention based on which asset they're trading and their direction."""
    if asset_id in DEMOCRATS_ASSETS:
        return "Democrats" if taker_is_buying else "Republicans"
    elif asset_id in REPUBLICANS_ASSETS:
        return "Republicans" if taker_is_buying else "Democrats"
    else:
        return "Unknown"

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

def decode_order_filled(data: str, topics: list) -> dict:
    """Decode OrderFilled event."""
    try:
        data_hex = bytes.fromhex(data[2:] if data.startswith('0x') else data)
        decoded = decode(['uint256', 'uint256', 'uint256', 'uint256', 'uint256'], data_hex)
        
        maker = '0x' + topics[2][-40:] if len(topics) > 2 else None
        taker = '0x' + topics[3][-40:] if len(topics) > 3 else None
        maker_asset_id = int(decoded[0])
        taker_asset_id = int(decoded[1])
        making = int(decoded[2])
        taking = int(decoded[3])
        fee = int(decoded[4])
        
        return {
            'maker': maker,
            'taker': taker,
            'makerAssetId': maker_asset_id,
            'takerAssetId': taker_asset_id,
            'making': making,
            'taking': taking,
            'fee': fee
        }
    except Exception as e:
        logger.warning(f"Failed to decode OrderFilled: {e}")
        return {}

def calculate_volume_metrics(records: list) -> tuple:
    """
    Process settlement records to calculate trading volumes.
    Returns two dataframes: one for volume by match type, one for volume by intention.
    """
    volume_by_match = []
    volume_by_intention = []
    computation_count = 0
    EXCHANGE_ADDRESS = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

    for row in records:
        try:
            trade = json.loads(row['trade'])
            
            if not trade.get('orders_filled') or len(trade.get('orders_filled', [])) < 2:
                continue
            
            computation_count += 1
            
            logger.info(f"[Settlement #{computation_count}] Hash: {row['transaction_hash']}")

            # Decode all OrderFilled events
            all_fills = []
            exchange_fill = None
            for idx, fill in enumerate(trade.get('orders_filled', [])):
                decoded_fill = decode_order_filled(fill['data'], fill['topics'])
                if decoded_fill:
                    is_exchange_fill = (decoded_fill['taker'].lower() == EXCHANGE_ADDRESS.lower())
                    decoded_fill['is_exchange'] = is_exchange_fill
                    all_fills.append(decoded_fill)
                    
                    if is_exchange_fill:
                        exchange_fill = decoded_fill

            # Determine taker direction from exchange fill
            if not exchange_fill:
                logger.info(f"  Skipping: No exchange fill found")
                continue
            
            if exchange_fill['makerAssetId'] == 0:
                taker_is_buying = True
            else:
                taker_is_buying = False

            # Get USDC volume from exchange fill
            exchange_making = Decimal(exchange_fill['making'])
            exchange_taking = Decimal(exchange_fill['taking'])
            exchange_usdc_volume = min(exchange_making, exchange_taking) / USDC_SCALAR
            
            logger.info(f"  Exchange fill: makerAssetId={exchange_fill['makerAssetId']}, takerAssetId={exchange_fill['takerAssetId']}, making={exchange_making}, taking={exchange_taking}, USDC Volume: ${exchange_usdc_volume}")
            # Determine taker intention
            non_zero_asset_id = exchange_fill['makerAssetId'] if exchange_fill['takerAssetId'] == 0 else exchange_fill['takerAssetId']
            taker_intention = determine_taker_intention(non_zero_asset_id, taker_is_buying)
            
            logger.info(f"  Taker direction: {'BUYING' if taker_is_buying else 'SELLING'}, Intention: {taker_intention}, USDC Volume: ${exchange_usdc_volume}")

            # Record volume by intention
            volume_by_intention.append({
                'timestamp': pd.to_datetime(block_to_timestamp(row['block_number']), unit='s'),
                'democrats_volume': exchange_usdc_volume if taker_intention == "Democrats" else Decimal(0),
                'republicans_volume': exchange_usdc_volume if taker_intention == "Republicans" else Decimal(0)
            })

            # Process maker fills to calculate volume by match type
            maker_fills = [f for f in all_fills if not f['is_exchange']]
            
            for fill in maker_fills:
                # Determine maker direction
                maker_is_buying = (fill['makerAssetId'] == 0)
                
                # Determine match type
                if maker_is_buying == taker_is_buying:
                    if taker_is_buying:
                        match_type = "SPLIT"
                    else:
                        match_type = "MERGE"
                else:
                    match_type = "COMPLEMENTARY"
                
                # Calculate USDC volume for this maker fill
                # For MERGE or SPLIT: use max(making, taking)
                # For COMPLEMENTARY: use min(making, taking)
                maker_making = Decimal(fill['making'])
                maker_taking = Decimal(fill['taking'])
                
                if match_type == "COMPLEMENTARY":
                    maker_usdc_volume = min(maker_making, maker_taking) / USDC_SCALAR
                else:  # SPLIT or MERGE
                    maker_usdc_volume = max(maker_making, maker_taking) / USDC_SCALAR
                
                logger.info(f"    Maker fill: {match_type}, makerAssetId={fill['makerAssetId']}, takerAssetId={fill['takerAssetId']}, making={maker_making}, taking={maker_taking}, USDC Volume: ${maker_usdc_volume}")
                
                volume_by_match.append({
                    'timestamp': pd.to_datetime(block_to_timestamp(row['block_number']), unit='s'),
                    'split_volume': maker_usdc_volume if match_type == "SPLIT" else Decimal(0),
                    'merge_volume': maker_usdc_volume if match_type == "MERGE" else Decimal(0),
                    'complementary_volume': maker_usdc_volume if match_type == "COMPLEMENTARY" else Decimal(0)
                })

        except Exception as e:
            logger.warning(f"Skipping settlement {row['transaction_hash']} due to error: {e}")
            continue

    return pd.DataFrame(volume_by_match), pd.DataFrame(volume_by_intention)

# ----------------------------------------------------------------
# PLOTTING FUNCTIONS
# ----------------------------------------------------------------

def plot_volume_by_match_type(df: pd.DataFrame):
    if df.empty:
        logger.warning("No volume data by match type to plot.")
        return

    df.sort_values('timestamp', inplace=True)
    df['cum_split'] = df['split_volume'].cumsum()
    df['cum_merge'] = df['merge_volume'].cumsum()
    df['cum_complementary'] = df['complementary_volume'].cumsum()
    df['cum_total'] = df['cum_split'] + df['cum_merge'] + df['cum_complementary']

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(16, 9))

    def get_legend_label(base_label: str, series: pd.Series) -> str:
        if series.empty: return f"{base_label} (Total: $0.00)"
        total = series.iloc[-1]
        return f"{base_label} (Total: ${total:,.0f})"

    ax.plot(df['timestamp'], df['cum_split'], color='steelblue', linestyle='-', linewidth=2, label=get_legend_label('SPLIT', df['cum_split']))
    ax.plot(df['timestamp'], df['cum_merge'], color='seagreen', linestyle='-', linewidth=2, label=get_legend_label('MERGE', df['cum_merge']))
    ax.plot(df['timestamp'], df['cum_complementary'], color='orange', linestyle='-', linewidth=2, label=get_legend_label('COMPLEMENTARY', df['cum_complementary']))
    ax.plot(df['timestamp'], df['cum_total'], color='black', linestyle='--', linewidth=2.5, label=get_legend_label('Total', df['cum_total']))

    ax.set_title('Cumulative USDC Trading Inflow From Takers and Makers by Matchtype', fontsize=18, pad=20)
    ax.set_xlabel('Date', fontsize=14)
    ax.set_ylabel('Cumulative Volume (in USDC)', fontsize=14)
    ax.legend(loc='best', fontsize='large')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    fig.autofmt_xdate()
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout()

    filename = 'volume_by_match_type.png'
    plt.savefig(filename, dpi=150)
    logger.info(f"✅ Volume by match type plot saved to {filename}")
    plt.close()

def plot_volume_by_intention(df: pd.DataFrame):
    if df.empty:
        logger.warning("No volume data by intention to plot.")
        return

    df.sort_values('timestamp', inplace=True)
    df['cum_democrats'] = df['democrats_volume'].cumsum()
    df['cum_republicans'] = df['republicans_volume'].cumsum()
    df['cum_total'] = df['cum_democrats'] + df['cum_republicans']

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(16, 9))

    def get_legend_label(base_label: str, series: pd.Series) -> str:
        if series.empty: return f"{base_label} (Total: $0.00)"
        total = series.iloc[-1]
        return f"{base_label} (Total: ${total:,.0f})"

    ax.plot(df['timestamp'], df['cum_democrats'], color='steelblue', linestyle='-', linewidth=2, label=get_legend_label('Democrats Win', df['cum_democrats']))
    ax.plot(df['timestamp'], df['cum_republicans'], color='crimson', linestyle='-', linewidth=2, label=get_legend_label('Republicans Win', df['cum_republicans']))
    ax.plot(df['timestamp'], df['cum_total'], color='black', linestyle='--', linewidth=2.5, label=get_legend_label('Total', df['cum_total']))

    ax.set_title('Cumulative USDC Trading Inflow From Takers, by Intention', fontsize=18, pad=20)
    ax.set_xlabel('Date', fontsize=14)
    ax.set_ylabel('Cumulative Volume (in USDC)', fontsize=14)
    ax.legend(loc='best', fontsize='large')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    fig.autofmt_xdate()
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout()

    filename = 'volume_by_intention.png'
    plt.savefig(filename, dpi=150)
    logger.info(f"✅ Volume by intention plot saved to {filename}")
    plt.close()

# ----------------------------------------------------------------
# MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    logger.info("Fetching all settlement data for volume analysis...")
    settlement_records = await fetch_data("SELECT block_number, transaction_hash, type, trade FROM settlements_house")
    
    logger.info("Calculating trading volumes...")
    df_by_match, df_by_intention = calculate_volume_metrics(settlement_records)
    
    logger.info("Generating volume by match type plot...")
    plot_volume_by_match_type(df_by_match)
    
    logger.info("Generating volume by intention plot...")
    plot_volume_by_intention(df_by_intention)

if __name__ == "__main__":
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST, PG_PORT]):
        logger.error("One or more required database environment variables are not set.")
    else:
        asyncio.run(main())
