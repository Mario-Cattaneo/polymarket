#!/usr/bin/env python3
"""
Analytics script to calculate and plot the cumulative slippage from on-chain settlements.

Slippage is defined as the value lost by a taker for accepting prices worse than the
best price available to them within a single atomic trade filled by multiple makers.

This script generates one plot with three lines:
- Cumulative slippage for takers with "Democrats Win" intention.
- Cumulative slippage for takers with "Republicans Win" intention.
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

# --- Taker Intention Asset IDs ---
# Democrats Win: buying Democrats Yes tokens OR selling Republicans (Yes or No) tokens
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
    """
    Determine taker's intention based on which asset they're trading and their direction.
    - BUYING Democrats tokens or SELLING Republicans tokens → "Democrats"
    - BUYING Republicans tokens or SELLING Democrats tokens → "Republicans"
    """
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
    """Decode OrderFilled event exactly as understand_split_base.py does."""
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

def calculate_slippage_from_settlements(records: list) -> pd.DataFrame:
    """
    Processes settlement records to calculate slippage for each multi-fill transaction.
    
    Logic:
    1. Decode all OrderFilled events from raw topics/data
    2. Filter out any where taker is the exchange
    3. If more than 1 fill exists, calculate slippage
    4. For each fill: if takerAssetId == "0", maker is SELLING (price = taking/making)
                      else maker is BUYING (price = making/taking)
    5. Determine market price: min if makers selling, max if makers buying
    6. Slippage = |p_market * total_shares - actual_usdc|
    """
    slippage_events = []
    computation_count = 0
    EXCHANGE_ADDRESS = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

    for row in records:
        try:
            trade = json.loads(row['trade'])
            
            if not trade.get('orders_matched') or len(trade.get('orders_filled', [])) < 2:
                continue
            
            computation_count += 1
            
            logger.info(f"[TX #{computation_count}] Hash: {row['transaction_hash']} | Type: {row.get('type', 'N/A')}")
            logger.info(f"  Total OrderFilled events: {len(trade.get('orders_filled', []))}")

            # 0. Decode all OrderFilled events using the exact same logic as understand_split_base.py
            all_fills = []
            exchange_fill = None
            for idx, fill in enumerate(trade.get('orders_filled', [])):
                decoded_fill = decode_order_filled(fill['data'], fill['topics'])
                if decoded_fill:
                    is_exchange_fill = (decoded_fill['taker'].lower() == EXCHANGE_ADDRESS.lower())
                    
                    logger.info(f"  OrderFilled #{idx+1}: Maker={decoded_fill['maker']} | Taker={decoded_fill['taker']} | MakerAsset={decoded_fill['makerAssetId']} | TakerAsset={decoded_fill['takerAssetId']} | making={decoded_fill['making']}, taking={decoded_fill['taking']} | IsExchange={is_exchange_fill}")
                    
                    decoded_fill['is_exchange'] = is_exchange_fill
                    all_fills.append(decoded_fill)
                    
                    if is_exchange_fill:
                        exchange_fill = decoded_fill

            # 1. Find the exchange fill to determine taker direction
            if not exchange_fill:
                logger.info(f"  Skipping: No exchange fill found")
                continue
            
            # Determine taker (exchange) direction: BUY if makerAssetId==0, else SELL
            if exchange_fill['makerAssetId'] == 0:
                taker_is_buying = True
                logger.info(f"  Exchange taker direction: BUYING (makerAssetId == 0)")
            else:
                taker_is_buying = False
                logger.info(f"  Exchange taker direction: SELLING (makerAssetId != 0)")

            # 2. Extract taker vs maker fills (exclude the exchange fill)
            taker_vs_maker_fills = [f for f in all_fills if not f['is_exchange']]
            
            logger.info(f"  → Selected {len(taker_vs_maker_fills)} taker vs maker fills (excluded taker=exchange={EXCHANGE_ADDRESS[:10]}...)")
            
            # Slippage requires more than 1 taker vs maker fill
            if len(taker_vs_maker_fills) < 2:
                logger.info(f"  Skipping: Need at least 2 taker vs maker fills, only found {len(taker_vs_maker_fills)}")
                continue

            # 3. For each maker fill: compute direction, price, then convert if same as taker
            fills_info = []
            
            for idx, fill in enumerate(taker_vs_maker_fills):
                maker_asset_id = fill['makerAssetId']
                taker_asset_id = fill['takerAssetId']
                making_raw = Decimal(fill['making'])
                taking_raw = Decimal(fill['taking'])
                
                # Determine if maker is buying or selling: BUY if makerAssetId==0, else SELL
                maker_is_buying = (maker_asset_id == 0)
                
                # Price is always min/max to keep in [0,1]
                price_per_token = min(making_raw, taking_raw) / max(making_raw, taking_raw)
                
                # Determine amounts based on asset IDs
                if taker_asset_id == 0:
                    # takerAssetId == 0 means taker receives USDC, so maker gives shares (SELLING)
                    amount_shares = making_raw / SHARE_SCALAR
                    amount_usdc = taking_raw / USDC_SCALAR
                    side_label = "SELLING"
                else:
                    # takerAssetId != 0 means taker receives shares, so maker receives USDC (BUYING)
                    amount_shares = taking_raw / SHARE_SCALAR
                    amount_usdc = making_raw / USDC_SCALAR
                    side_label = "BUYING"
                
                logger.info(f"    Fill #{idx+1} (Maker={fill['maker'][:10]}...): Maker {side_label} (makerAssetId={'0' if maker_is_buying else 'shares'}) | making={making_raw}, taking={taking_raw}")
                
                # If maker is on same side as taker, convert price to taker's perspective
                original_price = price_per_token
                if maker_is_buying == taker_is_buying:
                    price_per_token = Decimal(1) - price_per_token
                    logger.info(f"      Same side as taker (both {'BUYING' if taker_is_buying else 'SELLING'}): converted price from {original_price} to {price_per_token}")
                else:
                    logger.info(f"      Opposite side as taker: keep price {original_price}")
                
                logger.info(f"      → Price per token: {price_per_token} | Shares: {amount_shares} | USDC: {amount_usdc}")
                
                fills_info.append({
                    'price_per_token': price_per_token,
                    'amount_shares': amount_shares,
                    'amount_usdc': amount_usdc
                })

            # 4. Determine market price based on taker direction
            if len(fills_info) >= 2:
                prices = [f['price_per_token'] for f in fills_info]
                if taker_is_buying:
                    p_best = min(prices)
                    logger.info(f"  Taker BUYING - Best price (min): {p_best}")
                else:
                    p_best = max(prices)
                    logger.info(f"  Taker SELLING - Best price (max): {p_best}")

                # 5. Calculate slippage using exchange fill amounts
                exchange_making = Decimal(exchange_fill['making'])
                exchange_taking = Decimal(exchange_fill['taking'])
                amount_shares = max(exchange_making, exchange_taking) / SHARE_SCALAR
                actual_cost = min(exchange_making, exchange_taking) / USDC_SCALAR
                expected_cost = p_best * amount_shares
                tx_slippage = abs(expected_cost - actual_cost)
                
                logger.info(f"  Exchange making: {exchange_making}, taking: {exchange_taking}")
                logger.info(f"  Amount shares: {amount_shares}")
                logger.info(f"  Actual cost: {actual_cost}")
                logger.info(f"  Expected cost (p_best × amount_shares): {expected_cost}")
                logger.info(f"  SLIPPAGE: ${tx_slippage}")
            else:
                continue

            if tx_slippage > 0:
                # Determine taker's intention based on the non-zero asset ID
                non_zero_asset_id = exchange_fill['makerAssetId'] if exchange_fill['takerAssetId'] == 0 else exchange_fill['takerAssetId']
                taker_intention = determine_taker_intention(non_zero_asset_id, taker_is_buying)
                
                logger.info(f"  Taker intention: {taker_intention} (based on asset {non_zero_asset_id})")
                
                slippage_events.append({
                    'timestamp': pd.to_datetime(block_to_timestamp(row['block_number']), unit='s'),
                    'democrats_slippage': tx_slippage if taker_intention == "Democrats" else Decimal(0),
                    'republicans_slippage': tx_slippage if taker_intention == "Republicans" else Decimal(0)
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
    df['cum_democrats'] = df['democrats_slippage'].cumsum()
    df['cum_republicans'] = df['republicans_slippage'].cumsum()
    df['cum_combined'] = df['cum_democrats'] + df['cum_republicans']

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(16, 9))

    def get_legend_label(base_label: str, series: pd.Series) -> str:
        if series.empty: return f"{base_label} (Total: $0.00)"
        total = series.iloc[-1]
        return f"{base_label} (Total: ${total:,.2f})"

    ax.plot(df['timestamp'], df['cum_democrats'], color='steelblue', linestyle='-', linewidth=2, label=get_legend_label('Democrats Win Intention', df['cum_democrats']))
    ax.plot(df['timestamp'], df['cum_republicans'], color='crimson', linestyle='-', linewidth=2, label=get_legend_label('Republicans Win Intention', df['cum_republicans']))
    ax.plot(df['timestamp'], df['cum_combined'], color='black', linestyle='--', linewidth=2.5, label=get_legend_label('Combined Slippage', df['cum_combined']))

    ax.set_title('Cumulative Slippage in On-Chain Settlements by Taker Intention', fontsize=18, pad=20)
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
    settlement_records = await fetch_data("SELECT block_number, transaction_hash, type, trade FROM settlements_house")
    
    logger.info("Calculating slippage from settlement data...")
    df_slippage = calculate_slippage_from_settlements(settlement_records)
    
    logger.info("Generating cumulative slippage plot...")
    plot_cumulative_slippage(df_slippage)

if __name__ == "__main__":
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST, PG_PORT]):
        logger.error("One or more required database environment variables are not set.")
    else:
        asyncio.run(main())