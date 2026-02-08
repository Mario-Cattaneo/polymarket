#!/usr/bin/env python3
"""
Debug a specific transaction with anomalous slippage (zero or near-zero despite large order).
"""

import os
import asyncio
import asyncpg
import logging
import json
from decimal import Decimal, getcontext
from eth_abi import decode

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
getcontext().prec = 36

PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

USDC_SCALAR = Decimal("1_000_000")
SHARE_SCALAR = Decimal("1_000_000")

# --- Block Range Configuration ---
START_BLOCK_NR = None  # None = use database minimum
END_BLOCK_NR = 79172085 + 50000  # None = use database maximum

# --- Transaction to debug ---
TARGET_TX = "0xe9f6bb3db6a38a"  # The 1.3M share order

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
        logger.error(f"Failed to decode: {e}")
        return {}

async def debug_transaction(tx_hash_prefix: str):
    """Debug a specific transaction."""
    conn = None
    try:
        conn = await asyncpg.connect(host=PG_HOST, port=int(PG_PORT), database=DB_NAME, user=DB_USER, password=DB_PASS)
        
        # Get block bounds if using defaults
        start_blk = START_BLOCK_NR
        end_blk = END_BLOCK_NR
        
        if start_blk is None or end_blk is None:
            row = await conn.fetchrow("SELECT MIN(block_number) as min_b, MAX(block_number) as max_b FROM settlements")
            if start_blk is None:
                start_blk = row['min_b']
            if end_blk is None:
                end_blk = row['max_b']
        
        logger.info(f"Searching for transaction starting with {tx_hash_prefix}...")
        logger.info(f"Block range: {start_blk} - {end_blk}")
        
        # Find transaction matching prefix and within block range
        row = await conn.fetchrow(
            "SELECT block_number, transaction_hash, type, trade FROM settlements WHERE transaction_hash LIKE $1 AND block_number >= $2 AND block_number < $3 LIMIT 1",
            tx_hash_prefix + '%', start_blk, end_blk
        )
        
        if not row:
            logger.error(f"No transaction found matching {tx_hash_prefix}")
            return
        
        tx_hash = row['transaction_hash']
        logger.info(f"\nFound: {tx_hash}")
        logger.info(f"Settlement type: {row['type']}")
        logger.info(f"Block: {row['block_number']}")
        
        trade = json.loads(row['trade'])
        
        logger.info(f"\nDECODED EVENTS:")
        
        # Decode all OrderFilled events
        EXCHANGE_ADDRESS = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
        
        all_fills = []
        exchange_fill = None
        
        for idx, fill in enumerate(trade.get('orders_filled', [])):
            decoded = decode_order_filled(fill['data'], fill['topics'])
            is_exchange = decoded['taker'].lower() == EXCHANGE_ADDRESS.lower()
            decoded['is_exchange'] = is_exchange
            all_fills.append(decoded)
            
            if is_exchange:
                exchange_fill = decoded
            
            role = "EXCHANGE" if is_exchange else "MAKER"
            logger.info(f"\n[{idx}] {role} OrderFilled:")
            logger.info(f"  Maker: {decoded['maker']}")
            logger.info(f"  Taker: {decoded['taker']}")
            logger.info(f"  makerAssetId: {decoded['makerAssetId']}")
            logger.info(f"  takerAssetId: {decoded['takerAssetId']}")
            logger.info(f"  making: {decoded['making']:,}")
            logger.info(f"  taking: {decoded['taking']:,}")
            logger.info(f"  fee: {decoded['fee']:,}")
        
        if not exchange_fill:
            logger.error("No exchange fill found!")
            return
        
        # Analyze
        logger.info(f"\nANALYSIS:")
        
        # Taker direction
        if exchange_fill['makerAssetId'] == 0:
            taker_is_buying = True
        else:
            taker_is_buying = False
        
        logger.info(f"Taker direction: {'BUYING' if taker_is_buying else 'SELLING'}")
        
        # Exchange fill details
        exchange_making = Decimal(exchange_fill['making'])
        exchange_taking = Decimal(exchange_fill['taking'])
        order_size = max(exchange_making, exchange_taking) / SHARE_SCALAR
        
        logger.info(f"Exchange order size: {order_size:.0f} shares")
        logger.info(f"Exchange making: {exchange_making:,} (raw)")
        logger.info(f"Exchange taking: {exchange_taking:,} (raw)")
        
        # Get maker fills
        maker_fills = [f for f in all_fills if not f['is_exchange']]
        logger.info(f"\nMaker fills: {len(maker_fills)}")
        
        if len(maker_fills) < 2:
            logger.info("  WARNING: Less than 2 maker fills. No best price comparison possible.")
        
        # Calculate prices
        prices = []
        for idx, fill in enumerate(maker_fills):
            maker_is_buying = (fill['makerAssetId'] == 0)
            
            making_raw = Decimal(fill['making'])
            taking_raw = Decimal(fill['taking'])
            
            # Price calculation
            price_per_token = min(making_raw, taking_raw) / max(making_raw, taking_raw)
            
            # If maker same side as taker, convert
            if maker_is_buying == taker_is_buying:
                price_per_token = Decimal(1) - price_per_token
            
            prices.append(price_per_token)
            
            logger.info(f"  Maker {idx}: makerAssetId={fill['makerAssetId']}, making={making_raw:,}, taking={taking_raw:,}")
            logger.info(f"              Raw price: {float(price_per_token):.6f}, same_side={maker_is_buying == taker_is_buying}")
        
        if len(prices) >= 2:
            if taker_is_buying:
                p_best = min(prices)
            else:
                p_best = max(prices)
            
            logger.info(f"\nPrice comparison:")
            logger.info(f"  Prices: {[f'{p:.6f}' for p in prices]}")
            logger.info(f"  Best price: {float(p_best):.6f} ({'MIN' if taker_is_buying else 'MAX'})")
            
            # Slippage calculation
            amount_shares = max(exchange_making, exchange_taking) / SHARE_SCALAR
            actual_cost = min(exchange_making, exchange_taking) / USDC_SCALAR
            expected_cost = p_best * amount_shares
            
            logger.info(f"\nSlippage calculation:")
            logger.info(f"  Amount shares: {amount_shares:.0f}")
            logger.info(f"  Actual cost: ${actual_cost:.6f}")
            logger.info(f"  Expected cost (p_best × amount): ${expected_cost:.6f}")
            logger.info(f"  Slippage: ${abs(expected_cost - actual_cost):.6f}")
        
    finally:
        if conn:
            await conn.close()

if __name__ == "__main__":
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST, PG_PORT]):
        logger.error("Missing database environment variables")
    else:
        asyncio.run(debug_transaction(TARGET_TX))
