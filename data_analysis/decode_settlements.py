#!/usr/bin/env python3
"""
Fully decode settlement transactions from the database.
Decodes all OrderFilled events, OrdersMatched, and position_events for investigation.
"""

import os
import asyncio
import json
import logging
from typing import Optional
from eth_abi import decode
import asyncpg

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Database Credentials
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# Constants
COLLATERAL_ASSET_ID = "0"
EXCHANGE_ADDRESS = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

# Asset ID mapping
ASSET_IDS = {
    "33156410999665902694": "SHARE_TOKEN_1",
    "83247781037352156539": "SHARE_TOKEN_2",
}


def get_asset_name(asset_id: str) -> str:
    """Get human-readable asset name."""
    if asset_id == COLLATERAL_ASSET_ID:
        return "USDC"
    return ASSET_IDS.get(asset_id, f"ASSET_{asset_id[:8]}...")


def decode_order_filled(data: str, topics: list) -> dict:
    """Decode OrderFilled event exactly like understand_split_base.py"""
    try:
        data_hex = bytes.fromhex(data[2:] if data.startswith('0x') else data)
        decoded = decode(['uint256', 'uint256', 'uint256', 'uint256', 'uint256'], data_hex)
        
        maker = '0x' + topics[2][-40:] if len(topics) > 2 else None
        taker = '0x' + topics[3][-40:] if len(topics) > 3 else None
        maker_asset_id = decoded[0]
        taker_asset_id = decoded[1]
        making = int(decoded[2])
        taking = int(decoded[3])
        fee = int(decoded[4])
        
        if taker_asset_id == 0:
            side = 'SELL'
            price = taking / making if making > 0 else 0
        elif maker_asset_id == 0:
            side = 'BUY'
            price = making / taking if taking > 0 else 0
        else:
            side = 'UNKNOWN'
            price = 0
        
        return {
            'event': 'OrderFilled',
            'maker': maker,
            'taker': taker,
            'makerAssetId': str(maker_asset_id),
            'takerAssetId': str(taker_asset_id),
            'making': making,
            'taking': taking,
            'fee': fee,
            'maker_side': side,
            'price': price
        }
    except:
        return {}


def decode_position_split(data: str, topics: list, event_name: str) -> dict:
    """Decode PositionSplit/PositionsMerge event exactly like understand_split_base.py"""
    try:
        data_hex = bytes.fromhex(data[2:] if data.startswith('0x') else data)
        decoded = decode(['address', 'uint256[]', 'uint256'], data_hex)
        return {
            'event': event_name,
            'stakeholder': '0x' + topics[1][-40:] if len(topics) > 1 else None,
            'parentCollectionId': topics[2] if len(topics) > 2 else None,
            'conditionId': topics[3] if len(topics) > 3 else None,
            'collateralToken': decoded[0],
            'partition': [str(x) for x in decoded[1]],
            'amount': int(decoded[2])
        }
    except:
        return {}
        logger.info(f"  Stakeholder: {decoded.get('stakeholder', 'N/A')}")
        logger.info(f"  Condition: {decoded.get('conditionId', 'N/A')}")
        logger.info(f"  Amount: {decoded.get('amount', 'N/A')}")
        logger.info(f"  Partition: {decoded.get('partition', [])}")


async def decode_settlements_batch(tx_hashes: list) -> None:
    """Decode multiple settlement transactions and output JSON."""
    conn = await asyncpg.connect(
        host=PG_HOST,
        port=int(PG_PORT),
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )
    
    try:
        for tx_hash in tx_hashes:
            row = await conn.fetchrow(
                "SELECT * FROM settlements_house WHERE transaction_hash = $1",
                tx_hash
            )
            
            if not row:
                logger.error(f"Not found: {tx_hash}")
                continue
            
            row = dict(row)
            trade = row['trade']
            if isinstance(trade, str):
                trade = json.loads(trade)
            
            # Decode all events
            orders_matched = [
                decode_orders_matched(om['data'], om['topics']) 
                for om in trade.get('orders_matched', [])
            ]
            orders_filled = [
                decode_order_filled(of['data'], of['topics']) 
                for of in trade.get('orders_filled', [])
            ]
            # For position events, identify event type from signature
            position_events = []
            for pe in trade.get('position_events', []):
                sig = pe['topics'][0] if pe.get('topics') else ''
                event_name = 'PositionSplit' if sig == '0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298' else \
                             'PositionsMerge' if sig == '0xbbed930dbfb7907ae2d60ddf78345610214f26419a0128df39b6cc3d9e5df9b0' else \
                             'Unknown'
                position_events.append(decode_position_split(pe['data'], pe['topics'], event_name))
            
            decoded = {
                'block_number': row['block_number'],
                'transaction_hash': row['transaction_hash'],
                'type': row['type'],
                'exchange': row['exchange'],
                'trade': {
                    'orders_matched': orders_matched,
                    'orders_filled': orders_filled,
                    'position_events': position_events
                }
            }
            
            print(json.dumps(decoded, indent=2))
            print()
    finally:
        await conn.close()


def decode_orders_matched(data: str, topics: list) -> dict:
    """Decode OrdersMatched event exactly like understand_split_base.py"""
    try:
        data_hex = bytes.fromhex(data[2:] if data.startswith('0x') else data)
        decoded = decode(['uint256', 'uint256', 'uint256', 'uint256'], data_hex)
        return {
            'event': 'OrdersMatched',
            'takerOrderMaker': '0x' + topics[2][-40:] if len(topics) > 2 else None,
            'makerAssetId': str(decoded[0]),
            'takerAssetId': str(decoded[1]),
            'makerAmountFilled': int(decoded[2]),
            'takerAmountFilled': int(decoded[3])
        }
    except:
        return {}


if __name__ == '__main__':
    # Transaction hashes to decode
    TX_HASHES = [
        '0xb31e4beb82438a7508da21475a3f3a9799d483bf51805ba89341a74cc1d19000'
    ]
    
    asyncio.run(decode_settlements_batch(TX_HASHES))