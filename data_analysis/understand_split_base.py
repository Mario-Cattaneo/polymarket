#!/usr/bin/env python3
import os
import asyncio
import asyncpg
import json
from eth_abi import decode

PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

def decode_order_filled(data: str, topics: list) -> dict:
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
    except: return {}

def decode_orders_matched(data: str, topics: list) -> dict:
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
    except: return {}

def decode_position_split(data: str, topics: list, event_name: str) -> dict:
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
    except: return {}

def decode_settlement(row):
    """Decode settlement."""
    trade = json.loads(row['trade'])
    
    orders_matched = [decode_orders_matched(om['data'], om['topics']) for om in trade.get('orders_matched', [])]
    orders_filled = [decode_order_filled(of['data'], of['topics']) for of in trade.get('orders_filled', [])]
    position_events = [decode_position_split(pe['data'], pe['topics'], pe.get('event')) for pe in trade.get('position_events', [])]
    
    return {
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

async def main():
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)

    async with pool.acquire() as conn:
        # ===== SPLIT =====
        print("\n" + "="*100)
        print("SPLIT with 1 PositionSplit event")
        print("="*100)
        row = await conn.fetchrow("""
            SELECT id, block_number, transaction_hash, type, exchange, trade 
            FROM settlements 
            WHERE type = 'split' AND exchange = 'base'
            AND jsonb_array_length(trade->'position_events') = 1
            ORDER BY block_number ASC LIMIT 1
        """)
        if row:
            print(json.dumps(decode_settlement(row), indent=2))
        
        print("\n\n" + "="*100)
        print("SPLIT with 2 PositionSplit events")
        print("="*100)
        row = await conn.fetchrow("""
            SELECT id, block_number, transaction_hash, type, exchange, trade 
            FROM settlements 
            WHERE type = 'split' AND exchange = 'base'
            AND jsonb_array_length(trade->'position_events') = 2
            ORDER BY block_number ASC LIMIT 1
        """)
        if row:
            print(json.dumps(decode_settlement(row), indent=2))
        else:
            print("None found")
        
        print("\n\n" + "="*100)
        print("SPLIT with 3 PositionSplit events")
        print("="*100)
        row = await conn.fetchrow("""
            SELECT id, block_number, transaction_hash, type, exchange, trade 
            FROM settlements 
            WHERE type = 'split' AND exchange = 'base'
            AND jsonb_array_length(trade->'position_events') = 3
            ORDER BY block_number ASC LIMIT 1
        """)
        if row:
            print(json.dumps(decode_settlement(row), indent=2))
        else:
            print("None found")
        
        # ===== MERGE =====
        print("\n\n" + "="*100)
        print("MERGE with 1 PositionsMerge event")
        print("="*100)
        row = await conn.fetchrow("""
            SELECT id, block_number, transaction_hash, type, exchange, trade 
            FROM settlements 
            WHERE type = 'merge' AND exchange = 'base'
            AND jsonb_array_length(trade->'position_events') = 1
            ORDER BY block_number ASC LIMIT 1
        """)
        if row:
            print(json.dumps(decode_settlement(row), indent=2))
        else:
            print("None found")
        
        print("\n\n" + "="*100)
        print("MERGE with 2 PositionsMerge events")
        print("="*100)
        row = await conn.fetchrow("""
            SELECT id, block_number, transaction_hash, type, exchange, trade 
            FROM settlements 
            WHERE type = 'merge' AND exchange = 'base'
            AND jsonb_array_length(trade->'position_events') = 2
            ORDER BY block_number ASC LIMIT 1
        """)
        if row:
            print(json.dumps(decode_settlement(row), indent=2))
        else:
            print("None found")
        
        print("\n\n" + "="*100)
        print("MERGE with 3 PositionsMerge events")
        print("="*100)
        row = await conn.fetchrow("""
            SELECT id, block_number, transaction_hash, type, exchange, trade 
            FROM settlements 
            WHERE type = 'merge' AND exchange = 'base'
            AND jsonb_array_length(trade->'position_events') = 3
            ORDER BY block_number ASC LIMIT 1
        """)
        if row:
            print(json.dumps(decode_settlement(row), indent=2))
        else:
            print("None found")
        
        # ===== COMPLEMENTARY =====
        print("\n\n" + "="*100)
        print("COMPLEMENTARY with 2 OrderFilled events")
        print("="*100)
        row = await conn.fetchrow("""
            SELECT id, block_number, transaction_hash, type, exchange, trade 
            FROM settlements 
            WHERE type = 'complementary' AND exchange = 'base'
            AND jsonb_array_length(trade->'orders_filled') = 2
            ORDER BY block_number ASC LIMIT 1
        """)
        if row:
            print(json.dumps(decode_settlement(row), indent=2))
        else:
            print("None found")
        
        print("\n\n" + "="*100)
        print("COMPLEMENTARY with 3 OrderFilled events")
        print("="*100)
        row = await conn.fetchrow("""
            SELECT id, block_number, transaction_hash, type, exchange, trade 
            FROM settlements 
            WHERE type = 'complementary' AND exchange = 'base'
            AND jsonb_array_length(trade->'orders_filled') = 3
            ORDER BY block_number ASC LIMIT 1
        """)
        if row:
            print(json.dumps(decode_settlement(row), indent=2))
        else:
            print("None found")
        
        print("\n\n" + "="*100)
        print("COMPLEMENTARY with 4 OrderFilled events")
        print("="*100)
        row = await conn.fetchrow("""
            SELECT id, block_number, transaction_hash, type, exchange, trade 
            FROM settlements 
            WHERE type = 'complementary' AND exchange = 'base'
            AND jsonb_array_length(trade->'orders_filled') = 4
            ORDER BY block_number ASC LIMIT 1
        """)
        if row:
            print(json.dumps(decode_settlement(row), indent=2))
        else:
            print("None found")

    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
