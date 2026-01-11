#!/usr/bin/env python3
"""
Decoder for settlements table (Block Number Schema).
UPDATED: Correctly maps OrdersMatched topic 2 to 'takerOrderMaker'.
"""

import os
import asyncio
import asyncpg
import json
from eth_abi import decode
from typing import Dict, Any, List

# Database Credentials
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# Event signatures
EVENT_SIGS = {
    'OrderFilled': '0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6',
    'OrdersMatched': '0x63bf4d16b7fa898ef4c4b2b6d90fd201e9c56313b65638af6088d149d2ce956c',
    'PositionSplit': '0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298',
    'PositionsMerge': '0x6f13ca62553fcc2bcd2372180a43949c1e4cebba603901ede2f4e14f36b282ca',
}

def identify_event(topics: List[str]) -> str:
    if not topics: return 'Unknown'
    sig = topics[0]
    for event_name, event_sig in EVENT_SIGS.items():
        if sig.lower() == event_sig.lower(): return event_name
    return 'Unknown'

def decode_order_filled(data: str, topics: List[str]) -> Dict[str, Any]:
    # Topics: [sig, orderHash, maker, taker]
    # maker is topics[2], taker is topics[3]
    # Data: makerAssetId, takerAssetId, making, taking, fee
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
        
        # Determine maker's side and calculate price per token (USDC/token, in [0,1])
        if taker_asset_id == 0:
            # Maker is SELLING tokens (gives tokens, receives USDC)
            # Price = USDC / tokens = taking / making
            side = 'SELL'
            price = taking / making if making > 0 else 0
        elif maker_asset_id == 0:
            # Maker is BUYING tokens (gives USDC, receives tokens)
            # Price = USDC / tokens = making / taking
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
            'making': str(making),
            'taking': str(taking),
            'fee': str(fee),
            'maker_side': side,
            'price': round(price, 6)
        }
    except Exception as e: return {'event': 'OrderFilled', 'error': str(e)}

def decode_orders_matched(data: str, topics: List[str]) -> Dict[str, Any]:
    # Topics: [sig, takerOrderHash, takerOrderMaker]
    try:
        data_hex = bytes.fromhex(data[2:] if data.startswith('0x') else data)
        decoded = decode(['uint256', 'uint256', 'uint256', 'uint256'], data_hex)
        return {
            'event': 'OrdersMatched',
            'takerOrderMaker': '0x' + topics[2][-40:] if len(topics) > 2 else None,
            'makerAssetId': str(decoded[0]),
            'takerAssetId': str(decoded[1]),
            'makerAmountFilled': str(decoded[2]),
            'takerAmountFilled': str(decoded[3])
        }
    except Exception as e: return {'event': 'OrdersMatched', 'error': str(e)}

def decode_position_event(data: str, topics: List[str], event_name: str) -> Dict[str, Any]:
    # For PositionSplit/PositionsMerge:
    # topics[0]: signature, topics[1]: stakeholder (indexed), topics[2]: parentCollectionId (indexed), topics[3]: conditionId (indexed)
    # data: collateralToken (address), partition (uint[]), amount (uint256)
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
            'amount': str(decoded[2])
        }
    except Exception as e: return {'event': event_name, 'error': str(e)}

def decode_event(data: str, topics: List[str]) -> Dict[str, Any]:
    event_name = identify_event(topics)
    if event_name == 'OrderFilled': return decode_order_filled(data, topics)
    elif event_name == 'OrdersMatched': return decode_orders_matched(data, topics)
    elif event_name in ['PositionSplit', 'PositionsMerge']: return decode_position_event(data, topics, event_name)
    return {'event': 'Unknown'}

def decode_settlement(settlement: Dict[str, Any]) -> Dict[str, Any]:
    trade = settlement['trade']
    if isinstance(trade, str): trade = json.loads(trade)
    
    decoded_trade = {'orders_matched': [], 'orders_filled': [], 'position_events': []}
    
    for om in trade.get('orders_matched', []):
        decoded_trade['orders_matched'].append(decode_event(om['data'], om['topics']))
    for of in trade.get('orders_filled', []):
        decoded_trade['orders_filled'].append(decode_event(of['data'], of['topics']))
    for pe in trade.get('position_events', []):
        decoded_trade['position_events'].append(decode_event(pe['data'], pe['topics']))
    
    return {
        'block_number': settlement['block_number'],
        'transaction_hash': settlement['transaction_hash'],
        'type': settlement['type'],
        'exchange': settlement['exchange'],
        'trade': decoded_trade
    }

async def test_decoder():
    pool = await asyncpg.create_pool(host=PG_HOST, port=int(PG_PORT), database=DB_NAME, user=DB_USER, password=DB_PASS)
    try:
        async with pool.acquire() as conn:
            print("\n[TEST] Decoding complementary base settlements with >= 3 orders_filled...")
            
            # Query for complementary base settlements with at least 3 orders_filled
            rows = await conn.fetch("""
                SELECT * FROM settlements
                WHERE type = 'complementary'
                AND exchange = 'base'
                AND jsonb_array_length(trade->'orders_filled') >= 3
                LIMIT 2
            """)
            
            if rows:
                for i, row in enumerate(rows, 1):
                    print(f"\n{'='*80}")
                    print(f"Base Complementary Settlement #{i}")
                    print(f"{'='*80}")
                    decoded = decode_settlement(dict(row))
                    print(json.dumps(decoded, indent=2))
            else:
                print("No matching base complementary settlements found.")
            
            # Query for negrisk settlements (split, merge, conversion)
            print("\n" + "="*80)
            print("[TEST] Decoding NegRisk settlements (split, merge, conversion)...")
            print("="*80)
            
            for stype in ['split', 'merge', 'conversion']:
                print(f"\n>>> {stype.upper()} <<<")
                rows = await conn.fetch("""
                    SELECT * FROM settlements
                    WHERE type = $1
                    AND exchange = 'negrisk'
                    LIMIT 2
                """, stype)
                
                if rows:
                    for i, row in enumerate(rows, 1):
                        print(f"\n{'-'*80}")
                        print(f"NegRisk {stype.capitalize()} Settlement #{i}")
                        print(f"{'-'*80}")
                        decoded = decode_settlement(dict(row))
                        print(json.dumps(decoded, indent=2))
                else:
                    print(f"No {stype} settlements found.")
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(test_decoder())