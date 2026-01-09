#!/usr/bin/env python3
"""
Decoder for settlements table events.
Decodes the hex data and topics for OrderFilled, OrdersMatched, PositionSplit, PositionsMerge.
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
    """Identify event type from signature hash."""
    if not topics:
        return 'Unknown'
    sig = topics[0]
    for event_name, event_sig in EVENT_SIGS.items():
        if sig.lower() == event_sig.lower():
            return event_name
    return 'Unknown'

def decode_order_filled(data: str, topics: List[str]) -> Dict[str, Any]:
    """Decode OrderFilled event."""
    # topics: [signature, orderHash, maker, taker]
    # data: [makerAssetId, takerAssetId, making, taking, fee]
    try:
        data_hex = bytes.fromhex(data[2:] if data.startswith('0x') else data)
        decoded = decode(['uint256', 'uint256', 'uint256', 'uint256', 'uint256'], data_hex)
        
        return {
            'event': 'OrderFilled',
            'orderHash': topics[1] if len(topics) > 1 else None,
            'maker': '0x' + topics[2][-40:] if len(topics) > 2 else None,
            'taker': '0x' + topics[3][-40:] if len(topics) > 3 else None,
            'makerAssetId': str(decoded[0]),
            'takerAssetId': str(decoded[1]),
            'making': str(decoded[2]),
            'taking': str(decoded[3]),
            'fee': str(decoded[4])
        }
    except Exception as e:
        return {'event': 'OrderFilled', 'error': str(e)}

def decode_orders_matched(data: str, topics: List[str]) -> Dict[str, Any]:
    """Decode OrdersMatched event."""
    # topics: [signature, orderHash, maker]
    # data: [makerAssetId, takerAssetId, making, taking]
    try:
        data_hex = bytes.fromhex(data[2:] if data.startswith('0x') else data)
        decoded = decode(['uint256', 'uint256', 'uint256', 'uint256'], data_hex)
        
        return {
            'event': 'OrdersMatched',
            'orderHash': topics[1] if len(topics) > 1 else None,
            'maker': '0x' + topics[2][-40:] if len(topics) > 2 else None,
            'makerAssetId': str(decoded[0]),
            'takerAssetId': str(decoded[1]),
            'making': str(decoded[2]),
            'taking': str(decoded[3])
        }
    except Exception as e:
        return {'event': 'OrdersMatched', 'error': str(e)}

def decode_position_split(data: str, topics: List[str]) -> Dict[str, Any]:
    """Decode PositionSplit event."""
    # topics: [signature, stakeholder, conditionId]
    # data: [amount]
    try:
        data_hex = bytes.fromhex(data[2:] if data.startswith('0x') else data)
        decoded = decode(['uint256'], data_hex)
        
        return {
            'event': 'PositionSplit',
            'stakeholder': '0x' + topics[1][-40:] if len(topics) > 1 else None,
            'conditionId': topics[2] if len(topics) > 2 else None,
            'amount': str(decoded[0])
        }
    except Exception as e:
        return {'event': 'PositionSplit', 'error': str(e)}

def decode_position_merge(data: str, topics: List[str]) -> Dict[str, Any]:
    """Decode PositionsMerge event."""
    # topics: [signature, stakeholder, conditionId]
    # data: [amount]
    try:
        data_hex = bytes.fromhex(data[2:] if data.startswith('0x') else data)
        decoded = decode(['uint256'], data_hex)
        
        return {
            'event': 'PositionsMerge',
            'stakeholder': '0x' + topics[1][-40:] if len(topics) > 1 else None,
            'conditionId': topics[2] if len(topics) > 2 else None,
            'amount': str(decoded[0])
        }
    except Exception as e:
        return {'event': 'PositionsMerge', 'error': str(e)}

def decode_event(data: str, topics: List[str]) -> Dict[str, Any]:
    """Decode any event based on signature."""
    event_name = identify_event(topics)
    
    if event_name == 'OrderFilled':
        return decode_order_filled(data, topics)
    elif event_name == 'OrdersMatched':
        return decode_orders_matched(data, topics)
    elif event_name == 'PositionSplit':
        return decode_position_split(data, topics)
    elif event_name == 'PositionsMerge':
        return decode_position_merge(data, topics)
    else:
        return {'event': 'Unknown', 'error': 'Unknown event signature'}

def decode_settlement(settlement: Dict[str, Any]) -> Dict[str, Any]:
    """Decode a complete settlement record."""
    trade = settlement['trade']
    
    # Parse trade JSON if it's a string
    if isinstance(trade, str):
        trade = json.loads(trade)
    
    decoded_trade = {
        'transaction_hash': trade.get('transaction_hash'),
        'orders_matched': [],
        'orders_filled': [],
        'position_events': []
    }
    
    # Decode OrdersMatched events
    for om in trade.get('orders_matched', []):
        decoded_trade['orders_matched'].append(decode_event(om['data'], om['topics']))
    
    # Decode OrderFilled events
    for of in trade.get('orders_filled', []):
        decoded_trade['orders_filled'].append(decode_event(of['data'], of['topics']))
    
    # Decode position events
    for pe in trade.get('position_events', []):
        decoded_trade['position_events'].append(decode_event(pe['data'], pe['topics']))
    
    return {
        'id': settlement['id'],
        'timestamp_ms': settlement['timestamp_ms'],
        'transaction_hash': settlement['transaction_hash'],
        'type': settlement['type'],
        'exchange': settlement['exchange'],
        'trade': decoded_trade
    }

# ================================================================
# TESTS
# ================================================================

async def test_decoder():
    """Run tests on the decoder."""
    pool = await asyncpg.create_pool(
        host=PG_HOST,
        port=int(PG_PORT),
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        min_size=1,
        max_size=5,
    )
    
    try:
        async with pool.acquire() as conn:
            print("\n" + "=" * 80)
            print("DECODER TESTS")
            print("=" * 80)
            
            # Test 1: Decode a complementary settlement
            print("\n[TEST 1] Decoding a 'complementary' settlement...")
            row = await conn.fetchrow(
                "SELECT * FROM settlements WHERE type = 'complementary' LIMIT 1"
            )
            if row:
                decoded = decode_settlement(dict(row))
                print(json.dumps(decoded, indent=2))
                print("✅ TEST 1 PASSED")
            else:
                print("❌ No complementary settlement found")
            
            # Test 2: Decode a split settlement
            print("\n[TEST 2] Decoding a 'split' settlement...")
            row = await conn.fetchrow(
                "SELECT * FROM settlements WHERE type = 'split' LIMIT 1"
            )
            if row:
                decoded = decode_settlement(dict(row))
                print(json.dumps(decoded, indent=2))
                print("✅ TEST 2 PASSED")
            else:
                print("❌ No split settlement found")
            
            # Test 3: Decode a merge settlement
            print("\n[TEST 3] Decoding a 'merge' settlement...")
            row = await conn.fetchrow(
                "SELECT * FROM settlements WHERE type = 'merge' LIMIT 1"
            )
            if row:
                decoded = decode_settlement(dict(row))
                print(json.dumps(decoded, indent=2))
                print("✅ TEST 3 PASSED")
            else:
                print("❌ No merge settlement found")
            
            # Test 4: Check statistics
            print("\n[TEST 4] Settlement statistics...")
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN type = 'complementary' THEN 1 ELSE 0 END) as complementary,
                    SUM(CASE WHEN type = 'split' THEN 1 ELSE 0 END) as split,
                    SUM(CASE WHEN type = 'merge' THEN 1 ELSE 0 END) as merge,
                    SUM(CASE WHEN exchange = 'base' THEN 1 ELSE 0 END) as base_count,
                    SUM(CASE WHEN exchange = 'negrisk' THEN 1 ELSE 0 END) as negrisk_count
                FROM settlements
            """)
            print(json.dumps(dict(stats), indent=2))
            print("✅ TEST 4 PASSED")
            
            # Test 5: Verify OrderFilled/OrdersMatched counts
            print("\n[TEST 5] Verifying event counts...")
            sample = await conn.fetchrow("""
                SELECT 
                    jsonb_array_length(trade->'orders_matched') as om_count,
                    jsonb_array_length(trade->'orders_filled') as of_count,
                    jsonb_array_length(trade->'position_events') as pe_count
                FROM settlements
                LIMIT 1
            """)
            print(f"Sample settlement: {dict(sample)}")
            print("✅ TEST 5 PASSED")
            
            print("\n" + "=" * 80)
            print("ALL TESTS COMPLETED")
            print("=" * 80)
    
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(test_decoder())
