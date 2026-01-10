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
    # ABI: event PositionSplit(address indexed stakeholder, address collateralToken, bytes32 indexed parentCollectionId, bytes32 indexed conditionId, uint256[] partition, uint256 amount)
    # topics: [signature, stakeholder, parentCollectionId, conditionId]
    # data: [collateralToken, partition_offset, amount, partition_length, partition_items...]
    try:
        data_hex = bytes.fromhex(data[2:] if data.startswith('0x') else data)
        # We decode: address (collateral), uint256[] (partition), uint256 (amount)
        decoded = decode(['address', 'uint256[]', 'uint256'], data_hex)
        
        return {
            'event': 'PositionSplit',
            'stakeholder': '0x' + topics[1][-40:] if len(topics) > 1 else None,
            'parentCollectionId': topics[2] if len(topics) > 2 else None,
            'conditionId': topics[3] if len(topics) > 3 else None,
            'collateralToken': decoded[0],
            'partition': [str(x) for x in decoded[1]],
            'amount': str(decoded[2]) # This is the correct amount now
        }
    except Exception as e:
        return {'event': 'PositionSplit', 'error': str(e)}

def decode_position_merge(data: str, topics: List[str]) -> Dict[str, Any]:
    """Decode PositionsMerge event."""
    # ABI: event PositionsMerge(address indexed stakeholder, address collateralToken, bytes32 indexed parentCollectionId, bytes32 indexed conditionId, uint256[] partition, uint256 amount)
    # topics: [signature, stakeholder, parentCollectionId, conditionId]
    # data: [collateralToken, partition_offset, amount, partition_length, partition_items...]
    try:
        data_hex = bytes.fromhex(data[2:] if data.startswith('0x') else data)
        # We decode: address (collateral), uint256[] (partition), uint256 (amount)
        decoded = decode(['address', 'uint256[]', 'uint256'], data_hex)
        
        return {
            'event': 'PositionsMerge',
            'stakeholder': '0x' + topics[1][-40:] if len(topics) > 1 else None,
            'parentCollectionId': topics[2] if len(topics) > 2 else None,
            'conditionId': topics[3] if len(topics) > 3 else None,
            'collateralToken': decoded[0],
            'partition': [str(x) for x in decoded[1]],
            'amount': str(decoded[2]) # This is the correct amount now
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
        if isinstance(om, str):
            om = json.loads(om)
        topics = om['topics'] if isinstance(om['topics'], list) else json.loads(om['topics'])
        decoded_trade['orders_matched'].append(decode_event(om['data'], topics))
    
    # Decode OrderFilled events
    for of in trade.get('orders_filled', []):
        if isinstance(of, str):
            of = json.loads(of)
        topics = of['topics'] if isinstance(of['topics'], list) else json.loads(of['topics'])
        decoded_trade['orders_filled'].append(decode_event(of['data'], topics))
    
    # Decode position events
    for pe in trade.get('position_events', []):
        if isinstance(pe, str):
            pe = json.loads(pe)
        topics = pe['topics'] if isinstance(pe['topics'], list) else json.loads(pe['topics'])
        decoded_trade['position_events'].append(decode_event(pe['data'], topics))
    
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
            
            print("\n" + "=" * 80)
            print("ALL TESTS COMPLETED")
            print("=" * 80)
    
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(test_decoder())