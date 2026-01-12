#!/usr/bin/env python3
"""
Extract maker orders in the form (taker_side, maker_side, maker_orders)
where maker_orders = [(quantity, price), ...]
"""
import os
import asyncio
import asyncpg
import json
from typing import Tuple, List

PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

EXCHANGE_ADDRESS = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"

def extract_maker_orders(settlement: dict) -> Tuple[str, str, List[Tuple[int, float]]]:
    """
    Extract (taker_side, maker_side, maker_orders) from a decoded settlement.
    
    Returns:
        taker_side: 'BUY' or 'SELL' - what the taker is doing
        maker_side: 'BUY' or 'SELL' - what the makers are doing
        maker_orders: [(quantity_tokens, price_per_token), ...] sorted by price
    """
    trade = settlement['trade']
    settlement_type = settlement['type']
    
    # Get taker address from OrdersMatched
    orders_matched = trade.get('orders_matched', [])
    if not orders_matched:
        return None, None, []
    
    taker_addr = orders_matched[0].get('takerOrderMaker', '').lower()
    if not taker_addr:
        return None, None, []
    
    # Determine sides based on settlement type
    if settlement_type == 'split':
        # SPLIT: Taker sells tokens to makers who buy
        taker_side = 'SELL'
        maker_side = 'BUY'
    elif settlement_type == 'merge':
        # MERGE: Taker buys tokens from makers who sell
        taker_side = 'BUY'
        maker_side = 'SELL'
    elif settlement_type == 'complementary':
        # COMPLEMENTARY: Need to infer from maker fills
        # Look at real maker fills (exclude exchange)
        orders_filled = trade.get('orders_filled', [])
        real_maker_sides = []
        for of in orders_filled:
            maker = of.get('maker', '').lower()
            taker = of.get('taker', '').lower()
            # Skip exchange fill
            if taker == EXCHANGE_ADDRESS.lower():
                continue
            # Skip if this is actually the taker acting as maker vs exchange
            if maker == taker_addr:
                continue
            real_maker_sides.append(of.get('maker_side'))
        
        if not real_maker_sides:
            return None, None, []
        
        # All real makers should have same side
        maker_side = real_maker_sides[0]
        # Taker is opposite
        taker_side = 'BUY' if maker_side == 'SELL' else 'SELL'
    else:
        return None, None, []
    
    # Extract maker orders from OrderFilled events
    orders_filled = trade.get('orders_filled', [])
    maker_orders = []
    
    for of in orders_filled:
        maker = of.get('maker', '').lower()
        taker = of.get('taker', '').lower()
        
        # Skip exchange fill (taker = exchange)
        if taker == EXCHANGE_ADDRESS.lower():
            continue
        
        # Skip if this is the taker acting as maker against exchange
        if maker == taker_addr:
            continue
        
        # This is a real maker fill
        price = of.get('price', 0)
        
        # Determine token quantity
        maker_asset = of.get('makerAssetId', '')
        taker_asset = of.get('takerAssetId', '')
        making = of.get('making', 0)
        taking = of.get('taking', 0)
        
        # The token (non-USDC) quantity
        if maker_asset == '0':
            # Maker gives USDC, receives tokens
            quantity_tokens = taking
        elif taker_asset == '0':
            # Maker gives tokens, receives USDC
            quantity_tokens = making
        else:
            # Both are tokens (shouldn't happen in normal cases)
            quantity_tokens = making
        
        maker_orders.append((quantity_tokens, price))
    
    # Sort by price (ascending)
    maker_orders.sort(key=lambda x: x[1])
    
    return taker_side, maker_side, maker_orders


async def main():
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)

    async with pool.acquire() as conn:
        print("\n" + "="*100)
        print("Testing extract_maker_orders on example settlements")
        print("="*100)
        
        # Test SPLIT
        print("\n>>> SPLIT (1 PositionSplit) <<<")
        row = await conn.fetchrow("""
            SELECT * FROM settlements 
            WHERE type = 'split' AND exchange = 'base'
            AND jsonb_array_length(trade->'position_events') = 1
            ORDER BY block_number ASC LIMIT 1
        """)
        if row:
            from understand_split_base import decode_settlement
            decoded = decode_settlement(row)
            taker_side, maker_side, maker_orders = extract_maker_orders(decoded)
            
            print(f"TX: {decoded['transaction_hash']}")
            print(f"Taker Side: {taker_side}")
            print(f"Maker Side: {maker_side}")
            print(f"Maker Orders ({len(maker_orders)}):")
            for qty, price in maker_orders:
                print(f"  quantity={qty}, price={price:.6f}")
        
        # Test MERGE
        print("\n>>> MERGE (1 PositionsMerge) <<<")
        row = await conn.fetchrow("""
            SELECT * FROM settlements 
            WHERE type = 'merge' AND exchange = 'base'
            AND jsonb_array_length(trade->'position_events') = 1
            ORDER BY block_number ASC LIMIT 1
        """)
        if row:
            from understand_split_base import decode_settlement
            decoded = decode_settlement(row)
            taker_side, maker_side, maker_orders = extract_maker_orders(decoded)
            
            print(f"TX: {decoded['transaction_hash']}")
            print(f"Taker Side: {taker_side}")
            print(f"Maker Side: {maker_side}")
            print(f"Maker Orders ({len(maker_orders)}):")
            for qty, price in maker_orders:
                print(f"  quantity={qty}, price={price:.6f}")
        
        # Test COMPLEMENTARY with 2 OrderFilled
        print("\n>>> COMPLEMENTARY (2 OrderFilled) <<<")
        row = await conn.fetchrow("""
            SELECT * FROM settlements 
            WHERE type = 'complementary' AND exchange = 'base'
            AND jsonb_array_length(trade->'orders_filled') = 2
            ORDER BY block_number ASC LIMIT 1
        """)
        if row:
            from understand_split_base import decode_settlement
            decoded = decode_settlement(row)
            taker_side, maker_side, maker_orders = extract_maker_orders(decoded)
            
            print(f"TX: {decoded['transaction_hash']}")
            print(f"Taker Side: {taker_side}")
            print(f"Maker Side: {maker_side}")
            print(f"Maker Orders ({len(maker_orders)}):")
            for qty, price in maker_orders:
                print(f"  quantity={qty}, price={price:.6f}")
        
        # Test COMPLEMENTARY with 3 OrderFilled
        print("\n>>> COMPLEMENTARY (3 OrderFilled) <<<")
        row = await conn.fetchrow("""
            SELECT * FROM settlements 
            WHERE type = 'complementary' AND exchange = 'base'
            AND jsonb_array_length(trade->'orders_filled') = 3
            ORDER BY block_number ASC LIMIT 1
        """)
        if row:
            from understand_split_base import decode_settlement
            decoded = decode_settlement(row)
            taker_side, maker_side, maker_orders = extract_maker_orders(decoded)
            
            print(f"TX: {decoded['transaction_hash']}")
            print(f"Taker Side: {taker_side}")
            print(f"Maker Side: {maker_side}")
            print(f"Maker Orders ({len(maker_orders)}):")
            for qty, price in maker_orders:
                print(f"  quantity={qty}, price={price:.6f}")

    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
