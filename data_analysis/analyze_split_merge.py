#!/usr/bin/env python3
"""
Analyze Split/Merge Settlements Structure
==========================================
Focus on properly interpreting:
- OrdersMatched events
- OrderFilled events  
- PositionSplit/PositionsMerge events

Goal: Understand how these events relate to each other to correctly
compute volume and slippage.
"""
import os
import asyncio
import asyncpg
import json
from collections import defaultdict

PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

USDC_DECIMALS = 6

def format_token_id(token_id):
    """Shorten token ID for display."""
    s = str(token_id)
    return f"{s[:8]}...{s[-8:]}" if len(s) > 20 else s

def analyze_settlement(row):
    """Detailed analysis of a single settlement."""
    trade = json.loads(row['trade'])
    
    print(f"\n{'='*100}")
    print(f"Block: {row['block_number']}, TX: {row['transaction_hash']}")
    print(f"Type: {row['type']}, Exchange: {row['exchange']}")
    print(f"{'='*100}")
    
    # 1. OrdersMatched
    print("\n>>> ORDERS MATCHED <<<")
    for i, om in enumerate(trade.get('orders_matched', []), 1):
        taker = om.get('takerOrderMaker', 'N/A')
        maker_asset = om.get('makerAssetId', 'N/A')
        taker_asset = om.get('takerAssetId', 'N/A')
        maker_filled = int(om.get('makerAmountFilled', 0))
        taker_filled = int(om.get('takerAmountFilled', 0))
        
        print(f"\n  OrdersMatched #{i}:")
        print(f"    Taker (takerOrderMaker): {taker}")
        print(f"    makerAssetId: {format_token_id(maker_asset)} {'(USDC)' if maker_asset == '0' else '(TOKEN)'}")
        print(f"    takerAssetId: {format_token_id(taker_asset)} {'(USDC)' if taker_asset == '0' else '(TOKEN)'}")
        print(f"    makerAmountFilled: {maker_filled} ({maker_filled/10**USDC_DECIMALS:.2f} if USDC)")
        print(f"    takerAmountFilled: {taker_filled} ({taker_filled/10**USDC_DECIMALS:.2f} if USDC)")
        
        # Interpret direction
        if maker_asset == '0':
            print(f"    => Taker ORDER was: SELL tokens, receive USDC")
            print(f"    => Taker filled: gave {taker_filled} tokens, got {maker_filled/10**USDC_DECIMALS:.2f} USDC")
        else:
            print(f"    => Taker ORDER was: BUY tokens, pay USDC")
            print(f"    => Taker filled: gave {taker_filled/10**USDC_DECIMALS:.2f} USDC, got {maker_filled} tokens")
    
    # 2. OrderFilled events
    print("\n>>> ORDER FILLED EVENTS <<<")
    
    # Get taker address from OrdersMatched
    taker_addr = None
    if trade.get('orders_matched'):
        taker_addr = trade['orders_matched'][0].get('takerOrderMaker', '').lower()
    
    # Group by token ID to see complementary pairs
    by_token = defaultdict(list)
    exchange_fill = None
    
    for i, of in enumerate(trade.get('orders_filled', []), 1):
        maker = of.get('maker', '').lower()
        taker = of.get('taker', '').lower()
        maker_asset = of.get('makerAssetId', 'N/A')
        taker_asset = of.get('takerAssetId', 'N/A')
        making = int(of.get('making', 0))
        taking = int(of.get('taking', 0))
        maker_side = of.get('maker_side', 'N/A')
        price = float(of.get('price', 0))
        
        # Determine the token being traded (non-USDC asset)
        if taker_asset == '0':
            token_id = maker_asset
            token_amount = making
            usdc_amount = taking
        else:
            token_id = taker_asset
            token_amount = taking
            usdc_amount = making
        
        is_exchange_fill = (maker == taker_addr)
        if is_exchange_fill:
            exchange_fill = of
        
        fill_info = {
            'idx': i,
            'maker': maker,
            'taker': taker,
            'maker_side': maker_side,
            'price': price,
            'token_amount': token_amount,
            'usdc_amount': usdc_amount,
            'is_exchange_fill': is_exchange_fill
        }
        by_token[token_id].append(fill_info)
        
        marker = " *** EXCHANGE FILL (taker vs exchange) ***" if is_exchange_fill else ""
        print(f"\n  OrderFilled #{i}:{marker}")
        print(f"    maker: {maker[:20]}...")
        print(f"    taker: {taker[:20]}...")
        print(f"    makerAssetId: {format_token_id(maker_asset)} {'(USDC)' if maker_asset == '0' else '(TOKEN)'}")
        print(f"    takerAssetId: {format_token_id(taker_asset)} {'(USDC)' if taker_asset == '0' else '(TOKEN)'}")
        print(f"    making: {making}, taking: {taking}")
        print(f"    maker_side: {maker_side}, price: {price:.6f}")
        print(f"    => Token: {format_token_id(token_id)}, amount: {token_amount}, USDC: {usdc_amount/10**USDC_DECIMALS:.2f}")
    
    # 3. Position Events
    print("\n>>> POSITION EVENTS <<<")
    total_position_amount = 0
    for i, pe in enumerate(trade.get('position_events', []), 1):
        event_type = pe.get('event', 'N/A')
        stakeholder = pe.get('stakeholder', 'N/A')
        condition_id = pe.get('conditionId', 'N/A')
        amount = int(pe.get('amount', 0))
        partition = pe.get('partition', [])
        total_position_amount += amount
        
        print(f"\n  {event_type} #{i}:")
        print(f"    stakeholder: {stakeholder}")
        print(f"    conditionId: {condition_id[:20]}...")
        print(f"    partition: {partition}")
        print(f"    amount: {amount} ({amount/10**USDC_DECIMALS:.2f} USDC equivalent)")
    
    print(f"\n  Total position amount: {total_position_amount} ({total_position_amount/10**USDC_DECIMALS:.2f} USDC)")
    
    # 4. Analysis by token
    print("\n>>> ANALYSIS BY TOKEN <<<")
    print(f"Number of distinct tokens traded: {len(by_token)}")
    
    for token_id, fills in by_token.items():
        print(f"\n  Token: {format_token_id(token_id)}")
        total_tokens = sum(f['token_amount'] for f in fills if not f['is_exchange_fill'])
        total_usdc = sum(f['usdc_amount'] for f in fills if not f['is_exchange_fill'])
        prices = [f['price'] for f in fills if not f['is_exchange_fill']]
        
        print(f"    Fills (excl exchange): {len([f for f in fills if not f['is_exchange_fill']])}")
        print(f"    Total tokens: {total_tokens}")
        print(f"    Total USDC: {total_usdc} ({total_usdc/10**USDC_DECIMALS:.2f})")
        print(f"    Prices: {prices}")
        if prices:
            print(f"    Price range: {min(prices):.6f} - {max(prices):.6f}")
            if len(prices) > 1 and min(prices) != max(prices):
                print(f"    => SLIPPAGE POSSIBLE within this token!")
    
    # 5. Summary
    print("\n>>> SUMMARY <<<")
    print(f"Settlement type: {row['type'].upper()}")
    
    if row['type'] == 'split':
        print("SPLIT: USDC -> YES tokens + NO tokens")
        print("  - Taker provides USDC")
        print("  - PositionSplit creates token pairs")  
        print("  - Makers buy the created tokens (one buys YES, one buys NO)")
        print("  - OrderFilled events show sales to different makers for DIFFERENT tokens")
        print("  - Prices should sum to ~1.0 (e.g., 0.99 + 0.01 = 1.00)")
    elif row['type'] == 'merge':
        print("MERGE: YES tokens + NO tokens -> USDC")
        print("  - Taker provides tokens (buys from makers)")
        print("  - PositionsMerge destroys token pairs for USDC")
        print("  - Makers sell their tokens to taker")
        print("  - OrderFilled events show purchases from different makers for DIFFERENT tokens")
        print("  - Prices should sum to ~1.0 (e.g., 0.70 + 0.30 = 1.00)")
    
    # Check if prices sum to ~1.0
    if len(by_token) == 2:
        token_prices = []
        for fills in by_token.values():
            non_exchange = [f for f in fills if not f['is_exchange_fill']]
            if non_exchange:
                avg_price = sum(f['price'] for f in non_exchange) / len(non_exchange)
                token_prices.append(avg_price)
        
        if len(token_prices) == 2:
            price_sum = sum(token_prices)
            print(f"\n  Token prices: {token_prices[0]:.6f} + {token_prices[1]:.6f} = {price_sum:.6f}")
            print(f"  {'✓ Prices sum to ~1.0 (complementary tokens)' if 0.99 <= price_sum <= 1.01 else '✗ Prices do NOT sum to 1.0!'}")

async def main():
    print(f"Connecting to {DB_NAME}...")
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)

    async with pool.acquire() as conn:
        # Find SPLIT settlements with multiple position events
        print("\n" + "="*100)
        print("SPLIT SETTLEMENTS WITH MULTIPLE POSITION EVENTS (Base Exchange)")
        print("="*100)
        
        split_rows = await conn.fetch("""
            SELECT id, block_number, transaction_hash, type, exchange, trade 
            FROM settlements 
            WHERE type = 'split' AND exchange = 'base'
            AND jsonb_array_length(trade->'position_events') > 1
            ORDER BY block_number ASC
            LIMIT 2
        """)
        
        for row in split_rows:
            analyze_settlement(row)
        
        # Find MERGE settlements with multiple position events  
        print("\n" + "="*100)
        print("MERGE SETTLEMENTS WITH MULTIPLE POSITION EVENTS (Base Exchange)")
        print("="*100)
        
        merge_rows = await conn.fetch("""
            SELECT id, block_number, transaction_hash, type, exchange, trade 
            FROM settlements 
            WHERE type = 'merge' AND exchange = 'base'
            AND jsonb_array_length(trade->'position_events') > 1
            ORDER BY block_number ASC
            LIMIT 2
        """)
        
        for row in merge_rows:
            analyze_settlement(row)
        
        # Also show single position event cases for comparison
        print("\n" + "="*100)
        print("SPLIT SETTLEMENT WITH SINGLE POSITION EVENT (Base Exchange) - for comparison")
        print("="*100)
        
        single_split = await conn.fetch("""
            SELECT id, block_number, transaction_hash, type, exchange, trade 
            FROM settlements 
            WHERE type = 'split' AND exchange = 'base'
            AND jsonb_array_length(trade->'position_events') = 1
            ORDER BY block_number ASC
            LIMIT 1
        """)
        
        for row in single_split:
            analyze_settlement(row)
        
        print("\n" + "="*100)
        print("MERGE SETTLEMENT WITH SINGLE POSITION EVENT (Base Exchange) - for comparison")
        print("="*100)
        
        single_merge = await conn.fetch("""
            SELECT id, block_number, transaction_hash, type, exchange, trade 
            FROM settlements 
            WHERE type = 'merge' AND exchange = 'base'
            AND jsonb_array_length(trade->'position_events') = 1
            ORDER BY block_number ASC
            LIMIT 1
        """)
        
        for row in single_merge:
            analyze_settlement(row)

    await pool.close()
    print("\n✅ Analysis complete!")

if __name__ == "__main__":
    asyncio.run(main())
