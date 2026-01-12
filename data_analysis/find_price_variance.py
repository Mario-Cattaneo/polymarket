import os
import asyncio
import asyncpg
import json
from eth_abi import decode
from collections import defaultdict

PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

EXCHANGE_ADDRESS = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"

def decode_order_filled(data, topics):
    """Decode OrderFilled event"""
    maker = '0x' + topics[1][-40:]
    taker = '0x' + topics[2][-40:]
    
    decoded = decode(
        ['uint256', 'uint256', 'uint256', 'uint256', 'uint256'],
        bytes.fromhex(data[2:])
    )
    
    makerAssetId = str(decoded[0])
    takerAssetId = str(decoded[1])
    making = decoded[2]
    taking = decoded[3]
    fee = decoded[4]
    
    if makerAssetId == "0":
        maker_side = "BUY"
        price = taking / making if making > 0 else 0
    else:
        maker_side = "SELL"
        price = making / taking if taking > 0 else 0
    
    return {
        "maker": maker,
        "taker": taker,
        "makerAssetId": makerAssetId,
        "takerAssetId": takerAssetId,
        "making": making,
        "taking": taking,
        "fee": fee,
        "maker_side": maker_side,
        "price": price
    }

def extract_maker_orders(row):
    """
    Extract maker orders from a settlement row.
    Returns (taker_side, maker_side, maker_orders) where maker_orders is [(price, quantity), ...]
    Returns None if no valid maker orders found.
    """
    trade = row['trade']
    
    # Decode all OrderFilled events
    orders_filled = []
    for i, event_name in enumerate(trade.get('event_names', [])):
        if event_name == 'OrderFilled':
            try:
                order = decode_order_filled(trade['event_datas'][i], trade['event_topics'][i])
                orders_filled.append(order)
            except Exception as e:
                print(f"Error decoding OrderFilled: {e}")
                continue
    
    if not orders_filled:
        return None
    
    # Debug: print all orders
    # print(f"  Found {len(orders_filled)} OrderFilled events")
    # for o in orders_filled:
    #     print(f"    maker={o['maker'][:10]}, taker={o['taker'][:10]}, side={o['maker_side']}, price={o['price']:.6f}")
    
    # Filter out exchange fills (where taker is exchange)
    maker_fills = [o for o in orders_filled if o['taker'].lower() != EXCHANGE_ADDRESS.lower()]
    
    if not maker_fills:
        return None
    
    # Determine taker side from maker side (opposite)
    maker_side = maker_fills[0]['maker_side']
    taker_side = 'SELL' if maker_side == 'BUY' else 'BUY'
    
    # Extract (price, quantity) for each maker order
    maker_orders = []
    for order in maker_fills:
        if maker_side == 'BUY':
            # Maker is buying tokens, paying USDC
            quantity = order['taking']  # tokens received
            price = order['price']
        else:
            # Maker is selling tokens, receiving USDC
            quantity = order['making']  # tokens sold
            price = order['price']
        
        maker_orders.append((price, quantity))
    
    return (taker_side, maker_side, maker_orders)

async def main():
    pool = await asyncpg.create_pool(
        host=PG_HOST,
        port=PG_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    
    async with pool.acquire() as conn:
        # Query settlements with multiple OrderFilled events (potential for price variance)
        # First, let's just get all settlements and filter in Python
        query = """
        SELECT 
            block_number,
            transaction_hash,
            type,
            exchange,
            trade
        FROM settlements
        WHERE exchange = 'base'
        LIMIT 5000
        """
        
        rows = await conn.fetch(query)
        
        print(f"Processing {len(rows)} settlements...")
        
        price_variance_cases = []
        all_prices = []
        num_with_orderfilled = 0
        num_with_maker_fills = 0
        
        for row in rows:
            # Parse trade JSON if it's a string
            trade = row['trade']
            if isinstance(trade, str):
                trade = json.loads(trade)
            
            # Count OrderFilled events
            event_names = trade.get('event_names', [])
            num_orderfilled = sum(1 for e in event_names if e == 'OrderFilled')
            
            # Skip if less than 3 OrderFilled events
            if num_orderfilled < 3:
                continue
            
            num_with_orderfilled += 1
            
            # Create modified row with parsed trade
            row_dict = dict(row)
            row_dict['trade'] = trade
            
            result = extract_maker_orders(row_dict)
            if result is None:
                continue
            
            num_with_maker_fills += 1
            
            taker_side, maker_side, maker_orders = result
            
            # Collect all prices for analysis
            prices = [price for price, _ in maker_orders]
            all_prices.extend(prices)
            unique_prices = set(prices)
            
            if len(unique_prices) > 1:
                # Found price variance!
                price_variance_cases.append({
                    'block_number': row['block_number'],
                    'transaction_hash': row['transaction_hash'],
                    'type': row['type'],
                    'taker_side': taker_side,
                    'maker_side': maker_side,
                    'maker_orders': sorted(maker_orders),  # Sort by price
                    'num_makers': len(maker_orders),
                    'price_range': (min(prices), max(prices)),
                    'price_spread': max(prices) - min(prices)
                })
        
        # Sort by price spread (largest first)
        price_variance_cases.sort(key=lambda x: x['price_spread'], reverse=True)
        
        print(f"Found {len(price_variance_cases)} settlements with varying maker prices")
        print(f"Processed {len(rows)} settlements with 3+ OrderFilled events")
        print(f"Settlements with 3+ OrderFilled: {num_with_orderfilled}")
        print(f"Settlements with valid maker fills: {num_with_maker_fills}")
        print(f"Total prices collected: {len(all_prices)}")
        if all_prices:
            print(f"Price range across all: {min(all_prices):.6f} - {max(all_prices):.6f}")
        print("=" * 100)
        
        # Print top 20 examples
        for i, case in enumerate(price_variance_cases[:20], 1):
            print(f"\n{i}. Block: {case['block_number']}, TX: {case['transaction_hash']}")
            print(f"   Type: {case['type']}, Taker: {case['taker_side']}, Makers: {case['maker_side']}")
            print(f"   Number of makers: {case['num_makers']}")
            print(f"   Price range: {case['price_range'][0]:.6f} - {case['price_range'][1]:.6f}")
            print(f"   Price spread: {case['price_spread']:.6f}")
            print(f"   Maker orders (price, quantity):")
            for price, quantity in case['maker_orders']:
                print(f"      {price:.6f} @ {quantity:,}")
        
        # Statistics
        if price_variance_cases:
            print("\n" + "=" * 100)
            print("STATISTICS:")
            print(f"Average number of makers: {sum(c['num_makers'] for c in price_variance_cases) / len(price_variance_cases):.2f}")
            print(f"Average price spread: {sum(c['price_spread'] for c in price_variance_cases) / len(price_variance_cases):.6f}")
            print(f"Max price spread: {max(c['price_spread'] for c in price_variance_cases):.6f}")
            
            # By type
            by_type = defaultdict(int)
            for case in price_variance_cases:
                by_type[case['type']] += 1
            print("\nBy settlement type:")
            for stype, count in sorted(by_type.items()):
                print(f"  {stype}: {count}")
    
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
