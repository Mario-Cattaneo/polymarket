import os
import asyncio
import asyncpg
import json
from datetime import datetime
from collections import defaultdict
from decimal import Decimal

# --- Environment Variable Setup ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

async def connect_db():
    """Creates a connection pool to the database."""
    return await asyncpg.create_pool(
        host=PG_HOST,
        port=int(PG_PORT),
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        min_size=1,
        max_size=5
    )

class OrderBook:
    """Maintains orderbook state for a binary market (YES/NO)."""
    
    def __init__(self):
        # For Polymarket: separate bids/asks per outcome
        self.yes_bids = {}  # {price: quantity}
        self.yes_asks = {}
        self.no_bids = {}
        self.no_asks = {}
    
    def clear_book(self, outcome):
        """Clear all levels for an outcome."""
        getattr(self, f"{outcome}_bids").clear()
        getattr(self, f"{outcome}_asks").clear()
    
    def update_level(self, side, outcome, price, quantity):
        """Update a single price level. If quantity is 0, remove the level."""
        book = getattr(self, f"{outcome}_{side}")
        if quantity == 0:
            book.pop(price, None)
        else:
            book[price] = quantity
    
    def apply_delta(self, side, outcome, price, delta):
        """Apply a delta to a price level."""
        book = getattr(self, f"{outcome}_{side}")
        current = book.get(price, 0)
        new_qty = current + delta
        if new_qty <= 0:
            book.pop(price, None)
        else:
            book[price] = new_qty
    
    def get_best_bid(self, outcome):
        """Get best bid price for an outcome."""
        book = getattr(self, f"{outcome}_bids")
        return max(book.keys()) if book else None
    
    def get_best_ask(self, outcome):
        """Get best ask price for an outcome."""
        book = getattr(self, f"{outcome}_asks")
        return min(book.keys()) if book else None
    
    def get_spread(self, outcome):
        """Get spread for an outcome (ask - bid)."""
        best_bid = self.get_best_bid(outcome)
        best_ask = self.get_best_ask(outcome)
        if best_bid is not None and best_ask is not None:
            return best_ask - best_bid
        return None
    
    def calculate_slippage(self, side, outcome, avg_fill_price, quantity):
        """
        Calculate slippage as the cost beyond best available price.
        For a buy: slippage = (avg_fill_price - best_ask) * quantity
        For a sell: slippage = (best_bid - avg_fill_price) * quantity
        Positive slippage means worse execution than best available.
        """
        if side == "buy":
            best_price = self.get_best_ask(outcome)
            if best_price is not None:
                return (avg_fill_price - best_price) * quantity
        else:  # sell
            best_price = self.get_best_bid(outcome)
            if best_price is not None:
                return (best_price - avg_fill_price) * quantity
        return 0


class KalshiOrderBook:
    """Special orderbook for Kalshi where only bids are shown, asks are implicit."""
    
    def __init__(self):
        # Kalshi only shows bids (buy orders) for each outcome
        self.yes_bids = {}  # {price: quantity}
        self.no_bids = {}
    
    def clear_all(self):
        """Clear all levels."""
        self.yes_bids.clear()
        self.no_bids.clear()
    
    def update_bid(self, outcome, price, quantity):
        """Update a bid level."""
        book = getattr(self, f"{outcome}_bids")
        if quantity == 0:
            book.pop(price, None)
        else:
            book[price] = quantity
    
    def apply_delta(self, outcome, price, delta):
        """Apply a delta to a bid level."""
        book = getattr(self, f"{outcome}_bids")
        current = book.get(price, 0)
        new_qty = current + delta
        if new_qty <= 0:
            book.pop(price, None)
        else:
            book[price] = new_qty
    
    def get_best_bid(self, outcome):
        """Get best bid for an outcome."""
        book = getattr(self, f"{outcome}_bids")
        return max(book.keys()) if book else None
    
    def get_best_ask(self, outcome):
        """
        Get best ask for an outcome (implied from opposite side).
        Ask to sell YES = 1 - best bid for NO
        Ask to sell NO = 1 - best bid for YES
        """
        opposite = "no" if outcome == "yes" else "yes"
        opposite_bid = self.get_best_bid(opposite)
        if opposite_bid is not None:
            return 1.0 - opposite_bid
        return None
    
    def get_spread(self, outcome):
        """Get spread for an outcome."""
        best_bid = self.get_best_bid(outcome)
        best_ask = self.get_best_ask(outcome)
        if best_bid is not None and best_ask is not None:
            return best_ask - best_bid
        return None
    
    def calculate_slippage(self, outcome, fill_price, quantity):
        """
        Calculate slippage for buying an outcome.
        In Kalshi trades, taker is always buying, so compare to best ask.
        """
        best_ask = self.get_best_ask(outcome)
        if best_ask is not None:
            return (fill_price - best_ask) * quantity
        return 0

class MarketMetrics:
    """Tracks market metrics over time."""
    
    def __init__(self, use_kalshi=False):
        if use_kalshi:
            self.orderbook = KalshiOrderBook()
        else:
            self.orderbook = OrderBook()
        # Separate tracking for YES and NO orderbooks
        self.yes_volume = 0
        self.yes_slippage = 0
        self.no_volume = 0
        self.no_slippage = 0
        self.events = []  # List of (timestamp, metrics_dict)
    
    def record_snapshot(self, timestamp):
        """Record current market state."""
        yes_bid = self.orderbook.get_best_bid("yes")
        yes_ask = self.orderbook.get_best_ask("yes")
        no_bid = self.orderbook.get_best_bid("no")
        no_ask = self.orderbook.get_best_ask("no")
        
        yes_spread = self.orderbook.get_spread("yes")
        no_spread = self.orderbook.get_spread("no")
        
        metrics = {
            'timestamp_us': timestamp,
            # YES orderbook metrics
            'yes_volume': self.yes_volume,
            'yes_slippage': self.yes_slippage,
            'yes_best_bid': yes_bid,
            'yes_best_ask': yes_ask,
            'yes_spread': yes_spread,
            # NO orderbook metrics
            'no_volume': self.no_volume,
            'no_slippage': self.no_slippage,
            'no_best_bid': no_bid,
            'no_best_ask': no_ask,
            'no_spread': no_spread,
        }
        
        # Price discrepancies (should be 0 in perfect market)
        if yes_bid is not None and no_bid is not None:
            metrics['bid_sum_discrepancy'] = 1.0 - (yes_bid + no_bid)
        else:
            metrics['bid_sum_discrepancy'] = None
        
        if yes_ask is not None and no_ask is not None:
            metrics['ask_sum_discrepancy'] = 1.0 - (yes_ask + no_ask)
        else:
            metrics['ask_sum_discrepancy'] = None
        
        self.events.append((timestamp, metrics))

async def analyze_kalshi_market(pool, market_ticker):
    """Analyze a Kalshi market."""
    print(f"\n{'='*80}")
    print(f"ANALYZING KALSHI MARKET: {market_ticker}")
    print(f"{'='*80}\n")
    
    metrics = MarketMetrics(use_kalshi=True)
    
    async with pool.acquire() as conn:
        # Get all orderbook updates
        orderbook_updates = await conn.fetch(
            """SELECT server_time_us, message FROM kalshi_orderbook_updates_3 
               WHERE market_ticker = $1 ORDER BY server_time_us ASC""",
            market_ticker
        )
        
        # Get all trades
        trades = await conn.fetch(
            """SELECT server_time_us, message FROM kalshi_trades_3 
               WHERE market_ticker = $1 ORDER BY server_time_us ASC""",
            market_ticker
        )
    
    # Merge and sort all events
    all_events = []
    for row in orderbook_updates:
        msg = json.loads(row['message'])
        all_events.append((row['server_time_us'], 'orderbook', msg))
    
    for row in trades:
        msg = json.loads(row['message'])
        all_events.append((row['server_time_us'], 'trade', msg))
    
    all_events.sort(key=lambda x: x[0])
    
    print(f"Processing {len(all_events)} events ({len(trades)} trades, {len(orderbook_updates)} orderbook updates)")
    
    # Process events
    for timestamp, event_type, msg in all_events:
        if event_type == 'orderbook':
            if msg.get('type') == 'orderbook_snapshot':
                # Full snapshot - rebuild orderbook
                snapshot_msg = msg.get('msg', {})
                
                metrics.orderbook.clear_all()
                
                # YES bids
                for price_cents, qty in snapshot_msg.get('yes', []):
                    price = price_cents / 100.0
                    metrics.orderbook.update_bid('yes', price, qty)
                
                # NO bids
                for price_cents, qty in snapshot_msg.get('no', []):
                    price = price_cents / 100.0
                    metrics.orderbook.update_bid('no', price, qty)
                
            elif msg.get('type') == 'orderbook_delta':
                delta_msg = msg.get('msg', {})
                outcome = delta_msg.get('side', '').lower()  # 'yes' or 'no'
                price = delta_msg.get('price', 0) / 100.0
                delta = delta_msg.get('delta', 0)
                
                metrics.orderbook.apply_delta(outcome, price, delta)
        
        elif event_type == 'trade':
            # Trade execution
            yes_price = msg.get('yes_price', 0) / 100.0
            no_price = msg.get('no_price', 0) / 100.0
            count = msg.get('count', 0)
            taker_side = msg.get('taker_side', '').lower()  # 'yes' or 'no'
            
            # Volume and slippage tracked separately per outcome
            if taker_side == 'yes':
                # Taker bought YES at yes_price
                metrics.yes_volume += count
                slippage = metrics.orderbook.calculate_slippage('yes', yes_price, count)
                metrics.yes_slippage += slippage
            else:
                # Taker bought NO at no_price
                metrics.no_volume += count
                slippage = metrics.orderbook.calculate_slippage('no', no_price, count)
                metrics.no_slippage += slippage
        
        # Record snapshot after each event
        metrics.record_snapshot(timestamp)
    
    return metrics

async def analyze_polymarket_market(pool, market_id, asset_ids):
    """Analyze a Polymarket market."""
    print(f"\n{'='*80}")
    print(f"ANALYZING POLYMARKET MARKET: {market_id}")
    print(f"Asset IDs: {asset_ids}")
    print(f"{'='*80}\n")
    
    metrics = MarketMetrics()
    
    # Assume asset_ids[0] is YES, asset_ids[1] is NO (or vice versa)
    # We'll determine this from the data
    asset_map = {}  # Will map asset_id to 'yes' or 'no'
    
    async with pool.acquire() as conn:
        # Get all events for both assets
        all_events = []
        
        for asset_id in asset_ids:
            # Books
            books = await conn.fetch(
                """SELECT server_time_us, message FROM poly_book 
                   WHERE asset_id = $1 ORDER BY server_time_us ASC""",
                asset_id
            )
            for row in books:
                msg = json.loads(row['message'])
                all_events.append((row['server_time_us'], 'book', asset_id, msg))
            
            # Trades
            trades = await conn.fetch(
                """SELECT server_time_us, message FROM poly_last_trade_price 
                   WHERE asset_id = $1 ORDER BY server_time_us ASC""",
                asset_id
            )
            for row in trades:
                msg = json.loads(row['message'])
                all_events.append((row['server_time_us'], 'trade', asset_id, msg))
            
            # Price changes
            price_changes = await conn.fetch(
                """SELECT server_time_us, message FROM poly_price_change 
                   WHERE asset_id = $1 ORDER BY server_time_us ASC""",
                asset_id
            )
            for row in price_changes:
                msg = json.loads(row['message'])
                all_events.append((row['server_time_us'], 'price_change', asset_id, msg))
    
    all_events.sort(key=lambda x: x[0])
    
    # Heuristic: The asset with lower average price is likely NO (or the less favorable outcome)
    # For now, let's just assign based on order
    if len(asset_ids) >= 2:
        asset_map[asset_ids[0]] = 'yes'
        asset_map[asset_ids[1]] = 'no'
    
    print(f"Processing {len(all_events)} events")
    
    # Process events
    for timestamp, event_type, asset_id, msg in all_events:
        outcome = asset_map.get(asset_id, 'yes')
        
        if event_type == 'book':
            # Full book snapshot - rebuild this outcome's orderbook
            metrics.orderbook.clear_book(outcome)
            
            for bid in msg.get('bids', []):
                price = float(bid['price'])
                size = float(bid['size'])
                metrics.orderbook.update_level('bids', outcome, price, size)
            
            for ask in msg.get('asks', []):
                price = float(ask['price'])
                size = float(ask['size'])
                metrics.orderbook.update_level('asks', outcome, price, size)
        
        elif event_type == 'price_change':
            # Price change updates a specific level
            side = msg.get('side', '').upper()
            price = float(msg.get('price', 0))
            size = float(msg.get('size', 0))
            
            if side == 'BUY':
                metrics.orderbook.update_level('bids', outcome, price, size)
            else:
                metrics.orderbook.update_level('asks', outcome, price, size)
        
        elif event_type == 'trade':
            # Trade execution
            price = float(msg.get('price', 0))
            size = float(msg.get('size', 0))
            side = msg.get('side', '').upper()
            
            # Volume and slippage tracked separately per outcome
            if side == 'BUY':
                metrics.__dict__[f'{outcome}_volume'] += size
                slippage = metrics.orderbook.calculate_slippage('buy', outcome, price, size)
                metrics.__dict__[f'{outcome}_slippage'] += slippage
            else:
                metrics.__dict__[f'{outcome}_volume'] += size
                slippage = metrics.orderbook.calculate_slippage('sell', outcome, price, size)
                metrics.__dict__[f'{outcome}_slippage'] += slippage
        
        # Record snapshot
        metrics.record_snapshot(timestamp)
    
    return metrics

def print_metrics_summary(metrics, exchange_name):
    """Print summary statistics."""
    print(f"\n{'='*80}")
    print(f"{exchange_name} METRICS SUMMARY")
    print(f"{'='*80}")
    
    if not metrics.events:
        print("No events recorded")
        return
    
    print(f"Total Events: {len(metrics.events)}")
    
    # Final state
    _, final = metrics.events[-1]
    
    print(f"\n=== YES ORDERBOOK ===")
    print(f"  Volume: {final['yes_volume']:.2f}")
    print(f"  Slippage: {final['yes_slippage']:.4f}")
    print(f"  Best Bid: {final['yes_best_bid']}")
    print(f"  Best Ask: {final['yes_best_ask']}")
    print(f"  Spread: {final['yes_spread']}")
    
    print(f"\n=== NO ORDERBOOK ===")
    print(f"  Volume: {final['no_volume']:.2f}")
    print(f"  Slippage: {final['no_slippage']:.4f}")
    print(f"  Best Bid: {final['no_best_bid']}")
    print(f"  Best Ask: {final['no_best_ask']}")
    print(f"  Spread: {final['no_spread']}")
    
    print(f"\n=== CROSS-BOOK METRICS ===")
    print(f"  Bid Sum Discrepancy (1 - yes_bid - no_bid): {final['bid_sum_discrepancy']}")
    print(f"  Ask Sum Discrepancy (1 - yes_ask - no_ask): {final['ask_sum_discrepancy']}")
    
    # Calculate average discrepancies
    bid_discrepancies = [m['bid_sum_discrepancy'] for _, m in metrics.events if m['bid_sum_discrepancy'] is not None]
    ask_discrepancies = [m['ask_sum_discrepancy'] for _, m in metrics.events if m['ask_sum_discrepancy'] is not None]
    
    if bid_discrepancies:
        print(f"\n  Average Bid Sum Discrepancy: {sum(bid_discrepancies)/len(bid_discrepancies):.6f}")
    if ask_discrepancies:
        print(f"  Average Ask Sum Discrepancy: {sum(ask_discrepancies)/len(ask_discrepancies):.6f}")

async def main():
    """Main analysis function."""
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python analyze_market_metrics.py 'YYYY-MM-DD HH:MM:SS'")
        print("Example: python analyze_market_metrics.py '2026-01-06 18:00:00'")
        return
    
    utc_time_str = sys.argv[1]
    
    pool = await connect_db()
    try:
        # Import the query function to get market data
        from query_combined_market_data import get_combined_market_data
        
        combined_data = await get_combined_market_data(pool, utc_time_str)
        
        if not combined_data:
            print("Failed to retrieve market data")
            return
        
        # Analyze Kalshi
        if combined_data['kalshi']:
            kalshi_data = combined_data['kalshi']
            kalshi_metrics = await analyze_kalshi_market(pool, kalshi_data['market_ticker'])
            print_metrics_summary(kalshi_metrics, "KALSHI")
            
            # Save to CSV
            with open(f"kalshi_metrics_{utc_time_str.replace(' ', '_').replace(':', '')}.csv", 'w') as f:
                f.write("timestamp_us,yes_volume,yes_slippage,yes_bid,yes_ask,yes_spread,no_volume,no_slippage,no_bid,no_ask,no_spread,bid_disc,ask_disc\n")
                for ts, m in kalshi_metrics.events:
                    f.write(f"{ts},{m['yes_volume']},{m['yes_slippage']:.6f},")
                    f.write(f"{m['yes_best_bid']},{m['yes_best_ask']},{m['yes_spread']},")
                    f.write(f"{m['no_volume']},{m['no_slippage']:.6f},")
                    f.write(f"{m['no_best_bid']},{m['no_best_ask']},{m['no_spread']},")
                    f.write(f"{m['bid_sum_discrepancy']},{m['ask_sum_discrepancy']}\n")
        
        # Analyze Polymarket
        if combined_data['polymarket']:
            poly_data = combined_data['polymarket']
            poly_metrics = await analyze_polymarket_market(pool, poly_data['market_id'], poly_data['asset_ids'])
            print_metrics_summary(poly_metrics, "POLYMARKET")
            
            # Save to CSV
            with open(f"polymarket_metrics_{utc_time_str.replace(' ', '_').replace(':', '')}.csv", 'w') as f:
                f.write("timestamp_us,yes_volume,yes_slippage,yes_bid,yes_ask,yes_spread,no_volume,no_slippage,no_bid,no_ask,no_spread,bid_disc,ask_disc\n")
                for ts, m in poly_metrics.events:
                    f.write(f"{ts},{m['yes_volume']},{m['yes_slippage']:.6f},")
                    f.write(f"{m['yes_best_bid']},{m['yes_best_ask']},{m['yes_spread']},")
                    f.write(f"{m['no_volume']},{m['no_slippage']:.6f},")
                    f.write(f"{m['no_best_bid']},{m['no_best_ask']},{m['no_spread']},")
                    f.write(f"{m['bid_sum_discrepancy']},{m['ask_sum_discrepancy']}\n")
    
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
