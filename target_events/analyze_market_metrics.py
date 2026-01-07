import os
import asyncio
import asyncpg
import json
import csv
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

class KalshiOrderBook:
    """Bid-only orderbook for Kalshi (asks are implicit: 1 - opposite_bid)."""
    
    def __init__(self):
        self.yes_bids = {}  # {price_cents: quantity}
        self.no_bids = {}
    
    def clear_all(self):
        """Clear all levels."""
        self.yes_bids.clear()
        self.no_bids.clear()
    
    def update_bid(self, outcome, price_cents, quantity):
        """Update a bid level. If quantity is 0, remove it."""
        book = self.yes_bids if outcome == 'yes' else self.no_bids
        if quantity == 0:
            book.pop(price_cents, None)
        else:
            book[price_cents] = quantity
    
    def apply_delta(self, outcome, price_cents, delta):
        """Apply a delta to an existing level."""
        book = self.yes_bids if outcome == 'yes' else self.no_bids
        current_qty = book.get(price_cents, 0)
        new_qty = current_qty + delta
        if new_qty <= 0:
            book.pop(price_cents, None)
        else:
            book[price_cents] = new_qty
    
    def get_best_bid(self, outcome):
        """Get best bid price (highest) in cents."""
        book = self.yes_bids if outcome == 'yes' else self.no_bids
        if not book:
            return None
        return max(book.keys())
    
    def get_best_ask(self, outcome):
        """Get best ask price (lowest) - derived from opposite side's best bid.
        For YES: ask = 100 - NO_best_bid
        For NO: ask = 100 - YES_best_bid
        """
        opposite_bid = self.get_best_bid('no' if outcome == 'yes' else 'yes')
        if opposite_bid is None:
            return None
        return 100 - opposite_bid
    
    def calculate_slippage(self, outcome, trade_price_cents, trade_quantity):
        """Calculate slippage for a trade.
        For BUY: slippage = (trade_price - best_bid) * quantity
        We assume all trades are at the bid (market orders)
        """
        best_bid = self.get_best_bid(outcome)
        if best_bid is None:
            return None
        # Slippage: how much worse than best available price
        slippage_per_unit = abs(trade_price_cents - best_bid)
        return slippage_per_unit * trade_quantity
    
    def calculate_volume(self, trade_price_cents, trade_quantity):
        """Calculate notional volume: (price_cents / 100) * quantity."""
        return (trade_price_cents / 100.0) * trade_quantity

class PolyOrderBook:
    """Full orderbook for Polymarket (separate bids/asks per asset)."""
    
    def __init__(self):
        self.bids = {}  # {price: quantity}
        self.asks = {}
    
    def clear_all(self):
        """Clear all levels."""
        self.bids.clear()
        self.asks.clear()
    
    def update_level(self, side, price, quantity):
        """Update a single price level."""
        book = self.bids if side == 'bids' else self.asks
        if float(quantity) == 0:
            book.pop(float(price), None)
        else:
            book[float(price)] = float(quantity)
    
    def set_full_book(self, bids_list, asks_list):
        """Set entire book from lists of {price, size}."""
        self.clear_all()
        for level in bids_list:
            price = float(level['price'])
            size = float(level['size'])
            if size > 0:
                self.bids[price] = size
        for level in asks_list:
            price = float(level['price'])
            size = float(level['size'])
            if size > 0:
                self.asks[price] = size
    
    def get_best_bid(self):
        """Get best bid price (highest)."""
        if not self.bids:
            return None
        return max(self.bids.keys())
    
    def get_best_ask(self):
        """Get best ask price (lowest)."""
        if not self.asks:
            return None
        return min(self.asks.keys())
    
    def calculate_slippage(self, trade_price, trade_quantity, trade_side):
        """Calculate slippage for a trade.
        For BUY side: slippage = (trade_price - best_bid) * quantity
        For SELL side: slippage = (best_ask - trade_price) * quantity
        """
        if trade_side.upper() == 'BUY':
            best_bid = self.get_best_bid()
            if best_bid is None:
                return None
            slippage_per_unit = max(0, trade_price - best_bid)
        else:  # SELL
            best_ask = self.get_best_ask()
            if best_ask is None:
                return None
            slippage_per_unit = max(0, best_ask - trade_price)
        return slippage_per_unit * trade_quantity
    
    def calculate_volume(self, trade_price, trade_quantity):
        """Calculate notional volume: price * quantity."""
        return trade_price * trade_quantity

class MarketMetrics:
    """Tracks metrics over time for both YES and NO orderbooks."""
    
    def __init__(self):
        self.yes_volume = 0.0
        self.yes_slippage = 0.0
        self.no_volume = 0.0
        self.no_slippage = 0.0
        self.last_yes_bid = None
        self.last_yes_ask = None
        self.last_no_bid = None
        self.last_no_ask = None
        self.metrics_history = []  # List of (timestamp_us, metrics_dict)
    
    def record_snapshot(self, timestamp_us, yes_bid, yes_ask, no_bid, no_ask):
        """Record a snapshot if best prices have changed."""
        if (yes_bid != self.last_yes_bid or yes_ask != self.last_yes_ask or
            no_bid != self.last_no_bid or no_ask != self.last_no_ask):
            
            # Calculate spreads
            yes_spread = (yes_ask - yes_bid) if (yes_bid and yes_ask) else None
            no_spread = (no_ask - no_bid) if (no_bid and no_ask) else None
            
            # Calculate cross-book discrepancies (should be near 0 in efficient markets)
            # For binary market: YES + NO should equal $1
            bid_sum_disc = None
            ask_sum_disc = None
            if yes_bid is not None and no_bid is not None:
                bid_sum_disc = 1.0 - (yes_bid + no_bid)
            if yes_ask is not None and no_ask is not None:
                ask_sum_disc = 1.0 - (yes_ask + no_ask)
            
            self.metrics_history.append((timestamp_us, {
                'yes_volume': self.yes_volume,
                'yes_slippage': self.yes_slippage,
                'yes_bid': yes_bid,
                'yes_ask': yes_ask,
                'yes_spread': yes_spread,
                'no_volume': self.no_volume,
                'no_slippage': self.no_slippage,
                'no_bid': no_bid,
                'no_ask': no_ask,
                'no_spread': no_spread,
                'bid_sum_discrepancy': bid_sum_disc,
                'ask_sum_discrepancy': ask_sum_disc
            }))
            
            self.last_yes_bid = yes_bid
            self.last_yes_ask = yes_ask
            self.last_no_bid = no_bid
            self.last_no_ask = no_ask
    
    def add_trade(self, timestamp_us, outcome, volume, slippage, yes_bid, yes_ask, no_bid, no_ask):
        """Record a trade and update metrics."""
        if outcome == 'yes':
            self.yes_volume += volume
            if slippage is not None:
                self.yes_slippage += slippage
        else:
            self.no_volume += volume
            if slippage is not None:
                self.no_slippage += slippage
        
        # Always record snapshot after trade
        self.record_snapshot(timestamp_us, yes_bid, yes_ask, no_bid, no_ask)

def construct_kalshi_ticker_pattern(utc_time_str):
    """Construct KXBTC15M-YYMONYDHHMM-MM ticker for 15-minute interval.
    Input is START time in UTC, converts to ET and adds 15 min to get END time for ticker.
    """
    from datetime import timedelta
    
    utc_dt = datetime.strptime(utc_time_str, '%Y-%m-%d %H:%M:%S')
    
    # Convert UTC to ET (EST = UTC-5 in winter)
    if utc_dt.month in [11, 12, 1, 2, 3]:
        et_start_dt = utc_dt - timedelta(hours=5)
    else:
        et_start_dt = utc_dt - timedelta(hours=4)
    
    # Kalshi ticker uses END time of the interval
    et_end_dt = et_start_dt + timedelta(minutes=15)
    
    month_abbr = et_end_dt.strftime('%b').upper()
    minute_suffix = f"{et_end_dt.minute:02d}"
    ticker = f"KXBTC15M-{et_end_dt.strftime('%y')}{month_abbr}{et_end_dt.strftime('%d%H%M')}-{minute_suffix}"
    return ticker

def construct_polymarket_question_pattern(utc_time_str):
    """Construct Polymarket question pattern for 15-minute interval.
    Input is START time in UTC, converts to ET for the question range.
    """
    from datetime import timedelta
    
    utc_dt = datetime.strptime(utc_time_str, '%Y-%m-%d %H:%M:%S')
    
    # Convert UTC to ET (EST = UTC-5 in winter, EDT = UTC-4 in summer)
    if utc_dt.month in [11, 12, 1, 2, 3]:
        et_start_dt = utc_dt - timedelta(hours=5)
    else:
        et_start_dt = utc_dt - timedelta(hours=4)
    
    et_end_dt = et_start_dt + timedelta(minutes=15)
    
    # Format start time
    start_hour = et_start_dt.hour
    start_min = et_start_dt.minute
    start_ampm = 'AM' if start_hour < 12 else 'PM'
    start_hour_12 = start_hour if start_hour <= 12 else start_hour - 12
    if start_hour_12 == 0:
        start_hour_12 = 12
    
    # Format end time
    end_hour = et_end_dt.hour
    end_min = et_end_dt.minute
    end_ampm = 'AM' if end_hour < 12 else 'PM'
    end_hour_12 = end_hour if end_hour <= 12 else end_hour - 12
    if end_hour_12 == 0:
        end_hour_12 = 12
    
    month_day = et_start_dt.strftime('%B %#d' if os.name == 'nt' else '%B %-d')
    question_pattern = f"Bitcoin Up or Down - {month_day}, {start_hour_12}:{start_min:02d}{start_ampm}-{end_hour_12}:{end_min:02d}{end_ampm} ET"
    return question_pattern

async def analyze_kalshi_market(pool, market_ticker):
    """Analyze Kalshi market using event-driven approach with seq ordering."""
    print(f"\nAnalyzing Kalshi market: {market_ticker}")
    
    # Get first snapshot (orderbook_snapshot type, lowest seq)
    snapshot_query = """
        SELECT server_time_us, message, (message->>'seq')::int as seq
        FROM kalshi_orderbook_updates_3
        WHERE market_ticker = $1 AND message->>'type' = 'orderbook_snapshot'
        ORDER BY (message->>'seq')::int
        LIMIT 1
    """
    
    # Get all deltas ordered by seq
    deltas_query = """
        SELECT (message->>'seq')::int as seq, server_time_us, message
        FROM kalshi_orderbook_updates_3
        WHERE market_ticker = $1 AND message->>'type' = 'orderbook_delta'
        ORDER BY (message->>'seq')::int
    """
    
    # Get all trades ordered by server_time
    trades_query = """
        SELECT server_time_us, message
        FROM kalshi_trades_3
        WHERE market_ticker = $1
        ORDER BY server_time_us
    """
    
    snapshot_row = await pool.fetchrow(snapshot_query, market_ticker)
    delta_rows = await pool.fetch(deltas_query, market_ticker)
    trade_rows = await pool.fetch(trades_query, market_ticker)
    
    if not snapshot_row:
        print(f"No snapshot found for {market_ticker}")
        return None
    
    # Initialize orderbook and metrics
    orderbook = KalshiOrderBook()
    metrics = MarketMetrics()
    
    # Process snapshot
    snapshot_msg = json.loads(snapshot_row['message'])
    snapshot_time = snapshot_row['server_time_us']
    
    yes_levels = snapshot_msg.get('msg', {}).get('yes', [])
    no_levels = snapshot_msg.get('msg', {}).get('no', [])
    
    for price_cents, quantity in yes_levels:
        orderbook.update_bid('yes', price_cents, quantity)
    for price_cents, quantity in no_levels:
        orderbook.update_bid('no', price_cents, quantity)
    
    # Record initial state
    yes_bid_cents = orderbook.get_best_bid('yes')
    yes_ask_cents = orderbook.get_best_ask('yes')
    no_bid_cents = orderbook.get_best_bid('no')
    no_ask_cents = orderbook.get_best_ask('no')
    
    # Convert cents to dollars for metrics
    yes_bid = yes_bid_cents / 100.0 if yes_bid_cents else None
    yes_ask = yes_ask_cents / 100.0 if yes_ask_cents else None
    no_bid = no_bid_cents / 100.0 if no_bid_cents else None
    no_ask = no_ask_cents / 100.0 if no_ask_cents else None
    
    metrics.record_snapshot(snapshot_time, yes_bid, yes_ask, no_bid, no_ask)
    
    # Create iterators
    delta_idx = 0
    trade_idx = 0
    
    # Walk through events chronologically
    # Since deltas are ordered by seq and trades by time, we need to interleave carefully
    # Strategy: process all deltas first, then check for trades that happened after each delta
    
    for delta_row in delta_rows:
        delta_msg = json.loads(delta_row['message'])
        delta_time = delta_row['server_time_us']
        
        msg = delta_msg.get('msg', {})
        side = msg.get('side')  # 'yes' or 'no'
        price_cents = int(msg.get('price', 0))
        delta = int(msg.get('delta', 0))
        
        # Apply delta
        orderbook.apply_delta(side, price_cents, delta)
        
        # Check for trades that happened before this delta but after we last checked
        while trade_idx < len(trade_rows) and trade_rows[trade_idx]['server_time_us'] <= delta_time:
            trade_msg = json.loads(trade_rows[trade_idx]['message'])
            trade_time = trade_rows[trade_idx]['server_time_us']
            
            taker_side = trade_msg.get('taker_side')  # which outcome was bought
            count = int(trade_msg.get('count', 0))
            
            # Determine which price was used (buyer pays this)
            if taker_side == 'yes':
                trade_price_cents = int(trade_msg.get('yes_price', 0))
                outcome = 'yes'
            else:
                trade_price_cents = int(trade_msg.get('no_price', 0))
                outcome = 'no'
            
            # Calculate volume and slippage
            volume = orderbook.calculate_volume(trade_price_cents, count)
            slippage = orderbook.calculate_slippage(outcome, trade_price_cents, count)
            
            # Get current best prices in dollars
            yes_bid_cents = orderbook.get_best_bid('yes')
            yes_ask_cents = orderbook.get_best_ask('yes')
            no_bid_cents = orderbook.get_best_bid('no')
            no_ask_cents = orderbook.get_best_ask('no')
            
            yes_bid = yes_bid_cents / 100.0 if yes_bid_cents else None
            yes_ask = yes_ask_cents / 100.0 if yes_ask_cents else None
            no_bid = no_bid_cents / 100.0 if no_bid_cents else None
            no_ask = no_ask_cents / 100.0 if no_ask_cents else None
            
            # Convert slippage to dollars
            slippage_dollars = slippage / 100.0 if slippage else None
            
            metrics.add_trade(trade_time, outcome, volume, slippage_dollars, 
                            yes_bid, yes_ask, no_bid, no_ask)
            
            trade_idx += 1
        
        # Record state change if best prices changed
        yes_bid_cents = orderbook.get_best_bid('yes')
        yes_ask_cents = orderbook.get_best_ask('yes')
        no_bid_cents = orderbook.get_best_bid('no')
        no_ask_cents = orderbook.get_best_ask('no')
        
        yes_bid = yes_bid_cents / 100.0 if yes_bid_cents else None
        yes_ask = yes_ask_cents / 100.0 if yes_ask_cents else None
        no_bid = no_bid_cents / 100.0 if no_bid_cents else None
        no_ask = no_ask_cents / 100.0 if no_ask_cents else None
        
        metrics.record_snapshot(delta_time, yes_bid, yes_ask, no_bid, no_ask)
    
    # Process any remaining trades
    while trade_idx < len(trade_rows):
        trade_msg = json.loads(trade_rows[trade_idx]['message'])
        trade_time = trade_rows[trade_idx]['server_time_us']
        
        taker_side = trade_msg.get('taker_side')
        count = int(trade_msg.get('count', 0))
        
        if taker_side == 'yes':
            trade_price_cents = int(trade_msg.get('yes_price', 0))
            outcome = 'yes'
        else:
            trade_price_cents = int(trade_msg.get('no_price', 0))
            outcome = 'no'
        
        volume = orderbook.calculate_volume(trade_price_cents, count)
        slippage = orderbook.calculate_slippage(outcome, trade_price_cents, count)
        
        yes_bid_cents = orderbook.get_best_bid('yes')
        yes_ask_cents = orderbook.get_best_ask('yes')
        no_bid_cents = orderbook.get_best_bid('no')
        no_ask_cents = orderbook.get_best_ask('no')
        
        yes_bid = yes_bid_cents / 100.0 if yes_bid_cents else None
        yes_ask = yes_ask_cents / 100.0 if yes_ask_cents else None
        no_bid = no_bid_cents / 100.0 if no_bid_cents else None
        no_ask = no_ask_cents / 100.0 if no_ask_cents else None
        
        slippage_dollars = slippage / 100.0 if slippage else None
        
        metrics.add_trade(trade_time, outcome, volume, slippage_dollars,
                        yes_bid, yes_ask, no_bid, no_ask)
        
        trade_idx += 1
    
    print(f"Processed {len(delta_rows)} deltas and {len(trade_rows)} trades")
    print(f"Recorded {len(metrics.metrics_history)} metric snapshots")
    
    return metrics

async def analyze_polymarket_market(pool, question_pattern):
    """Analyze Polymarket market using timestamp ordering (no seq available)."""
    print(f"\nAnalyzing Polymarket market: {question_pattern}")
    
    # Get asset_ids from market - use wildcard search like the working script
    # Extract the key parts for matching
    import re
    match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d+),\s+(\d+:\d+[AP]M)-(\d+:\d+[AP]M)', question_pattern)
    if not match:
        print(f"Could not parse question pattern: {question_pattern}")
        return None
    
    month, day, start_time, end_time = match.groups()
    search_pattern = f"%Bitcoin Up or Down%{month}%{day}%{start_time}%{end_time}%"
    
    asset_query = """
        SELECT message->>'clobTokenIds' as clob_token_ids
        FROM poly_new_market
        WHERE message->>'question' ILIKE $1
        LIMIT 1
    """
    
    asset_row = await pool.fetchrow(asset_query, search_pattern)
    if not asset_row or not asset_row['clob_token_ids']:
        print(f"No market found for pattern: {question_pattern}")
        return None
    
    try:
        asset_ids = json.loads(asset_row['clob_token_ids'])
    except:
        print(f"Could not parse clobTokenIds: {asset_row['clob_token_ids']}")
        return None
    
    if len(asset_ids) != 2:
        print(f"Expected 2 asset_ids, got {len(asset_ids)}")
        return None
    
    yes_asset_id = str(asset_ids[0])
    no_asset_id = str(asset_ids[1])
    
    print(f"YES asset_id: {yes_asset_id}")
    print(f"NO asset_id: {no_asset_id}")
    
    # Get all events for both assets, ordered by timestamp
    # We'll merge them and process chronologically
    
    # For each asset, get books, price_changes, and last_trade_price
    events_query = """
        (SELECT server_time_us, 'book' as event_type, $1 as asset_id, message
         FROM poly_book WHERE asset_id = $1)
        UNION ALL
        (SELECT server_time_us, 'price_change' as event_type, $1 as asset_id, message
         FROM poly_price_change WHERE asset_id = $1)
        UNION ALL
        (SELECT server_time_us, 'last_trade_price' as event_type, $1 as asset_id, message
         FROM poly_last_trade_price WHERE asset_id = $1)
        ORDER BY server_time_us
    """
    
    yes_events = await pool.fetch(events_query, yes_asset_id)
    no_events = await pool.fetch(events_query, no_asset_id)
    
    print(f"YES events: {len(yes_events)}, NO events: {len(no_events)}")
    
    # Initialize orderbooks
    yes_orderbook = PolyOrderBook()
    no_orderbook = PolyOrderBook()
    metrics = MarketMetrics()
    
    # Merge events from both assets, sorted by timestamp
    all_events = []
    for row in yes_events:
        all_events.append(('yes', row))
    for row in no_events:
        all_events.append(('no', row))
    
    all_events.sort(key=lambda x: x[1]['server_time_us'])
    
    # Process events
    for outcome, event_row in all_events:
        event_type = event_row['event_type']
        timestamp_us = event_row['server_time_us']
        msg = json.loads(event_row['message'])
        
        orderbook = yes_orderbook if outcome == 'yes' else no_orderbook
        
        if event_type == 'book':
            # Full book update
            bids = msg.get('bids', [])
            asks = msg.get('asks', [])
            orderbook.set_full_book(bids, asks)
            
            # Record state change
            yes_bid = yes_orderbook.get_best_bid()
            yes_ask = yes_orderbook.get_best_ask()
            no_bid = no_orderbook.get_best_bid()
            no_ask = no_orderbook.get_best_ask()
            
            metrics.record_snapshot(timestamp_us, yes_bid, yes_ask, no_bid, no_ask)
        
        elif event_type == 'price_change':
            # Trade happened - extract info and calculate slippage
            price = float(msg.get('price', 0))
            size = float(msg.get('size', 0))
            side = msg.get('side', 'BUY')
            
            # Calculate volume and slippage
            volume = orderbook.calculate_volume(price, size)
            slippage = orderbook.calculate_slippage(price, size, side)
            
            # Get current best prices
            yes_bid = yes_orderbook.get_best_bid()
            yes_ask = yes_orderbook.get_best_ask()
            no_bid = no_orderbook.get_best_bid()
            no_ask = no_orderbook.get_best_ask()
            
            metrics.add_trade(timestamp_us, outcome, volume, slippage,
                            yes_bid, yes_ask, no_bid, no_ask)
        
        elif event_type == 'last_trade_price':
            # Another trade event - similar to price_change
            price = float(msg.get('price', 0))
            size = float(msg.get('size', 0))
            side = msg.get('side', 'BUY')
            
            volume = orderbook.calculate_volume(price, size)
            slippage = orderbook.calculate_slippage(price, size, side)
            
            yes_bid = yes_orderbook.get_best_bid()
            yes_ask = yes_orderbook.get_best_ask()
            no_bid = no_orderbook.get_best_bid()
            no_ask = no_orderbook.get_best_ask()
            
            metrics.add_trade(timestamp_us, outcome, volume, slippage,
                            yes_bid, yes_ask, no_bid, no_ask)
    
    print(f"Recorded {len(metrics.metrics_history)} metric snapshots")
    
    return metrics

def write_metrics_to_csv(metrics, output_file):
    """Write metrics history to CSV."""
    if not metrics or not metrics.metrics_history:
        print(f"No metrics to write to {output_file}")
        return
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'timestamp_us', 'yes_volume', 'yes_slippage', 'yes_bid', 'yes_ask', 'yes_spread',
            'no_volume', 'no_slippage', 'no_bid', 'no_ask', 'no_spread',
            'bid_sum_discrepancy', 'ask_sum_discrepancy'
        ])
        
        for timestamp_us, m in metrics.metrics_history:
            writer.writerow([
                timestamp_us,
                f"{m['yes_volume']:.2f}" if m['yes_volume'] else '',
                f"{m['yes_slippage']:.4f}" if m['yes_slippage'] else '',
                f"{m['yes_bid']:.4f}" if m['yes_bid'] is not None else '',
                f"{m['yes_ask']:.4f}" if m['yes_ask'] is not None else '',
                f"{m['yes_spread']:.4f}" if m['yes_spread'] is not None else '',
                f"{m['no_volume']:.2f}" if m['no_volume'] else '',
                f"{m['no_slippage']:.4f}" if m['no_slippage'] else '',
                f"{m['no_bid']:.4f}" if m['no_bid'] is not None else '',
                f"{m['no_ask']:.4f}" if m['no_ask'] is not None else '',
                f"{m['no_spread']:.4f}" if m['no_spread'] is not None else '',
                f"{m['bid_sum_discrepancy']:.6f}" if m['bid_sum_discrepancy'] is not None else '',
                f"{m['ask_sum_discrepancy']:.6f}" if m['ask_sum_discrepancy'] is not None else ''
            ])
    
    print(f"Metrics written to {output_file}")

async def main(utc_time_str):
    """Main entry point."""
    pool = await connect_db()
    
    try:
        # Construct patterns
        kalshi_ticker = construct_kalshi_ticker_pattern(utc_time_str)
        poly_question = construct_polymarket_question_pattern(utc_time_str)
        
        print(f"Analyzing markets for UTC time: {utc_time_str}")
        print(f"Kalshi ticker pattern: {kalshi_ticker}")
        print(f"Polymarket question pattern: {poly_question}")
        
        # Analyze both markets
        kalshi_metrics = await analyze_kalshi_market(pool, kalshi_ticker)
        poly_metrics = await analyze_polymarket_market(pool, poly_question)
        
        # Write results
        dt = datetime.strptime(utc_time_str, '%Y-%m-%d %H:%M:%S')
        timestamp_str = dt.strftime('%Y-%m-%d_%H%M%S')
        
        if kalshi_metrics:
            kalshi_output = f"kalshi_metrics_{timestamp_str}.csv"
            write_metrics_to_csv(kalshi_metrics, kalshi_output)
        
        if poly_metrics:
            poly_output = f"poly_metrics_{timestamp_str}.csv"
            write_metrics_to_csv(poly_metrics, poly_output)
        
        print("\nAnalysis complete!")
        
    finally:
        await pool.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python analyze_market_metrics.py '<YYYY-MM-DD HH:MM:SS>'")
        sys.exit(1)
    
    utc_time_str = sys.argv[1]
    asyncio.run(main(utc_time_str))
