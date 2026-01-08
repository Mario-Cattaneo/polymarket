import os
import asyncio
import asyncpg
import json
from datetime import datetime, timedelta
import re
import pandas as pd
from collections import OrderedDict
import logging

# Setup debug logging
logging.basicConfig(
    filename='debug.log',
    level=logging.DEBUG,
    format='%(message)s',
    filemode='w'
)

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

def utc_to_et(utc_dt):
    """Convert UTC datetime to ET (UTC-5)."""
    return utc_dt - timedelta(hours=5)

def format_et_time(et_dt):
    """Format ET datetime as HH:MMAM/PM."""
    hour = et_dt.hour
    minute = et_dt.minute
    ampm = "AM" if hour < 12 else "PM"
    if hour > 12:
        hour -= 12
    elif hour == 0:
        hour = 12
    return f"{hour}:{minute:02d}{ampm}"

def construct_kalshi_ticker_pattern(et_start_dt):
    """Construct Kalshi ticker for this 15-min window (uses end time)."""
    et_end_dt = et_start_dt + timedelta(minutes=15)
    year_short = et_end_dt.strftime("%y")
    month_abbr = et_end_dt.strftime("%b").upper()
    day = et_end_dt.day
    hour = et_end_dt.hour
    minute = et_end_dt.minute
    ticker_start = f"KXBTC15M-{year_short}{month_abbr}{day:02d}{hour:02d}{minute:02d}"
    return ticker_start

class KalshiMarketProcessor:
    def __init__(self, market_ticker):
        self.market_ticker = market_ticker
        self.order_book = {'yes': OrderedDict(), 'no': OrderedDict()}
        self.volume_changes = []
        self.best_price_changes = []
        self.last_best_prices = {'yes_price': None, 'no_price': None}
        self.event_counter = 0

    def get_best_prices(self):
        """Returns the best YES and NO prices from the independent order books."""
        yes_prices = self.order_book['yes']
        no_prices = self.order_book['no']

        best_yes_price = max(yes_prices) if yes_prices else None
        best_no_price = max(no_prices) if no_prices else None
        
        return {
            'yes_price': best_yes_price,
            'no_price': best_no_price
        }

    def get_best_quantities(self):
        """Get quantities at best prices."""
        best = self.get_best_prices()
        return {
            'yes_qty': self.order_book['yes'].get(best['yes_price'], None) if best['yes_price'] else None,
            'no_qty': self.order_book['no'].get(best['no_price'], None) if best['no_price'] else None,
        }

    def log_best_prices(self, timestamp, event_num, event_type, seq=None):
        """Log the current best prices."""
        best = self.get_best_prices()
        qty = self.get_best_quantities()
        seq_str = f"seq={seq}" if seq is not None else ""
        logging.debug(f"  Event #{event_num} ({event_type}) @ {timestamp} {seq_str}")
        yes_price_str = f"{best['yes_price']:>3}" if best['yes_price'] is not None else "  -"
        no_price_str = f"{best['no_price']:>3}" if best['no_price'] is not None else "  -"
        yes_qty = qty['yes_qty'] if qty['yes_qty'] is not None else 0
        no_qty = qty['no_qty'] if qty['no_qty'] is not None else 0
        logging.debug(f"    Best YES price: {yes_price_str} (qty={yes_qty}) | Best NO price: {no_price_str} (qty={no_qty})")

    def track_best_price_change(self, timestamp):
        """Checks for and records changes in the best prices."""
        current_best_prices = self.get_best_prices()
        if current_best_prices != self.last_best_prices:
            change_record = {'timestamp': timestamp, **current_best_prices}
            self.best_price_changes.append(change_record)
            self.last_best_prices = current_best_prices

    def apply_snapshot(self, snapshot_msg, timestamp, seq=None):
        """Initializes the order book from a snapshot."""
        self.event_counter += 1
        # Handle cases where 'yes' or 'no' keys might be missing
        yes_data = snapshot_msg.get('yes', {})
        no_data = snapshot_msg.get('no', {})
        
        self.order_book['yes'] = OrderedDict(sorted(yes_data.items())) if yes_data else OrderedDict()
        self.order_book['no'] = OrderedDict(sorted(no_data.items())) if no_data else OrderedDict()
        
        logging.debug(f"\nEvent #{self.event_counter} - ORDERBOOK_SNAPSHOT @ {timestamp} [seq={seq}]")
        logging.debug(f"  YES book: {dict(self.order_book['yes'])}")
        logging.debug(f"  NO book: {dict(self.order_book['no'])}")
        
        for price, quantity in self.order_book['yes'].items():
            self.volume_changes.append({
                'timestamp': timestamp, 'event_type': 'snapshot', 'side': 'yes',
                'price': price, 'quantity_change': quantity
            })
        for price, quantity in self.order_book['no'].items():
            self.volume_changes.append({
                'timestamp': timestamp, 'event_type': 'snapshot', 'side': 'no',
                'price': price, 'quantity_change': quantity
            })
        self.track_best_price_change(timestamp)

    def apply_delta(self, delta_msg, timestamp, seq=None):
        """Applies a delta update to the order book."""
        self.event_counter += 1
        side = delta_msg['side']
        price = delta_msg['price']
        delta = delta_msg['delta']
        
        current_quantity = self.order_book[side].get(price, 0)
        new_quantity = current_quantity + delta
        
        logging.debug(f"\nEvent #{self.event_counter} - ORDERBOOK_DELTA @ {timestamp} [seq={seq}]")
        logging.debug(f"  Side: {side}, Price: {price}, Delta: {delta}")
        logging.debug(f"  Before: qty={current_quantity}, After: qty={new_quantity}")
        
        if new_quantity > 0:
            self.order_book[side][price] = new_quantity
        else:
            if price in self.order_book[side]:
                del self.order_book[side][price]
        
        self.order_book[side] = OrderedDict(sorted(self.order_book[side].items()))

        self.volume_changes.append({
            'timestamp': timestamp, 'event_type': 'orderbook_update', 'side': side,
            'price': price, 'quantity_change': delta
        })
        self.track_best_price_change(timestamp)
        self.log_best_prices(timestamp, self.event_counter, 'DELTA', seq)

    def apply_trade(self, trade_msg, timestamp, seq=None):
        """Logs a trade without modifying the order book (deltas handle that)."""
        self.event_counter += 1
        taker_side = trade_msg['taker_side']
        yes_price = trade_msg['yes_price']
        quantity = trade_msg['count']

        logging.debug(f"\nEvent #{self.event_counter} - TRADE @ {timestamp} [seq={seq}]")
        logging.debug(f"  Taker side: {taker_side}, Yes price: {yes_price}, Count: {quantity}, Trade ID: {trade_msg.get('trade_id', 'N/A')}")

        self.volume_changes.append({
            'timestamp': timestamp, 'event_type': 'trade', 'side': trade_msg['taker_side'],
            'price': trade_msg['yes_price'], 'quantity_change': -quantity
        })
        self.log_best_prices(timestamp, self.event_counter, 'TRADE', seq)

    def process_events(self, events):
        """Processes events grouped by second with deltas before trades."""
        logging.debug("=" * 100)
        logging.debug(f"PROCESSING {len(events)} EVENTS")
        logging.debug("Sorted by (second, is_trade, seq) - deltas grouped before trades within each second")
        logging.debug("=" * 100)
        
        for event in events:
            timestamp = event['server_time_us']
            event_type = event['type']
            message = event['data']
            seq = event.get('seq')

            if event_type == 'orderbook_snapshot':
                self.apply_snapshot(message['msg'], timestamp, seq)
            elif event_type == 'orderbook_delta':
                self.apply_delta(message['msg'], timestamp, seq)
            elif event_type == 'trade':
                # For trades, extract the nested msg field
                self.apply_trade(message['msg'], timestamp, seq)

    def to_csv(self):
        """Saves the recorded changes to CSV files."""
        if self.volume_changes:
            df_volume = pd.DataFrame(self.volume_changes)
            df_volume.to_csv(f"{self.market_ticker}_volume_changes.csv", index=False)
            print(f"\n✓ Volume changes saved to {self.market_ticker}_volume_changes.csv")

        if self.best_price_changes:
            df_prices = pd.DataFrame(self.best_price_changes)
            df_prices.to_csv(f"{self.market_ticker}_best_price_changes.csv", index=False)
            print(f"✓ Best price changes saved to {self.market_ticker}_best_price_changes.csv")


async def get_and_process_kalshi_data(pool, utc_time_str):
    """Fetches, processes, and analyzes Kalshi data."""
    try:
        utc_dt = datetime.strptime(utc_time_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        print(f"Invalid UTC time format. Use: YYYY-MM-DD HH:MM:SS")
        return None

    et_dt = utc_to_et(utc_dt)
    ticker_pattern = construct_kalshi_ticker_pattern(et_dt)

    print(f"\n=== KALSHI DATA PROCESSING ===")
    print(f"Matching Ticker Pattern: {ticker_pattern}*")

    async with pool.acquire() as conn:
        markets = await conn.fetch(
            "SELECT market_ticker FROM kalshi_markets_3 WHERE market_ticker LIKE $1 LIMIT 1",
            ticker_pattern + '%'
        )

        if not markets:
            print("❌ No matching Kalshi markets found")
            return

        market_ticker = markets[0]['market_ticker']
        print(f"✓ Found Market Ticker: {market_ticker}")

        # 1. Fetch data
        orderbook_updates_raw = await conn.fetch(
            "SELECT server_time_us, message FROM kalshi_orderbook_updates_3 WHERE market_ticker = $1",
            market_ticker
        )
        trades_raw = await conn.fetch(
            "SELECT server_time_us, message FROM kalshi_trades_3 WHERE market_ticker = $1",
            market_ticker
        )

        # 2. Parse orderbook updates and trades, adding seq field
        orderbook_updates = []
        for row in orderbook_updates_raw:
            msg = json.loads(row['message'])
            orderbook_updates.append({
                'server_time_us': row['server_time_us'],
                'seq': msg.get('seq'),
                'type': msg.get('type'),
                'data': msg
            })

        trades = []
        for row in trades_raw:
            msg = json.loads(row['message'])
            trades.append({
                'server_time_us': row['server_time_us'],
                'seq': msg.get('seq'),
                'type': 'trade',
                'data': msg
            })

        print(f"✓ Parsed {len(orderbook_updates)} orderbook updates and {len(trades)} trades.")

        # 3. Create a combined event timeline sorted by (time_in_seconds, is_trade, seq)
        # This groups events by second and ensures deltas come before trades within each second
        all_events = orderbook_updates + trades
        all_events.sort(key=lambda x: (x['server_time_us'] // 1_000_000, x['type'] == 'trade', x['seq'] if x['seq'] is not None else 0))
        print(f"✓ Created a combined timeline of {len(all_events)} events.")
        print(f"  Sorted by (second, is_trade, seq) to group deltas before trades within each second.")

        # Log first 30 events for debugging
        logging.debug("\n" + "=" * 100)
        logging.debug("FIRST 30 EVENTS IN PROCESSING ORDER")
        logging.debug("=" * 100)
        for i, event in enumerate(all_events[:30]):
            logging.debug(f"{i:4d}. {event['type']:20s} @ {event['server_time_us']:16d} [seq={event['seq']}]")
        if len(all_events) > 30:
            logging.debug(f"... ({len(all_events) - 30} more events)")

        # 4. Process events
        processor = KalshiMarketProcessor(market_ticker)
        processor.process_events(all_events)
        
        # 5. Save to CSV
        processor.to_csv()

        # 6. Check for trade/orderbook update correlation
        print("\n--- Trade and Order Book Update Analysis ---")
        for i, event in enumerate(all_events):
            if event['type'] == 'trade':
                trade_time = event['server_time_us']
                trade_msg = event['data']['msg']
                
                # Determine which side was hit
                taker_side = trade_msg['taker_side']
                yes_price = trade_msg['yes_price']
                if taker_side == 'yes':
                    side_hit = 'yes'
                    price_on_book = yes_price
                else:
                    side_hit = 'no'
                    price_on_book = 100 - yes_price
                
                # Look for a nearby orderbook_delta
                for j in range(max(0, i - 5), min(len(all_events), i + 5)):
                    if i == j: continue
                    
                    other_event = all_events[j]
                    if other_event['type'] == 'orderbook_delta':
                        time_diff = abs(trade_time - other_event['server_time_us'])
                        if time_diff < 1_000_000: # 1 second
                            delta_msg = other_event['data']['msg']
                            
                            # Check if the delta corresponds to the trade
                            is_side_match = delta_msg['side'] == side_hit
                            is_price_match = delta_msg['price'] == price_on_book
                            is_quantity_match = abs(delta_msg['delta']) == trade_msg['count']

                            if is_side_match and is_price_match and is_quantity_match:
                                print(f"Trade at {trade_time} appears to have a corresponding order book delta.")
                                print(f"  - Trade: Taker='{taker_side}', Yes_price={yes_price}, Count={trade_msg['count']}")
                                print(f"  - Delta: Side='{delta_msg['side']}', Price={delta_msg['price']}, Delta={delta_msg['delta']}")
                                break


async def main():
    """Main function to query and process market data."""
    test_utc_time = "2026-01-07 09:30:00"
    
    pool = await connect_db()
    try:
        print(f"Testing with UTC time: {test_utc_time}\n")
        await get_and_process_kalshi_data(pool, test_utc_time)
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())