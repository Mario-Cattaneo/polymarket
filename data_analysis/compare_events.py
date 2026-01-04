import asyncio
import csv
import sys
import os
from typing import Dict, Set, Tuple
import orjson

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'orderbook_collection'))

from db_cli import DatabaseManager

BATCH_SIZE = 1000
CSV_PATH = os.path.expandvars("$POLY_CSV/gamma_markets.csv")

async def main():
    # Initialize database manager using same env vars as start_collection
    db_man = DatabaseManager(
        db_host=os.getenv("PG_SOCKET", "localhost"),
        db_port=int(os.getenv("POLY_PG_PORT", "5432")),
        db_name=os.getenv("POLY_DB", "polymarket"),
        db_user=os.getenv("POLY_DB_CLI", "mario_cattaneo"),
        db_pass=os.getenv("POLY_DB_CLI_PASS", "")
    )
    await db_man.connect()
    
    try:
        # Load markets from CSV
        csv_markets: Dict[Tuple[str, str], Dict] = {}  # (id, conditionId) -> market_data
        
        print(f"Loading markets from {CSV_PATH}...")
        with open(CSV_PATH, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                market_id = row.get('id')
                condition_id = row.get('conditionId')
                closed = row.get('closed', '').lower() in ['true', '1', 'yes']
                
                if market_id and condition_id:
                    csv_markets[(market_id, condition_id)] = {
                        'market_id': market_id,
                        'condition_id': condition_id,
                        'closed': closed
                    }
        
        print(f"Loaded {len(csv_markets)} markets from CSV")
        
        # Extract markets from events table in batches
        db_markets: Dict[Tuple[str, str], Dict] = {}  # (id, conditionId) -> market_data
        offset = 0
        processed_events = 0
        
        print("Scanning events table...")
        while True:
            task = db_man.fetch(
                f"fetch_events_batch_{offset}",
                f"SELECT event_id, message FROM events ORDER BY event_id LIMIT {BATCH_SIZE} OFFSET {offset};"
            )
            rows = await task
            
            if not rows:
                break
            
            for row in rows:
                try:
                    message = orjson.loads(row["message"]) if isinstance(row["message"], str) else row["message"]
                    markets = message.get("markets", [])
                    
                    for market in markets:
                        market_id = market.get("id")
                        condition_id = market.get("conditionId")
                        closed = market.get("closed", False)
                        
                        if market_id and condition_id:
                            key = (market_id, condition_id)
                            # Keep the market data, preferring the latest (last one seen)
                            db_markets[key] = {
                                'market_id': market_id,
                                'condition_id': condition_id,
                                'closed': closed,
                                'event_id': row.get('event_id')
                            }
                except Exception as e:
                    print(f"ERROR: Failed to parse event {row.get('event_id')}: {e}", file=sys.stderr)
            
            processed_events += len(rows)
            print(f"Processed {processed_events} events, found {len(db_markets)} unique markets...", end='\r')
            offset += BATCH_SIZE
        
        print(f"\nFound {len(db_markets)} unique markets in events table")
        
        # Compare
        csv_keys = set(csv_markets.keys())
        db_keys = set(db_markets.keys())
        
        # Markets found in both
        matched = csv_keys & db_keys
        matched_count = len(matched)
        
        # Markets in CSV but missing in events
        missing_in_events = csv_keys - db_keys
        missing_count = len(missing_in_events)
        
        # Check closed flag mismatches
        closed_mismatches = []
        for key in matched:
            csv_closed = csv_markets[key]['closed']
            db_closed = db_markets[key]['closed']
            if csv_closed != db_closed:
                closed_mismatches.append({
                    'market_id': key[0],
                    'condition_id': key[1],
                    'csv_closed': csv_closed,
                    'db_closed': db_closed,
                    'event_id': db_markets[key].get('event_id')
                })
        
        mismatch_count = len(closed_mismatches)
        
        # Print results
        print("\n" + "="*80)
        print("COMPARISON RESULTS")
        print("="*80)
        print(f"Markets in CSV: {len(csv_markets)}")
        print(f"Unique markets in events table: {len(db_markets)}")
        print(f"Markets found in both (matched): {matched_count}")
        print(f"Markets in CSV but missing in events: {missing_count}")
        print(f"Matched markets with mismatched closed flag: {mismatch_count}")
        print("="*80)
        
        # Print details of missing markets
        if missing_in_events:
            print(f"\nMarkets missing in events table (first 20):")
            for i, key in enumerate(sorted(missing_in_events)[:20]):
                market = csv_markets[key]
                print(f"  {market['market_id']} (condition: {market['condition_id'][:16]}..., closed: {market['closed']})")
            if len(missing_in_events) > 20:
                print(f"  ... and {len(missing_in_events) - 20} more")
        
        # Print details of closed mismatches
        if closed_mismatches:
            print(f"\nMarkets with mismatched closed flag (first 20):")
            for i, mismatch in enumerate(sorted(closed_mismatches, key=lambda x: x['market_id'])[:20]):
                print(f"  {mismatch['market_id']}: CSV={mismatch['csv_closed']}, DB={mismatch['db_closed']}")
            if len(closed_mismatches) > 20:
                print(f"  ... and {len(closed_mismatches) - 20} more")
        
        print("\n")
        
    finally:
        await db_man.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
