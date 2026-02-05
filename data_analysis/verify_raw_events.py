#!/usr/bin/env python3
"""
Verify raw events from the event tables for a specific transaction hash.
"""

import os
import asyncio
import asyncpg
import json

PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

async def main():
    conn = await asyncpg.connect(
        host=PG_HOST,
        port=int(PG_PORT),
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )
    
    tx_hashes = [
        '0xb31e4beb82438a7508da21475a3f3a9799d483bf51805ba89341a74cc1d19000',
        '0x609410b8492e7a144ca784ce143fe7de91cd3ca57ce62510f6288710ea19b4c4',
    ]
    
    for tx_hash in tx_hashes:
        print(f"\n{'='*80}")
        print(f"TX: {tx_hash}")
        print(f"{'='*80}")
        
        # Check events_neg_risk_exchange
        neg_events = await conn.fetch(
            "SELECT event_name, topics, data FROM events_neg_risk_exchange WHERE transaction_hash = $1",
            tx_hash
        )
        
        print(f"\nevents_neg_risk_exchange ({len(neg_events)} events):")
        for i, evt in enumerate(neg_events, 1):
            print(f"\n  Event #{i}: {evt['event_name']}")
            print(f"    Topics[0] (sig): {evt['topics'][0] if evt['topics'] else 'N/A'}")
            print(f"    Data: {evt['data'][:100]}...")
        
        # Check settlements table
        settlement = await conn.fetchrow(
            "SELECT type, exchange, trade FROM settlements WHERE transaction_hash = $1",
            tx_hash
        )
        
        if settlement:
            trade = json.loads(settlement['trade'])
            print(f"\nSettlements table:")
            print(f"  Type: {settlement['type']}")
            print(f"  Exchange: {settlement['exchange']}")
            print(f"  OrderFilled events in trade JSON: {len(trade.get('orders_filled', []))}")
            
            for i, fill in enumerate(trade.get('orders_filled', []), 1):
                print(f"\n    OrderFilled #{i}:")
                print(f"      Topics[0] (sig): {fill['topics'][0] if fill['topics'] else 'N/A'}")
                print(f"      Data: {fill['data'][:100]}...")
        else:
            print("\nNOT in settlements table")
    
    await conn.close()

if __name__ == '__main__':
    asyncio.run(main())
