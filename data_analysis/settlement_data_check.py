#!/usr/bin/env python3
import os
import asyncio
import asyncpg
import json

# ==============================================================================
# CONFIGURATION
# ==============================================================================

PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# The two actors
ACTOR_1 = "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296"
ACTOR_2 = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"

# Targets
TARGETS = {
    'Split': {ACTOR_1: [], ACTOR_2: []},
    'Merge': {ACTOR_1: [], ACTOR_2: []}
}
REQUIRED_COUNT = 2

# ==============================================================================
# HELPERS
# ==============================================================================

def get_stakeholder(event_data):
    if isinstance(event_data, str): event_data = json.loads(event_data)
    topics = event_data.get('topics', [])
    if len(topics) > 1: return '0x' + topics[1][-40:]
    return None

async def check_raw_events(conn, tx_hash):
    """
    Queries raw tables for Timestamp AND Block Number.
    """
    # We now fetch block_number as well
    q_ctf = "SELECT event_name, timestamp_ms, block_number FROM events_ctf_exchange WHERE transaction_hash = $1"
    q_neg = "SELECT event_name, timestamp_ms, block_number FROM events_neg_risk_exchange WHERE transaction_hash = $1"
    q_cft = "SELECT event_name, timestamp_ms, block_number FROM events_conditional_tokens WHERE transaction_hash = $1"
    
    ctf_rows = await conn.fetch(q_ctf, tx_hash)
    nr_rows = await conn.fetch(q_neg, tx_hash)
    cond_rows = await conn.fetch(q_cft, tx_hash)
    
    all_events = ctf_rows + nr_rows + cond_rows
    
    print(f"    🔍 Analysis for {tx_hash}:")
    if not all_events:
        print("       ❌ No events found in raw tables.")
        return

    # Print Table
    print(f"       {'Event Name':<20} | {'Block #':<10} | {'Timestamp (ms)':<15}")
    print(f"       {'-'*20}-+-{'-'*10}-+-{'-'*15}")
    
    blocks = set()
    timestamps = set()
    
    for r in all_events:
        print(f"       {r['event_name']:<20} | {r['block_number']:<10} | {r['timestamp_ms']:<15}")
        blocks.add(r['block_number'])
        timestamps.add(r['timestamp_ms'])

    # LOGIC CHECK
    print(f"       {'-'*48}")
    
    if len(blocks) == 1 and len(timestamps) > 1:
        print("       🚨 DIAGNOSIS: INTERPOLATION ERROR")
        print("           Events are in the SAME BLOCK but have DIFFERENT TIMESTAMPS.")
        print("           Your interpolation logic is inconsistent across tables.")
    elif len(blocks) > 1:
        print("       ⁉️  DIAGNOSIS: CRITICAL CHAIN ERROR")
        print("           Events with the same Transaction Hash are in DIFFERENT BLOCKS.")
        print("           (This is theoretically impossible on EVM chains).")
    elif len(blocks) == 1 and len(timestamps) == 1:
        print("       ✅ DIAGNOSIS: Data is consistent.")
        print("           If build_settlements.py failed, it might be a logic bug in the builder.")

# ==============================================================================
# MAIN
# ==============================================================================

async def main():
    print(f"Connecting to {DB_NAME}...")
    pool = await asyncpg.create_pool(
        host=PG_HOST, port=PG_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS
    )

    async with pool.acquire() as conn:
        print("Scanning CFT_no_exchange...")
        
        async with conn.transaction():
            async for row in conn.cursor("SELECT transaction_hash, event_name, event_data FROM CFT_no_exchange"):
                
                # Check completion
                done = True
                for e_type in TARGETS:
                    for actor in TARGETS[e_type]:
                        if len(TARGETS[e_type][actor]) < REQUIRED_COUNT: done = False
                if done: break

                tx_hash = row['transaction_hash']
                event_name = row['event_name']
                type_key = 'Split' if event_name == 'PositionSplit' else 'Merge'
                
                stakeholder = get_stakeholder(row['event_data'])
                if not stakeholder: continue
                
                if stakeholder in TARGETS[type_key]:
                    if len(TARGETS[type_key][stakeholder]) < REQUIRED_COUNT:
                        if tx_hash not in TARGETS[type_key][stakeholder]:
                            TARGETS[type_key][stakeholder].append(tx_hash)
                            print(f"\n  [FOUND] {type_key} | Actor: {stakeholder[:10]}... | Tx: {tx_hash}")
                            await check_raw_events(conn, tx_hash)

    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())