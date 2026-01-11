#!/usr/bin/env python3
import os
import asyncio
import asyncpg
import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from eth_abi import decode
from collections import Counter, defaultdict
from datetime import datetime, timezone

# ==============================================================================
# CONFIGURATION
# ==============================================================================

PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# Batch size for fetching from DB
DB_BATCH_SIZE = 50_000

USDC_DECIMALS = 6

# Time Mapping Constants
REF_BLOCK = 79172085
REF_TS_MS = int(datetime(2025, 11, 18, 7, 0, 1, tzinfo=timezone.utc).timestamp() * 1000)
MS_PER_BLOCK = 2000

# ==============================================================================
# DECODING & EXTRACTION HELPERS
# ==============================================================================

def block_to_time(block_num: int) -> datetime:
    ts_ms = REF_TS_MS + (block_num - REF_BLOCK) * MS_PER_BLOCK
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

def decode_cft_amount(data_hex: str) -> int:
    try:
        if data_hex.startswith('0x'): data_hex = data_hex[2:]
        decoded = decode(['address', 'uint256[]', 'uint256'], bytes.fromhex(data_hex))
        return decoded[2]
    except: return 0

def decode_adapter_amount(event_name: str, data_hex: str) -> int:
    try:
        if data_hex.startswith('0x'): data_hex = data_hex[2:]
        if event_name in ['PositionSplit', 'PositionsMerge']:
            decoded = decode(['address', 'bytes32', 'uint256'], bytes.fromhex(data_hex))
            return decoded[2]
        elif event_name == 'PositionsConverted':
            decoded = decode(['address', 'bytes32', 'uint256', 'uint256'], bytes.fromhex(data_hex))
            return decoded[3]
    except: return 0
    return 0

def get_settlement_volume(trade_data: dict) -> int:
    volume = 0
    for om in trade_data.get('orders_matched', []):
        if isinstance(om, str): om = json.loads(om)
        try:
            data = om.get('data', '')
            if data.startswith('0x'): data = data[2:]
            decoded = decode(['uint256', 'uint256', 'uint256', 'uint256'], bytes.fromhex(data))
            if decoded[0] == 0: volume += decoded[2]
            elif decoded[1] == 0: volume += decoded[3]
        except: continue
    return volume

def get_cft_tx_volume(cft_events: list, adapter_events: list) -> int:
    volume = 0
    if cft_events:
        for event in cft_events:
            volume += decode_cft_amount(event.get('data', ''))
    if adapter_events:
        for event in adapter_events:
            volume += decode_adapter_amount(event.get('event'), event.get('data', ''))
    return volume

def get_unique_makers_takers(trade_data: dict):
    takers = set()
    makers = set()
    for om in trade_data.get('orders_matched', []):
        if isinstance(om, str): om = json.loads(om)
        topics = om.get('topics', [])
        if len(topics) >= 3: takers.add('0x' + topics[2][-40:])
            
    for of in trade_data.get('orders_filled', []):
        if isinstance(of, str): of = json.loads(of)
        topics = of.get('topics', [])
        if len(topics) >= 3:
            maker_addr = '0x' + topics[2][-40:]
            if maker_addr not in takers: makers.add(maker_addr)
            
    return list(makers), list(takers)

def get_cft_tx_stakeholders(cft_events: list, adapter_events: list) -> list:
    """
    Extracts stakeholders with a hierarchy: Adapter events are the source of truth.
    """
    stakeholders = set()
    
    # HIERARCHY: If adapter events exist, they are the source of truth for the stakeholder.
    if adapter_events:
        for event in adapter_events:
            topics = event.get('topics', [])
            # Stakeholder is always topic 1 for these events
            if len(topics) > 1:
                stakeholders.add('0x' + topics[1][-40:])
    
    # Fallback: If no adapter events, use the stakeholder from the CFT events.
    elif cft_events:
        for event in cft_events:
            topics = event.get('topics', [])
            if len(topics) > 1:
                stakeholders.add('0x' + topics[1][-40:])
                
    return list(stakeholders)

# ==============================================================================
# MAIN
# ==============================================================================

async def main():
    print(f"Connecting to {DB_NAME}...")
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)

    ts_data = defaultdict(list)
    stats = defaultdict(lambda: {
        'vol': 0.0, 'tx': 0, 'fills': 0,
        'makers': Counter(), 'takers': Counter(), 'stakeholders': Counter()
    })

    async with pool.acquire() as conn:
        # 1. Process Settlements
        print("Processing Settlements...")
        cursor_id = 0
        while True:
            rows = await conn.fetch("""
                SELECT id, block_number, type, exchange, trade FROM settlements 
                WHERE id > $1 ORDER BY id ASC LIMIT $2
            """, cursor_id, DB_BATCH_SIZE)
            
            if not rows: break
            
            for r in rows:
                cursor_id = r['id']
                exch_label = "Base" if r['exchange'] == 'base' else "NegRisk"
                cat = f"{r['type'].capitalize()} ({exch_label})"
                trade = json.loads(r['trade'])
                
                vol = get_settlement_volume(trade) / 10**USDC_DECIMALS
                fills = len(trade.get('orders_filled', []))
                makers, takers = get_unique_makers_takers(trade)
                time = block_to_time(r['block_number'])
                
                stats[cat]['vol'] += vol
                stats[cat]['tx'] += 1
                stats[cat]['fills'] += fills
                stats[cat]['makers'].update(makers)
                stats[cat]['takers'].update(takers)
                ts_data[cat].append({'time': time, 'vol': vol})
            
            print(f"  Processed up to ID {cursor_id}...", end='\r')

        # 2. Process Standalone CFT
        print("\nProcessing Standalone CFT Events...")
        cursor_id = 0
        while True:
            rows = await conn.fetch("""
                SELECT id, block_number, type, cft_events, negrisk_adapter_events 
                FROM CFT_no_exchange WHERE id > $1 ORDER BY id ASC LIMIT $2
            """, cursor_id, DB_BATCH_SIZE)
            
            if not rows: break
            
            for r in rows:
                cursor_id = r['id']
                cat = r['type'].replace('_', ' ').capitalize()
                
                cft_events = json.loads(r['cft_events']) if r['cft_events'] else []
                adapter_events = json.loads(r['negrisk_adapter_events']) if r['negrisk_adapter_events'] else []
                
                vol = get_cft_tx_volume(cft_events, adapter_events) / 10**USDC_DECIMALS
                
                # UPDATED LOGIC HERE
                stakeholders = get_cft_tx_stakeholders(cft_events, adapter_events)
                
                time = block_to_time(r['block_number'])
                
                stats[cat]['vol'] += vol
                stats[cat]['tx'] += 1
                stats[cat]['stakeholders'].update(stakeholders)
                ts_data[cat].append({'time': time, 'vol': vol})
            
            print(f"  Processed up to ID {cursor_id}...", end='\r')

    await pool.close()

    # ==============================================================================
    # OUTPUT 1: STATISTICS TABLE
    # ==============================================================================
    print("\n" + "="*145)
    print(f"{'CATEGORY':<30} | {'VOL (USDC)':<16} | {'TXs':<8} | {'FILLS':<8} | {'UNIQ MAKERS':<12} | {'UNIQ TAKERS':<12} | {'TOTAL UNIQ':<12}")
    print("="*145)
    
    for cat, s in sorted(stats.items()):
        n_makers = len(s['makers'])
        n_takers = len(s['takers'])
        all_actors = set(s['makers'].keys()) | set(s['takers'].keys()) | set(s['stakeholders'].keys())
        n_total = len(all_actors)
        
        is_trade = n_makers > 0 or n_takers > 0
        
        if not is_trade:
            print(f"{cat:<30} | {s['vol']:>16,.0f} | {s['tx']:<8} | {'-':<8} | {'-':<12} | {'-':<12} | {n_total:<12}")
        else:
            print(f"{cat:<30} | {s['vol']:>16,.0f} | {s['tx']:<8} | {s['fills']:<8} | {n_makers:<12} | {n_takers:<12} | {n_total:<12}")

    # ==============================================================================
    # OUTPUT 2: TOP 10 LISTS
    # ==============================================================================
    print("\n" + "="*100)
    print("TOP 10 ACTORS (Separated by Role)")
    print("="*100)
    
    for cat, s in sorted(stats.items()):
        print(f"\n>>> {cat} <<<")
        
        is_trade = len(s['makers']) > 0 or len(s['takers']) > 0
        
        if not is_trade:
            print("  [Top Stakeholders]")
            for actor, count in s['stakeholders'].most_common(10):
                print(f"    {actor}: {count}")
        else:
            print("  [Top Makers]")
            for actor, count in s['makers'].most_common(10):
                print(f"    {actor}: {count}")
            print("\n  [Top Takers]")
            for actor, count in s['takers'].most_common(10):
                print(f"    {actor}: {count}")

    # ==============================================================================
    # OUTPUT 3: PLOT CUMULATIVE VOLUME
    # ==============================================================================
    print("\nGenerating Volume Plot...")
    plt.figure(figsize=(14, 8))
    for cat, records in sorted(ts_data.items()):
        if not records: continue
        df = pd.DataFrame(records).sort_values('time')
        df['cum_vol'] = df['vol'].cumsum()
        plt.plot(df['time'], df['cum_vol'], label=cat)
    
    plt.title('Cumulative USDC Volume by Category & Exchange')
    plt.xlabel('Date')
    plt.ylabel('Volume (USDC)')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig('analysis_volume_detailed.png')
    print("Saved analysis_volume_detailed.png")

    # ==============================================================================
    # OUTPUT 4: PLOT CDF (Lorenz Curve)
    # ==============================================================================
    print("Generating Actor CDF Plot...")
    plt.figure(figsize=(14, 8))
    
    for cat, s in sorted(stats.items()):
        combined_counts = s['makers'] + s['takers'] + s['stakeholders']
        if not combined_counts: continue
        
        frequencies = sorted(combined_counts.values())
        y_values = np.cumsum(frequencies)
        y_values = y_values / y_values[-1] 
        x_values = np.linspace(0, 1, len(y_values))
        
        plt.plot(x_values, y_values, label=f"{cat} (n={len(frequencies)})", linewidth=2)

    plt.title('Activity Concentration (Lorenz Curve)')
    plt.xlabel('Percentile of Actors (Sorted by Activity)')
    plt.ylabel('Cumulative Share of Total Activity')
    plt.plot([0, 1], [0, 1], 'k--', alpha=0.3, label='Perfect Equality')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig('analysis_actor_cdf_detailed.png')
    print("Saved analysis_actor_cdf_detailed.png")

if __name__ == "__main__":
    asyncio.run(main())