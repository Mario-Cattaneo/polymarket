#!/usr/bin/env python3
import os
import asyncio
import asyncpg
import json
import pandas as pd
import matplotlib.pyplot as plt
from eth_abi import decode
from collections import Counter, defaultdict
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================

PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# Batch size: 12 hours in milliseconds
T_BATCH_RANGE = 12 * 60 * 60 * 1000 
USDC_DECIMALS = 6

# ==============================================================================
# DECODING LOGIC
# ==============================================================================

def decode_cft_amount(data_hex: str) -> int:
    """
    Decodes the 'amount' from PositionSplit/PositionsMerge.
    Correct ABI: (address collateralToken, uint256[] partition, uint256 amount)
    We want the 3rd element.
    """
    try:
        if data_hex.startswith('0x'): data_hex = data_hex[2:]
        data_bytes = bytes.fromhex(data_hex)
        decoded = decode(['address', 'uint256[]', 'uint256'], data_bytes)
        return decoded[2]
    except Exception:
        return 0

def get_settlement_volume(trade_data: dict) -> int:
    """
    Calculates USDC volume from orders_matched.
    Checks for Asset ID 0 (USDC) in either maker or taker side.
    """
    volume = 0
    oms = trade_data.get('orders_matched', [])
    for om in oms:
        if isinstance(om, str): om = json.loads(om)
        data = om.get('data', '')
        if not data: continue
        try:
            if data.startswith('0x'): data = data[2:]
            # ABI: makerAssetId, takerAssetId, making, taking
            decoded = decode(['uint256', 'uint256', 'uint256', 'uint256'], bytes.fromhex(data))
            makerAssetId, takerAssetId, making, taking = decoded
            
            if makerAssetId == 0: volume += making
            elif takerAssetId == 0: volume += taking
        except: continue
    return volume

def get_actors_from_settlement(trade_data: dict):
    """Extracts makers and takers from orders_filled."""
    actors = []
    ofs = trade_data.get('orders_filled', [])
    for of in ofs:
        if isinstance(of, str): of = json.loads(of)
        topics = of.get('topics', [])
        if len(topics) >= 4:
            # topics[2] = maker, topics[3] = taker
            actors.append('0x' + topics[2][-40:])
            actors.append('0x' + topics[3][-40:])
    return actors

def get_stakeholder_from_cft(event_data: dict):
    """Extracts stakeholder from PositionSplit/Merge topics."""
    topics = event_data.get('topics', [])
    if len(topics) > 1:
        return ['0x' + topics[1][-40:]]
    return []

# ==============================================================================
# MAIN ANALYSIS
# ==============================================================================

async def main():
    print(f"Connecting to {DB_NAME}...")
    pool = await asyncpg.create_pool(
        host=PG_HOST, port=PG_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS
    )

    # 1. Get Global Time Bounds
    print("Determining time range...")
    async with pool.acquire() as conn:
        # We query min/max from both tables to find the absolute start and end
        row_s = await conn.fetchrow("SELECT MIN(timestamp_ms), MAX(timestamp_ms) FROM settlements")
        row_c = await conn.fetchrow("SELECT MIN(timestamp_ms), MAX(timestamp_ms) FROM CFT_no_exchange")
        
        mins = [x for x in [row_s[0], row_c[0]] if x]
        maxs = [x for x in [row_s[1], row_c[1]] if x]
        
        if not mins:
            print("No data found.")
            return

        start_ms = min(mins)
        end_ms = max(maxs)
        print(f"Range: {datetime.fromtimestamp(start_ms/1000)} to {datetime.fromtimestamp(end_ms/1000)}")

    # 2. Initialize Data Structures
    # We will store time/vol pairs for plotting
    plot_data = {
        'Complementary Trade': [],
        'Split Trade': [],
        'Merge Trade': [],
        'Split (Standalone)': [],
        'Merge (Standalone)': []
    }
    
    # We will store aggregate stats
    stats = {k: {'vol': 0.0, 'tx_count': 0, 'fill_count': 0, 'actors': Counter()} for k in plot_data.keys()}

    # 3. Batch Processing
    current_ms = start_ms
    total_batches = ((end_ms - start_ms) // T_BATCH_RANGE) + 1
    batch_idx = 0

    async with pool.acquire() as conn:
        while current_ms <= end_ms:
            next_ms = current_ms + T_BATCH_RANGE
            batch_idx += 1
            print(f"Processing Batch {batch_idx}/{total_batches}...", end='\r')

            # --- CASE 1: Complementary Trades ---
            rows = await conn.fetch("""
                SELECT timestamp_ms, trade FROM settlements 
                WHERE type = 'complementary' AND timestamp_ms >= $1 AND timestamp_ms < $2
                ORDER BY timestamp_ms ASC
            """, current_ms, next_ms)
            
            for r in rows:
                trade = json.loads(r['trade']) if isinstance(r['trade'], str) else r['trade']
                vol = get_settlement_volume(trade) / 10**USDC_DECIMALS
                fills = len(trade.get('orders_filled', []))
                actors = get_actors_from_settlement(trade)
                
                stats['Complementary Trade']['vol'] += vol
                stats['Complementary Trade']['tx_count'] += 1
                stats['Complementary Trade']['fill_count'] += fills
                stats['Complementary Trade']['actors'].update(actors)
                plot_data['Complementary Trade'].append({'time': r['timestamp_ms'], 'vol': vol})

            # --- CASE 2: Split Trades ---
            rows = await conn.fetch("""
                SELECT timestamp_ms, trade FROM settlements 
                WHERE type = 'split' AND timestamp_ms >= $1 AND timestamp_ms < $2
                ORDER BY timestamp_ms ASC
            """, current_ms, next_ms)

            for r in rows:
                trade = json.loads(r['trade']) if isinstance(r['trade'], str) else r['trade']
                vol = get_settlement_volume(trade) / 10**USDC_DECIMALS
                fills = len(trade.get('orders_filled', []))
                actors = get_actors_from_settlement(trade)

                stats['Split Trade']['vol'] += vol
                stats['Split Trade']['tx_count'] += 1
                stats['Split Trade']['fill_count'] += fills
                stats['Split Trade']['actors'].update(actors)
                plot_data['Split Trade'].append({'time': r['timestamp_ms'], 'vol': vol})

            # --- CASE 3: Merge Trades ---
            rows = await conn.fetch("""
                SELECT timestamp_ms, trade FROM settlements 
                WHERE type = 'merge' AND timestamp_ms >= $1 AND timestamp_ms < $2
                ORDER BY timestamp_ms ASC
            """, current_ms, next_ms)

            for r in rows:
                trade = json.loads(r['trade']) if isinstance(r['trade'], str) else r['trade']
                vol = get_settlement_volume(trade) / 10**USDC_DECIMALS
                fills = len(trade.get('orders_filled', []))
                actors = get_actors_from_settlement(trade)

                stats['Merge Trade']['vol'] += vol
                stats['Merge Trade']['tx_count'] += 1
                stats['Merge Trade']['fill_count'] += fills
                stats['Merge Trade']['actors'].update(actors)
                plot_data['Merge Trade'].append({'time': r['timestamp_ms'], 'vol': vol})

            # --- CASE 4: Split (Standalone) ---
            rows = await conn.fetch("""
                SELECT timestamp_ms, event_data FROM CFT_no_exchange 
                WHERE event_name = 'PositionSplit' AND timestamp_ms >= $1 AND timestamp_ms < $2
                ORDER BY timestamp_ms ASC
            """, current_ms, next_ms)

            for r in rows:
                data = json.loads(r['event_data']) if isinstance(r['event_data'], str) else r['event_data']
                vol = decode_cft_amount(data.get('data', '')) / 10**USDC_DECIMALS
                actors = get_stakeholder_from_cft(data)

                stats['Split (Standalone)']['vol'] += vol
                stats['Split (Standalone)']['tx_count'] += 1
                stats['Split (Standalone)']['actors'].update(actors)
                plot_data['Split (Standalone)'].append({'time': r['timestamp_ms'], 'vol': vol})

            # --- CASE 5: Merge (Standalone) ---
            rows = await conn.fetch("""
                SELECT timestamp_ms, event_data FROM CFT_no_exchange 
                WHERE event_name = 'PositionsMerge' AND timestamp_ms >= $1 AND timestamp_ms < $2
                ORDER BY timestamp_ms ASC
            """, current_ms, next_ms)

            for r in rows:
                data = json.loads(r['event_data']) if isinstance(r['event_data'], str) else r['event_data']
                vol = decode_cft_amount(data.get('data', '')) / 10**USDC_DECIMALS
                actors = get_stakeholder_from_cft(data)

                stats['Merge (Standalone)']['vol'] += vol
                stats['Merge (Standalone)']['tx_count'] += 1
                stats['Merge (Standalone)']['actors'].update(actors)
                plot_data['Merge (Standalone)'].append({'time': r['timestamp_ms'], 'vol': vol})

            current_ms = next_ms

    print(f"\nProcessing complete.")
    await pool.close()

    # ==============================================================================
    # OUTPUT: STATISTICS
    # ==============================================================================
    
    print("\n" + "="*110)
    print(f"{'CATEGORY':<22} | {'TOTAL VOL (USDC)':<18} | {'TX COUNT':<10} | {'FILLS':<10} | {'(FILLS-TX)/TX':<15}")
    print("="*110)
    
    for cat, s in stats.items():
        # Metric: (Total Orders Filled - Total Transactions) / Total Transactions
        # This shows the "extra" liquidity fragmentation per trade.
        # For standalone events, fills is 0, so this will be -1.0 (which makes sense, no orders filled).
        metric = 0.0
        if s['tx_count'] > 0:
            metric = (s['fill_count'] - s['tx_count']) / s['tx_count']
            
        print(f"{cat:<22} | {s['vol']:,.2f}".ljust(43) + 
              f" | {s['tx_count']:<10} | {s['fill_count']:<10} | {metric:.4f}")

    print("\n" + "="*110)
    print("TOP 10 ACTORS (Makers/Takers for Trades, Stakeholders for Standalone)")
    print("="*110)
    
    for cat, s in stats.items():
        print(f"\n--- {cat} ---")
        top = s['actors'].most_common(10)
        if not top: print("  (None)")
        for actor, count in top:
            print(f"  {actor}: {count}")

    # ==============================================================================
    # OUTPUT: PLOTTING
    # ==============================================================================
    print("\nGenerating Plot...")
    plt.figure(figsize=(12, 8))
    
    for cat, records in plot_data.items():
        if not records: continue
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        # Convert timestamp_ms to datetime objects for plotting
        df['datetime'] = pd.to_datetime(df['time'], unit='ms')
        df = df.sort_values('datetime')
        
        # Cumulative Sum
        df['cum_vol'] = df['vol'].cumsum()
        
        plt.plot(df['datetime'], df['cum_vol'], label=cat, linewidth=1.5)

    plt.title('Cumulative USDC Volume by Type')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Volume (USDC)')
    plt.legend()
    plt.grid(True, which='both', linestyle='--', alpha=0.5)
    plt.tight_layout()
    
    filename = 'cumulative_volume_5_cases.png'
    plt.savefig(filename)
    print(f"Plot saved to {filename}")

if __name__ == "__main__":
    asyncio.run(main())