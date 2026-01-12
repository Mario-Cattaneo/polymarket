#!/usr/bin/env python3
"""
Trade Analysis with Slippage Computation
=========================================
Tracks:
- Slippage per category (6 settlement categories)
- Volume per taker and maker
- Trade counts per taker and maker
- Cumulative plots over time
- Lorenz curves for distribution analysis
"""
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
    """Get USDC volume from OrdersMatched events."""
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

def get_cft_tx_stakeholders(cft_events: list, adapter_events: list) -> list:
    stakeholders = set()
    if adapter_events:
        for event in adapter_events:
            topics = event.get('topics', [])
            if len(topics) > 1:
                stakeholders.add('0x' + topics[1][-40:])
    elif cft_events:
        for event in cft_events:
            topics = event.get('topics', [])
            if len(topics) > 1:
                stakeholders.add('0x' + topics[1][-40:])
    return list(stakeholders)

def decode_order_filled(of_data: dict) -> dict:
    """
    Decode an OrderFilled event and compute the price.
    Returns: {maker, taker, maker_side, making, taking, price, usdc_amount, token_amount}
    """
    try:
        topics = of_data.get('topics', [])
        data = of_data.get('data', '')
        if data.startswith('0x'): data = data[2:]
        
        decoded = decode(['uint256', 'uint256', 'uint256', 'uint256', 'uint256'], bytes.fromhex(data))
        
        maker = '0x' + topics[2][-40:] if len(topics) > 2 else None
        taker = '0x' + topics[3][-40:] if len(topics) > 3 else None
        maker_asset_id = decoded[0]
        taker_asset_id = decoded[1]
        making = int(decoded[2])
        taking = int(decoded[3])
        
        # Determine side and price
        if taker_asset_id == 0:
            # Maker SELLING tokens (gives tokens, receives USDC)
            side = 'SELL'
            price = taking / making if making > 0 else 0
            usdc_amount = taking
            token_amount = making
        elif maker_asset_id == 0:
            # Maker BUYING tokens (gives USDC, receives tokens)
            side = 'BUY'
            price = making / taking if taking > 0 else 0
            usdc_amount = making
            token_amount = taking
        else:
            side = 'UNKNOWN'
            price = 0
            usdc_amount = 0
            token_amount = 0
        
        return {
            'maker': maker, 'taker': taker, 'maker_side': side,
            'making': making, 'taking': taking, 'price': price,
            'usdc_amount': usdc_amount, 'token_amount': token_amount
        }
    except:
        return None

def compute_slippage(trade_data: dict, settlement_type: str, debug_info: dict = None) -> tuple:
    """
    Compute slippage for the taker in a settlement.
    
    Returns: (taker_address, slippage_usdc, taker_usdc_volume, maker_volumes: dict)
    
    IMPORTANT: 
    - Filter out the exchange OrderFilled (where maker == taker, i.e., taker acting as maker vs exchange)
    - Determine taker direction from maker_side of real fills:
      * Makers BUY → Taker SELL (wants highest price, slippage = fills below best)
      * Makers SELL → Taker BUY (wants lowest price, slippage = fills above best)
    
    4 Cases:
    1) SPLIT with makers BUY: taker sells tokens, best = max price, slippage = sum of (best - price) * tokens
    2) MERGE with makers SELL: taker buys tokens, best = min price, slippage = sum of (price - best) * tokens
    3) COMPLEMENTARY with makers BUY: taker sells tokens, best = max price, slippage = sum of (best - price) * tokens
    4) COMPLEMENTARY with makers SELL: taker buys tokens, best = min price, slippage = sum of (price - best) * tokens
    """
    orders_filled = trade_data.get('orders_filled', [])
    if not orders_filled:
        return None, 0, 0, {}, None
    
    # Get the taker from OrdersMatched (takerOrderMaker field or decoded from topics)
    orders_matched = trade_data.get('orders_matched', [])
    if not orders_matched:
        return None, 0, 0, {}, None
    
    om = orders_matched[0]
    if isinstance(om, str): om = json.loads(om)
    
    # Try to get taker from decoded field first, then from topics
    taker_addr = om.get('takerOrderMaker')
    if not taker_addr:
        om_topics = om.get('topics', [])
        if len(om_topics) >= 3:
            taker_addr = '0x' + om_topics[2][-40:]
    
    if not taker_addr:
        return None, 0, 0, {}, None
    
    taker_addr = taker_addr.lower()
    
    # Decode OrderFilled events, EXCLUDING the exchange fill (where maker == taker_addr)
    # The exchange fill is where the taker acts as maker against the exchange contract
    fills = []
    maker_volumes = defaultdict(float)
    
    for of in orders_filled:
        if isinstance(of, str): of = json.loads(of)
        
        # Get maker address (from decoded field or decode it)
        maker = of.get('maker')
        if not maker:
            decoded = decode_order_filled(of)
            if decoded:
                maker = decoded['maker']
        
        if not maker:
            continue
        
        maker = maker.lower()
        
        # Skip the exchange fill (where maker == taker_addr)
        if maker == taker_addr:
            continue
        
        # Decode the full event data
        decoded = decode_order_filled(of) if 'maker_side' not in of else of
        if not decoded:
            continue
        
        # Use pre-decoded fields if available
        if 'maker_side' in of:
            decoded = {
                'maker': of['maker'].lower(),
                'taker': of.get('taker', '').lower(),
                'maker_side': of['maker_side'],
                'making': int(of.get('making', 0)),
                'taking': int(of.get('taking', 0)),
                'price': float(of.get('price', 0)),
                'usdc_amount': 0,
                'token_amount': 0
            }
            # Calculate usdc/token amounts from making/taking based on side
            if decoded['maker_side'] == 'SELL':
                decoded['token_amount'] = decoded['making']
                decoded['usdc_amount'] = decoded['taking']
            else:  # BUY
                decoded['usdc_amount'] = decoded['making']
                decoded['token_amount'] = decoded['taking']
        
        fills.append(decoded)
        maker_volumes[decoded['maker']] += decoded['usdc_amount'] / 10**USDC_DECIMALS
    
    if not fills:
        return taker_addr, 0, 0, dict(maker_volumes), None
    
    # Determine if taker is buying or selling based on maker_side of fills
    # If makers are SELL => taker is buying tokens
    # If makers are BUY => taker is selling tokens
    maker_sides = [f['maker_side'] for f in fills if f.get('maker_side')]
    if not maker_sides:
        return taker_addr, 0, 0, dict(maker_volumes), None
    
    # All fills should have same direction (makers all SELL or all BUY)
    taker_is_buying = maker_sides[0] == 'SELL'
    
    prices = [f['price'] for f in fills if f['price'] > 0]
    if not prices:
        return taker_addr, 0, 0, dict(maker_volumes), None
    
    if taker_is_buying:
        # Taker buying tokens: best price = lowest (pay less USDC per token)
        best_price = min(prices)
    else:
        # Taker selling tokens: best price = highest (receive more USDC per token)
        best_price = max(prices)
    
    # Calculate slippage - per-fill basis to see which fills caused slippage
    total_actual_usdc = 0
    total_tokens = 0
    slippage_breakdown = []
    
    for f in fills:
        total_actual_usdc += f['usdc_amount']
        total_tokens += f['token_amount']
        
        # Calculate per-fill slippage
        if taker_is_buying:
            # Taker wants low price, slippage when price > best
            fill_slippage = (f['price'] - best_price) * f['token_amount'] / 10**USDC_DECIMALS
        else:
            # Taker wants high price, slippage when price < best
            fill_slippage = (best_price - f['price']) * f['token_amount'] / 10**USDC_DECIMALS
        
        slippage_breakdown.append({
            'maker': f['maker'],
            'price': f['price'],
            'tokens': f['token_amount'],
            'usdc': f['usdc_amount'],
            'fill_slippage': max(0, fill_slippage)
        })
    
    if taker_is_buying:
        # What taker would have paid at best price
        ideal_usdc = best_price * total_tokens
        slippage = (total_actual_usdc - ideal_usdc) / 10**USDC_DECIMALS
    else:
        # What taker would have received at best price
        ideal_usdc = best_price * total_tokens
        slippage = (ideal_usdc - total_actual_usdc) / 10**USDC_DECIMALS
    
    taker_usdc_vol = total_actual_usdc / 10**USDC_DECIMALS
    
    # Build debug info if requested
    debug_result = {
        'taker': taker_addr,
        'taker_is_buying': taker_is_buying,
        'maker_side': maker_sides[0] if maker_sides else None,
        'best_price': best_price,
        'total_tokens': total_tokens,
        'total_usdc': total_actual_usdc,
        'ideal_usdc': ideal_usdc,
        'slippage': max(0, slippage),
        'fills': slippage_breakdown
    }
    
    return taker_addr, max(0, slippage), taker_usdc_vol, dict(maker_volumes), debug_result

# ==============================================================================
# MAIN
# ==============================================================================

def print_slippage_sanity_check(case_name: str, settlement: dict, debug: dict):
    """Print detailed slippage calculation for sanity checking."""
    print(f"\n{'='*80}")
    print(f"SANITY CHECK: {case_name}")
    print(f"{'='*80}")
    print(f"Block: {settlement.get('block_number')}, TX: {settlement.get('transaction_hash')}")
    print(f"Type: {settlement.get('type')}, Exchange: {settlement.get('exchange')}")
    print(f"\nTaker: {debug['taker']}")
    print(f"Taker is {'BUYING' if debug['taker_is_buying'] else 'SELLING'} tokens (makers are {debug['maker_side']})")
    print(f"Best price: {debug['best_price']:.6f} (taker wants {'lowest' if debug['taker_is_buying'] else 'highest'})")
    print(f"\nFills (excluding exchange fill):")
    for i, f in enumerate(debug['fills'], 1):
        marker = " <- SLIPPAGE" if f['fill_slippage'] > 0 else ""
        print(f"  {i}. Maker {f['maker'][:10]}... price={f['price']:.6f}, tokens={f['tokens']}, usdc={f['usdc']}, slip={f['fill_slippage']:.6f}{marker}")
    print(f"\nTotal tokens: {debug['total_tokens']}")
    print(f"Actual USDC: {debug['total_usdc']} ({debug['total_usdc']/10**USDC_DECIMALS:.2f} USDC)")
    print(f"Ideal USDC (at best price): {debug['ideal_usdc']:.0f} ({debug['ideal_usdc']/10**USDC_DECIMALS:.2f} USDC)")
    print(f"SLIPPAGE: {debug['slippage']:.6f} USDC")
    print(f"Volume: {debug['total_usdc']/10**USDC_DECIMALS:.2f} USDC")
    print(f"Sanity: vol ({debug['total_usdc']/10**USDC_DECIMALS:.2f}) > slippage ({debug['slippage']:.6f}) >= 0 ? {debug['total_usdc']/10**USDC_DECIMALS > debug['slippage'] >= 0}")

async def main():
    print(f"Connecting to {DB_NAME}...")
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)

    # Time series data
    ts_data = defaultdict(list)
    
    # Stats containers
    stats = defaultdict(lambda: {
        'vol': 0.0, 'tx': 0, 'fills': 0, 'slippage': 0.0,
        'makers': Counter(), 'takers': Counter(), 'stakeholders': Counter(),
        'maker_vol': Counter(), 'taker_vol': Counter(),
        'taker_slippage': Counter()
    })
    
    # Collect samples for sanity check (one per case with slippage > 0)
    # Cases: (type, taker_direction) -> split+buy, split+sell, merge+buy, merge+sell, comp+buy, comp+sell
    sanity_samples = {}

    async with pool.acquire() as conn:
        # ======================================================================
        # 1. SANITY CHECK PASS - Find one sample per case with slippage > 0
        # ======================================================================
        print("Running sanity check pass to find slippage samples...")
        
        rows = await conn.fetch("""
            SELECT id, block_number, transaction_hash, type, exchange, trade FROM settlements 
            ORDER BY id ASC LIMIT 50000
        """)
        
        for r in rows:
            trade = json.loads(r['trade'])
            taker_addr, slippage, taker_vol, maker_vols, debug = compute_slippage(trade, r['type'])
            
            if debug and slippage > 0.0001:  # Non-trivial slippage
                case_key = f"{r['type']}_{debug['maker_side']}"
                if case_key not in sanity_samples:
                    sanity_samples[case_key] = {
                        'settlement': {
                            'block_number': r['block_number'],
                            'transaction_hash': r['transaction_hash'],
                            'type': r['type'],
                            'exchange': r['exchange']
                        },
                        'debug': debug
                    }
        
        # Print sanity checks
        print("\n" + "="*80)
        print("SLIPPAGE SANITY CHECK - One sample per case with slippage > 0")
        print("="*80)
        
        expected_cases = [
            ('split_BUY', 'SPLIT + Makers BUY => Taker SELLING (wants high price, slippage when price < best)'),
            ('split_SELL', 'SPLIT + Makers SELL => Taker BUYING (wants low price, slippage when price > best)'),
            ('merge_BUY', 'MERGE + Makers BUY => Taker SELLING (wants high price, slippage when price < best)'),
            ('merge_SELL', 'MERGE + Makers SELL => Taker BUYING (wants low price, slippage when price > best)'),
            ('complementary_BUY', 'COMPLEMENTARY + Makers BUY => Taker SELLING (wants high price, slippage when price < best)'),
            ('complementary_SELL', 'COMPLEMENTARY + Makers SELL => Taker BUYING (wants low price, slippage when price > best)'),
        ]
        
        for case_key, case_desc in expected_cases:
            if case_key in sanity_samples:
                sample = sanity_samples[case_key]
                print_slippage_sanity_check(case_desc, sample['settlement'], sample['debug'])
            else:
                print(f"\n[{case_key}] No sample with slippage found for: {case_desc}")
        
        print("\n" + "="*80)
        print("END SANITY CHECK - Proceeding with full analysis...")
        print("="*80 + "\n")

        # ======================================================================
        # 2. Process Settlements (6 categories)
        # ======================================================================
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
                time = block_to_time(r['block_number'])
                
                # Compute slippage (ignore debug output here)
                taker_addr, slippage, taker_vol, maker_vols, _ = compute_slippage(trade, r['type'])
                
                # Update stats
                stats[cat]['vol'] += vol
                stats[cat]['tx'] += 1
                stats[cat]['fills'] += fills
                stats[cat]['slippage'] += slippage
                
                if taker_addr:
                    stats[cat]['takers'][taker_addr] += 1
                    stats[cat]['taker_vol'][taker_addr] += taker_vol
                    stats[cat]['taker_slippage'][taker_addr] += slippage
                
                for maker, mvol in maker_vols.items():
                    stats[cat]['makers'][maker] += 1
                    stats[cat]['maker_vol'][maker] += mvol
                
                ts_data[cat].append({'time': time, 'vol': vol, 'slippage': slippage, 'tx': 1})
            
            print(f"  Processed up to ID {cursor_id}...", end='\r')

        # ======================================================================
        # 2. Process Standalone CFT (5 categories)
        # ======================================================================
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
                stakeholders = get_cft_tx_stakeholders(cft_events, adapter_events)
                time = block_to_time(r['block_number'])
                
                stats[cat]['vol'] += vol
                stats[cat]['tx'] += 1
                stats[cat]['stakeholders'].update(stakeholders)
                
                # Track stakeholder volume
                for sh in stakeholders:
                    stats[cat]['taker_vol'][sh] += vol  # Use taker_vol for stakeholders too
                
                ts_data[cat].append({'time': time, 'vol': vol, 'slippage': 0, 'tx': 1})
            
            print(f"  Processed up to ID {cursor_id}...", end='\r')

    await pool.close()

    # ==========================================================================
    # OUTPUT 1: STATISTICS TABLE
    # ==========================================================================
    print("\n\n" + "="*160)
    print(f"{'CATEGORY':<35} | {'VOL (USDC)':<16} | {'TXs':<10} | {'FILLS':<8} | {'SLIPPAGE':<14} | {'UNIQ MAKERS':<12} | {'UNIQ TAKERS':<12}")
    print("="*160)
    
    total_vol, total_tx, total_slippage = 0, 0, 0
    
    for cat, s in sorted(stats.items()):
        n_makers = len(s['makers'])
        n_takers = len(s['takers'])
        n_stakeholders = len(s['stakeholders'])
        
        total_vol += s['vol']
        total_tx += s['tx']
        total_slippage += s['slippage']
        
        is_trade = n_makers > 0 or n_takers > 0
        
        if not is_trade:
            print(f"{cat:<35} | {s['vol']:>16,.0f} | {s['tx']:<10} | {'-':<8} | {'-':<14} | {'-':<12} | {n_stakeholders:<12}")
        else:
            print(f"{cat:<35} | {s['vol']:>16,.0f} | {s['tx']:<10} | {s['fills']:<8} | {s['slippage']:>14,.2f} | {n_makers:<12} | {n_takers:<12}")
    
    print("="*160)
    print(f"{'TOTAL':<35} | {total_vol:>16,.0f} | {total_tx:<10} | {'-':<8} | {total_slippage:>14,.2f} | {'-':<12} | {'-':<12}")

    # ==========================================================================
    # OUTPUT 2: TOP 10 LISTS
    # ==========================================================================
    print("\n" + "="*100)
    print("TOP 10 ACTORS BY ROLE")
    print("="*100)
    
    for cat, s in sorted(stats.items()):
        print(f"\n>>> {cat} <<<")
        
        is_trade = len(s['makers']) > 0 or len(s['takers']) > 0
        
        if not is_trade:
            print("  [Top Stakeholders by TX Count]")
            for actor, count in s['stakeholders'].most_common(10):
                vol = s['taker_vol'].get(actor, 0)
                print(f"    {actor}: {count} txs, {vol:,.2f} USDC")
        else:
            print("  [Top Makers by TX Count]")
            for actor, count in s['makers'].most_common(10):
                vol = s['maker_vol'].get(actor, 0)
                print(f"    {actor}: {count} txs, {vol:,.2f} USDC")
            
            print("\n  [Top Takers by TX Count]")
            for actor, count in s['takers'].most_common(10):
                vol = s['taker_vol'].get(actor, 0)
                slip = s['taker_slippage'].get(actor, 0)
                print(f"    {actor}: {count} txs, {vol:,.2f} USDC, slippage: {slip:,.2f} USDC")
            
            print("\n  [Top Takers by Slippage]")
            for actor, slip in s['taker_slippage'].most_common(10):
                count = s['takers'].get(actor, 0)
                vol = s['taker_vol'].get(actor, 0)
                print(f"    {actor}: slippage {slip:,.2f} USDC, {count} txs, {vol:,.2f} USDC")
            
            print("\n  [Top Makers by Volume]")
            for actor, vol in s['maker_vol'].most_common(10):
                count = s['makers'].get(actor, 0)
                print(f"    {actor}: {vol:,.2f} USDC, {count} txs")
            
            print("\n  [Top Takers by Volume]")
            for actor, vol in s['taker_vol'].most_common(10):
                count = s['takers'].get(actor, 0)
                print(f"    {actor}: {vol:,.2f} USDC, {count} txs")

    # ==========================================================================
    # PLOT 1: CUMULATIVE VOLUME OVER TIME
    # ==========================================================================
    print("\n\nGenerating Cumulative Volume Plot...")
    plt.figure(figsize=(16, 10))
    for cat, records in sorted(ts_data.items()):
        if not records: continue
        df = pd.DataFrame(records).sort_values('time')
        df['cum_vol'] = df['vol'].cumsum()
        plt.plot(df['time'], df['cum_vol'], label=cat, linewidth=1.5)
    
    plt.title('Cumulative USDC Volume by Category')
    plt.xlabel('Date')
    plt.ylabel('Volume (USDC)')
    plt.legend(loc='upper left', fontsize=8)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig('plot_cumulative_volume.png', dpi=150)
    print("Saved plot_cumulative_volume.png")

    # ==========================================================================
    # PLOT 2: CUMULATIVE TX COUNT OVER TIME
    # ==========================================================================
    print("Generating Cumulative TX Count Plot...")
    plt.figure(figsize=(16, 10))
    for cat, records in sorted(ts_data.items()):
        if not records: continue
        df = pd.DataFrame(records).sort_values('time')
        df['cum_tx'] = df['tx'].cumsum()
        plt.plot(df['time'], df['cum_tx'], label=cat, linewidth=1.5)
    
    plt.title('Cumulative Transaction Count by Category')
    plt.xlabel('Date')
    plt.ylabel('Transaction Count')
    plt.legend(loc='upper left', fontsize=8)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig('plot_cumulative_txcount.png', dpi=150)
    print("Saved plot_cumulative_txcount.png")

    # ==========================================================================
    # PLOT 3: CUMULATIVE SLIPPAGE OVER TIME (Settlement categories only)
    # ==========================================================================
    print("Generating Cumulative Slippage Plot...")
    plt.figure(figsize=(16, 10))
    settlement_cats = [c for c in ts_data.keys() if '(Base)' in c or '(NegRisk)' in c]
    for cat in sorted(settlement_cats):
        records = ts_data[cat]
        if not records: continue
        df = pd.DataFrame(records).sort_values('time')
        df['cum_slippage'] = df['slippage'].cumsum()
        plt.plot(df['time'], df['cum_slippage'], label=cat, linewidth=1.5)
    
    plt.title('Cumulative Slippage by Settlement Category (USDC)')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Slippage (USDC)')
    plt.legend(loc='upper left', fontsize=8)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig('plot_cumulative_slippage.png', dpi=150)
    print("Saved plot_cumulative_slippage.png")

    # ==========================================================================
    # PLOT 4: LORENZ CURVE - TAKER SLIPPAGE PER CATEGORY
    # ==========================================================================
    print("Generating Lorenz Curve for Taker Slippage...")
    plt.figure(figsize=(14, 10))
    
    for cat, s in sorted(stats.items()):
        if not s['taker_slippage']: continue
        frequencies = sorted(s['taker_slippage'].values())
        if not frequencies or sum(frequencies) == 0: continue
        y_values = np.cumsum(frequencies)
        y_values = y_values / y_values[-1]
        x_values = np.linspace(0, 1, len(y_values))
        plt.plot(x_values, y_values, label=f"{cat} (n={len(frequencies)})", linewidth=1.5)
    
    plt.plot([0, 1], [0, 1], 'k--', alpha=0.3, label='Perfect Equality')
    plt.title('Lorenz Curve: Taker Slippage Distribution by Category')
    plt.xlabel('Percentile of Takers')
    plt.ylabel('Cumulative Share of Slippage')
    plt.legend(loc='upper left', fontsize=8)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig('plot_lorenz_taker_slippage.png', dpi=150)
    print("Saved plot_lorenz_taker_slippage.png")

    # ==========================================================================
    # PLOT 5: LORENZ CURVE - TAKER TRADE COUNT (ALL CATEGORIES)
    # ==========================================================================
    print("Generating Lorenz Curve for Taker Trade Count...")
    plt.figure(figsize=(14, 10))
    
    for cat, s in sorted(stats.items()):
        combined = s['takers'] + s['stakeholders']
        if not combined: continue
        frequencies = sorted(combined.values())
        y_values = np.cumsum(frequencies)
        y_values = y_values / y_values[-1]
        x_values = np.linspace(0, 1, len(y_values))
        plt.plot(x_values, y_values, label=f"{cat} (n={len(frequencies)})", linewidth=1.5)
    
    plt.plot([0, 1], [0, 1], 'k--', alpha=0.3, label='Perfect Equality')
    plt.title('Lorenz Curve: Taker/Stakeholder Trade Count Distribution')
    plt.xlabel('Percentile of Takers/Stakeholders')
    plt.ylabel('Cumulative Share of Trades')
    plt.legend(loc='upper left', fontsize=8)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig('plot_lorenz_taker_trades.png', dpi=150)
    print("Saved plot_lorenz_taker_trades.png")

    # ==========================================================================
    # PLOT 6: LORENZ CURVE - MAKER TRADE COUNT (SETTLEMENT CATEGORIES)
    # ==========================================================================
    print("Generating Lorenz Curve for Maker Trade Count...")
    plt.figure(figsize=(14, 10))
    
    for cat, s in sorted(stats.items()):
        if not s['makers']: continue
        frequencies = sorted(s['makers'].values())
        y_values = np.cumsum(frequencies)
        y_values = y_values / y_values[-1]
        x_values = np.linspace(0, 1, len(y_values))
        plt.plot(x_values, y_values, label=f"{cat} (n={len(frequencies)})", linewidth=1.5)
    
    plt.plot([0, 1], [0, 1], 'k--', alpha=0.3, label='Perfect Equality')
    plt.title('Lorenz Curve: Maker Trade Count Distribution')
    plt.xlabel('Percentile of Makers')
    plt.ylabel('Cumulative Share of Trades')
    plt.legend(loc='upper left', fontsize=8)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig('plot_lorenz_maker_trades.png', dpi=150)
    print("Saved plot_lorenz_maker_trades.png")

    # ==========================================================================
    # PLOT 7: LORENZ CURVE - TAKER VOLUME (ALL CATEGORIES)
    # ==========================================================================
    print("Generating Lorenz Curve for Taker Volume...")
    plt.figure(figsize=(14, 10))
    
    for cat, s in sorted(stats.items()):
        if not s['taker_vol']: continue
        frequencies = sorted(s['taker_vol'].values())
        if sum(frequencies) == 0: continue
        y_values = np.cumsum(frequencies)
        y_values = y_values / y_values[-1]
        x_values = np.linspace(0, 1, len(y_values))
        plt.plot(x_values, y_values, label=f"{cat} (n={len(frequencies)})", linewidth=1.5)
    
    plt.plot([0, 1], [0, 1], 'k--', alpha=0.3, label='Perfect Equality')
    plt.title('Lorenz Curve: Taker/Stakeholder Volume Distribution')
    plt.xlabel('Percentile of Takers/Stakeholders')
    plt.ylabel('Cumulative Share of Volume')
    plt.legend(loc='upper left', fontsize=8)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig('plot_lorenz_taker_volume.png', dpi=150)
    print("Saved plot_lorenz_taker_volume.png")

    # ==========================================================================
    # PLOT 8: LORENZ CURVE - MAKER VOLUME (SETTLEMENT CATEGORIES)
    # ==========================================================================
    print("Generating Lorenz Curve for Maker Volume...")
    plt.figure(figsize=(14, 10))
    
    for cat, s in sorted(stats.items()):
        if not s['maker_vol']: continue
        frequencies = sorted(s['maker_vol'].values())
        if sum(frequencies) == 0: continue
        y_values = np.cumsum(frequencies)
        y_values = y_values / y_values[-1]
        x_values = np.linspace(0, 1, len(y_values))
        plt.plot(x_values, y_values, label=f"{cat} (n={len(frequencies)})", linewidth=1.5)
    
    plt.plot([0, 1], [0, 1], 'k--', alpha=0.3, label='Perfect Equality')
    plt.title('Lorenz Curve: Maker Volume Distribution')
    plt.xlabel('Percentile of Makers')
    plt.ylabel('Cumulative Share of Volume')
    plt.legend(loc='upper left', fontsize=8)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig('plot_lorenz_maker_volume.png', dpi=150)
    print("Saved plot_lorenz_maker_volume.png")

    print("\n✅ Analysis complete!")

if __name__ == "__main__":
    asyncio.run(main())
