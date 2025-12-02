import os
import asyncio
import asyncpg
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timezone
from web3 import Web3
import aiohttp
from pathlib import Path

# 1. CONFIGURATION
# ----------------
# DB Config
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# Plot Output Directory
POLY_PLOTS_DIR = os.getenv('POLY_PLOTS')
if not POLY_PLOTS_DIR:
    print("❌ Error: POLY_PLOTS not set.")
    exit(1)

# Infura Config (for block timestamps)
INFURA_KEY = os.getenv('INFURA_KEY_2')
if not INFURA_KEY:
    print("❌ Error: INFURA_KEY not set.")
    exit(1)
RPC_URL = f"https://polygon-mainnet.infura.io/v3/{INFURA_KEY}"

# Market Tables
MARKET_TABLES = {
    1: "markets",
    2: "markets_2"
}

# NEW FILTER PARAMETERS: Remove the first K markets by found_time_ms
FILTER_START_K = {
    1: 30750,  # For 'markets'
    2: 34250  # For 'markets_2'
}

# Contract Addresses (from LogHarvester)
_CTF_EXCHANGE = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
_NEG_RISK_ADAPTER = "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296"
_NEG_RISK_CTF = "0xc5d563a36ae78145c45a50134d48a1215220f80a"
_CONDITIONAL_TOKENS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
_NEG_RISK_OPERATOR = "0x71523d0f655b41e805cec45b17163f528b59b820"


# 2. TIMESTAMP INTERPOLATION & DYNAMIC RANGE
# ------------------------------------------

async def fetch_block_timestamps(conn, session):
    """Fetches min/max block numbers and their timestamps from DB/Infura."""
    query = "SELECT MIN(block_number) as min_blk, MAX(block_number) as max_blk FROM polygon_events"
    row = await conn.fetchrow(query)
    min_blk = row['min_blk']
    max_blk = row['max_blk']
    
    if not min_blk or not max_blk:
        print("⚠️ No events found in polygon_events table.")
        return None, None, None, None

    w3 = Web3()
    async def get_block_time(block_num):
        payload = {"jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": [hex(block_num), False], "id": 1}
        try:
            async with session.post(RPC_URL, json=payload, timeout=10) as resp:
                data = await resp.json()
                if 'result' in data and data['result']:
                    ts = int(data['result']['timestamp'], 16)
                    return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception as e:
            print(f"❌ Error fetching block {block_num}: {e}")
            return None

    min_ts = await get_block_time(min_blk)
    max_ts = await get_block_time(max_blk)
    
    if not min_ts or not max_ts:
        print("❌ Could not fetch timestamps for min/max blocks.")
        return None, None, None, None

    print(f"⏱️ Block Range: {min_blk} ({min_ts.isoformat()}) -> {max_blk} ({max_ts.isoformat()})")
    return min_blk, min_ts, max_blk, max_ts

def interpolate_timestamp(df, min_blk, min_ts, max_blk, max_ts):
    """Fills timestamps using linear interpolation based on block number."""
    if df.empty or not min_blk:
        return df

    total_time_diff = (max_ts - min_ts).total_seconds()
    total_block_diff = max_blk - min_blk
    
    if total_block_diff <= 0:
        df['time'] = min_ts
        return df

    def calculate_time(row):
        block_diff = row['block_number'] - min_blk
        time_offset = (block_diff / total_block_diff) * total_time_diff
        return min_ts + pd.Timedelta(seconds=time_offset)

    df['time'] = df.apply(calculate_time, axis=1)
    return df

async def calculate_dynamic_time_ranges(conn, table_name):
    """Calculates the min/max times for found and closed markets."""
    query = f"""
        SELECT found_time_ms, closed_time
        FROM {table_name} 
        WHERE condition_id IS NOT NULL
        ORDER BY found_time_ms ASC
    """
    rows = await conn.fetch(query)
    
    # Convert ms to datetime
    def to_dt(ms):
        if ms is None: return None
        ts = float(ms)
        if ts > 30000000000: ts /= 1000.0 
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    found_times = [to_dt(r['found_time_ms']) for r in rows if r['found_time_ms']]
    closed_times = [to_dt(r['closed_time']) for r in rows if r['closed_time']]
    
    if not found_times and not closed_times:
        return (None, None), (None, None), (None, None)

    # Plot 1 Range: Min Found -> Max Found
    min_found = min(found_times) if found_times else None
    max_found = max(found_times) if found_times else None
    plot_1_range = (min_found, max_found)
    
    # Plot 2 Range: Min Closed -> Max Closed
    min_closed = min(closed_times) if closed_times else None
    max_closed = max(closed_times) if closed_times else None
    plot_2_range = (min_closed, max_closed)
    
    # Plot 4 Range: Min(Min Found, Min Closed) -> Max(Max Found, Max Closed)
    all_mins = [dt for dt in [min_found, min_closed] if dt is not None]
    all_maxs = [dt for dt in [max_found, max_closed] if dt is not None]
    
    plot_4_range = (min(all_mins) if all_mins else None, max(all_maxs) if all_maxs else None)
    
    return plot_1_range, plot_2_range, plot_4_range


# 3. DATA LOADING
# ----------------
async def get_market_data(conn, table_name, time_range, filter_k):
    """Fetches market data for a specific fork table, filtered by time_range and K."""
    
    query = f"""
        SELECT found_time_ms, closed_time
        FROM {table_name} 
        WHERE condition_id IS NOT NULL
        ORDER BY found_time_ms ASC
    """
    rows = await conn.fetch(query)
    
    # Apply K filter: remove the first 'filter_k' markets by found_time_ms
    if filter_k > 0:
        rows = rows[filter_k:]
    
    found_times = []
    closed_times = []
    
    for r in rows:
        # 1. Found Time
        if r['found_time_ms']:
            dt = datetime.fromtimestamp(r['found_time_ms'] / 1000.0, tz=timezone.utc)
            if time_range[0] <= dt <= time_range[1]:
                found_times.append(dt)
            
        # 2. Closed Time
        if r['closed_time']:
            try:
                ts = float(r['closed_time'])
                if ts > 30000000000: ts /= 1000.0 # Handle ms vs s
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                if time_range[0] <= dt <= time_range[1]:
                    closed_times.append(dt)
            except: pass
            
    return pd.DataFrame(found_times, columns=['time']), pd.DataFrame(closed_times, columns=['time'])

async def get_event_data(conn, min_blk, min_ts, max_blk, max_ts):
    """Fetches and interpolates ALL relevant event data from polygon_events."""
    print("🔌 Fetching all event data from polygon_events...")
    
    events_of_interest = [
        'TokenRegistered', 'MarketPrepared', 'QuestionPrepared',
        'ConditionPreparation', 'ConditionResolution',
        'QuestionResolved', 'QuestionEmergencyResolved'
    ]
    
    query = f"""
        SELECT block_number, created_at, contract_address, event_name
        FROM polygon_events
        WHERE event_name = ANY($1::text[])
        ORDER BY block_number
    """
    rows = await conn.fetch(query, events_of_interest)
    
    df = pd.DataFrame(rows, columns=['block_number', 'time', 'contract_address', 'event_name'])
    
    df['time'] = pd.to_datetime(df['time']).dt.tz_localize(timezone.utc)
    
    df = interpolate_timestamp(df, min_blk, min_ts, max_blk, max_ts)
    
    return df

# 4. PLOTTING HELPER
# ------------------
def plot_cdf(data_dict, title, filename, time_range):
    plt.figure(figsize=(14, 9))
    sns.set_style("whitegrid")
    
    palette = sns.color_palette("bright", len(data_dict))
    
    print(f"   Generating {title}...")
    
    for i, (label, df) in enumerate(data_dict.items()):
        count = len(df)
        label_with_count = f"{label} (N={count})"
            
        sns.ecdfplot(
            data=df, 
            x="time", 
            label=label_with_count, 
            stat="count", 
            linewidth=2.5, 
            color=palette[i],
            alpha=0.8
        )

    plt.title(title, fontsize=16, fontweight='bold')
    plt.xlabel("Date (UTC)", fontsize=12)
    plt.ylabel("Cumulative Count", fontsize=12)
    
    plt.xlim(time_range[0], time_range[1])
    
    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.xticks(rotation=45)
    
    plt.legend(fontsize=11, loc="upper left", frameon=True, framealpha=0.9)
    plt.tight_layout()
    plt.savefig(filename)
    print(f"✅ Saved {filename}")
    plt.close()

def plot_net_active_count(market_found_df, market_closed_df, cond_prep_df, cond_res_df, title, filename, time_range):
    """
    Plots the difference between the CDFs for Markets and Conditions on one graph.
    """
    plt.figure(figsize=(14, 9))
    sns.set_style("whitegrid")
    
    print(f"   Generating {title}...")

    def calculate_net_active(found_df, closed_df, label, color):
        found_df = found_df.sort_values('time')
        closed_df = closed_df.sort_values('time')
        
        all_times = pd.concat([found_df['time'], closed_df['time']]).sort_values().unique()
        all_times = all_times[(all_times >= time_range[0]) & (all_times <= time_range[1])]
        
        if len(all_times) == 0:
            plt.plot([], [], label=f"{label} (N=0)", color=color)
            return

        cdf_data = []
        found_count = 0
        closed_count = 0
        found_idx = 0
        closed_idx = 0
        
        cdf_data.append({'time': time_range[0], 'active': 0})
        
        for t in all_times:
            while found_idx < len(found_df) and found_df.iloc[found_idx]['time'] <= t:
                found_count += 1
                found_idx += 1
            
            while closed_idx < len(closed_df) and closed_df.iloc[closed_idx]['time'] <= t:
                closed_count += 1
                closed_idx += 1
                
            cdf_data.append({'time': t, 'active': found_count - closed_count})

        active_df = pd.DataFrame(cdf_data)
        plt.plot(active_df['time'], active_df['active'], label=label, linewidth=2.5, color=color)

    # Plot 4.1: Markets
    calculate_net_active(market_found_df, market_closed_df, "Net Active Orderbooks (Found - Closed)", 'blue')
    
    # Plot 4.2: Conditions
    calculate_net_active(cond_prep_df, cond_res_df, "Net Active Conditions (Prepared - Resolved)", 'red')
    
    plt.title(title, fontsize=16, fontweight='bold')
    plt.xlabel("Date (UTC)", fontsize=12)
    plt.ylabel("Active Count (Net CDF)", fontsize=12)
    
    plt.xlim(time_range[0], time_range[1])
    
    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.xticks(rotation=45)
    
    plt.legend(fontsize=11, loc="upper left", frameon=True, framealpha=0.9)
    plt.tight_layout()
    plt.savefig(filename)
    print(f"✅ Saved {filename}")
    plt.close()


# 5. MAIN EXECUTION
# -----------------
async def main():
    # Initial DB connection for setup
    conn = await asyncpg.connect(
        user=DB_USER, password=DB_PASS,
        database=DB_NAME, host=PG_HOST, port=PG_PORT
    )
    
    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        min_blk, min_ts, max_blk, max_ts = await fetch_block_timestamps(conn, session)
        
        if not min_blk:
            await conn.close()
            return

        # Fetch all chain events once (unfiltered by time)
        all_events_df = await get_event_data(conn, min_blk, min_ts, max_blk, max_ts)
        
        await conn.close()

    # --- Process and Plot for each Fork ---
    for fork_id, table_name in MARKET_TABLES.items():
        print(f"\n--- Processing Fork {fork_id} ({table_name}) ---")
        
        # 1. Calculate Dynamic Time Ranges
        conn = await asyncpg.connect(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
        plot_1_range, plot_2_range, plot_4_range = await calculate_dynamic_time_ranges(conn, table_name)
        
        if not plot_4_range[0]:
            print(f"⚠️ Skipping Fork {fork_id}: No valid market data found for time range calculation.")
            await conn.close()
            continue
            
        # 2. Fetch Market Data (Filtered by Plot 4 Range and K)
        filter_k = FILTER_START_K.get(fork_id, 0)
        db_found, db_closed = await get_market_data(conn, table_name, plot_4_range, filter_k)
        await conn.close()

        # Create output directory
        output_dir = Path(POLY_PLOTS_DIR) / "lifetime" / str(fork_id)
        output_dir.mkdir(parents=True, exist_ok=True)

        # --- PLOT 1: CREATION / DISCOVERY (CDF) ---
        p1_events = all_events_df[
            (all_events_df['time'] >= plot_1_range[0]) & 
            (all_events_df['time'] <= plot_1_range[1])
        ]
        
        plot_1_data = {
            f"DB: Orderbooks Found (K={filter_k})": db_found[
                (db_found['time'] >= plot_1_range[0]) & (db_found['time'] <= plot_1_range[1])
            ],
            
            f"Chain: CTF Exchange (TokenRegistered)": p1_events[
                (p1_events['event_name'] == 'TokenRegistered') & (p1_events['contract_address'] == _CTF_EXCHANGE)
            ],
            f"Chain: NegRisk CTF (TokenRegistered)": p1_events[
                (p1_events['event_name'] == 'TokenRegistered') & (p1_events['contract_address'] == _NEG_RISK_CTF)
            ],
            f"Chain: Combined TokenRegistered": p1_events[
                (p1_events['event_name'] == 'TokenRegistered') & 
                (p1_events['contract_address'].isin([_CTF_EXCHANGE, _NEG_RISK_CTF]))
            ],
            
            f"Chain: NegRisk Adapter (MarketPrepared)": p1_events[
                (p1_events['event_name'] == 'MarketPrepared') & (p1_events['contract_address'] == _NEG_RISK_ADAPTER)
            ],
            f"Chain: NegRisk Operator (MarketPrepared)": p1_events[
                (p1_events['event_name'] == 'MarketPrepared') & (p1_events['contract_address'] == _NEG_RISK_OPERATOR)
            ],
            
            f"Chain: NegRisk Adapter (QuestionPrepared)": p1_events[
                (p1_events['event_name'] == 'QuestionPrepared') & (p1_events['contract_address'] == _NEG_RISK_ADAPTER)
            ],
            f"Chain: NegRisk Operator (QuestionPrepared)": p1_events[
                (p1_events['event_name'] == 'QuestionPrepared') & (p1_events['contract_address'] == _NEG_RISK_OPERATOR)
            ],
            
            f"Chain: Conditional Tokens (ConditionPreparation)": p1_events[
                p1_events['event_name'] == 'ConditionPreparation'
            ],
        }
        plot_cdf(plot_1_data, f"Fork {fork_id} CDF: Market Creation & Discovery", output_dir / "plot_1_creation.png", plot_1_range)

        # --- PLOT 2: RESOLUTION / END (CDF) ---
        p2_events = all_events_df[
            (all_events_df['time'] >= plot_2_range[0]) & 
            (all_events_df['time'] <= plot_2_range[1])
        ]
        
        plot_2_data = {
            f"DB: Orderbooks Closed (K={filter_k})": db_closed[
                (db_closed['time'] >= plot_2_range[0]) & (db_closed['time'] <= plot_2_range[1])
            ],
            
            f"Chain: Conditional Tokens (ConditionResolution)": p2_events[
                p2_events['event_name'] == 'ConditionResolution'
            ],
            
            f"Chain: NegRisk Operator (QuestionResolved)": p2_events[
                (p2_events['event_name'] == 'QuestionResolved') & 
                (p2_events['contract_address'] == _NEG_RISK_OPERATOR)
            ],
            f"Chain: NegRisk Operator (QuestionEmergencyResolved)": p2_events[
                (p2_events['event_name'] == 'QuestionEmergencyResolved') & 
                (p2_events['contract_address'] == _NEG_RISK_OPERATOR)
            ],
        }
        plot_cdf(plot_2_data, f"Fork {fork_id} CDF: Market Resolution & End Dates", output_dir / "plot_2_resolution.png", plot_2_range)

        # --- PLOT 4: NET ACTIVE COUNT (CDF Difference) ---
        p4_events = all_events_df[
            (all_events_df['time'] >= plot_4_range[0]) & 
            (all_events_df['time'] <= plot_4_range[1])
        ]
        p4_cond_prep_df = p4_events[p4_events['event_name'] == 'ConditionPreparation'].copy()
        p4_cond_res_df = p4_events[p4_events['event_name'] == 'ConditionResolution'].copy()
        
        plot_net_active_count(
            db_found, 
            db_closed, 
            p4_cond_prep_df, 
            p4_cond_res_df, 
            f"Fork {fork_id} Net Active Count (K={filter_k})", 
            output_dir / "plot_4_net_active.png", 
            plot_4_range
        )


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())