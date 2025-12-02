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
import json

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

# Trade Tables
TRADE_TABLES = {
    1: "buffer_last_trade_prices",
    2: "buffer_last_trade_prices_2"
}

# Contract Addresses (from LogHarvester)
_CTF_EXCHANGE = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
_NEG_RISK_CTF = "0xc5d563a36ae78145c45a50134d48a1215220f80a"
_CONDITIONAL_TOKENS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"


# 2. TIME RANGE & INTERPOLATION HELPERS
# -------------------------------------

async def fetch_block_timestamps(conn, session):
    """Fetches min/max block numbers and their timestamps from DB/Infura."""
    print("📊 Step 1: Fetching min/max block numbers for interpolation...")
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
        except Exception:
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

async def calculate_fork_time_range(conn, table_name):
    """Calculates the min/max server_time_ms for a single trade table."""
    print(f"📊 Step 2: Calculating time range for {table_name}...")
    query = f"SELECT MIN(server_time_ms) as min_t, MAX(server_time_ms) as max_t FROM {table_name}"
    try:
        row = await conn.fetchrow(query)
        min_ms = row['min_t']
        max_ms = row['max_t']
    except Exception as e:
        print(f"⚠️ Could not query {table_name}: {e}")
        return None, None

    if not min_ms or not max_ms:
        return None, None

    start_dt = datetime.fromtimestamp(min_ms / 1000.0, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(max_ms / 1000.0, tz=timezone.utc)
    
    print(f"🕒 Fork Range: {start_dt} -> {end_dt}")
    return start_dt, end_dt


# 3. DATA LOADING
# ----------------

async def get_trade_data(conn, table_name, time_range):
    """Fetches trade data and extracts side for a single table."""
    print(f"🔌 Fetching trade data from {table_name}...")
    
    is_v1 = (table_name == TRADE_TABLES[1])
    
    # Dynamic Query Construction: V1 does NOT have the 'side' column
    select_cols = "server_time_ms, message"
    if not is_v1:
        select_cols += ", side"
        
    query = f"""
        SELECT {select_cols}
        FROM {table_name}
        WHERE server_time_ms IS NOT NULL
    """
    rows = await conn.fetch(query)
    
    trades = []
    
    for r in rows:
        server_time_ms = r['server_time_ms']
        
        # Filter by time range
        dt = datetime.fromtimestamp(server_time_ms / 1000.0, tz=timezone.utc)
        if not (time_range[0] <= dt <= time_range[1]):
            continue
            
        side = None
        
        if is_v1: # V1: buffer_last_trade_prices (Side is in message JSON)
            try:
                msg = r['message']
                # asyncpg returns JSONB as dict/list, but we check just in case
                if isinstance(msg, str): msg = json.loads(msg) 
                side = msg.get('side')
            except:
                pass
        
        else: # V2: buffer_last_trade_prices_2 (Side is a direct column)
            side = r['side']
            
        if side in ['BUY', 'SELL']:
            trades.append({'time': dt, 'side': side})
            
    df = pd.DataFrame(trades)
    print(f"✅ Fetched {len(df)} trades from {table_name}.")
    return df

async def get_event_data(conn, min_blk, min_ts, max_blk, max_ts, time_range):
    """Fetches and interpolates event data, filtered by time_range."""
    
    events_of_interest = [
        'OrderFilled', 'OrdersMatched', 
        'PositionSplit', 'PositionsConverted', 'PositionsMerge'
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
    
    # Filter by time range
    df = df[(df['time'] >= time_range[0]) & (df['time'] <= time_range[1])].copy()
    
    return df

# 4. PLOTTING HELPER
# ------------------
def plot_cdf_volume(data_dict, title, filename, time_range):
    plt.figure(figsize=(16, 10))
    sns.set_style("whitegrid")
    
    palette = sns.color_palette("tab10", len(data_dict))
    
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
    
    plt.legend(fontsize=10, loc="upper left", frameon=True, framealpha=0.9)
    plt.tight_layout()
    plt.savefig(filename)
    print(f"✅ Saved {filename}")
    plt.close()


# 5. MAIN EXECUTION
# -----------------
async def main():
    print("🚀 Starting Trade Volume Plotting Script...")
    
    # Initial DB connection for block timestamps
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

    # Fetch all events once (unfiltered by time)
    print("📊 Step 3: Fetching all relevant chain events for interpolation...")
    all_events_df_unfiltered = await get_event_data(conn, min_blk, min_ts, max_blk, max_ts, (datetime.min.replace(tzinfo=timezone.utc), datetime.max.replace(tzinfo=timezone.utc)))
    
    await conn.close()

    # --- Process and Plot for each Fork ---
    for fork_id, trade_table_name in TRADE_TABLES.items():
        print(f"\n--- Processing Fork {fork_id} ({trade_table_name}) ---")
        
        # 1. Calculate Dynamic Time Range
        conn = await asyncpg.connect(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
        start_dt, end_dt = await calculate_fork_time_range(conn, trade_table_name)
        
        if not start_dt:
            print(f"⚠️ Skipping Fork {fork_id}: No valid trade data found.")
            await conn.close()
            continue
            
        time_range = (start_dt, end_dt)
        
        # 2. Fetch Trade Data
        trade_df = await get_trade_data(conn, trade_table_name, time_range)
        await conn.close()
        
        # 3. Filter Event Data by Fork's Time Range
        print(f"📊 Step 4: Filtering chain events to time range {time_range[0]} -> {time_range[1]}...")
        all_events_df = all_events_df_unfiltered[
            (all_events_df_unfiltered['time'] >= time_range[0]) & 
            (all_events_df_unfiltered['time'] <= time_range[1])
        ].copy()
        print(f"✅ Filtered down to {len(all_events_df)} events for this fork.")

        # 4. Prepare Plot Data Dictionary
        plot_data = {}
        
        # --- Trade Data (Last Trades) ---
        plot_data[f"Trades: Combined"] = trade_df
        plot_data[f"Trades: BUY"] = trade_df[trade_df['side'] == 'BUY']
        plot_data[f"Trades: SELL"] = trade_df[trade_df['side'] == 'SELL']

        # --- Exchange Events (OrderFilled, OrdersMatched) ---
        
        # OrderFilled
        of_df = all_events_df[all_events_df['event_name'] == 'OrderFilled']
        plot_data[f"Chain: Combined OrderFilled"] = of_df
        plot_data[f"Chain: CTF Exchange (OrderFilled)"] = of_df[of_df['contract_address'] == _CTF_EXCHANGE]
        plot_data[f"Chain: NegRisk CTF (OrderFilled)"] = of_df[of_df['contract_address'] == _NEG_RISK_CTF]
        
        # OrdersMatched
        om_df = all_events_df[all_events_df['event_name'] == 'OrdersMatched']
        plot_data[f"Chain: Combined OrdersMatched"] = om_df
        plot_data[f"Chain: CTF Exchange (OrdersMatched)"] = om_df[om_df['contract_address'] == _CTF_EXCHANGE]
        plot_data[f"Chain: NegRisk CTF (OrdersMatched)"] = om_df[om_df['contract_address'] == _NEG_RISK_CTF]
        
        # --- Conditional Token Events (Position Management) ---
        
        # PositionSplit
        ps_df = all_events_df[all_events_df['event_name'] == 'PositionSplit']
        plot_data[f"Chain: PositionSplit (Conditional Tokens)"] = ps_df[ps_df['contract_address'] == _CONDITIONAL_TOKENS]
        
        # PositionsConverted (NegRisk Adapter only)
        pc_df = all_events_df[all_events_df['event_name'] == 'PositionsConverted']
        plot_data[f"Chain: PositionsConverted (NegRisk Adapter)"] = pc_df
        
        # PositionsMerge
        pm_df = all_events_df[all_events_df['event_name'] == 'PositionsMerge']
        plot_data[f"Chain: PositionsMerge (Conditional Tokens)"] = pm_df[pm_df['contract_address'] == _CONDITIONAL_TOKENS]
        plot_data[f"Chain: PositionsMerge (NegRisk Adapter)"] = pm_df[pm_df['contract_address'] != _CONDITIONAL_TOKENS]


        # 5. Generate Plot
        output_dir = Path(POLY_PLOTS_DIR) / "volume" / str(fork_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        plot_cdf_volume(
            plot_data, 
            f"Fork {fork_id} CDF: Trade Volume and Exchange Events", 
            output_dir / "plot_trade_volume_cdf.png", 
            time_range
        )


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())