import os
import asyncio
import asyncpg
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timezone

# 1. CONFIGURATION
# ----------------
# DB Config
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# Files
CHAIN_CSV = "raw_polymarket_events.csv"
GAMMA_CSV = "gamma_markets.csv"

# TIME RANGE
TARGET_START_DATE = datetime(2025, 11, 18, 7, 0, 0, tzinfo=timezone.utc)
TARGET_END_DATE = datetime.now(timezone.utc)

print(f"🕒 Plotting Range: {TARGET_START_DATE} -> {TARGET_END_DATE}")

# 2. DATA LOADING
# ----------------
async def get_db_data():
    print("🔌 Connecting to Database...")
    conn = await asyncpg.connect(
        user=DB_USER, password=DB_PASS,
        database=DB_NAME, host=PG_HOST, port=PG_PORT
    )
    
    query = """
        SELECT found_time_ms, closed_time, exhaustion_cycle 
        FROM markets 
        WHERE condition_id IS NOT NULL
    """
    rows = await conn.fetch(query)
    await conn.close()
    
    found_times = []
    closed_times = []
    
    for r in rows:
        # 1. Found Time (Cycle > 0)
        if r['found_time_ms']:
            cycle = r['exhaustion_cycle']
            if cycle is not None and cycle != 0:
                dt = datetime.fromtimestamp(r['found_time_ms'] / 1000.0, tz=timezone.utc)
                if TARGET_START_DATE <= dt <= TARGET_END_DATE:
                    found_times.append(dt)
            
        # 2. Closed Time
        if r['closed_time']:
            try:
                ts = float(r['closed_time'])
                if ts > 30000000000: ts /= 1000.0
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                if TARGET_START_DATE <= dt <= TARGET_END_DATE:
                    closed_times.append(dt)
            except: pass
            
    return pd.DataFrame(found_times, columns=['time']), pd.DataFrame(closed_times, columns=['time'])

def get_chain_data():
    print(f"📂 Loading {CHAIN_CSV}...")
    if not os.path.exists(CHAIN_CSV):
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df = pd.read_csv(CHAIN_CSV)
    
    # Convert and Filter Range
    df['time'] = pd.to_datetime(df['block_timestamp_utc'], utc=True)
    df = df[(df['time'] >= TARGET_START_DATE) & (df['time'] <= TARGET_END_DATE)]
    
    ctf_reg = df[(df['event_name'] == 'TokenRegistered') & (df['contract_name'] == 'CTF Exchange')].copy()
    neg_reg = df[(df['event_name'] == 'TokenRegistered') & (df['contract_name'] == 'NegRisk Adapter')].copy()
    cond_prep = df[df['event_name'] == 'ConditionPreparation'].copy()
    cond_res = df[df['event_name'] == 'ConditionResolution'].copy()
    
    return ctf_reg, neg_reg, cond_prep, cond_res

def get_gamma_data():
    print(f"📂 Loading {GAMMA_CSV}...")
    if not os.path.exists(GAMMA_CSV):
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df = pd.read_csv(GAMMA_CSV)
    
    def process_col(col_name):
        series = pd.to_datetime(df[col_name], utc=True, errors='coerce')
        series = series.dropna()
        # STRICT FILTER: Start <= t <= End
        series = series[(series >= TARGET_START_DATE) & (series <= TARGET_END_DATE)]
        return series.to_frame(name='time')

    gamma_created = process_col('createdAt')
    gamma_start = process_col('startDate')
    gamma_end = process_col('endDate')
    
    return gamma_created, gamma_start, gamma_end

# 3. PLOTTING HELPER
# ------------------
def plot_cdf(data_dict, title, filename):
    plt.figure(figsize=(14, 9))
    sns.set_style("whitegrid")
    
    palette = sns.color_palette("bright", len(data_dict))
    
    print(f"   Generating {title}...")
    
    for i, (label, df) in enumerate(data_dict.items()):
        count = len(df)
        if count == 0:
            # Plot empty line to show in legend with N=0
            plt.plot([], [], label=f"{label} (N=0)", color=palette[i])
            continue
            
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
    
    # FORCE X-AXIS LIMITS to match the exact range requested
    plt.xlim(TARGET_START_DATE, TARGET_END_DATE)
    
    # Legend
    plt.legend(fontsize=11, loc="upper left", frameon=True, framealpha=0.9)
    
    # Date Formatting
    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig(filename)
    print(f"✅ Saved {filename}")
    plt.close()

# 4. MAIN
# -------
async def main():
    # 1. Load All Data
    db_found, db_closed = await get_db_data()
    ctf_reg, neg_reg, cond_prep, cond_res = get_chain_data()
    gamma_created, gamma_start, gamma_end = get_gamma_data()
    
    combined_reg = pd.concat([ctf_reg, neg_reg])
    
    # --- PLOT 1: CREATION / DISCOVERY ---
    plot_1_data = {
        "1.1 DB: Markets Found (Cycle > 0)": db_found,
        "1.2 Chain: CTF Tokens Registered": ctf_reg,
        "1.3 Chain: NegRisk Tokens Registered": neg_reg,
        "1.4 Chain: Combined Tokens Registered": combined_reg,
        "1.5 Chain: Conditions Prepared": cond_prep,
        "1.6 Gamma: Created At": gamma_created,
        "1.7 Gamma: Start Date": gamma_start
    }
    plot_cdf(plot_1_data, "CDF: Market Creation & Discovery (Nov 18 - Now)", "plot_1_creation_gamma.png")
    
    # --- PLOT 2: RESOLUTION / END ---
    plot_2_data = {
        "2.1 DB: Markets Closed": db_closed,
        "2.2 Chain: Conditions Resolved": cond_res,
        "2.3 Gamma: End Date": gamma_end
    }
    plot_cdf(plot_2_data, "CDF: Market Resolution & End Dates (Nov 18 - Now)", "plot_2_resolution_gamma.png")
    
    # --- PLOT 3: MASTER OVERVIEW ---
    plot_3_data = {**plot_1_data, **plot_2_data}
    plot_cdf(plot_3_data, "CDF: Master Lifecycle Overview (Nov 18 - Now)", "plot_3_master_gamma.png")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())