import asyncpg
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import os

# --- Configuration ---
PG_HOST = os.getenv("PG_SOCKET", "localhost")
PG_PORT = os.getenv("POLY_PG_PORT", 5432)
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

POLY_DEM_ASSET_ID = "83247781037352156539108067944461291821683755894607244160607042790356561625563"
POLY_REP_ASSET_ID = "65139230827417363158752884968303867495725894165574887635816574090175320800482"

# Time range offsets (in days from now)
T_START_OFFSET_D = 2
T_END_OFFSET_D = 3

async def fetch_all_data(pool, asset_id):
    """Fetch data for a specific asset_id in batches by day"""
    # Calculate time range in microseconds
    now = datetime.utcnow()
    end_time = now - timedelta(days=T_END_OFFSET_D)
    start_time = now - timedelta(days=T_START_OFFSET_D)
    
    end_us = int(end_time.timestamp() * 1_000_000)
    start_us = int(start_time.timestamp() * 1_000_000)
    
    print(f"\n>>> Fetching asset_id: {asset_id}")
    print(f"Fetching from {end_time} to {start_time}")
    print(f"Time range (us): {end_us} to {start_us}")
    
    all_records = []
    current_time = end_time
    
    # Fetch day by day
    while current_time < start_time:
        day_start = current_time
        day_end = current_time + timedelta(days=1)
        
        day_start_us = int(day_start.timestamp() * 1_000_000)
        day_end_us = int(day_end.timestamp() * 1_000_000)
        
        query = """
        SELECT asset_id, server_time_us, message
        FROM poly_book_state_house
        WHERE asset_id = $1
        AND server_time_us >= $2
        AND server_time_us < $3
        ORDER BY server_time_us ASC
        """
        
        records = await pool.fetch(query, asset_id, day_start_us, day_end_us)
        all_records.extend(records)
        print(f"  Day {day_start.date()}: {len(records)} records")
        
        current_time = day_end
    
    records = all_records
    print(f"Fetched {len(records)} records for asset_id {asset_id}")
    
    # Check what asset_ids are actually in the results
    if records:
        unique_asset_ids = set(r['asset_id'] for r in records)
        print(f"  Unique asset_ids in results: {unique_asset_ids}")
        if len(unique_asset_ids) > 1:
            print(f"  WARNING: Expected 1 asset_id, got {len(unique_asset_ids)}")
            for uid in unique_asset_ids:
                count = sum(1 for r in records if r['asset_id'] == uid)
                print(f"    {uid}: {count} records")
    
    times = []
    min_asks = []
    max_bids = []
    
    for record in records:
        try:
            msg = json.loads(record['message'])
            
            # FILTER BY MESSAGE ASSET_ID, NOT DATABASE COLUMN!
            msg_asset_id = msg.get('asset_id')
            if msg_asset_id != asset_id:
                continue
            
            # Extract min ask price
            if msg.get('asks'):
                asks = [float(ask['price']) for ask in msg['asks']]
                min_ask = min(asks)
                min_asks.append(min_ask)
            else:
                min_asks.append(None)
            
            # Extract max bid price
            if msg.get('bids'):
                bids = [float(bid['price']) for bid in msg['bids']]
                max_bid = max(bids)
                max_bids.append(max_bid)
            else:
                max_bids.append(None)
            
            times.append(record['server_time_us'] / 1_000_000)  # Convert to seconds
            
        except Exception as e:
            print(f"Error parsing record: {e}")
            continue
    
    df = pd.DataFrame({
        'timestamp': times,
        'min_ask': min_asks,
        'max_bid': max_bids
    })
    
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df = df.dropna()
    
    print(f"After extraction: {len(df)} records")
    print(f"Min ask range: {df['min_ask'].min():.4f} - {df['min_ask'].max():.4f}")
    print(f"Max bid range: {df['max_bid'].min():.4f} - {df['max_bid'].max():.4f}")
    
    return df

async def main():
    pool = await asyncpg.create_pool(
        host=PG_HOST,
        port=int(PG_PORT),
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        min_size=1,
        max_size=5
    )
    
    print("=" * 80)
    print("FETCHING DEMOCRATIC (DEM) DATA")
    print("=" * 80)
    dem_df = await fetch_all_data(pool, POLY_DEM_ASSET_ID)
    
    print("\n" + "=" * 80)
    print("FETCHING REPUBLICAN (REP) DATA")
    print("=" * 80)
    rep_df = await fetch_all_data(pool, POLY_REP_ASSET_ID)
    
    # Create plots
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    
    # DEM plots
    axes[0, 0].plot(dem_df['timestamp'], dem_df['max_bid'], label='Max Bid (YES)', color='green', linewidth=1)
    axes[0, 0].set_title('Democratic - Max Bid Price Over Time')
    axes[0, 0].set_xlabel('Time')
    axes[0, 0].set_ylabel('Price')
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)
    axes[0, 0].tick_params(axis='x', rotation=45)
    
    axes[0, 1].plot(dem_df['timestamp'], dem_df['min_ask'], label='Min Ask (NO)', color='red', linewidth=1)
    axes[0, 1].set_title('Democratic - Min Ask Price Over Time')
    axes[0, 1].set_xlabel('Time')
    axes[0, 1].set_ylabel('Price')
    axes[0, 1].legend()
    axes[0, 1].grid(True, alpha=0.3)
    axes[0, 1].tick_params(axis='x', rotation=45)
    
    # REP plots
    axes[1, 0].plot(rep_df['timestamp'], rep_df['max_bid'], label='Max Bid (YES)', color='green', linewidth=1)
    axes[1, 0].set_title('Republican - Max Bid Price Over Time')
    axes[1, 0].set_xlabel('Time')
    axes[1, 0].set_ylabel('Price')
    axes[1, 0].legend()
    axes[1, 0].grid(True, alpha=0.3)
    axes[1, 0].tick_params(axis='x', rotation=45)
    
    axes[1, 1].plot(rep_df['timestamp'], rep_df['min_ask'], label='Min Ask (NO)', color='red', linewidth=1)
    axes[1, 1].set_title('Republican - Min Ask Price Over Time')
    axes[1, 1].set_xlabel('Time')
    axes[1, 1].set_ylabel('Price')
    axes[1, 1].legend()
    axes[1, 1].grid(True, alpha=0.3)
    axes[1, 1].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig('/home/mario_cattaneo/polymarket/data_analysis/plots/debug_prices.png', dpi=150)
    print("\nPlot saved to plots/debug_prices.png")
    
    await pool.close()

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
