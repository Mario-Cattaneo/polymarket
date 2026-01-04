import asyncio
import asyncpg
import os
import pandas as pd
import logging
from datetime import datetime

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# --- Configuration ---
DB_CONFIG = {
    "host": os.environ.get("PG_SOCKET"),
    "port": os.environ.get("POLY_PG_PORT"),
    "database": os.environ.get("POLY_DB"),
    "user": os.environ.get("POLY_DB_CLI"),
    "password": os.environ.get("POLY_DB_CLI_PASS")
}

async def get_db_connection():
    """Create database connection."""
    try:
        conn = await asyncpg.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"]
        )
        logger.info("Database connection established.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

async def fetch_markets_from_db(conn):
    """Fetch all condition_ids from markets_4 table."""
    logger.info("Fetching condition_ids from markets_4 table...")
    
    query = """
        SELECT DISTINCT condition_id
        FROM markets_5 
        WHERE condition_id IS NOT NULL
    """
    
    rows = await conn.fetch(query)
    condition_ids = set(row['condition_id'].lower() for row in rows)
    
    # Normalize to 0x prefix
    normalized = set()
    for cid in condition_ids:
        if not cid.startswith('0x'):
            cid = f'0x{cid}'
        normalized.add(cid)
    
    logger.info(f"Fetched {len(normalized)} unique condition_ids from markets_4")
    return normalized

def load_csv_data(csv_path):
    """Load condition_ids and createdAt from CSV."""
    logger.info(f"Loading CSV from: {csv_path}")
    
    if not os.path.exists(csv_path):
        logger.critical(f"CSV file not found: {csv_path}")
        return None
    
    df = pd.read_csv(csv_path, low_memory=False)
    
    # Clean condition_id (CSV uses camelCase 'conditionId')
    df['condition_id'] = df['conditionId'].astype(str).str.lower().apply(
        lambda x: f'0x{x}' if not x.startswith('0x') else x
    )
    
    # Parse createdAt (CSV uses camelCase 'createdAt')
    df['createdAt_dt'] = pd.to_datetime(df['createdAt'], errors='coerce', utc=True)
    
    # Remove timezone info for comparison
    df['createdAt_dt'] = df['createdAt_dt'].dt.tz_localize(None)
    
    # Get unique condition_ids with their earliest creation time
    csv_data = df.groupby('condition_id')['createdAt_dt'].min().reset_index()
    csv_data.columns = ['condition_id', 'created_time']
    
    logger.info(f"Loaded {len(csv_data)} unique condition_ids from CSV")
    
    return csv_data

async def main():
    csv_path = os.path.join(os.environ.get('POLY_CSV', ''), 'gamma_markets.csv')
    
    # Load CSV data
    csv_data = load_csv_data(csv_path)
    if csv_data is None:
        return
    
    # Connect to database and fetch condition_ids
    conn = await get_db_connection()
    try:
        db_condition_ids = await fetch_markets_from_db(conn)
    finally:
        await conn.close()
        logger.info("Database connection closed.")

    if not db_condition_ids:
        logger.warning("No data from markets_4 table")
        return

    # Get CSV condition_ids lookup
    csv_lookup = csv_data.set_index('condition_id')['created_time']

    # Build DB IDs with timestamps (from CSV) - only those that exist in CSV
    db_ids_with_time = []
    for cid in db_condition_ids:
        if cid in csv_lookup:
            db_ids_with_time.append((cid, csv_lookup[cid]))

    if not db_ids_with_time:
        logger.warning("No DB condition_ids found in CSV for timestamp comparison")
        return

    # Create sorted lists for gap detection
    db_df = pd.DataFrame(db_ids_with_time, columns=['condition_id', 'created_time'])
    db_df = db_df.sort_values('created_time').reset_index(drop=True)
    
    # All CSV IDs sorted by time
    csv_sorted = csv_data.sort_values('created_time').reset_index(drop=True)
    
    # Find gaps: CSV-only IDs that fall between two consecutive DB IDs
    # Use single pass through CSV data for efficiency
    db_times = db_df['created_time'].values
    db_ids_list = db_df['condition_id'].values
    db_ids_set = set(db_ids_list)
    
    gaps = []
    gap_idx = 0  # Track current gap window in db_times
    
    logger.info(f"Scanning {len(csv_sorted)} CSV entries for gaps between {len(db_df)} DB entries...")
    
    for csv_idx, csv_row in csv_sorted.iterrows():
        csv_time = csv_row['created_time']
        csv_id = csv_row['condition_id']
        
        # Skip if CSV ID is in DB
        if csv_id in db_ids_set:
            continue
        
        # Find which gap window this CSV ID falls into
        while gap_idx < len(db_times) - 1 and csv_time >= db_times[gap_idx + 1]:
            gap_idx += 1
        
        # Check if strictly between gap_idx and gap_idx+1
        if gap_idx < len(db_times) - 1 and db_times[gap_idx] < csv_time < db_times[gap_idx + 1]:
            # Add to current gap or create new one
            if gaps and gaps[-1]['db_before_idx'] == gap_idx:
                gaps[-1]['gap_ids'].append((csv_id, csv_time))
                gaps[-1]['gap_count'] += 1
            else:
                gaps.append({
                    'db_before_idx': gap_idx,
                    'db_before': db_ids_list[gap_idx],
                    'db_after': db_ids_list[gap_idx + 1],
                    'time_before': db_times[gap_idx],
                    'time_after': db_times[gap_idx + 1],
                    'gap_ids': [(csv_id, csv_time)],
                    'gap_count': 1
                })

    # Print results
    print("\n" + "="*100)
    print(" " * 30 + "CONDITION ID GAP ANALYSIS")
    print("="*100)
    print(f"\nDB condition_ids: {len(db_df):,}")
    print(f"CSV condition_ids: {len(csv_data):,}")
    print(f"Shared: {len(db_ids_set):,}")
    print(f"CSV-only: {len(csv_data) - len(db_ids_set):,}")
    
    total_gap_ids = sum(g['gap_count'] for g in gaps)
    print(f"\n" + "-"*100)
    print(f"TRUE GAPS (CSV-only IDs between consecutive DB IDs): {len(gaps)}")
    print(f"Total CSV-only IDs in gaps: {total_gap_ids:,}")
    
    if gaps:
        print(f"\n" + "="*100)
        print(" " * 35 + "DETAILED GAPS")
        print("="*100)
        
        for idx, gap in enumerate(gaps[:10], 1):
            print(f"\nGap {idx}:")
            print(f"  Between DB: {gap['db_before']} ({gap['time_before']})")
            print(f"       and:  {gap['db_after']} ({gap['time_after']})")
            print(f"  Missing CSV-only IDs: {gap['gap_count']}")
            print(f"  Samples:")
            for cid, ctime in gap['gap_ids'][:3]:
                print(f"    - {cid} at {ctime}")
    else:
        print(f"\nNo gaps found: all CSV-only IDs are either before first or after last DB ID")

    print("\n" + "="*100)
    print(" " * 40 + "ANALYSIS COMPLETE")
    print("="*100)

if __name__ == "__main__":
    asyncio.run(main())
