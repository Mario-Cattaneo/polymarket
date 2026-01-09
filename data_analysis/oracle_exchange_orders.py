#!/usr/bin/env python3
"""
Oracle Analysis with OrderFilled and OrdersMatched Matching:
1. Fetch ALL ConditionPreparation events and deduplicate by condition_id
2. For each of 3 oracles: count their prepared conditions
3. Match prepared conditions to CSV rows by condition_id (tracks token_id1, token_id2)
4. Match token_ids to TokenRegistered events and count per oracle
5. Fetch all OrderFilled and OrdersMatched events
6. Match these events to the asset_ids identified for each oracle (efficient nlog(n) sort-then-match)
7. Report breakdown: Orders by Oracle, Exchange, Event Type
"""

import os
import asyncio
import asyncpg
import pandas as pd
import logging
import json
from datetime import datetime, timezone
from collections import defaultdict

# ----------------------------------------------------------------
# SETUP & CONFIGURATION
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

# Database Credentials
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# CSV Path
POLY_CSV_DIR = os.getenv("POLY_CSV")
POLY_CSV_PATH = os.path.join(POLY_CSV_DIR, "gamma_markets.csv") if POLY_CSV_DIR else "gamma_markets.csv"

# Conditional Tokens Contract
CT_ADDR = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower()

# Three Oracles of Interest
ORACLE_ADDRESSES = {
    "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7": "MOOV2 Adapter",
    "0x58e1745bedda7312c4cddb72618923da1b90efde": "Centralized Adapter",
    "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296": "Negrisk Adapter",
}

# Exchange Addresses
CTFE_ADDR = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e".lower()
NEGRISK_ADDR = "0x9e4a16ddf5a656a3fc1af9872b2c207024558bd0".lower()

# ----------------------------------------------------------------
# DATABASE FUNCTIONS
# ----------------------------------------------------------------

async def connect_db():
    """Create asyncpg connection pool."""
    return await asyncpg.create_pool(
        host=PG_HOST,
        port=int(PG_PORT),
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        min_size=1,
        max_size=10,
    )

async def fetch_all_prepared_conditions(pool):
    """Fetch ALL ConditionPreparation events and extract oracle from topics[3].
    
    ConditionPreparation event structure:
    - topics[1]: (not used)
    - topics[2]: conditionId (indexed)
    - topics[3]: oracle (indexed, as 32-byte hash, need last 20 bytes)
    
    Returns: DataFrame with condition_id, timestamp, oracle
    """
    query = """
        SELECT 
            LOWER(topics[2]) as condition_id, 
            timestamp_ms as timestamp, 
            '0x' || substring(topics[3] from 27) as oracle
        FROM events_conditional_tokens 
        WHERE event_name = 'ConditionPreparation' 
        AND LOWER(contract_address) = $1
    """
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, CT_ADDR)
    
    df = pd.DataFrame([dict(row) for row in rows])
    if not df.empty:
        df['oracle'] = df['oracle'].str.lower()
    return df

async def fetch_all_token_registered_events(pool):
    """Fetch ALL TokenRegistered events from token_reg table.
    
    Uses the pre-built token_reg table which consolidates both exchanges.
    Differentiates by contract_address.
    
    Token IDs in topics are stored as hex strings (32 bytes).
    """
    query = """
        SELECT 
            LOWER(topics[1]) as token0,
            LOWER(topics[2]) as token1,
            topics[3] as condition_id,
            LOWER(contract_address) as contract,
            transaction_hash,
            timestamp_ms
        FROM token_reg
        WHERE event_name = 'TokenRegistered'
    """
    
    logger.info("Fetching all TokenRegistered events from token_reg table...")
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)
    
    logger.info(f"Fetched {len(rows):,} total TokenRegistered events")
    return pd.DataFrame([dict(row) for row in rows])

async def fetch_all_order_events(pool, min_timestamp_ms=None, max_timestamp_ms=None, batch_size=100000):
    """
    Fetch OrderFilled and OrdersMatched events in simple 100k batches.
    Fetch unfiltered by timestamp, show progress %.
    """
    if min_timestamp_ms is None or max_timestamp_ms is None:
        min_max_query = """
            SELECT 
                MIN(timestamp_ms) as min_ts,
                MAX(timestamp_ms) as max_ts
            FROM (
                SELECT timestamp_ms FROM events_ctf_exchange
                UNION ALL
                SELECT timestamp_ms FROM events_neg_risk_exchange
            ) t
        """
        
        async with pool.acquire() as conn:
            min_max_row = await conn.fetchrow(min_max_query)
        
        min_ts = min_max_row['min_ts'] if min_max_row else 0
        max_ts = min_max_row['max_ts'] if min_max_row else 1
        
        if min_timestamp_ms is None:
            min_timestamp_ms = min_ts
        if max_timestamp_ms is None:
            max_timestamp_ms = max_ts
    
    ts_range = max_timestamp_ms - min_timestamp_ms if max_timestamp_ms > min_timestamp_ms else 1
    
    logger.info(f"Fetching events (range: {min_timestamp_ms} - {max_timestamp_ms}, span: {ts_range:,}ms)...")
    
    decoded_events = []
    offset = 0
    last_ts = min_timestamp_ms
    
    async with pool.acquire() as conn:
        while True:
            # Simple LIMIT/OFFSET query without UNION - just fetch both tables sequentially
            query = f"""
                SELECT transaction_hash, event_name, topics, data, timestamp_ms
                FROM events_ctf_exchange
                WHERE timestamp_ms BETWEEN ${1} AND ${2}
                ORDER BY timestamp_ms ASC
                LIMIT ${3} OFFSET ${4}
            """
            
            rows = await conn.fetch(query, min_timestamp_ms, max_timestamp_ms, batch_size, offset)
            
            if not rows:
                logger.info(f"✅ Finished CTF exchange")
                break
            
            batch_num = offset // batch_size + 1
            logger.info(f"  Batch {batch_num}: {len(rows):,} rows from events_ctf_exchange")
            
            for row in rows:
                event_dict = dict(row)
                last_ts = event_dict['timestamp_ms']
                
                if event_dict['event_name'] not in ('OrdersMatched', 'OrderFilled'):
                    continue
                
                asset_ids = decode_asset_ids(event_dict['data'], event_dict['event_name'])
                event_dict['asset_ids'] = asset_ids
                decoded_events.append(event_dict)
            
            progress_pct = ((last_ts - min_timestamp_ms) / ts_range * 100) if ts_range > 0 else 100
            logger.info(f"    Progress: {progress_pct:.1f}% | Matched: {len(decoded_events):,}")
            
            offset += batch_size
        
        # Now fetch from neg_risk_exchange
        offset = 0
        while True:
            query = f"""
                SELECT transaction_hash, event_name, topics, data, timestamp_ms
                FROM events_neg_risk_exchange
                WHERE timestamp_ms BETWEEN ${1} AND ${2}
                ORDER BY timestamp_ms ASC
                LIMIT ${3} OFFSET ${4}
            """
            
            rows = await conn.fetch(query, min_timestamp_ms, max_timestamp_ms, batch_size, offset)
            
            if not rows:
                logger.info(f"✅ Finished NegRisk exchange")
                break
            
            batch_num = offset // batch_size + 1
            logger.info(f"  Batch {batch_num}: {len(rows):,} rows from events_neg_risk_exchange")
            
            for row in rows:
                event_dict = dict(row)
                last_ts = event_dict['timestamp_ms']
                
                if event_dict['event_name'] not in ('OrdersMatched', 'OrderFilled'):
                    continue
                
                asset_ids = decode_asset_ids(event_dict['data'], event_dict['event_name'])
                event_dict['asset_ids'] = asset_ids
                decoded_events.append(event_dict)
            
            progress_pct = ((last_ts - min_timestamp_ms) / ts_range * 100) if ts_range > 0 else 100
            logger.info(f"    Progress: {progress_pct:.1f}% | Matched: {len(decoded_events):,}")
            
            offset += batch_size
    
    logger.info(f"✅ Fetched {len(decoded_events):,} total matched events")
    return decoded_events

def decode_asset_ids(data_hex: str, event_name: str) -> list:
    """
    Decode asset IDs from OrderFilled/OrdersMatched event data.
    
    OrderFilled data: [makerAssetId, takerAssetId, making, taking, fee]
    OrdersMatched data: [makerAssetId, takerAssetId, making, taking]
    
    Returns: [makerAssetId, takerAssetId]
    """
    try:
        if not data_hex:
            return []
        
        # Remove 0x prefix
        data_hex = data_hex[2:] if data_hex.startswith('0x') else data_hex
        
        # Pad to multiple of 64 (32 bytes = 256 bits)
        if len(data_hex) % 64 != 0:
            data_hex = data_hex.ljust((len(data_hex) // 64 + 1) * 64, '0')
        
        # Extract first two 256-bit values (makerAssetId, takerAssetId)
        asset_ids = []
        for i in range(0, min(128, len(data_hex)), 64):
            chunk = data_hex[i:i+64]
            value = int(chunk, 16)
            asset_ids.append(value)
        
        return asset_ids[:2]  # Return only first 2 (maker and taker asset IDs)
    except Exception as e:
        logger.warning(f"Failed to decode asset IDs from {event_name}: {e}")
        return []

def match_token_ids_sorted_merge(df_all_token_events, oracle_token_ids_decimal):
    """
    Efficiently match token_ids using set lookup O(1).
    
    CSV token_ids are decimal integers.
    Topics store them as 0x-prefixed hex strings (256-bit).
    
    Convert each decimal token_id to 0x hex format for matching.
    """
    if df_all_token_events.empty or len(oracle_token_ids_decimal) == 0:
        return pd.DataFrame()
    
    # Convert decimal token_ids to hex (0x-prefixed, lowercase)
    token_id_set = set()
    for token_id_decimal in oracle_token_ids_decimal:
        try:
            # Convert to hex string without 0x prefix, pad to 64 chars (256 bits)
            hex_str = hex(int(token_id_decimal))[2:].lower().zfill(64)
            token_id_set.add('0x' + hex_str)
        except (ValueError, TypeError):
            logger.warning(f"Failed to convert token_id {token_id_decimal} to hex")
    
    logger.info(f"  Converted {len(token_id_set):,} token_ids to hex format")
    
    # Find rows where token0 OR token1 is in token_id_set
    mask = (df_all_token_events['token0'].isin(token_id_set)) | \
           (df_all_token_events['token1'].isin(token_id_set))
    
    return df_all_token_events[mask].copy()

def match_orders_by_asset_id_sorted(decoded_events, asset_ids_set):
    """
    Efficiently match orders to asset IDs using sorted merge strategy.
    
    O(n log n) complexity: sort events by asset ID, then iterate matching.
    
    Args:
        decoded_events: List of dicts with asset_ids fields
        asset_ids_set: Set of asset IDs to match against
    
    Returns: List of matching events
    """
    matching_events = []
    
    for event in decoded_events:
        asset_ids = event.get('asset_ids', [])
        
        # Check if any asset ID in this event matches
        for asset_id in asset_ids:
            if asset_id in asset_ids_set:
                matching_events.append(event)
                break
    
    return matching_events

# ----------------------------------------------------------------
# CSV LOADING
# ----------------------------------------------------------------

def load_market_data(csv_path):
    """Load market data from CSV with conditionId and token_ids."""
    logger.info(f"Loading market data from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, usecols=['conditionId', 'token_id1', 'token_id2'])
        df['conditionId'] = df['conditionId'].str.lower()
        
        logger.info(f"Loaded {len(df):,} markets from CSV.")
        return df
    except Exception as e:
        logger.error(f"Error loading CSV: {e}")
        return pd.DataFrame()

# ----------------------------------------------------------------
# MAIN ANALYSIS
# ----------------------------------------------------------------

async def main():
    pool = await connect_db()
    
    try:
        # Load CSV data
        csv_data = load_market_data(POLY_CSV_PATH)
        if csv_data.empty:
            logger.error("Failed to load CSV data")
            return
        
        # Create condition_id to token_ids mapping from CSV
        csv_condition_map = {}
        for _, row in csv_data.iterrows():
            cond_id = str(row['conditionId']).lower()
            token1 = str(row['token_id1'])
            token2 = str(row['token_id2'])
            csv_condition_map[cond_id] = {
                'token_id1': token1,
                'token_id2': token2,
            }
        
        logger.info(f"CSV contains {len(csv_condition_map):,} unique condition_ids")
        
        # ====================================================================
        # STEP 1: Fetch ALL ConditionPreparation events and deduplicate
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: Fetch ALL ConditionPreparation Events (Deduplicated)")
        logger.info("=" * 80)
        
        logger.info("\nFetching all ConditionPreparation events...")
        df_all_prepared = await fetch_all_prepared_conditions(pool)
        
        logger.info(f"Total events fetched: {len(df_all_prepared):,}")
        
        # Deduplicate by condition_id, keeping first event per condition
        df_all_prepared['timestamp'] = pd.to_datetime(df_all_prepared['timestamp'], unit='ms', utc=True)
        df_prepared_dedup = df_all_prepared.sort_values('timestamp').drop_duplicates(subset='condition_id', keep='first')
        
        logger.info(f"After deduplication by condition_id: {len(df_prepared_dedup):,}")
        
        # Count by oracle
        logger.info("\nAll Prepared Conditions by Oracle:")
        oracle_totals = df_prepared_dedup.groupby('oracle').size()
        for oracle_addr in sorted(ORACLE_ADDRESSES.keys()):
            count = oracle_totals.get(oracle_addr, 0)
            alias = ORACLE_ADDRESSES.get(oracle_addr, oracle_addr)
            logger.info(f"  {alias}: {count:,}")
        
        # ====================================================================
        # STEP 2: Match prepared conditions to CSV rows
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: Match Prepared Conditions to CSV Rows")
        logger.info("=" * 80)
        
        # Find which prepared conditions match the CSV
        prepared_cond_ids = set(df_prepared_dedup['condition_id'].unique())
        csv_cond_ids = set(csv_condition_map.keys())
        matching_cond_ids = prepared_cond_ids & csv_cond_ids
        
        logger.info(f"\nPrepared conditions in DB: {len(prepared_cond_ids):,}")
        logger.info(f"Conditions in CSV: {len(csv_cond_ids):,}")
        logger.info(f"Matching conditions: {len(matching_cond_ids):,}")
        
        # Get prepared events that match CSV
        df_matched_prepared = df_prepared_dedup[df_prepared_dedup['condition_id'].isin(matching_cond_ids)].copy()
        
        logger.info("\nPrepared Conditions Matched to CSV by Oracle:")
        matched_by_oracle = df_matched_prepared.groupby('oracle').size()
        for oracle_addr in sorted(ORACLE_ADDRESSES.keys()):
            count = matched_by_oracle.get(oracle_addr, 0)
            alias = ORACLE_ADDRESSES.get(oracle_addr, oracle_addr)
            logger.info(f"  {alias}: {count:,}")
        
        # ====================================================================
        # STEP 3: Fetch ALL TokenRegistered events (once, then match locally)
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: Fetch ALL TokenRegistered Events (fetch once, match locally)")
        logger.info("=" * 80)
        
        # Fetch all TokenRegistered events once (no filtering yet)
        df_all_token_events = await fetch_all_token_registered_events(pool)
        
        # Categorize by exchange using contract address
        df_all_token_events['exchange'] = df_all_token_events['contract'].apply(
            lambda x: 'CTFE' if x.lower() == CTFE_ADDR else 'NegRisk'
        )
        
        logger.info(f"\nTokenRegistered Events (all oracles) by Exchange:")
        logger.info(f"  CTFE: {(df_all_token_events['exchange'] == 'CTFE').sum():,}")
        logger.info(f"  NegRisk: {(df_all_token_events['exchange'] == 'NegRisk').sum():,}")
        
        # ====================================================================
        # STEP 4: Calculate time window from matched prepared conditions
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 4: Calculate Time Window from Matched Prepared Conditions")
        logger.info("=" * 80)
        
        # Get min/max timestamps from matched prepared conditions
        df_matched_prepared['timestamp'] = pd.to_datetime(df_matched_prepared['timestamp'], unit='ms', utc=True)
        min_timestamp_ms = int(df_matched_prepared['timestamp'].min().timestamp() * 1000)
        max_timestamp_ms = int(df_matched_prepared['timestamp'].max().timestamp() * 1000)
        
        logger.info(f"Prepared conditions time range: {min_timestamp_ms} to {max_timestamp_ms}")
        logger.info(f"Time window: {max_timestamp_ms - min_timestamp_ms:,} ms")
        logger.info(f"Sample times (first 3):")
        for i, ts in enumerate(df_matched_prepared['timestamp'].head(3)):
            logger.info(f"  [{i}] {ts} (UTC)")
        
        # ====================================================================
        # STEP 5: Fetch OrderFilled and OrdersMatched events (filtered by time window)
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 5: Fetch OrderFilled and OrdersMatched Events (Filtered by Time Window)")
        logger.info("=" * 80)
        
        decoded_events = await fetch_all_order_events(pool, min_timestamp_ms, max_timestamp_ms)
        
        # Categorize events
        orders_matched_events = [e for e in decoded_events if e['event_name'] == 'OrdersMatched']
        order_filled_events = [e for e in decoded_events if e['event_name'] == 'OrderFilled']
        
        logger.info(f"\nOrder Events Fetched:")
        logger.info(f"  OrdersMatched: {len(orders_matched_events):,}")
        logger.info(f"  OrderFilled: {len(order_filled_events):,}")
        
        # ====================================================================
        # STEP 6: Build comprehensive summary per oracle (NOW WITH ORDER MATCHING)
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 6: Comprehensive Summary by Oracle (Including Order Matching)")
        logger.info("=" * 80)
        
        results = []
        
        for oracle_addr, oracle_name in sorted(ORACLE_ADDRESSES.items()):
            logger.info(f"\n{oracle_name}:")
            
            # Prepared conditions
            prepared_count = len(df_prepared_dedup[df_prepared_dedup['oracle'] == oracle_addr])
            
            # Matched conditions (to CSV)
            matched_count = len(df_matched_prepared[df_matched_prepared['oracle'] == oracle_addr])
            
            # Get token_ids for this oracle's matched conditions
            oracle_matched_conds = set(df_matched_prepared[df_matched_prepared['oracle'] == oracle_addr]['condition_id'])
            oracle_token_ids = set()
            for cond_id in oracle_matched_conds:
                if cond_id in csv_condition_map:
                    oracle_token_ids.add(csv_condition_map[cond_id]['token_id1'])
                    oracle_token_ids.add(csv_condition_map[cond_id]['token_id2'])
            
            logger.info(f"  Prepared Conditions: {prepared_count:,}")
            logger.info(f"  Matched to CSV: {matched_count:,}")
            logger.info(f"  Unique Token IDs: {len(oracle_token_ids):,}")
            
            # Find TokenRegistered events for this oracle's token_ids
            if len(oracle_token_ids) == 0:
                ctfe_token_count = 0
                negrisk_token_count = 0
                total_token_count = 0
                oracle_asset_ids = set()
            else:
                # Use sorted merge matching - much faster than database query
                df_oracle_tokens = match_token_ids_sorted_merge(df_all_token_events, oracle_token_ids)
                
                ctfe_token_count = (df_oracle_tokens['exchange'] == 'CTFE').sum()
                negrisk_token_count = (df_oracle_tokens['exchange'] == 'NegRisk').sum()
                total_token_count = len(df_oracle_tokens)
                
                # Extract asset IDs from token0 and token1 fields
                oracle_asset_ids = set()
                oracle_asset_ids.update(df_oracle_tokens['token0'].unique())
                oracle_asset_ids.update(df_oracle_tokens['token1'].unique())
            
            logger.info(f"  TokenRegistered Events: {total_token_count:,}")
            logger.info(f"    ├─ CTFE: {ctfe_token_count:,}")
            logger.info(f"    └─ NegRisk: {negrisk_token_count:,}")
            logger.info(f"  Unique Asset IDs (from token_reg): {len(oracle_asset_ids):,}")
            
            # ====================================================================
            # NOW MATCH ORDER EVENTS TO THESE ASSET IDS (efficient sort-then-match)
            # ====================================================================
            
            if len(oracle_asset_ids) == 0:
                logger.info(f"  ⚠️  No asset IDs found for this oracle")
                orders_matched_count = 0
                order_filled_count = 0
                orders_matched_ctfe = 0
                orders_matched_negrisk = 0
                orders_filled_ctfe = 0
                orders_filled_negrisk = 0
            else:
                # Convert asset IDs from hex strings to integers for matching
                asset_ids_int = set()
                for aid_hex in oracle_asset_ids:
                    try:
                        # Remove 0x prefix if present, convert from hex to int
                        aid_str = aid_hex[2:] if isinstance(aid_hex, str) and aid_hex.startswith('0x') else str(aid_hex)
                        asset_ids_int.add(int(aid_str, 16))
                    except (ValueError, AttributeError):
                        pass
                
                # Match orders using efficient sort-then-match
                matched_om = match_orders_by_asset_id_sorted(orders_matched_events, asset_ids_int)
                matched_of = match_orders_by_asset_id_sorted(order_filled_events, asset_ids_int)
                
                orders_matched_count = len(matched_om)
                order_filled_count = len(matched_of)
                
                # Count by exchange
                orders_matched_ctfe = sum(1 for e in matched_om if e['contract'] == CTFE_ADDR)
                orders_matched_negrisk = orders_matched_count - orders_matched_ctfe
                orders_filled_ctfe = sum(1 for e in matched_of if e['contract'] == CTFE_ADDR)
                orders_filled_negrisk = order_filled_count - orders_filled_ctfe
                
                logger.info(f"  Orders Matched Events: {orders_matched_count:,}")
                logger.info(f"    ├─ CTFE: {orders_matched_ctfe:,}")
                logger.info(f"    └─ NegRisk: {orders_matched_negrisk:,}")
                logger.info(f"  Orders Filled Events: {order_filled_count:,}")
                logger.info(f"    ├─ CTFE: {orders_filled_ctfe:,}")
                logger.info(f"    └─ NegRisk: {orders_filled_negrisk:,}")
            
            results.append({
                'Oracle': oracle_name,
                'Prepared': f"{prepared_count:,}",
                'CSV Matches': f"{matched_count:,}",
                'Unique Tokens': f"{len(oracle_token_ids):,}",
                'Token Events': f"{total_token_count:,}",
                'Token CTFE': f"{ctfe_token_count:,}",
                'Token NegRisk': f"{negrisk_token_count:,}",
                'Orders Matched': f"{orders_matched_count:,}",
                'OM CTFE': f"{orders_matched_ctfe:,}",
                'OM NegRisk': f"{orders_matched_negrisk:,}",
                'Orders Filled': f"{order_filled_count:,}",
                'OF CTFE': f"{orders_filled_ctfe:,}",
                'OF NegRisk': f"{orders_filled_negrisk:,}",
            })
        
        # ====================================================================
        # STEP 6: Summary Table
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("SUMMARY TABLE")
        logger.info("=" * 80)
        
        summary_df = pd.DataFrame(results)
        logger.info("\n" + summary_df.to_string(index=False))
        
    finally:
        await pool.close()

# ----------------------------------------------------------------
# ENTRY POINT
# ----------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(main())
