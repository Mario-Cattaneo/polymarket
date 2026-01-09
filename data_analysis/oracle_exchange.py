#!/usr/bin/env python3
"""
Oracle Analysis Script:
1. Fetch ALL ConditionPreparation events and deduplicate by condition_id
2. For each of 3 oracles: count their prepared conditions
3. Match prepared conditions to CSV rows by condition_id (tracks token_id1, token_id2)
4. Match token_ids to TokenRegistered events and count per oracle
5. Report breakdown: CTFE vs NegRisk per oracle
"""

import os
import asyncio
import asyncpg
import pandas as pd
import logging
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

# ----------------------------------------------------------------
# CSV LOADING
# ----------------------------------------------------------------

def load_market_data(csv_path):
    """Load market data from CSV with conditionId and token_ids."""
    logger.info(f"Loading market data from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, usecols=['conditionId', 'token_id1', 'token_id2'])
        df['conditionId'] = df['conditionId'].str.lower()
        
        # Debug: Show data types and samples
        logger.info(f"  token_id1 dtype: {df['token_id1'].dtype}")
        logger.info(f"  token_id2 dtype: {df['token_id2'].dtype}")
        sample_tokens = sorted(df['token_id1'].dropna().unique().tolist())[:3]
        logger.info(f"  token_id1 samples: {sample_tokens}")
        
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
        CTFE_ADDR = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
        NEGRISK_ADDR = "0x9e4a16ddf5a656a3fc1af9872b2c207024558bd0"
        
        df_all_token_events['exchange'] = df_all_token_events['contract'].apply(
            lambda x: 'CTFE' if x.lower() == CTFE_ADDR.lower() else 'NegRisk'
        )
        
        logger.info(f"\nTokenRegistered Events (all oracles) by Exchange:")
        logger.info(f"  CTFE: {(df_all_token_events['exchange'] == 'CTFE').sum():,}")
        logger.info(f"  NegRisk: {(df_all_token_events['exchange'] == 'NegRisk').sum():,}")
        
        # Debug: Show data types and samples from token_reg table
        logger.info(f"\nDebug - Token_reg table samples:")
        logger.info(f"  token0 dtype: {df_all_token_events['token0'].dtype}")
        logger.info(f"  token1 dtype: {df_all_token_events['token1'].dtype}")
        sample_token0 = sorted(df_all_token_events['token0'].dropna().unique().tolist())[:3]
        sample_token1 = sorted(df_all_token_events['token1'].dropna().unique().tolist())[:3]
        logger.info(f"  token0 samples: {sample_token0}")
        logger.info(f"  token1 samples: {sample_token1}")
        
        # ====================================================================
        # STEP 4: Build comprehensive summary per oracle
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 4: Comprehensive Summary by Oracle (Using Sorted Merge Matching)")
        logger.info("=" * 80)
        
        results = []
        
        for oracle_addr, oracle_name in sorted(ORACLE_ADDRESSES.items()):
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
            
            logger.info(f"\n{oracle_name}:")
            logger.info(f"  Prepared Conditions: {prepared_count:,}")
            logger.info(f"  Matched to CSV: {matched_count:,}")
            logger.info(f"  Unique Token IDs: {len(oracle_token_ids):,}")
            
            # Debug: Show samples of token IDs from CSV
            if oracle_token_ids:
                csv_token_samples = sorted(list(oracle_token_ids))[:5]
                logger.info(f"  CSV Token ID Samples (first 5): {csv_token_samples}")
            
            # Debug: Show samples of token IDs from token_reg table
            if not df_all_token_events.empty:
                token_reg_samples = sorted(df_all_token_events['token0'].unique().tolist())[:5]
                logger.info(f"  Token_reg table token0 samples (first 5): {token_reg_samples}")
            
            # Find TokenRegistered events for this oracle's token_ids (using sorted merge)
            if len(oracle_token_ids) == 0:
                ctfe_count = 0
                negrisk_count = 0
                total_token_count = 0
            else:
                # Use sorted merge matching - much faster than database query
                df_oracle_tokens = match_token_ids_sorted_merge(df_all_token_events, oracle_token_ids)
                
                ctfe_count = (df_oracle_tokens['exchange'] == 'CTFE').sum()
                negrisk_count = (df_oracle_tokens['exchange'] == 'NegRisk').sum()
                total_token_count = len(df_oracle_tokens)
            
            logger.info(f"  TokenRegistered Events: {total_token_count:,}")
            logger.info(f"    ├─ CTFE: {ctfe_count:,}")
            logger.info(f"    └─ NegRisk: {negrisk_count:,}")
            
            results.append({
                'Oracle': oracle_name,
                'Prepared': f"{prepared_count:,}",
                'CSV Matches': f"{matched_count:,}",
                'Unique Tokens': f"{len(oracle_token_ids):,}",
                'Token Events': f"{total_token_count:,}",
                'CTFE': f"{ctfe_count:,}",
                'NegRisk': f"{negrisk_count:,}",
            })
        
        # ====================================================================
        # STEP 5: Summary Table
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
