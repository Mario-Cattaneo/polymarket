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

async def fetch_token_registered_events(pool, token_ids):
    """Fetch TokenRegistered events matching token_ids (as token0 or token1).
    
    Uses CREATE TEMP TABLE for faster matching than IN clause with hundreds of thousands of values.
    """
    if not token_ids:
        return []
    
    async with pool.acquire() as conn:
        # Create temp table with token IDs
        await conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS token_id_lookup (token_id TEXT PRIMARY KEY);
        """)
        
        # Insert token IDs
        await conn.executemany(
            "INSERT INTO token_id_lookup (token_id) VALUES ($1) ON CONFLICT DO NOTHING",
            [(tid,) for tid in token_ids]
        )
        
        # Query using JOIN instead of IN clause
        query = """
            SELECT 
                LOWER(topics[1]) as token0,
                LOWER(topics[2]) as token1,
                topics[3] as condition_id,
                LOWER(contract_address) as contract,
                transaction_hash,
                timestamp_ms,
                'ctfe' as exchange_type
            FROM events_ctf_exchange
            WHERE event_name = 'TokenRegistered'
            AND (LOWER(topics[1]) IN (SELECT token_id FROM token_id_lookup)
                 OR LOWER(topics[2]) IN (SELECT token_id FROM token_id_lookup))
            UNION ALL
            SELECT 
                LOWER(topics[1]) as token0,
                LOWER(topics[2]) as token1,
                topics[3] as condition_id,
                LOWER(contract_address) as contract,
                transaction_hash,
                timestamp_ms,
                'negrisk' as exchange_type
            FROM events_neg_risk_exchange
            WHERE event_name = 'TokenRegistered'
            AND (LOWER(topics[1]) IN (SELECT token_id FROM token_id_lookup)
                 OR LOWER(topics[2]) IN (SELECT token_id FROM token_id_lookup))
        """
        
        rows = await conn.fetch(query)
        
        # Clean up
        await conn.execute("DROP TABLE IF EXISTS token_id_lookup")
        
        return rows

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
        # STEP 3: Match token_ids to TokenRegistered events
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: Match Token IDs to TokenRegistered Events")
        logger.info("=" * 80)
        
        # Collect all token_ids from matched prepared conditions' CSV rows
        token_ids = set()
        for cond_id in matching_cond_ids:
            if cond_id in csv_condition_map:
                token_ids.add(csv_condition_map[cond_id]['token_id1'])
                token_ids.add(csv_condition_map[cond_id]['token_id2'])
        
        logger.info(f"\nUnique token_ids to search: {len(token_ids):,}")
        
        # Fetch TokenRegistered events for these token_ids
        logger.info("Fetching TokenRegistered events for these token_ids...")
        token_reg_events = await fetch_token_registered_events(pool, list(token_ids))
        
        logger.info(f"TokenRegistered events found: {len(token_reg_events):,}")
        
        # Convert to DataFrame for easier analysis
        df_token_events = pd.DataFrame([dict(row) for row in token_reg_events])
        
        if df_token_events.empty:
            logger.warning("No TokenRegistered events found")
        else:
            logger.info(f"\nTokenRegistered Events by Exchange:")
            logger.info(f"  CTFE: {(df_token_events['exchange_type'] == 'ctfe').sum():,}")
            logger.info(f"  NegRisk: {(df_token_events['exchange_type'] == 'negrisk').sum():,}")
        
        # ====================================================================
        # STEP 4: Build comprehensive summary per oracle
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 4: Comprehensive Summary by Oracle")
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
            
            # Find TokenRegistered events for this oracle's token_ids
            if df_token_events.empty or len(oracle_token_ids) == 0:
                ctfe_count = 0
                negrisk_count = 0
                total_token_count = 0
            else:
                # Check if token0 or token1 matches oracle's token_ids
                mask_token0 = df_token_events['token0'].isin(oracle_token_ids)
                mask_token1 = df_token_events['token1'].isin(oracle_token_ids)
                mask_oracle_tokens = mask_token0 | mask_token1
                
                df_oracle_tokens = df_token_events[mask_oracle_tokens]
                
                ctfe_count = ((df_oracle_tokens['exchange'] == 'CTFE').sum())
                negrisk_count = ((df_oracle_tokens['exchange'] == 'NegRisk').sum())
                total_token_count = len(df_oracle_tokens)
            
            logger.info(f"\n{oracle_name}:")
            logger.info(f"  Prepared Conditions: {prepared_count:,}")
            logger.info(f"  Matched to CSV: {matched_count:,}")
            logger.info(f"  Unique Token IDs: {len(oracle_token_ids):,}")
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
