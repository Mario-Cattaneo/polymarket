#!/usr/bin/env python3
"""
Oracle Analysis Script (Condition ID Version):
Simplified approach using condition_id matching instead of token_id matching.

1. Fetch ALL ConditionPreparation events and deduplicate by condition_id
2. For each of 3 oracles: count their prepared conditions
3. Match prepared conditions to CSV rows by condition_id
4. Fetch TokenRegistered events and deduplicate by condition_id (one per exchange)
5. Match deduplicated TokenRegistered condition_ids to prepared conditions
6. Report breakdown: CTFE vs NegRisk per oracle
"""

import os
import asyncio
import asyncpg
import pandas as pd
import logging
from datetime import datetime, timezone

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
    
    TokenRegistered structure:
    - topics[0]: event signature hash
    - topics[1]: token0
    - topics[2]: token1
    - topics[3]: conditionId (the condition_id we need)
    
    Returns: DataFrame with condition_id, contract, and exchange categorization
    """
    query = """
        SELECT 
            LOWER(topics[3]) as condition_id,
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

# ----------------------------------------------------------------
# CSV LOADING
# ----------------------------------------------------------------

def load_market_data(csv_path):
    """Load market data from CSV with conditionId."""
    logger.info(f"Loading market data from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, usecols=['conditionId'])
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
        
        csv_cond_ids = set(csv_data['conditionId'].unique())
        logger.info(f"CSV contains {len(csv_cond_ids):,} unique condition_ids\n")
        
        # ====================================================================
        # STEP 1: Fetch ALL ConditionPreparation events and deduplicate
        # ====================================================================
        logger.info("=" * 80)
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
        
        prepared_cond_ids = set(df_prepared_dedup['condition_id'].unique())
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
        # STEP 3: Fetch ALL TokenRegistered events and deduplicate by condition_id
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: Fetch ALL TokenRegistered Events (Deduplicate by Condition_ID)")
        logger.info("=" * 80)
        
        df_all_token_events = await fetch_all_token_registered_events(pool)
        
        # Categorize by exchange using contract address
        CTFE_ADDR = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
        NEGRISK_ADDR = "0x9e4a16ddf5a656a3fc1af9872b2c207024558bd0"
        
        df_all_token_events['exchange'] = df_all_token_events['contract'].apply(
            lambda x: 'CTFE' if x.lower() == CTFE_ADDR.lower() else 'NegRisk'
        )
        
        if df_all_token_events.empty:
            logger.warning("No TokenRegistered events found!")
        else:
            logger.info(f"\nTokenRegistered Events (all exchanges):")
            logger.info(f"  Total events: {len(df_all_token_events):,}")
            logger.info(f"  CTFE: {(df_all_token_events['exchange'] == 'CTFE').sum():,}")
            logger.info(f"  NegRisk: {(df_all_token_events['exchange'] == 'NegRisk').sum():,}")
            
            # Deduplicate by condition_id (one per exchange type)
            # Note: "exactly one duplicate for every condition id" means one TokenRegistered
            # event per exchange (CTFE and NegRisk may both have same condition_id)
            df_token_dedup = df_all_token_events.drop_duplicates(subset='condition_id', keep='first')
            logger.info(f"  Unique condition_ids (deduplicated): {len(df_token_dedup):,}")
        
        # ====================================================================
        # STEP 4: Match TokenRegistered condition_ids to prepared conditions per oracle
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 4: Match TokenRegistered Condition_IDs to Prepared Conditions")
        logger.info("=" * 80)
        
        results = []
        
        for oracle_addr, oracle_name in sorted(ORACLE_ADDRESSES.items()):
            # Prepared conditions for this oracle
            oracle_prepared = df_prepared_dedup[df_prepared_dedup['oracle'] == oracle_addr]
            prepared_count = len(oracle_prepared)
            oracle_prepared_conds = set(oracle_prepared['condition_id'])
            
            # Matched conditions (to CSV) for this oracle
            oracle_matched = df_matched_prepared[df_matched_prepared['oracle'] == oracle_addr]
            matched_count = len(oracle_matched)
            oracle_matched_conds = set(oracle_matched['condition_id'])
            
            logger.info(f"\n{oracle_name}:")
            logger.info(f"  Prepared Conditions: {prepared_count:,}")
            logger.info(f"  Matched to CSV: {matched_count:,}")
            
            if df_all_token_events.empty:
                token_count = 0
                ctfe_count = 0
                negrisk_count = 0
            else:
                # Find TokenRegistered events whose condition_id matches this oracle's prepared conditions
                df_oracle_tokens = df_all_token_events[
                    df_all_token_events['condition_id'].isin(oracle_prepared_conds)
                ].copy()
                
                # Count by exchange
                ctfe_count = (df_oracle_tokens['exchange'] == 'CTFE').sum()
                negrisk_count = (df_oracle_tokens['exchange'] == 'NegRisk').sum()
                token_count = len(df_oracle_tokens)
            
            logger.info(f"  TokenRegistered Events (all exchanges): {token_count:,}")
            logger.info(f"    ├─ CTFE: {ctfe_count:,}")
            logger.info(f"    └─ NegRisk: {negrisk_count:,}")
            
            results.append({
                'Oracle': oracle_name,
                'Prepared': f"{prepared_count:,}",
                'CSV Matches': f"{matched_count:,}",
                'Token Events': f"{token_count:,}",
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
