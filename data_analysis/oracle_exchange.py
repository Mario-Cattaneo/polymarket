#!/usr/bin/env python3
"""
Oracle vs TokenRegistered Matching Script:
1. Fetch ALL ConditionPreparation events and deduplicate by condition_id
2. For each of 3 oracles: count their prepared conditions
3. Fetch TokenRegistered events (batched by block range)
4. Match ConditionPreparation to TokenRegistered by condition_id
5. Report cardinalities: prep by oracle, tokens by exchange, matched/unmatched counts
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

# Conditional Tokens Contract
CT_ADDR = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower()

# Three Oracles of Interest
ORACLE_ADDRESSES = {
    "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7": "MOOV2 Adapter",
    "0x58e1745bedda7312c4cddb72618923da1b90efde": "Centralized Adapter",
    "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296": "Negrisk Adapter",
}

# Block Range Filtering for TokenRegistered Events
START_BLOCK_NR = 79172085
END_BLOCK_NR = 81225971
BLOCK_BATCH_SIZE = 100000
TOKEN_REG_TABLE = "token_reg"

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

async def fetch_prepared_conditions(pool, batch_start: int, batch_end: int):
    """Fetch ConditionPreparation events for a specific block range.
    
    ConditionPreparation event structure:
    - topics[2]: conditionId (indexed)
    - topics[3]: oracle (indexed, as 32-byte hash, need last 20 bytes)
    
    Returns: DataFrame with condition_id, timestamp_ms, oracle
    """
    query = f"""
        SELECT 
            LOWER(topics[2]) as condition_id, 
            timestamp_ms, 
            '0x' || substring(topics[3] from 27) as oracle
        FROM events_conditional_tokens 
        WHERE event_name = 'ConditionPreparation' 
        AND LOWER(contract_address) = '{CT_ADDR}'
        AND block_number >= {batch_start} 
        AND block_number <= {batch_end}
    """
    
    events = []
    async with pool.acquire() as conn:
        async with conn.transaction():
            async for record in conn.cursor(query):
                events.append(dict(record))
    
    df = pd.DataFrame(events) if events else pd.DataFrame()
    if not df.empty:
        df['oracle'] = df['oracle'].str.lower()
    return df

async def fetch_token_registered_events(pool, table_name: str, batch_start: int, batch_end: int):
    """Fetch TokenRegistered events from token_reg table for a specific block range.
    
    Extracts condition_id from topics[3] and contract_address to categorize by exchange.
    
    Token IDs in topics are stored as hex strings (32 bytes).
    """
    query = f"""
        SELECT 
            LOWER(topics[4]) as condition_id,
            LOWER(contract_address) as contract,
            timestamp_ms,
            block_number
        FROM {table_name}
        WHERE event_name = 'TokenRegistered' 
        AND block_number >= {batch_start} 
        AND block_number <= {batch_end}
    """
    
    events = []
    async with pool.acquire() as conn:
        async with conn.transaction():
            async for record in conn.cursor(query):
                events.append(dict(record))
    
    return pd.DataFrame(events) if events else pd.DataFrame()

# ----------------------------------------------------------------
# MAIN ANALYSIS
# ----------------------------------------------------------------

async def main():
    pool = await connect_db()
    
    try:
        # ====================================================================
        # STEP 1: Fetch ConditionPreparation events (batched by block range)
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: Fetch ConditionPreparation Events (Batched by Block Range)")
        logger.info("=" * 80)
        logger.info(f"Fetching ConditionPreparation events in batches of {BLOCK_BATCH_SIZE:,} blocks from {START_BLOCK_NR:,} to {END_BLOCK_NR:,}...")
        
        all_prep_events = []
        current_block = START_BLOCK_NR
        batch_num = 0
        
        while current_block <= END_BLOCK_NR:
            batch_num += 1
            batch_end = min(current_block + BLOCK_BATCH_SIZE - 1, END_BLOCK_NR)
            logger.info(f"Batch {batch_num}: blocks {current_block:,} to {batch_end:,}")
            
            batch_df = await fetch_prepared_conditions(pool, current_block, batch_end)
            if not batch_df.empty:
                all_prep_events.append(batch_df)
            logger.info(f"  Fetched {len(batch_df):,} ConditionPreparation events in this batch")
            
            current_block = batch_end + 1
        
        # Combine all batches
        if all_prep_events:
            df_all_prepared = pd.concat(all_prep_events, ignore_index=True)
        else:
            df_all_prepared = pd.DataFrame()
        
        logger.info(f"\nTotal ConditionPreparation events fetched: {len(df_all_prepared):,}")
        
        # Deduplicate by condition_id, keeping first event per condition
        df_prepared_dedup = df_all_prepared.drop_duplicates(subset='condition_id', keep='first')
        logger.info(f"After deduplication by condition_id: {len(df_prepared_dedup):,}")
        
        # Count by oracle
        logger.info("\nConditionPreparation events by Oracle:")
        oracle_prep_counts = {}
        for oracle_addr in sorted(ORACLE_ADDRESSES.keys()):
            count = len(df_prepared_dedup[df_prepared_dedup['oracle'] == oracle_addr])
            oracle_prep_counts[oracle_addr] = count
            alias = ORACLE_ADDRESSES.get(oracle_addr, oracle_addr)
            logger.info(f"  {alias}: {count:,}")
        
        # ====================================================================
        # STEP 2: Fetch TokenRegistered events (batched by block range)
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: Fetch TokenRegistered Events (Batched by Block Range)")
        logger.info("=" * 80)
        logger.info(f"Fetching TokenRegistered events in batches of {BLOCK_BATCH_SIZE:,} blocks from {START_BLOCK_NR:,} to {END_BLOCK_NR:,}...")
        
        all_token_events = []
        current_block = START_BLOCK_NR
        batch_num = 0
        
        while current_block <= END_BLOCK_NR:
            batch_num += 1
            batch_end = min(current_block + BLOCK_BATCH_SIZE - 1, END_BLOCK_NR)
            logger.info(f"Batch {batch_num}: blocks {current_block:,} to {batch_end:,}")
            
            batch_df = await fetch_token_registered_events(pool, TOKEN_REG_TABLE, current_block, batch_end)
            if not batch_df.empty:
                all_token_events.append(batch_df)
            logger.info(f"  Fetched {len(batch_df):,} TokenRegistered events in this batch")
            
            current_block = batch_end + 1
        
        # Combine all batches
        if all_token_events:
            df_all_tokens = pd.concat(all_token_events, ignore_index=True)
        else:
            df_all_tokens = pd.DataFrame()
        
        logger.info(f"\nTotal TokenRegistered events: {len(df_all_tokens):,}")
        
        # Categorize by exchange
        if not df_all_tokens.empty:
            df_all_tokens['exchange'] = df_all_tokens['contract'].apply(
                lambda x: 'CTFE' if x == CTFE_ADDR else 'Other'
            )
            
            logger.info("\nTokenRegistered events by Exchange:")
            logger.info(f"  CTFE: {(df_all_tokens['exchange'] == 'CTFE').sum():,}")
            logger.info(f"  Other: {(df_all_tokens['exchange'] == 'Other').sum():,}")
        
        logger.info(f"{'='*80}\n")
        
        # ====================================================================
        # STEP 3: Match ConditionPreparation to TokenRegistered by condition_id
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: Match Events by condition_id")
        logger.info("=" * 80)
        
        prep_condition_ids = set(df_prepared_dedup['condition_id'].unique())
        logger.info(f"Unique condition_ids in ConditionPreparation: {len(prep_condition_ids):,}")
        
        if not df_all_tokens.empty:
            token_condition_ids = set(df_all_tokens['condition_id'].unique())
            logger.info(f"Unique condition_ids in TokenRegistered: {len(token_condition_ids):,}")
            
            # Find matching conditions (have both prep and tokens)
            matching_condition_ids = prep_condition_ids & token_condition_ids
            logger.info(f"Conditions with BOTH Prep + Tokens: {len(matching_condition_ids):,}")
            
            # Unmatched
            unmatched_prep = prep_condition_ids - token_condition_ids
            unmatched_tokens = token_condition_ids - prep_condition_ids
            logger.info(f"Prep events with NO tokens: {len(unmatched_prep):,}")
            logger.info(f"Token events with NO prep: {len(unmatched_tokens):,}")
        else:
            matching_condition_ids = set()
            unmatched_prep = prep_condition_ids
            unmatched_tokens = set()
            logger.info(f"Unique condition_ids in TokenRegistered: 0")
            logger.info(f"Conditions with BOTH Prep + Tokens: 0")
            logger.info(f"Prep events with NO tokens: {len(unmatched_prep):,}")
            logger.info(f"Token events with NO prep: 0")
        
        # ====================================================================
        # STEP 4: Summary by Oracle and Exchange
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 4: Summary by Oracle and Exchange")
        logger.info("=" * 80)
        
        results = []
        
        for oracle_addr, oracle_name in sorted(ORACLE_ADDRESSES.items()):
            prep_count = oracle_prep_counts[oracle_addr]
            
            # Get matched conditions for this oracle
            oracle_prep_conditions = set(df_prepared_dedup[df_prepared_dedup['oracle'] == oracle_addr]['condition_id'])
            oracle_matched = oracle_prep_conditions & matching_condition_ids
            oracle_unmatched = oracle_prep_conditions - matching_condition_ids
            
            logger.info(f"\n{oracle_name}:")
            logger.info(f"  Total Prep Events: {prep_count:,}")
            logger.info(f"  Matched (with tokens): {len(oracle_matched):,}")
            logger.info(f"  Unmatched (no tokens): {len(oracle_unmatched):,}")
            
            # Get token counts for this oracle's matched conditions
            if not df_all_tokens.empty and len(oracle_matched) > 0:
                oracle_tokens = df_all_tokens[df_all_tokens['condition_id'].isin(oracle_matched)]
                ctfe_count = (oracle_tokens['exchange'] == 'CTFE').sum()
                other_count = (oracle_tokens['exchange'] == 'Other').sum()
                logger.info(f"  Token Events: {len(oracle_tokens):,}")
                logger.info(f"    ├─ CTFE: {ctfe_count:,}")
                logger.info(f"    └─ Other: {other_count:,}")
                
                results.append({
                    'Oracle': oracle_name,
                    'Prep Events': f"{prep_count:,}",
                    'Matched': f"{len(oracle_matched):,}",
                    'Unmatched': f"{len(oracle_unmatched):,}",
                    'Token Events': f"{len(oracle_tokens):,}",
                    'CTFE Tokens': f"{ctfe_count:,}",
                    'Other Tokens': f"{other_count:,}",
                })
            else:
                logger.info(f"  Token Events: 0")
                results.append({
                    'Oracle': oracle_name,
                    'Prep Events': f"{prep_count:,}",
                    'Matched': f"{len(oracle_matched):,}",
                    'Unmatched': f"{len(oracle_unmatched):,}",
                    'Token Events': '0',
                    'CTFE Tokens': '0',
                    'Other Tokens': '0',
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
        logger.info("Database connection closed.")

# ----------------------------------------------------------------
# ENTRY POINT
# ----------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(main())
