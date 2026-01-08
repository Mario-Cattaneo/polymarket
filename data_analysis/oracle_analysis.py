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
    """Fetch TokenRegistered events matching token_ids (as token0 or token1)."""
    if not token_ids:
        return []
    
    query = """
        SELECT 
            LOWER(topics[1]) as token0,
            LOWER(topics[2]) as token1,
            topics[3] as condition_id,
            LOWER(contract_address) as contract,
            transaction_hash,
            timestamp_ms
        FROM events_ctf_exchange
        WHERE event_name = 'TokenRegistered'
        AND (LOWER(topics[1]) IN (SELECT LOWER(UNNEST($1::text[]))) 
             OR LOWER(topics[2]) IN (SELECT LOWER(UNNEST($1::text[]))))
        UNION ALL
        SELECT 
            LOWER(topics[1]) as token0,
            LOWER(topics[2]) as token1,
            topics[3] as condition_id,
            LOWER(contract_address) as contract,
            transaction_hash,
            timestamp_ms
        FROM events_neg_risk_exchange
        WHERE event_name = 'TokenRegistered'
        AND (LOWER(topics[1]) IN (SELECT LOWER(UNNEST($1::text[]))) 
             OR LOWER(topics[2]) IN (SELECT LOWER(UNNEST($1::text[]))))
    """
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, token_ids)
    
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
        
        # Initialize results structure
        results = {}
        
        # ====================================================================
        # STEP 1: Get prepared condition IDs per oracle
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: Prepared Condition IDs by Oracle")
        logger.info("=" * 80)
        
        for oracle_addr, oracle_name in ORACLE_ADDRESSES.items():
            logger.info(f"\nFetching conditions for {oracle_name} ({oracle_addr})...")
            prepared_conds = await fetch_all_prepared_conditions(pool, oracle_addr)
            
            logger.info(f"  Found {len(prepared_conds):,} prepared condition IDs")
            
            results[oracle_name] = {
                'oracle_addr': oracle_addr,
                'prepared_condition_ids': set(prepared_conds),
                'prepared_count': len(prepared_conds),
                'csv_matches': [],
                'csv_match_count': 0,
                'token_matches': [],
                'token_match_count': 0,
                'ctfe_count': 0,
                'negrisk_count': 0,
            }
        
        # ====================================================================
        # STEP 2: Match prepared conditions to CSV rows
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: Match Prepared Conditions to CSV Rows")
        logger.info("=" * 80)
        
        for oracle_name, data in results.items():
            prepared_set = data['prepared_condition_ids']
            matched_conditions = []
            
            for cond_id in prepared_set:
                if cond_id in csv_condition_map:
                    matched_conditions.append({
                        'condition_id': cond_id,
                        'token_id1': csv_condition_map[cond_id]['token_id1'],
                        'token_id2': csv_condition_map[cond_id]['token_id2'],
                    })
            
            data['csv_matches'] = matched_conditions
            data['csv_match_count'] = len(matched_conditions)
            
            logger.info(f"\n{oracle_name}:")
            logger.info(f"  Prepared Condition IDs: {data['prepared_count']:,}")
            logger.info(f"  Matched to CSV: {data['csv_match_count']:,}")
        
        # ====================================================================
        # STEP 3: Match token_ids to TokenRegistered events
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: Match Token IDs to TokenRegistered Events")
        logger.info("=" * 80)
        
        for oracle_name, data in results.items():
            # Collect all token_ids from matched CSV rows
            token_ids = set()
            for match in data['csv_matches']:
                token_ids.add(match['token_id1'])
                token_ids.add(match['token_id2'])
            
            logger.info(f"\n{oracle_name}:")
            logger.info(f"  Unique token_ids to search: {len(token_ids):,}")
            
            if not token_ids:
                logger.info("  No token_ids to search")
                continue
            
            # Fetch TokenRegistered events for these token_ids
            token_reg_events = await fetch_token_registered_events(pool, list(token_ids))
            
            # Categorize by exchange
            ctfe_events = [e for e in token_reg_events if e['contract'] == '0x4d97dcd97ec945f40cf65f87097ace5ea0476045']
            negrisk_events = [e for e in token_reg_events if e['contract'] != '0x4d97dcd97ec945f40cf65f87097ace5ea0476045']
            
            data['token_matches'] = token_reg_events
            data['token_match_count'] = len(token_reg_events)
            data['ctfe_count'] = len(ctfe_events)
            data['negrisk_count'] = len(negrisk_events)
            
            logger.info(f"  TokenRegistered events found: {len(token_reg_events):,}")
            logger.info(f"    - CTFE: {len(ctfe_events):,}")
            logger.info(f"    - NegRisk: {len(negrisk_events):,}")
        
        # ====================================================================
        # STEP 4: Summary Report
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("SUMMARY REPORT")
        logger.info("=" * 80)
        
        for oracle_name, data in results.items():
            logger.info(f"\n{oracle_name} ({data['oracle_addr']}):")
            logger.info(f"  ├─ Prepared Condition IDs: {data['prepared_count']:,}")
            logger.info(f"  ├─ Matched to CSV: {data['csv_match_count']:,}")
            logger.info(f"  └─ TokenRegistered Events: {data['token_match_count']:,}")
            logger.info(f"     ├─ CTFE: {data['ctfe_count']:,}")
            logger.info(f"     └─ NegRisk: {data['negrisk_count']:,}")
        
        # ====================================================================
        # DETAILED BREAKDOWN TABLE
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("DETAILED BREAKDOWN TABLE")
        logger.info("=" * 80)
        
        summary_data = []
        for oracle_name, data in results.items():
            summary_data.append({
                'Oracle': oracle_name,
                'Prepared Conditions': f"{data['prepared_count']:,}",
                'CSV Matches': f"{data['csv_match_count']:,}",
                'Token Events': f"{data['token_match_count']:,}",
                'CTFE': f"{data['ctfe_count']:,}",
                'NegRisk': f"{data['negrisk_count']:,}",
            })
        
        summary_df = pd.DataFrame(summary_data)
        logger.info("\n" + summary_df.to_string(index=False))
        
    finally:
        await pool.close()

# ----------------------------------------------------------------
# ENTRY POINT
# ----------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(main())
