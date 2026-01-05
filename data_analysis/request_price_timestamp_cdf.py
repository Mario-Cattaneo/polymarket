import os
import asyncio
import asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging
from datetime import datetime, timezone
import numpy as np
from hexbytes import HexBytes
from eth_abi import decode as abi_decode

# ----------------------------------------------------------------
# 1. SETUP & CONFIGURATION
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

# --- Database Credentials ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- Configuration ---
POLY_CSV_DIR = os.getenv("POLY_CSV")
POLY_CSV_PATH = os.path.join(POLY_CSV_DIR, "gamma_markets.csv") if POLY_CSV_DIR else "gamma_markets.csv"

# RequestPrice Event Definition
REQUEST_PRICE_EVENT = {
    "signature": "RequestPrice(address,bytes32,uint256,bytes,address,uint256,uint256)",
    "indexed_types": ["address"],
    "data_types": ["bytes32", "uint256", "bytes", "address", "uint256", "uint256"],
    "arg_names": ["requester", "identifier", "timestamp", "ancillaryData", "currency", "reward", "finalFee"]
}

# ----------------------------------------------------------------
# 2. DATABASE FUNCTIONS
# ----------------------------------------------------------------

async def get_db_pool(pool_size: int = 5):
    """Creates an asyncpg database connection pool."""
    try:
        for env_var in [PG_HOST, PG_PORT, DB_NAME, DB_USER, DB_PASS]:
            if not env_var:
                logger.error(f"Missing required environment variable")
                return None
        pool = await asyncpg.create_pool(
            host=PG_HOST,
            port=PG_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            min_size=pool_size,
            max_size=pool_size
        )
        return pool
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        raise e

def decode_argument(arg_type: str, value) -> str:
    """Decodes a single argument into a more readable format."""
    if isinstance(value, bytes):
        if "bytes32" in arg_type:
            try:
                return value.strip(b'\x00').decode('utf-8')
            except UnicodeDecodeError:
                return value.hex()
        else:
            try:
                decoded_string = value.decode('utf-8')
                return ''.join(char for char in decoded_string if char.isprintable())
            except UnicodeDecodeError:
                return f"<Non-UTF8 Bytes, len: {len(value)}>"
    return str(value)

# ----------------------------------------------------------------
# 3. FETCH & DECODE REQUESTPRICE EVENTS
# ----------------------------------------------------------------

async def fetch_request_price_events(db_pool):
    """Fetch all RequestPrice events from both oov2 and events_managed_oracle tables."""
    logger.info("Fetching RequestPrice events...")
    
    request_price_events = []
    
    try:
        async with db_pool.acquire() as conn:
            # Query oov2 table
            logger.info("Querying oov2 table for RequestPrice events...")
            query_oov2 = '''
                SELECT block_number, data, topics
                FROM "oov2"
                WHERE event_name = 'RequestPrice'
                ORDER BY block_number
            '''
            records_oov2 = await conn.fetch(query_oov2)
            logger.info(f"Found {len(records_oov2)} RequestPrice events in oov2")
            
            # Query events_managed_oracle table
            logger.info("Querying events_managed_oracle table for RequestPrice events...")
            query_emo = '''
                SELECT block_number, data, topics
                FROM "events_managed_oracle"
                WHERE event_name = 'RequestPrice'
                ORDER BY block_number
            '''
            records_emo = await conn.fetch(query_emo)
            logger.info(f"Found {len(records_emo)} RequestPrice events in events_managed_oracle")
            
            all_records = records_oov2 + records_emo
            
            # Decode each event
            for record in all_records:
                try:
                    block_timestamp = record.get('block_number')  # This is likely block number, not timestamp
                    topics = record.get('topics', [])
                    data_hex = record.get('data', '0x')
                    
                    if not topics or len(topics) < 2:
                        continue
                    
                    indexed_args_raw = [HexBytes(t) for t in topics[1:]]
                    data_bytes = HexBytes(data_hex)
                    
                    try:
                        decoded_data_args = abi_decode(REQUEST_PRICE_EVENT['data_types'], data_bytes)
                        decoded_indexed_args = [
                            abi_decode([dtype], val)[0] 
                            for dtype, val in zip(REQUEST_PRICE_EVENT['indexed_types'], indexed_args_raw)
                        ]
                        
                        # Reconstruct arguments
                        all_args = []
                        indexed_args_iter = iter(decoded_indexed_args)
                        data_args_iter = iter(decoded_data_args)
                        
                        for i, arg_name in enumerate(REQUEST_PRICE_EVENT['arg_names']):
                            if i < len(REQUEST_PRICE_EVENT['indexed_types']):
                                all_args.append(next(indexed_args_iter))
                            else:
                                all_args.append(next(data_args_iter))
                        
                        # Extract timestamp argument
                        all_types = REQUEST_PRICE_EVENT['indexed_types'] + REQUEST_PRICE_EVENT['data_types']
                        event_data = {}
                        for name, type_str, value in zip(REQUEST_PRICE_EVENT['arg_names'], all_types, all_args):
                            event_data[name] = value
                        
                        if 'timestamp' in event_data:
                            request_price_events.append({
                                'block_number': block_timestamp,
                                'event_timestamp': event_data['timestamp'],
                            })
                    
                    except Exception as e:
                        logger.warning(f"Failed to decode RequestPrice event: {e}")
                        continue
                
                except Exception as e:
                    logger.warning(f"Error processing record: {e}")
                    continue
    
    except Exception as e:
        logger.error(f"Failed to fetch RequestPrice events: {e}")
    
    return request_price_events

# ----------------------------------------------------------------
# 4. LOAD MARKET DATA
# ----------------------------------------------------------------

def load_market_data(csv_path: str):
    """Load market data from CSV file."""
    logger.info(f"Loading market data from {csv_path}...")
    
    try:
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} markets from CSV")
        
        # Check for timestamp columns
        timestamp_cols = [col for col in df.columns if 'time' in col.lower()]
        logger.info(f"Found timestamp columns: {timestamp_cols}")
        
        return df
    except Exception as e:
        logger.error(f"Failed to load market data: {e}")
        return None

# ----------------------------------------------------------------
# 5. CDF ANALYSIS
# ----------------------------------------------------------------

def compute_cdf(data, label="Data"):
    """Compute CDF for a dataset."""
    sorted_data = np.sort(data)
    y = np.arange(1, len(sorted_data) + 1) / len(sorted_data)
    return sorted_data, y

def plot_cdf_comparison(request_price_block_nums, request_price_timestamps, market_timestamps_list, market_timestamp_labels):
    """Plot CDF comparison of timestamps."""
    logger.info("Plotting CDF comparison...")
    
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    
    # CDF 1: RequestPrice Block Numbers
    if len(request_price_block_nums) > 0:
        x, y = compute_cdf(request_price_block_nums, "Block Number")
        axes[0].plot(x, y, label="RequestPrice Block Number", linewidth=2)
        axes[0].set_xlabel("Block Number")
        axes[0].set_ylabel("CDF")
        axes[0].set_title("RequestPrice Block Number Distribution")
        axes[0].grid(True, alpha=0.3)
        axes[0].legend()
    
    # CDF 2: RequestPrice Event Timestamps
    if len(request_price_timestamps) > 0:
        x, y = compute_cdf(request_price_timestamps, "Event Timestamp")
        axes[1].plot(x, y, label="RequestPrice Event Timestamp", linewidth=2)
        axes[1].set_xlabel("Timestamp (seconds since epoch)")
        axes[1].set_ylabel("CDF")
        axes[1].set_title("RequestPrice Event Timestamp Distribution")
        axes[1].grid(True, alpha=0.3)
        axes[1].legend()
    
    # CDF 3: Comparison of all timestamps
    if len(request_price_block_nums) > 0:
        x, y = compute_cdf(request_price_block_nums, "Block Number")
        axes[2].plot(x, y, label="RequestPrice Block Number", linewidth=2)
    
    if len(request_price_timestamps) > 0:
        x, y = compute_cdf(request_price_timestamps, "Event Timestamp")
        axes[2].plot(x, y, label="RequestPrice Event Timestamp", linewidth=2)
    
    for market_ts, label in zip(market_timestamps_list, market_timestamp_labels):
        if len(market_ts) > 0:
            market_ts_numeric = pd.to_datetime(market_ts).astype(np.int64) / 1e9
            x, y = compute_cdf(market_ts_numeric, label)
            axes[2].plot(x, y, label=label, linewidth=2, alpha=0.7)
    
    axes[2].set_xlabel("Timestamp Value")
    axes[2].set_ylabel("CDF")
    axes[2].set_title("All Timestamps Comparison")
    axes[2].grid(True, alpha=0.3)
    axes[2].legend()
    
    plt.tight_layout()
    plt.savefig('request_price_timestamp_cdf.png', dpi=300, bbox_inches='tight')
    logger.info("Saved plot to request_price_timestamp_cdf.png")
    plt.close()

# ----------------------------------------------------------------
# 6. MAIN ANALYSIS
# ----------------------------------------------------------------

async def main():
    logger.info("Starting RequestPrice Timestamp CDF Analysis...")
    
    # Connect to database
    db_pool = await get_db_pool()
    if not db_pool:
        logger.error("Failed to create database pool")
        return
    
    try:
        # Fetch RequestPrice events
        request_price_events = await fetch_request_price_events(db_pool)
        logger.info(f"Fetched {len(request_price_events)} RequestPrice events")
        
        if not request_price_events:
            logger.warning("No RequestPrice events found")
            return
        
        # Extract timestamps
        request_price_block_nums = np.array([ev['block_number'] for ev in request_price_events])
        request_price_timestamps = np.array([ev['event_timestamp'] for ev in request_price_events])
        
        logger.info(f"Block numbers: min={request_price_block_nums.min()}, max={request_price_block_nums.max()}")
        logger.info(f"Event timestamps: min={request_price_timestamps.min()}, max={request_price_timestamps.max()}")
        
        # Load market data
        market_df = load_market_data(POLY_CSV_PATH)
        
        market_timestamps_list = []
        market_timestamp_labels = []
        
        if market_df is not None:
            # Find timestamp columns
            timestamp_cols = [col for col in market_df.columns if 'time' in col.lower()]
            
            for col in timestamp_cols[:3]:  # Take first 3 timestamp columns
                try:
                    # Use format='mixed' to handle different datetime formats
                    market_ts = pd.to_datetime(market_df[col], format='mixed', utc=True)
                    market_timestamps_list.append(market_ts)
                    market_timestamp_labels.append(f"Market {col}")
                    logger.info(f"Market {col}: {len(market_ts)} entries")
                except Exception as e:
                    logger.warning(f"Failed to parse {col}: {e}")
        
        # Print statistics
        logger.info("\n" + "="*70)
        logger.info("REQUESTPRICE TIMESTAMP ANALYSIS")
        logger.info("="*70)
        logger.info(f"Total RequestPrice events: {len(request_price_events)}")
        logger.info(f"Block number range: {request_price_block_nums.min()} to {request_price_block_nums.max()}")
        logger.info(f"Event timestamp range: {request_price_timestamps.min()} to {request_price_timestamps.max()}")
        
        if market_df is not None:
            logger.info(f"Market data loaded: {len(market_df)} entries")
            logger.info(f"Market timestamp columns: {len(timestamp_cols)}")
        
        # Generate CDF plot
        plot_cdf_comparison(
            request_price_block_nums,
            request_price_timestamps,
            market_timestamps_list,
            market_timestamp_labels
        )
        
        logger.info("Analysis complete!")
    
    finally:
        if db_pool:
            await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())
