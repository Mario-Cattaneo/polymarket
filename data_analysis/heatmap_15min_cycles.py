import os
import asyncio
import asyncpg
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
import logging
from datetime import datetime, timedelta, timezone
import re

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Configuration ---
PG_HOST = os.getenv("PG_SOCKET", "localhost")
PG_PORT = os.getenv("POLY_PG_PORT", 5432)
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# Heatmap Configuration
Y_DELTA = 0.01  # Price bucket size
X_DELTA_S = 1   # Time bucket size in seconds
BATCH_CYCLES = 1  # Number of 15-minute cycles to fetch
CYCLE_DURATION_SECONDS = 15 * 60  # 15 minutes in seconds

HEATMAP_DIR = "heatmaps_15min"
os.makedirs(HEATMAP_DIR, exist_ok=True)

# --- Helper Functions ---

def utc_to_et(utc_dt):
    """Convert UTC datetime to ET (UTC-5)."""
    return utc_dt - timedelta(hours=5)

def format_et_time(et_dt):
    """Format ET datetime as HH:MMAM/PM."""
    hour = et_dt.hour
    minute = et_dt.minute
    ampm = "AM" if hour < 12 else "PM"
    if hour > 12:
        hour -= 12
    elif hour == 0:
        hour = 12
    return f"{hour}:{minute:02d}{ampm}"

def construct_polymarket_question_pattern(et_start_dt):
    """Construct regex pattern for Polymarket question matching this 15-min window."""
    month_name = et_start_dt.strftime("%B")
    day = et_start_dt.day
    start_time = format_et_time(et_start_dt)
    end_time = format_et_time(et_start_dt + timedelta(minutes=15))
    
    pattern = f"Bitcoin Up or Down\\s*-\\s*{month_name}\\s+{day},?\\s+{re.escape(start_time)}-{re.escape(end_time)}\\s+ET"
    return pattern

def construct_kalshi_ticker_pattern(et_start_dt):
    """Construct Kalshi ticker for this 15-min window (uses end time)."""
    et_end_dt = et_start_dt + timedelta(minutes=15)
    
    year_short = et_end_dt.strftime("%y")
    month_abbr = et_end_dt.strftime("%b").upper()
    day = et_end_dt.day
    hour = et_end_dt.hour
    minute = et_end_dt.minute
    
    ticker_start = f"KXBTC15M-{year_short}{month_abbr}{day:02d}{hour:02d}{minute:02d}"
    return ticker_start

def round_to_15min(dt):
    """Round datetime to nearest 15-minute boundary."""
    minute = dt.minute
    if minute < 15:
        new_minute = 0
    elif minute < 30:
        new_minute = 15
    elif minute < 45:
        new_minute = 30
    else:
        new_minute = 45
    return dt.replace(minute=new_minute, second=0, microsecond=0)

def next_15min(dt):
    """Get next 15-minute boundary."""
    rounded = round_to_15min(dt)
    if rounded <= dt:
        rounded += timedelta(minutes=15)
    return rounded

# --- Database Connection ---

async def connect_db():
    """Creates a connection pool to the database."""
    return await asyncpg.create_pool(
        host=PG_HOST,
        port=int(PG_PORT),
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        min_size=1,
        max_size=5
    )

# --- Market Analysis ---

async def get_polymarket_coverage_info(pool):
    """Get comprehensive market coverage information across both Polymarket tables.
    
    This function analyzes both poly_markets and poly_new_market tables to determine:
    1. First available Bitcoin 15-min market (earliest)
    2. Last available Bitcoin 15-min market (latest)
    3. Any gaps in the market sequence
    
    Implementation details:
    - poly_markets: HTTP REST API scraped markets (first N markets, older dates)
    - poly_new_market: WebSocket discovered markets (newer, has gaps but updated in real-time)
    - Market questions use ET times, extracted and normalized for analysis
    
    Returns:
        dict: {
            'first_utc': datetime,      # First market's start time in UTC
            'last_utc': datetime,       # Last market's start time in UTC
            'first_question': str,      # Question text
            'last_question': str,       # Question text
            'gaps': [                   # List of gaps (missing 15-min intervals)
                {'prev_utc': dt, 'next_utc': dt, 'gap_minutes': int, 'prev_question': str, 'next_question': str},
                ...
            ]
        }
        Returns None if no Bitcoin 15-min markets found.
    
    Raises:
        Exception: On database errors
    """
    async with pool.acquire() as conn:
        # Fetch all Bitcoin 15-min markets from both tables, with timestamps
        rows = await conn.fetch("""
            SELECT 
                market_id,
                (message->>'timestamp')::BIGINT as timestamp_ms,
                message->>'question' as question,
                'poly_markets' as source
            FROM poly_markets
            WHERE message->>'question' ILIKE '%Bitcoin Up or Down%'
            UNION ALL
            SELECT 
                market_id,
                (server_time_us / 1000)::BIGINT as timestamp_ms,
                message->>'question' as question,
                'poly_new_market' as source
            FROM poly_new_market
            WHERE message->>'question' ILIKE '%Bitcoin Up or Down%'
            ORDER BY timestamp_ms ASC
        """)
    
    if not rows:
        logger.info("No Bitcoin 15-min markets found in either table")
        return None
    
    # Parse each market to extract start time in ET
    markets_parsed = []
    for row in rows:
        question = row['question']
        timestamp_ms = row['timestamp_ms']
        
        try:
            # Extract ET start time from question: "Bitcoin Up or Down - Month Day, HH:MMAM/PM-..."
            match = re.search(r'(\d+):(\d+)(AM|PM)', question, re.IGNORECASE)
            if not match:
                continue
            
            hour = int(match.group(1))
            minute = int(match.group(2))
            ampm = match.group(3).upper()
            
            # Convert to 24-hour ET
            if ampm == 'PM' and hour != 12:
                hour += 12
            elif ampm == 'AM' and hour == 12:
                hour = 0
            
            et_minute_of_day = hour * 60 + minute
            
            # Convert timestamp to UTC datetime, then back to see what UTC time corresponds to this ET
            utc_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            
            markets_parsed.append({
                'utc_dt': utc_dt,
                'et_minute': et_minute_of_day,
                'question': question,
                'source': row['source'],
                'market_id': row['market_id']
            })
        except Exception as e:
            logger.debug(f"Failed to parse market: {question}: {e}")
            continue
    
    if not markets_parsed:
        logger.info("No parseable Bitcoin 15-min markets found")
        return None
    
    first_market = markets_parsed[0]
    last_market = markets_parsed[-1]
    
    # Detect gaps (should be exactly 15 minutes between consecutive markets)
    gaps = []
    for i in range(1, len(markets_parsed)):
        prev = markets_parsed[i - 1]
        curr = markets_parsed[i]
        
        curr_et_min = curr['et_minute']
        prev_et_min = prev['et_minute']
        
        # Handle day wrap-around (11:45PM -> 12:00AM = 1425 -> 0)
        if curr_et_min < prev_et_min:
            curr_et_min += 24 * 60
        
        gap_minutes = curr_et_min - prev_et_min
        
        if gap_minutes != 15:
            gaps.append({
                'prev_utc': prev['utc_dt'],
                'next_utc': curr['utc_dt'],
                'gap_minutes': gap_minutes,
                'prev_question': prev['question'],
                'next_question': curr['question']
            })
    
    return {
        'first_utc': first_market['utc_dt'],
        'last_utc': last_market['utc_dt'],
        'first_question': first_market['question'],
        'last_question': last_market['question'],
        'gaps': gaps
    }

async def analyze_poly_markets_coverage():
    """Analyze poly_markets table: find first/last market and detect gaps."""
    pool = await connect_db()
    
    try:
        async with pool.acquire() as conn:
            # Find all markets with questions and timestamps
            rows = await conn.fetch("""
                SELECT 
                    message->>'question' as question,
                    (message->>'timestamp')::BIGINT as timestamp_ms
                FROM poly_markets
                WHERE message->>'question' ILIKE '%Bitcoin Up or Down%'
                ORDER BY (message->>'timestamp')::BIGINT
            """)
        
        if not rows:
            logger.info("No Bitcoin markets found in poly_markets")
            return None, None, []
        
        # Parse questions to extract start times and dates
        markets_data = []
        for row in rows:
            question = row['question']
            timestamp_ms = row['timestamp_ms']
            
            # Extract time from question: "Bitcoin Up or Down - Month Day, HH:MMAM/PM-HH:MMAM/PM ET"
            try:
                # Match first time: HH:MMAM/PM
                match = re.search(r'(\d+):(\d+)(AM|PM)', question, re.IGNORECASE)
                if match:
                    hour = int(match.group(1))
                    minute = int(match.group(2))
                    ampm = match.group(3).upper()
                    
                    # Convert to 24-hour ET
                    if ampm == 'PM' and hour != 12:
                        hour += 12
                    elif ampm == 'AM' and hour == 12:
                        hour = 0
                    
                    markets_data.append({
                        'question': question,
                        'timestamp_ms': timestamp_ms,
                        'start_minute': hour * 60 + minute  # Minutes since midnight ET
                    })
            except Exception as e:
                logger.warning(f"Failed to parse question: {question}: {e}")
        
        if not markets_data:
            return None, None, []
        
        first_market = markets_data[0]
        last_market = markets_data[-1]
        
        logger.info(f"First market: {first_market['question']}")
        logger.info(f"Last market: {last_market['question']}")
        
        # Detect gaps - markets should be every 15 minutes
        gaps = []
        for i in range(1, len(markets_data)):
            curr = markets_data[i]
            prev = markets_data[i - 1]
            
            curr_min = curr['start_minute']
            prev_min = prev['start_minute']
            
            # Handle day wrap-around
            if curr_min < prev_min:
                curr_min += 24 * 60
            
            expected_diff = 15
            actual_diff = curr_min - prev_min
            
            if actual_diff != expected_diff:
                gaps.append({
                    'prev': prev['question'],
                    'curr': curr['question'],
                    'expected_diff_min': expected_diff,
                    'actual_diff_min': actual_diff
                })
        
        if gaps:
            logger.warning(f"Found {len(gaps)} gaps in market coverage:")
            for gap in gaps[:5]:  # Show first 5
                logger.warning(f"  Gap: {gap['actual_diff_min']}min instead of {gap['expected_diff_min']}min")
                logger.warning(f"    After: {gap['prev'][:60]}...")
                logger.warning(f"    Next:  {gap['curr'][:60]}...")
        else:
            logger.info("No gaps detected - continuous 15-minute market coverage")
        
        return first_market, last_market, gaps
    
    finally:
        await pool.close()

async def get_polymarket_yes_asset_id(pool, utc_15min_start):
    """Get the asset ID for the YES/UP outcome of a 15-min Bitcoin cycle.
    
    This method transparently searches both Polymarket tables to find the market for the given UTC timestamp.
    The markets are split across two tables:
    - poly_markets: Contains the first N historically scraped markets from HTTP REST API
    - poly_new_market: Contains WebSocket-discovered markets (more recent, has gaps)
    
    IMPORTANT: Market questions use ET times, not UTC. This method converts the input UTC timestamp to ET
    before matching against market questions.
    
    Args:
        pool: Database connection pool
        utc_15min_start: UTC datetime rounded to 15-minute boundary (0, 15, 30, or 45 minutes)
                         Example: 2026-01-08T00:30:00Z
    
    Returns:
        str: Asset ID (as string) for the YES/UP outcome, or None if market not found (gap)
    
    Raises:
        Exception: On database errors
    
    Example:
        >>> utc_start = datetime(2026, 1, 8, 0, 30, tzinfo=timezone.utc)
        >>> asset_id = await get_polymarket_yes_asset_id(pool, utc_start)
        >>> # asset_id is the first element of assets_ids array (YES outcome)
        >>> # Returns None if no market exists for this 15-min cycle (gap period)
    """
    # Convert UTC to ET (UTC-5)
    et_dt = utc_to_et(utc_15min_start)
    et_end_dt = et_dt + timedelta(minutes=15)
    
    # Extract time components from ET datetime for matching
    month_name = et_dt.strftime("%B")
    day = et_dt.day
    start_time_str = format_et_time(et_dt)
    end_time_str = format_et_time(et_end_dt)
    
    # Build a pattern-like search string (will use LIKE matching)
    search_pattern = f"%Bitcoin Up or Down%{month_name}%{day}%{start_time_str}%{end_time_str}%ET%"
    
    async with pool.acquire() as conn:
        # Search both tables with UNION - returns from poly_markets first, then poly_new_market
        markets = await conn.fetch("""
            SELECT message FROM poly_markets 
            WHERE message->>'question' ILIKE $1
            UNION ALL
            SELECT message FROM poly_new_market 
            WHERE message->>'question' ILIKE $1
            LIMIT 1
        """, search_pattern)
        
        if not markets:
            logger.debug(f"No market found for UTC {utc_15min_start} (ET {month_name} {day}, {start_time_str}-{end_time_str})")
            return None
        
        try:
            market_msg = json.loads(markets[0]['message'])
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Failed to parse market message for UTC {utc_15min_start}: {e}")
            return None
        
        # Extract asset IDs - try both field names since tables use different field names
        # poly_new_market uses: assets_ids (JSON array directly)
        # poly_markets uses: clobTokenIds (JSON string containing array)
        asset_ids = None
        
        # First try assets_ids (from poly_new_market)
        assets_ids_field = market_msg.get('assets_ids')
        if assets_ids_field:
            asset_ids = assets_ids_field
        
        # Fallback to clobTokenIds (from poly_markets)
        if not asset_ids:
            clob_token_ids_raw = market_msg.get('clobTokenIds')
            if clob_token_ids_raw:
                try:
                    # clobTokenIds is a JSON string, need to parse it
                    asset_ids = json.loads(clob_token_ids_raw) if isinstance(clob_token_ids_raw, str) else clob_token_ids_raw
                except (json.JSONDecodeError, TypeError):
                    asset_ids = None
        
        if not asset_ids or len(asset_ids) == 0:
            logger.warning(f"No asset IDs found for UTC {utc_15min_start} (ET {month_name} {day}, {start_time_str}-{end_time_str})")
            return None
        
        # Return first asset ID (YES/UP outcome in ["Up", "Down"] pair)
        yes_asset_id = asset_ids[0]
        logger.debug(f"Found YES asset ID for UTC {utc_15min_start}: {yes_asset_id[:40]}...")
        return yes_asset_id

# --- Data Fetching ---

async def get_polymarket_asset_id(pool, utc_dt):
    """Deprecated: Use get_polymarket_yes_asset_id instead.
    Get the first asset_id for the 15-minute Polymarket cycle starting at utc_dt."""
    return await get_polymarket_yes_asset_id(pool, utc_dt)

async def get_kalshi_ticker(pool, utc_dt):
    """Get the Kalshi market ticker for the 15-minute cycle starting at utc_dt.
    
    Given a UTC timestamp, this constructs the ticker pattern based on the ET end time
    of the 15-minute interval and searches the kalshi_markets_3 table for a matching market.
    
    Args:
        pool: Database connection pool
        utc_dt: UTC datetime for the cycle start
        
    Returns:
        Market ticker string, or None if not found
    """
    et_dt = utc_to_et(utc_dt)
    ticker_pattern = construct_kalshi_ticker_pattern(et_dt)
    
    async with pool.acquire() as conn:
        markets = await conn.fetch(
            "SELECT market_ticker FROM kalshi_markets_3 WHERE market_ticker LIKE $1 LIMIT 1",
            ticker_pattern + '%'
        )
        
        if not markets:
            logger.warning(f"No Kalshi market found for {utc_dt}")
            return None
        
        return markets[0]['market_ticker']

async def fetch_kalshi_orderbook_updates(pool, utc_cycle_start, ticker=None):
    """Fetch and filter Kalshi orderbook updates for a 15-minute cycle.
    
    Given a UTC timestamp for a 15-minute cycle start, this:
    1. Constructs the market ticker pattern from the ET end time (or uses provided ticker)
    2. Queries kalshi_orderbook_updates_3 for all updates in that 15-minute interval
    3. Filters to only updates matching that specific ticker
    4. Returns the filtered updates as raw records with server_time_us and message
    
    The 15-minute cycle uses the ET-converted time to determine which market matches,
    since Kalshi ticker uses the end time of the interval.
    
    Args:
        pool: Database connection pool
        utc_cycle_start: UTC datetime for 15-minute cycle start
        ticker: Optional ticker to use directly (if None, will be constructed)
        
    Returns:
        List of dicts with keys: server_time_us, message (JSON string)
    """
    # Get the ticker if not provided
    if ticker is None:
        ticker = await get_kalshi_ticker(pool, utc_cycle_start)
        if ticker is None:
            logger.warning(f"No Kalshi ticker found for {utc_cycle_start}")
            return []
    
    # Calculate cycle boundaries in microseconds
    cycle_start_us = int(utc_cycle_start.timestamp() * 1_000_000)
    cycle_end_us = cycle_start_us + (15 * 60 * 1_000_000)  # 15 minutes in microseconds
    
    async with pool.acquire() as conn:
        records = await conn.fetch(
            """SELECT server_time_us, message FROM kalshi_orderbook_updates_3 
               WHERE market_ticker = $1 AND server_time_us >= $2 AND server_time_us < $3 
               ORDER BY server_time_us ASC""",
            ticker, cycle_start_us, cycle_end_us
        )
    
    logger.info(f"Fetched {len(records)} Kalshi orderbook updates for {ticker} [{utc_cycle_start} to {utc_cycle_start + timedelta(minutes=15)})")
    return records

async def fetch_polymarket_orderbook_updates(pool, utc_cycle_start, asset_id=None):
    """Fetch and filter Polymarket orderbook updates for a 15-minute cycle.
    
    Given a UTC timestamp for a 15-minute cycle start, this:
    1. Gets the asset ID for the YES outcome using get_polymarket_yes_asset_id() (or uses provided asset_id)
    2. Queries poly_book_state for all updates in that 15-minute interval
    3. Filters to only updates matching that specific asset_id INSIDE the message JSON
    4. Returns the filtered updates as raw records with server_time_us and message
    
    Args:
        pool: Database connection pool
        utc_cycle_start: UTC datetime for 15-minute cycle start
        asset_id: Optional asset ID to use directly (if None, will be retrieved)
        
    Returns:
        List of dicts with keys: server_time_us, message (JSON string)
    """
    # Get the asset_id if not provided
    if asset_id is None:
        asset_id = await get_polymarket_yes_asset_id(pool, utc_cycle_start)
        if asset_id is None:
            logger.warning(f"No Polymarket market found for {utc_cycle_start}")
            return []
    
    # Calculate cycle boundaries in microseconds
    cycle_start_us = int(utc_cycle_start.timestamp() * 1_000_000)
    cycle_end_us = cycle_start_us + (15 * 60 * 1_000_000)  # 15 minutes in microseconds
    
    async with pool.acquire() as conn:
        # Use time range to enable partition pruning + composite index usage
        records = await conn.fetch(
            """SELECT server_time_us, message FROM poly_book_state 
               WHERE message->>'asset_id' = $1 
               AND server_time_us >= $2
               AND server_time_us < $3
               ORDER BY server_time_us ASC""",
            asset_id,
            cycle_start_us,
            cycle_end_us,
            timeout=30
        )
    
    return records
    
    logger.info(f"Fetched {len(filtered)} Polymarket orderbook updates for asset_id {asset_id[:40]}... [{utc_cycle_start} to {utc_cycle_start + timedelta(minutes=15)})")
    return filtered

async def fetch_polymarket_cycle_data(pool, asset_id, cycle_start_us, cycle_end_us):
    """Fetch poly_book_state data for a 15-minute cycle."""
    async with pool.acquire() as conn:
        records = await conn.fetch(
            "SELECT server_time_us, message FROM poly_book_state WHERE asset_id = $1 AND server_time_us >= $2 AND server_time_us < $3 ORDER BY server_time_us ASC",
            asset_id, cycle_start_us, cycle_end_us
        )
    
    data = []
    for row in records:
        try:
            msg = json.loads(row['message'])
            bids = msg.get('bids', [])
            asks = msg.get('asks', [])
            
            if bids and asks:
                yes_price = max(float(b['price']) for b in bids)
                no_price = min(float(a['price']) for a in asks)
                time_offset = (row['server_time_us'] - cycle_start_us) / 1_000_000  # Convert to seconds
                data.append({'time_offset': time_offset, 'yes_price': yes_price, 'no_price': no_price})
        except Exception as e:
            logger.debug(f"Error parsing book state: {e}")
    
    return data

async def fetch_kalshi_cycle_data(pool, ticker, cycle_start_us, cycle_end_us):
    """Fetch Kalshi orderbook data for a 15-minute cycle."""
    async with pool.acquire() as conn:
        records = await conn.fetch(
            "SELECT server_time_us, message FROM kalshi_orderbook_updates_3 WHERE market_ticker = $1 AND server_time_us >= $2 AND server_time_us < $3 ORDER BY server_time_us ASC",
            ticker, cycle_start_us, cycle_end_us
        )
    
    yes_book = {}
    no_book = {}
    data = []
    
    for row in records:
        try:
            wrapper = json.loads(row['message'])
            msg_type = wrapper.get('type')
            msg = wrapper.get('msg', {})
            
            if msg_type == 'orderbook_snapshot':
                yes_book = {i[0]: i[1] for i in msg.get('yes', [])}
                no_book = {i[0]: i[1] for i in msg.get('no', [])}
            elif msg_type == 'orderbook_delta':
                side = msg.get('side')
                price = msg.get('price')
                delta = msg.get('delta')
                target = yes_book if side == 'yes' else no_book
                new_qty = target.get(price, 0) + delta
                if new_qty <= 0:
                    if price in target:
                        del target[price]
                else:
                    target[price] = new_qty
            
            best_yes = (max(yes_book.keys()) / 100.0) if yes_book else np.nan
            best_no = (max(no_book.keys()) / 100.0) if no_book else np.nan
            
            if not np.isnan(best_yes) and not np.isnan(best_no):
                time_offset = (row['server_time_us'] - cycle_start_us) / 1_000_000
                data.append({'time_offset': time_offset, 'yes_price': best_yes, 'no_price': best_no})
        except Exception as e:
            logger.debug(f"Error parsing Kalshi data: {e}")
    
    return data

# --- Heatmap Generation ---

def create_heatmap(data, yes_col, no_col, title, filename, price_type='yes'):
    """Create a heatmap for price data over a 15-minute cycle."""
    if not data:
        logger.warning(f"No data for {title}")
        return
    
    df = pd.DataFrame(data)
    
    if price_type == 'yes':
        prices = df['yes_price'].values
    else:
        prices = df['no_price'].values
    
    times = df['time_offset'].values
    
    # Create bins
    time_bins = np.arange(0, CYCLE_DURATION_SECONDS + X_DELTA_S, X_DELTA_S)
    price_bins = np.arange(0, 1.0 + Y_DELTA, Y_DELTA)
    
    # Create 2D histogram
    heatmap, xedges, yedges = np.histogram2d(times, prices, bins=[time_bins, price_bins])
    
    # Plot
    fig, ax = plt.subplots(figsize=(14, 6))
    im = ax.imshow(heatmap.T, aspect='auto', origin='lower', cmap='YlOrRd',
                   extent=[xedges[0], xedges[-1], yedges[0], yedges[-1]],
                   interpolation='nearest')
    
    ax.set_xlabel('Time (seconds)', fontsize=12)
    ax.set_ylabel('Price', fontsize=12)
    ax.set_title(title, fontsize=14)
    
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('Frequency', fontsize=11)
    
    plt.tight_layout()
    path = os.path.join(HEATMAP_DIR, filename)
    plt.savefig(path, dpi=150)
    plt.close()
    
    logger.info(f"Saved heatmap: {path}")

def create_arbitrage_heatmap(data_list, title, filename, arb_type='internal'):
    """Create heatmap for arbitrage opportunities."""
    all_data = []
    
    for data in data_list:
        if not data:
            continue
        
        df = pd.DataFrame(data)
        
        # Check if required columns exist
        if 'yes_price' not in df.columns or 'no_price' not in df.columns or 'time_offset' not in df.columns:
            continue
        
        if arb_type == 'internal':
            # Internal: 1 - (yes + no)
            arb_values = 1.0 - (df['yes_price'] + df['no_price'])
        elif arb_type == 'mirror_yes':
            # Mirror yes: just use yes prices (will combine across markets later)
            arb_values = df['yes_price']
        elif arb_type == 'mirror_no':
            arb_values = df['no_price']
        
        times = df['time_offset'].values
        all_data.append({'times': times, 'values': arb_values.values})
    
    if not all_data:
        logger.warning(f"No data for {title}")
        return
    
    # Flatten data
    all_times = np.concatenate([d['times'] for d in all_data])
    all_values = np.concatenate([d['values'] for d in all_data])
    
    # Create bins
    time_bins = np.arange(0, CYCLE_DURATION_SECONDS + X_DELTA_S, X_DELTA_S)
    value_bins = np.arange(-0.5, 1.5 + Y_DELTA, Y_DELTA)
    
    # Create 2D histogram
    heatmap, xedges, yedges = np.histogram2d(all_times, all_values, bins=[time_bins, value_bins])
    
    # Plot
    fig, ax = plt.subplots(figsize=(14, 6))
    im = ax.imshow(heatmap.T, aspect='auto', origin='lower', cmap='RdYlGn',
                   extent=[xedges[0], xedges[-1], yedges[0], yedges[-1]],
                   interpolation='nearest')
    
    ax.set_xlabel('Time (seconds)', fontsize=12)
    ax.set_ylabel('Arbitrage Value', fontsize=12)
    ax.set_title(title, fontsize=14)
    
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('Frequency', fontsize=11)
    
    plt.tight_layout()
    path = os.path.join(HEATMAP_DIR, filename)
    plt.savefig(path, dpi=150)
    plt.close()
    
    logger.info(f"Saved arbitrage heatmap: {path}")

# --- Main ---

async def main(t_start=None, t_end=None):
    """
    Main function to generate heatmaps for BATCH_CYCLES 15-minute cycles.
    
    Pipeline:
    1. Find starting timestamp (aligned to full 15-min)
    2. Find market ticker and asset ID for this timestamp
    3. Fetch ALL data from timestamp to timestamp + 15 min
    4. LOCALLY filter by asset_id and market_ticker
    5. Extract prices and arbitrages
    6. Repeat with timestamp + 15 until t_end is reached
    """
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST]):
        logger.error("DB credentials missing")
        return
    
    pool = await connect_db()
    
    try:
        # Step 1: Determine start time (full 15-min boundary)
        if t_start is None:
            # Find the earliest timestamp with data in either table
            logger.info("Finding earliest timestamp with data in tables...")
            
            async with pool.acquire() as conn:
                poly_min = await conn.fetchval("SELECT MIN(server_time_us) FROM poly_book_state")
                kalshi_min = await conn.fetchval("SELECT MIN(server_time_us) FROM kalshi_orderbook_updates_3")
                
                logger.info(f"  Polymarket earliest: {poly_min}")
                logger.info(f"  Kalshi earliest: {kalshi_min}")
                
                # Use the earliest available timestamp
                earliest_us = None
                if poly_min and kalshi_min:
                    earliest_us = min(poly_min, kalshi_min)
                elif poly_min:
                    earliest_us = poly_min
                elif kalshi_min:
                    earliest_us = kalshi_min
                
                if not earliest_us:
                    logger.error("No data found in either table!")
                    return
                
                earliest_dt = datetime.fromtimestamp(earliest_us / 1_000_000, tz=timezone.utc)
                logger.info(f"Earliest data: {earliest_dt}")
                t_start = round_to_15min(earliest_dt)
                logger.info(f"Rounded to 15-min: {t_start}")
        else:
            t_start = round_to_15min(t_start)
        
        logger.info(f"Start time (UTC): {t_start}")
        
        # Determine end time (BATCH_CYCLES cycles from start)
        t_end = t_start + timedelta(minutes=15 * BATCH_CYCLES)
        logger.info(f"End time (UTC): {t_end}")
        logger.info(f"Generating heatmaps for {BATCH_CYCLES} cycles")
        
        # Collect data across all cycles
        all_poly_btc_data = []
        all_kalshi_btc_data = []
        
        # Step 6: Iterate through each 15-minute cycle
        current_time = t_start
        cycle_num = 0
        
        while current_time < t_end:
            cycle_num += 1
            cycle_start_us = int(current_time.timestamp() * 1_000_000)
            cycle_end_us = cycle_start_us + CYCLE_DURATION_SECONDS * 1_000_000
            
            logger.info(f"\n=== CYCLE {cycle_num} ===")
            logger.info(f"Time window: {current_time} to {current_time + timedelta(seconds=CYCLE_DURATION_SECONDS)}")
            logger.info(f"Time window (us): {cycle_start_us} to {cycle_end_us}")
            
            # Step 2a: Find Polymarket asset ID for this cycle
            logger.info("[Step 2a] Finding Polymarket asset ID...")
            et_dt = utc_to_et(current_time)
            et_str = format_et_time(et_dt)
            et_end = format_et_time(et_dt + timedelta(minutes=15))
            logger.info(f"ET time: {et_str} to {et_end}")
            
            poly_asset_id = None
            async with pool.acquire() as conn:
                search_pattern = f"%Bitcoin Up or Down%{et_dt.strftime('%B')}%{et_dt.day}%{et_str}%{et_end}%"
                logger.info(f"Search pattern: {search_pattern}")
                
                # Search both poly_markets (with clobTokenIds) and poly_new_market (with assets_ids)
                markets = await conn.fetch("""
                    SELECT market_id, message FROM poly_markets WHERE message->>'question' ILIKE $1
                    UNION
                    SELECT market_id, message FROM poly_new_market WHERE message->>'question' ILIKE $1
                    LIMIT 1
                """, search_pattern)
                
                logger.info(f"Found {len(markets)} markets matching pattern")
                
                if markets:
                    market = markets[0]
                    market_msg = json.loads(market['message'])
                    market_id = market['market_id']
                    
                    # Try assets_ids first (from poly_new_market), then clobTokenIds (from poly_markets)
                    asset_ids = market_msg.get('assets_ids', None)
                    
                    if asset_ids is None:
                        # Try clobTokenIds (poly_markets)
                        clob_token_ids_raw = market_msg.get('clobTokenIds', '[]')
                        try:
                            asset_ids = json.loads(clob_token_ids_raw) if isinstance(clob_token_ids_raw, str) else clob_token_ids_raw
                        except (json.JSONDecodeError, TypeError):
                            asset_ids = []
                    
                    if asset_ids:
                        poly_asset_id = asset_ids[0]
                        logger.info(f"✓ Found Polymarket {market_id} with asset_id: {poly_asset_id[:30]}...")
                    else:
                        logger.warning(f"✗ Market {market_id} found but no asset IDs extracted")
            
            if not poly_asset_id:
                logger.warning(f"✗ No Polymarket asset ID found for {current_time}")
            
            # Step 2b: Find Kalshi ticker for this cycle
            logger.info("[Step 2b] Finding Kalshi ticker...")
            kalshi_ticker = None
            async with pool.acquire() as conn:
                # Debug: show all Bitcoin tickers
                all_btc_tickers = await conn.fetch("SELECT market_ticker FROM kalshi_markets_3 WHERE market_ticker LIKE '%KXBTC%' LIMIT 10")
                logger.debug(f"Sample Kalshi Bitcoin tickers in DB: {len(all_btc_tickers)} total")
                for m in all_btc_tickers:
                    logger.debug(f"  - {m['market_ticker']}")
                
                # Query Kalshi markets - pattern should match KXBTC15M-YYMONDDHHMMM
                markets = await conn.fetch(
                    "SELECT market_ticker FROM kalshi_markets_3 WHERE market_ticker LIKE $1 LIMIT 10",
                    "KXBTC15M%"
                )
                logger.info(f"Found {len(markets)} Kalshi markets with KXBTC15M pattern")
                
                if markets:
                    for market in markets:
                        ticker = market['market_ticker']
                        logger.info(f"Checking ticker: {ticker}")
                        # For now, just take the first one
                        kalshi_ticker = ticker
                        logger.info(f"✓ Found Kalshi ticker: {kalshi_ticker}")
                        break
            
            if not kalshi_ticker:
                logger.warning(f"✗ No Kalshi ticker found for {current_time}")
            
            # Step 3: Fetch all data for this time window
            logger.info("[Step 3] Fetching raw data from database...")
            
            poly_raw_records = []
            if poly_asset_id:
                async with pool.acquire() as conn:
                    poly_raw_records = await conn.fetch(
                        "SELECT server_time_us, message FROM poly_book_state WHERE asset_id = $1 AND server_time_us >= $2 AND server_time_us < $3 ORDER BY server_time_us ASC",
                        poly_asset_id, cycle_start_us, cycle_end_us
                    )
                logger.info(f"  Polymarket: fetched {len(poly_raw_records)} raw records for asset_id={poly_asset_id}")
                if len(poly_raw_records) > 0:
                    logger.info(f"    First record: server_time_us={poly_raw_records[0]['server_time_us']}")
                    logger.info(f"    Last record: server_time_us={poly_raw_records[-1]['server_time_us']}")
            
            kalshi_raw_records = []
            if kalshi_ticker:
                async with pool.acquire() as conn:
                    kalshi_raw_records = await conn.fetch(
                        "SELECT server_time_us, message FROM kalshi_orderbook_updates_3 WHERE market_ticker = $1 AND server_time_us >= $2 AND server_time_us < $3 ORDER BY server_time_us ASC",
                        kalshi_ticker, cycle_start_us, cycle_end_us
                    )
                logger.info(f"  Kalshi: fetched {len(kalshi_raw_records)} raw records for ticker={kalshi_ticker}")
                if len(kalshi_raw_records) > 0:
                    logger.info(f"    First record: server_time_us={kalshi_raw_records[0]['server_time_us']}")
                    logger.info(f"    Last record: server_time_us={kalshi_raw_records[-1]['server_time_us']}")
                
                # Debug: check if ANY data exists for this ticker
                async with pool.acquire() as conn:
                    any_data = await conn.fetchval(f"SELECT COUNT(*) FROM kalshi_orderbook_updates_3 WHERE market_ticker = $1", kalshi_ticker)
                    logger.info(f"  Total Kalshi records for ticker (all time): {any_data}")
                    
                    # Get min/max timestamps
                    min_max = await conn.fetchrow(f"SELECT MIN(server_time_us) as min_time, MAX(server_time_us) as max_time FROM kalshi_orderbook_updates_3 WHERE market_ticker = $1", kalshi_ticker)
                    if min_max and min_max['min_time']:
                        logger.info(f"  Kalshi data range: {min_max['min_time']} to {min_max['max_time']}")
                        logger.info(f"  Query window: {cycle_start_us} to {cycle_end_us}")
                        logger.info(f"  Window start is {(cycle_start_us - min_max['min_time']) / 1_000_000 / 3600:.1f} hours after data start")
            
            # Step 4 & 5: LOCALLY filter and extract prices
            logger.info("[Step 4-5] Processing data locally...")
            
            poly_cycle_data = []
            if poly_raw_records:
                for row in poly_raw_records:
                    try:
                        msg = json.loads(row['message'])
                        bids = msg.get('bids', [])
                        asks = msg.get('asks', [])
                        
                        if bids and asks:
                            yes_price = max(float(b['price']) for b in bids)
                            no_price = min(float(a['price']) for a in asks)
                            time_offset = (row['server_time_us'] - cycle_start_us) / 1_000_000
                            poly_cycle_data.append({
                                'time_offset': time_offset,
                                'yes_price': yes_price,
                                'no_price': no_price
                            })
                    except Exception as e:
                        logger.debug(f"Error processing Polymarket record: {e}")
                
                logger.info(f"  Polymarket: extracted {len(poly_cycle_data)} price points")
                all_poly_btc_data.append(poly_cycle_data)
            
            kalshi_cycle_data = []
            if kalshi_raw_records:
                yes_book = {}
                no_book = {}
                
                for row in kalshi_raw_records:
                    try:
                        wrapper = json.loads(row['message'])
                        msg_type = wrapper.get('type')
                        msg = wrapper.get('msg', {})
                        
                        if msg_type == 'snapshot':
                            yes_book = {int(k): v for k, v in msg.get('yes_book', {}).items()}
                            no_book = {int(k): v for k, v in msg.get('no_book', {}).items()}
                        elif msg_type == 'delta':
                            for price, qty in msg.get('yes_delta', {}).items():
                                yes_book[int(price)] = qty if qty > 0 else yes_book.pop(int(price), None)
                            for price, qty in msg.get('no_delta', {}).items():
                                no_book[int(price)] = qty if qty > 0 else no_book.pop(int(price), None)
                        
                        if yes_book and no_book:
                            yes_price = max(yes_book.keys()) / 100.0
                            no_price = max(no_book.keys()) / 100.0
                            time_offset = (row['server_time_us'] - cycle_start_us) / 1_000_000
                            kalshi_cycle_data.append({
                                'time_offset': time_offset,
                                'yes_price': yes_price,
                                'no_price': no_price
                            })
                    except Exception as e:
                        logger.debug(f"Error processing Kalshi record: {e}")
                
                logger.info(f"  Kalshi: extracted {len(kalshi_cycle_data)} price points")
                all_kalshi_btc_data.append(kalshi_cycle_data)
            
            # Move to next cycle
            current_time += timedelta(minutes=15)
        
        # Generate heatmaps
        logger.info(f"\n=== GENERATING HEATMAPS ===")
        
        # Price heatmaps
        create_heatmap(pd.concat([pd.DataFrame(d) for d in all_poly_btc_data if d], ignore_index=True).to_dict('records') if all_poly_btc_data and any(all_poly_btc_data) else [],
                      'yes_price', 'no_price', 'Polymarket Bitcoin - Yes Price', '1_poly_btc_yes.png', 'yes')
        create_heatmap(pd.concat([pd.DataFrame(d) for d in all_poly_btc_data if d], ignore_index=True).to_dict('records') if all_poly_btc_data and any(all_poly_btc_data) else [],
                      'yes_price', 'no_price', 'Polymarket Bitcoin - No Price', '2_poly_btc_no.png', 'no')
        
        # Kalshi prices
        kalshi_all = pd.concat([pd.DataFrame(d) for d in all_kalshi_btc_data if d], ignore_index=True).to_dict('records') if all_kalshi_btc_data and any(all_kalshi_btc_data) else []
        create_heatmap(kalshi_all, 'yes_price', 'no_price', 'Kalshi Bitcoin - Yes Price', '3_kalshi_btc_yes.png', 'yes')
        create_heatmap(kalshi_all, 'yes_price', 'no_price', 'Kalshi Bitcoin - No Price', '4_kalshi_btc_no.png', 'no')
        
        # Internal arbitrage
        create_arbitrage_heatmap(all_poly_btc_data, 'Internal Arb: Polymarket Bitcoin (1 - yes - no)', '5_arb_internal_poly_btc.png', 'internal')
        create_arbitrage_heatmap(all_kalshi_btc_data, 'Internal Arb: Kalshi Bitcoin (1 - yes - no)', '6_arb_internal_kalshi_btc.png', 'internal')
        
        # Mirror arbitrage (Yes prices)
        create_arbitrage_heatmap([all_poly_btc_data, all_kalshi_btc_data], 'Mirror Arb: Bitcoin Yes Prices', '7_arb_mirror_yes.png', 'mirror_yes')
        
        # Mirror arbitrage (No prices)
        create_arbitrage_heatmap([all_poly_btc_data, all_kalshi_btc_data], 'Mirror Arb: Bitcoin No Prices', '8_arb_mirror_no.png', 'mirror_no')
        
        logger.info("\n✓ All heatmaps generated successfully!")
        
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
