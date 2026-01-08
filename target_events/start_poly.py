import os
import asyncio
import asyncpg
import logging
import json
import time
import websockets
import aiohttp
import re

# --- DEPENDENCIES: pip install websockets aiohttp asyncpg ---

# --- Environment Variable Setup ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

# --- Monitor Logger Setup (dedicated file) ---
monitor_logger = logging.getLogger('monitor')
monitor_logger.setLevel(logging.INFO)
monitor_log_path = os.path.join(os.path.dirname(__file__), "monitor_poly.log")
# File handler will be recreated each monitoring cycle to clear the file

# --- Message Statistics Tracking ---
message_stats = {}  # {market_id: {message_type: count}}

# --- Polymarket API Configuration ---
POLY_REST_URL = "https://gamma-api.polymarket.com/markets"
POLY_WSS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
FETCH_LIMIT = 500

# --- Database Schema Definition (simplified with JSON storage) ---
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS poly_markets (
    market_id TEXT PRIMARY KEY,
    found_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS poly_new_market (
    id SERIAL PRIMARY KEY,
    market_id TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_new_market_market_id ON poly_new_market(market_id);
CREATE INDEX IF NOT EXISTS idx_poly_new_market_server_time ON poly_new_market(server_time_us);

CREATE TABLE IF NOT EXISTS poly_market_resolved (
    id SERIAL PRIMARY KEY,
    market_id TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_market_resolved_market_id ON poly_market_resolved(market_id);
CREATE INDEX IF NOT EXISTS idx_poly_market_resolved_server_time ON poly_market_resolved(server_time_us);

CREATE TABLE IF NOT EXISTS poly_price_change (
    id SERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_price_change_asset_id ON poly_price_change(asset_id);
CREATE INDEX IF NOT EXISTS idx_poly_price_change_server_time ON poly_price_change(server_time_us);

CREATE TABLE IF NOT EXISTS poly_best_bid_ask (
    id SERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_best_bid_ask_asset_id ON poly_best_bid_ask(asset_id);
CREATE INDEX IF NOT EXISTS idx_poly_best_bid_ask_server_time ON poly_best_bid_ask(server_time_us);

CREATE TABLE IF NOT EXISTS poly_book (
    id SERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_book_asset_id ON poly_book(asset_id);
CREATE INDEX IF NOT EXISTS idx_poly_book_server_time ON poly_book(server_time_us);

CREATE TABLE IF NOT EXISTS poly_last_trade_price (
    id SERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_last_trade_price_asset_id ON poly_last_trade_price(asset_id);
CREATE INDEX IF NOT EXISTS idx_poly_last_trade_price_server_time ON poly_last_trade_price(server_time_us);

CREATE TABLE IF NOT EXISTS poly_tick_size_change (
    id SERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_tick_size_change_asset_id ON poly_tick_size_change(asset_id);
CREATE INDEX IF NOT EXISTS idx_poly_tick_size_change_server_time ON poly_tick_size_change(server_time_us);

CREATE TABLE IF NOT EXISTS poly_book_state (
    id SERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_book_state_asset_id ON poly_book_state(asset_id);
CREATE INDEX IF NOT EXISTS idx_poly_book_state_server_time ON poly_book_state(server_time_us);
"""

# --- Helper Functions ---

def increment_message_stat(market_id, message_type):
    """Increments the message count for a specific market and message type."""
    if market_id not in message_stats:
        message_stats[market_id] = {}
    if message_type not in message_stats[market_id]:
        message_stats[market_id][message_type] = 0
    message_stats[market_id][message_type] += 1

def is_Bitcoin_15min_market(question: str) -> bool:
    """Check if a market question matches the Bitcoin Up or Down 15-minute format."""
    pattern = r"^Bitcoin Up or Down\s*-\s*\w+\s+\d+,?\s+(\d+):(\d+)([AP]M)-(\d+):(\d+)([AP]M)\s+ET$"
    match = re.match(pattern, question, re.IGNORECASE)
    if not match:
        return False
    
    # Parse start and end times
    start_hour = int(match.group(1))
    start_min = int(match.group(2))
    start_ampm = match.group(3).upper()
    end_hour = int(match.group(4))
    end_min = int(match.group(5))
    end_ampm = match.group(6).upper()
    
    # Convert to 24-hour format
    if start_ampm == 'PM' and start_hour != 12:
        start_hour += 12
    elif start_ampm == 'AM' and start_hour == 12:
        start_hour = 0
        
    if end_ampm == 'PM' and end_hour != 12:
        end_hour += 12
    elif end_ampm == 'AM' and end_hour == 12:
        end_hour = 0
    
    # Calculate time difference in minutes
    start_total_min = start_hour * 60 + start_min
    end_total_min = end_hour * 60 + end_min
    
    # Handle day wrap-around (11:45PM to 12:00AM)
    if end_total_min < start_total_min:
        end_total_min += 24 * 60
    
    diff_min = end_total_min - start_total_min
    
    # Must be exactly 15 minutes
    return diff_min == 15

# --- Database Functions ---

async def init_db(pool):
    """Initializes the database by creating tables if they don't exist."""
    async with pool.acquire() as connection:
        await connection.execute(CREATE_TABLES_SQL)
    logger.info("Database tables and indexes initialized successfully.")

async def upsert_market(pool, market_json):
    """Inserts or updates a market in the poly_markets table."""
    market_id = market_json.get('id')
    if not market_id:
        logger.warning(f"Market JSON missing id field: {market_json}")
        return
    
    await pool.execute("""
        INSERT INTO poly_markets (market_id, found_time_us, message)
        VALUES ($1, $2, $3)
        ON CONFLICT (market_id) DO UPDATE SET
            found_time_us = EXCLUDED.found_time_us,
            message = EXCLUDED.message;
    """, market_id, int(time.time() * 1_000_000), json.dumps(market_json))
    logger.info(f"Upserted market '{market_id}' - '{market_json.get('question', '')}' into the database.")

async def insert_new_market(pool, msg):
    """Inserts a new_market message into the poly_new_market table."""
    server_time_us = int(msg.get('timestamp', 0)) * 1000  # Convert ms to us
    await pool.execute("""
        INSERT INTO poly_new_market (market_id, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4)
    """, msg.get('id'), int(time.time() * 1_000_000), server_time_us, json.dumps(msg))

async def insert_market_resolved(pool, msg):
    """Inserts a market_resolved message into the poly_market_resolved table."""
    server_time_us = int(msg.get('timestamp', 0)) * 1000  # Convert ms to us
    await pool.execute("""
        INSERT INTO poly_market_resolved (market_id, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4)
    """, msg.get('id'), int(time.time() * 1_000_000), server_time_us, json.dumps(msg))

async def insert_price_change(pool, msg):
    """Inserts price_change messages into the poly_price_change table.
    
    The price_change message contains an array of price_changes, each with its own asset_id.
    We store each individual price change as a separate row.
    """
    server_time_us = int(msg.get('timestamp', 0)) * 1000  # Convert ms to us
    price_changes = msg.get('price_changes', [])
    
    for price_change in price_changes:
        asset_id = price_change.get('asset_id', '')
        if asset_id:
            await pool.execute("""
                INSERT INTO poly_price_change (asset_id, found_time_us, server_time_us, message)
                VALUES ($1, $2, $3, $4)
            """, asset_id, int(time.time() * 1_000_000), server_time_us, json.dumps(price_change))

async def insert_filtered_price_changes(pool, msg, filtered_price_changes):
    """Inserts only the filtered price_changes that match tracked assets."""
    server_time_us = int(msg.get('timestamp', 0)) * 1000  # Convert ms to us
    
    for price_change in filtered_price_changes:
        asset_id = price_change.get('asset_id', '')
        if asset_id:
            await pool.execute("""
                INSERT INTO poly_price_change (asset_id, found_time_us, server_time_us, message)
                VALUES ($1, $2, $3, $4)
            """, asset_id, int(time.time() * 1_000_000), server_time_us, json.dumps(price_change))

async def insert_best_bid_ask(pool, msg):
    """Inserts a best_bid_ask message into the poly_best_bid_ask table."""
    server_time_us = int(msg.get('timestamp', 0)) * 1000  # Convert ms to us
    await pool.execute("""
        INSERT INTO poly_best_bid_ask (asset_id, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4)
    """, msg.get('asset_id', ''), int(time.time() * 1_000_000), server_time_us, json.dumps(msg))

async def insert_book(pool, msg):
    """Inserts a book message into the poly_book table."""
    server_time_us = int(msg.get('timestamp', 0)) * 1000  # Convert ms to us
    await pool.execute("""
        INSERT INTO poly_book (asset_id, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4)
    """, msg.get('asset_id', ''), int(time.time() * 1_000_000), server_time_us, json.dumps(msg))

async def insert_last_trade_price(pool, msg):
    """Inserts a last_trade_price message into the poly_last_trade_price table."""
    server_time_us = int(msg.get('timestamp', 0)) * 1000  # Convert ms to us
    await pool.execute("""
        INSERT INTO poly_last_trade_price (asset_id, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4)
    """, msg.get('asset_id', ''), int(time.time() * 1_000_000), server_time_us, json.dumps(msg))

async def insert_tick_size_change(pool, msg):
    """Inserts a tick_size_change message into the poly_tick_size_change table."""
    server_time_us = int(msg.get('timestamp', 0)) * 1000  # Convert ms to us
    await pool.execute("""
        INSERT INTO poly_tick_size_change (asset_id, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4)
    """, msg.get('asset_id', ''), int(time.time() * 1_000_000), server_time_us, json.dumps(msg))

async def insert_book_state(pool, asset_id, book_msg):
    """Inserts a book state snapshot into the poly_book_state table."""
    server_time_us = int(book_msg.get('timestamp', 0)) * 1000  # Convert ms to us
    await pool.execute("""
        INSERT INTO poly_book_state (asset_id, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4)
    """, asset_id, int(time.time() * 1_000_000), server_time_us, json.dumps(book_msg))

# --- Main Logic ---

MONITORING_INTERVAL_SECONDS = 15  # How often to log market statistics
BOOK_STATE_POLL_INTERVAL_SECONDS = 0.5 # How often to fetch book state snapshots

async def log_market_statistics(tracked_markets):
    """Periodically logs statistics about subscribed markets and their message counts."""
    while True:
        await asyncio.sleep(MONITORING_INTERVAL_SECONDS)
        
        # Clear and write to monitor_poly.log
        with open(monitor_log_path, 'w') as f:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"=== Market Statistics Report - {timestamp} ===\n")
            
            if not tracked_markets:
                f.write("No markets currently tracked.\n")
            else:
                f.write(f"Tracking {len(tracked_markets)} markets\n\n")
                
                # Sort by market_id for consistent ordering
                for market_id in sorted(tracked_markets.keys()):
                    question = tracked_markets[market_id].get('question', 'unknown')
                    f.write(f"Market {market_id}: {question[:60]}...\n")
                    
                    if market_id in message_stats:
                        stats = message_stats[market_id]
                        total = sum(stats.values())
                        stats_str = ", ".join([f"{msg_type}: {count}" for msg_type, count in sorted(stats.items())])
                        f.write(f"  Total: {total}\n")
                        f.write(f"  {stats_str}\n")
                    else:
                        f.write(f"  No messages yet\n")
                    f.write(f"\n")

def extract_start_time_from_question(question: str) -> int:
    """Extract start time in minutes (24h format) from market question.
    
    Expected format: "Bitcoin Up or Down - Month Day, HH:MMAM/PM-HH:MMAM/PM ET"
    Returns minutes since midnight, or 999999 if parsing fails.
    """
    try:
        # Match: "Bitcoin Up or Down - ... HH:MMAM/PM-..."
        pattern = r"(\d+):(\d+)([AP]M)"
        matches = re.findall(pattern, question)
        if not matches:
            return 999999
        
        # Get first time (start time)
        hour = int(matches[0][0])
        minute = int(matches[0][1])
        ampm = matches[0][2].upper()
        
        # Convert to 24-hour format
        if ampm == 'PM' and hour != 12:
            hour += 12
        elif ampm == 'AM' and hour == 12:
            hour = 0
        
        return hour * 60 + minute
    except:
        return 999999

async def poll_book_state(session, pool, tracked_markets):
    """Periodically fetches and stores book state snapshots for tracked assets."""
    logger.info("--- Starting Book State Poller ---")
    
    while True:
        try:
            await asyncio.sleep(BOOK_STATE_POLL_INTERVAL_SECONDS)
            
            # Get all asset IDs from tracked markets, sorted by market start time (ascending)
            market_assets = []
            for market_id, market_data in tracked_markets.items():
                question = market_data.get('question', '')
                start_time = extract_start_time_from_question(question)
                for asset_id in market_data['clobTokenIds']:
                    market_assets.append((start_time, asset_id, market_id))
            
            # Sort by start_time (ascending - lower start times have priority)
            market_assets.sort(key=lambda x: x[0])
            
            # Take up to 500 asset IDs
            selected_assets = market_assets[:500]
            
            if not selected_assets:
                logger.debug("No tracked assets available for book state polling")
                continue
            
            # Prepare request body
            request_body = [{"token_id": asset_id} for _, asset_id, _ in selected_assets]
            
            try:
                async with session.post(
                    "https://clob.polymarket.com/books",
                    json=request_body,
                    timeout=30
                ) as response:
                    if response.status != 200:
                        logger.warning(f"Book state request failed with status {response.status}")
                        continue
                    
                    books = await response.json()
                    
                    if not isinstance(books, list):
                        logger.warning(f"Unexpected book state response format: {type(books)}")
                        continue
                    
                    # Store each book state, matching asset_id from request order
                    inserted_count = 0
                    for idx, book_msg in enumerate(books):
                        if idx < len(selected_assets):
                            _, asset_id, _ = selected_assets[idx]
                            try:
                                await insert_book_state(pool, asset_id, book_msg)
                                inserted_count += 1
                            except Exception as e:
                                logger.warning(f"Failed to insert book state for {asset_id}: {e}")
                    
                    #logger.info(f"Polled book state: requested {len(selected_assets)} assets, inserted {inserted_count} snapshots")
            
            except asyncio.TimeoutError:
                logger.warning("Book state request timed out")
            except Exception as e:
                logger.error(f"Error polling book state: {e}", exc_info=True)
        
        except asyncio.CancelledError:
            logger.info("Book state poller cancelled")
            break
        except Exception as e:
            logger.error(f"Unexpected error in book state poller: {e}", exc_info=True)
            await asyncio.sleep(5)  # Brief sleep before retry

async def fetch_initial_markets(session, pool):
    """Fetches all Bitcoin 15-min markets that are not closed."""
    logger.info("--- Starting Initial Market Scan (HTTP Endpoint) ---")
    offset = 0
    tracked_markets = {}  # key: market_id, value: {clobTokenIds: [...], market_address: ...}

    while True:
        url = f"{POLY_REST_URL}?limit={FETCH_LIMIT}&offset={offset}"
        logger.info(f"Fetching: {url}")
        
        try:
            async with session.get(url, timeout=30) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch markets at offset {offset}. Status: {response.status}")
                    break
                
                markets = await response.json()
                
                if not isinstance(markets, list) or len(markets) == 0:
                    logger.info(f"No more markets found at offset {offset}")
                    break
                
                for market in markets:
                    question = market.get('question', '')
                    closed = market.get('closed', False)
                    
                    # Filter: Bitcoin 15-min markets that are not closed
                    if is_Bitcoin_15min_market(question) and not closed:
                        await upsert_market(pool, market)
                        market_id = market.get('id')
                        clob_token_ids_raw = market.get('clobTokenIds', '[]')
                        market_address = market.get('conditionId', '')
                        
                        # Parse clobTokenIds - it's a JSON string containing an array
                        try:
                            if isinstance(clob_token_ids_raw, str):
                                clob_token_ids = json.loads(clob_token_ids_raw)
                            else:
                                clob_token_ids = clob_token_ids_raw
                        except json.JSONDecodeError:
                            logger.error(f"Failed to parse clobTokenIds for market {market_id}: {clob_token_ids_raw}")
                            clob_token_ids = []
                        
                        if market_id and clob_token_ids:
                            tracked_markets[market_id] = {
                                'clobTokenIds': clob_token_ids,
                                'market_address': market_address,
                                'question': question
                            }
                            logger.info(f"Market {market_id} ({question[:50]}...) - Assets: {clob_token_ids}")
                
                logger.info(f"Processed {len(markets)} markets (total tracked so far: {len(tracked_markets)})")
                
                if len(markets) < FETCH_LIMIT:
                    logger.info(f"Reached end of results (got {len(markets)} < {FETCH_LIMIT})")
                    break
                
                offset += FETCH_LIMIT
                
        except Exception as e:
            logger.error(f"Error fetching markets at offset {offset}: {e}")
            break
    
    logger.info(f"--- Initial Scan Complete. Found {len(tracked_markets)} Bitcoin 15-min markets. ---")
    return tracked_markets

async def send_ping(websocket: websockets.WebSocketClientProtocol, interval=5):
    """Send ping to keep the WebSocket connection alive."""
    try:
        while True:
            await asyncio.sleep(interval)
            await websocket.send("PING")
    except websockets.ConnectionClosed:
        logger.info("WebSocket connection closed. Stopping ping.")
    except asyncio.CancelledError:
        logger.debug("Ping task canceled.")

async def receive_messages(websocket: websockets.WebSocketClientProtocol, pool, tracked_markets):
    """Receive messages from the WebSocket connection and insert into database."""
    try:
        while True:
            message = await websocket.recv()
            
            if message == "PONG":
                continue
            
            try:
                msg = json.loads(message)
            except json.JSONDecodeError as e:
                logger.warning(f"Error parsing message: {e} - {message}")
                continue
            
            if not isinstance(msg, dict):
                continue
            
            event_type = msg.get("event_type")
            
            try:
                if event_type == "new_market":
                    question = msg.get('question', '')
                    if is_Bitcoin_15min_market(question):
                        market_id = msg.get('id')
                        logger.info(f"New Bitcoin 15-min market: {market_id} - {question}")
                        await insert_new_market(pool, msg)
                        increment_message_stat(market_id, 'new_market')
                        
                        # Subscribe to this new market
                        assets_ids = msg.get('assets_ids', [])
                        if assets_ids:
                            tracked_markets[market_id] = {
                                'clobTokenIds': assets_ids,
                                'market_address': msg.get('market', ''),
                                'question': question
                            }
                            await websocket.send(json.dumps({
                                "operation": "subscribe",
                                "assets_ids": assets_ids,
                                "custom_feature_enabled": True,
                            }))
                            logger.info(f"Subscribed to new market {market_id}")
                
                elif event_type == "market_resolved":
                    # Only process if it's one of our tracked markets
                    market_id = msg.get('id')
                    if market_id in tracked_markets:
                        logger.info(f"Market resolved: {market_id} - {msg.get('question', '')}")
                        await insert_market_resolved(pool, msg)
                        increment_message_stat(market_id, 'market_resolved')
                        
                        # Unsubscribe from resolved market
                        assets_ids = msg.get('assets_ids', [])
                        if assets_ids:
                            await websocket.send(json.dumps({
                                "operation": "unsubscribe",
                                "assets_ids": assets_ids,
                                "custom_feature_enabled": True,
                            }))
                            logger.info(f"Unsubscribed from resolved market {market_id}")
                            # Remove from tracked markets
                            del tracked_markets[market_id]
                
                elif event_type == "price_change":
                    # Lookup market_id from asset_id in price_changes
                    price_changes = msg.get('price_changes', [])
                    if price_changes:
                        # Only insert price_changes for tracked asset_ids
                        filtered_price_changes = []
                        for change in price_changes:
                            asset_id = change.get('asset_id')
                            # Check if this specific asset_id is tracked
                            for mid, market_data in tracked_markets.items():
                                if asset_id in market_data['clobTokenIds']:
                                    filtered_price_changes.append(change)
                                    increment_message_stat(mid, 'price_change')
                                    break
                        
                        # Only insert if we found matching price_changes
                        if filtered_price_changes:
                            await insert_filtered_price_changes(pool, msg, filtered_price_changes)
                
                elif event_type == "best_bid_ask":
                    asset_id = msg.get('asset_id')
                    if asset_id:
                        found = False
                        for mid, market_data in tracked_markets.items():
                            if asset_id in market_data['clobTokenIds']:
                                await insert_best_bid_ask(pool, msg)
                                increment_message_stat(mid, 'best_bid_ask')
                                found = True
                                break
                        if not found:
                            logger.debug(f"Received best_bid_ask for untracked asset_id: {asset_id[:30]}...")
                
                elif event_type == "book":
                    asset_id = msg.get('asset_id')
                    if asset_id:
                        found = False
                        for mid, market_data in tracked_markets.items():
                            if asset_id in market_data['clobTokenIds']:
                                await insert_book(pool, msg)
                                increment_message_stat(mid, 'book')
                                found = True
                                break
                        if not found:
                            logger.debug(f"Received book for untracked asset_id: {asset_id[:30]}...")
                
                elif event_type == "last_trade_price":
                    asset_id = msg.get('asset_id')
                    if asset_id:
                        found = False
                        for mid, market_data in tracked_markets.items():
                            if asset_id in market_data['clobTokenIds']:
                                await insert_last_trade_price(pool, msg)
                                increment_message_stat(mid, 'last_trade_price')
                                found = True
                                break
                        if not found:
                            logger.debug(f"Received last_trade_price for untracked asset_id: {asset_id[:30]}...")
                
                elif event_type == "tick_size_change":
                    asset_id = msg.get('asset_id')
                    if asset_id:
                        found = False
                        for mid, market_data in tracked_markets.items():
                            if asset_id in market_data['clobTokenIds']:
                                await insert_tick_size_change(pool, msg)
                                increment_message_stat(mid, 'tick_size_change')
                                found = True
                                break
                        if not found:
                            logger.debug(f"Received tick_size_change for untracked asset_id: {asset_id[:30]}...")
                
            except Exception as e:
                logger.error(f"Error processing {event_type} message: {e}", exc_info=True)
    
    except websockets.ConnectionClosed:
        logger.info("WebSocket connection closed. Stopping receive messages.")
    except asyncio.CancelledError:
        logger.debug("Receive messages task canceled.")

async def websocket_listener(tracked_markets, pool):
    """Connects to the WebSocket to listen for real-time events."""
    logger.info("--- Starting Real-Time Market Listener (WebSocket) ---")
    
    while True:
        try:
            async with websockets.connect(
                POLY_WSS_URL,
                ping_interval=10,
                ping_timeout=10,
                open_timeout=10,
                close_timeout=10,
            ) as websocket:
                logger.info("WebSocket connection established successfully!")
                
                # Collect all asset IDs from all tracked markets
                all_assets_ids = []
                for market_data in tracked_markets.values():
                    all_assets_ids.extend(market_data['clobTokenIds'])
                
                # Send subscription message with all assets at once
                initial_sub = {
                    "operation": "subscribe",
                    "assets_ids": all_assets_ids,
                    "custom_feature_enabled": True,
                }
                
                logger.info(f"Sending subscription for {len(tracked_markets)} markets with {len(all_assets_ids)} total assets")
                logger.info(f"Subscription message: {json.dumps(initial_sub)[:500]}...")  # Log first 500 chars
                
                await websocket.send(json.dumps(initial_sub))
                logger.info(f"Subscription sent successfully.")
                
                ping_task = asyncio.create_task(send_ping(websocket), name="ping")
                recv_task = asyncio.create_task(receive_messages(websocket, pool, tracked_markets), name="recv")
                
                try:
                    done, _ = await asyncio.wait(
                        {ping_task, recv_task},
                        return_when=asyncio.FIRST_EXCEPTION,
                    )
                    
                    for t in done:
                        t.result()  # raises if failed
                
                finally:
                    for t in (ping_task, recv_task):
                        t.cancel()
                    await asyncio.gather(ping_task, recv_task, return_exceptions=True)
        
        except Exception as e:
            logger.error(f"WebSocket error: {e}. Reconnecting in 15 seconds...", exc_info=True)
            await asyncio.sleep(15)

# --- Main Execution ---

async def main():
    """Main function to run the initial scan and then start the WebSocket listener."""
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST]):
        logger.error("One or more required environment variables are not set.")
        return
    
    logger.info(f"Monitor log: {monitor_log_path}")
    
    pool = None
    try:
        pool = await asyncpg.create_pool(
            user=DB_USER, 
            password=DB_PASS, 
            database=DB_NAME, 
            host=PG_HOST, 
            port=PG_PORT
        )
        logger.info("Successfully connected to PostgreSQL database.")
        await init_db(pool)
        
        async with aiohttp.ClientSession() as session:
            tracked_markets = await fetch_initial_markets(session, pool)
            
            # Start monitoring, websocket listener, and book state poller concurrently
            await asyncio.gather(
                log_market_statistics(tracked_markets),
                websocket_listener(tracked_markets, pool),
                poll_book_state(session, pool, tracked_markets)
            )
    
    except Exception as e:
        logger.critical(f"A critical error occurred in the main function: {e}", exc_info=True)
    finally:
        if pool:
            await pool.close()
            logger.info("Database connection pool closed.")

if __name__ == "__main__":
    asyncio.run(main())
