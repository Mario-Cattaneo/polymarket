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
asset_stats = {}  # {asset_id: {'market_id': id, 'question': q, 'outcome': o, 'counts': {msg_type: count}}}
market_to_assets = {}  # {market_id: [asset_ids]}

# --- Polymarket API Configuration ---
POLY_REST_URL = "https://gamma-api.polymarket.com/markets"
POLY_WSS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
FETCH_LIMIT = 500

# --- Database Schema Definition (simplified with JSON storage) ---
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS poly_markets (
    market_id TEXT PRIMARY KEY,
    found_time_ms BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_markets_found_time ON poly_markets(found_time_ms);

CREATE TABLE IF NOT EXISTS poly_new_market (
    id SERIAL PRIMARY KEY,
    market_id TEXT NOT NULL,
    found_time_ms BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_new_market_found_time ON poly_new_market(found_time_ms);
CREATE INDEX IF NOT EXISTS idx_poly_new_market_id ON poly_new_market(market_id);

CREATE TABLE IF NOT EXISTS poly_market_resolved (
    id SERIAL PRIMARY KEY,
    market_id TEXT NOT NULL,
    found_time_ms BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_market_resolved_found_time ON poly_market_resolved(found_time_ms);
CREATE INDEX IF NOT EXISTS idx_poly_market_resolved_id ON poly_market_resolved(market_id);

CREATE TABLE IF NOT EXISTS poly_price_change (
    id SERIAL PRIMARY KEY,
    market_address TEXT NOT NULL,
    found_time_ms BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_price_change_found_time ON poly_price_change(found_time_ms);
CREATE INDEX IF NOT EXISTS idx_poly_price_change_market ON poly_price_change(market_address);

CREATE TABLE IF NOT EXISTS poly_best_bid_ask (
    id SERIAL PRIMARY KEY,
    market_address TEXT NOT NULL,
    found_time_ms BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_best_bid_ask_found_time ON poly_best_bid_ask(found_time_ms);
CREATE INDEX IF NOT EXISTS idx_poly_best_bid_ask_market ON poly_best_bid_ask(market_address);

CREATE TABLE IF NOT EXISTS poly_book (
    id SERIAL PRIMARY KEY,
    market_address TEXT NOT NULL,
    found_time_ms BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_book_found_time ON poly_book(found_time_ms);
CREATE INDEX IF NOT EXISTS idx_poly_book_market ON poly_book(market_address);

CREATE TABLE IF NOT EXISTS poly_last_trade_price (
    id SERIAL PRIMARY KEY,
    market_address TEXT NOT NULL,
    found_time_ms BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_last_trade_price_found_time ON poly_last_trade_price(found_time_ms);
CREATE INDEX IF NOT EXISTS idx_poly_last_trade_price_market ON poly_last_trade_price(market_address);

CREATE TABLE IF NOT EXISTS poly_tick_size_change (
    id SERIAL PRIMARY KEY,
    market_address TEXT NOT NULL,
    found_time_ms BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_tick_size_change_found_time ON poly_tick_size_change(found_time_ms);
CREATE INDEX IF NOT EXISTS idx_poly_tick_size_change_market ON poly_tick_size_change(market_address);
"""

# --- Helper Functions ---

def increment_message_stat(asset_id, message_type):
    """Increments the message count for a specific asset and message type."""
    if asset_id not in asset_stats:
        asset_stats[asset_id] = {'counts': {}}
    if message_type not in asset_stats[asset_id]['counts']:
        asset_stats[asset_id]['counts'][message_type] = 0
    asset_stats[asset_id]['counts'][message_type] += 1

def is_Bitcoin_15min_market(question: str) -> bool:
    """Check if a market question matches the Bitcoin Up or Down 15-minute format."""
    pattern = r"^Bitcoin Up or Down\s*-\s*\w+\s+\d+,\s+\d+:\d+[AP]M-\d+:\d+[AP]M\s+ET$"
    return bool(re.match(pattern, question, re.IGNORECASE))

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
        INSERT INTO poly_markets (market_id, found_time_ms, message)
        VALUES ($1, $2, $3)
        ON CONFLICT (market_id) DO UPDATE SET
            found_time_ms = EXCLUDED.found_time_ms,
            message = EXCLUDED.message;
    """, market_id, int(time.time() * 1000), json.dumps(market_json))
    logger.info(f"Upserted market '{market_id}' - '{market_json.get('question', '')}' into the database.")

async def insert_new_market(pool, msg):
    """Inserts a new_market message into the poly_new_market table."""
    await pool.execute("""
        INSERT INTO poly_new_market (market_id, found_time_ms, message)
        VALUES ($1, $2, $3)
    """, msg.get('id'), int(time.time() * 1000), json.dumps(msg))

async def insert_market_resolved(pool, msg):
    """Inserts a market_resolved message into the poly_market_resolved table."""
    await pool.execute("""
        INSERT INTO poly_market_resolved (market_id, found_time_ms, message)
        VALUES ($1, $2, $3)
    """, msg.get('id'), int(time.time() * 1000), json.dumps(msg))

async def insert_price_change(pool, msg):
    """Inserts a price_change message into the poly_price_change table."""
    await pool.execute("""
        INSERT INTO poly_price_change (market_address, found_time_ms, message)
        VALUES ($1, $2, $3)
    """, msg.get('market', ''), int(time.time() * 1000), json.dumps(msg))

async def insert_best_bid_ask(pool, msg):
    """Inserts a best_bid_ask message into the poly_best_bid_ask table."""
    await pool.execute("""
        INSERT INTO poly_best_bid_ask (market_address, found_time_ms, message)
        VALUES ($1, $2, $3)
    """, msg.get('market', ''), int(time.time() * 1000), json.dumps(msg))

async def insert_book(pool, msg):
    """Inserts a book message into the poly_book table."""
    await pool.execute("""
        INSERT INTO poly_book (market_address, found_time_ms, message)
        VALUES ($1, $2, $3)
    """, msg.get('market', ''), int(time.time() * 1000), json.dumps(msg))

async def insert_last_trade_price(pool, msg):
    """Inserts a last_trade_price message into the poly_last_trade_price table."""
    await pool.execute("""
        INSERT INTO poly_last_trade_price (market_address, found_time_ms, message)
        VALUES ($1, $2, $3)
    """, msg.get('market', ''), int(time.time() * 1000), json.dumps(msg))

async def insert_tick_size_change(pool, msg):
    """Inserts a tick_size_change message into the poly_tick_size_change table."""
    await pool.execute("""
        INSERT INTO poly_tick_size_change (market_address, found_time_ms, message)
        VALUES ($1, $2, $3)
    """, msg.get('market', ''), int(time.time() * 1000), json.dumps(msg))

# --- Main Logic ---

MONITORING_INTERVAL_SECONDS = 15  # How often to log market statistics

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
                f.write(f"Tracking {len(tracked_markets)} markets ({len(asset_stats)} assets with messages)\n\n")
                
                # Group stats by market_id
                market_stats = {}
                for asset_id, stats in asset_stats.items():
                    market_id = stats.get('market_id', 'unknown')
                    if market_id not in market_stats:
                        market_stats[market_id] = {
                            'question': stats.get('question', 'unknown'),
                            'assets': []
                        }
                    market_stats[market_id]['assets'].append({
                        'asset_id': asset_id,
                        'outcome': stats.get('outcome', 'unknown'),
                        'counts': stats['counts']
                    })
                
                # Sort by market_id for consistent ordering
                for market_id in sorted(tracked_markets.keys()):
                    question = tracked_markets[market_id].get('question', 'unknown')
                    f.write(f"Market {market_id}: {question[:60]}...\n")
                    
                    if market_id in market_stats:
                        for asset_info in market_stats[market_id]['assets']:
                            outcome = asset_info['outcome']
                            counts = asset_info['counts']
                            total = sum(counts.values())
                            stats_str = ", ".join([f"{msg_type}: {count}" for msg_type, count in sorted(counts.items())])
                            f.write(f"  {outcome}: Total={total}, {stats_str}\n")
                    else:
                        f.write(f"  No messages yet\n")
                    f.write(f"\n")

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
                        outcomes_raw = market.get('outcomes', '[]')
                        
                        # Parse clobTokenIds - it's a JSON string containing an array
                        try:
                            if isinstance(clob_token_ids_raw, str):
                                clob_token_ids = json.loads(clob_token_ids_raw)
                            else:
                                clob_token_ids = clob_token_ids_raw
                        except json.JSONDecodeError:
                            logger.error(f"Failed to parse clobTokenIds for market {market_id}: {clob_token_ids_raw}")
                            clob_token_ids = []
                        
                        # Parse outcomes - also a JSON string
                        try:
                            if isinstance(outcomes_raw, str):
                                outcomes = json.loads(outcomes_raw)
                            else:
                                outcomes = outcomes_raw
                        except json.JSONDecodeError:
                            logger.error(f"Failed to parse outcomes for market {market_id}: {outcomes_raw}")
                            outcomes = []
                        
                        if market_id and clob_token_ids:
                            tracked_markets[market_id] = {
                                'clobTokenIds': clob_token_ids,
                                'market_address': market_address,
                                'question': question,
                                'outcomes': outcomes
                            }
                            market_to_assets[market_id] = clob_token_ids
                            
                            # Register each asset_id with its market info
                            for i, asset_id in enumerate(clob_token_ids):
                                outcome = outcomes[i] if i < len(outcomes) else f"Outcome{i}"
                                asset_stats[asset_id] = {
                                    'market_id': market_id,
                                    'question': question,
                                    'outcome': outcome,
                                    'counts': {}
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
                        
                        # Subscribe to this new market
                        assets_ids = msg.get('assets_ids', [])
                        outcomes = msg.get('outcomes', [])
                        if assets_ids:
                            tracked_markets[market_id] = {
                                'clobTokenIds': assets_ids,
                                'market_address': msg.get('market', ''),
                                'question': question,
                                'outcomes': outcomes
                            }
                            market_to_assets[market_id] = assets_ids
                            
                            # Register each asset_id
                            for i, asset_id in enumerate(assets_ids):
                                outcome = outcomes[i] if i < len(outcomes) else f"Outcome{i}"
                                asset_stats[asset_id] = {
                                    'market_id': market_id,
                                    'question': question,
                                    'outcome': outcome,
                                    'counts': {}
                                }
                                increment_message_stat(asset_id, 'new_market')
                            
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
                        
                        # Increment stats for all assets in this market
                        if market_id in market_to_assets:
                            for asset_id in market_to_assets[market_id]:
                                increment_message_stat(asset_id, 'market_resolved')
                        
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
                            del market_to_assets[market_id]
                
                elif event_type == "price_change":
                    # Each price_change contains an array of changes, one per asset
                    price_changes = msg.get('price_changes', [])
                    for change in price_changes:
                        asset_id = change.get('asset_id')
                        if asset_id and asset_id in asset_stats:
                            await insert_price_change(pool, msg)
                            increment_message_stat(asset_id, 'price_change')
                            break  # Only insert once per message
                
                elif event_type == "best_bid_ask":
                    asset_id = msg.get('asset_id')
                    if asset_id and asset_id in asset_stats:
                        await insert_best_bid_ask(pool, msg)
                        increment_message_stat(asset_id, 'best_bid_ask')
                
                elif event_type == "book":
                    asset_id = msg.get('asset_id')
                    if asset_id and asset_id in asset_stats:
                        await insert_book(pool, msg)
                        increment_message_stat(asset_id, 'book')
                
                elif event_type == "last_trade_price":
                    asset_id = msg.get('asset_id')
                    if asset_id and asset_id in asset_stats:
                        await insert_last_trade_price(pool, msg)
                        increment_message_stat(asset_id, 'last_trade_price')
                
                elif event_type == "tick_size_change":
                    asset_id = msg.get('asset_id')
                    if asset_id and asset_id in asset_stats:
                        await insert_tick_size_change(pool, msg)
                        increment_message_stat(asset_id, 'tick_size_change')
                
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
        
        # Start monitoring task and websocket listener concurrently
        await asyncio.gather(
            log_market_statistics(tracked_markets),
            websocket_listener(tracked_markets, pool)
        )
    
    except Exception as e:
        logger.critical(f"A critical error occurred in the main function: {e}", exc_info=True)
    finally:
        if pool:
            await pool.close()
            logger.info("Database connection pool closed.")

if __name__ == "__main__":
    asyncio.run(main())
