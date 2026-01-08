import os
import asyncio
import asyncpg
import logging
import json
import time
import websockets
import aiohttp

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
monitor_log_path = os.path.join(os.path.dirname(__file__), "monitor_poly_house.log")

# --- Message Statistics Tracking (per market, per outcome) ---
message_stats = {}  # {market_id: {outcome: {message_type: count}}}

# --- Target Markets Configuration ---
TARGET_MARKETS = {
    "562802": {
        "question": "Will the Democratic Party control the House after the 2026 Midterm elections?",
        "outcomes": ["Yes", "No"],
        "asset_ids": [
            "83247781037352156539108067944461291821683755894607244160607042790356561625563",  # Yes
            "33156410999665902694791064431724433042010245771106314074312009703157423879038"   # No
        ]
    },
    "562803": {
        "question": "Will the Republican Party control the House after the 2026 Midterm elections?",
        "outcomes": ["Yes", "No"],
        "asset_ids": [
            "65139230827417363158752884968303867495725894165574887635816574090175320800482",  # Yes
            "17371217118862125782438074585166210555214661810823929795910191856905580975576"   # No
        ]
    }
}

# --- Polymarket API Configuration ---
POLY_WSS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# --- Database Schema Definition ---
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS poly_markets_house (
    market_id TEXT PRIMARY KEY,
    found_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS poly_new_market_house (
    id SERIAL PRIMARY KEY,
    market_id TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_new_market_house_market_id ON poly_new_market_house(market_id);
CREATE INDEX IF NOT EXISTS idx_poly_new_market_house_server_time ON poly_new_market_house(server_time_us);

CREATE TABLE IF NOT EXISTS poly_market_resolved_house (
    id SERIAL PRIMARY KEY,
    market_id TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_market_resolved_house_market_id ON poly_market_resolved_house(market_id);
CREATE INDEX IF NOT EXISTS idx_poly_market_resolved_house_server_time ON poly_market_resolved_house(server_time_us);

CREATE TABLE IF NOT EXISTS poly_price_change_house (
    id SERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_price_change_house_asset_id ON poly_price_change_house(asset_id);
CREATE INDEX IF NOT EXISTS idx_poly_price_change_house_market_id ON poly_price_change_house(market_id);
CREATE INDEX IF NOT EXISTS idx_poly_price_change_house_server_time ON poly_price_change_house(server_time_us);

CREATE TABLE IF NOT EXISTS poly_best_bid_ask_house (
    id SERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_best_bid_ask_house_asset_id ON poly_best_bid_ask_house(asset_id);
CREATE INDEX IF NOT EXISTS idx_poly_best_bid_ask_house_market_id ON poly_best_bid_ask_house(market_id);
CREATE INDEX IF NOT EXISTS idx_poly_best_bid_ask_house_server_time ON poly_best_bid_ask_house(server_time_us);

CREATE TABLE IF NOT EXISTS poly_book_house (
    id SERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_book_house_asset_id ON poly_book_house(asset_id);
CREATE INDEX IF NOT EXISTS idx_poly_book_house_market_id ON poly_book_house(market_id);
CREATE INDEX IF NOT EXISTS idx_poly_book_house_server_time ON poly_book_house(server_time_us);

CREATE TABLE IF NOT EXISTS poly_last_trade_price_house (
    id SERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_last_trade_price_house_asset_id ON poly_last_trade_price_house(asset_id);
CREATE INDEX IF NOT EXISTS idx_poly_last_trade_price_house_market_id ON poly_last_trade_price_house(market_id);
CREATE INDEX IF NOT EXISTS idx_poly_last_trade_price_house_server_time ON poly_last_trade_price_house(server_time_us);

CREATE TABLE IF NOT EXISTS poly_tick_size_change_house (
    id SERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_tick_size_change_house_asset_id ON poly_tick_size_change_house(asset_id);
CREATE INDEX IF NOT EXISTS idx_poly_tick_size_change_house_market_id ON poly_tick_size_change_house(market_id);
CREATE INDEX IF NOT EXISTS idx_poly_tick_size_change_house_server_time ON poly_tick_size_change_house(server_time_us);

CREATE TABLE IF NOT EXISTS poly_book_state_house (
    id SERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poly_book_state_house_asset_id ON poly_book_state_house(asset_id);
CREATE INDEX IF NOT EXISTS idx_poly_book_state_house_market_id ON poly_book_state_house(market_id);
CREATE INDEX IF NOT EXISTS idx_poly_book_state_house_server_time ON poly_book_state_house(server_time_us);
"""

# --- Helper Functions ---

def get_market_and_outcome_for_asset(asset_id: str) -> tuple:
    """Returns (market_id, outcome) for a given asset_id, or (None, None) if not found."""
    for market_id, market_info in TARGET_MARKETS.items():
        for idx, aid in enumerate(market_info['asset_ids']):
            if aid == asset_id:
                return market_id, market_info['outcomes'][idx]
    return None, None

def increment_message_stat(market_id, outcome, message_type):
    """Increments the message count for a specific market, outcome, and message type."""
    if market_id not in message_stats:
        message_stats[market_id] = {}
    if outcome not in message_stats[market_id]:
        message_stats[market_id][outcome] = {}
    if message_type not in message_stats[market_id][outcome]:
        message_stats[market_id][outcome][message_type] = 0
    message_stats[market_id][outcome][message_type] += 1

# --- Database Functions ---

async def init_db(pool):
    """Initializes the database by creating tables if they don't exist."""
    async with pool.acquire() as connection:
        await connection.execute(CREATE_TABLES_SQL)
    logger.info("Database tables and indexes initialized successfully.")

async def insert_price_change(pool, market_id, outcome, msg):
    """Inserts a price_change message into the poly_price_change_house table."""
    server_time_us = int(msg.get('timestamp', 0)) * 1000  # Convert ms to us
    asset_id = msg.get('asset_id', '')
    
    await pool.execute("""
        INSERT INTO poly_price_change_house (asset_id, market_id, outcome, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4, $5, $6)
    """, asset_id, market_id, outcome, int(time.time() * 1_000_000), server_time_us, json.dumps(msg))

async def insert_best_bid_ask(pool, market_id, outcome, msg):
    """Inserts a best_bid_ask message into the poly_best_bid_ask_house table."""
    server_time_us = int(msg.get('timestamp', 0)) * 1000  # Convert ms to us
    asset_id = msg.get('asset_id', '')
    
    await pool.execute("""
        INSERT INTO poly_best_bid_ask_house (asset_id, market_id, outcome, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4, $5, $6)
    """, asset_id, market_id, outcome, int(time.time() * 1_000_000), server_time_us, json.dumps(msg))

async def insert_book(pool, market_id, outcome, msg):
    """Inserts a book message into the poly_book_house table."""
    server_time_us = int(msg.get('timestamp', 0)) * 1000  # Convert ms to us
    asset_id = msg.get('asset_id', '')
    
    await pool.execute("""
        INSERT INTO poly_book_house (asset_id, market_id, outcome, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4, $5, $6)
    """, asset_id, market_id, outcome, int(time.time() * 1_000_000), server_time_us, json.dumps(msg))

async def insert_last_trade_price(pool, market_id, outcome, msg):
    """Inserts a last_trade_price message into the poly_last_trade_price_house table."""
    server_time_us = int(msg.get('timestamp', 0)) * 1000  # Convert ms to us
    asset_id = msg.get('asset_id', '')
    
    await pool.execute("""
        INSERT INTO poly_last_trade_price_house (asset_id, market_id, outcome, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4, $5, $6)
    """, asset_id, market_id, outcome, int(time.time() * 1_000_000), server_time_us, json.dumps(msg))

async def insert_tick_size_change(pool, market_id, outcome, msg):
    """Inserts a tick_size_change message into the poly_tick_size_change_house table."""
    server_time_us = int(msg.get('timestamp', 0)) * 1000  # Convert ms to us
    asset_id = msg.get('asset_id', '')
    
    await pool.execute("""
        INSERT INTO poly_tick_size_change_house (asset_id, market_id, outcome, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4, $5, $6)
    """, asset_id, market_id, outcome, int(time.time() * 1_000_000), server_time_us, json.dumps(msg))

async def insert_book_state(pool, market_id, outcome, asset_id, book_msg):
    """Inserts a book state snapshot into the poly_book_state_house table."""
    server_time_us = int(book_msg.get('timestamp', 0)) * 1000  # Convert ms to us
    
    await pool.execute("""
        INSERT INTO poly_book_state_house (asset_id, market_id, outcome, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4, $5, $6)
    """, asset_id, market_id, outcome, int(time.time() * 1_000_000), server_time_us, json.dumps(book_msg))

# --- Main Logic ---

MONITORING_INTERVAL_SECONDS = 15  # How often to log market statistics
BOOK_STATE_POLL_INTERVAL_SECONDS = 0.5  # How often to fetch book state snapshots

async def log_market_statistics():
    """Periodically logs statistics about tracked markets and their message counts (per outcome)."""
    while True:
        await asyncio.sleep(MONITORING_INTERVAL_SECONDS)
        
        # Clear and write to monitor_poly_house.log
        with open(monitor_log_path, 'w') as f:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"=== House Elections Market Statistics Report - {timestamp} ===\n\n")
            
            for market_id in sorted(TARGET_MARKETS.keys()):
                market_info = TARGET_MARKETS[market_id]
                f.write(f"Market {market_id}: {market_info['question']}\n")
                
                if market_id in message_stats:
                    for outcome in market_info['outcomes']:
                        f.write(f"  [{outcome}]:\n")
                        
                        if outcome in message_stats[market_id]:
                            stats = message_stats[market_id][outcome]
                            total = sum(stats.values())
                            stats_str = ", ".join([f"{msg_type}: {count}" for msg_type, count in sorted(stats.items())])
                            f.write(f"    Total: {total}\n")
                            f.write(f"    {stats_str}\n")
                        else:
                            f.write(f"    No messages yet\n")
                else:
                    f.write(f"  No activity yet\n")
                
                f.write(f"\n")

async def poll_book_state(session, pool):
    """Periodically fetches and stores book state snapshots for all tracked assets."""
    logger.info("--- Starting Book State Poller ---")
    
    while True:
        try:
            await asyncio.sleep(BOOK_STATE_POLL_INTERVAL_SECONDS)
            
            # Collect all asset IDs from target markets
            request_body = []
            asset_to_market_outcome = {}  # Map asset_id to (market_id, outcome)
            
            for market_id, market_info in TARGET_MARKETS.items():
                for idx, asset_id in enumerate(market_info['asset_ids']):
                    request_body.append({"token_id": asset_id})
                    asset_to_market_outcome[asset_id] = (market_id, market_info['outcomes'][idx])
            
            if not request_body:
                logger.debug("No tracked assets available for book state polling")
                continue
            
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
                        if idx < len(request_body):
                            asset_id = request_body[idx]["token_id"]
                            if asset_id in asset_to_market_outcome:
                                market_id, outcome = asset_to_market_outcome[asset_id]
                                try:
                                    await insert_book_state(pool, market_id, outcome, asset_id, book_msg)
                                    inserted_count += 1
                                except Exception as e:
                                    logger.warning(f"Failed to insert book state for {asset_id}: {e}")
                    
                    # logger.info(f"Polled book state: requested {len(request_body)} assets, inserted {inserted_count} snapshots")
            
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

async def receive_messages(websocket: websockets.WebSocketClientProtocol, pool):
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
                if event_type == "price_change":
                    price_changes = msg.get('price_changes', [])
                    if price_changes:
                        for change in price_changes:
                            asset_id = change.get('asset_id')
                            market_id, outcome = get_market_and_outcome_for_asset(asset_id)
                            
                            if market_id:  # Only insert if it's one of our target assets
                                await insert_price_change(pool, market_id, outcome, change)
                                increment_message_stat(market_id, outcome, 'price_change')
                
                elif event_type == "best_bid_ask":
                    asset_id = msg.get('asset_id')
                    if asset_id:
                        market_id, outcome = get_market_and_outcome_for_asset(asset_id)
                        
                        if market_id:
                            await insert_best_bid_ask(pool, market_id, outcome, msg)
                            increment_message_stat(market_id, outcome, 'best_bid_ask')
                
                elif event_type == "book":
                    asset_id = msg.get('asset_id')
                    if asset_id:
                        market_id, outcome = get_market_and_outcome_for_asset(asset_id)
                        
                        if market_id:
                            await insert_book(pool, market_id, outcome, msg)
                            increment_message_stat(market_id, outcome, 'book')
                
                elif event_type == "last_trade_price":
                    asset_id = msg.get('asset_id')
                    if asset_id:
                        market_id, outcome = get_market_and_outcome_for_asset(asset_id)
                        
                        if market_id:
                            await insert_last_trade_price(pool, market_id, outcome, msg)
                            increment_message_stat(market_id, outcome, 'last_trade_price')
                
                elif event_type == "tick_size_change":
                    asset_id = msg.get('asset_id')
                    if asset_id:
                        market_id, outcome = get_market_and_outcome_for_asset(asset_id)
                        
                        if market_id:
                            await insert_tick_size_change(pool, market_id, outcome, msg)
                            increment_message_stat(market_id, outcome, 'tick_size_change')
                
            except Exception as e:
                logger.error(f"Error processing {event_type} message: {e}", exc_info=True)
    
    except websockets.ConnectionClosed:
        logger.info("WebSocket connection closed. Stopping receive messages.")
    except asyncio.CancelledError:
        logger.debug("Receive messages task canceled.")

async def websocket_listener(pool):
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
                
                # Collect all asset IDs from target markets
                all_assets_ids = []
                for market_info in TARGET_MARKETS.values():
                    all_assets_ids.extend(market_info['asset_ids'])
                
                # Send subscription message with all target assets
                initial_sub = {
                    "operation": "subscribe",
                    "assets_ids": all_assets_ids,
                    "custom_feature_enabled": True,
                }
                
                logger.info(f"Subscribing to {len(TARGET_MARKETS)} markets with {len(all_assets_ids)} total assets")
                logger.info(f"Asset IDs: {all_assets_ids}")
                
                await websocket.send(json.dumps(initial_sub))
                logger.info(f"Subscription sent successfully.")
                
                ping_task = asyncio.create_task(send_ping(websocket), name="ping")
                recv_task = asyncio.create_task(receive_messages(websocket, pool), name="recv")
                
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
    """Main function to start the WebSocket listener and book state polling."""
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST]):
        logger.error("One or more required environment variables are not set.")
        return
    
    logger.info(f"Monitor log: {monitor_log_path}")
    logger.info("=== Starting House Elections Market Monitor ===")
    logger.info(f"Target markets: {list(TARGET_MARKETS.keys())}")
    
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
            # Start monitoring, websocket listener, and book state poller concurrently
            await asyncio.gather(
                log_market_statistics(),
                websocket_listener(pool),
                poll_book_state(session, pool)
            )
    
    except Exception as e:
        logger.critical(f"A critical error occurred in the main function: {e}", exc_info=True)
    finally:
        if pool:
            await pool.close()
            logger.info("Database connection pool closed.")

if __name__ == "__main__":
    asyncio.run(main())
