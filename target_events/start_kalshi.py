import os
import asyncio
import asyncpg
import logging
import json
import time
import base64
import websockets
import aiohttp

# --- DEPENDENCIES: pip install websockets cryptography aiohttp asyncpg ---
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

# --- Environment Variable Setup ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")
KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
KALSHI_PRIVATE_KEY_PATH = os.path.expanduser("~/.keys/kalshi_private.pem")

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

# --- Monitor Logger Setup (dedicated file) ---
monitor_logger = logging.getLogger('monitor')
monitor_logger.setLevel(logging.INFO)
monitor_log_path = os.path.join(os.path.dirname(__file__), "monitor_kalshi.log")
# File handler will be recreated each monitoring cycle to clear the file

# --- Message Statistics Tracking ---
message_stats = {}  # {market_ticker: {message_type: count}}

# --- Kalshi API Configuration ---
KALSHI_V1_REST_URL = "https://api.elections.kalshi.com/v1"
KALSHI_V2_REST_URL = "https://api.elections.kalshi.com/trade-api/v2"
WS_BASE_URL = "wss://api.elections.kalshi.com"
WS_URL_SUFFIX = "/trade-api/ws/v2"
KALSHI_WSS_URL = WS_BASE_URL + WS_URL_SUFFIX
MONITORING_INTERVAL_SECONDS = 15  # How often to log market statistics

# --- Database Schema Definition (simplified with JSON storage) ---
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS kalshi_markets_3 (
    market_ticker TEXT PRIMARY KEY,
    found_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS kalshi_trades_3 (
    id SERIAL PRIMARY KEY,
    market_ticker TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kalshi_trades_3_ticker ON kalshi_trades_3(market_ticker);
CREATE INDEX IF NOT EXISTS idx_kalshi_trades_3_server_time ON kalshi_trades_3(server_time_us);

CREATE TABLE IF NOT EXISTS kalshi_orderbook_updates_3 (
    id SERIAL PRIMARY KEY,
    market_ticker TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kalshi_orderbook_updates_3_ticker ON kalshi_orderbook_updates_3(market_ticker);
CREATE INDEX IF NOT EXISTS idx_kalshi_orderbook_updates_3_server_time ON kalshi_orderbook_updates_3(server_time_us);

CREATE TABLE IF NOT EXISTS kalshi_market_lifecycle_3 (
    id SERIAL PRIMARY KEY,
    market_ticker TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kalshi_market_lifecycle_3_ticker ON kalshi_market_lifecycle_3(market_ticker);
"""

# --- Helper & Auth Functions ---

def get_auth_headers(method: str, path: str):
    """Generates authentication headers by reading the private key from a file."""
    try:
        with open(KALSHI_PRIVATE_KEY_PATH, "rb") as key_file:
            private_key = serialization.load_pem_private_key(key_file.read(), password=None)
    except FileNotFoundError:
        logger.error(f"FATAL: Private key file not found at '{KALSHI_PRIVATE_KEY_PATH}'.")
        return None
    except Exception as e:
        logger.error(f"FATAL: Failed to load private key from file. Error: {e}")
        return None

    timestamp_ms = str(int(time.time() * 1000))
    message_to_sign = (timestamp_ms + method + path).encode('utf-8')
    signature = private_key.sign(
        message_to_sign,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256()
    )
    encoded_signature = base64.b64encode(signature).decode('utf-8')
    return {
        "KALSHI-ACCESS-KEY": KALSHI_API_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": encoded_signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
    }

def is_target_market(market_ticker):
    """Checks if a market ticker starts with our target prefixes."""
    ticker_upper = market_ticker.upper()
    return ticker_upper.startswith('KXBTC15M')

def increment_message_stat(market_ticker, message_type):
    """Increments the message count for a specific market and message type."""
    if market_ticker not in message_stats:
        message_stats[market_ticker] = {}
    if message_type not in message_stats[market_ticker]:
        message_stats[market_ticker][message_type] = 0
    message_stats[market_ticker][message_type] += 1

async def get_market_details_v2(session, ticker):
    """Fetches full market details from the V2 endpoint, which requires auth."""
    path = f"/trade-api/v2/markets/{ticker}"
    url = f"https://api.elections.kalshi.com{path}"
    auth_headers = get_auth_headers("GET", path)
    if not auth_headers:
        return None
    try:
        async with session.get(url, headers=auth_headers) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('market')
            else:
                logger.error(f"Failed to get V2 details for {ticker}. Status: {response.status}, Response: {await response.text()}")
                return None
    except aiohttp.ClientError as e:
        logger.error(f"Client error fetching V2 details for {ticker}: {e}")
        return None

# --- Database Functions ---

async def init_db(pool):
    """Initializes the database by creating tables if they don't exist."""
    async with pool.acquire() as connection:
        await connection.execute(CREATE_TABLES_SQL)
    logger.info("Database initialized.")

async def upsert_market(pool, market_json):
    """Inserts or updates a market in the kalshi_markets_3 table."""
    ticker = market_json.get('ticker_name') or market_json.get('ticker')
    if not ticker:
        logger.warning(f"Market JSON missing ticker field: {market_json}")
        return
    
    await pool.execute("""
        INSERT INTO kalshi_markets_3 (market_ticker, found_time_us, message)
        VALUES ($1, $2, $3)
        ON CONFLICT (market_ticker) DO UPDATE SET
            found_time_us = EXCLUDED.found_time_us,
            message = EXCLUDED.message;
    """, ticker, int(time.time() * 1_000_000), json.dumps(market_json))

async def insert_trade(pool, trade_data):
    """Inserts a trade record into the kalshi_trades_3 table."""
    # Trade ts is in seconds, convert to microseconds
    server_time_us = trade_data['ts'] * 1_000_000
    await pool.execute("""
        INSERT INTO kalshi_trades_3 (market_ticker, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4)
    """, trade_data['market_ticker'], int(time.time() * 1_000_000), server_time_us, json.dumps(trade_data))

async def insert_orderbook_update(pool, complete_msg):
    """Inserts an order book update record into the kalshi_orderbook_updates_3 table."""
    market_ticker = complete_msg['msg']['market_ticker']
    found_time_us = int(time.time() * 1_000_000)
    
    # Extract server time based on message type (stored in microseconds)
    if complete_msg['type'] == 'orderbook_snapshot':
        # Snapshots don't have ts, use found_time_us
        server_time_us = found_time_us
    elif complete_msg['type'] == 'orderbook_delta':
        # Delta has ISO 8601 timestamp string with microsecond precision, parse it
        ts_str = complete_msg['msg'].get('ts')
        if ts_str:
            # Parse ISO 8601: "2026-01-06T16:41:16.613685Z"
            from datetime import datetime
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            server_time_us = int(dt.timestamp() * 1_000_000)
        else:
            server_time_us = found_time_us
    else:
        server_time_us = found_time_us
    
    await pool.execute("""
        INSERT INTO kalshi_orderbook_updates_3 (market_ticker, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4)
    """, market_ticker, found_time_us, server_time_us, json.dumps(complete_msg))

async def insert_market_lifecycle(pool, lifecycle_data):
    """Inserts a market lifecycle event into the kalshi_market_lifecycle_3 table."""
    await pool.execute("""
        INSERT INTO kalshi_market_lifecycle_3 (market_ticker, found_time_us, event_type, message)
        VALUES ($1, $2, $3, $4)
    """, lifecycle_data['market_ticker'], int(time.time() * 1_000_000), lifecycle_data['event_type'], json.dumps(lifecycle_data))

# --- Main Logic ---

async def log_market_statistics(tracked_tickers):
    """Periodically logs statistics about subscribed markets and their message counts."""
    while True:
        await asyncio.sleep(MONITORING_INTERVAL_SECONDS)
        
        # Clear and write to monitor.log
        with open(monitor_log_path, 'w') as f:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"=== Market Statistics Report - {timestamp} ===\n")
            
            if not tracked_tickers:
                f.write("No markets currently tracked.\n")
            else:
                f.write(f"Tracking {len(tracked_tickers)} markets\n\n")
                
                # Sort by ticker for consistent ordering
                for ticker in sorted(tracked_tickers):
                    if ticker in message_stats:
                        stats = message_stats[ticker]
                        total = sum(stats.values())
                        stats_str = ", ".join([f"{msg_type}: {count}" for msg_type, count in sorted(stats.items())])
                        f.write(f"{ticker}\n")
                        f.write(f"  Total: {total}\n")
                        f.write(f"  {stats_str}\n\n")
                    else:
                        f.write(f"{ticker}\n")
                        f.write(f"  No messages yet\n\n")

async def find_initial_markets(session, pool):
    """Finds all existing open markets via HTTP and stores them in the DB."""
    logger.info("Starting initial market scan...")
    target_series = ['KXBTC15M']
    initial_tickers = []

    for series in target_series:
        page_number = 1
        while True:
            url = (f"{KALSHI_V1_REST_URL}/events/?status=open,unopened"
                   f"&series_tickers={series}&page_size=100&page_number={page_number}")
            try:
                async with session.get(url) as response:
                    if response.status != 200: break
                    data = await response.json()
                    events = data.get('events', [])
                    if not events: break
                    
                    for event in events:
                        for market in event.get('markets', []):
                            await upsert_market(pool, market)
                            initial_tickers.append(market['ticker_name'])
                    page_number += 1
            except aiohttp.ClientError as e:
                logger.error(f"Client error during initial scan for {series}: {e}")
                break
    
    logger.info(f"Initial scan complete. Found {len(initial_tickers)} markets.")
    return initial_tickers

async def websocket_listener(initial_tickers, pool):
    """Connects to the WebSocket to listen for real-time events."""
    tracked_tickers = set(initial_tickers)
    msg_id_counter = 2 

    while True:
        try:
            auth_headers = get_auth_headers("GET", WS_URL_SUFFIX)
            if not auth_headers: break

            async with websockets.connect(KALSHI_WSS_URL, additional_headers=auth_headers) as websocket:
                logger.info("WebSocket connected.")
                
                # Always subscribe to market lifecycle channel
                lifecycle_sub = {"id": 1, "cmd": "subscribe", "params": {"channels": ["market_lifecycle_v2"]}}
                await websocket.send(json.dumps(lifecycle_sub))

                # Resubscribe to all tracked tickers (trade and orderbook_delta) after connection/reconnection
                if tracked_tickers:
                    trade_sub = {
                        "id": msg_id_counter, "cmd": "subscribe", 
                        "params": {"channels": ["trade", "orderbook_delta"], "market_tickers": list(tracked_tickers)}
                    }
                    await websocket.send(json.dumps(trade_sub))
                    logger.info(f"Subscribed to {len(tracked_tickers)} markets.")
                    msg_id_counter += 1

                async with aiohttp.ClientSession() as session:
                    async for message in websocket:
                        try:
                            msg = json.loads(message)
                            msg_type = msg.get('type')

                            if msg_type == 'trade':
                                trade_msg = msg['msg']
                                await insert_trade(pool, trade_msg)
                                increment_message_stat(trade_msg['market_ticker'], 'trade')
                            
                            elif msg_type == 'orderbook_snapshot':
                                # Store the complete message including type, sid, seq, msg
                                await insert_orderbook_update(pool, msg)
                                increment_message_stat(msg['msg']['market_ticker'], 'orderbook_snapshot')
                            
                            elif msg_type == 'orderbook_delta':
                                # Store the complete message including type, sid, seq, msg
                                await insert_orderbook_update(pool, msg)
                                increment_message_stat(msg['msg']['market_ticker'], 'orderbook_delta')

                            elif msg_type == 'market_lifecycle_v2':
                                lifecycle_msg = msg['msg']
                                event_type = lifecycle_msg['event_type']
                                market_ticker = lifecycle_msg['market_ticker']
                                
                                # Only process and store lifecycle events for target markets
                                if not is_target_market(market_ticker):
                                    continue
                                
                                # Store lifecycle events for target markets
                                await insert_market_lifecycle(pool, lifecycle_msg)
                                increment_message_stat(market_ticker, f'lifecycle_{event_type}')
                                
                                if event_type == 'created':
                                    if market_ticker not in tracked_tickers:
                                        logger.info(f"New target market created: {market_ticker}. Fetching details...")
                                        market_details = await get_market_details_v2(session, market_ticker)
                                        if market_details:
                                            await upsert_market(pool, market_details)
                                            tracked_tickers.add(market_ticker)
                                            
                                            new_sub = {
                                                "id": msg_id_counter, "cmd": "subscribe",
                                                "params": {"channels": ["trade", "orderbook_delta"], "market_tickers": [market_ticker]}
                                            }
                                            await websocket.send(json.dumps(new_sub))
                                            logger.info(f"Subscribed to new market {market_ticker}")
                                            msg_id_counter += 1
                                
                                elif event_type in ['determined', 'settled']:
                                    # Unsubscribe from determined or settled markets
                                    if market_ticker in tracked_tickers:
                                        logger.info(f"Market {market_ticker} {event_type}. Unsubscribing...")
                                        unsub = {
                                            "id": msg_id_counter, "cmd": "unsubscribe",
                                            "params": {"channels": ["trade", "orderbook_delta"], "market_tickers": [market_ticker]}
                                        }
                                        await websocket.send(json.dumps(unsub))
                                        tracked_tickers.remove(market_ticker)
                                        logger.info(f"Unsubscribed from {market_ticker}")
                                        msg_id_counter += 1
                                
                                elif event_type == 'activated':
                                    logger.info(f"Market {market_ticker} activated")
                                
                                elif event_type == 'deactivated':
                                    logger.info(f"Market {market_ticker} deactivated")
                                
                                elif event_type == 'close_date_updated':
                                    logger.info(f"Market {market_ticker} close date updated")
                                    
                        except (json.JSONDecodeError, KeyError) as e:
                            logger.warning(f"Received message with unexpected format: {message}. Error: {e}")

        except websockets.exceptions.InvalidStatusCode as e:
            logger.error(f"WebSocket connection failed: {e.status_code}. Check credentials.", exc_info=True)
            break 
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}. Reconnecting in 15 seconds...", exc_info=True)
            await asyncio.sleep(15)

# --- Main Execution ---

async def main():
    """Main function to run the initial scan and then start the WebSocket listener."""
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST, KALSHI_API_KEY_ID]):
        logger.error("One or more required environment variables are not set.")
        return

    logger.info(f"Monitor log: {monitor_log_path}")
    
    pool = None
    try:
        pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
        logger.info("Database connected.")
        await init_db(pool)
        
        async with aiohttp.ClientSession() as session:
            initial_tickers = await find_initial_markets(session, pool)
        
        # Start monitoring task and websocket listener concurrently
        monitored_tickers = set(initial_tickers)
        await asyncio.gather(
            log_market_statistics(monitored_tickers),
            websocket_listener(initial_tickers, pool)
        )

    except Exception as e:
        logger.critical(f"A critical error occurred in the main function: {e}", exc_info=True)
    finally:
        if pool:
            await pool.close()

if __name__ == "__main__":
    asyncio.run(main())