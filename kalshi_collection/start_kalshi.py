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

# --- Kalshi API Configuration ---
KALSHI_V1_REST_URL = "https://api.elections.kalshi.com/v1"
KALSHI_V2_REST_URL = "https://api.elections.kalshi.com/trade-api/v2"
WS_BASE_URL = "wss://api.elections.kalshi.com"
WS_URL_SUFFIX = "/trade-api/ws/v2"
KALSHI_WSS_URL = WS_BASE_URL + WS_URL_SUFFIX

# --- Database Schema Definition (with message columns and indexes) ---
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS kalshi_markets (
    market_ticker TEXT PRIMARY KEY,
    found_time_ms BIGINT NOT NULL,
    market JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS kalshi_trades (
    id SERIAL PRIMARY KEY,
    market_ticker TEXT NOT NULL REFERENCES kalshi_markets(market_ticker),
    timestamp BIGINT NOT NULL,
    price BIGINT NOT NULL,
    quantity BIGINT NOT NULL,
    taker_side TEXT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kalshi_trades_timestamp ON kalshi_trades(timestamp);

CREATE TABLE IF NOT EXISTS kalshi_orderbook_updates (
    id SERIAL PRIMARY KEY,
    market_ticker TEXT NOT NULL REFERENCES kalshi_markets(market_ticker),
    timestamp BIGINT NOT NULL,
    side TEXT NOT NULL,
    price BIGINT NOT NULL,
    delta BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kalshi_orderbook_updates_timestamp ON kalshi_orderbook_updates(timestamp);
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
    return ticker_upper.startswith('KXBTCD') or ticker_upper.startswith('KXNBAGAME')

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
    logger.info("Database tables and indexes initialized successfully.")

async def upsert_market(pool, market_json):
    """Inserts or updates a market in the kalshi_markets table."""
    ticker = market_json.get('ticker_name') or market_json.get('ticker')
    if not ticker:
        logger.warning(f"Market JSON missing ticker field: {market_json}")
        return
    
    await pool.execute("""
        INSERT INTO kalshi_markets (market_ticker, found_time_ms, market)
        VALUES ($1, $2, $3)
        ON CONFLICT (market_ticker) DO UPDATE SET
            found_time_ms = EXCLUDED.found_time_ms,
            market = EXCLUDED.market;
    """, ticker, int(time.time() * 1000), json.dumps(market_json))
    logger.info(f"Upserted market '{ticker}' into the database.")

async def insert_trade(pool, trade_data):
    """Inserts a trade record into the kalshi_trades table."""
    await pool.execute("""
        INSERT INTO kalshi_trades (market_ticker, timestamp, price, quantity, taker_side, message)
        VALUES ($1, $2, $3, $4, $5, $6)
    """, trade_data['market_ticker'], trade_data['ts'] * 1_000_000_000, trade_data['yes_price'], trade_data['count'], trade_data['taker_side'], json.dumps(trade_data))

async def insert_orderbook_update(pool, ob_data):
    """Inserts an order book delta record into the kalshi_orderbook_updates table."""
    await pool.execute("""
        INSERT INTO kalshi_orderbook_updates (market_ticker, timestamp, side, price, delta, message)
        VALUES ($1, $2, $3, $4, $5, $6)
    """, ob_data['market_ticker'], int(time.time() * 1_000_000_000), ob_data['side'], ob_data['price'], ob_data['delta'], json.dumps(ob_data))

# --- Main Logic ---

async def find_initial_markets(session, pool):
    """Finds all existing open markets via HTTP and stores them in the DB."""
    logger.info("--- Starting Initial Market Scan (HTTP V1 Endpoint) ---")
    target_series = ['KXBTCD', 'KXNBAGAME']
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
    
    logger.info(f"--- Initial Scan Complete. Found {len(initial_tickers)} markets. ---")
    return initial_tickers

async def websocket_listener(initial_tickers, pool):
    """Connects to the WebSocket to listen for real-time events."""
    logger.info("--- Starting Real-Time Market Listener (WebSocket) ---")
    tracked_tickers = set(initial_tickers)
    msg_id_counter = 2 

    while True:
        try:
            auth_headers = get_auth_headers("GET", WS_URL_SUFFIX)
            if not auth_headers: break

            async with websockets.connect(KALSHI_WSS_URL, additional_headers=auth_headers) as websocket:
                logger.info("WebSocket connection established successfully!")
                
                lifecycle_sub = {"id": 1, "cmd": "subscribe", "params": {"channels": ["market_lifecycle_v2"]}}
                await websocket.send(json.dumps(lifecycle_sub))

                if tracked_tickers:
                    trade_sub = {
                        "id": msg_id_counter, "cmd": "subscribe", 
                        "params": {"channels": ["trade", "orderbook_delta"], "market_tickers": list(tracked_tickers)}
                    }
                    await websocket.send(json.dumps(trade_sub))
                    msg_id_counter += 1

                async with aiohttp.ClientSession() as session:
                    async for message in websocket:
                        try:
                            msg = json.loads(message)
                            msg_type = msg.get('type')

                            if msg_type == 'trade':
                                await insert_trade(pool, msg['msg'])
                            
                            elif msg_type == 'orderbook_delta':
                                await insert_orderbook_update(pool, msg['msg'])

                            elif msg_type == 'market_lifecycle_v2' and msg['msg']['event_type'] == 'created':
                                new_ticker = msg['msg']['market_ticker']
                                if is_target_market(new_ticker) and new_ticker not in tracked_tickers:
                                    logger.info(f"New target market created: {new_ticker}. Fetching details...")
                                    market_details = await get_market_details_v2(session, new_ticker)
                                    if market_details:
                                        await upsert_market(pool, market_details)
                                        tracked_tickers.add(new_ticker)
                                        
                                        new_sub = {
                                            "id": msg_id_counter, "cmd": "subscribe",
                                            "params": {"channels": ["trade", "orderbook_delta"], "market_tickers": [new_ticker]}
                                        }
                                        await websocket.send(json.dumps(new_sub))
                                        msg_id_counter += 1
                        except (json.JSONDecodeError, KeyError):
                            logger.warning(f"Received message with unexpected format: {message}")

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

    pool = None
    try:
        pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
        logger.info("Successfully connected to PostgreSQL database.")
        await init_db(pool)
        
        async with aiohttp.ClientSession() as session:
            initial_tickers = await find_initial_markets(session, pool)
        
        await websocket_listener(initial_tickers, pool)

    except Exception as e:
        logger.critical(f"A critical error occurred in the main function: {e}", exc_info=True)
    finally:
        if pool:
            await pool.close()
            logger.info("Database connection pool closed.")

if __name__ == "__main__":
    asyncio.run(main())