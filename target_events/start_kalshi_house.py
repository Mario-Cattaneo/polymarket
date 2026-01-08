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
monitor_log_path = os.path.join(os.path.dirname(__file__), "monitor_kalshi_house.log")

# --- Message Statistics Tracking (per outcome) ---
message_stats = {}  # {market_ticker: {outcome: {message_type: count}}}

# --- Target Market Configuration ---
TARGET_MARKET = {
    "ticker": "CONTROLH-2026-D",
    "title": "Will Democrats win the House in 2026?",
    "outcomes": ["Yes", "No"]  # Based on market structure
}

# --- Kalshi API Configuration ---
KALSHI_V2_REST_URL = "https://api.elections.kalshi.com/trade-api/v2"
WS_BASE_URL = "wss://api.elections.kalshi.com"
WS_URL_SUFFIX = "/trade-api/ws/v2"
KALSHI_WSS_URL = WS_BASE_URL + WS_URL_SUFFIX
MONITORING_INTERVAL_SECONDS = 15  # How often to log market statistics

# --- Database Schema Definition ---
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS kalshi_markets_house (
    market_ticker TEXT PRIMARY KEY,
    found_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS kalshi_trades_house (
    id SERIAL PRIMARY KEY,
    market_ticker TEXT NOT NULL,
    outcome TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kalshi_trades_house_ticker ON kalshi_trades_house(market_ticker);
CREATE INDEX IF NOT EXISTS idx_kalshi_trades_house_outcome ON kalshi_trades_house(outcome);
CREATE INDEX IF NOT EXISTS idx_kalshi_trades_house_server_time ON kalshi_trades_house(server_time_us);

CREATE TABLE IF NOT EXISTS kalshi_orderbook_updates_house (
    id SERIAL PRIMARY KEY,
    market_ticker TEXT NOT NULL,
    outcome TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    server_time_us BIGINT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kalshi_orderbook_updates_house_ticker ON kalshi_orderbook_updates_house(market_ticker);
CREATE INDEX IF NOT EXISTS idx_kalshi_orderbook_updates_house_outcome ON kalshi_orderbook_updates_house(outcome);
CREATE INDEX IF NOT EXISTS idx_kalshi_orderbook_updates_house_server_time ON kalshi_orderbook_updates_house(server_time_us);

CREATE TABLE IF NOT EXISTS kalshi_market_lifecycle_house (
    id SERIAL PRIMARY KEY,
    market_ticker TEXT NOT NULL,
    found_time_us BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kalshi_market_lifecycle_house_ticker ON kalshi_market_lifecycle_house(market_ticker);
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

def increment_message_stat(market_ticker, outcome, message_type):
    """Increments the message count for a specific market, outcome, and message type."""
    if market_ticker not in message_stats:
        message_stats[market_ticker] = {}
    if outcome not in message_stats[market_ticker]:
        message_stats[market_ticker][outcome] = {}
    if message_type not in message_stats[market_ticker][outcome]:
        message_stats[market_ticker][outcome][message_type] = 0
    message_stats[market_ticker][outcome][message_type] += 1

def get_outcome_from_symbol(symbol: str) -> str:
    """Maps contract symbol to outcome name.
    
    Kalshi symbols are typically like:
    - CONTROLH-2026-D-YES (or similar for Yes outcome)
    - CONTROLH-2026-D-NO (or similar for No outcome)
    """
    symbol_upper = symbol.upper()
    if 'YES' in symbol_upper:
        return 'Yes'
    elif 'NO' in symbol_upper:
        return 'No'
    else:
        # Default: check last character or fallback
        if symbol_upper.endswith('Y'):
            return 'Yes'
        elif symbol_upper.endswith('N'):
            return 'No'
        else:
            return 'Unknown'

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
    """Inserts or updates a market in the kalshi_markets_house table."""
    ticker = market_json.get('ticker_name') or market_json.get('ticker')
    if not ticker:
        logger.warning(f"Market JSON missing ticker field: {market_json}")
        return
    
    await pool.execute("""
        INSERT INTO kalshi_markets_house (market_ticker, found_time_us, message)
        VALUES ($1, $2, $3)
        ON CONFLICT (market_ticker) DO UPDATE SET
            found_time_us = EXCLUDED.found_time_us,
            message = EXCLUDED.message;
    """, ticker, int(time.time() * 1_000_000), json.dumps(market_json))

async def insert_trade(pool, complete_msg):
    """Inserts a trade record into the kalshi_trades_house table."""
    trade_data = complete_msg['msg']
    market_ticker = trade_data['market_ticker']
    found_time_us = int(time.time() * 1_000_000)
    server_time_us = trade_data['ts'] * 1_000_000
    
    # Determine outcome from the contract symbol
    contract_symbol = trade_data.get('contract_symbol', '')
    outcome = get_outcome_from_symbol(contract_symbol)
    
    await pool.execute("""
        INSERT INTO kalshi_trades_house (market_ticker, outcome, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4, $5)
    """, market_ticker, outcome, found_time_us, server_time_us, json.dumps(complete_msg))

async def insert_orderbook_update(pool, complete_msg):
    """Inserts an order book update record into the kalshi_orderbook_updates_house table."""
    market_ticker = complete_msg['msg']['market_ticker']
    found_time_us = int(time.time() * 1_000_000)
    
    # Extract server time based on message type
    if complete_msg['type'] == 'orderbook_snapshot':
        server_time_us = found_time_us
    elif complete_msg['type'] == 'orderbook_delta':
        ts_str = complete_msg['msg'].get('ts')
        if ts_str:
            from datetime import datetime
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            server_time_us = int(dt.timestamp() * 1_000_000)
        else:
            server_time_us = found_time_us
    else:
        server_time_us = found_time_us
    
    # Determine outcome from contract_symbol if present
    contract_symbol = complete_msg['msg'].get('contract_symbol', '')
    outcome = get_outcome_from_symbol(contract_symbol)
    
    await pool.execute("""
        INSERT INTO kalshi_orderbook_updates_house (market_ticker, outcome, found_time_us, server_time_us, message)
        VALUES ($1, $2, $3, $4, $5)
    """, market_ticker, outcome, found_time_us, server_time_us, json.dumps(complete_msg))

async def insert_market_lifecycle(pool, lifecycle_data):
    """Inserts a market lifecycle event into the kalshi_market_lifecycle_house table."""
    await pool.execute("""
        INSERT INTO kalshi_market_lifecycle_house (market_ticker, found_time_us, event_type, message)
        VALUES ($1, $2, $3, $4)
    """, lifecycle_data['market_ticker'], int(time.time() * 1_000_000), lifecycle_data['event_type'], json.dumps(lifecycle_data))

# --- Main Logic ---

async def log_market_statistics():
    """Periodically logs statistics about the tracked market and its outcomes."""
    while True:
        await asyncio.sleep(MONITORING_INTERVAL_SECONDS)
        
        with open(monitor_log_path, 'w') as f:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"=== House Elections Market Statistics Report - {timestamp} ===\n\n")
            
            ticker = TARGET_MARKET['ticker']
            f.write(f"Market {ticker}: {TARGET_MARKET['title']}\n")
            
            if ticker in message_stats:
                for outcome in TARGET_MARKET['outcomes']:
                    f.write(f"  [{outcome}]:\n")
                    
                    if outcome in message_stats[ticker]:
                        stats = message_stats[ticker][outcome]
                        total = sum(stats.values())
                        stats_str = ", ".join([f"{msg_type}: {count}" for msg_type, count in sorted(stats.items())])
                        f.write(f"    Total: {total}\n")
                        f.write(f"    {stats_str}\n")
                    else:
                        f.write(f"    No messages yet\n")
            else:
                f.write(f"  No activity yet\n")
            
            f.write(f"\n")

async def websocket_listener(pool):
    """Connects to the WebSocket to listen for real-time events."""
    msg_id_counter = 1
    
    while True:
        try:
            auth_headers = get_auth_headers("GET", WS_URL_SUFFIX)
            if not auth_headers:
                break

            async with websockets.connect(KALSHI_WSS_URL, additional_headers=auth_headers) as websocket:
                logger.info("WebSocket connected.")
                
                # Subscribe to market lifecycle channel
                lifecycle_sub = {"id": msg_id_counter, "cmd": "subscribe", "params": {"channels": ["market_lifecycle_v2"]}}
                await websocket.send(json.dumps(lifecycle_sub))
                msg_id_counter += 1
                
                # Subscribe to the target market for trades and orderbook updates
                trade_sub = {
                    "id": msg_id_counter, "cmd": "subscribe", 
                    "params": {"channels": ["trade", "orderbook_delta"], "market_tickers": [TARGET_MARKET['ticker']]}
                }
                await websocket.send(json.dumps(trade_sub))
                logger.info(f"Subscribed to target market: {TARGET_MARKET['ticker']}")
                msg_id_counter += 1

                async with aiohttp.ClientSession() as session:
                    async for message in websocket:
                        try:
                            msg = json.loads(message)
                            msg_type = msg.get('type')

                            if msg_type == 'trade':
                                market_ticker = msg['msg']['market_ticker']
                                if market_ticker == TARGET_MARKET['ticker']:
                                    contract_symbol = msg['msg'].get('contract_symbol', '')
                                    outcome = get_outcome_from_symbol(contract_symbol)
                                    await insert_trade(pool, msg)
                                    increment_message_stat(market_ticker, outcome, 'trade')
                            
                            elif msg_type == 'orderbook_snapshot':
                                market_ticker = msg['msg']['market_ticker']
                                if market_ticker == TARGET_MARKET['ticker']:
                                    contract_symbol = msg['msg'].get('contract_symbol', '')
                                    outcome = get_outcome_from_symbol(contract_symbol)
                                    await insert_orderbook_update(pool, msg)
                                    increment_message_stat(market_ticker, outcome, 'orderbook_snapshot')
                            
                            elif msg_type == 'orderbook_delta':
                                market_ticker = msg['msg']['market_ticker']
                                if market_ticker == TARGET_MARKET['ticker']:
                                    contract_symbol = msg['msg'].get('contract_symbol', '')
                                    outcome = get_outcome_from_symbol(contract_symbol)
                                    await insert_orderbook_update(pool, msg)
                                    increment_message_stat(market_ticker, outcome, 'orderbook_delta')

                            elif msg_type == 'market_lifecycle_v2':
                                lifecycle_msg = msg['msg']
                                market_ticker = lifecycle_msg['market_ticker']
                                
                                # Only process if it's our target market
                                if market_ticker == TARGET_MARKET['ticker']:
                                    event_type = lifecycle_msg['event_type']
                                    
                                    # Store lifecycle events
                                    await insert_market_lifecycle(pool, lifecycle_msg)
                                    increment_message_stat(market_ticker, "lifecycle", f'event_{event_type}')
                                    
                                    logger.info(f"Market lifecycle event for {market_ticker}: {event_type}")
                                
                        except (json.JSONDecodeError, KeyError) as e:
                            logger.warning(f"Received message with unexpected format. Error: {e}")

        except websockets.exceptions.InvalidStatusCode as e:
            logger.error(f"WebSocket connection failed: {e.status_code}. Check credentials.", exc_info=True)
            break 
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}. Reconnecting in 15 seconds...", exc_info=True)
            await asyncio.sleep(15)

# --- Main Execution ---

async def main():
    """Main function to start the WebSocket listener and monitoring."""
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST, KALSHI_API_KEY_ID]):
        logger.error("One or more required environment variables are not set.")
        return

    logger.info(f"Monitor log: {monitor_log_path}")
    logger.info("=== Starting House Elections Market Monitor (Kalshi) ===")
    logger.info(f"Target market: {TARGET_MARKET['ticker']} - {TARGET_MARKET['title']}")
    
    pool = None
    try:
        pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=PG_HOST, port=PG_PORT)
        logger.info("Database connected.")
        await init_db(pool)
        
        async with aiohttp.ClientSession() as session:
            # Fetch and store initial market details
            market_details = await get_market_details_v2(session, TARGET_MARKET['ticker'])
            if market_details:
                await upsert_market(pool, market_details)
                logger.info(f"Stored market details for {TARGET_MARKET['ticker']}")
        
        # Start monitoring task and websocket listener concurrently
        await asyncio.gather(
            log_market_statistics(),
            websocket_listener(pool)
        )

    except Exception as e:
        logger.critical(f"A critical error occurred in the main function: {e}", exc_info=True)
    finally:
        if pool:
            await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
