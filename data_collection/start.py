import os
import asyncio
import time
import traceback
from pathlib import Path
from datetime import datetime
import logging  # Import the standard logging library

# Import your custom modules
import db_cli
import http_cli
import logger
import orderbook
import markets
import events
import analytics
import psutil

# --- Configuration ---
# WARNING: Setting RESET to True will wipe all data on startup.
# This should be False for production.
RESET = True

# --- Loggers ---

def start_log(msg: str):
    """A simple, silent logger that writes a message to the startup.log file."""
    try:
        poly_home = os.getenv("POLY_HOME")
        log_dir = Path(poly_home) / "data_collection" if poly_home else Path(".")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        log_line = f"[{timestamp}] {msg}\n"

        with open(log_dir / "startup.log", 'a', encoding='utf-8') as f:
            f.write(log_line)
    except (IOError, PermissionError):
        # Fail silently if logging isn't possible during startup
        pass

def monitor_log(log_content: str):
    """Writes the monitoring summary to a single "monitor.log" file, overwriting it each time."""
    try:
        poly_home = os.getenv("POLY_HOME")
        log_dir = Path(poly_home) / "data_collection" if poly_home else Path(".")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        log_path = log_dir / "monitor.log"

        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(log_content)
    except (IOError, PermissionError):
        pass

# --- SQL Statements ---
SETUP_SCHEMA_STMT = """
    -- Conditionally create ENUM types to prevent errors on restart
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'side') THEN
            CREATE TYPE SIDE AS ENUM ('BUY', 'SELL');
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'event') THEN
            CREATE TYPE EVENT AS ENUM ('price_change', 'last_trade_price', 'book');
        END IF;
    END$$;

    CREATE TABLE IF NOT EXISTS markets (
        found_time BIGINT NOT NULL,
        asset_id TEXT PRIMARY KEY,
        exhaustion_cycle BIGINT NOT NULL,
        market_id TEXT NOT NULL, 
        condition_id TEXT,
        negrisk_id TEXT,
        "offset" BIGINT NOT NULL,
        closed_time BIGINT DEFAULT NULL,
        message jsonb NOT NULL
    );

    CREATE TABLE IF NOT EXISTS books (
        server_time BIGINT,
        asset_id TEXT,
        conflict_count BIGINT NOT NULL DEFAULT 1,
        message jsonb NOT NULL,
        PRIMARY KEY (server_time, asset_id)
    );

    CREATE TABLE IF NOT EXISTS price_changes (
        server_time BIGINT,
        asset_id TEXT,
        price REAL,
        size REAL,
        side SIDE,
        conflict_count BIGINT NOT NULL DEFAULT 1,
        message jsonb NOT NULL,
        PRIMARY KEY (server_time, side, price, asset_id)
    );

    CREATE TABLE IF NOT EXISTS last_trade_prices (
        server_time BIGINT,
        asset_id TEXT,
        price REAL,
        size REAL,
        side SIDE,
        conflict_count BIGINT NOT NULL DEFAULT 1,
        fee_rate_bps REAL NOT NULL,
        message jsonb NOT NULL,
        PRIMARY KEY (server_time, side, price, asset_id)
    );

    CREATE TABLE IF NOT EXISTS tick_changes (
        server_time BIGINT,
        asset_id TEXT,
        tick_size REAL,
        conflict_count BIGINT NOT NULL DEFAULT 1,
        message jsonb NOT NULL,
        PRIMARY KEY (server_time, asset_id)
    );

    CREATE TABLE IF NOT EXISTS server_book (
        server_time BIGINT,
        asset_id TEXT,
        false_misses BIGINT NOT NULL,
        message jsonb NOT NULL,
        PRIMARY KEY (server_time, asset_id)
    );

    CREATE TABLE IF NOT EXISTS markets_rtt (timestamp_ms BIGINT PRIMARY KEY, rtt REAL NOT NULL );
    CREATE TABLE IF NOT EXISTS analytics_rtt (timestamp_ms BIGINT PRIMARY KEY, rtt REAL NOT NULL );

    DROP TABLE IF EXISTS events_connections;
    CREATE TABLE events_connections (
        timestamp_ms BIGINT NOT NULL,
        success BOOLEAN NOT NULL, 
        reason TEXT DEFAULT NULL,
        conflict_count BIGINT NOT NULL DEFAULT 1,
        CONSTRAINT pk_events_connections PRIMARY KEY (timestamp_ms, success)
    );

    -- === INDEXES ===
    CREATE INDEX IF NOT EXISTS idx_markets_found_time ON markets(found_time);
    CREATE INDEX IF NOT EXISTS idx_markets_closed_time ON markets(closed_time);
    CREATE INDEX IF NOT EXISTS idx_books_server_time ON books (server_time);
    CREATE INDEX IF NOT EXISTS idx_books_server_time_asset_id ON books (server_time, asset_id);
    CREATE INDEX IF NOT EXISTS idx_price_changes_server_time ON price_changes (server_time);
    CREATE INDEX IF NOT EXISTS idx_price_changes_server_time_asset_id ON price_changes (server_time, asset_id);
    CREATE INDEX IF NOT EXISTS idx_last_trade_prices_server_time ON last_trade_prices (server_time);
    CREATE INDEX IF NOT EXISTS idx_last_trade_prices_server_time_asset_id ON last_trade_prices (server_time, asset_id);
    CREATE INDEX IF NOT EXISTS idx_tick_changes_server_time ON tick_changes (server_time);
    CREATE INDEX IF NOT EXISTS idx_tick_changes_server_time_asset_id ON tick_changes (server_time, asset_id);
    CREATE INDEX IF NOT EXISTS idx_server_book_server_time ON server_book (server_time);
    CREATE INDEX IF NOT EXISTS idx_server_book_server_time_asset_id ON server_book (server_time, asset_id);
    CREATE INDEX IF NOT EXISTS idx_markets_rtt_timestamp_ms ON markets_rtt(timestamp_ms);
    CREATE INDEX IF NOT EXISTS idx_analytics_rtt_timestamp_ms ON analytics_rtt(timestamp_ms);
    CREATE INDEX IF NOT EXISTS idx_events_connections_timestamp_ms ON events_connections(timestamp_ms);
"""

TEARDOWN_SCHEMA_STMT = """
    DROP TABLE IF EXISTS
        book_metrics, markets, books, price_changes, 
        last_trade_prices, tick_changes, server_book,
        markets_rtt, analytics_rtt, events_connections CASCADE;

    DROP TYPE IF EXISTS
        SIDE, EVENT CASCADE;
"""

# --- System Monitoring ---
monitor_sleep = 2

async def monitor_system(markets, analytics, events):
    """A unified monitoring task that logs high-level status and memory footprint."""
    global monitor_sleep
    try:
        process = psutil.Process(os.getpid())
        memory_mb = process.memory_info().rss / (1024 ** 2)
        
        markets_tasks = markets.task_summary()
        analytics_tasks = analytics.task_summary()
        events_tasks = events.task_summary()
        task_count = len(asyncio.all_tasks())
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        log_content = (
            f"--- System Status at {timestamp} ---\n"
            f"Memory Footprint: {memory_mb:.2f} MB\n"
            f"AsyncIO Tasks: {task_count}\n\n"
            f"--- Markets ---\n{markets_tasks}\n\n"
            f"--- Analytics ---\n{analytics_tasks}\n\n"
            f"--- Events ---\n"
            f"Summary: {events_tasks}\n"
        )
        
        monitor_log(log_content)
        
        monitor_sleep = 2
        await asyncio.sleep(monitor_sleep)
    except Exception as e:
        start_log(f"[ERROR] Unified monitor task failed: {e!r}")
        monitor_sleep = min(monitor_sleep * 2, 777)
        await asyncio.sleep(monitor_sleep)

# --- Main Application Logic ---

async def start_collection():
    start_log("[INFO] --- Application Starting ---")

    loop = asyncio.get_running_loop()
    if loop.get_debug():
        loop.slow_callback_duration = 0.100
        start_log(f"[INFO] Asyncio debug mode is ON. Slow callback threshold set to {loop.slow_callback_duration}s.")
    else:
        start_log("[INFO] Asyncio debug mode is OFF.")
        
    start_log("[INFO] Validating environment variables...")
    poly_home = os.getenv("POLY_HOME")
    if not poly_home:
        start_log("[FATAL] Environment variable POLY_HOME is not set.")
        return
    
    poly_logs = os.getenv("POLY_LOGS")
    if not poly_logs:
        start_log("[FATAL] Environment variable POLY_LOGS is not set.")
        return

    db_socket = os.getenv("PG_SOCKET")
    db_name = os.getenv("POLY_DB")
    db_user = os.getenv("POLY_DB_CLI")
    db_pass = os.getenv("POLY_DB_CLI_PASS")
    if not all([db_socket, db_name, db_user, db_pass]):
        start_log("[FATAL] One or more database environment variables are not set.")
        return
        
    start_log("[INFO] All environment variables are present.")

    try:
        _logger = logger.Logger(log_dir=poly_logs)
        log = _logger.get_logger("main")
        log("Main logger initialized.", "INFO")
    except Exception as e:
        start_log(f"[FATAL] Failed to initialize main logger in '{poly_logs}': {e!r}")
        return

    _db_cli = db_cli.DatabaseManager(db_host=db_socket, db_name=db_name, db_user=db_user, db_pass=db_pass)
    try:
        await _db_cli.connect()
        log("Database manager connected.", "INFO")
    except Exception as e:
        log(f"Failed to connect to database: {e!r}", "FATAL")
        return

    try:
        if RESET:
            log("RESET flag is TRUE. Tearing down existing schema...", "WARN")
            await _db_cli.exec(task_id="teardown_schema", stmt=TEARDOWN_SCHEMA_STMT)
            log("Schema teardown complete.", "WARN")
        
        log("Setting up database schema...", "INFO")
        await _db_cli.exec(task_id="setup_schema", stmt=SETUP_SCHEMA_STMT)
        log("Database schema setup complete.", "INFO")
    except Exception as e:
        log(f"Failed to set up database schema: {e!r}", "FATAL")
        await _db_cli.disconnect()
        return
    
    try:
        log("Initializing application modules...", "INFO")
        _http_cli = http_cli.HttpManager(keepalive_expiry=7.0)
        _orderbook_registry = orderbook.OrderbookRegistry()
        
        _markets = markets.Markets(log=_logger.get_logger("markets"), http_man=_http_cli, db_man=_db_cli, orderbook_reg=_orderbook_registry)
        _analytics = analytics.Analytics(log=_logger.get_logger("analytics"), http_man=_http_cli, db_man=_db_cli, orderbook_reg=_orderbook_registry)
        _events = events.Events(log=_logger.get_logger("events"), db_man=_db_cli, orderbook_reg=_orderbook_registry)
    
        log("All application modules initialized.", "INFO")
    except Exception as e:
        log(f"Failed to initialize core application modules: {e!r}", "FATAL")
        await _db_cli.disconnect()
        return

    try:
        log("Starting collection services...", "INFO")
        await _markets.start(restore=True)
        await _events.start(restore=True, markets_exhausted=_markets.markets_exhausted)
        await _analytics.start()
        log("All collection services started successfully. System is running.", "INFO")
    except Exception as e:
        log(f"A critical error occurred while starting collection services: {e!r}\n{traceback.format_exc()}", "FATAL")
        await _db_cli.disconnect()
        return

    try:
        while True:
            await monitor_system(_markets, _analytics, _events)
    finally:
        log("Shutdown signal received. Cleaning up...", "INFO")
        if '_events' in locals(): _events.stop()
        if '_analytics' in locals(): _analytics.stop()
        if '_markets' in locals(): _markets.stop()
        await _db_cli.disconnect()
        _logger.close()
        log("Cleanup complete. Exiting.", "INFO")

if __name__ == "__main__":
    # --- Main Execution Block ---
    
    # Set to True for verbose asyncio logging, False for production.
    DEBUG = False

    try:
        # --- Robust Logging Setup ---
        poly_logs_dir = Path(os.getenv("POLY_LOGS", "."))
        poly_logs_dir.mkdir(parents=True, exist_ok=True)
        
        # This file will capture all warnings and errors from any library (e.g., asyncio)
        warnings_log_path = poly_logs_dir / "warnings.log"

        # Configure the root logger BEFORE starting asyncio.
        # This ensures all subsequent logging is captured.
        logging.basicConfig(
            level=logging.DEBUG if DEBUG else logging.WARNING,
            format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
            filename=warnings_log_path,
            filemode='w' # Overwrite the log on each start
        )
        
        # Run the main application loop.
        asyncio.run(start_collection(), debug=DEBUG)

    except KeyboardInterrupt:
        start_log("[INFO] Application interrupted by user (KeyboardInterrupt).")
    except Exception as e:
        start_log(f"[FATAL] An unhandled exception occurred in the main execution block: {e!r}")
        # Also log the full traceback to the startup log for diagnosis.
        start_log(traceback.format_exc())