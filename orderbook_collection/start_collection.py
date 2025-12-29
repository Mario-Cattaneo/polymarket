import os
import sys
import asyncio
import traceback
from pathlib import Path
from datetime import datetime
import logging
import psutil
import db_cli
import http_cli
import logger
import orderbook
import markets
import events
import analytics

# Set to True to wipe the database and start fresh
RESET = False

POLY_HOME = None
POLY_LOGS = None

def monitor_log(log_content: str):
    """Writes the monitoring summary to POLY_HOME/orderbook_collection/monitor.log."""
    try:
        log_dir = Path(POLY_HOME) / "orderbook_collection"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "monitor.log"
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(log_content)
    except (IOError, PermissionError) as e:
        print(f"[ERROR] Could not write to monitor log: {e}")


# ====================================================================
# CONFIGURABLE PARAMETERS
# ====================================================================

# --- Partition Timing ---
partition_duration_ms = 10800000
partition_buffer_count = 2

# --- Application Scheduler ---
MAINTENANCE_INTERVAL_SECONDS = 7200

PARTITIONED_TABLES = [
    'buffer_books_2', 'buffer_price_changes_2', 'buffer_last_trade_prices_2',
    'buffer_tick_changes_2', 'buffer_server_book_2', 'buffer_markets_rtt_2',
    'buffer_analytics_rtt_2', 'buffer_events_connections_2',
]


# ====================================================================
# DATABASE INITIALIZATION SCRIPT
# ====================================================================

# --- REFACTORED: Updated schema with 'events' and 'markets_4' tables ---
SETUP_SCHEMA_STMT = f"""
-- Part 1: Schema and Function Definition
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'side') THEN CREATE TYPE SIDE AS ENUM ('BUY', 'SELL'); END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'event') THEN CREATE TYPE EVENT AS ENUM ('price_change', 'last_trade_price', 'book'); END IF;
END$$;

-- NEW: events table to store the raw event payload
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    found_time_ms BIGINT,
    message jsonb
);

-- UPDATED: markets_4 table, lighter and references the events table
CREATE TABLE IF NOT EXISTS markets_4 (
    token_id TEXT PRIMARY KEY,
    market_id TEXT,
    condition_id TEXT,
    question_id TEXT,
    event_id TEXT REFERENCES events(event_id),
    found_index BIGINT,
    found_time_ms BIGINT,
    closed_time_ms BIGINT DEFAULT NULL,
    exhaustion_cycle BIGINT,
    missed_before_gone BIGINT DEFAULT NULL
);
CREATE INDEX IF NOT EXISTS idx_markets_4_event_id ON markets_4(event_id);

-- Buffer tables (Partitioned by time)
CREATE TABLE IF NOT EXISTS buffer_books_2 (found_index BIGINT, found_time_ms BIGINT, server_time_ms BIGINT, asset_id TEXT, message jsonb ) PARTITION BY RANGE (found_time_ms);
CREATE TABLE IF NOT EXISTS buffer_price_changes_2 (found_index BIGINT, found_time_ms BIGINT, server_time_ms BIGINT, asset_id TEXT, price REAL, size REAL, side SIDE, message jsonb ) PARTITION BY RANGE (found_time_ms);
CREATE TABLE IF NOT EXISTS buffer_last_trade_prices_2 (found_index BIGINT, found_time_ms BIGINT, server_time_ms BIGINT, asset_id TEXT, price REAL, size REAL, side SIDE, fee_rate_bps REAL , message jsonb ) PARTITION BY RANGE (found_time_ms);
CREATE TABLE IF NOT EXISTS buffer_tick_changes_2 (found_index BIGINT, found_time_ms BIGINT, server_time_ms BIGINT, asset_id TEXT, tick_size REAL, message jsonb ) PARTITION BY RANGE (found_time_ms);
CREATE TABLE IF NOT EXISTS buffer_server_book_2 (found_index BIGINT, found_time_ms BIGINT, server_time_ms BIGINT, asset_id TEXT, false_misses BIGINT , message jsonb ) PARTITION BY RANGE (found_time_ms);
CREATE TABLE IF NOT EXISTS buffer_markets_rtt_2 (found_time_ms BIGINT, rtt REAL) PARTITION BY RANGE (found_time_ms);
CREATE TABLE IF NOT EXISTS buffer_analytics_rtt_2 (found_time_ms BIGINT, rtt REAL) PARTITION BY RANGE (found_time_ms);
CREATE TABLE IF NOT EXISTS buffer_events_connections_2 (found_time_ms BIGINT, success BOOLEAN, reason TEXT) PARTITION BY RANGE (found_time_ms);

CREATE TABLE IF NOT EXISTS partition_manager_config (system_start_time_ms BIGINT PRIMARY KEY);

CREATE OR REPLACE FUNCTION create_floating_partition_if_needed(parent_table_name TEXT)
RETURNS void AS $$
DECLARE
    initial_start_time_ms BIGINT;
    current_time_ms BIGINT;
    partition_duration BIGINT := {partition_duration_ms};
    day_index BIGINT;
    partition_start_ms BIGINT;
    partition_end_ms BIGINT;
    partition_name TEXT;
BEGIN
    SELECT system_start_time_ms INTO initial_start_time_ms FROM partition_manager_config LIMIT 1;
    IF initial_start_time_ms IS NULL THEN RAISE EXCEPTION 'Partition manager is not initialized.'; END IF;
    current_time_ms := (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT;
    FOR i IN 0..{partition_buffer_count} LOOP
        day_index := floor((current_time_ms - initial_start_time_ms) / partition_duration)::BIGINT + i;
        partition_name := parent_table_name || '_p' || day_index;
        IF NOT EXISTS(SELECT 1 FROM pg_class WHERE relname=partition_name) THEN
            partition_start_ms := initial_start_time_ms + (day_index * partition_duration);
            partition_end_ms := partition_start_ms + partition_duration;
            EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%s) TO (%s);', partition_name, parent_table_name, partition_start_ms, partition_end_ms);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Part 2: One-Time System Initialization
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM partition_manager_config) THEN RETURN; END IF;
    RAISE NOTICE 'INITIALIZING PARTITION MANAGER...';
    INSERT INTO partition_manager_config (system_start_time_ms) VALUES ((EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT);
    
    -- Initialize partitions for all buffer tables
    PERFORM create_floating_partition_if_needed('buffer_books_2');
    PERFORM create_floating_partition_if_needed('buffer_price_changes_2');
    PERFORM create_floating_partition_if_needed('buffer_last_trade_prices_2');
    PERFORM create_floating_partition_if_needed('buffer_tick_changes_2');
    PERFORM create_floating_partition_if_needed('buffer_server_book_2');
    PERFORM create_floating_partition_if_needed('buffer_markets_rtt_2');
    PERFORM create_floating_partition_if_needed('buffer_analytics_rtt_2');
    PERFORM create_floating_partition_if_needed('buffer_events_connections_2');
    RAISE NOTICE 'INITIALIZATION COMPLETE.';
END;
$$;
"""

TEARDOWN_SCHEMA_STMT = """
    DROP TABLE IF EXISTS
        markets_4, events, -- Updated tables
        buffer_books_2, buffer_price_changes_2, buffer_last_trade_prices_2,
        buffer_tick_changes_2, buffer_server_book_2, buffer_markets_rtt_2,
        buffer_analytics_rtt_2, buffer_events_connections_2,
        partition_manager_config CASCADE;
    
    DROP FUNCTION IF EXISTS create_floating_partition_if_needed(TEXT) CASCADE;
    DROP TYPE IF EXISTS SIDE, EVENT CASCADE;
"""

# ====================================================================
# MONITORING AND MAINTENANCE (No changes needed here)
# ====================================================================

monitor_sleep = 2
previous_tasks = set()

def _format_task_info(task: asyncio.Task) -> str:
    try:
        name = task.get_name()
        if name.startswith('Task-'):
             coro = task.get_coro()
             if hasattr(coro, '__qualname__'): name = f"{name}: {coro.__qualname__}"
        return name
    except Exception:
        return str(task)

def _format_buffer_sizes(buffer_dict: dict) -> str:
    if not buffer_dict: return "  Buffers: (none)"
    items = ", ".join([f"{k}={v}" for k, v in buffer_dict.items()])
    return f"  Buffers: {items}"

async def monitor_system(markets, analytics, events):
    global monitor_sleep, previous_tasks
    try:
        process = psutil.Process(os.getpid())
        memory_mb = process.memory_info().rss / (1024 ** 2)
        
        current_tasks = set(asyncio.all_tasks())
        new_tasks = current_tasks - previous_tasks
        finished_tasks = previous_tasks - current_tasks
        
        new_tasks_info = "\n".join([f"  + {_format_task_info(t)}" for t in new_tasks])
        finished_tasks_info = "\n".join([f"  - {_format_task_info(t)}" for t in finished_tasks])
        
        markets_summary = markets.task_summary()
        analytics_summary = analytics.task_summary()
        events_summary = events.task_summary()
        
        markets_buffers = _format_buffer_sizes(markets.get_buffer_sizes())
        analytics_buffers = _format_buffer_sizes(analytics.get_buffer_sizes())
        events_buffers = _format_buffer_sizes(events.get_buffer_sizes())
        
        task_count = len(current_tasks)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        log_content = (
            f"--- System Status at {timestamp} ---\n"
            f"Memory Footprint: {memory_mb:.2f} MB\n"
            f"AsyncIO Tasks: {task_count} (New: {len(new_tasks)}, Finished: {len(finished_tasks)})\n\n"
        )
        if new_tasks: log_content += f"--- New Tasks ---\n{new_tasks_info}\n\n"
        if finished_tasks: log_content += f"--- Finished Tasks ---\n{finished_tasks_info}\n\n"
        log_content += (
            f"--- Markets ---\n{markets_summary}\n{markets_buffers}\n\n"
            f"--- Analytics ---\n{analytics_summary}\n{analytics_buffers}\n\n"
            f"--- Events ---\n{events_summary}\n{events_buffers}\n"
        )
        
        monitor_log(log_content)
        previous_tasks = current_tasks
        monitor_sleep = 2
        await asyncio.sleep(monitor_sleep)
    except Exception as e:
        print(f"[ERROR] Unified monitor task failed: {e!r}")
        monitor_sleep = min(monitor_sleep * 2, 777)
        await asyncio.sleep(monitor_sleep)

async def partition_maintenance_worker(_db_cli: db_cli.DatabaseManager):
    """Background task for scheduling partition creation."""
    print("[INFO] Partition maintenance worker started.")
    while True:
        try:
            for table_name in PARTITIONED_TABLES:
                stmt = f"SELECT create_floating_partition_if_needed('{table_name}');"
                await _db_cli.exec(task_id=f"maintenance_{table_name}", stmt=stmt)
        except Exception as e:
            print(f"[ERROR] Partition maintenance failed: {e!r}")
        await asyncio.sleep(MAINTENANCE_INTERVAL_SECONDS)


# ====================================================================
# MAIN EXECUTION (No changes needed here)
# ====================================================================

async def start_collection():
    global POLY_HOME, POLY_LOGS
    print("[INFO] starting")

    # Validate Environment
    env_vars = ["PG_SOCKET", "POLY_PG_PORT", "POLY_DB", "POLY_DB_CLI", "POLY_DB_CLI_PASS", "POLY_HOME", "POLY_LOGS"]
    for var in env_vars:
        if not os.getenv(var):
            print(f"[ERROR] missing env var {var}")
            return
    
    # Set global variables from environment
    POLY_HOME = os.getenv("POLY_HOME")
    POLY_LOGS = os.getenv("POLY_LOGS")

    # Initialize Logger
    try:
        _logger = logger.Logger(log_dir=os.getenv("POLY_LOGS"))
    except Exception as e:
        print(f"[FATAL] failed to initialize logger: {e!r}")
        return

    # Database Connection
    _db_cli = db_cli.DatabaseManager(
        db_host=os.getenv("PG_SOCKET"), db_port=os.getenv("POLY_PG_PORT"), 
        db_name=os.getenv("POLY_DB"), db_user=os.getenv("POLY_DB_CLI"), 
        db_pass=os.getenv("POLY_DB_CLI_PASS")
    )
    try:
        await _db_cli.connect()
    except Exception as e:
        print(f"[FATAL] failed to connect to database: {e}")
        return

    # Schema Setup
    try:
        if RESET:
            print("[WARN] RESET is TRUE. Tearing down schema...")
            await _db_cli.exec(task_id="teardown", stmt=TEARDOWN_SCHEMA_STMT)
        
        await _db_cli.exec(task_id="setup", stmt=SETUP_SCHEMA_STMT)
        print("[INFO] Database schema ready.")
    except Exception as e:
        print(f"[FATAL] Schema setup failed: {e}")
        await _db_cli.disconnect()
        return
    
    maintenance_task = asyncio.create_task(partition_maintenance_worker(_db_cli))
    
    try:
        _http_cli = http_cli.HttpManager(keepalive_expiry=7.0)
        _orderbook_registry = orderbook.OrderbookRegistry()
        
        _markets = markets.Markets(log=_logger.get_logger("markets"), http_man=_http_cli, db_man=_db_cli, orderbook_reg=_orderbook_registry)
        _analytics = analytics.Analytics(log=_logger.get_logger("analytics"), http_man=_http_cli, db_man=_db_cli, orderbook_reg=_orderbook_registry)
        _events = events.Events(log=_logger.get_logger("events"), db_man=_db_cli, orderbook_reg=_orderbook_registry)
    
        print("[INFO] Modules initialized. Starting services...")
        
        await _markets.start(restore=not RESET)
        await _events.start(markets_exhausted=_markets.markets_exhausted)
        await _analytics.start()
        
        print("[INFO] System running.")
        while True:
            await monitor_system(_markets, _analytics, _events)

    except Exception as e:
        print(f"[FATAL] Critical error: {e!r}\n{traceback.format_exc()}")
    finally:
        maintenance_task.cancel()
        if '_events' in locals(): _events.stop()
        if '_analytics' in locals(): _analytics.stop()
        if '_markets' in locals(): _markets.stop()
        await _db_cli.disconnect()
        _logger.close()

if __name__ == "__main__":
    try:
        asyncio.run(start_collection())
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user.")
    except Exception as e:
        print(f"[FATAL] Unhandled exception: {e!r}")