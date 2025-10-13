import asyncio
import asyncpg
import websockets
import json
import time
from log import log
from typing import Callable, List

class state:
    new_event_tokens : asyncio.Queue = asyncio.Queue()
    pg_conn_pool : asyncpg.Pool = None
    books_queue : asyncio.Queue = asyncio.Queue()
    changes_queue : asyncio.Queue = asyncio.Queue()
    tick_changes_queue : asyncio.Queue = asyncio.Queue()
    resub_count : int = 0
    subscription : dict = {"type": "market", "initial_dump": True, "assets_ids": []}
    register_analytics_tokens : Callable[[List[str]], None] = None

async def create_books_table(reset: bool = True):
    log("events.create_books_table started", "INFO")
    conn = None
    try:
        conn = await state.pg_conn_pool.acquire()

        if reset:
            await conn.execute("DROP TABLE IF EXISTS books")
            log("events.create_books_table dropped table if exists", "INFO")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS books (
                row_index SERIAL PRIMARY KEY,
                insert_time TIMESTAMP(3) WITH TIME ZONE DEFAULT now(),
                server_time BIGINT,
                resub_count BIGINT,
                asset_id TEXT,
                book jsonb
            );
            CREATE INDEX IF NOT EXISTS book_analytics_idx
            ON books(asset_id, server_time ASC, resub_count ASC);
        """)
        log("events.create_books_table created table if not exists", "INFO")

    except Exception as e:
        log(f"events.create_books_table caught exception '{e}' with pg_conn_pool '{state.pg_conn_pool}' and conn '{conn}'", "ERROR")
        raise

    finally:
        if conn is not None:
            await state.pg_conn_pool.release(conn)

async def create_changes_table(reset: bool = True):
    log("events.create_changes_table started", "INFO")
    conn = None
    try:
        conn = await state.pg_conn_pool.acquire()

        if reset:
            await conn.execute("DROP TABLE IF EXISTS changes")
            log("events.create_changes_table dropped table if exists", "INFO")

        await conn.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'change_type') THEN
                    CREATE TYPE change_type AS ENUM ('price_change', 'last_trade_price');
                END IF;
            END$$;

            CREATE TABLE IF NOT EXISTS changes (
                row_index SERIAL PRIMARY KEY,
                insert_time TIMESTAMP(3) WITH TIME ZONE DEFAULT now(),
                server_time BIGINT,
                resub_count BIGINT,
                type change_type,
                asset_id TEXT,
                change jsonb
            );

            CREATE INDEX IF NOT EXISTS changes_analytics_idx
                ON changes(asset_id, server_time ASC, resub_count ASC, type ASC);
        """)
        log("events.create_changes_table created table if not exists", "INFO")

    except Exception as e:
        log(f"events.create_changes_table caught exception '{e}' with pg_conn_pool '{state.pg_conn_pool}' and conn '{conn}'", "ERROR")
        raise

    finally:
        if conn is not None:
            await state.pg_conn_pool.release(conn)

async def create_tick_changes_table(reset: bool = True):
    log("events.create_tick_changes_table started", "INFO")
    conn = None
    try:
        conn = await state.pg_conn_pool.acquire()

        if reset:
            await conn.execute("DROP TABLE IF EXISTS tick_changes")
            log("events.create_tick_changes_table dropped table if exists", "INFO")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tick_changes (
                row_index SERIAL PRIMARY KEY,
                insert_time TIMESTAMP(3) WITH TIME ZONE DEFAULT now(),
                server_time BIGINT,
                resub_count BIGINT,
                asset_id TEXT,
                tick_change jsonb
            );
            CREATE INDEX IF NOT EXISTS ticks_analytics_idx
            ON tick_changes(asset_id, server_time asc, resub_count asc);
        """)
        log("events.create_tick_changes_table created table if not exists", "INFO")

    except Exception as e:
        log(f"events.create_tick_changes_table caught exception '{e}' with pg_conn_pool '{state.pg_conn_pool}' and conn '{conn}'", "ERROR")
        raise

    finally:
        if conn is not None:
            await state.pg_conn_pool.release(conn)

async def empty_queue(queue: asyncio.Queue):
    try:
        while True:
            _ = queue.get_nowait()
            queue.task_done()
    except asyncio.QueueEmpty:
        pass
    return
async def init(register_analytics_tokens: Callable[[List[str]], None] = None, pg_conn_pool:asyncpg.Pool=None, reset:bool=True, ):
    log(f"events.init starting", "INFO")
    if reset:
        state.resub_count = 0
        state.subscription["assets_ids"] = []
        log(f"events.init cleared subscription asset_ids and set resub_count to 0", "INFO")
        await empty_queue(state.books_queue)
        log(f"events.init emptied books_queue", "INFO")
        await empty_queue(state.changes_queue)
        log(f"events.init emptied changes_queue", "INFO")
        await empty_queue(state.tick_changes_queue)
        log(f"events.init emptied tick_changes_queue", "INFO")
        await empty_queue(state.new_event_tokens)
        log(f"events.init emptied new_event_tokens queue", "INFO")

    try:
        if not callable(register_analytics_tokens):
            log(f"events.init received non callable register_analytics_tokens {register_analytics_tokens}")
            raise ValueError(f"market init received invalid register_analytics_tokens")
        state.register_analytics_tokens = register_analytics_tokens
        if not isinstance(pg_conn_pool, asyncpg.Pool):
            log(f"events.init received non asyncpg.Pool pg_conn_pool {pg_conn_pool}")
            raise ValueError(f"market init received invalid pg_conn_pool {pg_conn_pool}")
        state.pg_conn_pool = pg_conn_pool
        log(f"events.init waiting for events.create_books_table", "INFO")
        await create_books_table(reset=reset)
        log(f"events.init waiting for events.create_changes_table", "INFO")
        await create_changes_table(reset=reset)
        log(f"events.init waiting for events.create_tick_changes_table", "INFO")
        await create_tick_changes_table(reset=reset)
        log(f"events.init finished")
    except Exception as e:
        log(f"events.init caught exception: {e}", "ERROR")
        raise RuntimeError("events failed initial connection") from e

def parse_timestamp(timestamp_str:str):
    return int(timestamp_str) if str(timestamp_str).isdigit() else None

async def parse_message(message: str, resub_count:int):
    try:
        msg_json = json.loads(message)
    except Exception as e:
        log(f"events.parse_message caught exception '{e}' with message '{message}'", "WARNING")
        return

    if isinstance(msg_json, dict):
            msg_json = [msg_json]
        
    if not isinstance(msg_json, list):
        log(f"events.parse_message received non list or dict event message '{msg_json}'", "WARNING")
        return
        
    if len(msg_json) == 0:
        log(f"events.parse_message received message with no event '{msg_json}'", "WARNING")
        return

    event_type = msg_json[0].get("event_type", None)
    timestamp = msg_json[0].get("timestamp", None)

    insert_batch = []
    if event_type == "book":
        for book in msg_json:       
            timestamp = book.get("timestamp", None)
            i_timestamp = parse_timestamp(timestamp)
            asset_id = book.get("asset_id", None)
            
            insert_batch.append([i_timestamp, resub_count, asset_id, json.dumps(book)])
        
        if __debug__:
            log(f"events.parse_message enqueueing book batch of size {len(insert_batch)}", "DEBUG")
        await state.books_queue.put(insert_batch)
    
    elif event_type == "price_change":
        price_changes = msg_json[0].get("price_changes", None)
        if not isinstance(price_changes, list):
            log(f"events.parse_message found invalid price_changes {price_changes} in {msg_json[0]} in message {msg_json}", "WARNING")
            return

        i_timestamp = parse_timestamp(timestamp)

        for price_change in price_changes:
            price_change["fee_rate_bps"] = None
            asset_id = price_change.get("asset_id", None)
            insert_batch.append([i_timestamp, resub_count, event_type, asset_id, json.dumps(price_change)])

        if __debug__:
            log(f"events.parse_message enqueueing price_change batch of size {len(insert_batch)}", "DEBUG")
        await state.changes_queue.put(insert_batch)
        
    elif event_type == "last_trade_price":
        for last_trade in msg_json:
            timestamp = last_trade.get("timestamp", None)
            i_timestamp = parse_timestamp(timestamp)
            last_trade["best_bid"] = None
            last_trade["best_ask"] = None
            asset_id = last_trade.get("asset_id", None)
            insert_batch.append([i_timestamp, resub_count, event_type, asset_id, json.dumps(last_trade)])
        if __debug__:
            log(f"events.parse_message enqueuing last_trade_price batch of size {len(insert_batch)}", "DEBUG")
        await state.changes_queue.put(insert_batch)
        
    elif event_type == "tick_size_change":
        for tick_change in msg_json:
            timestamp = tick_change.get("timestamp", None)
            i_timestamp = parse_timestamp(timestamp)
            asset_id = tick_change.get("asset_id", None)
            insert_batch.append([i_timestamp, resub_count, asset_id, json.dumps(tick_change)])
        if __debug__:
            log(f"events.parse_message enqueuing tick_size_change batch of size {len(insert_batch)}", "DEBUG")
        await state.tick_changes_queue.put(insert_batch)

    else:
        log(f"events.parse_message invalid event type {event_type} found in {msg_json}", "WARNING")
        return   
    
async def read_events(wss_cli:websockets.WebSocketClientProtocol, resub_count:int):
    try:
        while True:
                message = None
                message =  await wss_cli.recv()
                #if __debug__:
                    #log(f"events.read_events raw message: {message}", "DEBUG")
                await parse_message(message, resub_count)
    except asyncio.CancelledError:
        if __debug__:
            log(f"events.read_events task cancelled intentionally", "DEBUG")
        return
    except Exception as e:
        log(f"events.read_events caught exception '{e}' with wss_cli '{wss_cli}' and message '{message}'", "ERROR")
        raise
    finally:
        await wss_cli.close()
    
async def subscriber():
    new_read_task = None
    read_task = None
    wss_cli = None
    book_insert_task = None
    change_insert_task = None
    tick_change_insert_task = None
    try:
        book_insert_task = asyncio.create_task(book_inserter())
        change_insert_task = asyncio.create_task(change_inserter())
        tick_change_insert_task = asyncio.create_task(tick_change_inserter())
        wss_cli = await websockets.connect("wss://ws-subscriptions-clob.polymarket.com/ws/market", max_queue=64, ping_interval=10, ping_timeout=10)
        read_task = asyncio.create_task(read_events(wss_cli, state.resub_count))
        while True:
            new_tokens = await state.new_event_tokens.get()
            state.subscription["assets_ids"].extend(new_tokens)
            wss_cli = await websockets.connect("wss://ws-subscriptions-clob.polymarket.com/ws/market", max_queue=64, ping_interval=10, ping_timeout=10)
            await wss_cli.send(json.dumps(state.subscription))
            state.resub_count += 1
            new_read_task = asyncio.create_task(read_events(wss_cli, state.resub_count))
            
            read_task.cancel()
            try:
                await read_task
            except asyncio.CancelledError:
                if __debug__:
                    log("events.subscriber old read_events task cancelled", "DEBUG")
            read_task = new_read_task
            new_read_task = None
            state.register_analytics_tokens(new_tokens)
            if __debug__:
                log(f"events.subscriber resubscription {state.resub_count} complete with {len(state.subscription['assets_ids'])} tokens", "INFO")
    except Exception as e:
        log(f"events.subscriber caught exception '{e}' with wss_cli '{wss_cli}', read_task '{read_task}', new_read_task '{new_read_task}', book_insert_task '{book_insert_task}', change_insert_task '{change_insert_task}', tick_change_insert_task '{tick_change_insert_task}' subscription '{state.subscription}", "ERROR")
        if new_read_task is not None:
            new_read_task.cancel()
        if read_task is not None:
            read_task.cancel()
        if book_insert_task is not None:
            book_insert_task.cancel()
        if change_insert_task is not None:
            change_insert_task.cancel()
        if tick_change_insert_task is not None:
            tick_change_insert_task.cancel()
        if wss_cli is not None:
            await wss_cli.close()
        raise
                
async def book_inserter():
    log(f"events.book_inserter started", "INFO")
    conn = None
    try:
        while True:
            insert_batch = await state.books_queue.get()
            try:
                conn = await state.pg_conn_pool.acquire()
                await conn.executemany("""
                    INSERT INTO books (server_time, resub_count, asset_id, book)
                    VALUES ($1, $2, $3, $4)
                """, insert_batch)
                if __debug__:
                    log(f"events.book_inserter successfully inserted batch of size {len(insert_batch)}", "DEBUG")
            finally:
                state.books_queue.task_done()
                if conn is not None:
                    await state.pg_conn_pool.release(conn)
                    conn = None
    except asyncio.CancelledError:
        if __debug__:
            log("events.book_inserter task cancelled intentionally", "DEBUG")
        return
    except Exception as e:
        log(f"events.book_inserter caught exception '{e}' with pg_conn_pool '{state.pg_conn_pool}', conn '{conn}' and insert_batch '{insert_batch}'", "ERROR")
        raise

async def change_inserter():
    log(f"events.change_inserter started", "INFO")
    conn = None
    try:
        while True:
            insert_batch = await state.changes_queue.get()
            try:
                conn = await state.pg_conn_pool.acquire()
                await conn.executemany("""
                    INSERT INTO changes (server_time, resub_count, type, asset_id, change)
                    VALUES ($1, $2, $3, $4, $5)
                """, insert_batch)
                if __debug__:
                    log(f"events.change_inserter successfully inserted batch of size {len(insert_batch)}", "DEBUG")
            finally:
                state.changes_queue.task_done()
                if conn is not None:
                    await state.pg_conn_pool.release(conn)
                    conn = None
    except asyncio.CancelledError:
        if __debug__:
            log("events.change_inserter task cancelled intentionally", "DEBUG")
        return
    except Exception as e:
        log(f"events.change_inserter caught exception '{e}' with pg_conn_pool '{state.pg_conn_pool}', conn '{conn}' and insert_batch '{insert_batch}'", "ERROR")
        raise


async def tick_change_inserter():
    log(f"events.tick_change_inserter started", "INFO")
    conn = None
    try:
        while True:
            insert_batch = await state.tick_changes_queue.get()
            try:
                conn = await state.pg_conn_pool.acquire()
                await conn.executemany("""
                    INSERT INTO tick_changes (server_time, resub_count, asset_id, tick_change)
                    VALUES ($1, $2, $3, $4)
                """, insert_batch)
                if __debug__:
                    log(f"events.tick_change_inserter successfully inserted batch of size {len(insert_batch)}", "DEBUG")
            finally:
                state.tick_changes_queue.task_done()
                if conn is not None:
                    await state.pg_conn_pool.release(conn)
                    conn = None
    except asyncio.CancelledError:
        if __debug__:
            log("events.tick_change_inserter task cancelled intentionally", "DEBUG")
        return
    except Exception as e:
        log(f"events.tick_change_inserter caught exception '{e}' with pg_conn_pool '{state.pg_conn_pool}', conn '{conn}' and insert_batch '{insert_batch}'", "ERROR")
        raise

