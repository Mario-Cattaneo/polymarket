import asyncio
import asyncpg
import websockets
import json
import inspect
import time
from log import log
from typing import Callable, List

MAX_TOKENS_PER_SUB = 10000

class state:
    new_event_tokens : asyncio.Queue = asyncio.Queue()
    pg_conn_pool : asyncpg.Pool = None
    books_queue : asyncio.Queue = asyncio.Queue()
    changes_queue : asyncio.Queue = asyncio.Queue()
    tick_changes_queue : asyncio.Queue = asyncio.Queue()
    resub_count : int = 0
    subscriptions : dict = [{"type": "market", "initial_dump": True, "assets_ids": []}]
    register_analytics_tokens : Callable[[List[str]], None] = None
    new_tokens : list[str] = []

async def force_resubscribe():
    return

async def register_tokens(new_tokens : list[str]):
    try:
        all_subs = state.subscriptions
        end = len(all_subs) - 1
        sub = all_subs[end]
        for token in new_tokens:
            if len(sub["assets_ids"]) == MAX_TOKENS_PER_SUB:
                all_subs.append({"type": "market", "initial_dump": True, "assets_ids": []})
                end += 1
                sub = all_subs[end]
            sub["assets_ids"].append(token)
        
        state.tokens_for_analytics = new_tokens
        await state.new_event_tokens.put([-1])
        state.new_tokens = new_tokens
        if __debug__:
            log(f"events.register_tokens registered {len(new_tokens)} tokens", "DEBUG")
    except Exception as e:
        log(f"events.register_tokens failed with exception '{e}' with new_tokens '{new_tokens}", "ERROR")
        raise

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
                insert_time BIGINT DEFAULT (extract(epoch FROM now()) * 1000)::BIGINT NOT NULL, -- ms since unix epoch
                server_time BIGINT, -- ms since unix epoch, NULL if not found in server ws msg
                reader_task BIGINT NOT NULL, 
                resub_count BIGINT NOT NULL,
                asset_id TEXT NOT NULL, -- UTF-8
                book jsonb NOT NULL -- json object
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
                insert_time BIGINT DEFAULT (extract(epoch FROM now()) * 1000)::BIGINT, -- ms since unix epoch
                server_time BIGINT, -- ms since unix epoch, NULL if not found in server ws msg
                reader_task BIGINT NOT NULL,
                resub_count BIGINT NOT NULL,
                type change_type NOT NULL,
                asset_id TEXT NOT NULL, -- UTF-8
                change jsonb NOT NULL -- json object
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
                insert_time BIGINT DEFAULT (extract(epoch FROM now()) * 1000)::BIGINT, -- ms since unix epoch
                server_time BIGINT, -- ms since unix epoch, NULL if not found in server ws msg
                reader_task BIGINT NOT NULL,
                resub_count BIGINT NOT NULL,
                asset_id TEXT NOT NULL, -- UTF-8
                tick_change jsonb NOT NULL -- json object
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
        state.subscriptions = [{"type": "market", "initial_dump": True, "assets_ids": []}]
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
        if not callable(register_analytics_tokens) or inspect.iscoroutinefunction(register_analytics_tokens):
            log(f"events.init received non callable register_analytics_tokens {register_analytics_tokens}")
            raise ValueError(f"events.init received invalid register_analytics_tokens")
        if len(inspect.signature(register_analytics_tokens).parameters) != 1:
            log(f"events.init received  register_analytics_tokens that does not take exactly one argument'{register_analytics_tokens}'", "ERROR")
            raise ValueError("invalid register_analytics_tokens argument for markets.init")
    
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

async def parse_message(message: str, reader_task: int, resub_count:int):
    try:
        msg_json = json.loads(message)
    except Exception as e:
        log(f"events.parse_message caught exception '{e}' with message '{message}'", "WARNING")
        return

    # should only be the case for price change message
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
            if not isinstance(book, dict):
                log(f"events.parse_message found non dict book in message '{message}'", "ERROR")
                return

            timestamp = book.get("timestamp", None)
            i_timestamp = parse_timestamp(timestamp)
            if not isinstance(i_timestamp, int):
                log(f"events.parse_message invalid timestamp '{i_timestamp}' found in {msg_json}", "WARNING")

            asset_id = book.get("asset_id", None)
            if not isinstance(asset_id, str):
                log(f"events.parse_message invalid asset_id '{asset_id}' found in {msg_json}", "WARNING")
                return
            
            insert_batch.append([i_timestamp, reader_task, resub_count, asset_id, json.dumps(book)])
        
        if __debug__:
            log(f"events.parse_message enqueueing book batch of size {len(insert_batch)}", "DEBUG")
        await state.books_queue.put(insert_batch)
    
    elif event_type == "price_change":
        price_changes = msg_json[0].get("price_changes", None)
        if not isinstance(price_changes, list):
            log(f"events.parse_message found invalid price_changes {price_changes} in {msg_json[0]} in message {msg_json}", "WARNING")
            return

        i_timestamp = parse_timestamp(timestamp)
        if not isinstance(i_timestamp, int):
            log(f"events.parse_message invalid timestamp '{i_timestamp}' found in {msg_json}", "WARNING")

        for price_change in price_changes:
            if not isinstance(price_change, dict):
                log(f"events.parse_message found non dict price_change in message '{message}'", "ERROR")
                return

            price_change["fee_rate_bps"] = None
            asset_id = price_change.get("asset_id", None)
            if not isinstance(asset_id, str):
                log(f"events.parse_message invalid asset_id '{asset_id}' found in {msg_json}", "WARNING")
                return
            

            insert_batch.append([i_timestamp, reader_task, resub_count, event_type, asset_id, json.dumps(price_change)])

        if __debug__:
            log(f"events.parse_message enqueueing price_change batch of size {len(insert_batch)}", "DEBUG")
        await state.changes_queue.put(insert_batch)
        
    elif event_type == "last_trade_price":
        for last_trade in msg_json:
            if not isinstance(last_trade, dict):
                log(f"events.parse_message found non dict last_trade in message '{message}'", "ERROR")
                return

            timestamp = last_trade.get("timestamp", None)
            i_timestamp = parse_timestamp(timestamp)
            if not isinstance(i_timestamp, int):
                log(f"events.parse_message invalid timestamp '{i_timestamp}' found in {msg_json}", "WARNING")

            last_trade["best_bid"] = None
            last_trade["best_ask"] = None
            asset_id = last_trade.get("asset_id", None)
            if not isinstance(asset_id, str):
                log(f"events.parse_message invalid asset_id '{asset_id}' found in {msg_json}", "WARNING")
                return
            
            insert_batch.append([i_timestamp, reader_task, resub_count, event_type, asset_id, json.dumps(last_trade)])
        if __debug__:
            log(f"events.parse_message enqueuing last_trade_price batch of size {len(insert_batch)}", "DEBUG")
        await state.changes_queue.put(insert_batch)
        
    elif event_type == "tick_size_change":
        for tick_change in msg_json:
            if not isinstance(tick_change, dict):
                log(f"events.parse_message found non dict tick_change in message '{message}'", "ERROR")
                return
            
            timestamp = tick_change.get("timestamp", None)
            i_timestamp = parse_timestamp(timestamp)
            if not isinstance(i_timestamp, int):
                log(f"events.parse_message invalid timestamp '{i_timestamp}' found in {msg_json}", "WARNING")

            asset_id = tick_change.get("asset_id", None)
            if not isinstance(asset_id, str):
                log(f"events.parse_message invalid asset_id '{asset_id}' found in {msg_json}", "WARNING")
                return
                
            insert_batch.append([i_timestamp, reader_task, resub_count, asset_id, json.dumps(tick_change)])
        if __debug__:
            log(f"events.parse_message enqueuing tick_size_change batch of size {len(insert_batch)}", "DEBUG")
        await state.tick_changes_queue.put(insert_batch)

    else:
        log(f"events.parse_message invalid event type {event_type} found in {msg_json}", "WARNING")
        return   
    
async def read_events(wss_cli:websockets.WebSocketClientProtocol, reader_task : int, resub_count:int):
    try:
        while True:
                message = None
                message =  await wss_cli.recv()
                #if __debug__:
                    #log(f"events.read_events raw message: {message}", "DEBUG")
                await parse_message(message, reader_task, resub_count)
    except asyncio.CancelledError:
        if __debug__:
            log(f"events.read_events task cancelled intentionally", "DEBUG")
        return
    except Exception as e:
        log(f"events.read_events caught exception '{e}' with wss_cli '{wss_cli}' and message '{message}'", "ERROR")
        await state.new_event_tokens.put([])
        raise
    finally:
        await wss_cli.close()
    
async def subscriber():
    reader_tasks = []
    wss_cli = None
    book_insert_task = None
    change_insert_task = None
    tick_change_insert_task = None
    new_read_task = None
    try:
        book_insert_task = asyncio.create_task(book_inserter())
        change_insert_task = asyncio.create_task(change_inserter())
        tick_change_insert_task = asyncio.create_task(tick_change_inserter())
        wss_cli = await websockets.connect("wss://ws-subscriptions-clob.polymarket.com/ws/market", max_queue=64, ping_interval=10, ping_timeout=10)
        reader_tasks.append((asyncio.create_task(read_events(wss_cli, 0, 0)), 0))

        while True:
            signal = await state.new_event_tokens.get() # wait for signal
            if len(signal) == 0:
                log(f"subscriber awakened to escalate exception", "INFO")
                raise RuntimeError("signal")
            if signal[0] == -1: # new tokens signal
                all_subs = state.subscriptions
                end = len(reader_tasks)-1
                wss_cli = await websockets.connect("wss://ws-subscriptions-clob.polymarket.com/ws/market", max_queue=128, ping_interval=10, ping_timeout=10)
                await wss_cli.send(json.dumps(all_subs[end]))
                resub_count = reader_tasks[end][1]+1
                new_read_task = asyncio.create_task(read_events(wss_cli, end, resub_count))
                curr_read_task = reader_tasks[end][0]
                curr_read_task.cancel()
                try:
                    await curr_read_task
                except asyncio.CancelledError:
                    if __debug__:
                        log("events.subscriber old read_events task cancelled", "DEBUG")
                reader_tasks[end] = (new_read_task, resub_count)
                end += 1
                while end < len(all_subs):
                    wss_cli = await websockets.connect("wss://ws-subscriptions-clob.polymarket.com/ws/market", max_queue=128, ping_interval=10, ping_timeout=10)
                    await wss_cli.send(json.dumps(all_subs[end]))
                    reader_tasks.append((asyncio.create_task(read_events(wss_cli, end, 1)), 1))
                    end += 1
                state.register_analytics_tokens(state.new_tokens)
                state.new_tokens.clear()
    except Exception as e:
        log(f"events.subscriber caught exception '{e}' with wss_cli '{wss_cli}', reader_tasks '{reader_tasks}', book_insert_task '{book_insert_task}', change_insert_task '{change_insert_task}', tick_change_insert_task '{tick_change_insert_task}' subscriptions '{state.subscriptions}", "ERROR")
        if new_read_task is not None:
            new_read_task.cancel()
        for reader_task in reader_tasks:
            reader_task.cancel()
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
    log("events.book_inserter started", "INFO")
    while True:
        insert_batch = await state.books_queue.get()
        conn = None
        try:
            conn = await state.pg_conn_pool.acquire()
            await conn.executemany("""
                INSERT INTO books (server_time, reader_task, resub_count, asset_id, book)
                VALUES ($1, $2, $3, $4, $5)
            """, insert_batch)
            if __debug__:
                log(f"events.book_inserter successfully inserted batch of size {len(insert_batch)}", "DEBUG")
        except asyncio.CancelledError:
            if __debug__:
                log("events.book_inserter task cancelled intentionally", "DEBUG")
            raise
        except Exception as e:
            log(f"events.book_inserter caught exception '{e}' with insert_batch '{insert_batch}'", "ERROR")
            await state.new_event_tokens.put([])
            raise
        finally:
            state.books_queue.task_done()
            if conn is not None:
                await state.pg_conn_pool.release(conn)

async def change_inserter():
    log("events.change_inserter started", "INFO")
    while True:
        insert_batch = await state.changes_queue.get()
        conn = None
        try:
            conn = await state.pg_conn_pool.acquire()
            await conn.executemany("""
                INSERT INTO changes (server_time, reader_task, resub_count, type, asset_id, change)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, insert_batch)
            if __debug__:
                log(f"events.change_inserter successfully inserted batch of size {len(insert_batch)}", "DEBUG")
        except asyncio.CancelledError:
            if __debug__:
                log("events.change_inserter task cancelled intentionally", "DEBUG")
            raise
        except Exception as e:
            log(f"events.change_inserter caught exception '{e}' with insert_batch '{insert_batch}'", "ERROR")
            await state.new_event_tokens.put([])
            raise
        finally:
            state.changes_queue.task_done()
            if conn is not None:
                await state.pg_conn_pool.release(conn)

async def tick_change_inserter():
    log("events.tick_change_inserter started", "INFO")
    while True:
        insert_batch = await state.tick_changes_queue.get()
        conn = None
        try:
            conn = await state.pg_conn_pool.acquire()
            await conn.executemany("""
                INSERT INTO tick_changes (server_time, reader_task, resub_count, asset_id, tick_change)
                VALUES ($1, $2, $3, $4, $5)
            """, insert_batch)
            if __debug__:
                log(f"events.tick_change_inserter successfully inserted batch of size {len(insert_batch)}", "DEBUG")
        except asyncio.CancelledError:
            if __debug__:
                log("events.tick_change_inserter task cancelled intentionally", "DEBUG")
            raise
        except Exception as e:
            log(f"events.tick_change_inserter caught exception '{e}' with insert_batch '{insert_batch}'", "ERROR")
            await state.new_event_tokens.put([])
            raise
        finally:
            state.tick_changes_queue.task_done()
            if conn is not None:
                await state.pg_conn_pool.release(conn)
