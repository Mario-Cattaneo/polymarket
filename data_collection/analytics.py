import asyncio
import asyncpg
import aiohttp
import orjson
import time
import numpy as np
from typing import List, Tuple, Dict, Any, Optional
from log import log

class analytics:
    https_cli : aiohttp.ClientSession = None
    pg_conn_pool : asyncpg.Pool = None
    rate_limit_ns : int = 125 * 1_000_000
    url : str = "https://clob.polymarket.com/books"
    post_body_offset : int = 0
    post_bodies : list[list[dict[str, str]]] = [[]]
    batch_buf : list[list] = []
    last_register_time : int = -2

class rate_limit_exception(Exception):
    pass

def register_tokens(new_tokens : list[str]):
    try:
        end = len(analytics.post_bodies) - 1
        body = analytics.post_bodies[end]
        for token in new_tokens:
            if len(body) == 500:
                analytics.post_bodies.append([])
                end += 1
                body = analytics.post_bodies[end]
            body.append({"token_id": token})
        analytics.last_register_time = time.monotonic_ns()
        if __debug__:
            log(f"analytics.register_tokens registered {len(new_tokens)} tokens", "DEBUG")
    except Exception as e:
        log(f"analytics.register_tokens failed with exception '{e}' with new_tokens '{new_tokens}", "ERROR")
        raise

def remove_unresponsive_token(token: str):
    try:
        for body in analytics.post_bodies:
            for i, entry in reversed(list(enumerate(body))):
                if body[i]["token_id"] == token:
                    del body[i]
                    if __debug__:
                        log(f"analytics.remove_unresponsive_token removed token '{token}' from post bodies", "DEBUG")
                    return
    except Exception as e:
        log(f"analytics.remove_unresponsive_token failed with exception '{e}' for token '{token}'", "ERROR")
        raise

async def create_analytics_table(reset : bool = True):
    log("analytics.create_analytics_table started", "INFO")
    try:
        conn = await analytics.pg_conn_pool.acquire()
        if reset:
            await conn.execute("DROP TABLE IF EXISTS analytics")
            log("analytics.create_analytics_table dropped table if exists", "INFO")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS analytics (
                row_index SERIAL PRIMARY KEY,
                insert_time BIGINT DEFAULT (extract(epoch FROM now()) * 1000)::BIGINT NOT NULL, -- ms since unix epoch
                asset_id TEXT NOT NULL, -- UTF-8          
                server_time BIGINT NOT NULL, -- ms since unix epoch,
                ws_books_count BIGINT NOT NULL,
                ws_books jsonb NOT NULL, -- json array of json objects
                ws_book_resub_count BIGINT, -- only NULL if ws_books is empty
                ws_book_time BIGINT, -- only NULL if not ws_books is empty
                ws_ambiguous_start_count BIGINT, -- only NULL if ws_books is empty
                ws_ambiguous_end_count BIGINT, -- only NULL if ws_books is empty
                ws_changes_count BIGINT, -- only NULL if ws_books is empty,
                ws_changes jsonb, -- only NULL if ws_books is empty otherwise json array of json objects
                bid_depth_diff BIGINT, -- only NULL if local_bids is NULL or server_bids is NULL
                ask_depth_diff BIGINT, -- only NULL if local_asks is NULL or server_asks is NULL
                bids_distance REAL, -- only NULL if local_bids is NULL or server_bids is NULL
                asks_distance REAL, -- only NULL if local_asks is NULL or server_asks is NULL
                local_bids jsonb, -- only NULL if no bids are found in ws_books, otherwise sorted json array
                local_asks jsonb, -- only NULL if no asks are found in ws_books, otherwise sorted json array
                server_bids jsonb, -- only NULL if not found in server response, othwerwise sorted json array
                server_asks jsonb, -- only NULL if not found in server response, othwerwise sorted json array
                ws_tick_changes_count BIGINT NOT NULL,
                ws_tick_changes jsonb NOT NULL, -- json array of json objects
                ws_tick_change_resub_count BIGINT, -- only NULL if ws_tick_changes is empty
                ws_tick_change_time BIGINT, -- only NULL if ws_tick_changes is empty
                local_tick_size REAL, -- NULL if not found in ws_tick_changes
                server_tick_size REAL, -- NULL if not found in server_response
                tick_size_distance REAL, -- NULL if local or server tick_size are NULL
                server_response jsonb NOT NULL
            );
        """)
        log("analytics.create_analytics_table created table if not exists", "INFO")
    except Exception as e:
        log(f"analytics.create_analytics_table failed with exception '{e}'", "ERROR")
        raise RuntimeError("analytics.create_analytics_table failed") from e
    finally:
        if conn is not None:
            await analytics.pg_conn_pool.release(conn)

async def init(https_cli : aiohttp.ClientSession = None, pg_conn_pool : asyncpg.Pool = None, reset : bool = True):
    log(f"analytics.init started", "INFO")
    if not isinstance(https_cli, aiohttp.ClientSession):
        log(f"analytics.init received non aiohttp.ClientSession https_cli '{https_cli}'", "ERROR")
        raise ValueError(f"analytics.init received invalid https_cli")
    analytics.https_cli = https_cli
    if not isinstance(pg_conn_pool, asyncpg.Pool):
        log(f"analytics.init received non asyncpg.Pool pg_conn_pool '{pg_conn_pool}'", "ERROR")
        raise ValueError(f"analytics.init received invalid pg_conn_pool")
    analytics.pg_conn_pool = pg_conn_pool
    log(f"analytics.init waiting for event.create_analytics_table", "INFO")
    try: 
        await create_analytics_table(reset=reset)
    except Exception as e:
        log(f"analytics.init failed to create analytics table with exception '{e}'", "ERROR")
        raise
    log(f"analytics.init finished", "INFO")

def records_to_maps(records: list[asyncpg.Record]) -> list[dict]:
    """
    recrods must be list and consist of asyncpg.Record entries
    """
    for i in range(len(records)):
        records[i] = dict(records[i])
    return records

def parse_decimal_string(decimal_str: str) -> int:
    """
    decimal_str can not be None and must be str
    """
    try: 
        if "." not in decimal_str:
            return int(decimal_str) * 1_000_000
        whole, frac = decimal_str.split(".")
        if len(frac) > 6:
            log(f"analytics.parse_decimal_string received decimal '{decimal_str}' with more than 6 trailing significant digits", "WARNING")
            raise ValueError("analytics.parse_decimal_string received invalid decimal_str argument")
        frac = frac.ljust(6, "0")
        return int(whole) * 1_000_000 + int(frac)
    except Exception as e:
        log(f"analytics.parse_decimal_string received invalid decimal string '{decimal_str}'", "WARNING")
        raise

def parse_decimal_side(side: list[dict[str, str]]) -> dict[int, int]:
    if side is None:
        return None
    try:
        return {parse_decimal_string(entry["price"]): parse_decimal_string(entry["size"]) for entry in side}
    except Exception as e:
        log(f"analytics.parse_decimal_side failed with side '{side}' exception '{e}'")
        raise

def parse_changes(changes : list[asyncpg.Record]) -> Tuple[list[str, int, int], list[str, int, int]]:
    bid_changes  = []
    ask_changes = []
    for record in changes:
        change_str = record["change"] # change is not None and json object by events.py
        event_type = record["type"]
        change = orjson.loads(change_str) 
        side = change.get("side", None)
        if not isinstance(side, str):
            log(f"analytics.parse_changes invalid or missing side in '{change_str}'", "WARNING")
            continue
        price = change.get("price", None)
        if not isinstance(price, str):
            log(f"analytics.parse_changes invalid or missing price in '{change_str}'", "WARNING")
            continue
        size = change.get("size", None)
        if not isinstance(size, str):
            log(f"analytics.parse_changes invalid or missing size in '{change_str}'", "WARNING")
            continue
        try:
            price = parse_decimal_string(price)
            size = parse_decimal_string(size)
        except Exception as e:
            log(f"analytics.parse_changes parse decimal failed for '{change_str}'", "WARNING")
            continue
        if side == "BUY":
            bid_changes.append((price, size))
        elif side == "SELL":
            ask_changes.append((price, size))
        else:
            log(f"analytics.parse_changes found unexpected string '{change_str}'", "WARNING")

    return (bid_changes, ask_changes)

def apply_changes(bids: dict[int, int], asks: dict[int, int], bid_changes: list[int, int], ask_changes: list[int, int]):
    """
    bids and asks can be none or dict, bid_changes and ask_changes must be dict
    """
    if bids:
        for (price, size) in bid_changes:
            bids[price] = size
    if asks:
        for (price, size) in ask_changes:
            asks[price] = size

def distance(side1: list[tuple[int, int]], side2: list[tuple[int, int]]) -> float:
    """
    both side must be either sorted lists or None
    """
    if not (side1 and side2):
        return None

    s1 = np.array(side1, dtype=np.float64)
    s2 = np.array(side2, dtype=np.float64)

    l = min(len(s1), len(s2))
    s1, s2 = s1[:l], s2[:l]

    weights = 1.0 / (np.arange(l) + 1)

    diff_price = s1[:, 0] - s2[:, 0]
    diff_size = s1[:, 1] - s2[:, 1]
    diff = np.sqrt(diff_price**2 + diff_size**2)

    dist = np.sum(np.square(diff) * weights) / np.sum(weights)
    return dist

def get_ambiguous_changes_count(start_time:int, end_time:int, changes:list)->Tuple[int, int]:
    """
    end_time, changes and start_time can be None and must be of correct type
    """
    change_count = len(changes)
    ambiguous_start_count = 0
    while ambiguous_start_count < change_count and changes[ambiguous_start_count]["server_time"] == start_time:
        ambiguous_start_count += 1
    
    ambiguous_end_count = change_count - 1
    while ambiguous_end_count >= 0 and changes[ambiguous_end_count]["server_time"] == end_time:
        ambiguous_end_count -= 1

    ambiguous_end_count = change_count - (1 + ambiguous_end_count)
    return (ambiguous_start_count, ambiguous_end_count)
    
async def fetch_batch(offset: int):
    try:
        response = await analytics.https_cli.post(analytics.url, json=analytics.post_bodies[offset])
        if response.status == 200:
            data = await response.json()
            return data
        elif response.status == 429:
            analytics.rate_limit_ns = min(1.5*analytics.rate_limit_ns, 2_000_000_000)
            log(f"analytics.fetch_batch hit rate limit from request body '{analytics.post_bodies[offset]}' backing off for 15s and setting rate limit to {analytics.rate_limit_ns / 1_000_000}ms", "ERROR")
            await asyncio.sleep(15)
            raise rate_limit_exception("rate limit hit in analytics.fetch_batch")
        else:
            log(f"analytics.fetch_batch got bad response status '{response.status}' from request body '{analytics.post_bodies[offset]}'", "ERROR")
            raise RuntimeError("bad response status in analytics.fetch_batch")
    except Exception as e:
        log(f"analytics.fetch_batch failed for offset {offset} with '{e}'", "ERROR")
        raise 
    finally:
        if 'response' in locals():
            await response.release()

async def insert_batch():
    conn = None
    try:
        conn = await analytics.pg_conn_pool.acquire()
        await conn.executemany("""
            INSERT INTO analytics (
                asset_id,
                server_time,
                ws_books_count,
                ws_books,
                ws_book_resub_count,
                ws_book_time,
                ws_ambiguous_start_count,
                ws_ambiguous_end_count,
                ws_changes_count,
                ws_changes,
                bid_depth_diff,
                ask_depth_diff,
                bids_distance,
                asks_distance,
                local_bids,
                local_asks,
                server_bids,
                server_asks,
                ws_tick_changes_count,
                ws_tick_changes,
                ws_tick_change_resub_count,
                ws_tick_change_time,
                local_tick_size,
                server_tick_size,
                tick_size_distance,
                server_response
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18,
                $19, $20, $21, $22, $23, $24, $25, $26
            );
        """, analytics.batch_buf)

        if __debug__:
            log(f"analytics.insert_batch successfully inserted {len(analytics.batch_buf)} rows", "DEBUG")
    except Exception as e:
        log(f"analytics.insert_batch caught exception '{e}' for rows '{analytics.batch_buf}'", "ERROR")
        raise
    finally:
        if conn is not None:
            await analytics.pg_conn_pool.release(conn)

async def query_ws_data(asset_id: str, server_time: int) -> Tuple[list, int, int, int, int, list, dict, dict, list, int, int, float]:
    """
    asset_id id and server_time must be not None and correct type
    """ 
    conn = None
    books = []
    book_resub_count = None
    book_time = None
    ambiguous_start_count = None
    ambiguous_end_count = None
    changes = []
    local_bids = None
    local_asks = None
    tick_changes = []
    tick_change_resub_count = None
    tick_change_time = None
    local_tick_size = None
    try:
        conn = await analytics.pg_conn_pool.acquire()
        tick_changes = await conn.fetch("""
            SELECT *
            FROM tick_changes
            WHERE asset_id = $1 AND server_time <= $2
            ORDER BY server_time DESC, resub_count DESC
            LIMIT 25;
        """, asset_id, server_time)
        if tick_changes:
            top_tick_change = tick_changes[0]
            tick_change = orjson.loads(top_tick_change["tick_change"])
            #log(f"analytics.query_ws_data tick_change found has type '{type(tick_change)}' and is '{tick_change}'", "DEBUG")
            local_tick_size = tick_change.get("new_tick_size", None) # 'tick_change' is not None and dict by events.py
            tick_change_time = top_tick_change["server_time"] # must exist since query can't return non null by <= comparison in postgre
            tick_change_resub_count = top_tick_change["resub_count"] # must exist and be not None bigint by events.py
            tick_changes = [
                t for t in tick_changes 
                if t["server_time"] == tick_change_time and t["resub_count"] == tick_change_resub_count]

        books = await conn.fetch("""
            SELECT *
            FROM books
            WHERE asset_id = $1 AND server_time <= $2
            ORDER BY server_time DESC, resub_count DESC
            LIMIT 25;
        """, asset_id, server_time)

        if books:
            top_book = books[0]
            book_time = top_book["server_time"] # must exist for same reason as above
            local_book = orjson.loads(top_book["book"]) # must be not None and be json dict by events.py
            #log(f"analytics.query_ws_data local_book found has type '{type(local_book)}' and is '{local_book}'", "DEBUG")
            local_bids = local_book.get("bids", None)
            local_asks = local_book.get("asks", None)
            book_resub_count = top_book["resub_count"]  # must exist and be not None bigint by events.py
            books = [
                t for t in books 
                if t["server_time"] == book_time and t["resub_count"] == book_resub_count]

            changes = await conn.fetch("""
                SELECT row_index, insert_time, server_time, resub_count, type, change
                FROM changes
                WHERE asset_id = $1
                AND server_time BETWEEN $2 AND $3
                AND resub_count = $4
                ORDER BY server_time ASC, type ASC;
            """, asset_id, book_time, server_time, book_resub_count)
            (ambiguous_start_count, ambiguous_end_count) = get_ambiguous_changes_count(book_time, server_time, changes)
    except Exception as e:
        log(f"analytics.query_ws_data caught exception '{e}' with asset_id '{asset_id}' and server_time '{server_time}'", "ERROR")
        raise
    finally:
        if conn is not None:
            await analytics.pg_conn_pool.release(conn)

    return (books,
            book_resub_count,
            book_time,
            ambiguous_start_count,
            ambiguous_end_count,
            changes,
            local_bids,
            local_asks,
            tick_changes,
            tick_change_resub_count,
            tick_change_time,
            local_tick_size)

async def per_token_pipeline(server_book: dict):
    """
    server_book can not be None and must be correct type
    """
    asset_id = server_book.get("asset_id", None)
    if not isinstance(asset_id, str):
        log(f"analytics.per_token_pipeline received invalid asset_id '{asset_id}' from server_book '{server_book}'", "WARNING")
        return
    try:
        server_time = int(server_book["timestamp"])
    except Exception as e:
        log(f"analytics.per_token_pipeline received invalid timestamp from server_book '{server_book}' with exception '{e}'", "WARNING")
        return
    try:
        server_tick_size = float(server_book["tick_size"])
    except Exception as e:
        log(f"analytics.per_token_pipeline received invalid tick_size from server_book '{server_book}' with exception '{e}'", "WARNING")
        server_tick_size = None

    server_bids = server_book.get("bids", None)
    if not isinstance(server_bids, list):
        log(f"analytics.per_token_pipeline received invalid bids from server_book '{server_book}'", "WARNING")
        server_bids = None

    server_asks = server_book.get("asks", None)
    if not isinstance(server_asks, list):
        log(f"analytics.per_token_pipeline received invalid asks from server_book '{server_book}'", "WARNING")
        server_asks = None
    try:
        (
            ws_books,
            ws_book_resub_count,
            ws_book_time,
            ws_ambiguous_start_count,
            ws_ambiguous_end_count,
            ws_changes,
            local_bids,
            local_asks,
            ws_tick_changes,
            ws_tick_change_resub_count,
            ws_tick_change_time,
            local_tick_size
        ) = await query_ws_data(asset_id, server_time)
    except Exception as e:
        log(f"analytics.per_token_pipeline failed to query_ws_data for asset_id '{asset_id}' and server_time '{server_time}' with exception '{e}'", "ERROR")
        raise
    
    if local_tick_size is not None:
        try:
            local_tick_size = float(local_tick_size)
        except Exception as e:
            log(f"analytics.per_token_pipeline found invalid local_tick_size in '{ws_tick_changes}' with exception '{e}'", "WARNING")
            local_tick_size = None


    
    tick_size_distance = None
    if local_tick_size is not None and server_tick_size is not None:
        tick_size_distance = abs(local_tick_size - server_tick_size)

    try: 
        local_bids = parse_decimal_side(local_bids)
    except Exception as e:
        log(f"analytics.per_token_pipeline failed to parse_decimal for local_bids", "WARNING")
        local_bids = None
    try:
        local_asks = parse_decimal_side(local_asks)
    except Exception as e:
        log(f"analytics.per_token_pipeline failed to parse_decimal for local_asks", "WARNING")
        local_asks = None
    try:
        server_bids = parse_decimal_side(server_bids)
    except Exception as e:
        log(f"analytics.per_token_pipeline failed to parse_decimal for server_bids", "WARNING")
        server_bids = None
    try:
        server_asks = parse_decimal_side(server_asks)
    except Exception as e:
        log(f"analytics.per_token_pipeline failed to parse_decimal for server_asks", "WARNING")
        server_asks = None
    
    (bid_changes, ask_changes) = parse_changes(ws_changes)
    #log(f"analytics.per_token_pipeline bid_changes '{bid_changes}' and ask_changes '{ask_changes}'", "DEBUG")

    #try:
    apply_changes(local_bids, local_asks, bid_changes, ask_changes)
    #except Exception as e:
        #log(f"analytics.per_token_pipeline apply_changes exception '{e}'", "DEBUG")


    if local_bids:
        local_bids = sorted(local_bids.items(), key=lambda x: x[0], reverse=True)
    
    if local_asks:
        local_asks = sorted(local_asks.items(), key=lambda x: x[0], reverse=False)

    if server_bids:
        server_bids = sorted(server_bids.items(), key=lambda x: x[0], reverse=True)

    if server_asks:
        server_asks = sorted(server_asks.items(), key=lambda x: x[0], reverse=False)

    bid_depth_diff = None
    if local_bids and server_bids:
        bid_depth_diff = abs(len(local_bids) - len(server_bids))
    
    ask_depth_diff = None
    if local_asks and server_asks:
        ask_depth_diff = abs(len(local_asks) - len(server_asks))

    bids_distance = distance(local_bids, server_bids)
    asks_distance = distance(local_asks, server_asks)

    records_to_maps(ws_books)
    records_to_maps(ws_changes)
    records_to_maps(ws_tick_changes)

    row = (
        asset_id,
        server_time,
        len(ws_books),
        orjson.dumps(ws_books).decode("utf-8"),
        ws_book_resub_count,
        ws_book_time,
        ws_ambiguous_start_count,
        ws_ambiguous_end_count,
        len(ws_changes),
        orjson.dumps(ws_changes).decode("utf-8"),
        bid_depth_diff,
        ask_depth_diff,
        bids_distance,
        asks_distance,
        orjson.dumps(local_bids).decode("utf-8") if local_bids else None,
        orjson.dumps(local_asks).decode("utf-8") if local_asks else None,
        orjson.dumps(server_bids).decode("utf-8") if server_bids else None,
        orjson.dumps(server_asks).decode("utf-8") if server_asks else None,
        len(ws_tick_changes),
        orjson.dumps(ws_tick_changes).decode("utf-8"),
        ws_tick_change_resub_count,
        ws_tick_change_time,
        local_tick_size,
        server_tick_size,
        tick_size_distance,
        orjson.dumps(server_book).decode("utf-8")
    )
    analytics.batch_buf.append(row)

    if __debug__:
        log(f"""
            analytics.per_token_pipeline appended new row:\n
            asset_id: {asset_id}\n
            server_time: {server_time}\n
            ws_books_count: {len(ws_books)}\n
            ws_books: {ws_books}\n
            ws_book_resub_count: {ws_book_resub_count}\n
            ws_book_time: {ws_book_time}\n
            ws_ambiguous_start_count: {ws_ambiguous_start_count}\n
            ws_ambiguous_end_count: {ws_ambiguous_end_count}\n
            ws_changes_count: {len(ws_changes)}\n
            ws_changes: {ws_changes}\n
            bid_depth_diff: {bid_depth_diff}\n
            ask_depth_diff: {ask_depth_diff}\n
            bids_distance: {bids_distance}\n
            asks_distance: {asks_distance}\n
            local_bids: {local_bids}\n
            local_asks: {local_asks}\n
            server_bids: {server_bids}\n
            server_asks: {server_asks}\n
            ws_tick_changes_count: {len(ws_tick_changes)}\n
            ws_tick_changes: {ws_tick_changes}\n
            ws_tick_change_resub_count: {ws_tick_change_resub_count}\n
            ws_tick_change_time: {ws_tick_change_time}\n
            local_tick_size: {local_tick_size}\n
            server_tick_size: {server_tick_size}\n
            tick_size_distance: {tick_size_distance}\n
            server_response: {server_book}\n
            buffer_size: {len(analytics.batch_buf)}
        """, "DEBUG")

async def pipeline():
    log(f"analytics.do_analytics started", "INFO")
    last_request = -analytics.rate_limit_ns
    while True:
        await asyncio.sleep(5)
        batch_count = len(analytics.post_bodies)
        if __debug__:
            log(f"analytics.pipeline new iteration started with {batch_count} batches", "DEBUG")
        for batch_offset in range(batch_count):
            now = time.monotonic_ns()
            should_be = last_request + analytics.rate_limit_ns
            if should_be > now:
                await asyncio.sleep((should_be - now) / 1_000_000_000)
            now = time.monotonic_ns()
            should_be = analytics.last_register_time + 2_000_000_000
            if should_be > now:
                await asyncio.sleep((should_be - now) / 1_000_000_000)
            last_request = time.monotonic_ns()

            if __debug__:
                log(f"analytics.pipeline handling batch {batch_offset} out of {batch_count}")
            try:
                json_response = await fetch_batch(batch_offset) 
            except rate_limit_exception:
                log(f"analytcis.pipeline returning from rate limit back off")
                continue
            except Exception as e:
                log(f"analytcis.pipeline caught exception '{e}' from fetching batch")
                raise
            if not isinstance(json_response, list):
                log(f"analytics.pipeline returned non list '{json_response}'", "WARNING")
                continue

            for book in json_response:
                try:
                    await per_token_pipeline(book)
                except Exception as e:
                    log(f"analytics.pipeline caught exception '{e}' for {book}'", "ERROR")
                    raise
            try:
                await insert_batch()
            except Exception as e:
                log(f"analytics.pipeline failed to insert_batch with exception '{e}'", "ERROR")
                raise
            finally:
                analytics.batch_buf.clear()

        
    
