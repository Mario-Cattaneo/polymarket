import asyncio
import asyncpg
import aiohttp
import orjson
import time
import numpy as np
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
                insert_time TIMESTAMP(3) WITH TIME ZONE DEFAULT now(),
                asset_id TEXT,                
                server_time BIGINT,
                resub_count BIGINT,
                ws_books_count BIGINT,
                ws_books jsonb,
                ws_ambiguous_start_count BIGINT,
                ws_ambiguous_end_count BIGINT,
                ws_changes jsonb,
                bid_depth_diff INTEGER,
                ask_depth_diff INTEGER,
                bids_distance REAL,
                asks_distance REAL,
                ws_tick_changes_count BIGINT,
                ws_tick_changes jsonb,
                tick_size_distance REAL,
                local_bids jsonb,
                local_asks jsonb,
                server_bids jsonb,
                server_asks jsonb,
                server_response jsonb
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
    for i in range(len(records)):
        records[i] = dict(records[i])
    return records

def parse_decimal_string(decimal_str: str) -> int:
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
    try:
        return {parse_decimal_string(entry["price"]): parse_decimal_string(entry["size"]) for entry in side}
    except Exception as e:
        log(f"analytics.parse_decimal_side failed with side '{side}' exception '{e}'")
        raise

def apply_changes(bids: dict[int, int], asks: dict[int, int], changes: list[dict]):
    try:
        for change in changes:
            size = parse_decimal_string(change["size"])
            price = parse_decimal_string(change["price"])
            side = change["side"]
            if change["event_type"] == "last_trade_price":
                size = -size
            if side == "BUY":
                bids[price] = bids.get(price, 0) + size
            elif side == "SELL":
                asks[price] = asks.get(price, 0) + size
    except Exception as e:
        log(f"analytics.apply_changes with bids '{bids}',asks '{asks}' and changes '{changes}' failed with exception '{e}'")
        raise

def distance(side1: dict[int, int], side2: dict[int, int], side: str) -> (float, list[tuple[int, int]], list[tuple[int, int]]):
    try:
        if not side1 or not side2:
            return None

        descending = side == "bids"

        sorted_side1_list = sorted(side1.items(), key=lambda x: x[0], reverse=descending)
        sorted_side2_list = sorted(side2.items(), key=lambda x: x[0], reverse=descending)

        s1 = np.array(sorted_side1_list, dtype=np.float64)
        s2 = np.array(sorted_side2_list, dtype=np.float64)

        l = min(len(s1), len(s2))
        s1, s2 = s1[:l], s2[:l]

        weights = 1.0 / (np.arange(l) + 1)

        diff_price = s1[:, 0] - s2[:, 0]
        diff_size = s1[:, 1] - s2[:, 1]
        diff = np.sqrt(diff_price**2 + diff_size**2)

        dist = np.sum(np.square(diff) * weights) / np.sum(weights)
        return (dist, sorted_side1_list, sorted_side2_list)

    except Exception as e:
        log(f"analytics.distance failed for side '{side}' with exception '{e}'", "WARNING")
        raise

def get_ambiguous_changes_count(start_time:int, end_time:int, changes:list)->(int, int):
    ambiguous_start_count = None
    ambiguous_end_count = None
    try:
        change_count = len(changes)
        if change_count == 0:
            return (None, None)
        ambiguous_start_count = 0
        ambiguous_end_count = change_count - 1
        while ambiguous_start_count < change_count and changes[ambiguous_start_count]["server_time"] == start_time:
            ambiguous_start_count += 1
        
        while ambiguous_end_count >= 0 and changes[ambiguous_end_count]["end_time"] == end_time:
            ambiguous_end_count -= 1

        ambiguous_end_count = change_count - (1 + ambiguous_end_count)
    except Exception as e:
        log(f"analytics.get_ambiguous_count caught exception '{e}' for start_time '{start_time}', end_time '{end_time}' and changes '{changes}'", "WARNING")

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
                asset_id, server_time, resub_count, ws_books_count, ws_books, 
                ws_ambiguous_start_count, ws_ambiguous_end_count,
                ws_changes, bid_depth_diff, ask_depth_diff, bids_distance, asks_distance,
                ws_tick_changes_count, ws_tick_changes, tick_size_distance,
                local_bids, local_asks, server_bids, server_asks, server_response
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
            )
        """, analytics.batch_buf)
        if __debug__:
            log(f"analytics.insert_row successfully inserted {len(analytics.batch_buf)} rows", "INFO")
    except Exception as e:
        log(f"analytics.insert_row caught exception '{e}' for rows '{analytics.batch_buf}'", "ERROR")
        raise
    finally:
        if conn is not None:
            await analytics.pg_conn_pool.release(conn)

async def query_ws_data(asset_id: str, timestamp: int) -> (list, list, list, int, int):
    conn = None
    max_resub_count = None
    f_new_tick_size = None
    tick_changes = []
    tick_changes_count = None
    books = []
    books_count = None
    changes = []
    ambiguous_start_count = None
    ambiguous_end_count = None
    try:
        conn = await analytics.pg_conn_pool.acquire()
        tick_changes = await conn.fetch("""
            SELECT *
            FROM tick_changes
            WHERE asset_id = $1 AND server_time <= $2
            ORDER BY server_time DESC, resub_count DESC
            LIMIT 25;
        """, asset_id, timestamp)

        if tick_changes:
            top_tick_change = tick_changes[0]
            max_time = top_tick_change["server_time"]
            max_resub_count = top_tick_change["resub_count"]
            tick_changes = [
                t for t in tick_changes 
                if t["server_time"] == max_time and t["resub_count"] == max_resub_count]
            tick_changes_count = len(tick_changes)
            f_new_tick_size = float(top_tick_change["tick"]["new_tick_size"])
    

        books = await conn.fetch("""
            SELECT *
            FROM books
            WHERE asset_id = $1 AND server_time <= $2
            ORDER BY server_time DESC, resub_count DESC
            LIMIT 25;
        """, asset_id, timestamp)

        if not books:
            return (max_resub_count, books_count, books, ambiguous_start_count, ambiguous_end_count, changes, tick_changes_count, tick_changes, f_new_tick_size)
        
        top_book = books[0]
        max_time = top_book["server_time"]
        max_resub_count = top_book["resub_count"]
        books = [
            t for t in books 
            if t["server_time"] == max_time and t["resub_count"] == max_resub_count]
        books_count = len(books)

        changes = await conn.fetch("""
            SELECT row_index, insert_time, server_time, resub_count, type, change
            FROM changes
            WHERE asset_id = $1
              AND server_time BETWEEN $2 AND $3
              AND resub_count = $4
            ORDER BY server_time ASC, type ASC;
        """, asset_id, max_time, timestamp, max_resub_count)

        (ambiguous_start_count, ambiguous_end_count) = get_ambiguous_changes_count(max_time, timestamp, changes)
    except Exception as e:
        log(f"analytics.query_ws_data caught exception '{e}' with asset_id '{asset_id}' and server_time '{timestamp}'", "ERROR")
        raise
    finally:
        if conn is not None:
            await analytics.pg_conn_pool.release(conn)

    return (max_resub_count, books_count, books, ambiguous_start_count, ambiguous_end_count, changes, tick_changes_count, tick_changes, f_new_tick_size)

async def per_token_pipeline(server_book: dict):
    asset_id = None
    i_timestamp = None
    resub_count = None
    ws_books_count = None
    ws_books = []
    ws_ambiguous_start_count = None
    ws_ambiguous_end_count = None
    ws_changes = []
    ws_tick_changes_count = None
    ws_tick_changes = []
    f_server_tick_size = None
    f_new_tick_size = None
    tick_size_distance = None
    local_asks = []
    local_bids = []
    server_asks = []
    server_bids = []
    bid_depth_diff = None
    ask_depth_diff = None
    bids_distance = None
    asks_distance = None

    asset_id = server_book.get("asset_id", None)
    if not isinstance(asset_id, str):
        log(f"analytics.per_token_pipeline received invalid asset_id '{asset_id}' from server_book '{server_book}'", "WARNING")
        return
    try:
        i_timestamp = int(server_book["timestamp"])
    except Exception as e:
        log(f"analytics.per_token_pipeline received invalid timestamp '{server_book.get('timestamp')}' from server_book '{server_book}' with exception '{e}'", "WARNING")
        return
    try:
        f_server_tick_size = float(server_book["tick_size"])
    except Exception as e:
        log(f"analytics.per_token_pipeline received invalid tick_size '{server_book.get('tick_size')}' from server_book '{server_book}' with exception '{e}'", "WARNING")
        f_server_tick_size = None

    server_bids = server_book.get("bids", None)
    if not isinstance(server_bids, list):
        log(f"analytics.per_token_pipeline received invalid bids '{server_book.get('bids')}' from server_book '{server_book}'", "WARNING")
        server_bids = None

    server_asks = server_book.get("asks", None)
    if not isinstance(server_asks, list):
        log(f"analytics.per_token_pipeline received invalid asks '{server_book.get('asks')}' from server_book '{server_book}'", "WARNING")
        server_asks = None
 
    try:
        (
            resub_count,
            ws_books_count,
            ws_books,
            ws_ambiguous_start_count,
            ws_ambiguous_end_count,
            ws_changes,
            ws_tick_changes_count,
            ws_tick_changes,
            f_new_tick_size,
        ) = await query_ws_data(asset_id, i_timestamp)
    except Exception as e:
        log(f"analytics.per_token_pipeline failed to query_ws_data for asset_id '{asset_id}' and timestamp '{i_timestamp}' with exception '{e}'", "ERROR")
        raise
    
    try:
        if f_new_tick_size is not None and f_server_tick_size is not None:
            tick_size_distance = abs(f_new_tick_size - f_server_tick_size)
    except Exception as e:
        log(f"""analytics.per_token_pipeline failed to compute tick metrics:\n
            exception: {e}\n
            asset_id: {asset_id}\n
            ws_tick_changes_count: {ws_tick_changes_count}\n
            f_new_tick_size: {f_new_tick_size}\n
            f_server_tick_size: {f_server_tick_size}\n
            tick_size_distance: {tick_size_distance}\n
            ws_tick_changes: {ws_tick_changes}""", "WARNING")

    top_book = None
    if ws_books:
        try:
            top_book = orjson.loads(ws_books[0]["book"])
            local_bids = top_book["bids"]
            local_asks = top_book["asks"]

            local_bids = parse_decimal_side(local_bids)
            local_asks = parse_decimal_side(local_asks)
            server_bids = parse_decimal_side(server_bids)
            server_asks = parse_decimal_side(server_asks)

            apply_changes(local_bids, local_asks, ws_changes)

            bid_depth_diff = abs(len(local_bids) - len(server_bids))
            ask_depth_diff = abs(len(local_asks) - len(server_asks))
            bids_distance, local_bids, server_bids = distance(local_bids, server_bids, "bids")
            asks_distance, local_asks, server_asks = distance(local_asks, server_asks, "asks")

        except Exception as e:
            log(f"""analytics.per_token_pipeline failed to compute book metrics:\n
                exception: {e}\n
                asset_id: {server_book.get('asset_id')}\n
                ws_books_count: {ws_books_count}\n
                bid_depth_diff: {bid_depth_diff}\n
                ask_depth_diff: {ask_depth_diff}\n
                bids_distance: {bids_distance}\n
                asks_distance: {asks_distance}\n
                top_book: {top_book}\n
                server_bids: {server_bids}\n
                server_asks: {server_asks}\n
                local_bids: {local_bids}\n
                local_asks: {local_asks}""", "WARNING")

            if not isinstance(local_bids, list):
                local_bids = []
            if not isinstance(local_asks, list):
                local_asks = []
            if not isinstance(server_bids, list):
                server_bids = []
            if not isinstance(server_asks, list):
                server_asks = []


    else:
        remove_unresponsive_token(asset_id)
    try:
        ws_books_json = orjson.dumps(records_to_maps(ws_books)).decode("utf-8") if ws_books else None
        ws_changes_json = orjson.dumps(records_to_maps(ws_changes)).decode("utf-8") if ws_changes else None
        ws_tick_changes_json = orjson.dumps(records_to_maps(ws_tick_changes)) if ws_tick_changes else None
        local_bids_json = orjson.dumps(local_bids).decode("utf-8") if local_bids else None
        local_asks_json = orjson.dumps(local_asks).decode("utf-8") if local_asks else None
        server_bids_json = orjson.dumps(server_bids).decode("utf-8") if server_bids else None
        server_asks_json = orjson.dumps(server_asks).decode("utf-8") if server_asks else None
        server_book_json = orjson.dumps(server_book).decode("utf-8") if server_book else None
    except Exception as e:
        log(f"""analytics.per_token_pipeline failed to serialize json fields with exception '{e}':\n
            ws_books: {ws_books}\n
            ws_changes: {ws_changes}\n
            ws_tick_changes: {ws_tick_changes}\n
            local_bids: {local_bids}\n
            local_asks: {local_asks}\n
            server_bids: {server_bids}\n
            server_asks: {server_asks}\n
            server_book: {server_book}
            """, "ERROR")
        raise

    row = [
        asset_id,
        i_timestamp,
        resub_count,
        ws_books_count,
        ws_books_json,
        ws_ambiguous_start_count,
        ws_ambiguous_end_count,
        ws_changes_json,
        bid_depth_diff,
        ask_depth_diff,
        bids_distance,
        asks_distance,
        ws_tick_changes_count,
        ws_tick_changes_json,
        tick_size_distance,
        local_bids_json,
        local_asks_json,
        server_bids_json,
        server_asks_json,
        server_book_json,
    ]
    analytics.batch_buf.append(row)

    if __debug__:
        log(f"""analytics.per_token_pipeline appended new row:\n
            asset_id: {asset_id}\n
            timestamp: {i_timestamp}\n
            resub_count: {resub_count}\n
            ws_books_count: {ws_books_count}\n
            ws_books: {ws_books}\n
            ws_changes: {ws_changes}\n
            server_bids: {server_bids}\n
            server_asks: {server_asks}\n
            local_bids: {local_bids}\n
            local_asks: {local_asks}\n
            ws_ambiguous_start_count: {ws_ambiguous_start_count}\n
            ws_ambiguous_end_count: {ws_ambiguous_end_count}\n
            bid_depth_diff: {bid_depth_diff}\n
            ask_depth_diff: {ask_depth_diff}\n
            bids_distance: {bids_distance}\n
            asks_distance: {asks_distance}\n
            ws_tick_changes_count: {ws_tick_changes_count}\n
            ws_tick_changes: {ws_tick_changes}\n
            f_new_tick_size: {f_new_tick_size}\n
            f_server_tick_size: {f_server_tick_size}\n
            tick_size_distance: {tick_size_distance}\n
            top_book: {top_book}\n
            server_book: {server_book}\n
            buffer_size: {len(analytics.batch_buf)}
            """, "DEBUG")

async def pipeline():
    log(f"analytics.do_analytics started", "INFO")
    last_request = -analytics.rate_limit_ns
    while True:
        await asyncio.sleep(5)
        batch_count = len(analytics.post_bodies)
        if __debug__:
            log(f"analytics.pipeline new iteration started with {batch_count} batches")
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
                log(f"analytics.do_analytics returned non list '{json_response}'", "WARNING")
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
            analytics.batch_buf.clear()


        
    
