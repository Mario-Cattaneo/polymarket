import asyncio
import aiohttp
import asyncpg
import json
import inspect
import time
from log import log
from typing import Callable

class state:
    https_cli : aiohttp.ClientSession = None
    pg_conn_pool : asyncpg.Pool  = None
    register_events_tokens: Callable[[list[str]], None]
    rate_limit_ns : int = 100 * 1_000_000
    last_request : int = -100 * 1_000_000
    batch_buff : list = []
    new_tokens : list[str] = []
    offset : int = 0

async def create_market_table(reset : bool = True):
    log("markets.create_market_table started", "INFO")
    conn = None
    try:
        conn = await state.pg_conn_pool.acquire()

        if reset:
            await conn.execute("DROP TABLE IF EXISTS markets")
            log("markets.create_market_table dropped table if not exists", "INFO")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS markets (
                row_index SERIAL PRIMARY KEY,
                insert_time BIGINT DEFAULT (extract(epoch FROM now()) * 1000)::BIGINT  NOT NULL, -- ms since unix epoch
                asset_id TEXT, --UTF-8, NULL if not found in server response
                market_id TEXT, -- UTF-8, NULL if not found in server response
                condition_id TEXT, -- UTF-8, NULL if not found in server response
                negrisk_id TEXT, -- UTF-8, NULL if not found in server response (this is normal behavior)
                market jsonb  NOT NULL -- json object
            );
        """)
        log("markets.create_market_table created table if not exists", "INFO")

    except Exception as e:
        log(f"markets.create_market_table failed with '{e}', pg_conn_pool '{state.pg_conn_pool}' and conn '{conn}'", "ERROR")
        raise

    finally:
        if conn is not None:
            await state.pg_conn_pool.release(conn)

async def init(
https_cli: aiohttp.ClientSession = None, 
pg_conn_pool: asyncpg.Pool = None, 
register_events_tokens : Callable[[list[str]], None] = None, # must also coro
reset: bool = True):
    log(f"markets.init started", "INFO")
    if reset:
        state.offset = 0
        state.batch_buff = []
        state.new_tokens = []
        state.rate_limit_ns = 100 * 1_000_000
        state.last_request = -100 * 1_000_000
    if not isinstance(https_cli, aiohttp.ClientSession):
        log(f"markets.init received non aiohttp.ClientSession https_cli '{https_cli}'", "ERROR")
        raise ValueError(f"invalid https_cli argument for markets.init")
    state.https_cli = https_cli
    if not isinstance(pg_conn_pool, asyncpg.Pool):
        log(f"markets.init received non asyncpg.Pool pg_conn_pool '{pg_conn_pool}'", "ERROR")
        raise ValueError(f"invalid pg_conn_pool argument for markets.init")
    state.pg_conn_pool = pg_conn_pool
    if not inspect.iscoroutinefunction(register_events_tokens):
        log(f"markets.init received non coroutine register_events_tokens '{register_events_tokens}'", "ERROR")
        raise ValueError(f"invalid register_events_tokens argument for markets.init")
    if len(inspect.signature(register_events_tokens).parameters) != 1:
        log(f"markets.init received  register_events_tokens that does not take exactly one argument'{register_events_tokens}'", "ERROR")
        raise ValueError("invalid register_events_tokens argument for markets.init")
    state.register_events_tokens = register_events_tokens
    log(f"markets.init waiting for create_market_table", "INFO")
    try:
        await create_market_table(reset=reset)
    except Exception as e:
        log(f"markets.init failed from create_market_table with exception '{e}'", "ERROR")
        raise
    log(f"markets.init finished successfully", "INFO")


async def collector():
    log(f"markets.collector starting at offset {state.offset}", "INFO")
    while True:
        now = time.monotonic_ns()
        should_be = state.last_request + state.rate_limit_ns
        if should_be > now:
            await asyncio.sleep((should_be - now) / 1_000_000_000)
        state.last_request = time.monotonic_ns()
        response = None
        try:
            response = await state.https_cli.get(f"https://gamma-api.polymarket.com/markets?limit=500&offset={state.offset}&closed=false")
            new_markets = await response.json()

            # redirections are handled implicitly by aiohttp
            if response.status == 200:
                if __debug__:
                    log(f"markets.collector got good response at offset {state.offset}", "DEBUG")
            elif response.status == 429:
                state.rate_limit_ns = min(1.5 * state.rate_limit_ns, 2_000_000_000)
                log(f"markets.collector got 429 response status backing off for 15s and increasing rate limit to {state.rate_limit_ns/1_000_000}ms", "WARING")
                await asyncio.sleep(15)
                continue
            else:
                log(f"markets.collector got bad response status {response.status} restarting task with new socket", "ERROR")
                raise RuntimeError(f"bad response status in markets.collector")
        except Exception as e:
            log(f"markets.collector caught exception '{e}' with https_cli '{state.https_cli}' ", "ERROR")
            raise
        finally:
            if response is not None:
                await response.release()

        if not isinstance(new_markets, list):
            log(f"markets.collector at offset {state.offset} received non list json '{json.dumps(new_markets)}'", "WARNING")
            continue

        for index, market_obj in enumerate(new_markets):
            if not isinstance(market_obj, dict):
                log(f"markets.collector at offset {state.offset+index} received non dict market_obj '{json.dumps(market_obj)}'", "WARNING")
                continue

            market_id = market_obj.get("id", None)
            if not isinstance(market_id, str):
                log(f"markets.collector missing id at offset {state.offset+index} market_obj '{json.dumps(market_obj)}'", "WARNING")


            condition_id = market_obj.get("conditionId", None)
            if not isinstance(condition_id, str):
                log(f"markets.collector missing conditionId at offset {state.offset+index} with market_id {market_id} and market_obj '{json.dumps(market_obj)}'", "WARNING")
            
            negrisk_id = market_obj.get("negRiskMarketID", None)

            token_ids = market_obj.get("clobTokenIds", [None, None])
            if not isinstance(token_ids, str):
                log(f"markets.collector missing clobTokenIds at offset {state.offset+index} with market_id {market_id} and market_obj '{json.dumps(market_obj)}'", "WARNING")
            
            else:
                try:
                    token_ids = json.loads(token_ids)            
                    token_ids[0] = str(token_ids[0])
                    token_ids[1] = str(token_ids[1])
                    state.new_tokens.append(token_ids[0])
                    state.new_tokens.append(token_ids[1])
                except Exception as e:
                    log(f"markets.collector failed to parse clobTokenIds at offset {state.offset+index} with exception '{e}', token_ids '{token_ids}', market_id {market_id} and market_obj '{json.dumps(market_obj)}'", "WARNING")
                    token_ids = [None, None]

            row1 = (token_ids[0], market_id, condition_id, negrisk_id, json.dumps(market_obj))
            row2 = (token_ids[1], market_id, condition_id, negrisk_id, json.dumps(market_obj))
            state.batch_buff.append(row1)
            state.batch_buff.append(row2)

        new_markets_count = len(new_markets)

        if new_markets_count < 500 and len(state.batch_buff) > 0:
            try:
                await insert_batch(state.batch_buff)
                log(f"markets.init inserted batch of {len(state.batch_buff)} new market rows into markets table", "INFO")
                state.batch_buff.clear()
            except Exception as e:
                log(f"markets.collector failed to insert_batch with exception '{e}'", "ERROR")
                raise
            await state.register_events_tokens(state.new_tokens)
            state.new_tokens = []

        state.offset += new_markets_count
        
async def insert_batch(batch_buff : list):
    log(f"markets.insert_batch started")
    conn = None
    try:
        conn = await state.pg_conn_pool.acquire()
        await conn.executemany("""
                INSERT INTO markets (asset_id, market_id, condition_id, negrisk_id, market)
                VALUES ($1, $2, $3, $4, $5)
            """, batch_buff)

        if __debug__:
            log(f"markets.insert_batch succesfully inserted batch of size {len(batch_buff)}")
    except Exception as e:
        log(f"markets.insert_batch caught exception '{e}' with pg_conn_pool '{state.pg_conn_pool}', conn '{conn}' and batch_buff '{batch_buff}'", "ERROR")
        raise
    finally:
        if conn is not None:
            await state.pg_conn_pool.release(conn)
