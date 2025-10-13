import asyncio
import inspect
import aiohttp
import asyncpg
import os
from log import log
import markets
import events
import analytics

async def dispatch_task(
name: str = None,
restart_delay: int = 5,
persistent_task=None,
init_routine=None,
*init_args
):
    if not isinstance(name, str):
        log(f"dispatcher.dispatch_task received invalid name {name}", "ERROR")
        return

    if not inspect.iscoroutinefunction(persistent_task):
        log(f"dispatcher.dispatch_task received invalid persistent_task {persistent_task}", "ERROR")
        return

    if not callable(init_routine):
        log(f"dispatcher.dispatch_task received invalid init_routine {init_routine}", "ERROR")
        return

    reset = True
    restart_count = 0

    while True:
        if restart_count > 0:
            reset = False
            log(f"dispatcher.dispatch_task sleeping {restart_delay}s before restarting {name}", "INFO")
            await asyncio.sleep(restart_delay)

        log(f"dispatcher.dispatch_task starting {name} (restart #{restart_count})", "INFO")

        try:
            if inspect.iscoroutinefunction(init_routine):
                await init_routine(*init_args, reset)
            else:
                init_routine(*init_args)
        except Exception as e:
            restart_count += 1
            log(f"dispatcher.dispatch_task for {name} failed init_routine with: {e}", "ERROR")
            continue  # retry after delay

        log(f"dispatcher.dispatch_task completed init_routine for {name}", "INFO")

        try:
            await persistent_task()
        except Exception as e:
            restart_count += 1
            log(f"dispatcher.dispatch_task for {name} aborted persistent_task with: {e}", "ERROR")
            continue  # retry after delay


async def start_data_collection():
    while True:
        tcp_conn = aiohttp.TCPConnector(
            limit=2,
            limit_per_host=1,
            keepalive_timeout=5
        )
        db_user = os.environ.get("POLY_DB_CLI")
        db_user_pw = os.environ.get("POLY_DB_CLI_PASS")
        socket_dir = os.environ.get("PG_SOCKET")
        db_conn_pool = None
        https_session = None
        try:
            db_conn_pool = await asyncpg.create_pool(
                user=db_user,
                password=db_user_pw,
                database="polymarket",
                host=socket_path,
                port=5432,
                min_size=1,
                max_size=10,
            )

            https_session = aiohttp.ClientSession(connector=tcp_conn)

            # Replace these with your actual tasks
            tasks = [
                dispatch_task("markets", 15, markets.collector, markets.init, https_session, db_conn_pool, events.state.new_event_tokens),
                dispatch_task("events", 5, events.subscriber, events.init, analytics.register_tokens, db_conn_pool),
                dispatch_task("analytics", 15, analytics.pipeline, analytics.init, https_session, db_conn_pool),
            ]

            log("dispatcher.start_data_collection starting tasks", "INFO")
            # Properly run all tasks concurrently
            if tasks:
                await asyncio.gather(*tasks)

        except Exception as e:
            log(f"dispatcher.start_data_collection caught exception: {e}", "ERROR")

        finally:
            if https_session is not None:
                await https_session.close()
            if db_conn_pool is not None:
                await db_conn_pool.close()
            log("dispatcher cleaned up resources, restarting soon", "INFO")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(start_data_collection())
