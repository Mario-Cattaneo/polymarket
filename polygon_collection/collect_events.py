import os
import asyncio
import logging
import aiohttp
import asyncpg
import time
import json
from web3 import Web3

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger()

CONFIG_FILE = "collect_events_config.json"

class RateLimitController:
    """
    A robust, asyncio-compatible rate limiter that enforces a minimum delay
    between calls from any number of concurrent workers. This is based on the
    original, proven implementation.
    """
    def __init__(self, delay: float):
        self.delay = delay
        self.lock = asyncio.Lock()
        self.last_request_time = 0

    async def acquire_permission(self):
        """
        Acquires the lock and waits if necessary to enforce the global delay.
        This guarantees that calls are spaced out by at least `self.delay`.
        """
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_request_time
            
            if elapsed < self.delay:
                wait_for = self.delay - elapsed
                await asyncio.sleep(wait_for)
            
            # Update the last request time *after* the wait
            self.last_request_time = time.monotonic()

class ConfigManager:
    """Manages loading, parsing, and saving the configuration file."""
    def __init__(self, config_path):
        self.path = config_path
        with open(config_path, 'r') as f:
            self.raw = json.load(f)
        
        self.gen = self.raw['general']
        self.w3 = Web3()
        self.lock = asyncio.Lock()
        
        infura_key = os.getenv(self.gen['rpc_env_var'])
        if not infura_key:
            raise ValueError(f"Environment variable {self.gen['rpc_env_var']} not set")
        self.rpc_url = f"{self.gen['rpc_base_url']}{infura_key}"
        
        self.all_events = []
        self.topic_map = {}
        
        for contract_cfg in self.raw['contracts']:
            contract_addr = self.w3.to_checksum_address(contract_cfg['address'])
            for event_cfg in contract_cfg['events']:
                sig = event_cfg['signature']
                topic_hash = self.w3.keccak(text=sig).hex()
                if not topic_hash.startswith("0x"):
                    topic_hash = "0x" + topic_hash
                
                event_obj = {
                    "signature": sig, "name": sig.split('(')[0], "topic_hash": topic_hash,
                    "config_entry": event_cfg, # Direct reference for state updates
                    "contract": {
                        "id": contract_cfg['id'], "address": contract_addr,
                        "table": contract_cfg['table_name']
                    }
                }
                self.all_events.append(event_obj)
                self.topic_map[topic_hash] = event_obj

    def get_db_params(self):
        d = self.gen['db']
        return {
            "host": os.getenv(d['socket_env']), "port": os.getenv(d['port_env']),
            "database": os.getenv(d['name_env']), "user": os.getenv(d['user_env']),
            "password": os.getenv(d['pass_env']),
        }

    async def merge_range(self, event, new_start, new_end):
        """Directly modifies the collected_range in the raw config object."""
        async with self.lock:
            config_entry = event['config_entry']
            start, end = config_entry['collected_range']
            
            if start is None:
                config_entry['collected_range'] = [new_start, new_end]
                return

            config_entry['collected_range'][0] = min(start, new_start)
            config_entry['collected_range'][1] = max(end, new_end)

    async def save_to_disk_async(self):
        """Asynchronously saves the current state to disk without blocking the event loop."""
        def blocking_save():
            with open(self.path, 'w') as f:
                json.dump(self.raw, f, indent=2)

        async with self.lock:
            logger.info(f"Saving configuration state to {self.path}...")
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, blocking_save)


class LogHarvester:
    def __init__(self, config_manager):
        self.cfg = config_manager
        self.pool = None
        self.current_batch_size = self.cfg.gen['batch_size']['initial']
        self.total_logs_found = 0
        self.latest_chain_block = 0
        self.global_start_block = 0
        self.global_end_block = 0
        self.start_block_timestamp_ms = 0
        self.rate_limit_active = asyncio.Event(); self.rate_limit_active.set()
        self.backoff_time = 10
        self.rpc_id_counter = 0
        self.counter_lock = asyncio.Lock()
        self.rate_controller = RateLimitController(delay=self.cfg.gen['global_request_delay'])

    async def init_db(self):
        logger.info("Initializing database schema...")
        async with self.pool.acquire() as conn:
            for contract_cfg in self.cfg.raw['contracts']:
                table_name = contract_cfg['table_name']
                should_drop = contract_cfg.get('drop_on_start', False)
                if should_drop:
                    logger.warning(f"Dropping table '{table_name}' as requested by configuration.")
                    await conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        transaction_hash TEXT, log_index BIGINT, block_number BIGINT,
                        contract_address TEXT, event_name TEXT, topics TEXT[],
                        data TEXT, timestamp_ms BIGINT,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        PRIMARY KEY (transaction_hash, log_index)
                    );
                """)
                timestamp_idx_name = f"idx_{table_name}_timestamp"
                block_idx_name = f"idx_{table_name}_block_number"
                await conn.execute(f"CREATE INDEX IF NOT EXISTS {timestamp_idx_name} ON {table_name}(timestamp_ms);")
                await conn.execute(f"CREATE INDEX IF NOT EXISTS {block_idx_name} ON {table_name}(block_number);")
        logger.info("Database schema initialization complete.")

    async def get_latest_block(self, session):
        try:
            async with self.counter_lock: self.rpc_id_counter += 1; request_id = self.rpc_id_counter
            payload = {"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":request_id}
            async with session.post(self.cfg.rpc_url, json=payload, timeout=10) as resp:
                data = await resp.json()
                return int(data['result'], 16)
        except Exception as e:
            logger.error(f"Could not fetch latest block: {e}")
            return 0

    async def get_block_timestamp(self, session, block_number):
        try:
            async with self.counter_lock: self.rpc_id_counter += 1; request_id = self.rpc_id_counter
            payload = {"jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": [hex(block_number), False], "id": request_id}
            async with session.post(self.cfg.rpc_url, json=payload, timeout=15) as resp:
                resp.raise_for_status()
                data = await resp.json()
                if "result" in data and data["result"] and "timestamp" in data["result"]:
                    return int(data["result"]["timestamp"], 16) * 1000
                logger.error(f"Failed to get timestamp for block {block_number}. Response: {data}")
                return 0
        except Exception as e:
            logger.error(f"Could not fetch timestamp for block {block_number}: {e}")
            return 0

    def get_work_for_chunk(self, chunk_start, chunk_end):
        # --- FIX #2: Corrected logic to find uncollected ranges ---
        events_in_query = []
        for event in self.cfg.all_events:
            collected_range = event['config_entry']['collected_range']
            
            # If the event has never been collected, it needs work.
            if collected_range is None or collected_range[0] is None:
                events_in_query.append(event)
                continue

            collected_start, collected_end = collected_range
            
            # If the current chunk is NOT fully contained within what we've already collected, it needs work.
            is_fully_contained = (chunk_start >= collected_start and chunk_end <= collected_end)
            if not is_fully_contained:
                events_in_query.append(event)

        if not events_in_query: return None, None
        addresses = list(set(e['contract']['address'] for e in events_in_query))
        topics = list(set(e['topic_hash'] for e in events_in_query))
        return addresses, topics
    
    async def fetch_chunk(self, session, start, end, addresses, topics):
        async with self.counter_lock: self.rpc_id_counter += 1; request_id = self.rpc_id_counter
        params = [{"fromBlock": hex(start), "toBlock": hex(end), "address": addresses, "topics": [topics]}]
        payload = {"jsonrpc":"2.0", "method":"eth_getLogs", "params":params, "id": request_id}
        try:
            await self.rate_controller.acquire_permission()
            await self.rate_limit_active.wait()
            async with session.post(self.cfg.rpc_url, json=payload, timeout=45) as resp:
                if resp.status == 429: return {"status": "RATE_LIMIT"}
                if resp.status != 200: return {"status": "HTTP_ERROR", "code": resp.status}
                data = await resp.json()
                if "error" in data:
                    msg = data['error'].get('message', '').lower()
                    if "limit" in msg or "10000" in msg: return {"status": "LIMIT_EXCEEDED"}
                    logger.error(f"RPC Error for payload {json.dumps(payload)}: {json.dumps(data['error'])}")
                    return {"status": "RPC_ERROR", "msg": msg}
                return {"status": "OK", "result": data.get("result", [])}
        except Exception as e:
            return {"status": "EXCEPTION", "msg": str(e)}

    async def worker(self, name, queue, session):
        while True:
            priority, start, end = await queue.get()
            addresses, topics = self.get_work_for_chunk(start, end)
            if not addresses:
                queue.task_done()
                continue
            resp = await self.fetch_chunk(session, start, end, addresses, topics)
            status = resp.get("status")
            await asyncio.sleep(self.cfg.gen['worker_cooldown'])
            if status == "OK":
                logs = resp["result"]
                self.total_logs_found += len(logs)
                if self.current_batch_size < self.cfg.gen['batch_size']['max']:
                    self.current_batch_size = min(self.cfg.gen['batch_size']['max'], self.current_batch_size + 2)
                progress = (start - self.global_start_block) / (self.global_end_block - self.global_start_block) * 100
                logger.info(f"[{progress:.2f}%] Worker {name} | {start}-{end} | Found: {len(logs)} | Batch: {self.current_batch_size}")
                if logs: await self.insert_logs(logs)
                for event in self.cfg.all_events:
                    if event['topic_hash'] in topics:
                        await self.cfg.merge_range(event, start, end)
                queue.task_done()
            elif status == "LIMIT_EXCEEDED":
                old_size = self.current_batch_size
                self.current_batch_size = max(self.cfg.gen['batch_size']['min'], self.current_batch_size // 2)
                logger.warning(f"Worker {name} | {start}-{end} | Range too large | Batch: {old_size} -> {self.current_batch_size}")
                mid = (start + end) // 2
                if start >= end: logger.error(f"Cannot split single block {start}, skipping.")
                else:
                    queue.put_nowait((0, start, mid))
                    queue.put_nowait((0, mid + 1, end))
                queue.task_done()
            elif status == "RATE_LIMIT":
                logger.warning(f"Global rate limit hit. Pausing all workers for {self.backoff_time}s...")
                self.rate_limit_active.clear()
                await asyncio.sleep(self.backoff_time)
                self.backoff_time = min(self.backoff_time * 1.5, 60)
                self.rate_limit_active.set()
                logger.info("Resuming workers...")
                queue.put_nowait((priority, start, end))
                queue.task_done()
            else:
                msg = resp.get('msg', 'Unknown Error')
                logger.error(f"Worker {name} | {start}-{end} | {status}: {msg} | Retrying in 5s...")
                await asyncio.sleep(5)
                queue.put_nowait((priority, start, end))
                queue.task_done()

    async def producer(self, queue):
        current_block = self.global_start_block
        while current_block <= self.global_end_block:
            if queue.qsize() > self.cfg.gen['concurrency'] * 3:
                await asyncio.sleep(0.5)
                continue
            end_block = min(current_block + self.current_batch_size - 1, self.global_end_block)
            queue.put_nowait((1, current_block, end_block))
            current_block = end_block + 1
        logger.info("Producer has generated all required block ranges.")

    async def insert_logs(self, logs):
        inserts = {}
        for log in logs:
            topic0 = log['topics'][0]
            event = self.cfg.topic_map.get(topic0)
            if not event: continue
            table = event['contract']['table']
            if table not in inserts: inserts[table] = []
            log_block_number = int(log['blockNumber'], 16)
            block_delta = log_block_number - self.global_start_block
            interpolated_timestamp_ms = self.start_block_timestamp_ms + (block_delta * 2000)
            inserts[table].append((
                log['transactionHash'], int(log['logIndex'], 16), log_block_number,
                log['address'].lower(), event['name'], log['topics'], log['data'],
                interpolated_timestamp_ms
            ))
        async with self.pool.acquire() as conn:
            for table, rows in inserts.items():
                try:
                    await conn.executemany(f"""
                        INSERT INTO {table} 
                        (transaction_hash, log_index, block_number, contract_address, event_name, topics, data, timestamp_ms)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (transaction_hash, log_index) DO NOTHING
                    """, rows)
                except Exception as e:
                    logger.error(f"DB Insert Error ({table}): {e}")

    async def background_saver(self):
        save_interval = self.cfg.gen.get('save_interval_seconds', 15)
        logger.info(f"State will be saved to disk every {save_interval} seconds.")
        while True:
            try:
                await asyncio.sleep(save_interval)
                await self.cfg.save_to_disk_async()
            except asyncio.CancelledError:
                logger.info("Background saver task is shutting down.")
                break
            except Exception as e:
                logger.error(f"Error in background saver: {e}")

    async def run(self):
        self.pool = await asyncpg.create_pool(**self.cfg.get_db_params())
        await self.init_db()
        async with aiohttp.ClientSession() as session:
            self.latest_chain_block = await self.get_latest_block(session)
            if self.latest_chain_block == 0: return

            # --- FIX #1: Dynamically determine the starting block ---
            resume_from_block = None
            all_ranges_found = True
            for event in self.cfg.all_events:
                collected_range = event['config_entry'].get('collected_range')
                if collected_range and collected_range[1] is not None:
                    if resume_from_block is None:
                        resume_from_block = collected_range[1]
                    else:
                        resume_from_block = min(resume_from_block, collected_range[1])
                else:
                    # If any event has no collected range, we must start from the beginning.
                    all_ranges_found = False
                    break
            
            if all_ranges_found and resume_from_block is not None:
                self.global_start_block = resume_from_block + 1
            else:
                self.global_start_block = self.cfg.raw['collection_range']['start_block']
            
            self.global_end_block = self.cfg.raw['collection_range']['end_block'] or self.latest_chain_block

            if self.global_start_block > self.global_end_block:
                logger.info("Collection is up to date. Nothing to do.")
                return

            self.start_block_timestamp_ms = await self.get_block_timestamp(session, self.global_start_block)
            if self.start_block_timestamp_ms == 0:
                logger.error("Could not get initial block timestamp. Aborting.")
                return
                
            logger.info(f"Starting Harvester")
            logger.info(f"Global Range: {self.global_start_block} -> {self.global_end_block}")
            logger.info(f"Base Timestamp for Block {self.global_start_block}: {self.start_block_timestamp_ms} ms")
            
            queue = asyncio.PriorityQueue()
            saver_task = asyncio.create_task(self.background_saver())
            producer_task = asyncio.create_task(self.producer(queue))
            workers = [asyncio.create_task(self.worker(i, queue, session)) for i in range(self.cfg.gen['concurrency'])]
            
            await producer_task
            await queue.join()
            
            for w in workers: w.cancel()
            saver_task.cancel()
            try: await saver_task
            except asyncio.CancelledError: pass
            
            logger.info(f"Sync Complete. Total Logs Found: {self.total_logs_found}")

if __name__ == "__main__":
    cfg_manager = None
    try:
        cfg_manager = ConfigManager(CONFIG_FILE)
        harvester = LogHarvester(cfg_manager)
        asyncio.run(harvester.run())
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    except Exception as e:
        logger.error(f"A fatal error occurred: {e}", exc_info=True)
    finally:
        # --- Reverted to original, safe final save logic ---
        if cfg_manager:
            logger.info("Performing final state save...")
            with open(cfg_manager.path, 'w') as f:
                json.dump(cfg_manager.raw, f, indent=2)
            logger.info("Final save complete.")