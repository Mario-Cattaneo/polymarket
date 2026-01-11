import asyncio
import json
import os
import logging
import asyncpg
import heapq
import time
from typing import Optional, Tuple, List, Dict, Any
from collections import deque
from Crypto.Hash import keccak
import httpx

# Import the provided CLI tools
from http_cli import HttpManager, HttpTaskConfig, HttpTaskCallbacks, Method

# --- Configuration & Constants ---
INFURA_MAX_LOGS = 10_000
ALPHA = 0.2  # Exponential Moving Average smoothing factor
TOLERANCE = 0.1  # Safety margin (10%)
CONFIG_FILE = "collect_events_config.json"
BLOCK_TIME_MS = 2000  # 2 seconds per block

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Silence noisy libraries
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# --- Helper Functions ---

def get_keccak_hash(signature: str) -> str:
    k = keccak.new(digest_bits=256)
    k.update(signature.encode('utf-8'))
    return '0x' + k.hexdigest()

async def get_db_pool(db_config: dict, pool_size: int):
    try:
        pool = await asyncpg.create_pool(
            host=os.environ.get(db_config['socket_env'], 'localhost'),
            port=os.environ.get(db_config['port_env'], '5432'),
            database=os.environ.get(db_config['name_env']),
            user=os.environ.get(db_config['user_env']),
            password=os.environ.get(db_config['pass_env']),
            min_size=pool_size,
            max_size=pool_size
        )
        return pool
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise e

async def get_latest_block(rpc_url: str) -> int:
    async with httpx.AsyncClient() as client:
        payload = {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}
        resp = await client.post(rpc_url, json=payload)
        return int(resp.json()['result'], 16)

async def get_block_timestamp(rpc_url: str, block_number: int) -> int:
    async with httpx.AsyncClient() as client:
        payload = {"jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": [hex(block_number), False], "id": 1}
        resp = await client.post(rpc_url, json=payload)
        data = resp.json()
        if "result" in data and data["result"] and "timestamp" in data["result"]:
            return int(data["result"]["timestamp"], 16) * 1000
        return 0

# --- Main Logic ---

class EventCollector:
    def __init__(self, config: dict):
        self.config = config
        self.general = config['general']
        self.api_key = os.environ.get(self.general['rpc_env_var'])
        self.rpc_url = f"{self.general['rpc_base_url']}{self.api_key}"
        
        self.min_batch = self.general['batch_size']['min']
        self.max_batch = self.general['batch_size']['max']
        
        self.db_pool = None
        
        # State
        self.current_contract_address: str = ""
        self.current_event_signature: str = ""
        self.current_event_name: str = ""
        self.current_topic: str = ""
        self.current_table: str = ""
        
        self.current_event_config: Optional[dict] = None
        self.current_run_start_block: int = 0
        
        self.global_start_block: int = 0
        self.global_end_block: int = 0
        self.start_block_timestamp_ms: int = 0
        
        self.next_start_block: int = 0
        self.avg_logs_per_block: float = 0.0
        self.current_batch_size: int = self.max_batch
        
        self.retry_queue: deque[Tuple[int, int]] = deque()
        
        # Logic Flags
        self.initial_estimation_done = False
        self.estimation_in_flight = False
        self.active_requests: int = 0
        self.processing_complete = False
        
        # Watchdog for hang detection
        self.last_progress_time: float = 0.0
        self.last_progress_block: int = 0
        self.watchdog_timeout_s: float = 300  # 5 minutes of no progress = force completion

        # Contiguous Progress Tracking
        self.last_contiguous_block: int = 0
        self.completed_ranges_heap: List[Tuple[int, int]] = []
        
        # Gap Filling State
        self.skip_range: Optional[Tuple[int, int]] = None

    async def init_db_schema(self):
        async with self.db_pool.acquire() as conn:
            for contract in self.config['contracts']:
                table_name = contract['table_name']
                if contract.get('drop_on_start', False):
                    logger.warning(f"Dropping table {table_name}")
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
                await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp_ms);")
                await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_block_number ON {table_name}(block_number);")

    def _mark_range_complete(self, start: int, end: int):
        heapq.heappush(self.completed_ranges_heap, (start, end))

        while self.completed_ranges_heap:
            next_range = self.completed_ranges_heap[0]
            r_start, r_end = next_range
            
            if r_start == self.last_contiguous_block + 1:
                heapq.heappop(self.completed_ranges_heap)
                self.last_contiguous_block = max(self.last_contiguous_block, r_end)
            elif r_start <= self.last_contiguous_block:
                heapq.heappop(self.completed_ranges_heap)
                self.last_contiguous_block = max(self.last_contiguous_block, r_end)
            else:
                break

    def save_config(self, force_end_block: Optional[int] = None):
        if not self.current_event_config: return

        if force_end_block is not None:
            run_end = force_end_block
        else:
            run_end = self.last_contiguous_block

        run_start = self.current_run_start_block
        
        if run_end < run_start: return

        existing_range = self.current_event_config.get('collected_range')
        new_start = run_start
        new_end = run_end

        if existing_range and len(existing_range) == 2 and existing_range[0] is not None:
            new_start = min(existing_range[0], run_start)
            new_end = max(existing_range[1], run_end)
        
        self.current_event_config['collected_range'] = [new_start, new_end]

        try:
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self.config, f, indent=2)
            logger.info(f"Config saved. Range: [{new_start}, {new_end}]")
        except Exception as e:
            logger.error(f"Failed to save config: {e}")

    async def run(self):
        pool_size = self.general['max_concurrent'] + 2
        logger.info(f"Initializing DB pool with size: {pool_size}")
        self.db_pool = await get_db_pool(self.general['db'], pool_size)
        
        await self.init_db_schema()

        self.global_start_block = self.config['collection_range']['start_block']
        conf_end = self.config['collection_range']['end_block']
        
        if conf_end is None:
            logger.info("Global end block is null, fetching latest...")
            self.global_end_block = await get_latest_block(self.rpc_url)
        else:
            self.global_end_block = conf_end
        
        logger.info(f"Fetching timestamp for start block {self.global_start_block}...")
        self.start_block_timestamp_ms = await get_block_timestamp(self.rpc_url, self.global_start_block)
        if self.start_block_timestamp_ms == 0:
            logger.error("Could not fetch start block timestamp. Aborting.")
            return

        logger.info(f"Global Collection Range: {self.global_start_block} -> {self.global_end_block}")

        http_manager = HttpManager(
            max_connections=self.general['max_concurrent'],
            max_keepalive_connections=self.general['max_concurrent']
        )
        task_config = HttpTaskConfig(request_break_s=1.0 / self.general['global_rate_limit'])

        for contract in self.config['contracts']:
            self.current_contract_address = contract['address']
            self.current_table = contract['table_name']
            
            for event in contract['events']:
                self.current_event_config = event 
                self.current_event_signature = event['signature']
                self.current_event_name = self.current_event_signature.split('(')[0]
                self.current_topic = get_keccak_hash(self.current_event_signature)
                
                # --- GAP DETECTION LOGIC ---
                collected = event.get('collected_range')
                self.skip_range = None
                
                if collected and len(collected) == 2 and collected[0] is not None:
                    prev_start, prev_end = collected
                    if self.global_start_block < prev_start:
                        # Gap detected at the start. Start from global, but skip the already collected middle.
                        start_block = self.global_start_block
                        self.skip_range = (prev_start, prev_end)
                        logger.info(f"Gap detected! Fetching {start_block}->{prev_start-1}, then skipping to {prev_end+1}")
                    else:
                        start_block = max(prev_end + 1, self.global_start_block)
                else:
                    start_block = self.global_start_block
                
                if start_block > self.global_end_block:
                    logger.info(f"Skipping {self.current_event_name}: Up to date.")
                    continue

                logger.info(f"Starting {self.current_event_name} from {start_block}")
                
                self.current_run_start_block = start_block
                self.next_start_block = start_block
                self.avg_logs_per_block = 0.0 
                
                init_batch = event.get('init_batch_size')
                if init_batch and isinstance(init_batch, int) and init_batch > 0:
                    self.current_batch_size = max(self.min_batch, min(init_batch, self.max_batch))
                    logger.info(f"Using custom initial batch size: {self.current_batch_size}")
                else:
                    self.current_batch_size = self.max_batch
                
                self.retry_queue.clear()
                self.active_requests = 0
                self.processing_complete = False
                self.initial_estimation_done = False
                self.estimation_in_flight = False
                
                self.last_contiguous_block = start_block - 1
                self.completed_ranges_heap = []
                
                # Initialize progress tracking
                self.last_progress_time = time.time()
                self.last_progress_block = start_block - 1
                
                callbacks = HttpTaskCallbacks(
                    next_request=self.next_request,
                    on_response=self.on_response,
                    on_exception=self.on_exception
                )
                
                producer = http_manager.get_request_producer(
                    config=task_config,
                    callbacks=callbacks,
                    max_concurrent_requests=self.general['max_concurrent']
                )
                
                producer.start()
                
                while not self.processing_complete:
                    await asyncio.sleep(1)
                    
                    # Standard completion check
                    if (self.next_start_block > self.global_end_block and 
                        len(self.retry_queue) == 0 and 
                        self.active_requests == 0 and
                        self.initial_estimation_done):
                        self.processing_complete = True
                        continue
                    
                    # Watchdog safety valve: force completion if no progress for 5 mins
                    # but we've exhausted the block range
                    if (self.next_start_block > self.global_end_block and 
                        self.initial_estimation_done):
                        time_since_progress = time.time() - self.last_progress_time
                        if time_since_progress > self.watchdog_timeout_s:
                            logger.warning(f"WATCHDOG: No progress for {time_since_progress:.0f}s. "
                                         f"Active requests: {self.active_requests}, "
                                         f"Retry queue: {len(self.retry_queue)}. "
                                         f"Force completing.")
                            self.processing_complete = True
                
                producer.stop()
                self.save_config()
                logger.info(f"Finished {self.current_event_name}")

        await http_manager.shutdown()
        await self.db_pool.close()
        logger.info("All events collected.")

    def next_request(self) -> Tuple[Any, str, Method, Optional[dict], Optional[bytes]]:
        # 1. Retry Queue
        if self.retry_queue:
            start, end = self.retry_queue.popleft()
            self.active_requests += 1
            return self._build_request((start, end, "retry"), start, end)

        # 2. Probe
        if not self.initial_estimation_done:
            if self.estimation_in_flight:
                return self._build_dummy_request()
            else:
                start = self.next_start_block
                end = min(start + self.current_batch_size - 1, self.global_end_block)
                self.estimation_in_flight = True
                self.active_requests += 1
                return self._build_request((start, end, "probe"), start, end)

        # 3. New Requests
        if self.next_start_block <= self.global_end_block:
            
            # --- GAP JUMPING LOGIC ---
            if self.skip_range:
                skip_start, skip_end = self.skip_range
                # If we are about to request a block inside the skip range
                if self.next_start_block >= skip_start and self.next_start_block <= skip_end:
                    logger.info(f"Skipping already collected range: {skip_start} -> {skip_end}")
                    # Fast forward
                    self.next_start_block = skip_end + 1
                    # Mark the skipped range as 'done' for contiguous tracking
                    self.last_contiguous_block = max(self.last_contiguous_block, skip_end)
                    
                    # If we jumped past the end, we are done
                    if self.next_start_block > self.global_end_block:
                        return self._build_dummy_request()

            if self.avg_logs_per_block > 0:
                target_logs = INFURA_MAX_LOGS * (1 - TOLERANCE)
                calculated_batch = int(target_logs / self.avg_logs_per_block)
                self.current_batch_size = min(self.max_batch, max(self.min_batch, calculated_batch))
            else:
                self.current_batch_size = self.max_batch
            
            start = self.next_start_block
            end = min(start + self.current_batch_size - 1, self.global_end_block)
            self.next_start_block = end + 1
            self.active_requests += 1
            return self._build_request((start, end, "new"), start, end)
        
        # 4. Idle
        return self._build_dummy_request()

    def _build_request(self, req_id_tuple, start, end):
        # FIX: Include current_contract_address in the ID to prevent collisions
        # between different contracts sharing the same HttpManager
        unique_req_id = (self.current_contract_address, ) + req_id_tuple
        
        payload = {
            "jsonrpc": "2.0", "method": "eth_getLogs", "id": 1,
            "params": [{"address": self.current_contract_address, "fromBlock": hex(start), "toBlock": hex(end), "topics": [self.current_topic]}]
        }
        return unique_req_id, self.rpc_url, Method.POST, {"Content-Type": "application/json"}, json.dumps(payload).encode()

    def _build_dummy_request(self):
        # FIX: Match the tuple size (4 elements)
        return (None, 0, 0, "ignore"), self.rpc_url, Method.POST, None, None

    async def on_response(self, req_id: Any, content: bytes, status: int, headers: Any, rtt: float) -> bool:
        # FIX: Unpack 4 elements
        addr, start, end, req_type = req_id
        
        if req_type == "ignore": return True

        # FIX: Zombie Check - Ignore responses from previous contracts
        if addr != self.current_contract_address:
            return True

        try:
            response = json.loads(content)
        except json.JSONDecodeError:
            logger.error(f"JSON Error {start}-{end}")
            self._handle_failure(start, end, req_type)
            return True

        if 'error' in response:
            code = response['error'].get('code')
            if code == -32005: # Limit exceeded
                self._handle_limit_exceeded(start, end, req_type)
            else:
                logger.error(f"RPC Error {code}: {response['error'].get('message')}")
                self._handle_failure(start, end, req_type)
            return True

        if 'result' in response:
            logs = response['result']
            
            if len(logs) > 0:
                try:
                    await self.save_logs_to_db(logs)
                except Exception as e:
                    logger.error(f"DB Write Failed {start}-{end}: {e}")
                    self._handle_failure(start, end, req_type)
                    return True
            
            self._update_metrics(start, end, len(logs), req_type)
            self._mark_range_complete(start, end)
            
            # Update progress tracking for watchdog
            if self.last_contiguous_block > self.last_progress_block:
                self.last_progress_block = self.last_contiguous_block
                self.last_progress_time = time.time()
            
            total_blocks = self.global_end_block - self.current_run_start_block
            progress = 0.0
            if total_blocks > 0:
                progress = ((self.last_contiguous_block - self.current_run_start_block + 1) / total_blocks) * 100
            
            status_tag = "Probe" if req_type == "probe" else "Retry" if req_type == "retry" else "OK"
            logger.info(f"[{self.current_event_name}] Block {end} ({progress:5.1f}%) | Found {len(logs):4} logs | Batch {end-start+1:6} | {status_tag}")

            if req_type == "probe":
                logger.info(f"Estimation done. Avg: {self.avg_logs_per_block:.4f} logs/blk")
                self.initial_estimation_done = True
                self.estimation_in_flight = False
                self.next_start_block = end + 1
                # Update progress time after probe
                self.last_progress_time = time.time()

        self.active_requests -= 1
        return True

    def _handle_limit_exceeded(self, start, end, req_type):
        failed_len = end - start + 1
        new_batch = failed_len // 2
        
        logger.warning(f"Limit Hit | Range: {start}-{end} ({failed_len}) | Splitting -> {new_batch}")
        
        if new_batch < self.min_batch:
            logger.critical(f"Batch size too small at {start}. Aborting.")
            self.processing_complete = True
            self.active_requests -= 1
            return

        self.current_batch_size = new_batch

        if req_type == "probe":
            self.estimation_in_flight = False
        else:
            mid = start + new_batch - 1
            self.retry_queue.appendleft((mid + 1, end))
            self.retry_queue.appendleft((start, mid))
        
        self.active_requests -= 1

    def _handle_failure(self, start, end, req_type):
        if req_type == "probe":
            self.estimation_in_flight = False
        else:
            self.retry_queue.append((start, end))
        self.active_requests -= 1

    def _update_metrics(self, start, end, log_count, req_type):
        range_len = end - start + 1
        logs_per_block = log_count / range_len
        if self.avg_logs_per_block == 0:
            self.avg_logs_per_block = logs_per_block
        else:
            self.avg_logs_per_block = (ALPHA * logs_per_block) + ((1 - ALPHA) * self.avg_logs_per_block)

    async def on_exception(self, req_id: Any, e: Exception) -> bool:
        # FIX: Unpack 4 elements
        addr, start, end, req_type = req_id
        
        if req_type == "ignore": return True
        
        # FIX: Zombie Check
        if addr != self.current_contract_address:
            return True

        logger.error(f"Exception {start}-{end}: {e}")
        self._handle_failure(start, end, req_type)
        return True

    async def save_logs_to_db(self, logs: List[dict]):
        rows = []
        for log in logs:
            block_number = int(log.get('blockNumber'), 16)
            block_delta = block_number - self.global_start_block
            timestamp_ms = self.start_block_timestamp_ms + (block_delta * BLOCK_TIME_MS)
            rows.append((
                log.get('transactionHash'), int(log.get('logIndex'), 16), block_number,
                log.get('address').lower(), self.current_event_name, log.get('topics'),
                log.get('data'), timestamp_ms
            ))

        sql = f"""
            INSERT INTO {self.current_table} 
            (transaction_hash, log_index, block_number, contract_address, event_name, topics, data, timestamp_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (transaction_hash, log_index) DO NOTHING
        """
        
        async with self.db_pool.acquire() as conn:
            await conn.executemany(sql, rows)

if __name__ == "__main__":
    with open(CONFIG_FILE, 'r') as f:
        config_data = json.load(f)
    
    collector = EventCollector(config_data)
    try:
        asyncio.run(collector.run())
    except KeyboardInterrupt:
        logger.info("Aborting... Saving progress.")
        collector.save_config(force_end_block=collector.last_contiguous_block)
        logger.info("Progress saved. Exiting.")