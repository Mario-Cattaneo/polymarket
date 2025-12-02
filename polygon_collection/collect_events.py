import os
import asyncio
import logging
import aiohttp
import asyncpg
import time
import json
import random
from asyncio import PriorityQueue
from web3 import Web3

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger()

# --- CONFIG ---
INFURA_KEY = os.getenv('INFURA_KEY_3')
if not INFURA_KEY:
    logger.error("❌ Error: INFURA_KEY not set.")
    exit(1)

RPC_URL = f"https://polygon-mainnet.infura.io/v3/{INFURA_KEY}"

# DB Config
PG_SOCKET = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- TUNING ---
# CRITICAL: Reduced concurrency to stay under the 2000 credits/sec limit
CONCURRENCY = 6
INITIAL_BATCH_SIZE = 100 
MIN_BATCH_SIZE = 5
MAX_BATCH_SIZE = 2000
CURRENT_BATCH_SIZE = INITIAL_BATCH_SIZE

# Rate Limit Tuning
GLOBAL_REQUEST_DELAY = 2 # Minimum seconds between ANY request (Global)
WORKER_COOLDOWN = 0.1       # Extra sleep per worker after a request
INITIAL_BACKOFF = 10        # Seconds to sleep on 429
MAX_BACKOFF = 60

DEFAULT_START_BLOCK = 79172085 

# --- CONTRACTS & EVENTS ---
w3 = Web3()

_CTF_EXCHANGE = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
_NEG_RISK_ADAPTER = "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296"
_NEG_RISK_CTF = "0xc5d563a36ae78145c45a50134d48a1215220f80a"
_CONDITIONAL_TOKENS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
_NEG_RISK_OPERATOR = "0x71523d0f655b41e805cec45b17163f528b59b820"

RAW_EVENTS = [
    (_CTF_EXCHANGE, "FeeCharged(address,uint256,uint256)", "FeeCharged"),
    (_CTF_EXCHANGE, "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)", "OrderFilled"),
    (_CTF_EXCHANGE, "OrdersMatched(bytes32,address,uint256,uint256,uint256,uint256)", "OrdersMatched"),
    (_CTF_EXCHANGE, "ProxyFactoryUpdated(address,address)", "ProxyFactoryUpdated"),
    (_CTF_EXCHANGE, "SafeFactoryUpdated(address,address)", "SafeFactoryUpdated"),
    (_CTF_EXCHANGE, "TokenRegistered(uint256,uint256,bytes32)", "TokenRegistered"),
    (_CTF_EXCHANGE, "TradingPaused(address)", "TradingPaused"),
    (_CTF_EXCHANGE, "TradingUnpaused(address)", "TradingUnpaused"),
    (_CONDITIONAL_TOKENS, "ConditionPreparation(bytes32,address,bytes32,uint256)", "ConditionPreparation"),
    (_CONDITIONAL_TOKENS, "ConditionResolution(bytes32,address,bytes32,uint256,uint256[])", "ConditionResolution"),
    (_CONDITIONAL_TOKENS, "PositionSplit(address,address,bytes32,bytes32,uint256[],uint256)", "PositionSplit"),
    (_CONDITIONAL_TOKENS, "PositionsMerge(address,address,bytes32,bytes32,uint256[],uint256)", "PositionsMerge"),
    (_CONDITIONAL_TOKENS, "PayoutRedemption(address,address,bytes32,bytes32,uint256[],uint256)", "PayoutRedemption"),
    #(_CONDITIONAL_TOKENS, "TransferSingle(address,address,address,uint256,uint256)", "TransferSingle"),
    #(_CONDITIONAL_TOKENS, "TransferBatch(address,address,address,uint256[],uint256[])", "TransferBatch"),
    (_CONDITIONAL_TOKENS, "ApprovalForAll(address,address,bool)", "ApprovalForAll"),
    (_CONDITIONAL_TOKENS, "URI(string,uint256)", "URI"),
    (_NEG_RISK_ADAPTER, "MarketPrepared(bytes32,address,uint256,bytes)", "MarketPrepared"),
    (_NEG_RISK_ADAPTER, "OutcomeReported(bytes32,bytes32,bool)", "OutcomeReported"),
    (_NEG_RISK_ADAPTER, "PayoutRedemption(address,bytes32,uint256[],uint256)", "PayoutRedemption"),
    (_NEG_RISK_ADAPTER, "PositionSplit(address,bytes32,uint256)", "PositionSplit"),
    (_NEG_RISK_ADAPTER, "PositionsConverted(address,bytes32,uint256,uint256)", "PositionsConverted"),
    (_NEG_RISK_ADAPTER, "PositionsMerge(address,bytes32,uint256)", "PositionsMerge"),
    (_NEG_RISK_ADAPTER, "QuestionPrepared(bytes32,bytes32,uint256,bytes)", "QuestionPrepared"),
    (_NEG_RISK_CTF, "FeeCharged(address,uint256,uint256)", "FeeCharged"),
    (_NEG_RISK_CTF, "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)", "OrderFilled"),
    (_NEG_RISK_CTF, "OrdersMatched(bytes32,address,uint256,uint256,uint256,uint256)", "OrdersMatched"),
    (_NEG_RISK_CTF, "SafeFactoryUpdated(address,address)", "SafeFactoryUpdated"),
    (_NEG_RISK_CTF, "TokenRegistered(uint256,uint256,bytes32)", "TokenRegistered"),
    (_NEG_RISK_CTF, "TradingPaused(address)", "TradingPaused"),
    (_NEG_RISK_CTF, "TradingUnpaused(address)", "TradingUnpaused"),
    (_NEG_RISK_OPERATOR, "MarketPrepared(bytes32,uint256,bytes)", "MarketPrepared"),
    (_NEG_RISK_OPERATOR, "QuestionEmergencyResolved(bytes32,bool)", "QuestionEmergencyResolved"),
    (_NEG_RISK_OPERATOR, "QuestionFlagged(bytes32)", "QuestionFlagged"),
    (_NEG_RISK_OPERATOR, "QuestionPrepared(bytes32,bytes32,bytes32,uint256,bytes)", "QuestionPrepared"),
    (_NEG_RISK_OPERATOR, "QuestionReported(bytes32,bytes32,bool)", "QuestionReported"),
    (_NEG_RISK_OPERATOR, "QuestionResolved(bytes32,bool)", "QuestionResolved"),
    (_NEG_RISK_OPERATOR, "QuestionUnflagged(bytes32)", "QuestionUnflagged"),
]

EVENT_REGISTRY = {}
TOPICS_OF_INTEREST = set()
ADDRESSES_OF_INTEREST = set()

for addr, sig, name in RAW_EVENTS:
    t_hash = w3.keccak(text=sig).hex()
    if not t_hash.startswith("0x"): t_hash = "0x" + t_hash
    EVENT_REGISTRY[t_hash] = name
    TOPICS_OF_INTEREST.add(t_hash)
    ADDRESSES_OF_INTEREST.add(addr.lower())

RPC_ADDRESS_LIST = [Web3.to_checksum_address(a) for a in ADDRESSES_OF_INTEREST]
RPC_TOPIC_LIST = list(TOPICS_OF_INTEREST)

class RateLimitController:
    """
    Strictly enforces a global gap between requests.
    """
    def __init__(self):
        self.active = asyncio.Event()
        self.active.set()
        self.backoff_time = INITIAL_BACKOFF
        self.lock = asyncio.Lock()
        self.last_request_time = 0

    async def acquire_permission(self):
        """
        Ensures that NO requests are sent within GLOBAL_REQUEST_DELAY seconds of each other.
        """
        await self.active.wait()
        
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_request_time
            if elapsed < GLOBAL_REQUEST_DELAY:
                wait_time = GLOBAL_REQUEST_DELAY - elapsed
                await asyncio.sleep(wait_time)
            
            self.last_request_time = time.time()

    async def trigger_backoff(self):
        async with self.lock:
            # Debounce: If we just backed off, don't do it again immediately
            if time.time() - self.last_request_time > (self.backoff_time + 1):
                return

            logger.warning(f"🛑 GLOBAL RATE LIMIT HIT. Pausing all workers for {self.backoff_time}s...")
            self.active.clear()
            
            await asyncio.sleep(self.backoff_time)
            
            # Exponential backoff
            self.backoff_time = min(self.backoff_time * 1.5, MAX_BACKOFF)
            
            logger.info("🟢 Resuming workers...")
            self.active.set()
            self.last_request_time = time.time()

    async def success(self):
        if self.backoff_time > INITIAL_BACKOFF:
            self.backoff_time = max(INITIAL_BACKOFF, self.backoff_time - 1)

class LogHarvester:
    def __init__(self):
        self.pool = None
        self.id_counter = 0
        self.latest_chain_block = 0
        self.start_block_initial = 0
        self.total_logs_found = 0
        
        self.inflight_blocks = set()
        self.producer_cursor = 0
        self.blocks_processed_count = 0
        self.state_lock = asyncio.Lock()
        
        self.rate_controller = RateLimitController()

    async def init_db(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS polygon_events (
                    transaction_hash TEXT,
                    log_index BIGINT,
                    block_number BIGINT,
                    contract_address TEXT,
                    event_name TEXT,
                    topics TEXT[],
                    data TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (transaction_hash, log_index)
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS indexer_state (
                    network_name TEXT PRIMARY KEY,
                    last_synced_block BIGINT,
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)

    async def get_start_block(self):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT last_synced_block FROM indexer_state WHERE network_name = 'polygon'")
            if row and row['last_synced_block']:
                return row['last_synced_block']
            
            row = await conn.fetchrow("SELECT MAX(block_number) as max_blk FROM polygon_events")
            if row and row['max_blk']:
                return max(DEFAULT_START_BLOCK, row['max_blk'] - 1000)
            
            return DEFAULT_START_BLOCK

    async def save_checkpoint(self):
        async with self.state_lock:
            if not self.inflight_blocks:
                safe_block = self.producer_cursor
            else:
                safe_block = min(self.inflight_blocks)

        if safe_block > 0:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO indexer_state (network_name, last_synced_block, updated_at)
                    VALUES ('polygon', $1, NOW())
                    ON CONFLICT (network_name) 
                    DO UPDATE SET last_synced_block = $1, updated_at = NOW()
                """, safe_block)

    async def background_saver(self):
        while True:
            try:
                await asyncio.sleep(10)
                await self.save_checkpoint()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Save state failed: {e}")

    async def get_latest_block(self, session):
        payload = {"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}
        try:
            async with session.post(RPC_URL, json=payload) as resp:
                data = await resp.json()
                return int(data['result'], 16)
        except:
            return 0

    async def fetch_chunk(self, session, start, end):
        self.id_counter += 1
        params = [{
            "fromBlock": hex(start),
            "toBlock": hex(end),
            "address": RPC_ADDRESS_LIST,
            "topics": [RPC_TOPIC_LIST]
        }]
        payload = {"jsonrpc":"2.0","method":"eth_getLogs","params":params,"id":self.id_counter}

        try:
            # 1. Wait for global permission (Strict Pacing)
            await self.rate_controller.acquire_permission()

            async with session.post(RPC_URL, json=payload, timeout=30) as resp:
                # 2. Handle HTTP Status Codes
                if resp.status == 429: 
                    # Try to read body for details
                    try:
                        err_body = await resp.text()
                        logger.warning(f"🛑 429 Body: {err_body[:200]}")
                    except: pass
                    return {"status": "RATE_LIMIT"}
                
                if resp.status != 200:
                    text = await resp.text()
                    return {"status": "HTTP_ERROR", "code": resp.status, "msg": text}

                # 3. Handle JSON-RPC Errors
                data = await resp.json()
                if "error" in data:
                    err_msg = data['error'].get('message', '').lower()
                    code = data['error'].get('code', 0)
                    
                    # Log the full error for debugging
                    logger.error(f"⚠️ RPC Error Body: {json.dumps(data['error'])}")

                    if code == -32005 or "limit exceeded" in err_msg or "more than 10000" in err_msg:
                        return {"status": "LIMIT_EXCEEDED", "msg": err_msg}
                    
                    return {"status": "RPC_ERROR", "msg": err_msg}
                
                return {"status": "OK", "result": data.get("result", [])}
        except asyncio.TimeoutError:
            return {"status": "TIMEOUT"}
        except Exception as e:
            return {"status": "EXCEPTION", "msg": str(e)}

    def get_progress_str(self):
        total_range = self.latest_chain_block - self.start_block_initial
        if total_range <= 0: return "0%"
        pct = (self.blocks_processed_count / total_range) * 100
        return f"[{pct:.2f}%]"

    async def worker(self, name, queue, session):
        global CURRENT_BATCH_SIZE
        
        while True:
            try:
                priority, start, end = await queue.get()
            except asyncio.CancelledError:
                break

            async with self.state_lock:
                self.inflight_blocks.add(start)

            resp = await self.fetch_chunk(session, start, end)
            status = resp["status"]

            # Mandatory fixed wait per worker to cool down
            await asyncio.sleep(WORKER_COOLDOWN)

            if status == "OK":
                logs = resp["result"]
                count = len(logs)
                self.total_logs_found += count
                
                async with self.state_lock:
                    self.blocks_processed_count += (end - start + 1)
                
                if CURRENT_BATCH_SIZE < MAX_BATCH_SIZE:
                    CURRENT_BATCH_SIZE += 1
                
                await self.rate_controller.success()

                logger.info(f"{self.get_progress_str()} ✅ W{name} | {start}-{end} | Found: {count} | Batch: {CURRENT_BATCH_SIZE}")
                
                await self.insert_logs(logs)
                
                async with self.state_lock:
                    self.inflight_blocks.discard(start)
                
                queue.task_done()

            elif status == "LIMIT_EXCEEDED":
                old_size = CURRENT_BATCH_SIZE
                CURRENT_BATCH_SIZE = max(MIN_BATCH_SIZE, CURRENT_BATCH_SIZE - 15)
                logger.warning(f"{self.get_progress_str()} ⚠️ W{name} | {start}-{end} | SIZE LIMIT | Batch: {old_size} -> {CURRENT_BATCH_SIZE}")
                
                mid = start + CURRENT_BATCH_SIZE
                if mid >= end: mid = (start + end) // 2
                if start == end: mid = start

                async with self.state_lock:
                    self.inflight_blocks.discard(start)
                
                queue.put_nowait((0, start, mid))
                queue.put_nowait((0, mid + 1, end))
                queue.task_done()

            elif status == "RATE_LIMIT":
                await self.rate_controller.trigger_backoff()
                queue.put_nowait((priority, start, end))
                queue.task_done()

            else:
                msg = resp.get('msg', 'Unknown')
                logger.error(f"{self.get_progress_str()} 🔥 W{name} | {start}-{end} | {status}: {msg} | Retrying...")
                await asyncio.sleep(2)
                queue.put_nowait((priority, start, end))
                queue.task_done()

    async def producer(self, queue, start_block):
        current_ptr = start_block
        self.producer_cursor = start_block
        
        while current_ptr <= self.latest_chain_block:
            if queue.qsize() > CONCURRENCY * 3:
                await asyncio.sleep(0.5)
                continue
            
            size = CURRENT_BATCH_SIZE
            end = min(current_ptr + size, self.latest_chain_block)
            
            queue.put_nowait((10, current_ptr, end))
            
            current_ptr = end + 1
            async with self.state_lock:
                self.producer_cursor = current_ptr
        
        logger.info("🏁 Producer finished generating ranges.")

    async def insert_logs(self, logs):
        if not logs: return
        rows = []
        for log in logs:
            rows.append((
                log['transactionHash'],
                int(log['logIndex'], 16),
                int(log['blockNumber'], 16),
                log['address'].lower(),
                EVENT_REGISTRY.get(log['topics'][0], "Unknown"),
                log['topics'],
                log['data']
            ))
        
        try:
            async with self.pool.acquire() as conn:
                await conn.executemany("""
                    INSERT INTO polygon_events 
                    (transaction_hash, log_index, block_number, contract_address, event_name, topics, data)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (transaction_hash, log_index) DO NOTHING
                """, rows)
        except Exception as e:
            logger.error(f"DB Insert Error: {e}")

    async def run(self):
        self.pool = await asyncpg.create_pool(host=PG_SOCKET, port=PG_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        await self.init_db()
        
        connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=connector) as session:
            
            start_block = await self.get_start_block()
            self.start_block_initial = start_block
            self.latest_chain_block = await self.get_latest_block(session)
            
            logger.info(f"🚀 Starting Harvester")
            logger.info(f"   Range: {start_block} -> {self.latest_chain_block}")
            logger.info(f"   Total Blocks: {self.latest_chain_block - start_block}")
            
            queue = asyncio.PriorityQueue()
            
            producer_task = asyncio.create_task(self.producer(queue, start_block))
            saver_task = asyncio.create_task(self.background_saver())
            
            workers = []
            for i in range(CONCURRENCY):
                task = asyncio.create_task(self.worker(i, queue, session))
                workers.append(task)
            
            await producer_task
            await queue.join()
            
            for t in workers: t.cancel()
            saver_task.cancel()
            
            await self.save_checkpoint()
            logger.info(f"\n✅ Sync Complete. Total Logs Found: {self.total_logs_found}")

if __name__ == "__main__":
    harvester = LogHarvester()
    try:
        asyncio.run(harvester.run())
    except KeyboardInterrupt:
        logger.info("\n🛑 Stopped by user.")