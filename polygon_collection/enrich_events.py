import os
import asyncio
import json
import aiohttp
import asyncpg

# --- CONFIG ---
INFURA_KEY = os.getenv('INFURA_KEY')
RPC_URL = f"https://polygon-mainnet.infura.io/v3/{INFURA_KEY}"

# DB Config
PG_SOCKET = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

CONCURRENCY = 20  # Parallel block fetches

class Enricher:
    def __init__(self):
        self.pool = None

    async def fetch_block(self, session, blk_num):
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [hex(blk_num), True], # True = Full Transaction Objects
            "id": 1
        }
        try:
            async with session.post(RPC_URL, json=payload) as resp:
                if resp.status == 429: return "RATE_LIMIT"
                data = await resp.json()
                return data.get("result")
        except:
            return None

    async def worker(self, name, queue, session):
        while True:
            try:
                blk_num = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            # Fetch Block
            block_data = await self.fetch_block(session, blk_num)
            
            if block_data == "RATE_LIMIT":
                print(f"   🛑 Rate Limit. Sleeping 2s...")
                await asyncio.sleep(2)
                queue.put_nowait(blk_num) # Put back in queue
                queue.task_done()
                continue
            
            if not block_data:
                queue.task_done()
                continue

            # Process Transactions
            tx_map = {}
            for tx in block_data.get('transactions', []):
                tx_hash = tx['hash']
                # Add timestamp to the tx object for convenience
                tx['block_timestamp'] = block_data['timestamp'] 
                tx_map[tx_hash] = json.dumps(tx)

            # Update DB
            if tx_map:
                await self.update_db(tx_map)
            
            print(f"   ✅ Enriched Block {blk_num}", end='\r')
            queue.task_done()

    async def update_db(self, tx_map):
        async with self.pool.acquire() as conn:
            values = [(v, k) for k, v in tx_map.items()]
            await conn.executemany("""
                UPDATE polygon_events
                SET transaction_data = $1::jsonb
                WHERE transaction_hash = $2
            """, values)

    async def run(self):
        self.pool = await asyncpg.create_pool(host=PG_SOCKET, port=PG_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        
        print("🔍 Scanning for blocks needing enrichment...")
        
        # 1. Find blocks to process using the Partial Index
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT block_number 
                FROM polygon_events 
                WHERE transaction_data IS NULL
                ORDER BY block_number ASC
            """)
        
        blocks_to_do = [r['block_number'] for r in rows]
        total = len(blocks_to_do)
        print(f"🧱 Found {total} blocks to enrich.")
        
        if total == 0: return

        # 2. Queue & Workers
        queue = asyncio.Queue()
        for b in blocks_to_do: queue.put_nowait(b)
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for i in range(CONCURRENCY):
                task = asyncio.create_task(self.worker(i, queue, session))
                tasks.append(task)
            
            await queue.join()
            for t in tasks: t.cancel()
            
        print("\n✨ Enrichment Complete.")

if __name__ == "__main__":
    enricher = Enricher()
    asyncio.run(enricher.run())