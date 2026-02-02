import asyncio
import asyncpg
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

MOO_UMA_ADAPTER = "0x65070BE91477460D8A7AeEb94ef92fe056C2f2A7"

async def check_events():
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
    
    async with pool.acquire() as conn:
        # Get all unique event names
        query = "SELECT DISTINCT event_name FROM events_uma_adapter WHERE LOWER(contract_address) = LOWER($1) ORDER BY event_name"
        events = await conn.fetch(query, MOO_UMA_ADAPTER)
        
        logger.info(f"Events in MOO V2 Adapter ({MOO_UMA_ADAPTER}):")
        for row in events:
            event_name = row['event_name']
            count = await conn.fetchval("SELECT COUNT(*) FROM events_uma_adapter WHERE event_name = $1 AND LOWER(contract_address) = LOWER($2)", event_name, MOO_UMA_ADAPTER)
            logger.info(f"  {event_name}: {count:,}")
    
    await pool.close()

if __name__ == "__main__":
    asyncio.run(check_events())
