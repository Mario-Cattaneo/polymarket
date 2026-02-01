import os
import asyncio
import asyncpg
import json

async def debug_messages():
    pool = await asyncpg.create_pool(
        host=os.getenv("PG_SOCKET", "localhost"),
        port=int(os.getenv("POLY_PG_PORT", 5432)),
        database=os.getenv("POLY_DB"),
        user=os.getenv("POLY_DB_CLI"),
        password=os.getenv("POLY_DB_CLI_PASS"),
    )

    async with pool.acquire() as conn:
        records = await conn.fetch("""
            SELECT asset_id, outcome, server_time_us, message
            FROM poly_book_state_house
            WHERE asset_id = '83247781037352156539108067944461291821683755894607244160607042790356561625563'
            ORDER BY server_time_us ASC
            LIMIT 10;
        """)

        for i, row in enumerate(records):
            print(f"\n=== MESSAGE {i+1} ===")
            print(f"asset_id: {row['asset_id'][:16]}...")
            print(f"outcome: {row['outcome']}")
            print(f"server_time_us: {row['server_time_us']}")
            msg = row['message']
            if isinstance(msg, str):
                msg = json.loads(msg)
            print(f"Message keys: {msg.keys() if msg else 'None'}")
            if msg:
                print(f"Full message:\n{json.dumps(msg, indent=2)}")

asyncio.run(debug_messages())
