import os
import json
import asyncio
import asyncpg
import logging
from decimal import Decimal, getcontext

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger()
getcontext().prec = 10

# --- CONFIGURATION ---
COLLECTOR_CONFIG_FILE = "../polygon_collection/collect_events_config.json"
SCHEMA_FILE = "event_args.json"
SEMANTIC_SCHEMA_FILE = "semantic_event_args.json"

async def get_db_connection(config):
    """Establishes a connection to the PostgreSQL database."""
    db_config = config['general']['db']
    try:
        conn = await asyncpg.connect(
            host=os.getenv(db_config['socket_env']),
            port=os.getenv(db_config['port_env']),
            database=os.getenv(db_config['name_env']),
            user=os.getenv(db_config['user_env']),
            password=os.getenv(db_config['pass_env'])
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

async def get_top_addresses_for_arg(conn, table_name, event_name, contract_address, sql_extraction_logic):
    """Queries the database for the top 10 most frequent addresses for a specific event argument."""
    if not sql_extraction_logic:
        return []
    
    query = f"""
        SELECT {sql_extraction_logic} as address, COUNT(*) as address_count
        FROM {table_name}
        WHERE event_name = $1 AND contract_address = $2
        GROUP BY address
        ORDER BY address_count DESC
        LIMIT 10;
    """
    try:
        return await conn.fetch(query, event_name, contract_address)
    except Exception as e:
        logger.error(f"Could not query top addresses for {table_name}.{event_name}: {e}")
        return []

def print_top_addresses_table(arg_name, records):
    """Prints a formatted table of the top addresses."""
    if not records:
        return
        
    print(f"    └── Top 10 '{arg_name}':")
    print(f"        {'Address':<42} | {'Count':<10}")
    print(f"        {'-'*42} | {'-'*10}")
    for record in records:
        address = record['address'] if record['address'] else 'N/A'
        count = record['address_count']
        print(f"        {address:<42} | {count:<10}")

async def calculate_event_distribution(config, conn, schema, semantic_schema):
    """
    Calculates event distribution and analyzes arguments using semantic names.
    """
    for contract in config['contracts']:
        contract_id = contract['id']
        table_name = contract['table_name']
        contract_address = contract['address'].lower()

        try:
            total_events_query = f"SELECT COUNT(*) FROM {table_name} WHERE contract_address = $1;"
            total_events = await conn.fetchval(total_events_query, contract_address)

            if not total_events or total_events == 0:
                logger.warning(f"No events found for contract '{contract_id}'. Skipping.")
                continue

            event_counts_query = f"""
                SELECT event_name, COUNT(*) as event_count
                FROM {table_name} WHERE contract_address = $1
                GROUP BY event_name ORDER BY event_count DESC;
            """
            event_counts_records = await conn.fetch(event_counts_query, contract_address)

            if not event_counts_records: continue

            max_name_len = max(len(r['event_name']) for r in event_counts_records) if event_counts_records else 0
            print(f"\n--- {contract_id} (Total Events: {total_events}) ---")

            for record in event_counts_records:
                event_name = record['event_name']
                count = record['event_count']
                probability = (Decimal(count) / Decimal(total_events))
                aligned_name = event_name.ljust(max_name_len)
                
                base_output = f"  {aligned_name} | P={probability:<.6f} ({count})"
                
                distinct_value_stats = []
                top_addresses_tables_data = []
                
                event_schema = schema.get(table_name, {}).get(event_name, {})
                semantic_event_schema = semantic_schema.get(table_name, {}).get(event_name, {})

                for arg_index, mapping in event_schema.items():
                    arg_info = semantic_event_schema.get(arg_index, {})
                    arg_name = arg_info.get('name', f"arg{arg_index}")
                    arg_type = arg_info.get('type') # Get type from semantic file

                    location = mapping['location']
                    slot = mapping['slot']
                    
                    # Generic extraction for any 32-byte value for distinct counting
                    sql_extraction_logic = ""
                    if location == 'topics':
                        sql_extraction_logic = f"topics[{slot}]"
                    elif location == 'data':
                        start_char = 3 + (slot * 64)
                        sql_extraction_logic = f"'0x' || substring(data from {start_char} for 64)"
                    
                    if not sql_extraction_logic: continue

                    try:
                        distinct_query = f"""
                            SELECT COUNT(DISTINCT({sql_extraction_logic}))
                            FROM {table_name}
                            WHERE event_name = $1 AND contract_address = $2;
                        """
                        distinct_count = await conn.fetchval(distinct_query, event_name, contract_address)
                        distinct_value_stats.append(f"{arg_name}: {distinct_count}")

                        # Specialized logic for 'address' types for Top 10 analysis
                        if arg_type == 'address':
                            address_extraction_logic = ""
                            if location == 'topics':
                                address_extraction_logic = f"'0x' || substring(topics[{slot}] from 27 for 40)"
                            elif location == 'data':
                                start_char_addr = 3 + (slot * 64) + 24
                                address_extraction_logic = f"'0x' || substring(data from {start_char_addr} for 40)"
                            
                            if address_extraction_logic:
                                top_addresses_records = await get_top_addresses_for_arg(
                                    conn, table_name, event_name, contract_address, address_extraction_logic
                                )
                                top_addresses_tables_data.append({"arg_name": arg_name, "records": top_addresses_records})

                    except Exception as e:
                        logger.error(f"Could not query distinct values for {contract_id}.{event_name}.{arg_name}: {e}")
                        distinct_value_stats.append(f"{arg_name}: Error")

                if distinct_value_stats:
                    print(f"{base_output} | Distinct Values ({', '.join(distinct_value_stats)})")
                else:
                    print(base_output)

                for table_data in top_addresses_tables_data:
                    print_top_addresses_table(table_data["arg_name"], table_data["records"])

        except asyncpg.exceptions.UndefinedTableError:
            logger.error(f"Table '{table_name}' does not exist. Skipping contract '{contract_id}'.")
        except Exception as e:
            logger.error(f"An error occurred while querying for contract '{contract_id}': {e}")

async def main():
    try:
        with open(COLLECTOR_CONFIG_FILE, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        logger.error(f"Collector config file '{COLLECTOR_CONFIG_FILE}' not found.")
        return

    try:
        with open(SCHEMA_FILE, 'r') as f:
            schema = json.load(f)
        logger.info(f"Successfully loaded structural schema from '{SCHEMA_FILE}'")
    except FileNotFoundError:
        logger.error(f"Structural schema file '{SCHEMA_FILE}' not found. Please run your generator script first.")
        return
    
    try:
        with open(SEMANTIC_SCHEMA_FILE, 'r') as f:
            semantic_schema = json.load(f)
        logger.info(f"Successfully loaded semantic schema from '{SEMANTIC_SCHEMA_FILE}'")
    except FileNotFoundError:
        logger.warning(f"Semantic schema file '{SEMANTIC_SCHEMA_FILE}' not found. Argument names will be generic.")
        semantic_schema = {}
    
    conn = await get_db_connection(config)
    if conn:
        try:
            await calculate_event_distribution(config, conn, schema, semantic_schema)
        finally:
            await conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    asyncio.run(main())