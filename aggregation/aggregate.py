import asyncio
import asyncpg
import os
from datetime import datetime

# --- Configuration ---
# Directory where your SQL files are located
SCRIPT_DIR = "/home/user/postgres_agg_scripts"

# List of SQL files to run, in order.
SQL_SCRIPTS = [
    "/home/mario_cattaneo/polymarket/aggregation/pg_scripts/aggregate_events.sql",
    "/home/mario_cattaneo/polymarket/aggregation/pg_scripts/miss_count_agg.sql",
]

# Scheduling interval in seconds (3 hours = 3 * 60 * 60 = 10800)
INTERVAL_SECONDS = 10800 

# --- Database Connection Variables ---
# asyncpg uses standard connection parameters.
PG_HOST = os.environ.get("PG_SOCKET")
PG_PORT = os.environ.get("POLY_PG_PORT")
PG_USER = os.environ.get("POLY_DB_CLI")
PG_DBNAME = os.environ.get("POLY_DB")

# --- Log Directory Variable (NEW) ---
LOG_DIR = os.environ.get("POLY_LOGS")

# --- Core Functions ---

def get_sql_content(script_name):
    """Reads the content of an SQL file."""
    full_path = os.path.join(SCRIPT_DIR, script_name)
    try:
        with open(full_path, 'r') as f:
            return f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL script not found: {full_path}")

async def run_sql_scripts(log_file):
    """Connects to DB and executes the list of SQL scripts sequentially."""
    
    log_file.write(f"\n--- {datetime.now()} - Starting Aggregation Run ---\n")
    
    # 1. Check for required environment variables
    required_vars = [PG_HOST, PG_PORT, PG_USER, PG_DBNAME, LOG_DIR]
    if not all(required_vars):
        error_msg = "ERROR: Missing one or more required environment variables (PG_SOCKET, POLY_PG_PORT, POLY_DB_CLI, POLY_DB, POLY_LOGS)."
        log_file.write(error_msg + "\n")
        print(error_msg)
        return False

    conn = None
    try:
        # 2. Establish the connection
        conn = await asyncpg.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            database=PG_DBNAME
        )
        
        # 3. Sequential Execution Loop
        for script in SQL_SCRIPTS:
            log_file.write(f"Running script: {script}\n")
            print(f"Running script: {script}")
            
            sql_content = get_sql_content(script)
            
            # Execute the entire DO $$...$$ block
            await conn.execute(sql_content)
            
            log_file.write(f"Script {script} completed successfully.\n")
            print(f"Script {script} completed successfully.")

    except asyncpg.PostgresError as e:
        error_msg = f"ERROR: A PostgreSQL error occurred during script execution.\n"
        error_msg += f"Details: {e}\n"
        log_file.write(error_msg)
        print(error_msg)
        return False # Stop the entire run on failure

    except FileNotFoundError as e:
        error_msg = f"ERROR: {e}"
        log_file.write(error_msg + "\n")
        print(error_msg)
        return False
        
    except Exception as e:
        error_msg = f"ERROR: An unexpected error occurred: {e}"
        log_file.write(error_msg + "\n")
        print(error_msg)
        return False
        
    finally:
        # 4. Close the connection
        if conn:
            await conn.close()
            
    log_file.write(f"--- {datetime.now()} - All Scripts Completed Successfully ---\n")
    return True

async def main_scheduler():
    """The main asynchronous loop for scheduling."""
    # Check for LOG_DIR early
    if not LOG_DIR:
        print("FATAL: POLY_LOGS environment variable is not set. Cannot proceed.")
        return

    print(f"Starting async scheduler. Scripts will run every {INTERVAL_SECONDS} seconds.")
    # Ensure the log directory exists
    os.makedirs(LOG_DIR, exist_ok=True)
    
    while True:
        log_file_path = os.path.join(LOG_DIR, f"agg_run_{datetime.now().strftime('%Y%m%d')}.log")
        
        try:
            with open(log_file_path, "a") as log_file:
                # Run the aggregation task
                await run_sql_scripts(log_file)
            
            # Wait for the next interval using asyncio.sleep
            print(f"Waiting for {INTERVAL_SECONDS} seconds...")
            await asyncio.sleep(INTERVAL_SECONDS)
            
        except asyncio.CancelledError:
            print("\nScheduler stopped by external signal. Exiting.")
            break
        except Exception as e:
            print(f"An unexpected error occurred in the main loop: {e}. Restarting wait cycle.")
            await asyncio.sleep(60) # Wait a minute before trying again

# --- Entry Point ---

if __name__ == "__main__":
    try:
        # Use asyncio.run to start the main async function
        asyncio.run(main_scheduler())
    except KeyboardInterrupt:
        print("Scheduler manually stopped.")