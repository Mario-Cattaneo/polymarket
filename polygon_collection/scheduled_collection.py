import asyncio
import os
from datetime import datetime

# --- Configuration ---
# The command to run.
# Note: If 'python' is not in your PATH, you might need to use the full path
# to the python executable within your virtual environment,
# e.g., "/home/user/venv/bin/python collect_actual_events.py"
EVENT_SCRIPT_COMMAND = "python collect_actual_events.py" # Assuming the actual logic is in another script

# Scheduling interval in seconds (1 hour = 60 * 60 = 3600)
INTERVAL_SECONDS = 3600

# Log file for monitoring the last run status
MONITOR_LOG_FILE = "monitor.log"

# Log file for capturing the output of the event script
OUTPUT_LOG_FILE = "collect_events.log"


async def run_collect_events():
    """
    Runs the event collection command and captures its output.
    """
    print(f"[{datetime.now()}] --- Starting Event Collection ---")

    # Update the monitor log to show the script is running
    try:
        with open(MONITOR_LOG_FILE, "w") as monitor_file:
            monitor_file.write(f"[{datetime.now()}] - Running collect_events.py...\n")
    except IOError as e:
        print(f"Error: Could not write to {MONITOR_LOG_FILE}. {e}")
        # We can decide to continue or not, for now, we'll print and continue.

    # Create a subprocess to run the command
    process = await asyncio.create_subprocess_shell(
        EVENT_SCRIPT_COMMAND,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    # Wait for the command to complete and capture the output
    stdout, stderr = await process.communicate()

    # Append the output to the main log file
    try:
        with open(OUTPUT_LOG_FILE, "a") as output_file:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            output_file.write(f"\n--- Log Entry: {timestamp} ---\n")
            if stdout:
                output_file.write("--- STDOUT ---\n")
                output_file.write(stdout.decode())
            if stderr:
                output_file.write("--- STDERR ---\n")
                output_file.write(stderr.decode())
            output_file.write(f"--- End of Entry (Return Code: {process.returncode}) ---\n")
    except IOError as e:
        print(f"Error: Could not write to {OUTPUT_LOG_FILE}. {e}")

    # Update the monitor log with the completion status
    try:
        with open(MONITOR_LOG_FILE, "w") as monitor_file:
            monitor_file.write(f"[{datetime.now()}] - Last run finished. Waiting for next cycle.\n")
    except IOError as e:
        print(f"Error: Could not update {MONITOR_LOG_FILE} after run. {e}")

    print(f"[{datetime.now()}] --- Event Collection Finished ---")


async def main_scheduler():
    """
    The main asynchronous loop for scheduling the event collection.
    """
    print(f"Starting scheduler. The script will run every {INTERVAL_SECONDS} seconds.")
    while True:
        try:
            await run_collect_events()

            print(f"Waiting for {INTERVAL_SECONDS} seconds until the next run...")
            await asyncio.sleep(INTERVAL_SECONDS)

        except asyncio.CancelledError:
            print("\nScheduler stopped by external signal. Exiting.")
            break
        except Exception as e:
            print(f"An unexpected error occurred in the main loop: {e}. Restarting wait cycle.")
            # In case of an unexpected error, wait a minute before trying again
            await asyncio.sleep(60)


# --- Entry Point ---

if __name__ == "__main__":
    # It's good practice to have a placeholder for the actual logic script
    # to avoid the script calling itself in an infinite loop.
    # We will create a dummy 'collect_actual_events.py' if it doesn't exist.
    if not os.path.exists("collect_actual_events.py"):
        with open("collect_actual_events.py", "w") as f:
            f.write("from datetime import datetime\n")
            f.write("print(f'[{datetime.now()}] - Dummy event collection script executed successfully.')\n")

    try:
        asyncio.run(main_scheduler())
    except KeyboardInterrupt:
        print("\nScheduler manually stopped.")