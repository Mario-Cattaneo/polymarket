import os
import sys
import asyncio
import time
from pathlib import Path
from enum import Enum
from datetime import datetime, timezone, timedelta

import asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# --- Configuration ---
PLOT_INTERVAL_s = 60
DEFAULT_SAMPLE_SIZE = 5000  # Controls the resolution of all CDF plots
DEFAULT_TIME_WINDOW_HOURS = 24

# --- Asset ID Ordering Enum ---
class AssetIdOrder(Enum):
    BY_BOOK_COUNT = 1
    BY_PRICE_CHANGE_COUNT = 2
    BY_LAST_TRADE_COUNT = 3
    BY_TICK_SIZE_CHANGE_COUNT = 4
    BY_AVERAGE_EVENT_COUNT = 5
    BY_LIFETIME = 6
    BY_AVG_FALSE_MISSES = 7 # New ordering option

# --- Database Query Functions ---

class DatabaseFetcher:
    """Encapsulates all SQL queries and data fetching logic based on the new schema."""

    # --- REWRITTEN QUERIES FOR THE NEW SCHEMA ---

    BOOKS_CDF = """
        WITH filtered_events AS (
            SELECT server_time FROM books WHERE server_time BETWEEN $2 AND $3
        ),
        full_cdf AS (
            SELECT server_time, ROW_NUMBER() OVER (ORDER BY server_time ASC) as cdf_value
            FROM filtered_events
        ),
        tiled_events AS (
            SELECT server_time, cdf_value, ntile($1) OVER (ORDER BY server_time) as tile
            FROM full_cdf
        )
        SELECT MAX(server_time) as bucket_start_ms, MAX(cdf_value) as cdf_value
        FROM tiled_events GROUP BY tile ORDER BY tile;
    """

    PRICE_CHANGES_CDF = """
        WITH filtered_events AS (
            SELECT server_time FROM price_changes WHERE server_time BETWEEN $2 AND $3
        ),
        full_cdf AS (
            SELECT server_time, ROW_NUMBER() OVER (ORDER BY server_time ASC) as cdf_value
            FROM filtered_events
        ),
        tiled_events AS (
            SELECT server_time, cdf_value, ntile($1) OVER (ORDER BY server_time) as tile
            FROM full_cdf
        )
        SELECT MAX(server_time) as bucket_start_ms, MAX(cdf_value) as cdf_value
        FROM tiled_events GROUP BY tile ORDER BY tile;
    """

    LAST_TRADE_PRICES_CDF = """
        WITH filtered_events AS (
            SELECT server_time FROM last_trade_prices WHERE server_time BETWEEN $2 AND $3
        ),
        full_cdf AS (
            SELECT server_time, ROW_NUMBER() OVER (ORDER BY server_time ASC) as cdf_value
            FROM filtered_events
        ),
        tiled_events AS (
            SELECT server_time, cdf_value, ntile($1) OVER (ORDER BY server_time) as tile
            FROM full_cdf
        )
        SELECT MAX(server_time) as bucket_start_ms, MAX(cdf_value) as cdf_value
        FROM tiled_events GROUP BY tile ORDER BY tile;
    """

    TICK_CHANGES_CDF = """
        WITH filtered_events AS (
            SELECT server_time FROM tick_changes WHERE server_time BETWEEN $2 AND $3
        ),
        full_cdf AS (
            SELECT server_time, ROW_NUMBER() OVER (ORDER BY server_time ASC) as cdf_value
            FROM filtered_events
        ),
        tiled_events AS (
            SELECT server_time, cdf_value, ntile($1) OVER (ORDER BY server_time) as tile
            FROM full_cdf
        )
        SELECT MAX(server_time) as bucket_start_ms, MAX(cdf_value) as cdf_value
        FROM tiled_events GROUP BY tile ORDER BY tile;
    """

    FOUND_MARKETS_CDF = """
        WITH filtered_events AS (
            SELECT found_time FROM markets WHERE found_time BETWEEN $2 AND $3
        ),
        full_cdf AS (
            SELECT found_time, ROW_NUMBER() OVER (ORDER BY found_time ASC) as cdf_value
            FROM filtered_events
        ),
        tiled_events AS (
            SELECT found_time, cdf_value, ntile($1) OVER (ORDER BY found_time) as tile
            FROM full_cdf
        )
        SELECT MAX(found_time) as bucket_start_ms, MAX(cdf_value) as cdf_value
        FROM tiled_events GROUP BY tile ORDER BY tile;
    """

    CLOSED_MARKETS_CDF = """
        WITH filtered_events AS (
            SELECT closed_time FROM markets WHERE closed_time IS NOT NULL AND closed_time BETWEEN $2 AND $3
        ),
        full_cdf AS (
            SELECT closed_time, ROW_NUMBER() OVER (ORDER BY closed_time ASC) as cdf_value
            FROM filtered_events
        ),
        tiled_events AS (
            SELECT closed_time, cdf_value, ntile($1) OVER (ORDER BY closed_time) as tile
            FROM full_cdf
        )
        SELECT MAX(closed_time) as bucket_start_ms, MAX(cdf_value) as cdf_value
        FROM tiled_events GROUP BY tile ORDER BY tile;
    """

    GOOD_CONNECTIONS_CDF = """
        WITH filtered_events AS (
            SELECT timestamp_ms FROM events_connections WHERE success = TRUE AND timestamp_ms BETWEEN $2 AND $3
        ),
        full_cdf AS (
            SELECT timestamp_ms, ROW_NUMBER() OVER (ORDER BY timestamp_ms ASC) as cdf_value
            FROM filtered_events
        ),
        tiled_events AS (
            SELECT timestamp_ms, cdf_value, ntile($1) OVER (ORDER BY timestamp_ms) as tile
            FROM full_cdf
        )
        SELECT MAX(timestamp_ms) as bucket_start_ms, MAX(cdf_value) as cdf_value
        FROM tiled_events GROUP BY tile ORDER BY tile;
    """

    BAD_CONNECTIONS_CDF = """
        WITH filtered_events AS (
            SELECT timestamp_ms FROM events_connections WHERE success = FALSE AND timestamp_ms BETWEEN $2 AND $3
        ),
        full_cdf AS (
            SELECT timestamp_ms, ROW_NUMBER() OVER (ORDER BY timestamp_ms ASC) as cdf_value
            FROM filtered_events
        ),
        tiled_events AS (
            SELECT timestamp_ms, cdf_value, ntile($1) OVER (ORDER BY timestamp_ms) as tile
            FROM full_cdf
        )
        SELECT MAX(timestamp_ms) as bucket_start_ms, MAX(cdf_value) as cdf_value
        FROM tiled_events GROUP BY tile ORDER BY tile;
    """

    # --- NEW: Combined Event Count CDF ---
    EVENT_COUNT_CDF = """
        WITH all_events AS (
            SELECT server_time FROM books WHERE server_time BETWEEN $2 AND $3
            UNION ALL
            SELECT server_time FROM price_changes WHERE server_time BETWEEN $2 AND $3
            UNION ALL
            SELECT server_time FROM last_trade_prices WHERE server_time BETWEEN $2 AND $3
        ),
        full_cdf AS (
            SELECT server_time, ROW_NUMBER() OVER (ORDER BY server_time ASC) as cdf_value
            FROM all_events
        ),
        tiled_events AS (
            SELECT server_time, cdf_value, ntile($1) OVER (ORDER BY server_time) as tile
            FROM full_cdf
        )
        SELECT MAX(server_time) as bucket_start_ms, MAX(cdf_value) as cdf_value
        FROM tiled_events GROUP BY tile ORDER BY tile;
    """

    # --- NEW: False Misses PDF ---
    FALSE_MISSES_PDF = """
        SELECT (server_time / $1) * $1 AS bucket_start_ms, AVG(false_misses) AS avg_false_misses
        FROM server_book
        WHERE server_time BETWEEN $2 AND $3
        GROUP BY 1
        ORDER BY 1;
    """

    EXHAUSTION_CYCLE_CDF = "SELECT (found_time / $1) * $1 AS bucket_start_ms, MAX(exhaustion_cycle) AS cdf_value FROM markets WHERE found_time BETWEEN $2 AND $3 GROUP BY 1 ORDER BY 1;"
    MARKETS_RTT = "SELECT (timestamp_ms / $1) * $1 AS bucket_start_ms, AVG(rtt) AS agg_rtt FROM markets_rtt WHERE timestamp_ms BETWEEN $2 AND $3 GROUP BY 1 ORDER BY 1;"
    ANALYTICS_RTT = "SELECT (timestamp_ms / $1) * $1 AS bucket_start_ms, AVG(rtt) AS agg_rtt FROM analytics_rtt WHERE timestamp_ms BETWEEN $2 AND $3 GROUP BY 1 ORDER BY 1;"

    # Queries for asset-specific plots
    ASSET_BOOK_COUNT = "SELECT asset_id, COUNT(*) as book_count FROM books GROUP BY asset_id ORDER BY book_count ASC;"
    ASSET_PRICE_CHANGE_COUNT = "SELECT asset_id, COUNT(*) as price_change_count FROM price_changes GROUP BY asset_id ORDER BY price_change_count ASC;"
    ASSET_LAST_TRADE_COUNT = "SELECT asset_id, COUNT(*) as last_trade_count FROM last_trade_prices GROUP BY asset_id ORDER BY last_trade_count ASC;"
    ASSET_TICK_CHANGE_COUNT = "SELECT asset_id, COUNT(*) as tick_change_count FROM tick_changes GROUP BY asset_id ORDER BY tick_change_count ASC;"
    TOKEN_LIFETIME = "SELECT asset_id, (closed_time - found_time) as life_time FROM markets WHERE closed_time IS NOT NULL AND found_time IS NOT NULL ORDER BY life_time ASC;"
    
    AVERAGE_EVENT_COUNT_PER_ASSET = """
        WITH all_events AS (
            SELECT asset_id FROM books
            UNION ALL
            SELECT asset_id FROM price_changes
            UNION ALL
            SELECT asset_id FROM last_trade_prices
        )
        SELECT asset_id, COUNT(*) as avg_event_count
        FROM all_events
        GROUP BY asset_id
        ORDER BY avg_event_count ASC;
    """
    # --- NEW: Average False Misses per Asset ---
    AVERAGE_FALSE_MISSES_PER_ASSET = "SELECT asset_id, AVG(false_misses) as avg_false_misses FROM server_book GROUP BY asset_id ORDER BY avg_false_misses ASC;"


    @staticmethod
    async def fetch_time_series_data(pool: asyncpg.Pool, query: str, sample_or_bucket_size: int, start_time_ms: int, end_time_ms: int):
        return await pool.fetch(query, sample_or_bucket_size, start_time_ms, end_time_ms)

    @staticmethod
    async def fetch_asset_ordered_data(pool: asyncpg.Pool, query: str):
        return await pool.fetch(query)


class PlottingService:
    def __init__(self, pool: asyncpg.Pool, plot_dir: Path):
        self.pool = pool
        self.fetcher = DatabaseFetcher()
        self.plot_dir = plot_dir
        self.plot_dir.mkdir(parents=True, exist_ok=True)
        print(f"Plots will be saved in: {self.plot_dir.resolve()}")

    async def plot_over_time(self, start_time_ms: int, end_time_ms: int, sample_size: int = DEFAULT_SAMPLE_SIZE):
        interval_size = end_time_ms - start_time_ms
        if interval_size <= 0:
            print("Warning: Invalid time interval for plotting.")
            return
            
        bucket_size_ms = max(1, (interval_size + sample_size - 1) // sample_size)

        all_data_tasks = {
            "found_markets": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.FOUND_MARKETS_CDF, sample_size, start_time_ms, end_time_ms),
            "closed_markets": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.CLOSED_MARKETS_CDF, sample_size, start_time_ms, end_time_ms),
            "exhaustion_cycles": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.EXHAUSTION_CYCLE_CDF, bucket_size_ms, start_time_ms, end_time_ms),
            "books": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.BOOKS_CDF, sample_size, start_time_ms, end_time_ms),
            "price_changes": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.PRICE_CHANGES_CDF, sample_size, start_time_ms, end_time_ms),
            "last_trades": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.LAST_TRADE_PRICES_CDF, sample_size, start_time_ms, end_time_ms),
            "tick_changes": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.TICK_CHANGES_CDF, sample_size, start_time_ms, end_time_ms),
            "good_connections": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.GOOD_CONNECTIONS_CDF, sample_size, start_time_ms, end_time_ms),
            "bad_connections": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.BAD_CONNECTIONS_CDF, sample_size, start_time_ms, end_time_ms),
            "markets_rtt": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.MARKETS_RTT, bucket_size_ms, start_time_ms, end_time_ms),
            "analytics_rtt": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.ANALYTICS_RTT, bucket_size_ms, start_time_ms, end_time_ms),
            "event_count_cdf": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.EVENT_COUNT_CDF, sample_size, start_time_ms, end_time_ms), # Changed to CDF
            "false_misses_pdf": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.FALSE_MISSES_PDF, bucket_size_ms, start_time_ms, end_time_ms), # New plot
        }
        
        results = await asyncio.gather(*all_data_tasks.values())
        data_map = dict(zip(all_data_tasks.keys(), results))

        # Increased subplot grid size to accommodate new plots
        fig, axes = plt.subplots(7, 2, figsize=(20, 35), constrained_layout=True)
        fig.suptitle(f'Time-Series Analysis (Since Nov 11, 2025)', fontsize=16)
        axes = axes.flatten()

        plot_defs = {
            0: ("found_markets", "CDF of Markets Found", "cdf_value"), 1: ("closed_markets", "CDF of Markets Closed", "cdf_value"),
            2: ("exhaustion_cycles", "Max Exhaustion Cycle", "cdf_value"), 3: ("books", "CDF of Book Events", "cdf_value"),
            4: ("price_changes", "CDF of Price Change Events", "cdf_value"), 5: ("last_trades", "CDF of Last Trade Events", "cdf_value"),
            6: ("tick_changes", "CDF of Tick Size Change Events", "cdf_value"), 7: ("event_count_cdf", "CDF of All Events", "cdf_value"), # Changed to CDF
            8: ("good_connections", "CDF of Good Connections", "cdf_value"), 9: ("bad_connections", "CDF of Bad Connections", "cdf_value"),
            10: ("markets_rtt", "Markets RTT (ms)", "agg_rtt"), 11: ("analytics_rtt", "Analytics RTT (ms)", "agg_rtt"),
            12: ("false_misses_pdf", "Avg. False Misses (PDF)", "avg_false_misses"), # New plot
        }

        has_any_data = False
        for i, ax in enumerate(axes):
            if i in plot_defs:
                key, title, y_col = plot_defs[i]
                raw_data = data_map.get(key)
                if raw_data:
                    has_any_data = True
                    column_names = ['bucket_start_ms', y_col]
                    df = pd.DataFrame(raw_data, columns=column_names)
                    df['time'] = pd.to_datetime(df['bucket_start_ms'], unit='ms')
                    ax.plot(df['time'], df[y_col], drawstyle='steps-post')
                    ax.set_title(title); ax.grid(True, linestyle='--', alpha=0.6)
                    
                    locator = mdates.AutoDateLocator()
                    formatter = mdates.AutoDateFormatter(locator)
                    ax.xaxis.set_major_locator(locator)
                    ax.xaxis.set_major_formatter(formatter)
                    
                    plt.setp(ax.get_xticklabels(), rotation=30, ha='right')
                else:
                    ax.text(0.5, 0.5, 'No Data', ha='center', va='center'); ax.set_title(title)
            else: ax.set_visible(False)

        if has_any_data:
            save_path = self.plot_dir / "time_series.png"
            plt.savefig(save_path)
        plt.close(fig)

    async def plot_over_asset_ids(self, order_by: AssetIdOrder = AssetIdOrder.BY_BOOK_COUNT):
        query_map = {
            "book_count": self.fetcher.ASSET_BOOK_COUNT,
            "price_change_count": self.fetcher.ASSET_PRICE_CHANGE_COUNT,
            "last_trade_count": self.fetcher.ASSET_LAST_TRADE_COUNT,
            "tick_change_count": self.fetcher.ASSET_TICK_CHANGE_COUNT,
            "avg_event_count": self.fetcher.AVERAGE_EVENT_COUNT_PER_ASSET,
            "lifetime": self.fetcher.TOKEN_LIFETIME,
            "avg_false_misses": self.fetcher.AVERAGE_FALSE_MISSES_PER_ASSET, # New plot
        }

        all_asset_tasks = {k: self.fetcher.fetch_asset_ordered_data(self.pool, q) for k, q in query_map.items()}
        results = await asyncio.gather(*all_asset_tasks.values())
        
        asset_plot_columns = {
            "book_count": ['asset_id', 'book_count'], "price_change_count": ['asset_id', 'price_change_count'],
            "last_trade_count": ['asset_id', 'last_trade_count'], "tick_change_count": ['asset_id', 'tick_change_count'],
            "avg_event_count": ['asset_id', 'avg_event_count'], "lifetime": ['asset_id', 'life_time'],
            "avg_false_misses": ['asset_id', 'avg_false_misses'], # New plot
        }
        data_map = {}
        for i, key in enumerate(all_asset_tasks.keys()):
            raw_data = results[i]
            cols = asset_plot_columns[key]
            data_map[key] = pd.DataFrame(raw_data, columns=cols) if raw_data else pd.DataFrame(columns=cols)

        primary_key_map = {
            AssetIdOrder.BY_BOOK_COUNT: "book_count", AssetIdOrder.BY_PRICE_CHANGE_COUNT: "price_change_count",
            AssetIdOrder.BY_LAST_TRADE_COUNT: "last_trade_count", AssetIdOrder.BY_TICK_SIZE_CHANGE_COUNT: "tick_change_count",
            AssetIdOrder.BY_AVERAGE_EVENT_COUNT: "avg_event_count", AssetIdOrder.BY_LIFETIME: "lifetime",
            AssetIdOrder.BY_AVG_FALSE_MISSES: "avg_false_misses", # New ordering option
        }
        primary_key = primary_key_map[order_by]
        
        primary_df = data_map[primary_key]

        if primary_df.empty:
            print("Cannot generate asset plots: primary ordering data is empty. This is expected if the database is new.")
            return
            
        ordered_asset_ids = primary_df['asset_id'].tolist()

        reindexed_dfs = {}
        for key, df in data_map.items():
            if not df.empty and 'asset_id' in df.columns:
                reindexed_dfs[key] = df.set_index('asset_id').reindex(ordered_asset_ids).reset_index()

        # Increased subplot grid size
        fig, axes = plt.subplots(4, 2, figsize=(20, 28), constrained_layout=True)
        fig.suptitle(f'Asset ID Analysis (Ordered by {order_by.name})', fontsize=16)
        axes = axes.flatten()

        plot_defs = {
            0: ("book_count", "Total Book Events per Asset", "book_count"), 1: ("price_change_count", "Total Price Changes per Asset", "price_change_count"),
            2: ("last_trade_count", "Total Last Trades per Asset", "last_trade_count"), 3: ("tick_change_count", "Total Tick Size Changes per Asset", "tick_change_count"),
            4: ("avg_event_count", "Avg. Event Count per Asset", "avg_event_count"), 5: ("lifetime", "Token Lifetime (ms)", "life_time"),
            6: ("avg_false_misses", "Avg. False Misses per Asset", "avg_false_misses"), # New plot
        }

        for i, ax in enumerate(axes):
            if i in plot_defs:
                key, title, y_col = plot_defs[i]
                if key in reindexed_dfs:
                    df = reindexed_dfs[key]
                    ax.bar(df.index, df[y_col].fillna(0)); ax.set_title(title)
                    ax.set_xlabel("Asset Enumeration"); ax.set_ylabel("Value")
                    ax.grid(True, linestyle='--', alpha=0.6)
                else:
                    ax.text(0.5, 0.5, 'No Data', ha='center', va='center'); ax.set_title(title)
            else:
                ax.set_visible(False)

        save_path = self.plot_dir / "asset_ids.png"
        plt.savefig(save_path); plt.close(fig)

    async def loop(self):
        while True:
            start_dt = datetime(2025, 11, 11, 0, 0, 0, tzinfo=timezone.utc)
            start_time_ms = int(start_dt.timestamp() * 1000)
            
            end_time_ms = int(time.time() * 1000)

            print(f"Generating plots for time range: {start_dt} to {datetime.now(timezone.utc)}...")
            await self.plot_over_time(start_time_ms=start_time_ms, end_time_ms=end_time_ms)
            # Example of ordering by the new metric
            await self.plot_over_asset_ids(order_by=AssetIdOrder.BY_AVG_FALSE_MISSES)
            print("Plots generated successfully.")
            
            await asyncio.sleep(PLOT_INTERVAL_s)

def get_db_config_from_env():
    """Reads database configuration from environment variables."""
    required_vars = ["PG_SOCKET", "POLY_DB", "POLY_DB_CLI", "POLY_DB_CLI_PASS"]
    for var in required_vars:
        if var not in os.environ:
            print(f"FATAL: Environment variable {var} is not set.", file=sys.stderr)
            sys.exit(1)
    
    return {
        "host": os.getenv("PG_SOCKET"),
        "database": os.getenv("POLY_DB"),
        "user": os.getenv("POLY_DB_CLI"),
        "password": os.getenv("POLY_DB_CLI_PASS")
    }

def get_plot_dir_from_env():
    """Constructs the plot directory path from the POLY_PLOTS env var."""
    poly_plots_path = os.getenv("POLY_PLOTS")
    if not poly_plots_path:
        print("FATAL: Environment variable POLY_PLOTS is not set.", file=sys.stderr)
        sys.exit(1)
    
    return Path(poly_plots_path)

async def main():
    db_config = get_db_config_from_env()
    plot_dir = get_plot_dir_from_env()
    db_pool = None
    
    try:
        db_pool = await asyncpg.create_pool(**db_config)
        print("Successfully connected to the database.")
        service = PlottingService(db_pool, plot_dir)
        await service.loop()
    except (asyncpg.exceptions.PostgresError, OSError) as e:
        print(f"Database connection failed: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    finally:
        if db_pool:
            await db_pool.close()
            print("Database connection closed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down plotter.")