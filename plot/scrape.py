import os
import sys
import asyncio
import time
import logging
from pathlib import Path
from enum import Enum
from datetime import datetime, timezone, timedelta

import asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# --- Configuration ---
PLOT_INTERVAL_s = 60
DEFAULT_SAMPLE_SIZE = 5000

# --- Time Window Configuration ---
# Set a specific UTC datetime to use a fixed start/end time for the analysis window.
# Set ANALYSIS_END_TIME to None to use the current time ("now") on each loop.
# Format: datetime(YYYY, M, D, H, M, tzinfo=timezone.utc)
ANALYSIS_START_TIME = datetime(2025, 11, 12, 18, 30, tzinfo=timezone.utc)
ANALYSIS_END_TIME = None  # Use None for "now"


# --- Asset ID Ordering Enum ---
class AssetIdOrder(Enum):
    BY_BOOK_COUNT = 1
    BY_PRICE_CHANGE_COUNT = 2
    BY_LAST_TRADE_COUNT = 3
    BY_TICK_SIZE_CHANGE_COUNT = 4
    BY_AVERAGE_EVENT_COUNT = 5
    BY_LIFETIME = 6
    BY_AVG_FALSE_MISSES = 7

# --- Database Query Storage ---
class DatabaseFetcher:
    """
    Encapsulates all SQL queries and the logic for fetching data from the database.
    Includes performance logging and optional query plan analysis in __debug__ mode.
    """
    # --- REVISED, HIGH-PERFORMANCE TIME-SERIES QUERIES ---
    _CDF_TEMPLATE = """
        WITH buckets AS (
            SELECT ( {time_column} / 1000) * 1000 AS bucket_start_ms, COUNT(*) AS events_in_bucket
            FROM {table}
            WHERE {time_column} BETWEEN $2 AND $3 {extra_filters}
            GROUP BY 1
        ),
        cumulative AS (
            SELECT bucket_start_ms, SUM(events_in_bucket) OVER (ORDER BY bucket_start_ms ASC) AS cdf_value
            FROM buckets
        ),
        sampled_cdf AS (
            SELECT bucket_start_ms, cdf_value, ntile($1) OVER (ORDER BY bucket_start_ms) as tile
            FROM cumulative
        )
        SELECT MAX(bucket_start_ms) AS ts, MAX(cdf_value) AS v
        FROM sampled_cdf GROUP BY tile ORDER BY tile;
    """

    BOOKS_CDF = _CDF_TEMPLATE.format(table='books', time_column='server_time', extra_filters='')
    PRICE_CHANGES_CDF = _CDF_TEMPLATE.format(table='price_changes', time_column='server_time', extra_filters='')
    LAST_TRADE_PRICES_CDF = _CDF_TEMPLATE.format(table='last_trade_prices', time_column='server_time', extra_filters='')
    TICK_CHANGES_CDF = _CDF_TEMPLATE.format(table='tick_changes', time_column='server_time', extra_filters='')
    FOUND_MARKETS_CDF = _CDF_TEMPLATE.format(table='markets', time_column='found_time', extra_filters='')
    CLOSED_MARKETS_CDF = _CDF_TEMPLATE.format(table='markets', time_column='closed_time', extra_filters='AND closed_time IS NOT NULL')
    GOOD_CONNECTIONS_CDF = _CDF_TEMPLATE.format(table='events_connections', time_column='timestamp_ms', extra_filters='AND success = TRUE')
    BAD_CONNECTIONS_CDF = _CDF_TEMPLATE.format(table='events_connections', time_column='timestamp_ms', extra_filters='AND success = FALSE')

    EVENT_COUNT_CDF = """
        WITH all_events AS (
            SELECT server_time FROM books WHERE server_time BETWEEN $2 AND $3
            UNION ALL SELECT server_time FROM price_changes WHERE server_time BETWEEN $2 AND $3
            UNION ALL SELECT server_time FROM last_trade_prices WHERE server_time BETWEEN $2 AND $3
        ),
        buckets AS (
            SELECT (server_time / 1000) * 1000 AS bucket_start_ms, COUNT(*) AS events_in_bucket
            FROM all_events GROUP BY 1
        ),
        cumulative AS (
            SELECT bucket_start_ms, SUM(events_in_bucket) OVER (ORDER BY bucket_start_ms ASC) AS cdf_value
            FROM buckets
        ),
        sampled_cdf AS (
            SELECT bucket_start_ms, cdf_value, ntile($1) OVER (ORDER BY bucket_start_ms) as tile
            FROM cumulative
        )
        SELECT MAX(bucket_start_ms) AS ts, MAX(cdf_value) AS v
        FROM sampled_cdf GROUP BY tile ORDER BY tile;
    """

    # --- Other Time-Series Queries (Unchanged) ---
    FALSE_MISSES_PDF = "SELECT (server_time / $1) * $1 AS ts, AVG(false_misses) AS v FROM server_book WHERE server_time BETWEEN $2 AND $3 GROUP BY 1 ORDER BY 1;"
    EXHAUSTION_CYCLE_CDF = "SELECT (found_time / $1) * $1 AS ts, MAX(exhaustion_cycle) AS v FROM markets WHERE found_time BETWEEN $2 AND $3 GROUP BY 1 ORDER BY 1;"
    MARKETS_RTT = "SELECT (timestamp_ms / $1) * $1 AS ts, AVG(rtt) AS v FROM markets_rtt WHERE timestamp_ms BETWEEN $2 AND $3 GROUP BY 1 ORDER BY 1;"
    ANALYTICS_RTT = "SELECT (timestamp_ms / $1) * $1 AS ts, AVG(rtt) AS v FROM analytics_rtt WHERE timestamp_ms BETWEEN $2 AND $3 GROUP BY 1 ORDER BY 1;"

    # --- Per-Asset Queries ---
    ASSET_BOOK_COUNT = "SELECT asset_id, COUNT(*) AS v FROM books WHERE server_time BETWEEN $1 AND $2 GROUP BY asset_id ORDER BY v ASC;"
    ASSET_PRICE_CHANGE_COUNT = "SELECT asset_id, COUNT(*) AS v FROM price_changes WHERE server_time BETWEEN $1 AND $2 GROUP BY asset_id ORDER BY v ASC;"
    ASSET_LAST_TRADE_COUNT = "SELECT asset_id, COUNT(*) AS v FROM last_trade_prices WHERE server_time BETWEEN $1 AND $2 GROUP BY asset_id ORDER BY v ASC;"
    ASSET_TICK_CHANGE_COUNT = "SELECT asset_id, COUNT(*) AS v FROM tick_changes WHERE server_time BETWEEN $1 AND $2 GROUP BY asset_id ORDER BY v ASC;"
    AVERAGE_FALSE_MISSES_PER_ASSET = "SELECT asset_id, AVG(false_misses) AS v FROM server_book WHERE server_time BETWEEN $1 AND $2 GROUP BY asset_id ORDER BY v ASC;"
    TOKEN_LIFETIME = "SELECT asset_id, (closed_time - found_time) AS v FROM markets WHERE closed_time IS NOT NULL AND found_time BETWEEN $1 AND $2 ORDER BY v ASC;"
    AVERAGE_EVENT_COUNT_PER_ASSET = """
        WITH all_events AS (
            SELECT asset_id FROM books WHERE server_time BETWEEN $1 AND $2
            UNION ALL SELECT asset_id FROM price_changes WHERE server_time BETWEEN $1 AND $2
            UNION ALL SELECT asset_id FROM last_trade_prices WHERE server_time BETWEEN $1 AND $2
        )
        SELECT asset_id, COUNT(*) AS v FROM all_events GROUP BY asset_id ORDER BY v ASC;
    """

    @staticmethod
    async def _run_query(pool, query, args, timeout=30.0):
        start = time.perf_counter()
        try:
            if __debug__:
                logging.debug(f"Executing query with args: {args}")
            result = await pool.fetch(query, *args, timeout=timeout)
            elapsed = time.perf_counter() - start
            logging.info(f"Query finished in {elapsed:.3f}s, returned {len(result)} rows.")

            if __debug__:
                try:
                    explain_q = f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE) {query}"
                    plan = await pool.fetch(explain_q, *args, timeout=timeout)
                    plan_output = "\n".join(r[0] for r in plan)
                    logging.debug(f"[QUERY PLAN]\n{plan_output}")
                except Exception as e:
                    logging.warning(f"EXPLAIN ANALYZE failed: {e}")
            return result
        except Exception:
            logging.error(f"Query failed! Query: {query} Args: {args}", exc_info=True)
            return []

    @classmethod
    async def fetch_time_series_data(cls, pool, query, sample_or_bucket_size, start_time_ms, end_time_ms):
        args = (sample_or_bucket_size, start_time_ms, end_time_ms)
        return await cls._run_query(pool, query, args)

    @classmethod
    async def fetch_asset_ordered_data(cls, pool, query, start_time_ms, end_time_ms):
        args = (start_time_ms, end_time_ms)
        return await cls._run_query(pool, query, args)


class PlottingService:
    def __init__(self, pool, plot_dir):
        self.pool = pool
        self.fetcher = DatabaseFetcher()
        self.plot_dir = Path(plot_dir)
        self.plot_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Plots will be saved in: {self.plot_dir.resolve()}")

    async def plot_over_time(self, start_time_ms, end_time_ms, sample_size=DEFAULT_SAMPLE_SIZE):
        interval_size = end_time_ms - start_time_ms
        if interval_size <= 0:
            logging.warning("Invalid time interval for plotting, skipping.")
            return
        bucket_size_ms = max(1, (interval_size + sample_size - 1) // sample_size)

        tasks = {
            "books": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.BOOKS_CDF, sample_size, start_time_ms, end_time_ms),
            "price_changes": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.PRICE_CHANGES_CDF, sample_size, start_time_ms, end_time_ms),
            "last_trades": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.LAST_TRADE_PRICES_CDF, sample_size, start_time_ms, end_time_ms),
            "tick_changes": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.TICK_CHANGES_CDF, sample_size, start_time_ms, end_time_ms),
            "found_markets": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.FOUND_MARKETS_CDF, sample_size, start_time_ms, end_time_ms),
            "closed_markets": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.CLOSED_MARKETS_CDF, sample_size, start_time_ms, end_time_ms),
            "good_connections": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.GOOD_CONNECTIONS_CDF, sample_size, start_time_ms, end_time_ms),
            "bad_connections": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.BAD_CONNECTIONS_CDF, sample_size, start_time_ms, end_time_ms),
            "event_count_cdf": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.EVENT_COUNT_CDF, sample_size, start_time_ms, end_time_ms),
            "false_misses_pdf": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.FALSE_MISSES_PDF, bucket_size_ms, start_time_ms, end_time_ms),
            "exhaustion_cycles": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.EXHAUSTION_CYCLE_CDF, bucket_size_ms, start_time_ms, end_time_ms),
            "markets_rtt": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.MARKETS_RTT, bucket_size_ms, start_time_ms, end_time_ms),
            "analytics_rtt": self.fetcher.fetch_time_series_data(self.pool, self.fetcher.ANALYTICS_RTT, bucket_size_ms, start_time_ms, end_time_ms),
        }

        results = await asyncio.gather(*tasks.values())
        data_map = dict(zip(tasks.keys(), results))

        plot_defs = [
            ("found_markets", "CDF of Markets Found"), ("closed_markets", "CDF of Markets Closed"),
            ("books", "CDF of Book Events"), ("price_changes", "CDF of Price Change Events"),
            ("last_trades", "CDF of Last Trade Events"), ("tick_changes", "CDF of Tick Size Change Events"),
            ("event_count_cdf", "CDF of All Events"), ("false_misses_pdf", "Avg False Misses (PDF)"),
            ("good_connections", "CDF of Good Connections"), ("bad_connections", "CDF of Bad Connections"),
            ("exhaustion_cycles", "Max Exhaustion Cycle"), ("markets_rtt", "Markets RTT (ms)"),
            ("analytics_rtt", "Analytics RTT (ms)"),
        ]

        fig, axes = plt.subplots(7, 2, figsize=(20, 35), constrained_layout=True)
        axes = axes.flatten()
        
        start_dt = datetime.fromtimestamp(start_time_ms/1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_time_ms/1000, tz=timezone.utc)
        fig.suptitle(f'Time-Series Analysis: {start_dt.strftime("%Y-%m-%d %H:%M")} to {end_dt.strftime("%Y-%m-%d %H:%M %Z")}', fontsize=16)

        for i, ax in enumerate(axes):
            if i >= len(plot_defs):
                ax.set_visible(False)
                continue

            key, title = plot_defs[i]
            raw = data_map.get(key)
            if not raw:
                ax.text(0.5, 0.5, "No Data or Query Failed", ha="center", va="center")
            else:
                df = pd.DataFrame(raw, columns=["ts", "v"])
                df["time"] = pd.to_datetime(df["ts"], unit="ms")
                ax.plot(df["time"], df["v"], drawstyle="steps-post")
                ax.grid(True, linestyle="--", alpha=0.6)
                locator = mdates.AutoDateLocator()
                formatter = mdates.AutoDateFormatter(locator)
                ax.xaxis.set_major_locator(locator)
                ax.xaxis.set_major_formatter(formatter)
                plt.setp(ax.get_xticklabels(), rotation=30, ha="right")
            
            ax.set_xlim(start_dt, end_dt)
            ax.set_title(title)

        save_path = self.plot_dir / "time_series.png"
        plt.savefig(save_path)
        plt.close(fig)
        logging.info(f"Time-series plot saved to {save_path}")

    async def plot_over_asset_ids(self, start_time_ms, end_time_ms, order_by=AssetIdOrder.BY_BOOK_COUNT):
        query_map = {
            "book_count": self.fetcher.ASSET_BOOK_COUNT,
            "price_change_count": self.fetcher.ASSET_PRICE_CHANGE_COUNT,
            "last_trade_count": self.fetcher.ASSET_LAST_TRADE_COUNT,
            "tick_change_count": self.fetcher.ASSET_TICK_CHANGE_COUNT,
            "avg_false_misses": self.fetcher.AVERAGE_FALSE_MISSES_PER_ASSET,
            "lifetime": self.fetcher.TOKEN_LIFETIME,
            "avg_event_count": self.fetcher.AVERAGE_EVENT_COUNT_PER_ASSET,
        }

        tasks = {k: self.fetcher.fetch_asset_ordered_data(self.pool, q, start_time_ms, end_time_ms) for k, q in query_map.items()}
        results = await asyncio.gather(*tasks.values())

        dfs = {key: pd.DataFrame(res, columns=["asset_id", key]) if res else pd.DataFrame(columns=["asset_id", key]) for key, res in zip(tasks.keys(), results)}

        primary_key_map = {
            AssetIdOrder.BY_BOOK_COUNT: "book_count",
            AssetIdOrder.BY_PRICE_CHANGE_COUNT: "price_change_count",
            AssetIdOrder.BY_LAST_TRADE_COUNT: "last_trade_count",
            AssetIdOrder.BY_TICK_SIZE_CHANGE_COUNT: "tick_change_count",
            AssetIdOrder.BY_AVG_FALSE_MISSES: "avg_false_misses",
            AssetIdOrder.BY_LIFETIME: "lifetime",
            AssetIdOrder.BY_AVERAGE_EVENT_COUNT: "avg_event_count",
        }
        
        # --- NEW: Resilient Fallback Logic ---
        
        # 1. Determine the primary key from the user's choice
        chosen_primary_key = primary_key_map[order_by]
        effective_primary_key = chosen_primary_key
        ordering_succeeded = not dfs[chosen_primary_key].empty

        # 2. If the chosen primary key failed, try to find a fallback
        if not ordering_succeeded:
            logging.warning(f"Primary ordering key '{chosen_primary_key}' failed. Attempting to find a fallback.")
            fallback_keys = [
                "avg_event_count", "book_count", "price_change_count", 
                "last_trade_count", "tick_change_count", "lifetime"
            ]
            for key in fallback_keys:
                if not dfs[key].empty:
                    effective_primary_key = key
                    ordering_succeeded = True
                    logging.info(f"Fallback successful. Now ordering by '{effective_primary_key}'.")
                    break
        
        # 3. Prepare for plotting
        fig, axes = plt.subplots(4, 2, figsize=(20, 25), constrained_layout=True)
        axes = axes.flatten()
        
        start_dt = datetime.fromtimestamp(start_time_ms/1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_time_ms/1000, tz=timezone.utc)
        
        title_suffix = f"(Ordered by {effective_primary_key})"
        if effective_primary_key != chosen_primary_key:
            title_suffix = f"(Ordered by FALLBACK: {effective_primary_key})"
            
        fig.suptitle(f'Asset ID Analysis {title_suffix}', fontsize=16)

        # 4. Re-index dataframes if ordering was successful
        if ordering_succeeded:
            ordered_assets = dfs[effective_primary_key]["asset_id"].tolist()
            for key, df in dfs.items():
                if not df.empty:
                    dfs[key] = df.set_index("asset_id").reindex(ordered_assets).reset_index()
        else:
            logging.error("All fallback ordering keys failed. Unable to order asset plots.")

        plot_defs = [
            ("book_count", "Book Events per Asset"), ("price_change_count", "Price Changes per Asset"),
            ("last_trade_count", "Last Trades per Asset"), ("tick_change_count", "Tick Changes per Asset"),
            ("avg_false_misses", "Avg False Misses per Asset"), ("lifetime", "Token Lifetime (ms)"),
            ("avg_event_count", "Total Events per Asset"),
        ]

        # 5. Render each subplot
        for i, ax in enumerate(axes):
            if i >= len(plot_defs):
                ax.set_visible(False)
                continue

            key, title = plot_defs[i]
            ax.set_title(title)
            
            if not ordering_succeeded:
                ax.text(0.5, 0.5, "Ordering Failed", ha="center", va="center")
                continue

            df = dfs.get(key)
            if df is None or df.empty:
                # This handles failures of non-primary queries
                ax.text(0.5, 0.5, "No Data or Query Failed", ha="center", va="center")
            else:
                ax.bar(df.index, df[key].fillna(0))
                ax.set_xlabel("Asset Enumeration (Sorted)")
                ax.grid(True, linestyle="--", alpha=0.6)

        save_path = self.plot_dir / "asset_ids.png"
        plt.savefig(save_path)
        plt.close(fig)
        logging.info(f"Asset ID plot saved to {save_path}")

    async def loop(self):
        while True:
            start_dt = ANALYSIS_START_TIME
            end_dt = ANALYSIS_END_TIME if ANALYSIS_END_TIME is not None else datetime.now(timezone.utc)

            if not start_dt:
                logging.error("ANALYSIS_START_TIME is not set. Please configure it at the top of the script.")
                await asyncio.sleep(PLOT_INTERVAL_s)
                continue

            if start_dt >= end_dt:
                logging.warning(f"Start time {start_dt} is at or after end time {end_dt}. Skipping plot generation.")
                await asyncio.sleep(PLOT_INTERVAL_s)
                continue
            
            start_ms = int(start_dt.timestamp() * 1000)
            end_ms = int(end_dt.timestamp() * 1000)

            logging.info(f"--- Generating plots for time range: {start_dt} to {end_dt} ---")
            
            # Both functions now use the same, full time range
            await self.plot_over_time(start_time_ms=start_ms, end_time_ms=end_ms)
            await self.plot_over_asset_ids(start_time_ms=start_ms, end_time_ms=end_ms, order_by=AssetIdOrder.BY_AVG_FALSE_MISSES)
            
            logging.info(f"--- Plot generation complete. Waiting for {PLOT_INTERVAL_s} seconds. ---")
            await asyncio.sleep(PLOT_INTERVAL_s)


def get_db_config_from_env():
    required = ["PG_SOCKET", "POLY_DB", "POLY_DB_CLI", "POLY_DB_CLI_PASS"]
    config = {}
    for var in required:
        value = os.getenv(var)
        if not value:
            logging.critical(f"FATAL: Environment variable {var} is not set.")
            sys.exit(1)
        config[var] = value
    return {
        "host": config["PG_SOCKET"],
        "database": config["POLY_DB"],
        "user": config["POLY_DB_CLI"],
        "password": config["POLY_DB_CLI_PASS"]
    }


def get_plot_dir_from_env():
    p = os.getenv("POLY_PLOTS")
    if not p:
        logging.critical("FATAL: POLY_PLOTS environment variable not set.")
        sys.exit(1)
    return Path(p)


async def main():
    if not __debug__:
        logging.info("Running in production mode. Set PYTHONOPTIMIZE=0 or run as `python script.py` for debug logs.")
    else:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Running in __debug__ mode. Query plans will be logged.")

    db_config = get_db_config_from_env()
    plot_dir = get_plot_dir_from_env()
    db_pool = None
    try:
        db_pool = await asyncpg.create_pool(**db_config, min_size=1, max_size=4)
        logging.info("Successfully connected to the database.")
        service = PlottingService(db_pool, plot_dir)
        await service.loop()
    except (asyncpg.PostgresError, OSError) as e:
        logging.critical(f"Database connection error: {e}", exc_info=True)
        sys.exit(1)
    except Exception:
        logging.critical("An unexpected error occurred in main loop.", exc_info=True)
    finally:
        if db_pool:
            await db_pool.close()
            logging.info("Database connection pool closed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutdown requested by user.")