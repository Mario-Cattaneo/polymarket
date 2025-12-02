import os
import asyncio
import asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from scipy.stats import skew, kurtosis
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
USE_MOCK_DATA = False

# Base Path Logic
POLY_PLOTS = os.getenv("POLY_PLOTS", "plots") # Default to local 'plots' if env var missing
BASE_OUTPUT_DIR = os.path.join(POLY_PLOTS, "events_agg")

# Time Aggregation
K = 2  # Interval multiplier (e.g., K=2 * 3h = 6h steps)
PARTITION_SIZE_HOURS = 3

# Visual Settings
FIG_WIDTH = 22
SCATTER_ALPHA = 0.3
SCATTER_SIZE = 15
LINE_COLOR = '#D32F2F'

# Table Settings
TABLE_ROW_HEIGHT = 0.035
TABLE_FONT_SIZE = 9
PLOT_HEIGHT_INCHES = 5.0  # Fixed height for the graph part only

# ==========================================
# PER-TABLE CONFIGURATION
# ==========================================
# Note: 'table_name' here is the BASE name. Suffixes ('', '_2') are added dynamically.
PLOTS_CONFIG = {
    "price_changes": {
        "table_name": "agg_price_changes",
        "title_prefix": "Price Changes",
        "metrics": [
            {
                'col': 'total_count', 'title': 'Total Count', 'color': 'blue', 
                'percentiles': [0.50, 0.95, 0.999]
            },
            {
                'col': 'ambiguous_count', 'title': 'Ambiguous', 'color': 'orange', 
                'percentiles': [0.50, 0.90, 0.95]
            },
            {
                'col': 'duplicate_count', 'title': 'Exact Duplicates', 'color': 'green', 
                'percentiles': [0.50, 0.90, 0.95]
            }
        ]
    },
    "books": {
        "table_name": "agg_books",
        "title_prefix": "Order Books",
        "metrics": [
            {
                'col': 'total_count', 'title': 'Total Count', 'color': 'blue', 
                'percentiles': [0.50, 0.90, 0.99]
            },
            {
                'col': 'ambiguous_count', 'title': 'Ambiguous', 'color': 'orange', 
                'percentiles': [0.50, 0.90, 0.95]
            },
            {
                'col': 'duplicate_count', 'title': 'Exact Duplicates', 'color': 'green', 
                'percentiles': [0.50, 0.90, 0.95]
            }
        ]
    },
    "tick_changes": {
        "table_name": "agg_tick_changes",
        "title_prefix": "Tick Changes",
        "metrics": [
            {
                'col': 'total_count', 'title': 'Total Count', 'color': 'blue', 
                'percentiles': [0.50, 0.75, 0.95]
            },
            {
                'col': 'ambiguous_count', 'title': 'Ambiguous', 'color': 'orange', 
                'percentiles': [0.50, 0.90]
            },
            {
                'col': 'duplicate_count', 'title': 'Exact Duplicates', 'color': 'green', 
                'percentiles': [0.50, 0.90]
            }
        ]
    },
    "last_trade_prices": {
        "table_name": "agg_last_trade_prices",
        "title_prefix": "Last Trades",
        "metrics": [
            {
                'col': 'total_count', 'title': 'Total Count', 'color': 'blue', 
                'percentiles': [0.50, 0.95]
            },
            {
                'col': 'duplicate_count', 'title': 'Exact Duplicates', 'color': 'green', 
                'percentiles': [0.50, 0.90]
            }
        ]
    }
}

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def fmt_pct(p):
    val = p * 100
    if val % 1 != 0: return f"{val:.1f}%"
    return f"{int(val)}%"

async def fetch_table_data(table_name):
    print(f"[INFO] Fetching data for {table_name}...")
    required_vars = ["PG_SOCKET", "POLY_PG_PORT", "POLY_DB", "POLY_DB_CLI", "POLY_DB_CLI_PASS"]
    if any(not os.getenv(v) for v in required_vars):
        print(f"[ERROR] Missing environment variables.")
        return pd.DataFrame()

    try:
        conn = await asyncpg.connect(
            host=os.getenv("PG_SOCKET"),
            port=os.getenv("POLY_PG_PORT"),
            database=os.getenv("POLY_DB"),
            user=os.getenv("POLY_DB_CLI"),
            password=os.getenv("POLY_DB_CLI_PASS")
        )
        # Check if table exists first to avoid crash
        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = $1)", 
            table_name
        )
        if not exists:
            print(f"[WARN] Table {table_name} does not exist in DB.")
            await conn.close()
            return pd.DataFrame()

        query = f"SELECT * FROM {table_name} ORDER BY partition_start_time_ms ASC"
        rows = await conn.fetch(query)
        await conn.close()
        
        if not rows:
            print(f"[WARN] No data found in {table_name}.")
            return pd.DataFrame()

        data = [dict(row) for row in rows]
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['partition_start_time_ms'].astype(float), unit='ms')
        return df
    except Exception as e:
        print(f"[FATAL] Database error on {table_name}: {e}")
        return pd.DataFrame()

def generate_mock_data():
    print("[INFO] Generating mock data...")
    dates = pd.date_range(start='2025-11-20', periods=15, freq='3h')
    data = []
    for d in dates:
        for i in range(50):
            asset_id = f'asset_{i}'
            total = int(np.random.lognormal(mean=4, sigma=1)) 
            data.append({
                'partition_start_time_ms': int(d.timestamp() * 1000),
                'timestamp': d,
                'asset_id': asset_id,
                'total_count': total,
                'ambiguous_count': int(total * 0.1) if i % 2 == 0 else 0,
                'duplicate_count': int(total * 0.02)
            })
    return pd.DataFrame(data)

# ==========================================
# DRAWING LOGIC
# ==========================================

def draw_metric_and_table(ax, df_resampled, metric_col, metric_title, color, percentiles, title_prefix, step_width_days, all_ticks_pydatetime):
    """
    Draws a single metric plot and its corresponding data table.
    Returns the number of rows in the table (for height calculations).
    """
    grouped_steps = df_resampled.groupby('timestamp')
    percentiles_100 = [p * 100 for p in percentiles]
    
    table_data = []
    step_counter = 1
    max_y_in_plot = 0
    
    # Track min/max X for tight limits
    x_limit_min = None
    x_limit_max = None

    for time_start, group in grouped_steps:
        values = group[metric_col].values
        if len(values) == 0: 
            step_counter += 1
            continue

        # Coordinates
        ts_pydatetime = pd.Timestamp(time_start).to_pydatetime()
        x_start = mdates.date2num(ts_pydatetime)
        x_end = x_start + step_width_days
        x_center = x_start + (step_width_days / 2)
        
        # Update Limits
        if x_limit_min is None or x_start < x_limit_min: x_limit_min = x_start
        if x_limit_max is None or x_end > x_limit_max: x_limit_max = x_end

        # 1. Scatter
        active_values = values[values > 0]
        if len(active_values) > 0:
            ax.scatter([x_center] * len(active_values), active_values, 
                       alpha=SCATTER_ALPHA, s=SCATTER_SIZE, color=color, zorder=2)

        # 2. Percentiles
        if len(active_values) > 0:
            stats = np.percentile(active_values, percentiles_100)
            for stat in stats:
                ax.hlines(y=stat, xmin=x_start, xmax=x_end, 
                          colors=LINE_COLOR, linestyles='-', linewidth=1.5, zorder=3)

        # 3. Grid & Borders
        ax.axvline(x=x_start, color='gray', linestyle='-', linewidth=0.5, alpha=0.3)
        ax.axvline(x=x_end, color='gray', linestyle='-', linewidth=0.5, alpha=0.3)

        # 4. Step Index Label
        trans = ax.get_xaxis_transform()
        ax.text(x_center, 1.01, str(step_counter), 
                transform=trans, ha='center', va='bottom', 
                fontsize=10, fontweight='bold', color='#555555')

        # 5. Calculate Stats
        if len(active_values) > 0:
            s_sum = np.sum(active_values)
            s_n = len(active_values)
            s_mean = np.mean(active_values)
            s_std = np.std(active_values)
            s_skew = skew(active_values) if len(active_values) > 1 else 0
            s_kurt = kurtosis(active_values, fisher=True) if len(active_values) > 1 else 0
        else:
            s_sum = s_n = s_mean = s_std = s_skew = s_kurt = 0

        # Append to table
        time_str = ts_pydatetime.strftime('%m-%d %H:%M')
        table_data.append([
            step_counter,
            time_str,
            f"{s_sum:,}",
            f"{s_n:,}",
            f"{s_mean:.1f}",
            f"{s_std:.1f}",
            f"{s_skew:.2f}",
            f"{s_kurt:.2f}"
        ])

        if len(values) > 0 and values.max() > max_y_in_plot: max_y_in_plot = values.max()
        step_counter += 1

    # --- FIX: REMOVE EMPTY STEPS ---
    if x_limit_min is not None and x_limit_max is not None:
        ax.set_xlim(x_limit_min, x_limit_max)

    # Formatting
    ax.set_title(f"{title_prefix} - {metric_title}", fontsize=14, fontweight='bold', pad=15)
    ax.set_ylabel("Count", fontsize=12)
    ax.grid(True, axis='y', linestyle='--', alpha=0.3)
    
    # X-Axis
    ax.set_xticks([mdates.date2num(t) for t in all_ticks_pydatetime])
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.setp(ax.get_xticklabels(), rotation=30, ha='right', fontsize=9)
    
    # Legend
    from matplotlib.lines import Line2D
    custom_lines = [Line2D([0], [0], color=LINE_COLOR, lw=2)]
    p_labels = ", ".join([fmt_pct(p) for p in percentiles])
    ax.legend(custom_lines, [f'Percentiles ({p_labels})'], loc='upper right', fontsize=10)

    # 6. Draw Table (Compact)
    if table_data:
        col_labels = ["Step", "Time (UTC)", "Sum (Σ)", "Assets (N)", "Mean (μ)", "Std (σ)", "Skew (γ₁)", "Kurt (κ)"]
        
        # Calculate exact height needed
        total_rows = len(table_data) + 1
        total_table_height_bbox = total_rows * TABLE_ROW_HEIGHT
        
        offset_from_axis = 0.15 
        
        the_table = ax.table(
            cellText=table_data,
            colLabels=col_labels,
            loc='bottom',
            cellLoc='center',
            bbox=[0.0, -(offset_from_axis + total_table_height_bbox), 1.0, total_table_height_bbox]
        )
        
        the_table.auto_set_font_size(False)
        the_table.set_fontsize(TABLE_FONT_SIZE)
        
        for r in range(len(table_data)):
            color = "#f2f2f2" if r % 2 == 0 else "#ffffff"
            for c in range(len(col_labels)):
                the_table[r+1, c].set_facecolor(color)
                
        return total_rows
    return 0

# ==========================================
# MAIN PROCESSING LOGIC
# ==========================================

def process_table(key, config, df, save_dir, actual_table_name):
    if df.empty: return

    print(f"[INFO] Processing {actual_table_name} -> {save_dir}")
    
    interval_str = f'{K * PARTITION_SIZE_HOURS}h'
    all_cols = [m['col'] for m in config['metrics']]
    
    # Aggregate
    df_resampled = df.groupby(
        [pd.Grouper(key='timestamp', freq=interval_str), 'asset_id']
    )[all_cols].sum().reset_index()

    # Common Vars
    step_hours = K * PARTITION_SIZE_HOURS
    step_width_days = step_hours / 24.0
    unique_starts = np.sort(df_resampled['timestamp'].unique())
    
    if len(unique_starts) > 0:
        last_end = unique_starts[-1] + pd.Timedelta(hours=step_hours)
        all_ticks_pd = np.append(unique_starts, last_end)
        all_ticks_pydatetime = [pd.Timestamp(t).to_pydatetime() for t in all_ticks_pd]
    else:
        all_ticks_pydatetime = []

    # ---------------------------------------------------------
    # 1. GENERATE INDIVIDUAL PLOTS
    # ---------------------------------------------------------
    master_plot_items = []

    for metric_cfg in config['metrics']:
        
        # Store for Master
        master_plot_items.append({
            'df': df_resampled,
            'cfg': metric_cfg,
            'prefix': config['title_prefix'],
            'width': step_width_days,
            'ticks': all_ticks_pydatetime
        })

        # Calculate height for individual plot
        num_rows = len(unique_starts) + 1
        table_height_inches = num_rows * 0.25
        total_fig_height = PLOT_HEIGHT_INCHES + table_height_inches + 1

        fig = plt.figure(figsize=(FIG_WIDTH, total_fig_height))
        ax = fig.add_subplot(111)
        
        draw_metric_and_table(
            ax, df_resampled, metric_cfg['col'], metric_cfg['title'], metric_cfg['color'], 
            metric_cfg['percentiles'], config['title_prefix'], 
            step_width_days, all_ticks_pydatetime
        )
        
        # Adjust bottom margin
        bottom_margin = (table_height_inches + 0.5) / total_fig_height
        plt.subplots_adjust(bottom=bottom_margin)
        
        filename = f"{actual_table_name}_{metric_cfg['col']}.png"
        save_path = os.path.join(save_dir, filename)
        plt.savefig(save_path, dpi=300)
        print(f"   -> Saved: {filename}")
        plt.close(fig)

    # ---------------------------------------------------------
    # 2. GENERATE MASTER PLOT (Stacked) FOR THIS TABLE
    # ---------------------------------------------------------
    if not master_plot_items: return

    # Calculate Total Height & Spacing
    num_rows = len(unique_starts) + 1
    table_height_inches = num_rows * 0.25
    gap_inches = table_height_inches + 1.0
    
    num_metrics = len(master_plot_items)
    total_master_height = (PLOT_HEIGHT_INCHES + gap_inches) * num_metrics
    
    fig = plt.figure(figsize=(FIG_WIDTH, total_master_height))
    axes = fig.subplots(num_metrics, 1)
    if num_metrics == 1: axes = [axes]

    for i, item in enumerate(master_plot_items):
        draw_metric_and_table(
            axes[i], item['df'], item['cfg']['col'], item['cfg']['title'], item['cfg']['color'],
            item['cfg']['percentiles'], item['prefix'], item['width'], item['ticks']
        )

    hspace_ratio = gap_inches / PLOT_HEIGHT_INCHES
    plt.subplots_adjust(hspace=hspace_ratio, bottom=0.05, top=0.98)

    master_filename = f"MASTER_{actual_table_name}.png"
    master_path = os.path.join(save_dir, master_filename)
    plt.savefig(master_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    print(f"[INIT] Base Output Directory: {BASE_OUTPUT_DIR}")

    # Define the two versions we want to process
    versions = [
        {"suffix": "",   "subdir": "1"}, # Original tables -> /1/
        {"suffix": "_2", "subdir": "2"}  # V2 tables -> /2/
    ]

    for ver in versions:
        suffix = ver["suffix"]
        subdir = ver["subdir"]
        
        # Create specific directory: $POLY_PLOTS/events_agg/1/ or /2/
        current_save_dir = os.path.join(BASE_OUTPUT_DIR, subdir)
        os.makedirs(current_save_dir, exist_ok=True)
        
        print(f"\n==================================================")
        print(f" PROCESSING VERSION: '{subdir}' (Suffix: '{suffix}')")
        print(f" Saving to: {current_save_dir}")
        print(f"==================================================")

        # 1. Fetch Data for this version
        all_data = {}
        for key, config in PLOTS_CONFIG.items():
            # Construct actual table name (e.g., agg_price_changes_2)
            actual_table_name = config['table_name'] + suffix
            
            if USE_MOCK_DATA:
                all_data[key] = generate_mock_data()
            else:
                try:
                    all_data[key] = loop.run_until_complete(fetch_table_data(actual_table_name))
                except Exception as e:
                    print(f"[ERROR] Failed to fetch {actual_table_name}: {e}")
                    all_data[key] = pd.DataFrame()

        # 2. Process Tables & Prepare Global Master for this version
        version_master_items = []

        for key, config in PLOTS_CONFIG.items():
            df = all_data[key]
            if df.empty: continue
            
            actual_table_name = config['table_name'] + suffix
            
            # Process individual table (generates individual plots + table master)
            process_table(key, config, df, current_save_dir, actual_table_name)
            
            # Prepare for GLOBAL MASTER (All tables in this version combined)
            interval_str = f'{K * PARTITION_SIZE_HOURS}h'
            all_cols = [m['col'] for m in config['metrics']]
            df_resampled = df.groupby([pd.Grouper(key='timestamp', freq=interval_str), 'asset_id'])[all_cols].sum().reset_index()
            
            step_hours = K * PARTITION_SIZE_HOURS
            step_width_days = step_hours / 24.0
            unique_starts = np.sort(df_resampled['timestamp'].unique())
            if len(unique_starts) > 0:
                last_end = unique_starts[-1] + pd.Timedelta(hours=step_hours)
                all_ticks_pd = np.append(unique_starts, last_end)
                all_ticks_pydatetime = [pd.Timestamp(t).to_pydatetime() for t in all_ticks_pd]
            else:
                all_ticks_pydatetime = []

            for metric_cfg in config['metrics']:
                version_master_items.append({
                    'df': df_resampled,
                    'cfg': metric_cfg,
                    'prefix': config['title_prefix'],
                    'width': step_width_days,
                    'ticks': all_ticks_pydatetime,
                    'rows': len(unique_starts) + 1
                })

        # 3. Generate GLOBAL MASTER Plot for this version
        if version_master_items:
            print(f"[INFO] Generating GLOBAL MASTER for Version {subdir}...")
            
            max_rows = max(item['rows'] for item in version_master_items)
            table_height_inches = max_rows * 0.25
            gap_inches = table_height_inches + 1.0
            total_height = (PLOT_HEIGHT_INCHES + gap_inches) * len(version_master_items)

            fig = plt.figure(figsize=(FIG_WIDTH, total_height))
            axes = fig.subplots(len(version_master_items), 1)

            for i, item in enumerate(version_master_items):
                draw_metric_and_table(
                    axes[i], item['df'], item['cfg']['col'], item['cfg']['title'], item['cfg']['color'],
                    item['cfg']['percentiles'], item['prefix'], item['width'], item['ticks']
                )

            hspace_ratio = gap_inches / PLOT_HEIGHT_INCHES
            plt.subplots_adjust(hspace=hspace_ratio, bottom=0.02, top=0.99)

            master_filename = "ALL_METRICS_MASTER.png"
            master_path = os.path.join(current_save_dir, master_filename)
            plt.savefig(master_path, dpi=300, bbox_inches='tight')
            print(f"[SUCCESS] Saved Global Master to: {master_path}")
            plt.close(fig)

if __name__ == "__main__":
    main()