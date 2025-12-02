import os
import asyncio
import asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as patches
import numpy as np
from scipy.stats import skew, kurtosis
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
USE_MOCK_DATA = False

# Base Path Logic
POLY_PLOTS = os.getenv("POLY_PLOTS", "plots")
BASE_OUTPUT_DIR = os.path.join(POLY_PLOTS, "miss_counts")

# Time Aggregation
K = 2  # Interval multiplier (e.g., K=2 * 3h = 6h steps)
PARTITION_SIZE_HOURS = 3

# Visual Settings
FIG_WIDTH = 22
SCATTER_ALPHA = 0.3
SCATTER_SIZE = 15
LINE_COLOR = '#D32F2F' # Red for Misses
CDF_COLOR = '#1976D2'

# Table Settings
TABLE_ROW_HEIGHT = 0.035
TABLE_FONT_SIZE = 9
PLOT_HEIGHT_INCHES = 5.0

# ==========================================
# V2 SPECIFIC CONFIGURATION
# ==========================================
V2_METRIC_CONFIG = {
    'col': 'false_misses', 
    'title': 'False Misses Distribution (>0)', 
    'color': '#D32F2F', 
    'percentiles': [0.50, 0.90, 0.99, 0.999]
}

# ==========================================
# DATA FETCHING
# ==========================================

async def fetch_data_for_fork(raw_table, agg_count_table):
    print(f"[INFO] Fetching data for {raw_table} & {agg_count_table}...")
    
    if USE_MOCK_DATA:
        return generate_mock_data()

    required_vars = ["PG_SOCKET", "POLY_PG_PORT", "POLY_DB", "POLY_DB_CLI", "POLY_DB_CLI_PASS"]
    if any(not os.getenv(v) for v in required_vars):
        print(f"[ERROR] Missing environment variables.")
        return None, None

    try:
        conn = await asyncpg.connect(
            host=os.getenv("PG_SOCKET"),
            port=os.getenv("POLY_PG_PORT"),
            database=os.getenv("POLY_DB"),
            user=os.getenv("POLY_DB_CLI"),
            password=os.getenv("POLY_DB_CLI_PASS")
        )

        # 1. RAW MISS DATA
        q_raw = f"""
            SELECT found_time_ms, false_misses
            FROM {raw_table} 
            WHERE false_misses > 0
            ORDER BY found_time_ms ASC
        """
        rows_raw = await conn.fetch(q_raw)
        df_raw = pd.DataFrame([dict(r) for r in rows_raw])
        if not df_raw.empty:
            df_raw['timestamp'] = pd.to_datetime(df_raw['found_time_ms'].astype(float), unit='ms')

        # 2. META DATA (Total Rows per Partition)
        q_meta = f"""
            SELECT 
                partition_index,
                partition_start_time_ms,
                total_count as total_rows_in_partition
            FROM {agg_count_table}
            ORDER BY partition_index ASC
        """
        rows_meta = await conn.fetch(q_meta)
        df_meta = pd.DataFrame([dict(r) for r in rows_meta])
        if not df_meta.empty:
            df_meta['timestamp'] = pd.to_datetime(df_meta['partition_start_time_ms'].astype(float), unit='ms')

        await conn.close()
        return df_raw, df_meta

    except Exception as e:
        print(f"[FATAL] Database error: {e}")
        return None, None

def generate_mock_data():
    return pd.DataFrame(), pd.DataFrame()

def fmt_pct(p):
    val = p * 100
    if val % 1 != 0: return f"{val:.1f}%"
    return f"{int(val)}%"

# ==========================================
# PLOT 1: CDF (Common)
# ==========================================
def plot_cdf(df_raw, save_dir):
    if df_raw is None or df_raw.empty: return
    print("[INFO] Generating CDF Plot...")
    
    df_raw = df_raw.sort_values('timestamp')
    y_values = np.arange(1, len(df_raw) + 1)
    x_values = df_raw['timestamp']

    fig, ax = plt.subplots(figsize=(FIG_WIDTH, 8))
    ax.step(x_values, y_values, where='post', color=CDF_COLOR, linewidth=2)
    
    ax.set_title("Cumulative Count of Non-Zero False Misses Over Time", fontsize=16, fontweight='bold', pad=15)
    ax.set_ylabel("Cumulative Error Count", fontsize=12)
    ax.set_xlabel("Time (UTC)", fontsize=12)
    ax.grid(True, which='both', linestyle='--', alpha=0.5)
    
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.setp(ax.get_xticklabels(), rotation=30, ha='right')
    
    final_count = len(df_raw)
    last_date = df_raw['timestamp'].iloc[-1]
    ax.annotate(f"Total Errors: {final_count}", 
                xy=(last_date, final_count), 
                xytext=(10, -20), textcoords='offset points',
                arrowprops=dict(arrowstyle="->", color='black'),
                fontsize=12, fontweight='bold', color=CDF_COLOR)

    save_path = os.path.join(save_dir, "miss_cdf_over_time.png")
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"[SUCCESS] Saved CDF to: {save_path}")
    plt.close(fig)

# ==========================================
# PLOT 2: V1 HEATMAP (1-13)
# ==========================================
def plot_v1_heatmap(df_raw, df_meta, save_dir, interval_hours):
    if df_raw.empty or df_meta.empty: return
    print("[INFO] Generating V1 Heatmap...")

    interval_str = f'{interval_hours}h'
    
    df_grouped = df_raw.groupby([pd.Grouper(key='timestamp', freq=interval_str), 'false_misses']).size().unstack(fill_value=0)
    
    for i in range(1, 14):
        if i not in df_grouped.columns: df_grouped[i] = 0
    
    df_meta_grouped = df_meta.groupby(pd.Grouper(key='timestamp', freq=interval_str))['total_rows_in_partition'].sum()
    df_final = df_grouped.join(df_meta_grouped).fillna(0)
    
    miss_cols = list(range(1, 14))
    df_final['error_rows'] = df_final[miss_cols].sum(axis=1)

    for col in miss_cols:
        df_final[f'{col}_pct'] = df_final.apply(
            lambda row: (row[col] / row['error_rows']) if row['error_rows'] > 0 else 0, axis=1
        )

    num_steps = len(df_final)
    fig, ax = plt.subplots(figsize=(max(12, num_steps * 1.8), 10))
    
    ax.set_xlim(0, num_steps)
    ax.set_ylim(1, 14)
    
    time_labels = []
    stats_labels = []
    
    for x_idx, (ts, row) in enumerate(df_final.iterrows()):
        time_labels.append(ts.strftime('%m-%d\n%H:%M'))
        
        total_n = int(row['total_rows_in_partition'])
        error_n = int(row['error_rows'])
        t_str = f"{total_n/1_000_000:.1f}M" if total_n > 1_000_000 else f"{total_n:,}"
        stats_labels.append(f"Err: {error_n}\nTot: {t_str}")
        
        if error_n > 0:
            for y_val in range(1, 14):
                pct = row[f'{y_val}_pct']
                if pct > 0:
                    padding = 0.05
                    bar_height = 0.8
                    rect = patches.Rectangle(
                        (x_idx + padding, y_val - (bar_height/2)), 
                        width=(1.0 - (padding*2)) * pct, height=bar_height,
                        linewidth=0, facecolor=LINE_COLOR, alpha=0.7
                    )
                    ax.add_patch(rect)
                    if pct > 0.05: 
                        ax.text(x_idx + 0.5, y_val, f"{pct*100:.1f}%",
                                ha='center', va='center', fontsize=8, color='white', fontweight='bold')

    for y in range(1, 15): ax.axhline(y=y - 0.5, color='gray', lw=0.5, alpha=0.3)
    for x in range(num_steps + 1): ax.axvline(x=x, color='gray', lw=0.5, alpha=0.3)

    ax.set_yticks(range(1, 14))
    ax.set_ylabel("False Misses Value (1-13)", fontsize=12, fontweight='bold')
    ax.set_xticks([x + 0.5 for x in range(num_steps)])
    ax.set_xticklabels(time_labels, rotation=0, fontsize=9)
    
    trans = ax.get_xaxis_transform()
    for i, txt in enumerate(stats_labels):
        color = LINE_COLOR if "Err: 0" not in txt else '#999999'
        ax.text(i + 0.5, -0.12, txt, transform=trans, ha='center', va='top', fontsize=9, color=color, fontweight='bold')

    plt.title("V1: Distribution of Non-Zero Misses (1-13)", fontsize=14, pad=25)
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.15)
    
    save_path = os.path.join(save_dir, "miss_distribution_v1.png")
    plt.savefig(save_path, dpi=300)
    print(f"[SUCCESS] Saved V1 Heatmap to: {save_path}")
    plt.close(fig)

# ==========================================
# PLOT 3: V2 SCATTER + TABLE
# ==========================================

def draw_metric_and_table(ax, df_resampled, metric_col, metric_title, color, percentiles, title_prefix, step_width_days, all_ticks_pydatetime):
    grouped_steps = df_resampled.groupby('timestamp')
    percentiles_100 = [p * 100 for p in percentiles]
    
    table_data = []
    step_counter = 1
    max_y_in_plot = 0
    
    x_limit_min = None
    x_limit_max = None

    for time_start, group in grouped_steps:
        values = group[metric_col].values
        
        # Extract Total Rows (N) from the merged data
        # It should be the same for all rows in the group, or 0 if missing
        total_rows_n = 0
        if 'total_rows_in_partition' in group.columns:
            total_rows_n = int(group['total_rows_in_partition'].max()) # max/min/first should be same

        # If no errors, we still might want to show the step if we have total_rows
        if len(values) == 0 and total_rows_n == 0: 
            step_counter += 1
            continue

        ts_pydatetime = pd.Timestamp(time_start).to_pydatetime()
        x_start = mdates.date2num(ts_pydatetime)
        x_end = x_start + step_width_days
        x_center = x_start + (step_width_days / 2)
        
        if x_limit_min is None or x_start < x_limit_min: x_limit_min = x_start
        if x_limit_max is None or x_end > x_limit_max: x_limit_max = x_end

        # 1. Scatter (Only for non-zero misses)
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
            s_n = len(active_values) # This is 'n' (errors)
            s_mean = np.mean(active_values)
            s_std = np.std(active_values)
            s_skew = skew(active_values) if len(active_values) > 1 else 0
            s_kurt = kurtosis(active_values, fisher=True) if len(active_values) > 1 else 0
        else:
            s_n = 0
            s_mean = s_std = s_skew = s_kurt = 0

        time_str = ts_pydatetime.strftime('%m-%d %H:%M')
        
        # Format Total N
        if total_rows_n > 1_000_000:
            t_str = f"{total_rows_n/1_000_000:.1f}M"
        else:
            t_str = f"{total_rows_n:,}"

        table_data.append([
            step_counter,
            time_str,
            t_str,              # Total (N)
            f"{s_n:,}",         # Errors (n)
            f"{s_mean:.1f}",
            f"{s_std:.1f}",
            f"{s_skew:.2f}",
            f"{s_kurt:.2f}"
        ])

        if len(values) > 0 and values.max() > max_y_in_plot: max_y_in_plot = values.max()
        step_counter += 1

    if x_limit_min is not None and x_limit_max is not None:
        ax.set_xlim(x_limit_min, x_limit_max)

    ax.set_title(f"{title_prefix} - {metric_title}", fontsize=14, fontweight='bold', pad=15)
    ax.set_ylabel("False Misses Count", fontsize=12)
    ax.grid(True, axis='y', linestyle='--', alpha=0.3)
    
    ax.set_xticks([mdates.date2num(t) for t in all_ticks_pydatetime])
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.setp(ax.get_xticklabels(), rotation=30, ha='right', fontsize=9)
    
    from matplotlib.lines import Line2D
    custom_lines = [Line2D([0], [0], color=LINE_COLOR, lw=2)]
    p_labels = ", ".join([fmt_pct(p) for p in percentiles])
    ax.legend(custom_lines, [f'Percentiles ({p_labels})'], loc='upper right', fontsize=10)

    if table_data:
        # UPDATED COLUMNS: Removed Sum, Added Total (N)
        col_labels = ["Step", "Time (UTC)", "Total (N)", "Errors (n)", "Mean (μ)", "Std (σ)", "Skew (γ₁)", "Kurt (κ)"]
        
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

def plot_v2_distribution(df_raw, df_meta, save_dir, interval_hours):
    if df_raw.empty: return
    print("[INFO] Generating V2 Distribution Plot (Price Changes Style)...")

    interval_str = f'{interval_hours}h'
    
    # 1. Bucket Raw Data
    df_raw['bucket_ts'] = df_raw['timestamp'].dt.floor(interval_str)
    df_prepared = df_raw[['bucket_ts', 'false_misses']].rename(columns={'bucket_ts': 'timestamp'})
    
    # 2. Bucket Meta Data (Total Rows)
    # Sum total rows per interval
    df_meta_grouped = df_meta.groupby(pd.Grouper(key='timestamp', freq=interval_str))['total_rows_in_partition'].sum().reset_index()
    
    # 3. Merge Meta into Prepared
    df_prepared = df_prepared.merge(df_meta_grouped, on='timestamp', how='left')
    
    # Calculate Ticks
    step_hours = interval_hours
    step_width_days = step_hours / 24.0
    unique_starts = np.sort(df_prepared['timestamp'].unique())
    
    if len(unique_starts) > 0:
        last_end = unique_starts[-1] + pd.Timedelta(hours=step_hours)
        all_ticks_pd = np.append(unique_starts, last_end)
        all_ticks_pydatetime = [pd.Timestamp(t).to_pydatetime() for t in all_ticks_pd]
    else:
        all_ticks_pydatetime = []

    # Setup Figure
    num_rows = len(unique_starts) + 1
    table_height_inches = num_rows * 0.25
    total_fig_height = PLOT_HEIGHT_INCHES + table_height_inches + 1

    fig = plt.figure(figsize=(FIG_WIDTH, total_fig_height))
    ax = fig.add_subplot(111)

    # Draw
    draw_metric_and_table(
        ax, 
        df_prepared, 
        V2_METRIC_CONFIG['col'], 
        V2_METRIC_CONFIG['title'], 
        V2_METRIC_CONFIG['color'], 
        V2_METRIC_CONFIG['percentiles'], 
        "V2 Misses", 
        step_width_days, 
        all_ticks_pydatetime
    )

    bottom_margin = (table_height_inches + 0.5) / total_fig_height
    plt.subplots_adjust(bottom=bottom_margin)

    save_path = os.path.join(save_dir, "miss_distribution_v2.png")
    plt.savefig(save_path, dpi=300)
    print(f"[SUCCESS] Saved V2 Distribution to: {save_path}")
    plt.close(fig)

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    print(f"[INIT] Base Output Directory: {BASE_OUTPUT_DIR}")

    versions = [
        {"subdir": "1", "raw_table": "buffer_miss_rows", "agg_count_table": "agg_partition_counts"},
        {"subdir": "2", "raw_table": "buffer_miss_rows_2", "agg_count_table": "agg_partition_counts_2"}
    ]

    for ver in versions:
        subdir = ver["subdir"]
        raw_tbl = ver["raw_table"]
        agg_tbl = ver["agg_count_table"]
        
        current_save_dir = os.path.join(BASE_OUTPUT_DIR, subdir)
        os.makedirs(current_save_dir, exist_ok=True)
        
        print(f"\n==================================================")
        print(f" PROCESSING VERSION: '{subdir}'")
        print(f"==================================================")

        # 1. Fetch
        df_raw, df_meta = loop.run_until_complete(fetch_data_for_fork(raw_tbl, agg_tbl))
        
        if df_raw is None or df_raw.empty:
            print("[WARN] No miss data found. Skipping plots.")
            continue

        # 2. Plot CDF (Common)
        plot_cdf(df_raw, current_save_dir)
        
        # 3. Plot Distribution (Fork Specific)
        step_hours = K * PARTITION_SIZE_HOURS
        
        if subdir == "1":
            plot_v1_heatmap(df_raw, df_meta, current_save_dir, step_hours)
        elif subdir == "2":
            # Pass df_meta here now
            plot_v2_distribution(df_raw, df_meta, current_save_dir, step_hours)

if __name__ == "__main__":
    main()