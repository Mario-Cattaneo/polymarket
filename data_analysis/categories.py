import os
import asyncio
import asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import sys
from datetime import datetime
from collections import Counter

# ==========================================
# CONFIGURATION
# ==========================================
USE_MOCK_DATA = False # Set to True to use generated data instead of DB
CSV_FILE_PATH = f"{os.getenv("POLY_CSV")}/gamma_events.csv" # Assumes the CSV from the previous script is here

# Base Path Logic
POLY_PLOTS = os.getenv("POLY_PLOTS", "plots") 
BASE_OUTPUT_DIR = os.path.join(POLY_PLOTS, "label_agg_plots")

# Time Aggregation
K = 1  # Interval multiplier (e.g., K=1 * 3h = 3h steps)
PARTITION_SIZE_HOURS = 3
INTERVAL_STR = f'{K * PARTITION_SIZE_HOURS}h'

# Visual Settings
FIG_WIDTH = 22
PLOT_HEIGHT_INCHES = 8.0 

# Table Settings
TABLE_ROW_HEIGHT = 0.035
TABLE_FONT_SIZE = 9

# Label Configuration
STATIC_LABELS = ['Sports', 'Hide From New', 'Recurring', 'Crypto', 'Crypto Prices', 'Games', 'Up or Down', 'Politics', '15M', 'Bitcoin']
REST_LABEL = "Rest"
ALL_LABELS = STATIC_LABELS + [REST_LABEL]

# Event Type Configuration (The tables to query)
PLOTS_CONFIG = {
    "price_changes": {"table_name": "agg_price_changes_2", "title": "Price Changes"},
    "books": {"table_name": "agg_books_2", "title": "Order Books"},
    "tick_changes": {"table_name": "agg_tick_changes_2", "title": "Tick Changes"},
    "last_trade_prices": {"table_name": "agg_last_trade_prices_2", "title": "Last Trades"}
}

# Color map for labels (11 colors: 10 static + 1 Rest)
# Using a distinct color map (e.g., 'tab20' or 'Paired') and manually selecting 11
COLOR_MAP = plt.cm.get_cmap('tab20', len(ALL_LABELS))
LABEL_COLORS = {label: COLOR_MAP(i) for i, label in enumerate(ALL_LABELS)}


# ==========================================
# DATA LOADING & PREP FUNCTIONS
# ==========================================

def load_csv_data(file_path):
    """Loads the CSV and converts the tag_label string back to a list."""
    print(f"[INFO] Loading CSV data from {file_path}...")
    try:
        df_csv = pd.read_csv(file_path)
        # Convert the string representation of a list back to a list object
        # Use literal_eval for safe conversion if needed, but simple string split/strip often works for clean data
        import ast
        df_csv['tag_label'] = df_csv['tag_label'].apply(lambda x: ast.literal_eval(x) if pd.notna(x) and x.startswith('[') else [])
        df_csv = df_csv.rename(columns={'condition_id': 'condition_id'})
        return df_csv[['condition_id', 'tag_label']]
    except Exception as e:
        print(f"[FATAL] Error loading or parsing CSV: {e}")
        return pd.DataFrame()

async def fetch_markets_2_data():
    """Fetches the condition_id to asset_id mapping from markets_2."""
    print("[INFO] Fetching markets_2 data...")
    required_vars = ["PG_SOCKET", "POLY_PG_PORT", "POLY_DB", "POLY_DB_CLI", "POLY_DB_CLI_PASS"]
    if any(not os.getenv(v) for v in required_vars):
        print(f"[ERROR] Missing environment variables for DB connection.")
        return pd.DataFrame()

    try:
        conn = await asyncpg.connect(
            host=os.getenv("PG_SOCKET"),
            port=os.getenv("POLY_PG_PORT"),
            database=os.getenv("POLY_DB"),
            user=os.getenv("POLY_DB_CLI"),
            password=os.getenv("POLY_DB_CLI_PASS")
        )
        query = "SELECT asset_id, condition_id FROM markets_2"
        rows = await conn.fetch(query)
        await conn.close()
        
        if not rows:
            print("[WARN] No data found in markets_2.")
            return pd.DataFrame()

        data = [dict(row) for row in rows]
        return pd.DataFrame(data)
    except Exception as e:
        print(f"[FATAL] Database error on markets_2: {e}")
        return pd.DataFrame()

def categorize_label(label_list):
    """
    Matches the first label in the list against STATIC_LABELS, 
    or returns 'Rest'.
    """
    if not label_list:
        return REST_LABEL
    
    for label in label_list:
        if label in STATIC_LABELS:
            return label
    
    return REST_LABEL

def prepare_data(df_csv, df_markets_2, df_event):
    """Merges and categorizes all dataframes."""
    
    # 1. Map condition_id to a single label
    df_labels = df_csv.copy()
    df_labels['categorized_label'] = df_labels['tag_label'].apply(categorize_label)
    df_labels = df_labels[['condition_id', 'categorized_label']]
    
    # 2. Map asset_id to condition_id and then to label
    df_map = pd.merge(df_markets_2, df_labels, on='condition_id', how='left')
    df_map = df_map[['asset_id', 'categorized_label']].drop_duplicates()
    
    # Fill NaN labels (from markets_2 rows with no matching condition_id in CSV) with 'Rest'
    df_map['categorized_label'] = df_map['categorized_label'].fillna(REST_LABEL)
    
    # 3. Merge event data with label map
    df_merged = pd.merge(df_event, df_map, on='asset_id', how='left')
    
    # Fill any remaining NaN labels (from event data with no matching asset_id in markets_2) with 'Rest'
    df_merged['categorized_label'] = df_merged['categorized_label'].fillna(REST_LABEL)
    
    # 4. Group and Resample
    df_agg = df_merged.groupby(
        [pd.Grouper(key='timestamp', freq=INTERVAL_STR), 'categorized_label']
    )['total_count'].sum().reset_index()
    
    # Rename for clarity
    df_agg = df_agg.rename(columns={'total_count': 'count'})
    
    return df_agg

# ==========================================
# DRAWING LOGIC
# ==========================================

def draw_stacked_bar_plot(ax, df_agg, title, step_width_days, all_ticks_pydatetime):
    """
    Draws the stacked bar plot (heatmap style) and returns the table data.
    """
    
    # Pivot the data to have labels as columns and time as index
    df_pivot = df_agg.pivot(index='timestamp', columns='categorized_label', values='count').fillna(0)
    
    # Ensure all 11 labels are present as columns (even if all zeros)
    for label in ALL_LABELS:
        if label not in df_pivot.columns:
            df_pivot[label] = 0
            
    # Reorder columns to match ALL_LABELS order
    df_pivot = df_pivot[ALL_LABELS]
    
    # Calculate total count per time step for normalization (optional, but useful for context)
    df_pivot['total_step_count'] = df_pivot.sum(axis=1)
    
    # Prepare table data
    table_data = []
    step_counter = 1
    
    # X-axis coordinates
    unique_starts = df_pivot.index
    
    # Track min/max X for tight limits
    x_limit_min = None
    x_limit_max = None
    
    # Plotting the stacked bars
    for i, time_start in enumerate(unique_starts):
        
        # Coordinates
        ts_pydatetime = pd.Timestamp(time_start).to_pydatetime()
        x_start = mdates.date2num(ts_pydatetime)
        x_end = x_start + step_width_days
        x_center = x_start + (step_width_days / 2)
        
        # Update Limits
        if x_limit_min is None or x_start < x_limit_min: x_limit_min = x_start
        if x_limit_max is None or x_end > x_limit_max: x_limit_max = x_end
        
        # Data for this step
        row = df_pivot.loc[time_start]
        total_count = row['total_step_count']
        
        # Prepare table row
        time_str = ts_pydatetime.strftime('%m-%d %H:%M')
        table_row = [step_counter, time_str, f"{total_count:,}"]
        
        # Stacked bar logic
        bottom = 0
        for label in ALL_LABELS:
            count = row[label]
            
            if count > 0:
                # Draw the cell (bar segment)
                ax.bar(
                    x_center, 
                    count, 
                    width=step_width_days, 
                    bottom=bottom, 
                    color=LABEL_COLORS[label], 
                    align='center', 
                    edgecolor='white', 
                    linewidth=0.5
                )
            
            bottom += count
            table_row.append(f"{int(count):,}")
            
        # Add total count label above the bar
        if total_count > 0:
            ax.text(x_center, total_count, f"{int(total_count):,}", 
                    ha='center', va='bottom', fontsize=8, color='black')
        
        # Grid & Borders
        ax.axvline(x=x_start, color='gray', linestyle='-', linewidth=0.5, alpha=0.3)
        
        # Step Index Label
        trans = ax.get_xaxis_transform()
        ax.text(x_center, 1.01, str(step_counter), 
                transform=trans, ha='center', va='bottom', 
                fontsize=10, fontweight='bold', color='#555555')
        
        table_data.append(table_row)
        step_counter += 1

    # Final vertical line
    if x_limit_max is not None:
        ax.axvline(x=x_limit_max, color='gray', linestyle='-', linewidth=0.5, alpha=0.3)
        ax.set_xlim(x_limit_min, x_limit_max)

    # Formatting
    ax.set_title(f"Event Aggregation by Label: {title}", fontsize=16, fontweight='bold', pad=15)
    ax.set_ylabel("Total Count", fontsize=12)
    ax.grid(True, axis='y', linestyle='--', alpha=0.3)
    
    # X-Axis
    ax.set_xticks([mdates.date2num(t) for t in all_ticks_pydatetime])
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.setp(ax.get_xticklabels(), rotation=30, ha='right', fontsize=9)
    
    # Legend
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor=LABEL_COLORS[label], edgecolor='black', label=label) for label in ALL_LABELS]
    ax.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(1.01, 1), title="Label Category", fontsize=10)

    # ---------------------------------------------------------
    # Draw Table
    # ---------------------------------------------------------
    if table_data:
        col_labels = ["Step", "Time (UTC)", "Total Count"] + ALL_LABELS
        
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
        
        # Color the header row
        for c in range(len(col_labels)):
            the_table[0, c].set_facecolor("#cccccc")
            
        # Color the label columns in the header
        for c, label in enumerate(ALL_LABELS):
            col_index = c + 3 # Labels start at column index 3
            the_table[0, col_index].set_facecolor(LABEL_COLORS[label])
            
        for r in range(len(table_data)):
            color = "#f2f2f2" if r % 2 == 0 else "#ffffff"
            for c in range(len(col_labels)):
                the_table[r+1, c].set_facecolor(color)
                
        return total_rows
    return 0

# ==========================================
# MAIN EXECUTION
# ==========================================

async def fetch_event_data(table_name):
    """Fetches event data (agg_..._2 tables)."""
    print(f"[INFO] Fetching event data for {table_name}...")
    try:
        conn = await asyncpg.connect(
            host=os.getenv("PG_SOCKET"),
            port=os.getenv("POLY_PG_PORT"),
            database=os.getenv("POLY_DB"),
            user=os.getenv("POLY_DB_CLI"),
            password=os.getenv("POLY_DB_CLI_PASS")
        )
        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = $1)", 
            table_name
        )
        if not exists:
            print(f"[WARN] Table {table_name} does not exist in DB.")
            await conn.close()
            return pd.DataFrame()

        # We only need partition_start_time_ms, asset_id, and total_count
        query = f"SELECT partition_start_time_ms, asset_id, total_count FROM {table_name} ORDER BY partition_start_time_ms ASC"
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

def generate_mock_event_data(table_name):
    """Generates mock event data for testing."""
    print(f"[INFO] Generating mock data for {table_name}...")
    dates = pd.date_range(start='2025-11-20', periods=15, freq=INTERVAL_STR)
    data = []
    for d in dates:
        for i in range(100):
            asset_id = f'asset_{i}'
            total = int(np.random.lognormal(mean=4, sigma=1)) 
            data.append({
                'partition_start_time_ms': int(d.timestamp() * 1000),
                'timestamp': d,
                'asset_id': asset_id,
                'total_count': total,
            })
    return pd.DataFrame(data)

def generate_mock_markets_2_data():
    """Generates mock markets_2 data for testing."""
    print("[INFO] Generating mock markets_2 data...")
    data = []
    for i in range(100):
        asset_id = f'asset_{i}'
        condition_id = f'cond_{i}'
        data.append({'asset_id': asset_id, 'condition_id': condition_id})
    return pd.DataFrame(data)

def generate_mock_csv_data():
    """Generates mock CSV data for testing."""
    print("[INFO] Generating mock CSV data...")
    data = []
    for i in range(100):
        condition_id = f'cond_{i}'
        # Assign a label or a list of labels
        if i % 11 == 0:
            labels = []
        elif i % 11 == 1:
            labels = ['Crypto']
        elif i % 11 == 2:
            labels = ['Politics', 'US']
        else:
            labels = [STATIC_LABELS[i % len(STATIC_LABELS)]]
            
        data.append({'condition_id': condition_id, 'tag_label': labels})
    return pd.DataFrame(data)


async def main_async():
    
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
    print(f"[INIT] Output Directory: {BASE_OUTPUT_DIR}")

    # 1. Load Static Data
    if USE_MOCK_DATA:
        df_csv = generate_mock_csv_data()
        df_markets_2 = generate_mock_markets_2_data()
    else:
        df_csv = load_csv_data(CSV_FILE_PATH)
        df_markets_2 = await fetch_markets_2_data()

    if df_csv.empty or df_markets_2.empty:
        print("[FATAL] Cannot proceed due to missing CSV or markets_2 data.")
        return

    # 2. Process Each Event Type
    for key, config in PLOTS_CONFIG.items():
        table_name = config['table_name']
        
        print(f"\n==================================================")
        print(f" PROCESSING EVENT: {table_name}")
        print(f"==================================================")
        
        # Fetch event data
        if USE_MOCK_DATA:
            df_event = generate_mock_event_data(table_name)
        else:
            df_event = await fetch_event_data(table_name)
            
        if df_event.empty:
            continue
            
        # Prepare and aggregate data
        df_agg = prepare_data(df_csv, df_markets_2, df_event)
        
        if df_agg.empty:
            print(f"[WARN] Aggregated data for {table_name} is empty. Skipping plot.")
            continue

        # 3. Plotting
        
        # Determine X-axis ticks
        step_hours = K * PARTITION_SIZE_HOURS
        step_width_days = step_hours / 24.0
        
        unique_starts = np.sort(df_agg['timestamp'].unique())
        if len(unique_starts) > 0:
            last_end = unique_starts[-1] + pd.Timedelta(hours=step_hours)
            all_ticks_pd = np.append(unique_starts, last_end)
            all_ticks_pydatetime = [pd.Timestamp(t).to_pydatetime() for t in all_ticks_pd]
        else:
            all_ticks_pydatetime = []
            
        # Calculate height for plot
        num_steps = len(unique_starts)
        if num_steps == 0: continue
        
        # The table has num_steps rows + 1 header row
        num_rows_in_table = num_steps + 1
        table_height_inches = num_rows_in_table * 0.25
        total_fig_height = PLOT_HEIGHT_INCHES + table_height_inches + 1

        fig = plt.figure(figsize=(FIG_WIDTH, total_fig_height))
        ax = fig.add_subplot(111)
        
        draw_stacked_bar_plot(
            ax, df_agg, config['title'], 
            step_width_days, all_ticks_pydatetime
        )
        
        # Adjust bottom margin
        bottom_margin = (table_height_inches + 0.5) / total_fig_height
        plt.subplots_adjust(bottom=bottom_margin, right=0.85) # Adjust right for legend
        
        filename = f"label_agg_{table_name}.png"
        save_path = os.path.join(BASE_OUTPUT_DIR, filename)
        plt.savefig(save_path, dpi=300)
        print(f"[SUCCESS] Saved plot to: {save_path}")
        plt.close(fig)


if __name__ == "__main__":
    # Ensure the CSV file exists if not using mock data
    if not USE_MOCK_DATA and not os.path.exists(CSV_FILE_PATH):
        print(f"[FATAL] CSV file not found at: {CSV_FILE_PATH}. Please run the previous script first or set USE_MOCK_DATA = True.")
        sys.exit(1)
        
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main_async())