import os
import asyncio
import asyncpg
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as patches
import numpy as np
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
USE_MOCK_DATA = False
SAVE_DIR = "miss_count_plots"

# Time Aggregation
K = 2  # Interval multiplier (e.g., K=2 * 3h = 6h steps)
PARTITION_SIZE_HOURS = 3

# Visual Settings
FIG_WIDTH = 20
FIG_HEIGHT = 10  # Slightly shorter as we have 1 less row (0 is gone)
BAR_COLOR = '#D32F2F'  # Red/Crimson to signify "Misses/Errors"
BAR_ALPHA = 0.7
TEXT_COLOR = '#333333'

# Database Config
DB_TABLE_NAME = "miss_count_asset_agg"

# ==========================================
# HELPER FUNCTIONS
# ==========================================

async def fetch_nonzero_data():
    """
    Fetches ONLY columns 1-13 from the aggregation table.
    Excludes count_miss_0 entirely from the initial query.
    """
    print(f"[INFO] Fetching NON-ZERO data for {DB_TABLE_NAME}...")
    
    if USE_MOCK_DATA:
        return generate_mock_data()

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
        
        # Construct query to ONLY get 1-13
        cols_to_select = ", ".join([f"count_miss_{i}" for i in range(1, 14)])
        query = f"""
            SELECT 
                partition_start_time_ms, 
                {cols_to_select}
            FROM {DB_TABLE_NAME} 
            ORDER BY partition_start_time_ms ASC
        """
        
        rows = await conn.fetch(query)
        await conn.close()
        
        if not rows:
            print(f"[WARN] No data found in {DB_TABLE_NAME}.")
            return pd.DataFrame()

        data = [dict(row) for row in rows]
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['partition_start_time_ms'].astype(float), unit='ms')
        return df
    except Exception as e:
        print(f"[FATAL] Database error: {e}")
        return pd.DataFrame()

def generate_mock_data():
    print("[INFO] Generating mock data...")
    dates = pd.date_range(start='2025-11-20', periods=20, freq='3h')
    data = []
    
    for d in dates:
        row = {
            'partition_start_time_ms': int(d.timestamp() * 1000),
            'timestamp': d
        }
        # Only generate 1-13
        for m in range(1, 14):
            # Random values, some zeros
            row[f'count_miss_{m}'] = int(np.random.exponential(scale=5))
        data.append(row)
            
    return pd.DataFrame(data)

# ==========================================
# PLOTTING LOGIC
# ==========================================

def draw_nonzero_distribution_grid(df, interval_hours):
    """
    Draws a grid for Miss Counts 1-13 (Excluding 0).
    """
    
    # 1. Prepare Data
    # Identify the miss columns (1 to 13)
    miss_cols = [f'count_miss_{i}' for i in range(1, 14)]
    
    # Group by Time Interval
    interval_str = f'{interval_hours}h'
    
    # Sum columns
    df_grouped = df.groupby(pd.Grouper(key='timestamp', freq=interval_str))[miss_cols].sum()
    
    # Calculate Total Non-Zero Events per step
    df_grouped['step_total'] = df_grouped.sum(axis=1)
    
    # Filter: If a step has 0 non-zero misses, we can either drop it or show it as empty.
    # Let's keep it to maintain the timeline, but we handle div/0 later.
    if df_grouped.empty:
        print("[WARN] No data after grouping.")
        return

    # Calculate Percentages
    for col in miss_cols:
        # If step_total is 0, pct is 0
        df_grouped[f'{col}_pct'] = df_grouped.apply(
            lambda row: (row[col] / row['step_total']) if row['step_total'] > 0 else 0, 
            axis=1
        )

    # 2. Setup Plot
    num_steps = len(df_grouped)
    # Y-Axis is 1 to 13, so we need 13 slots.
    # We will map index 0 -> miss_1, index 1 -> miss_2, etc.
    
    fig, ax = plt.subplots(figsize=(max(12, num_steps * 1.5), FIG_HEIGHT))
    
    # Set Limits
    ax.set_xlim(0, num_steps)
    ax.set_ylim(1, 14) # Y axis from 1 to 14 (exclusive of 14)
    
    # 3. Draw Cells
    time_labels = []
    total_counts = []
    
    # Iterate through time steps (Columns)
    for x_idx, (ts, row) in enumerate(df_grouped.iterrows()):
        
        step_total = int(row['step_total'])
        time_labels.append(ts.strftime('%m-%d\n%H:%M'))
        
        if step_total > 0:
            total_counts.append(f"N={step_total:,}")
        else:
            total_counts.append("N=0")
        
        # Iterate through Miss Counts (Rows 1-13)
        for y_val in range(1, 14):
            pct = row[f'count_miss_{y_val}_pct']
            
            if pct > 0:
                # Draw Bar
                # y_val is 1, 2, ... 13.
                # We want the bar centered on y_val.
                # Rectangle bottom-left: (x, y_val - 0.4)
                
                padding = 0.05
                bar_height = 0.8
                # Center the bar on the integer tick
                y_pos = y_val - (bar_height / 2)
                
                rect = patches.Rectangle(
                    (x_idx + padding, y_pos), 
                    width=(1.0 - (padding*2)) * pct, 
                    height=bar_height,
                    linewidth=0,
                    edgecolor=None,
                    facecolor=BAR_COLOR,
                    alpha=BAR_ALPHA
                )
                ax.add_patch(rect)
                
                # Add Text Label
                if pct > 0.05: 
                    ax.text(
                        x_idx + 0.5, 
                        y_val, 
                        f"{pct*100:.1f}%",
                        ha='center', va='center',
                        fontsize=8, color='white', fontweight='bold'
                    )
                elif pct > 0.01:
                     ax.text(
                        x_idx + padding + ((1.0-padding*2)*pct) + 0.05,
                        y_val,
                        f"{pct*100:.0f}%",
                        ha='left', va='center',
                        fontsize=7, color='#666666'
                    )

    # 4. Grid & Formatting
    
    # Horizontal lines
    for y in range(1, 15):
        ax.axhline(y=y - 0.5, color='gray', linestyle='-', linewidth=0.5, alpha=0.3)
        
    # Vertical lines
    for x in range(num_steps + 1):
        ax.axvline(x=x, color='gray', linestyle='-', linewidth=0.5, alpha=0.3)

    # Y-Axis Labels (1 to 13)
    ax.set_yticks(range(1, 14))
    ax.set_yticklabels([str(i) for i in range(1, 14)])
    ax.set_ylabel("False Misses Count (Non-Zero)", fontsize=12, fontweight='bold')
    
    # X-Axis Labels
    ax.set_xticks([x + 0.5 for x in range(num_steps)])
    ax.set_xticklabels(time_labels, rotation=0, fontsize=9)
    ax.set_xlabel(f"Time Steps ({interval_hours}h)", fontsize=12, fontweight='bold')

    # 5. Add "Legend" (Total Counts)
    trans = ax.get_xaxis_transform()
    for i, txt in enumerate(total_counts):
        ax.text(i + 0.5, -0.08, txt, transform=trans, 
                ha='center', va='top', fontsize=10, color='#8B0000', fontweight='bold')

    # Title
    plt.title(f"Distribution of Non-Zero False Misses (1-13)\n(Excluding '0 misses' | N = Count of errors only)", 
              fontsize=14, pad=20)

    # Save
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.1)
    
    filename = "miss_count_nonzero_distribution.png"
    save_path = os.path.join(SAVE_DIR, filename)
    plt.savefig(save_path, dpi=300)
    print(f"[SUCCESS] Saved plot to: {save_path}")
    plt.close(fig)

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    os.makedirs(SAVE_DIR, exist_ok=True)

    # 1. Fetch Data (Columns 1-13 only)
    df = loop.run_until_complete(fetch_nonzero_data())
    
    if df.empty:
        print("[ERROR] No data to process.")
        return

    # 2. Process and Plot
    step_hours = K * PARTITION_SIZE_HOURS
    print(f"[INFO] Processing with step size: {step_hours} hours")
    
    draw_nonzero_distribution_grid(df, step_hours)

if __name__ == "__main__":
    main()