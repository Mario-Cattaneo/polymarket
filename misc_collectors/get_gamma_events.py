import requests
import csv
import os
import time
import sys
from tqdm import tqdm
from collections import Counter

# Configuration
API_URL = "https://gamma-api.polymarket.com/events"
# Ensure the directory exists or handle the path correctly
output_dir = os.getenv("POLY_CSV", ".") 
OUTPUT_FILE = os.path.join(output_dir, "gamma_events.csv")
LIMIT = 500

def fetch_and_process_markets():
    offset = 0
    all_rows = []
    
    # Statistics container
    label_counts = Counter()
    
    print(f"🚀 Starting Gamma API Scrape (Limit: {LIMIT})...")
    
    pbar = tqdm(desc="Processing Events", unit="evts")
    
    while True:
        try:
            params = {
                "limit": LIMIT,
                "offset": offset
            }
            
            response = requests.get(API_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                break
            
            batch_rows = []
            
            for event in data:
                # 1. Handle Tags (Collect ALL labels into a list)
                tags = event.get("tags", [])
                
                # Extract all labels, filtering out any None values
                current_labels = [t.get("label") for t in tags if t.get("label")]
                
                # 2. Handle Markets (Condition IDs)
                markets = event.get("markets", [])
                
                for market in markets:
                    condition_id = market.get("conditionId")
                    
                    # Only add if we have a condition_id
                    if condition_id:
                        # Update stats: Count EVERY label in the list for this market
                        label_counts.update(current_labels)
                        
                        batch_rows.append({
                            "condition_id": condition_id,
                            "tag_label": current_labels  # This will write as "['Label1', 'Label2']" in CSV
                        })

            all_rows.extend(batch_rows)
            
            # Update progress bar
            pbar.update(len(data))
            
            if len(data) < LIMIT:
                break
                
            offset += LIMIT
            time.sleep(0.11) # Rate limiting
            
        except Exception as e:
            print(f"\n❌ Error at offset {offset}: {e}")
            break
            
    pbar.close()
    return all_rows, label_counts

def save_to_csv(rows):
    print(f"💾 Saving {len(rows)} records to {OUTPUT_FILE}...")
    
    fieldnames = ["condition_id", "tag_label"]
    
    try:
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print("✅ Save complete.")
    except Exception as e:
        print(f"❌ Error saving CSV: {e}")

def print_stats(label_counts):
    print("\n" + "="*40)
    print("📊 STATISTICS")
    print("="*40)
    
    print(f"Total Unique Labels: {len(label_counts)}")
        
    print("\n🏆 Top 10 Tag Labels (by frequency):")
    
    # Get the top 10 labels (keys)
    top_10_labels = [label for label, count in label_counts.most_common(10)]
    
    # Print the list of labels
    print(str(top_10_labels))
    
    # Also print the list of labels AND their counts (for full context)
    print("\n🏆 Top 10 Tag Labels and Counts:")
    for label, count in label_counts.most_common(10):
        print(f"   - {label}: {count}")
        
    print("="*40 + "\n")

if __name__ == "__main__":
    rows, l_counts = fetch_and_process_markets()
    
    if rows:
        save_to_csv(rows)
        print_stats(l_counts)
    else:
        print("No data found.")