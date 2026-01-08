import sqlite3
import json
import logging
from collections import defaultdict
from tqdm import tqdm
import os

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
DB_FILE = "polymarket_events.db"
TABLE_NAME = "events"

def check_labels_for_nba_and_bitcoin():
    """
    Reads all events from the database in batches and searches for labels containing NBA or Bitcoin.
    Prints the exact format of matching labels and counts them.
    """
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_FILE)
    
    if not os.path.exists(db_path):
        logger.error(f"Database file '{db_path}' not found.")
        return
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        total_events = cursor.fetchone()[0]
        logger.info(f"Total events in database: {total_events}")
        
        if total_events == 0:
            logger.warning("Database table is empty.")
            return
        
        matching_labels = defaultdict(lambda: {'count': 0, 'unique_events': set()})  # Track labels, counts, and unique event IDs
        event_count_with_matching_labels = 0
        
        logger.info("Scanning for labels containing 'NBA' or 'Bitcoin'...")
        
        # Process rows in large batches to avoid loading entire table into memory
        BATCH_SIZE = 1000
        offset = 0
        
        with tqdm(total=total_events, desc="Scanning Events") as pbar:
            while offset < total_events:
                batch_cursor = conn.cursor()
                batch_cursor.execute(f"SELECT id, data FROM {TABLE_NAME} LIMIT ? OFFSET ?", (BATCH_SIZE, offset))
                rows = batch_cursor.fetchall()
                
                if not rows:
                    break
                
                for row in rows:
                    doc_id = row['id']
                    try:
                        event_data = json.loads(row['data'])
                        
                        # Check if the event has tags
                        if 'tags' in event_data and isinstance(event_data['tags'], list):
                            event_has_match = False
                            for tag in event_data['tags']:
                                if isinstance(tag, dict) and 'label' in tag:
                                    label = tag['label']
                                    # Case-insensitive search for NBA or Bitcoin
                                    if 'NBA' in label.upper() or 'BITCOIN' in label.upper():
                                        matching_labels[label]['count'] += 1
                                        matching_labels[label]['unique_events'].add(doc_id)
                                        if not event_has_match:
                                            event_count_with_matching_labels += 1
                                            event_has_match = True
                        
                        pbar.update(1)
                            
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON for event ID {doc_id}: {e}")
                        pbar.update(1)
                        continue
                    except Exception as e:
                        logger.warning(f"Error processing event ID {doc_id}: {e}")
                        pbar.update(1)
                        continue
                
                offset += BATCH_SIZE
                batch_cursor.close()
        
        # Print results
        logger.info("\n" + "="*80)
        logger.info("RESULTS: Labels containing 'NBA' or 'Bitcoin'")
        logger.info("="*80)
        
        if matching_labels:
            print("\nMatching Labels (Exact Format, Total Occurrences, and Unique Events):")
            print("-" * 100)
            for label, data in sorted(matching_labels.items(), key=lambda x: x[1]['count'], reverse=True):
                unique_count = len(data['unique_events'])
                print(f"  Label: '{label}'")
                print(f"  Total Occurrences: {data['count']}")
                print(f"  Unique Events: {unique_count}")
                print()
            
            print("-" * 100)
            total_all_occurrences = sum(d['count'] for d in matching_labels.values())
            print(f"\nTotal unique labels with 'NBA' or 'Bitcoin': {len(matching_labels)}")
            print(f"Total occurrences across all events: {total_all_occurrences}")
            print(f"Total events containing these labels: {event_count_with_matching_labels}")
            
            # Detailed Bitcoin info
            if 'Bitcoin' in matching_labels:
                bitcoin_unique = len(matching_labels['Bitcoin']['unique_events'])
                print(f"\n*** Bitcoin Label Specific Stats ***")
                print(f"Bitcoin label appears: {matching_labels['Bitcoin']['count']} times")
                print(f"Unique events with Bitcoin label: {bitcoin_unique}")
        else:
            print("\nNo labels containing 'NBA' or 'Bitcoin' found.")
        
        logger.info("Scan complete.")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        conn.close()
        logger.info("Database connection closed.")

if __name__ == "__main__":
    check_labels_for_nba_and_bitcoin()
