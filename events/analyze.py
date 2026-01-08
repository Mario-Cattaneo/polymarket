import json
import logging
import sqlite3
from collections import defaultdict, Counter
from functools import lru_cache
import numpy as np
import pandas as pd
from tqdm import tqdm
import os
import datetime
import string
import math # Import math for isinf and isnan checks

# --- LOGGING SETUP ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---

POLY_CSV = os.getenv('POLY_CSV', os.path.expanduser('~/polymarket_data'))
DB_FILE = os.path.join(POLY_CSV, "polymarket_events.db")
TABLE_NAME = "events"

TIMESTAMP_FORMATS = [
    '%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S.%f%z',
    '%Y-%m-%d %H:%M:%S%z', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S',
    '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d',
]

@lru_cache(maxsize=10000)
def characterize_string(s):
    if not s:
        return "str:empty"
    
    # 1. Check for only spaces
    if s.isspace():
        return "str:whitespace_only"
    
    # 2. Check for case insensitive null/none/nan/true/false (using stripped and lowercased)
    s_stripped_lower = s.strip().lower()
    if s_stripped_lower in ('null', 'none', 'nan'):
        return f"str:{s_stripped_lower}_like"
    if s_stripped_lower in ('true', 'false'):
        return "str:boolean_like"
        
    # 3. Check for JSON encoded (must not be stripped)
    if (s.startswith('{') and s.endswith('}')) or (s.startswith('[') and s.endswith(']')):
        try:
            json.loads(s)
            return "str:json_encoded"
        except json.JSONDecodeError:
            pass

    # 4. Check for hexadecimal
    if s.startswith('0x'):
        if s[2:] and all(c in string.hexdigits for c in s[2:]):
            return "str:hexadecimal"

    # 5. Check for numbers (integer vs float/IEEE 754)
    try:
        num = float(s)
        
        # FIX: Check for inf or nan before attempting int() conversion
        if math.isinf(num) or math.isnan(num):
            return "str:float" # Classify inf/nan as float
            
        s_lower = s.lower()
        # Check if the float value is an integer AND the string representation doesn't contain
        # float signifiers like '.' or 'e'/'E'.
        if num == int(num) and '.' not in s_lower and 'e' not in s_lower:
            return "str:integer"
        else:
            return "str:float" # IEEE 754 floating-point
    except ValueError:
        pass
        
    # 6. Check for standardized timestamp formats
    for fmt in TIMESTAMP_FORMATS:
        try:
            datetime.datetime.strptime(s, fmt)
            return "str:timestamp"
        except ValueError:
            continue
            
    # 7. Default general string
    return "str:general"

def analyze_events_from_db():
    """
    Analyzes events by streaming them one-by-one from the database
    to ensure low memory usage.
    """
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_FILE)
    if not os.path.exists(db_path):
        logger.critical(f"Database file '{DB_FILE}' not found! Run 'fetch_and_store.py' first.")
        return

    logger.info(f"Connecting to database '{DB_FILE}' to stream and analyze events...")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        cursor = conn.cursor()
        
        # Get total number of docs for percentage calculations
        total_docs_cursor = conn.cursor()
        total_docs_cursor.execute(f"SELECT COUNT(id) FROM {TABLE_NAME}")
        total_docs = total_docs_cursor.fetchone()[0]
        if total_docs == 0:
            logger.warning("Database table is empty. No analysis to perform.")
            return

        # REDUCED MEMORY FOOTPRINT:
        # 'in_docs' is replaced by 'doc_presence_count' (an int) and 'last_doc_id' (to prevent double counting)
        # 'sizes' is replaced with running stats to avoid storing millions of size values
        paths_stats = defaultdict(lambda: {
            'is_aggregate': False, 'occurrences': 0, 'doc_presence_count': 0, 'last_doc_id': None,
            'empty_count': 0, 'size_sum': 0, 'size_count': 0, 'size_min': float('inf'), 
            'size_max': 0, 'types': Counter(), 'parent_path': None
        })

        def get_parent_path(path):
            if '.' not in path: return "ROOT"
            return '.'.join(path.split('.')[:-1])

        def traverse(node, doc_id, path):
            stats = paths_stats[path]
            stats['occurrences'] += 1
            
            # Memory Optimization: Only increment doc_presence_count if this is the first time
            # we see this path in the current document.
            if stats['last_doc_id'] != doc_id:
                stats['doc_presence_count'] += 1
                stats['last_doc_id'] = doc_id
                
            node_type = type(node)

            if node_type is dict:
                if stats['parent_path'] is None:
                    stats['parent_path'] = get_parent_path(path)
                stats['is_aggregate'] = True
                size = len(node)
                stats['size_sum'] += size
                stats['size_count'] += 1
                if size < stats['size_min']:
                    stats['size_min'] = size
                if size > stats['size_max']:
                    stats['size_max'] = size
                if size == 0:
                    stats['empty_count'] += 1
                else:
                    for key, value in node.items():
                        traverse(value, doc_id, f"{path}.{key}")
            elif node_type is list:
                if stats['parent_path'] is None:
                    stats['parent_path'] = get_parent_path(path)
                stats['is_aggregate'] = True
                size = len(node)
                stats['size_sum'] += size
                stats['size_count'] += 1
                if size < stats['size_min']:
                    stats['size_min'] = size
                if size > stats['size_max']:
                    stats['size_max'] = size
                if size == 0:
                    stats['empty_count'] += 1
                else:
                    path_array = f"{path}.[]"
                    for item in node:
                        traverse(item, doc_id, path_array)
            else:
                if stats['parent_path'] is None:
                    stats['parent_path'] = get_parent_path(path)
                if node_type is str:
                    stats['types'][characterize_string(node)] += 1
                else:
                    stats['types'][node_type.__name__] += 1

        logger.info("Analyzing JSON structures...")
        
        # Process rows in large batches to avoid loading entire table into memory
        BATCH_SIZE = 1000
        offset = 0
        
        with tqdm(total=total_docs, desc="Analyzing Events") as pbar:
            while offset < total_docs:
                batch_cursor = conn.cursor()
                batch_cursor.execute(f"SELECT id, data FROM {TABLE_NAME} LIMIT ? OFFSET ?", (BATCH_SIZE, offset))
                rows = batch_cursor.fetchall()
                
                if not rows:
                    break
                
                for row in rows:
                    doc_id = row['id']
                    try:
                        doc = json.loads(row['data'])
                        traverse(doc, doc_id, "ROOT")
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON for doc ID {doc_id}: {e}")
                        continue
                    
                    pbar.update(1)
                
                offset += BATCH_SIZE
                batch_cursor.close()

        # --- The reporting logic remains the same, but uses 'doc_presence_count' ---
        logger.info("Generating final reports...")
        aggregate_report, primitive_report = [], []
        for path, data in sorted(paths_stats.items()):
            if path == "ROOT": continue
            if data['is_aggregate']:
                # Use doc_presence_count instead of len(data['in_docs'])
                presence_count = data['doc_presence_count']
                presence_pct = (presence_count / total_docs) * 100
                empty_pct = (data['empty_count'] / data['occurrences']) * 100 if data['occurrences'] > 0 else 0
                avg_size = (data['size_sum'] / data['size_count']) if data['size_count'] > 0 else 0
                max_size = data['size_max'] if data['size_count'] > 0 else 0
                aggregate_report.append({"sort_key": presence_pct, "Path": path, "Type": "Array" if path.endswith("[]") else "Object", "Presence in Events": f"{presence_count} ({presence_pct:.1f}%)", "Times Empty": f"{data['empty_count']} ({empty_pct:.1f}%)", "Avg Size": f"{avg_size:.2f}", "Max Size": int(max_size)})
            else:
                parent_stats = paths_stats.get(data['parent_path'])
                if not parent_stats: continue
                parent_occurrences = parent_stats['occurrences']
                occurrences = data['occurrences']
                presence_in_parent_pct = (occurrences / parent_occurrences) * 100 if parent_occurrences > 0 else 0
                type_dist = ", ".join([f"{t} ({c})" for t, c in data['types'].most_common()])
                primitive_report.append({"sort_key": presence_in_parent_pct, "Path": path, "Presence in Parent": f"{occurrences} / {parent_occurrences} ({presence_in_parent_pct:.1f}%)", "Data Formats Found": type_dist})
        
        pd.set_option('display.max_rows', 500); pd.set_option('display.max_columns', 10); pd.set_option('display.width', 180)
        if aggregate_report:
            agg_df = pd.DataFrame(aggregate_report).sort_values(by="sort_key", ascending=False).drop(columns=["sort_key"])
            print("\n" + "="*120 + "\n" + " " * 45 + "C: Aggregate Node Analysis" + "\n" + " " * 38 + "(Sorted by Presence in Events %)" + "\n" + "="*120); print(agg_df.to_string(index=False))
        if primitive_report:
            prim_df = pd.DataFrame(primitive_report).sort_values(by="sort_key", ascending=False).drop(columns=["sort_key"])
            print("\n" + "="*120 + "\n" + " " * 45 + "D: Primitive Leaf Analysis" + "\n" + " " * 38 + "(Sorted by Presence in Parent %)" + "\n" + "="*120); print(prim_df.to_string(index=False))

    finally:
        conn.close()

if __name__ == "__main__":
    analyze_events_from_db()