import sqlite3
import json
import csv
import os
import logging
from tqdm import tqdm

# --- CONFIGURATION ---
POLY_CSV = os.getenv('POLY_CSV', os.path.expanduser('~/polymarket_data'))
DB_FILE = os.path.join(POLY_CSV, "polymarket_events.db")
TABLE_NAME = "events"
CSV_OUTPUT_FILE = os.path.join(POLY_CSV, "gamma_markets.csv")

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

def create_markets_csv():
    """
    Connects to the SQLite database, iterates through events, extracts all markets,
    and writes their specified attributes to a CSV file.
    """
    # Ensure POLY_CSV directory exists
    os.makedirs(POLY_CSV, exist_ok=True)
    
    db_path = DB_FILE
    if not os.path.exists(db_path):
        logger.critical(f"Database file '{db_path}' not found. Please ensure it's in the POLY_CSV directory.")
        return

    # These are the headers for the CSV file, in the desired order.
    # Typo corrections like 'holdingRewardsEnables' -> 'holdingRewardsEnabled' are handled here.
    fieldnames = [
        'id', 'active', 'createdAt', 'cyom', 'umaResolutionStatuses', 'description', 
        'feesEnabled', 'funded', 'archived', 'bestAsk', 'manualActivation', 
        'holdingRewardsEnabled', 'question', 'ready', 'rewardsMaxSpread', 'outcomes', 
        'rfqEnabled', 'rewardsMinSize', 'spread', 'requiresTranslation', 'restricted', 
        'conditionId', 'closed', 'startDate', 'rewardsDailyRate', 'marketMakerAddress', 
        'token_id1', 'token_id2', 'updatedAt', 'questionId', 'enableOrderBook', 'endDate', 
        'orderMinSize', 'orderPriceMinTickSize', 'acceptingOrders', 'outcomePrices', 
        'automaticallyActive', 'featured', 'volumeNum', 'acceptingOrdersTimestamp', 
        'automaticallyResolved', 'closedTime', 'lastTradePrice', 'umaEndDate', 
        'umaResolutionStatus', 'resolved', 'liquidityNum', 'liquidityCLOB', 
        'negriskId', 'gameId', 'negriskAugmented', 'tags'
    ]

    logger.info(f"Connecting to database '{DB_FILE}' to extract market data.")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # Get total number of events for the progress bar
        total_docs_cursor = conn.cursor()
        total_docs_cursor.execute(f"SELECT COUNT(id) FROM {TABLE_NAME}")
        total_docs = total_docs_cursor.fetchone()[0]
        if total_docs == 0:
            logger.warning("Database table is empty. No CSV will be generated.")
            return

        logger.info(f"Starting extraction of markets to '{CSV_OUTPUT_FILE}'.")
        
        with open(CSV_OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # Process in batches to avoid loading everything into memory
            batch_size = 10000
            offset = 0
            
            with tqdm(total=total_docs, desc="Processing Events") as pbar:
                while offset < total_docs:
                    cursor.execute(f"SELECT data FROM {TABLE_NAME} LIMIT ? OFFSET ?", (batch_size, offset))
                    batch = cursor.fetchall()
                    
                    if not batch:
                        break
                    
                    for row in batch:
                        event_data = json.loads(row['data'])
                        
                        # Get attributes from the root of the event that apply to all its markets
                        negrisk_augmented_parent = event_data.get('negRiskAugmented')

                        # Events might not have markets, so we get the list safely
                        markets_list = event_data.get('markets', [])
                        if not isinstance(markets_list, list):
                            continue # Skip if 'markets' isn't a list

                        for m in markets_list:
                            market_row = {}

                            # --- Direct Attribute Mapping ---
                            # This handles most of the requested fields by getting them from the market object `m`
                            for field in fieldnames:
                                # Use .get() to avoid errors if a key is missing, defaulting to None
                                market_row[field] = m.get(field)

                            # --- Special Handling & Name Corrections ---
                            # The user's list had some variations from the analysis output. We correct them here.
                            market_row['holdingRewardsEnabled'] = m.get('holdingRewardsEnabled')
                            market_row['questionId'] = m.get('questionID') # Case difference
                            market_row['liquidityCLOB'] = m.get('liquidityClob') # Typo correction
                            market_row['negriskId'] = m.get('negRiskMarketID') # Map to the most likely field
                            
                            # This field is from the parent event object, not the market object
                            market_row['negriskAugmented'] = negrisk_augmented_parent
                            
                            # Extract labels from parent event tags
                            parent_tags = event_data.get('tags', [])
                            labels = []
                            if isinstance(parent_tags, list):
                                for tag in parent_tags:
                                    if isinstance(tag, dict) and 'label' in tag:
                                        labels.append(tag['label'])
                            market_row['tags'] = json.dumps(labels)  # Store as JSON string

                            # Handle 'rewardsDailyRate' which is nested inside 'clobRewards'
                            # We'll take the first value found if it exists.
                            clob_rewards = m.get('clobRewards', [])
                            if clob_rewards and isinstance(clob_rewards, list) and len(clob_rewards) > 0:
                                market_row['rewardsDailyRate'] = clob_rewards[0].get('rewardsDailyRate')

                            # --- Handle `clobTokenIds` parsing for token_id1 and token_id2 ---
                            market_row['token_id1'] = None
                            market_row['token_id2'] = None
                            clob_token_ids_str = m.get('clobTokenIds')
                            if clob_token_ids_str and isinstance(clob_token_ids_str, str):
                                try:
                                    token_ids = json.loads(clob_token_ids_str)
                                    # Check if it's a list with exactly 2 items
                                    if isinstance(token_ids, list) and len(token_ids) == 2:
                                        market_row['token_id1'] = token_ids[0]
                                        market_row['token_id2'] = token_ids[1]
                                except json.JSONDecodeError:
                                    # The string was not valid JSON, so we leave the tokens as None
                                    pass
                            
                            writer.writerow(market_row)
                    
                    # Update progress bar and move to next batch
                    pbar.update(len(batch))
                    offset += batch_size
                    
                    # Force garbage collection after each batch
                    del batch

        logger.info(f"Successfully created '{CSV_OUTPUT_FILE}'.")

    except Exception as e:
        logger.error(f"An error occurred during CSV creation: {e}")
    finally:
        conn.close()
        logger.info("Database connection closed.")

if __name__ == "__main__":
    create_markets_csv()