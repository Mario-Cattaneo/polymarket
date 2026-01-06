import requests
import json
import re
from typing import List, Dict

BASE_URL = "https://gamma-api.polymarket.com/markets"
LIMIT = 500

def fetch_markets(offset: int = 0) -> List[Dict]:
    """
    Fetch markets from Polymarket API with pagination.
    """
    url = f"{BASE_URL}?limit={LIMIT}&offset={offset}"
    print(f"Fetching: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # API returns an array of market objects
        if isinstance(data, list):
            return data
        else:
            print(f"Unexpected response format: {type(data)}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Error fetching markets at offset {offset}: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON at offset {offset}: {e}")
        return []

def is_Bitcoin_15min_market(question: str) -> bool:
    """
    Check if a market question matches the Bitcoin Up or Down 15-minute format.
    Pattern: "Bitcoin Up or Down - [Month] [Day], [Time]-[Time] ET"
    """
    # Match pattern like "Bitcoin Up or Down - January 6, 3:15AM-3:30AM ET"
    pattern = r"^Bitcoin Up or Down\s*-\s*\w+\s+\d+,\s+\d+:\d+[AP]M-\d+:\d+[AP]M\s+ET$"
    return bool(re.match(pattern, question, re.IGNORECASE))

def fetch_all_markets() -> List[Dict]:
    """
    Fetch all markets by paginating through the API.
    """
    all_markets = []
    offset = 0
    
    while True:
        markets = fetch_markets(offset)
        
        if not markets:
            print(f"No more markets found at offset {offset}")
            break
        
        all_markets.extend(markets)
        print(f"Fetched {len(markets)} markets (total so far: {len(all_markets)})")
        
        # If we got fewer markets than the limit, we've reached the end
        if len(markets) < LIMIT:
            print(f"Reached end of results (got {len(markets)} < {LIMIT})")
            break
        
        offset += LIMIT
    
    return all_markets

def analyze_markets():
    """
    Main function to fetch all markets and count Bitcoin 15-min markets.
    """
    print("Starting to fetch all markets from Polymarket API...")
    print("=" * 60)
    
    all_markets = fetch_all_markets()
    
    print("\n" + "=" * 60)
    print(f"Total markets fetched: {len(all_markets)}")
    print("=" * 60)
    
    # Count and collect Bitcoin 15-min markets
    eth_15min_markets = []
    
    for market in all_markets:
        question = market.get("question", "")
        if is_Bitcoin_15min_market(question):
            eth_15min_markets.append(market)
    
    print(f"\nBitcoin Up or Down (15-min) markets found: {len(eth_15min_markets)}")
    
    if eth_15min_markets:
        print("\nSample Bitcoin 15-min markets:")
        for i, market in enumerate(eth_15min_markets[:5], 1):
            print(f"\n{i}. {market.get('question')}")
            print(f"   ID: {market.get('id')}")
            print(f"   Slug: {market.get('slug')}")
            print(f"   End Date: {market.get('endDate')}")
    
    # Additional statistics
    print("\n" + "=" * 60)
    print("Summary Statistics:")
    print(f"  Total markets: {len(all_markets)}")
    print(f"  Bitcoin 15-min markets: {len(eth_15min_markets)}")
    if len(all_markets) > 0:
        print(f"  Percentage: {len(eth_15min_markets) / len(all_markets) * 100:.2f}%")
    print("=" * 60)
    
    return all_markets, eth_15min_markets

if __name__ == "__main__":
    all_markets, eth_markets = analyze_markets()
