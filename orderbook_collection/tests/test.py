import asyncio
import orjson
import aiohttp
import websockets
import logging


TEST_MODE = "COMBINED"

ASSETS_CLOSED = [
    "23957885615115430922384185661294483989521212430808224513177413172438775950057",
    "44065917169138815451032058926556960033374557137879250075091545322436931840853"
]
ASSETS_OPEN = [
    "13258564955502292700175505967514180269278255407255384581475000155653305646788"
]
ASSETS_COMBINED = ASSETS_OPEN + ASSETS_CLOSED

EVENTS_WSS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
ANALYTICS_HTTP_URL = "https://clob.polymarket.com/books"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


async def subscribe_to_events(asset_ids: list[str]):
    """
    Connects to the Events WebSocket, subscribes to assets, and prints messages.
    """
    logging.info("[EVENTS] Preparing to subscribe to %d assets.", len(asset_ids))
    
    subscription_payload = {
        "type": "market",
        "initial_dump": True,
        "assets_ids": asset_ids
    }
    
    try:
        async with websockets.connect(EVENTS_WSS_URL) as websocket:
            logging.info("[EVENTS] WebSocket connection established.")
            
            await websocket.send(orjson.dumps(subscription_payload))
            logging.info("[EVENTS] Subscription payload sent.")
            
            logging.info("[EVENTS] --- Waiting for acknowledgment message ---")
            ack_message = await websocket.recv()
            
            print("\n" + "="*25)
            print("  EVENTS ACK RECEIVED")
            print("="*25)
            try:
                parsed_json = orjson.loads(ack_message)
                print(f"Number of assets in response: {len(parsed_json)}")
                print(orjson.dumps(parsed_json, option=orjson.OPT_INDENT_2).decode())
            except orjson.JSONDecodeError:
                print(ack_message)
            print("="*25 + "\n")

    except websockets.exceptions.ConnectionClosed as e:
        logging.error("[EVENTS] WebSocket connection closed unexpectedly: %s", e)
    except Exception as e:
        logging.error("[EVENTS] An error occurred in the WebSocket client: %s", e)


async def make_analytics_post_request(asset_ids: list[str]):
    """
    Sends a POST request to the Analytics endpoint and prints the response.
    """
    logging.info("[ANALYTICS] Preparing POST request for %d assets.", len(asset_ids))
    
    analytics_payload = [{"token_id": token} for token in asset_ids]
    
    try:
        async with aiohttp.ClientSession() as session:
            logging.info("[ANALYTICS] Sending POST request to %s", ANALYTICS_HTTP_URL)
            async with session.post(ANALYTICS_HTTP_URL, json=analytics_payload) as response:
                
                print("\n" + "="*35)
                print("  ANALYTICS RESPONSE RECEIVED")
                print("="*35)
                print(f"Status Code: {response.status}")
                
                response_json = await response.json()
                print(f"Number of assets in response: {len(response_json)}")
                print("Response Body (JSON):")
                print(orjson.dumps(response_json, option=orjson.OPT_INDENT_2).decode())
                print("="*35 + "\n")

    except aiohttp.ClientError as e:
        logging.error("[ANALYTICS] An aiohttp client error occurred: %s", e)
    except Exception as e:
        logging.error("[ANALYTICS] An error occurred in the HTTP client: %s", e)


async def main():
    """Main function to run the tests."""
    if TEST_MODE == "CLOSED":
        logging.info(">>> Using asset list: ASSETS_CLOSED <<<")
        selected_assets = ASSETS_CLOSED
    elif TEST_MODE == "OPEN":
        logging.info(">>> Using asset list: ASSETS_OPEN <<<")
        selected_assets = ASSETS_OPEN
    elif TEST_MODE == "COMBINED":
        logging.info(">>> Using asset list: ASSETS_COMBINED <<<")
        selected_assets = ASSETS_COMBINED
    else:
        logging.error("Invalid TEST_MODE: %s. Please use 'CLOSED', 'OPEN', or 'COMBINED'.", TEST_MODE)
        return
        
    # Run both tasks concurrently
    await asyncio.gather(
        subscribe_to_events(selected_assets),
        make_analytics_post_request(selected_assets)
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Program interrupted by user.")