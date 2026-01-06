import asyncio
import orjson
import websockets


async def send_ping(websocket: websockets.WebSocketClientProtocol, interval=5):
    """Send ping to keep the WebSocket connection alive."""
    try:
        while True:
            await asyncio.sleep(interval)  # Wait for the specified interval
            await websocket.send("PING")
    except websockets.ConnectionClosed:
        print("WebSocket connection closed. Stopping ping.")
        # Stop ping task when the connection closes
    except asyncio.CancelledError:
        print("Ping task canceled.")


async def receive_messages(ws: websockets.WebSocketClientProtocol):
    """Receive messages from the WebSocket connection."""
    try:
        while True:
            message = await ws.recv()
            if message != "PONG":
                m = {}
                try:
                    m = orjson.loads(message)
                except Exception as e:
                    print(f"Error parsing message: {e} - {message}", flush=True)
                    continue

                if not isinstance(m, dict):
                    continue

                event_type = m.get("event_type")
                match event_type:
                    case "new_market":
                        print(f"New market: {m}")
                        await ws.send(
                            orjson.dumps(
                                {
                                    "operation": "subscribe",
                                    "assets_ids": m.get("assets_ids"),
                                    "custom_feature_enabled": True,
                                }
                            ).decode("utf-8")
                        )
                    case "market_resolved":
                        print(f"Market resolved: {m}")
                        await ws.send(
                            orjson.dumps(
                                {
                                    "operation": "unsubscribe",
                                    "assets_ids": m.get("assets_ids"),
                                    "custom_feature_enabled": True,
                                }
                            ).decode("utf-8")
                        )
                    case "best_bid_ask":
                        print(f"Best bid ask: {m}")
                    case "book":
                        print(f"Book: {m}")
                        pass
                    case "last_trade_price":
                        print(f"Last trade price: {m}")
                        pass
                    case "tick_size_change":
                        print(f"Tick size change: {m}")
                        pass
                    case "price_change":
                        print(f"Price change: {m}")
                        pass

    except websockets.ConnectionClosed:
        print("WebSocket connection closed. Stopping receive messages.")
    except asyncio.CancelledError:
        print("Receive messages task canceled.")


async def main():
    url_websocket_market_data = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    while True:
        try:
            ws = await websockets.connect(
                url_websocket_market_data,
                ping_interval=10,
                ping_timeout=10,
                open_timeout=10,
                close_timeout=10,
            )
            await ws.send(
                orjson.dumps({"custom_feature_enabled": True, "assets_ids": []}).decode(
                    "utf-8"
                )
            )
            print("Connected")

            ping_task = asyncio.create_task(send_ping(ws), name="ping")
            recv_task = asyncio.create_task(receive_messages(ws), name="recv")

            try:
                # This returns/raises as soon as either task errors (or ends)
                done, _ = await asyncio.wait(
                    {ping_task, recv_task},
                    return_when=asyncio.FIRST_EXCEPTION,
                )

                # If one finished because of exception, surface it
                for t in done:
                    t.result()  # raises if failed

            finally:
                for t in (ping_task, recv_task):
                    t.cancel()
                await asyncio.gather(ping_task, recv_task, return_exceptions=True)

        except Exception as e:
            print(f"Error: {e}")
            print("Connection closed. Reconnecting...")
            continue


if __name__ == "__main__":
    asyncio.run(main())