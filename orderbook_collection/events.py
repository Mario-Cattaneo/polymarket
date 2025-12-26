import asyncio
import orjson
import time
import traceback
from typing import List, Tuple, Optional, Dict, Any, Set

from orderbook import OrderbookRegistry
from db_cli import DatabaseManager
from wss_cli import WebSocketTaskConfig, WebSocketTaskCallbacks, WebSocketManager


BOOKS_INSERTER_ID = "books_inserter"
PRICE_CHANGES_INSERTER_ID = "price_changes_inserter"
LAST_TRADE_PRICES_INSERTER_ID = "last_trade_prices_inserter"
TICK_CHANGES_INSERTER_ID = "tick_changes_inserter"
CONNECTIONS_INSERTER_ID = "connections_inserter"


class Events:
    def __init__(self, log: callable, db_man: DatabaseManager, orderbook_reg: OrderbookRegistry):
        self._log = log
        self._db_cli = db_man
        self._orderbook_registry = orderbook_reg

        self._url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        self._wss_callbacks = WebSocketTaskCallbacks(
            on_read=self._on_read, on_acknowledgement=self._on_ack, on_read_failure=self._on_read_failure,
            on_connect=self._on_connect, on_connect_failure=self._on_connect_failure
        )
        # Increased max_msg_size to 2MB to safely accommodate 10k asset ID batches
        self._wss_config = WebSocketTaskConfig(timeout=10, ping_interval=15, max_msg_size=2_097_152)
        
        # Single connection manager
        self._wss_cli = WebSocketManager(callbacks=self._wss_callbacks, config=self._wss_config, request_break_s=0.15, max_concurrent_subscriptions=1)

        self._limit_per_subscription = 10000
        
        self._active_tokens: Set[str] = set()
        self._initial_batch_tokens: Set[str] = set() # Track tokens handled by the persistent initial_payload
        self._pending_unsubscribes: Set[str] = set()
        
        self._started = False
        self._connection_started = False
        self._running_db_tasks: Dict[str, asyncio.Task] = {}
        self._running_internal_tasks: Dict[str, asyncio.Task] = {}
        
        # Signals
        self._markets_exhausted: asyncio.Event = asyncio.Event()
        self._reconnected_event: asyncio.Event = asyncio.Event()
        self._cleanup_event: asyncio.Event = asyncio.Event()
        
        self._token_delete_callback_handle: Optional[int] = None

        self._book_idx, self._price_change_idx, self._last_trade_price_idx, self._tick_change_idx = 0, 0, 0, 0
        self._book_rows, self._price_change_rows, self._last_trade_price_rows, self._tick_change_rows, self._connection_rows = [], [], [], [], []
        self._book_flag, self._price_change_flag, self._last_trade_price_flag, self._tick_change_flag, self._connection_flag = asyncio.Event(), asyncio.Event(), asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def start(self, markets_exhausted: asyncio.Event):
        if self._started: 
            self._log("start: Events module is already running.", "WARNING")
            return
        
        self._markets_exhausted = markets_exhausted
            
        try:
            self._start_db_inserters()
            
            # NOTE: We do NOT start the WS task here. 
            # We wait for the first batch of tokens in the manager to ensure we send a valid initial payload.

            manager_task = asyncio.create_task(self._subscription_manager())
            self._running_internal_tasks["subscription_manager"] = manager_task
            manager_task.add_done_callback(lambda t: self._running_internal_tasks.pop("subscription_manager", None))
            
            self._started = True
            self._log("start: Events module started.", "INFO")

        except Exception as e: 
            self._log(f"start: Failed with an unhandled exception: '{e}'", "FATAL")
            traceback.print_exc()
            self.stop()

    async def _subscription_manager(self):
        """
        Manages dynamic subscriptions, unsubscriptions, and re-subscriptions.
        """
        
        # --- PHASE 1: BOOTSTRAP ---
        # Wait for the first signal of markets to create the connection with a valid payload
        if not self._connection_started:
            self._log("subscription_manager: Waiting for initial market tokens to bootstrap connection...", "INFO")
            await self._markets_exhausted.wait()
            
            new_tokens = self._orderbook_registry.consume_exhaustion_cycle_batch()
            if not new_tokens:
                self._log("subscription_manager: Exhaustion signal received but no tokens found. Retrying...", "WARNING")
            
            # Prepare initial batch (max 10k)
            initial_batch = new_tokens[:self._limit_per_subscription]
            self._active_tokens.update(initial_batch)
            self._initial_batch_tokens = set(initial_batch)
            
            self._log(f"subscription_manager: Bootstrapping connection with {len(initial_batch)} tokens.", "INFO")
            
            # Start the connection with a real subscribe payload.
            initial_payload = orjson.dumps({"operation": "subscribe", "assets_ids": initial_batch}).decode('utf-8')
            self._wss_cli.start_task(0, self._url, initial_payload)
            self._connection_started = True

            # Wait for connection to be established with timeout
            try:
                await asyncio.wait_for(self._reconnected_event.wait(), timeout=20.0)
                self._log("subscription_manager: Bootstrap connection established.", "INFO")
            except asyncio.TimeoutError:
                self._log("subscription_manager: Timeout waiting for bootstrap connection. The server might be unreachable or rejecting the payload.", "ERROR")
            
            # Handle any remaining tokens from the first exhaustion cycle
            remaining_tokens = new_tokens[self._limit_per_subscription:]
            if remaining_tokens:
                self._log(f"subscription_manager: Subscribing to remaining {len(remaining_tokens)} tokens from bootstrap batch.", "INFO")
                self._active_tokens.update(remaining_tokens)
                
                for i in range(0, len(remaining_tokens), self._limit_per_subscription):
                    batch = remaining_tokens[i:i + self._limit_per_subscription]
                    payload = orjson.dumps({"operation": "subscribe", "assets_ids": batch}).decode('utf-8')
                    
                    # Send and Log
                    success = await self._wss_cli.send_message(0, payload)
                    batch_num = (i // self._limit_per_subscription) + 1
                    total_batches = (len(remaining_tokens) + self._limit_per_subscription - 1) // self._limit_per_subscription
                    
                    if success:
                        self._log(f"subscription_manager: Successfully sent bootstrap batch {batch_num}/{total_batches} ({len(batch)} tokens).", "INFO")
                    else:
                        self._log(f"subscription_manager: Failed to send bootstrap batch {batch_num}/{total_batches}. Will be handled by reconnection logic.", "WARNING")
            
            self._log("subscription_manager: Bootstrap phase complete. Entering main event loop.", "INFO")
            
            self._markets_exhausted.clear()
            self._reconnected_event.clear()

            if self._token_delete_callback_handle is None: 
                self._token_delete_callback_handle = self._orderbook_registry.register_delete_callback(self._token_delete_callback)

        # --- PHASE 2: EVENT LOOP ---
        while True:
            try:
                wait_tasks = [
                    asyncio.create_task(self._markets_exhausted.wait()),
                    asyncio.create_task(self._reconnected_event.wait()),
                    asyncio.create_task(self._cleanup_event.wait())
                ]

                done, pending = await asyncio.wait(
                    wait_tasks,
                    return_when=asyncio.FIRST_COMPLETED
                )

                for t in pending: t.cancel()

                # --- PRIORITY 1: Reconnection ---
                if self._reconnected_event.is_set():
                    self._reconnected_event.clear()
                    self._markets_exhausted.clear()
                    self._cleanup_event.clear()
                    
                    # wss_cli automatically resends the 'initial_payload' (the bootstrap batch).
                    # We only need to resubscribe to tokens that were NOT in that bootstrap batch.
                    tokens_to_resub = [t for t in self._active_tokens if t not in self._initial_batch_tokens]
                    
                    if tokens_to_resub:
                        self._log(f"subscription_manager: Reconnected. Resubscribing to {len(tokens_to_resub)} additional tokens...", "INFO")
                        for i in range(0, len(tokens_to_resub), self._limit_per_subscription):
                            batch = tokens_to_resub[i:i + self._limit_per_subscription]
                            payload = orjson.dumps({"operation": "subscribe", "assets_ids": batch}).decode('utf-8')
                            success = await self._wss_cli.send_message(0, payload)
                            if not success:
                                self._log("subscription_manager: Failed to send resubscribe batch. Connection dropped.", "WARNING")
                                break 

                # --- PRIORITY 2: New Markets ---
                elif self._markets_exhausted.is_set():
                    self._markets_exhausted.clear()
                    
                    new_tokens = self._orderbook_registry.consume_exhaustion_cycle_batch()
                    if not new_tokens: continue

                    unique_new_tokens = [t for t in new_tokens if t not in self._active_tokens]
                    
                    if unique_new_tokens:
                        self._log(f"subscription_manager: Found {len(unique_new_tokens)} new tokens to subscribe.", "INFO")
                        self._active_tokens.update(unique_new_tokens)
                        
                        for i in range(0, len(unique_new_tokens), self._limit_per_subscription):
                            batch = unique_new_tokens[i:i + self._limit_per_subscription]
                            payload = orjson.dumps({"operation": "subscribe", "assets_ids": batch}).decode('utf-8')
                            success = await self._wss_cli.send_message(0, payload)
                            if success:
                                self._log(f"subscription_manager: Sent subscribe batch {i//self._limit_per_subscription + 1} ({len(batch)} tokens).", "INFO")
                            else:
                                self._log("subscription_manager: Failed to send subscribe batch (socket not ready).", "WARNING")

                # --- PRIORITY 3: Unsubscriptions ---
                elif self._cleanup_event.is_set():
                    self._cleanup_event.clear()
                    
                    tokens_to_remove = list(self._pending_unsubscribes)
                    self._pending_unsubscribes.clear()
                    
                    if tokens_to_remove:
                        self._log(f"subscription_manager: Unsubscribing from {len(tokens_to_remove)} tokens.", "INFO")
                        for i in range(0, len(tokens_to_remove), self._limit_per_subscription):
                            batch = tokens_to_remove[i:i + self._limit_per_subscription]
                            payload = orjson.dumps({"operation": "unsubscribe", "assets_ids": batch}).decode('utf-8')
                            await self._wss_cli.send_message(0, payload)

            except asyncio.CancelledError:
                self._log("subscription_manager: Task cancelled.", "INFO")
                break
            except Exception as e:
                self._log(f"subscription_manager: Task failed with an unhandled exception: '{e}'", "ERROR")
                traceback.print_exc()
                await asyncio.sleep(5)

    def stop(self):
        self._log("stop: Stopping Events module...", "INFO")
        self._clean_up()
        self._started = False
        self._log("stop: Events module stopped.", "INFO")

    def _clean_up(self):
        if self._token_delete_callback_handle is not None:
            self._orderbook_registry.unregister_delete_callback(self._token_delete_callback_handle)
            self._token_delete_callback_handle = None

        self._wss_cli.purge_tasks()
        
        for task in self._running_db_tasks.values():
            if not task.done(): task.cancel()
        self._running_db_tasks.clear()
        
        for task in self._running_internal_tasks.values():
            if not task.done(): task.cancel()
        self._running_internal_tasks.clear()

    def _on_connect(self, task_offset: int) -> bool:
        self._log(f"on_connect: Task {task_offset}: Successfully connected.", "INFO")
        self._connection_rows.append((time.time_ns() // 1_000_000, True, None))
        self._connection_flag.set()
        self._reconnected_event.set()
        return True

    def _on_connect_failure(self, task_offset: int, exception: Exception, url: str, payload: str) -> bool:
        reason = f"{type(exception).__name__}: {exception}"
        payload_size = len(payload.encode('utf-8'))
        self._log(f"on_connect_failure: Task {task_offset} to URL '{url}' failed: {reason}. Payload size: {payload_size} bytes.", "WARNING")
        self._connection_rows.append((time.time_ns() // 1_000_000, False, reason[:250]))
        self._connection_flag.set()
        return True

    def _token_delete_callback(self, token: str) -> bool:
        if token in self._active_tokens:
            self._active_tokens.remove(token)
            self._pending_unsubscribes.add(token)
            self._cleanup_event.set()
            self._log(f"token_delete_callback: Scheduled unsubscription for {token}", "INFO")
        
        if self._orderbook_registry.get_token_count() == 0:
            self._log("token_delete_callback: All tokens removed. Stopping module.", "WARNING")
            self.stop()
            return False
        return True

    def get_buffer_sizes(self) -> Dict[str, int]:
        return {
            "book_rows": len(self._book_rows),
            "price_change_rows": len(self._price_change_rows),
            "last_trade_price_rows": len(self._last_trade_price_rows),
            "tick_change_rows": len(self._tick_change_rows),
            "connection_rows": len(self._connection_rows),
        }

    def task_summary(self) -> dict:
        db_tasks_running = len(self._running_db_tasks)
        internal_tasks_running = len(self._running_internal_tasks)
        wss_stats = self._wss_cli.get_all_stats()
        connection_summary = wss_stats.get("connection_summary", {})
        active_wss_tasks = connection_summary.get("connected", 0) + connection_summary.get("connecting", 0)
        total_running_tasks = db_tasks_running + internal_tasks_running + active_wss_tasks
        
        return {
            "total_running_tasks": total_running_tasks,
            "db_tasks": {"count": db_tasks_running, "tasks": list(self._running_db_tasks.keys())},
            "internal_tasks": {"count": internal_tasks_running, "tasks": list(self._running_internal_tasks.keys())},
            "websocket_client": wss_stats,
            "active_subscriptions": len(self._active_tokens)
        }

    def _start_db_inserters(self):
        self._log("start: Starting database inserters...", "INFO")
        self._running_db_tasks[BOOKS_INSERTER_ID] = self._db_cli.copy_persistent(task_id=BOOKS_INSERTER_ID, table_name="buffer_books_2", params_buffer=self._book_rows, signal=self._book_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure)
        self._running_db_tasks[PRICE_CHANGES_INSERTER_ID] = self._db_cli.copy_persistent(task_id=PRICE_CHANGES_INSERTER_ID, table_name="buffer_price_changes_2", params_buffer=self._price_change_rows, signal=self._price_change_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure)
        self._running_db_tasks[LAST_TRADE_PRICES_INSERTER_ID] = self._db_cli.copy_persistent(task_id=LAST_TRADE_PRICES_INSERTER_ID, table_name="buffer_last_trade_prices_2", params_buffer=self._last_trade_price_rows, signal=self._last_trade_price_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure)
        self._running_db_tasks[TICK_CHANGES_INSERTER_ID] = self._db_cli.copy_persistent(task_id=TICK_CHANGES_INSERTER_ID, table_name="buffer_tick_changes_2", params_buffer=self._tick_change_rows, signal=self._tick_change_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure)
        self._running_db_tasks[CONNECTIONS_INSERTER_ID] = self._db_cli.copy_persistent(task_id=CONNECTIONS_INSERTER_ID, table_name="buffer_events_connections_2", params_buffer=self._connection_rows, signal=self._connection_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure)

    def _on_read(self, task_offset: int, message: str) -> bool:
        try:
            event_data = orjson.loads(message)
        except orjson.JSONDecodeError as e:
            self._log(f"on_read: Task {task_offset}: Failed to decode JSON with '{e}'. Snippet: '{message[:200]}'", "WARNING")
            return True
        
        if isinstance(event_data, dict):
            self._handle_event_dict(event_data)
        elif isinstance(event_data, list):
            for i, item in enumerate(event_data):
                if not isinstance(item, dict):
                    self._log(f"on_read: Task {task_offset}: Skipping non-dict item at index {i} in event list. Item: '{str(item)[:150]}'", "WARNING")
                    continue
                self._handle_event_dict(item)
        return True

    def _on_ack(self, task_offset: int, message: str) -> bool:
        self._log(f"on_ack: task {task_offset} acked with len {len(message)}", "INFO")
        return self._on_read(task_offset, message)

    def _on_read_failure(self, task_offset: int, exception: Exception, url: str) -> bool:
        reason = f"{type(exception).__name__}: {exception}"
        self._log(f"on_read_failure: Task {task_offset} on URL '{url}' failed: {reason}", "ERROR")
        self._connection_rows.append((time.time_ns() // 1_000_000, False, reason[:250]))
        self._connection_flag.set()
        return True

    async def _on_insert_success(self, task_id: str, params: List[Tuple]):
        if task_id == BOOKS_INSERTER_ID: self._book_rows.clear()
        elif task_id == PRICE_CHANGES_INSERTER_ID: self._price_change_rows.clear()
        elif task_id == LAST_TRADE_PRICES_INSERTER_ID: self._last_trade_price_rows.clear()
        elif task_id == TICK_CHANGES_INSERTER_ID: self._tick_change_rows.clear()
        elif task_id == CONNECTIONS_INSERTER_ID: self._connection_rows.clear()

    async def _on_inserter_failure(self, task_id: str, exception: Exception, params: List[Tuple]):
        self._log(f"on_inserter_failure: DB inserter '{task_id}' failed with '{exception}' on {len(params)} params. Stopping module.", "FATAL")
        traceback.print_exc()
        self.stop()

    def _handle_event_dict(self, event: dict):
        event_type = event.get("event_type")
        
        if not event_type:
            self._log(f"handle_event_dict: Received generic message: {str(event)[:500]}", "INFO")
            return

        if event_type == "error":
            self._log(f"handle_event_dict: Received ERROR from server: {str(event)}", "ERROR")
            return
            
        if event_type == "info":
            self._log(f"handle_event_dict: Received INFO from server: {str(event)}", "INFO")
            return

        timestamp_str = event.get("timestamp")
        if not isinstance(timestamp_str, str) or not timestamp_str:
            return
        try:
            timestamp = int(timestamp_str)
        except (ValueError, TypeError):
            return

        if event_type == "book":
            self._parse_book_event(event, timestamp)
        elif event_type == "price_change":
            self._parse_price_change_event(event, timestamp)
        elif event_type == "last_trade_price":
            self._parse_last_trade_price_event(event, timestamp)
        elif event_type == "tick_size_change":
            self._parse_tick_size_event(event, timestamp)

    def _parse_book_event(self, event: dict, timestamp: int):
        asset_id = event.get("asset_id")
        if not isinstance(asset_id, str) or not asset_id: return
        self._book_rows.append((self._book_idx, time.time_ns() // 1_000_000, timestamp, asset_id,  orjson.dumps(event).decode('utf-8')))
        self._book_idx += 1
        self._book_flag.set()

    def _parse_price_change_event(self, event: dict, timestamp: int):
        price_changes = event.get("price_changes")
        if not isinstance(price_changes, list): return
        rows_added = 0
        for item in price_changes:
            if not isinstance(item, dict): continue
            asset_id = item.get("asset_id")
            if not isinstance(asset_id, str) or not asset_id: continue
            price_str = item.get("price")
            size_str = item.get("size")
            side = item.get("side")
            if not (isinstance(price_str, str) and isinstance(size_str, str) and side in ("BUY", "SELL")): continue
            try:
                price, size = float(price_str), float(size_str)
                self._price_change_rows.append((self._price_change_idx, time.time_ns() // 1_000_000, timestamp, asset_id, price, size, side,  orjson.dumps(item).decode('utf-8')))
                self._price_change_idx += 1
                rows_added += 1
            except (ValueError, TypeError): continue
        if rows_added > 0: self._price_change_flag.set()

    def _parse_last_trade_price_event(self, event: dict, timestamp: int):
        asset_id = event.get("asset_id")
        if not isinstance(asset_id, str) or not asset_id: return
        price_str = event.get("price")
        size_str = event.get("size")
        side = event.get("side")
        fee_rate_bps_str = event.get("fee_rate_bps")
        if not (isinstance(price_str, str) and isinstance(size_str, str) and side in ("BUY", "SELL") and isinstance(fee_rate_bps_str, str)): return
        try:
            price, size, fee_rate_bps = float(price_str), float(size_str), float(fee_rate_bps_str)
            self._last_trade_price_rows.append((self._last_trade_price_idx, time.time_ns() // 1_000_000, timestamp, asset_id, price, size, side,  fee_rate_bps, orjson.dumps(event).decode('utf-8')))
            self._last_trade_price_idx += 1
            self._last_trade_price_flag.set()
        except (ValueError, TypeError): return

    def _parse_tick_size_event(self, event: dict, timestamp: int):
        asset_id = event.get("asset_id")
        if not isinstance(asset_id, str) or not asset_id: return
        tick_size_str = event.get("new_tick_size")
        if not isinstance(tick_size_str, str): return
        try:
            tick_size = float(tick_size_str)
            self._tick_change_rows.append((self._tick_change_idx, time.time_ns() // 1_000_000, timestamp, asset_id, tick_size,  orjson.dumps(event).decode('utf-8')))
            self._tick_change_idx += 1
            self._tick_change_flag.set()
        except (ValueError, TypeError): return