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
        
        # Websocket configuration
        self._max_msg_size = 1_048_576  # 1MB
        self._soft_msg_break_s = 0.1
        self._wss_config = WebSocketTaskConfig(timeout=10, ping_interval=15, max_msg_size=self._max_msg_size)
        self._wss_cli = WebSocketManager(callbacks=self._wss_callbacks, config=self._wss_config, request_break_s=self._soft_msg_break_s, max_concurrent_subscriptions=1)
        
        # Callback buffers and state
        self._subscribe_buffer: List[Tuple[str, str]] = []
        self._unsubscribe_buffer: List[str] = []
        self._subscribe_task_queued = False
        self._unsubscribe_task_queued = False
        self._last_send_time = 0.0
        self._max_tokens_per_batch = 10000  # Conservative batch size to stay under 1MB message limit
        
        self._started = False
        self._running_db_tasks: Dict[str, asyncio.Task] = {}
        
        self._market_found_callback_handle: Optional[int] = None
        self._token_delete_callback_handle: Optional[int] = None

        self._book_idx, self._price_change_idx, self._last_trade_price_idx, self._tick_change_idx = 0, 0, 0, 0
        self._book_rows, self._price_change_rows, self._last_trade_price_rows, self._tick_change_rows, self._connection_rows = [], [], [], [], []
        self._book_flag, self._price_change_flag, self._last_trade_price_flag, self._tick_change_flag, self._connection_flag = asyncio.Event(), asyncio.Event(), asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def start(self):
        if self._started: 
            self._log("start: Events module is already running.", "WARNING")
            return
            
        try:
            self._start_db_inserters()
            
            # Unconditionally start websocket connection without initial message
            self._wss_cli.start_task(0, self._url, "")
            self._log("start: Websocket connection started.", "INFO")
            
            # Register callbacks with the orderbook registry
            self._market_found_callback_handle = self._orderbook_registry.register_market_found_callback(self._on_market_found)
            self._token_delete_callback_handle = self._orderbook_registry.register_delete_callback(self._on_token_deleted)
            
            self._started = True
            self._log("start: Events module started.", "INFO")

        except Exception as e: 
            self._log(f"start: Failed with an unhandled exception: '{e}'", "FATAL")
            traceback.print_exc()
            self.stop()

    def _on_market_found(self, token_id_1: str, token_id_2: str):
        """
        Synchronous callback from registry when a new market is found.
        Buffers the token IDs and schedules rate-limited sending if needed.
        """
        self._subscribe_buffer.append((token_id_1, token_id_2))
        
        # Check if task already queued
        if self._subscribe_task_queued:
            return
        
        # Check when last message was sent
        current_time = time.time()
        time_since_last = current_time - self._last_send_time
        
        if time_since_last >= self._soft_msg_break_s:
            # Can send immediately
            asyncio.create_task(self._send_subscribe_buffer())
        else:
            # Schedule after remaining break time
            wait_time = self._soft_msg_break_s - time_since_last
            self._subscribe_task_queued = True
            asyncio.create_task(self._delayed_send_subscribe(wait_time))
    
    def _on_token_deleted(self, token_id: str):
        """
        Synchronous callback from registry when a token is deleted.
        Buffers the token ID and schedules rate-limited sending if needed.
        """
        self._unsubscribe_buffer.append(token_id)
        
        # Check if task already queued
        if self._unsubscribe_task_queued:
            return
        
        # Check when last message was sent
        current_time = time.time()
        time_since_last = current_time - self._last_send_time
        
        if time_since_last >= self._soft_msg_break_s:
            # Can send immediately
            asyncio.create_task(self._send_unsubscribe_buffer())
        else:
            # Schedule after remaining break time
            wait_time = self._soft_msg_break_s - time_since_last
            self._unsubscribe_task_queued = True
            asyncio.create_task(self._delayed_send_unsubscribe(wait_time))
    
    async def _delayed_send_subscribe(self, wait_time: float):
        """Waits for the required time then sends buffered subscribe messages."""
        await asyncio.sleep(wait_time)
        await self._send_subscribe_buffer()
        self._subscribe_task_queued = False
    
    async def _delayed_send_unsubscribe(self, wait_time: float):
        """Waits for the required time then sends buffered unsubscribe messages."""
        await asyncio.sleep(wait_time)
        await self._send_unsubscribe_buffer()
        self._unsubscribe_task_queued = False
    
    async def _send_subscribe_buffer(self):
        """Sends all buffered subscription messages and clears the buffer."""
        if not self._subscribe_buffer:
            return
        
        # Build list of all tokens to subscribe
        tokens = []
        for token_1, token_2 in self._subscribe_buffer:
            tokens.extend([token_1, token_2])
        
        payload = orjson.dumps({"operation": "subscribe", "assets_ids": tokens}).decode('utf-8')
        success = await self._wss_cli.send_message(0, payload)
        
        if success:
            self._log(f"_send_subscribe_buffer: Subscribed to {len(tokens)} tokens ({len(self._subscribe_buffer)} markets).", "INFO")
        else:
            self._log(f"_send_subscribe_buffer: Failed to send subscription for {len(tokens)} tokens (websocket not ready).", "WARNING")
        
        self._subscribe_buffer.clear()
        self._last_send_time = time.time()
    
    async def _send_unsubscribe_buffer(self):
        """Sends all buffered unsubscription messages and clears the buffer."""
        if not self._unsubscribe_buffer:
            return
        
        payload = orjson.dumps({"operation": "unsubscribe", "assets_ids": self._unsubscribe_buffer}).decode('utf-8')
        success = await self._wss_cli.send_message(0, payload)
        
        if success:
            self._log(f"_send_unsubscribe_buffer: Unsubscribed from {len(self._unsubscribe_buffer)} tokens.", "INFO")
        else:
            self._log(f"_send_unsubscribe_buffer: Failed to send unsubscription for {len(self._unsubscribe_buffer)} tokens (websocket not ready).", "WARNING")
        
        self._unsubscribe_buffer.clear()
        self._last_send_time = time.time()
    
    async def _resubscribe_all_tokens(self, tokens_list: List[str]):
        """Resubscribe to all active tokens in batches after reconnection."""
        if not tokens_list:
            return
        
        total_tokens = len(tokens_list)
        
        # Send subscriptions in batches to avoid exceeding 1MB message size
        for i in range(0, total_tokens, self._max_tokens_per_batch):
            batch = tokens_list[i:i + self._max_tokens_per_batch]
            payload = orjson.dumps({"operation": "subscribe", "assets_ids": batch}).decode('utf-8')
            
            success = await self._wss_cli.send_message(0, payload)
            
            batch_num = i // self._max_tokens_per_batch + 1
            total_batches = (total_tokens + self._max_tokens_per_batch - 1) // self._max_tokens_per_batch
            
            if success:
                self._log(f"_resubscribe_all_tokens: Resubscribed batch {batch_num}/{total_batches} ({len(batch)} tokens).", "INFO")
            else:
                self._log(f"_resubscribe_all_tokens: Failed to resubscribe batch {batch_num}/{total_batches} ({len(batch)} tokens).", "WARNING")
            
            # Rate limit between batches
            if i + self._max_tokens_per_batch < total_tokens:
                await asyncio.sleep(self._soft_msg_break_s)
        
        self._log(f"_resubscribe_all_tokens: Completed resubscription of {total_tokens} tokens in {(total_tokens + self._max_tokens_per_batch - 1) // self._max_tokens_per_batch} batches.", "INFO")

    def stop(self):
        self._log("stop: Stopping Events module...", "INFO")
        self._clean_up()
        self._started = False
        self._log("stop: Events module stopped.", "INFO")

    def _clean_up(self):
        if self._market_found_callback_handle is not None:
            self._orderbook_registry.unregister_market_found_callback(self._market_found_callback_handle)
            self._market_found_callback_handle = None
        
        if self._token_delete_callback_handle is not None:
            self._orderbook_registry.unregister_delete_callback(self._token_delete_callback_handle)
            self._token_delete_callback_handle = None

        self._wss_cli.purge_tasks()
        
        for task in self._running_db_tasks.values():
            if not task.done(): task.cancel()
        self._running_db_tasks.clear()

    def _on_connect(self, task_offset: int) -> bool:
        self._log(f"on_connect: Task {task_offset}: Successfully connected.", "INFO")
        self._connection_rows.append((time.time_ns() // 1_000_000, True, None))
        self._connection_flag.set()
        
        # Resubscribe to all active tokens from the registry after reconnection
        active_tokens = self._orderbook_registry.get_all_tokens()
        if active_tokens:
            self._log(f"on_connect: Resubscribing to {len(active_tokens)} active tokens from registry.", "INFO")
            asyncio.create_task(self._resubscribe_all_tokens(active_tokens))
        else:
            self._log(f"on_connect: No active tokens yet, skipping resubscription.", "INFO")
        
        return True

    def _on_connect_failure(self, task_offset: int, exception: Exception, url: str, payload: str) -> bool:
        reason = f"{type(exception).__name__}: {exception}"
        payload_size = len(payload.encode('utf-8'))
        self._log(f"on_connect_failure: Task {task_offset} to URL '{url}' failed: {reason}. Payload size: {payload_size} bytes.", "WARNING")
        self._connection_rows.append((time.time_ns() // 1_000_000, False, reason[:250]))
        self._connection_flag.set()
        return True

    def _token_delete_callback(self, token: str) -> bool:
        # Token deletion is handled by the registry, no local tracking needed
        # This callback is kept for potential future use
        
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
            "subscribe_buffer": len(self._subscribe_buffer),
            "unsubscribe_buffer": len(self._unsubscribe_buffer),
        }

    def task_summary(self) -> dict:
        db_tasks_running = len(self._running_db_tasks)
        wss_stats = self._wss_cli.get_all_stats()
        connection_summary = wss_stats.get("connection_summary", {})
        active_wss_tasks = connection_summary.get("connected", 0) + connection_summary.get("connecting", 0)
        total_running_tasks = db_tasks_running + active_wss_tasks
        
        return {
            "total_running_tasks": total_running_tasks,
            "db_tasks": {"count": db_tasks_running, "tasks": list(self._running_db_tasks.keys())},
            "websocket_client": wss_stats,
            "subscribe_task_queued": self._subscribe_task_queued,
            "unsubscribe_task_queued": self._unsubscribe_task_queued,
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