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
        self._wss_config = WebSocketTaskConfig(timeout=10, ping_interval=15)
        self._wss_cli = WebSocketManager(callbacks=self._wss_callbacks, config=self._wss_config, request_break_s=0.15, max_concurrent_subscriptions=40)

        self._limit_per_subscription = 10_000
        self._reserialize_threshold = 1.5
        self._loss_guard_s = 3
        self._last_subscription_time_ns = 0
        self._subscriptions: List[str] = []
        self._new_batch: List[str] = []
        self._deleted_count_since_reserialize = 0

        self._started = False
        self._running_db_tasks: Dict[str, asyncio.Task] = {}
        self._running_internal_tasks: Dict[str, asyncio.Task] = {}
        self._serializer_signal = asyncio.Event()
        self._markets_exhausted: asyncio.Event = asyncio.Event()
        
        self._bootstrap_callback_handle: Optional[int] = None
        self._token_insert_callback_handle: Optional[int] = None
        self._token_delete_callback_handle: Optional[int] = None

        self._book_idx = 0
        self._book_rows: List[Tuple] = []
        self._book_flag = asyncio.Event()

        self._price_change_idx = 0
        self._price_change_rows: List[Tuple] = []
        self._price_change_flag = asyncio.Event()

        self._last_trade_price_idx = 0
        self._last_trade_price_rows: List[Tuple] = []
        self._last_trade_price_flag = asyncio.Event()

        self._tick_change_idx = 0
        self._tick_change_rows: List[Tuple] = []
        self._tick_change_flag = asyncio.Event()

        self._connection_rows: List[Tuple] = []
        self._connection_flag = asyncio.Event()

    def get_buffer_sizes(self) -> Dict[str, int]:
        """
        Returns the current sizes of the internal database buffers.
        """
        return {
            "book_rows": len(self._book_rows),
            "price_change_rows": len(self._price_change_rows),
            "last_trade_price_rows": len(self._last_trade_price_rows),
            "tick_change_rows": len(self._tick_change_rows),
            "connection_rows": len(self._connection_rows),
        }

    def task_summary(self) -> dict:
        """
        Provides a summary of the current state and running tasks of the Events module.
        """
        db_tasks_running = len(self._running_db_tasks)
        internal_tasks_running = len(self._running_internal_tasks)

        wss_stats = self._wss_cli.get_all_stats()
        
        connection_summary = wss_stats.get("connection_summary", {})
        active_wss_tasks = connection_summary.get("connected", 0) + connection_summary.get("connecting", 0)

        total_running_tasks = db_tasks_running + internal_tasks_running + active_wss_tasks
        
        return {
            "total_running_tasks": total_running_tasks,
            "db_tasks": {
                "count": db_tasks_running,
                "tasks": list(self._running_db_tasks.keys())
            },
            "internal_tasks": {
                "count": internal_tasks_running,
                "tasks": list(self._running_internal_tasks.keys())
            },
            "websocket_client": wss_stats
        }
        
    async def start(self, markets_exhausted: asyncio.Event, restore = False):
        if self._started: 
            self._log("start: Events module is already running.", "WARNING")
            return
        if restore: 
            await self._restore_subscriptions_from_db()
        if self._orderbook_registry.get_token_count() == 0:
            self._log("start: No tokens in registry, setting up bootstrap callback.", "INFO")
            self._markets_exhausted = markets_exhausted
            if self._bootstrap_callback_handle is None: 
                self._bootstrap_callback_handle = self._orderbook_registry.register_insert_callback(self._bootstrap)
            return
        try:
            self._markets_exhausted = markets_exhausted; 
            self._start_db_inserters()
            
            self._orderbook_registry.iterate_over_tokens(self._token_insert_callback)

            if self._new_batch:
                offset = len(self._subscriptions)
                self._log(f"start: Creating final batch for remaining {len(self._new_batch)} tokens at offset {offset}", "INFO")
                payload = orjson.dumps({"type": "market", "initial_dump": True, "assets_ids": self._new_batch}).decode('utf-8')
                self._wss_cli.start_task(offset, self._url, payload)
                self._new_batch.clear()

            if self._token_insert_callback_handle is None: self._token_insert_callback_handle = self._orderbook_registry.register_insert_callback(self._token_insert_callback)
            if self._token_delete_callback_handle is None: self._token_delete_callback_handle = self._orderbook_registry.register_delete_callback(self._token_delete_callback)
            
            serializer_task = asyncio.create_task(self._serializer())
            self._running_internal_tasks["serializer"] = serializer_task
            serializer_task.add_done_callback(lambda t: self._running_internal_tasks.pop("serializer", None))
            
            self._started = True
            self._log("start: Events module started successfully.", "INFO")

        except Exception as e: self._log(f"start: Failed with an unhandled exception: '{e}'", "FATAL"); traceback.print_exc(); self.stop()
        

    async def _restore_subscriptions_from_db(self):
        self._log("restore_subscriptions_from_db: starting", "INFO")
        # UPDATED: markets_2
        stmt = "SELECT DISTINCT asset_id FROM markets_2 WHERE closed_time IS NULL;"
        try:
            results = await self._db_cli.fetch(task_id="get_all_active_asset_ids", stmt=stmt)
            if not results:
                self._log("No existing active tokens found in database to restore.", "INFO")
                return
            
            self._log(f"Found {len(results)} active tokens in database to restore.", "INFO")
            for record in results:
                asset_id = record.get("asset_id")
                if isinstance(asset_id, str):
                    self._orderbook_registry.insert_token(asset_id)
            self._log("Finished inserting restored tokens into registry.", "INFO")
        except Exception as e:
            self._log(f"Failed to restore subscriptions from database: {e!r}", "ERROR")

    def stop(self):
        self._log("stop: Stopping Events module...", "INFO")
        self._clean_up()
        self._started = False
        self._log("stop: Events module stopped.", "INFO")

    def _clean_up(self):
        if self._bootstrap_callback_handle is not None:
            self._orderbook_registry.unregister_insert_callback(self._bootstrap_callback_handle)
            self._bootstrap_callback_handle = None
        if self._token_insert_callback_handle is not None:
            self._orderbook_registry.unregister_insert_callback(self._token_insert_callback_handle)
            self._token_insert_callback_handle = None
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

    def _bootstrap(self, token: str):
        if self._bootstrap_callback_handle is not None:
            self._log(f"bootstrap: First token '{token}' received, starting main module.", "INFO")
            self._orderbook_registry.unregister_insert_callback(self._bootstrap_callback_handle)
            self._bootstrap_callback_handle = None
            task = asyncio.create_task(self.start(self._markets_exhausted, False))
            self._running_internal_tasks["bootstrap_starter"] = task
            task.add_done_callback(lambda t: self._running_internal_tasks.pop("bootstrap_starter", None))

    def _start_db_inserters(self):
        self._log("start: Starting database inserters...", "INFO")

        # UPDATED: buffer_books_2
        self._running_db_tasks[BOOKS_INSERTER_ID] = self._db_cli.copy_persistent(
            task_id=BOOKS_INSERTER_ID, table_name="buffer_books_2", params_buffer=self._book_rows,
            signal=self._book_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure
        )
        # UPDATED: buffer_price_changes_2
        self._running_db_tasks[PRICE_CHANGES_INSERTER_ID] = self._db_cli.copy_persistent(
            task_id=PRICE_CHANGES_INSERTER_ID, table_name="buffer_price_changes_2", params_buffer=self._price_change_rows,
            signal=self._price_change_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure
        )
        # UPDATED: buffer_last_trade_prices_2
        self._running_db_tasks[LAST_TRADE_PRICES_INSERTER_ID] = self._db_cli.copy_persistent(
            task_id=LAST_TRADE_PRICES_INSERTER_ID, table_name="buffer_last_trade_prices_2", params_buffer=self._last_trade_price_rows,
            signal=self._last_trade_price_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure
        )
        # UPDATED: buffer_tick_changes_2
        self._running_db_tasks[TICK_CHANGES_INSERTER_ID] = self._db_cli.copy_persistent(
            task_id=TICK_CHANGES_INSERTER_ID, table_name="buffer_tick_changes_2", params_buffer=self._tick_change_rows,
            signal=self._tick_change_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure
        )
        # UPDATED: buffer_events_connections_2
        self._running_db_tasks[CONNECTIONS_INSERTER_ID] = self._db_cli.copy_persistent(
            task_id=CONNECTIONS_INSERTER_ID, table_name="buffer_events_connections_2", params_buffer=self._connection_rows,
            signal=self._connection_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure
        )

    def _token_insert_callback(self, token: str) -> bool:
        offset = len(self._subscriptions)
        if len(self._new_batch) >= self._limit_per_subscription:
            self._log(f"token_insert_callback: new full batch at offset {offset}")
            payload = orjson.dumps({"type": "market", "initial_dump": True, "assets_ids": self._new_batch}).decode('utf-8')
            self._wss_cli.start_task(offset, self._url, payload)
            self._subscriptions.append(payload)
            self._new_batch.clear()
            self._markets_exhausted.clear()

        self._new_batch.append(token)
        if self._markets_exhausted.is_set():
            self._log(f"token_insert_callback: markets exhausted")
            payload = orjson.dumps({"type": "market", "initial_dump": True, "assets_ids": self._new_batch}).decode('utf-8')
            self._wss_cli.start_task(offset, self._url, payload)
            self._markets_exhausted.clear()
        return True


    def _token_delete_callback(self, token: str)->bool:
        self._deleted_count_since_reserialize += 1
        
        if self._orderbook_registry.get_token_count() == 0:
            self._log("token_delete_callback: All tokens removed. Stopping and resetting to bootstrap.", "WARNING")
            self.stop()
            if self._bootstrap_callback_handle is None:
                self._bootstrap_callback_handle = self._orderbook_registry.register_insert_callback(self._bootstrap)
            return False

        if self._deleted_count_since_reserialize > self._limit_per_subscription * self._reserialize_threshold:
            self._log("token_delete_callback: Deletion threshold met, triggering serializer.", "INFO")
            self._deleted_count_since_reserialize = 0
            self._serializer_signal.set()
        
        return True

    def _serialize(self, token: str)->bool:
        if len(self._new_batch) >= self._limit_per_subscription:
            self._subscriptions.append(orjson.dumps({"type": "market", "initial_dump": True, "assets_ids": self._new_batch}).decode('utf-8'))
            self._new_batch.clear()
            self._markets_exhausted.clear()
        self._new_batch.append(token)
        return True

    async def _serializer(self):
        while True:
            try:
                await self._serializer_signal.wait()
                self._serializer_signal.clear()

                old_subscription_count = len(self._subscriptions)
                if self._new_batch:
                    old_subscription_count += 1
                    
                self._log(f"serializer: Re-serializing subscriptions.")
                self._new_batch.clear()
                self._subscriptions.clear()
                self._orderbook_registry.iterate_over_tokens(self._serialize)
                
                new_subscription_count = len(self._subscriptions)
                for offset in range(new_subscription_count):
                    self._wss_cli.start_task(offset, self._url, self._subscriptions[offset])
                
                if self._new_batch:
                    final_payload = orjson.dumps({"type": "market", "initial_dump": True, "assets_ids": self._new_batch}).decode('utf-8')
                    self._wss_cli.start_task(new_subscription_count, self._url, final_payload)
                    new_subscription_count += 1

                if old_subscription_count > new_subscription_count:
                    self._log(f"serializer: waiting {self._loss_guard_s}s before terminating old connections.", "INFO")
                    await asyncio.sleep(self._loss_guard_s)
                    
                    self._log(f"serializer: Terminating {old_subscription_count - new_subscription_count} old subscription tasks.", "INFO")
                    for offset in range(new_subscription_count, old_subscription_count):
                        self._wss_cli.terminate_task(offset)

            except asyncio.CancelledError:
                self._log("serializer: Task cancelled.", "INFO")
                break
            except Exception as e:
                self._log(f"serializer: Task failed with '{e}'", "ERROR")
                traceback.print_exc()

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
        try:
            event_data = orjson.loads(message)
        except orjson.JSONDecodeError as e:
            self._log(f"on_read: Task {task_offset}: Failed to decode JSON with '{e}'. Snippet: '{message[:200]}'", "WARNING")
            return True
        if not isinstance(event_data, list):
            self._log(f"non list ack {message[:300]}", "WARNING")
            return True

        
        self._log(f"on_ack: task {task_offset} acked with len", "INFO")
        return self._on_read(task_offset, message)

    def _on_connect(self, task_offset: int) -> bool:
        self._log(f"on_connect: Task {task_offset}: Successfully connected.", "INFO")
        self._connection_rows.append((time.time_ns() // 1_000_000, True, None))
        self._connection_flag.set()
        return True

    def _on_connect_failure(self, task_offset: int, exception: Exception, url: str, payload: str) -> bool:
        reason = f"{type(exception).__name__}: {exception}"
        self._log(f"on_connect_failure: Task {task_offset} to URL '{url}' failed: {reason}", "WARNING")
        self._connection_rows.append((time.time_ns() // 1_000_000, False, reason[:250]))
        self._connection_flag.set()
        return True

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
        self._log(f"on_inserter_failure: {task_id} failed with '{exception}' on {len(params)} params. Stopping module.", "FATAL")
        self.stop()

    def _handle_event_dict(self, event: dict):
        event_type = event.get("event_type")
        if not isinstance(event_type, str) or not event_type:
            self._log(f"handle_event_dict: Skipping event with invalid 'event_type'. Event: '{str(event)[:300]}'", "WARNING")
            return

        timestamp_str = event.get("timestamp")
        if not isinstance(timestamp_str, str) or not timestamp_str:
            self._log(f"handle_event_dict: Skipping event '{event_type}' due to invalid 'timestamp'. Event: '{str(event)[:300]}'", "WARNING")
            return
        try:
            timestamp = int(timestamp_str)
        except (ValueError, TypeError):
            self._log(f"handle_event_dict: Skipping event '{event_type}' due to unparseable timestamp: '{timestamp_str}'.", "WARNING")
            return

        if event_type == "book":
            self._parse_book_event(event, timestamp)
        elif event_type == "price_change":
            self._parse_price_change_event(event, timestamp)
        elif event_type == "last_trade_price":
            self._parse_last_trade_price_event(event, timestamp)
        elif event_type == "tick_size_change":
            self._parse_tick_size_event(event, timestamp)
        else:
            self._log(f"handle_event_dict: Received unhandled event_type: '{event_type}'.", "INFO")

    def _parse_book_event(self, event: dict, timestamp: int):
        asset_id = event.get("asset_id")
        if not isinstance(asset_id, str) or not asset_id:
            self._log(f"parse_book_event: Skipping book event due to missing or invalid 'asset_id'. Event: '{str(event)[:300]}'", "WARNING")
            return
        
        self._book_rows.append((self._book_idx, time.time_ns() // 1_000_000, timestamp, asset_id,  orjson.dumps(event).decode('utf-8')))
        self._book_idx += 1
        self._book_flag.set()

    def _parse_price_change_event(self, event: dict, timestamp: int):
        price_changes = event.get("price_changes")
        if not isinstance(price_changes, list):
            self._log(f"parse_price_change_event: Expected 'price_changes' to be a list, but got {type(price_changes)}. Event: '{str(event)[:300]}'", "WARNING")
            return
            
        rows_added = 0
        for item in price_changes:
            if not isinstance(item, dict): 
                continue

            asset_id = item.get("asset_id")
            if not isinstance(asset_id, str) or not asset_id:
                self._log(f"parse_price_change_event: Skipping item due to missing or invalid 'asset_id'. Item: '{str(item)[:300]}'", "WARNING")
                continue

            price_str = item.get("price")
            if not isinstance(price_str, str):
                self._log(f"parse_price_change_event: Skipping item for asset '{asset_id}' due to missing 'price'. Item: '{str(item)[:300]}'", "WARNING")
                continue

            size_str = item.get("size")
            if not isinstance(size_str, str):
                self._log(f"parse_price_change_event: Skipping item for asset '{asset_id}' due to missing 'size'. Item: '{str(item)[:300]}'", "WARNING")
                continue

            side = item.get("side")
            if not isinstance(side, str) or side not in ("BUY", "SELL"):
                self._log(f"parse_price_change_event: Skipping item for asset '{asset_id}' due to invalid 'side'. Item: '{str(item)[:300]}'", "WARNING")
                continue

            try:
                price, size = float(price_str), float(size_str)
                self._price_change_rows.append((self._price_change_idx, time.time_ns() // 1_000_000, timestamp, asset_id, price, size, side,  orjson.dumps(item).decode('utf-8')))
                self._price_change_idx += 1
                rows_added += 1
            except (ValueError, TypeError):
                self._log(f"parse_price_change_event: Skipping item for asset '{asset_id}' due to unparseable numeric values. Item: '{item}'", "WARNING")
                continue
        
        if rows_added > 0:
            self._price_change_flag.set()

    def _parse_last_trade_price_event(self, event: dict, timestamp: int):
        asset_id = event.get("asset_id")
        if not isinstance(asset_id, str) or not asset_id:
            self._log(f"parse_last_trade_price_event: Skipping event due to missing or invalid 'asset_id'. Event: '{str(event)[:300]}'", "WARNING")
            return

        price_str = event.get("price")
        if not isinstance(price_str, str):
            self._log(f"parse_last_trade_price_event: Skipping event for asset '{asset_id}' due to missing 'price'. Event: '{str(event)[:300]}'", "WARNING")
            return

        size_str = event.get("size")
        if not isinstance(size_str, str):
            self._log(f"parse_last_trade_price_event: Skipping event for asset '{asset_id}' due to missing 'size'. Event: '{str(event)[:300]}'", "WARNING")
            return

        side = event.get("side")
        if not isinstance(side, str) or side not in ("BUY", "SELL"):
            self._log(f"parse_last_trade_price_event: Skipping event for asset '{asset_id}' due to invalid 'side'. Event: '{str(event)[:300]}'", "WARNING")
            return

        fee_rate_bps_str = event.get("fee_rate_bps")
        if not isinstance(fee_rate_bps_str, str):
            self._log(f"parse_last_trade_price_event: Skipping event for asset '{asset_id}' due to missing 'fee_rate_bps'. Event: '{str(event)[:300]}'", "WARNING")
            return

        try:
            price = float(price_str)
            size = float(size_str)
            fee_rate_bps = float(fee_rate_bps_str)
        except (ValueError, TypeError):
            self._log(f"parse_last_trade_price_event: Skipping event for asset '{asset_id}' due to unparseable numeric values. Event: '{event}'", "WARNING")
            return
            
        self._last_trade_price_rows.append((self._last_trade_price_idx, time.time_ns() // 1_000_000, timestamp, asset_id, price, size, side,  fee_rate_bps, orjson.dumps(event).decode('utf-8')))
        self._last_trade_price_idx += 1
        self._last_trade_price_flag.set()

    def _parse_tick_size_event(self, event: dict, timestamp: int):
        asset_id = event.get("asset_id")
        if not isinstance(asset_id, str) or not asset_id:
            self._log(f"parse_tick_size_event: Skipping event due to missing or invalid 'asset_id'. Event: '{str(event)[:300]}'", "WARNING")
            return

        tick_size_str = event.get("new_tick_size")
        if not isinstance(tick_size_str, str):
            self._log(f"parse_tick_size_event: Skipping event for asset '{asset_id}' due to missing or invalid 'new_tick_size'. Event: '{str(event)[:300]}'", "WARNING")
            return

        try:
            tick_size = float(tick_size_str)
        except (ValueError, TypeError):
            self._log(f"parse_tick_size_event: Skipping event for asset '{asset_id}' due to unparseable 'new_tick_size': '{tick_size_str}'.", "WARNING")
            return

        self._tick_change_rows.append((self._tick_change_idx, time.time_ns() // 1_000_000, timestamp, asset_id, tick_size,  orjson.dumps(event).decode('utf-8')))
        self._tick_change_idx += 1
        self._tick_change_flag.set()