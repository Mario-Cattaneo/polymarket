import asyncio
import orjson
import time
import traceback
from typing import List, Tuple, Optional, Dict, Any

from orderbook import OrderbookRegistry
from db_cli import DatabaseManager
from http_cli import (HttpTaskConfig, HttpTaskCallbacks, HttpManager, Method, 
                      RequestProducer, ResponseContent, StatusCode, RTT)

SERVER_BOOK_INSERTER_ID = "server_book_inserter"

RTT_INSERTER_ID = "analytics_rtt_inserter"

MISSED_UPDATER_ID = "missed_count_updater"
# *** CORRECTED LINE: Updated to use 'markets_4' and 'token_id' ***
MISSED_UPDATE_STMT = "UPDATE markets_4 SET missed_before_gone = $1 WHERE token_id = $2"

SERIALIZER_ID = "serializer"

class Analytics:
    def __init__(self, log: callable, http_man: HttpManager, db_man: DatabaseManager, orderbook_reg: OrderbookRegistry):
        self._log = log
        self._db_cli = db_man
        self._http_cli = http_man
        self._orderbook_registry = orderbook_reg

        self._found_index = 0

        self._server_book_rows: List[Tuple] = []
        self._server_book_flag = asyncio.Event()
        
        self._rtt_rows: List[Tuple] = []
        self._rtt_flag = asyncio.Event()
        
        self._missed_update_rows: List[Tuple] = []
        self._missed_update_flag = asyncio.Event()

        self._http_config = HttpTaskConfig(
            base_back_off_s=1.0, max_back_off_s=150.0,
            back_off_rate=2.0, request_break_s=0.03
        )
        self._http_callbacks = HttpTaskCallbacks(
            next_request=self._next_request,
            on_response=self._on_response,
            on_exception=self._on_exception
        )
        
        self._limit = 500
        self._tokens: List[set[str]] = [set()]
        self._payload_offset: int = 0
        self._payloads: List[bytes] = []
        self._new_batch: List[Dict[str, str]] = []

        self._started = False
        self._poster_task: Optional[RequestProducer] = None
        self._running_db_tasks: Dict[str, asyncio.Task] = {}
        self._running_internal_tasks: Dict[str, asyncio.Task] = {}
        
        self._serializer_signal = asyncio.Event()
        self._reserialize_threshold = 2.5
        self._deleted_token_count = 0 

        self._generation_id: int = 0

        self._miss_counts: Dict[str, int] = {} 

        self._bootstrap_callback_handle: Optional[int] = None
        self._insert_token_callback_handle: Optional[int] = None
        self._delete_token_callback_handle: Optional[int] = None

    def get_buffer_sizes(self) -> Dict[str, int]:
        return {
            "server_book_rows": len(self._server_book_rows),
            "rtt_rows": len(self._rtt_rows),
            "missed_update_rows": len(self._missed_update_rows),
        }

    def task_summary(self) -> dict:
        db_tasks_running = len(self._running_db_tasks)
        internal_tasks_running = len(self._running_internal_tasks)
        
        poster_stats = {}
        total_running = db_tasks_running + internal_tasks_running

        if self._poster_task:
            poster_stats = self._poster_task.to_dict()
            if poster_stats.get("running"):
                total_running += 1 + poster_stats.get("active_request_count", 0)
        
        return {
            "total_running_tasks": total_running,
            "db_tasks": {"count": db_tasks_running, "tasks": list(self._running_db_tasks.keys())},
            "internal_tasks": {"count": internal_tasks_running, "tasks": list(self._running_internal_tasks.keys())},
            "analytics_poster": poster_stats,
        }

    async def start(self, restore: bool = False):
        if self._started:
            self._log("start: Analytics module is already running.", "WARNING")
            return

        if self._orderbook_registry.get_token_count() == 0:
            self._log("start: No tokens in registry, setting up bootstrap callback.", "INFO")
            if self._bootstrap_callback_handle is None:
                self._bootstrap_callback_handle = self._orderbook_registry.register_insert_callback(self._bootstrap)
            return

        try:
            self._orderbook_registry.iterate_over_tokens(self._token_insert_callback)
            self._start_db_inserters()
            self._start_http_producer()
            self._start_serializer()
            
            if self._insert_token_callback_handle is None:
                self._insert_token_callback_handle = self._orderbook_registry.register_insert_callback(self._token_insert_callback)
            
            if self._delete_token_callback_handle is None:
                self._delete_token_callback_handle = self._orderbook_registry.register_delete_callback(self._token_delete_callback)
            
            self._started = True
            self._log("start: Analytics module started successfully.", "INFO")

        except Exception as e:
            self._log(f"start: Failed with an unhandled exception: '{e}'\n{traceback.format_exc()}", "FATAL")
            self.stop()

    def stop(self):
        self._log("stop: Stopping Analytics module...", "INFO")
        self._clean_up()
        self._started = False
        self._log("stop: Analytics module stopped.", "INFO")

    def _clean_up(self):
        if self._bootstrap_callback_handle is not None:
            self._orderbook_registry.unregister_insert_callback(self._bootstrap_callback_handle)
            self._bootstrap_callback_handle = None
        if self._insert_token_callback_handle is not None:
            self._orderbook_registry.unregister_insert_callback(self._insert_token_callback_handle)
            self._insert_token_callback_handle = None
        if self._delete_token_callback_handle is not None:
            self._orderbook_registry.unregister_delete_callback(self._delete_token_callback_handle)
            self._delete_token_callback_handle = None

        if self._poster_task:
            self._poster_task.stop()
            self._poster_task = None
        
        for task in self._running_db_tasks.values():
            if not task.done(): task.cancel()
        self._running_db_tasks.clear()
        
        for task in self._running_internal_tasks.values():
            if not task.done(): task.cancel()
        self._running_internal_tasks.clear()

        self._server_book_rows.clear(); self._rtt_rows.clear(); self._missed_update_rows.clear()
        self._tokens = [set()]; self._payloads = []; self._new_batch = []
        self._payload_offset = 0; self._deleted_token_count = 0

    def _bootstrap(self, token: str):
        if self._bootstrap_callback_handle is not None:
            self._log(f"bootstrap: First token received, starting main module.", "INFO")
            self._orderbook_registry.unregister_insert_callback(self._bootstrap_callback_handle)
            self._bootstrap_callback_handle = None
            task = asyncio.create_task(self.start())
            self._running_internal_tasks["bootstrap_starter"] = task
            task.add_done_callback(lambda t: self._running_internal_tasks.pop("bootstrap_starter", None))

    def _start_db_inserters(self):
        self._log("start: Starting database inserters...", "INFO")
        self._running_db_tasks[SERVER_BOOK_INSERTER_ID] = self._db_cli.copy_persistent(
            task_id=SERVER_BOOK_INSERTER_ID, table_name="buffer_server_book_2" , params_buffer=self._server_book_rows,
            signal=self._server_book_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure
        )
        self._running_db_tasks[RTT_INSERTER_ID] = self._db_cli.copy_persistent(
            task_id=RTT_INSERTER_ID, table_name="buffer_analytics_rtt_2", params_buffer=self._rtt_rows,
            signal=self._rtt_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure
        )
        self._running_db_tasks[MISSED_UPDATER_ID] = self._db_cli.exec_persistent(
            task_id=MISSED_UPDATER_ID, stmt=MISSED_UPDATE_STMT, params_buffer=self._missed_update_rows,
            signal=self._missed_update_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure
        )

    def _start_http_producer(self):
        self._log("start: Starting analytics poster.", "INFO")
        self._poster_task = self._http_cli.get_request_producer(
            config=self._http_config, callbacks=self._http_callbacks, max_concurrent_requests=15
        )
        self._poster_task.start()

    def _start_serializer(self):
        self._log(f"start: Starting {SERIALIZER_ID}.", "INFO")
        task = asyncio.create_task(self._serializer())
        self._running_internal_tasks[SERIALIZER_ID] = task
        task.add_done_callback(lambda t: self._running_internal_tasks.pop(SERIALIZER_ID, None))

    def _token_insert_callback(self, token: str) -> bool:
        if len(self._new_batch) == self._limit:
            self._tokens.append(set()) 
            self._payloads.append(orjson.dumps(self._new_batch))
            self._new_batch = []
        self._tokens[-1].add(token)
        self._new_batch.append({"token_id": token})
        return True

    def _token_delete_callback(self, token: str):
        self._deleted_token_count += 1
        
        final_miss_count = self._miss_counts.pop(token, 0)
        
        self._missed_update_rows.append((final_miss_count, token))
        self._missed_update_flag.set()

        if self._orderbook_registry.get_token_count() == 0:
            self._log("token_delete_callback: All tokens removed. Stopping and resetting to bootstrap.", "WARNING")
            self.stop()
            if self._bootstrap_callback_handle is None:
                self._bootstrap_callback_handle = self._orderbook_registry.register_insert_callback(self._bootstrap)
            return

        if self._deleted_token_count >= self._limit * self._reserialize_threshold:
            self._serializer_signal.set()
            self._deleted_token_count = 0

    def _next_request(self) -> Tuple[Any, str, Method, Optional[Dict], Optional[bytes]]:
        offset = self._payload_offset
        if offset == len(self._payloads):
            payload = orjson.dumps(self._new_batch)
            self._payload_offset = 0
        else:
            payload = self._payloads[offset]
            self._payload_offset += 1
        
        request_context = (self._generation_id, offset)
        return (request_context, "https://clob.polymarket.com/books", Method.POST, None, payload)

    async def _serializer(self):
        while True:
            try: 
                await self._serializer_signal.wait()
                self._serializer_signal.clear()
                
                self._log("serializer: Reserializing tokens and restarting HTTP producer.", "INFO")
                if self._poster_task:
                    self._poster_task.stop()
                
                self._generation_id += 1
                
                self._tokens = [set()]
                self._new_batch = []
                self._payloads = []
                
                self._orderbook_registry.iterate_over_tokens(self._token_insert_callback)
                self._log(f"serializer: Serialized {self._orderbook_registry.get_token_count()} tokens into {len(self._payloads)} payloads for generation {self._generation_id}.", "INFO")

                self._payload_offset = 0
                self._start_http_producer()

            except asyncio.CancelledError:
                self._log("serializer: Task cancelled.", "INFO")
                break
            except Exception as e:
                self._log(f"serializer: Task failed with '{e}'", "ERROR")
                traceback.print_exc()

    async def _on_response(self, request_id: Any, response_content: ResponseContent, status_code: StatusCode, headers: Optional[Dict[str, str]], rtt: RTT) -> bool:
        try:
            response_generation, response_offset = request_id
        except (TypeError, ValueError):
            self._log(f"on_response: received invalid request_id format: {request_id}", "ERROR")
            return True

        if response_generation != self._generation_id:
            self._log(f"on_response: Discarding stale response from generation {response_generation} (current is {self._generation_id}).", "WARNING")
            return True

        if response_offset >= len(self._tokens):
            self._log(f"on_response: received response for out-of-bounds offset {response_offset} in generation {response_generation}. Discarding.", "WARNING")
            return True
            
        if status_code != 200:
            self._log(f"on_response: request_id {request_id} bad status {status_code} with response content '{response_content.decode('utf-8', 'replace')[0:300]}'", "WARNING")
            return False

        now_ms = time.time_ns() // 1_000_000
        self._rtt_rows.append((now_ms, rtt))
        self._rtt_flag.set()
        
        try:
            server_books = orjson.loads(response_content)
        except orjson.JSONDecodeError as e:
            self._log(f"on_response: json parse failed with '{e}' for request_id {request_id}. Content: {response_content[:300]}", "ERROR")
            return False

        if not isinstance(server_books, list):
            self._log(f"on_response: non-list server books for request_id {request_id}. Content: {response_content[:300]}", "WARNING")
            return True
        
        response_tokens = set()
        books_to_add = []
        
        for server_book in server_books:
            try:
                timestamp = int(server_book.get("timestamp"))
            except Exception as e:
                self._log(f"on_response: failed timestamp {server_book}", "WARNING")
                timestamp = None
            asset_id = server_book.get("asset_id")
            if isinstance(asset_id, str) and asset_id:
                response_tokens.add(asset_id)
                
                false_misses = self._miss_counts.get(asset_id, 0)
                
                book_tuple = (self._found_index, now_ms, timestamp, asset_id, false_misses, orjson.dumps(server_book).decode('utf-8'))
                books_to_add.append(book_tuple)
                self._found_index += 1

        sent_tokens = self._tokens[response_offset]

        for token in sent_tokens:
            if token in response_tokens:
                if token in self._miss_counts:
                    self._miss_counts.pop(token, None)
            else:
                self._miss_counts[token] = self._miss_counts.get(token, 0) + 1
        
        if books_to_add:
            self._server_book_rows.extend(books_to_add)
            self._server_book_flag.set()
        
        return True

    async def _on_exception(self, request_id: Any, exception: Exception) -> bool:
        self._log(f"on_exception: request_id {request_id} failed with an unhandled exception.", "ERROR")
        self._log(traceback.format_exc(), "ERROR")
        return False
    
    async def _on_insert_success(self, task_id: str, params: List[Tuple]):
        if task_id == SERVER_BOOK_INSERTER_ID: self._server_book_rows.clear()
        elif task_id == RTT_INSERTER_ID: self._rtt_rows.clear()
        elif task_id == MISSED_UPDATER_ID: self._missed_update_rows.clear()
    
    async def _on_inserter_failure(self, task_id: str, exception: Exception, params: List[Tuple]):
        self._log(f"on_inserter_failure: '{task_id}' failed with '{exception}'. Stopping analytics.", "FATAL")
        self.stop()