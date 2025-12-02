import asyncio
import orjson
import time
import bisect
from enum import Enum
from typing import List, Tuple, Optional, Dict, Any

from orderbook import OrderbookRegistry
from db_cli import DatabaseManager
from http_cli import (HttpTaskConfig, HttpTaskCallbacks, HttpManager, Method, 
                      RequestProducer, ResponseContent, StatusCode, RTT)

MARKET_INSERTER_ID = "market_inserter"
MARKETS_INSERT_STMT = """
    INSERT INTO markets_2 (found_index, found_time_ms, asset_id, exhaustion_cycle, market_id, condition_id, question_id, negrisk_id, "offset", message) 
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) 
    ON CONFLICT (asset_id) DO NOTHING;
"""

MARKET_COPIER_ID = "market_copier"

RTT_INSERTER_ID = "market_rtt_inserters"

MARKET_CLOSER_ID = "market_closer"
MARKET_CLOSE_STMT = "UPDATE markets_2 SET closed_time = $1 WHERE asset_id = $2"

class MarketScanState(Enum):
    REAPING = 0    # Iterating over known active offsets
    DISCOVERY = 1  # Checking for new markets at the end of the known world

class Markets:
    def __init__(self, log: callable, http_man: HttpManager, db_man: DatabaseManager, orderbook_reg : OrderbookRegistry):
        self._log = log
        self._db_cli = db_man
        self._http_cli = http_man
        self._orderbook_reg = orderbook_reg

        self._found_index = 0
        
        self._new_market_inserts: List[Tuple] = []
        self._new_market_insert_flag = asyncio.Event()

        self._new_market_copies: List[Tuple] = []
        self._new_market_copy_flag = asyncio.Event()
        
        self._rtt_rows: List[Tuple] = []
        self._rtt_flag = asyncio.Event()

        self._closed_market_rows: List[Tuple] = []
        self._closed_market_flag = asyncio.Event()

        self._http_config = HttpTaskConfig(
            base_back_off_s=1.0,
            max_back_off_s=150.0,
            back_off_rate=2.0,
            request_break_s=0.1
        )
        
        self._http_callbacks = HttpTaskCallbacks(
            next_request=self._next_request,
            on_response=self._on_response,
            on_exception=self._on_exception
        )

        self._limit = 500
        
        # State for smart traversal
        self._state = MarketScanState.DISCOVERY
        self._active_offsets: List[int] = [] # Sorted list of offsets where active markets reside
        self._reap_iter_index = 0 # Pointer for iterating _active_offsets during REAPING
        
        # Initialize to -1 so that if DB is empty, start offset is (-1 + 1) = 0
        self._max_seen_offset = -1 
        self._discovery_offset = 0 

        self._unexhausted_market_count = 0
        self._exhaustion_cycle = 0
        self.markets_exhausted = asyncio.Event()

        self._getter_task: Optional[RequestProducer] = None
        self._running_db_tasks: Dict[str, asyncio.Task] = {}
    
    def get_buffer_sizes(self) -> Dict[str, int]:
        return {
            "new_market_inserts": len(self._new_market_inserts),
            "new_market_copies": len(self._new_market_copies),
            "rtt_rows": len(self._rtt_rows),
            "closed_market_rows": len(self._closed_market_rows),
            "active_offsets": len(self._active_offsets)
        }

    def task_summary(self) -> dict:
        db_tasks_running = len(self._running_db_tasks)
        getter_stats = {}
        total_running_tasks = db_tasks_running

        if self._getter_task:
            getter_stats = self._getter_task.to_dict()
            if getter_stats.get("running"):
                total_running_tasks += 1 + getter_stats.get("active_request_count", 0)

        return {
            "total_running_tasks": total_running_tasks,
            "db_tasks": {
                "count": db_tasks_running,
                "tasks": list(self._running_db_tasks.keys())
            },
            "market_getter": getter_stats,
            "scan_state": {
                "mode": self._state.name,
                "active_offsets_count": len(self._active_offsets),
                "max_seen_offset": self._max_seen_offset,
                "discovery_offset": self._discovery_offset,
                "cycle": self._exhaustion_cycle
            }
        }
    
    async def start(self, restore: bool = False):
        try:
            if self._getter_task or self._running_db_tasks:
                self._log("start: stop existing tasks before starting", "WARN")
                return
            if restore:
                await self._restore_from_db()
            
            self._log(f"start: starting {MARKET_INSERTER_ID}", "INFO")
            self._running_db_tasks[MARKET_INSERTER_ID] = self._db_cli.exec_persistent(
                task_id=MARKET_INSERTER_ID, stmt=MARKETS_INSERT_STMT, params_buffer=self._new_market_inserts,
                signal=self._new_market_insert_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure
            )

            self._log(f"start: starting {RTT_INSERTER_ID}", "INFO")
            self._running_db_tasks[RTT_INSERTER_ID] = self._db_cli.copy_persistent(
                task_id=RTT_INSERTER_ID, table_name="buffer_markets_rtt_2", params_buffer=self._rtt_rows,
                signal=self._rtt_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure
            )
            self._running_db_tasks[MARKET_COPIER_ID] = self._db_cli.copy_persistent(
                task_id=MARKET_COPIER_ID, table_name="buffer_markets_2" , params_buffer=self._new_market_copies,
                signal=self._new_market_copy_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure
            )
            
            self._log(f"start: starting {MARKET_CLOSER_ID}", "INFO")
            self._running_db_tasks[MARKET_CLOSER_ID] = self._db_cli.exec_persistent(
                task_id=MARKET_CLOSER_ID, stmt=MARKET_CLOSE_STMT, params_buffer=self._closed_market_rows,
                signal=self._closed_market_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure
            )
            
            # Determine start offset based on state
            start_offset = self._discovery_offset if self._state == MarketScanState.DISCOVERY else (self._active_offsets[0] if self._active_offsets else 0)
            
            self._log(f"start: starting market getter in {self._state.name} mode at offset {start_offset}", "INFO")
            self._getter_task = self._http_cli.get_request_producer(
                config=self._http_config, callbacks=self._http_callbacks, max_concurrent_requests=1
            )
            self._getter_task.start()

        except Exception as e:
            self._log(f"start: failed '{e}'", "FATAL")
            self.stop()
    
    def stop(self):
        if self._getter_task:
            self._getter_task.stop()
            self._getter_task = None

        for task_id, task in self._running_db_tasks.items():
            if not task.done():
                task.cancel()
            self._log(f"stop: {task_id} cancelled", "INFO")
        
        self._running_db_tasks.clear()
        self._log("stop: done", "INFO")

    async def _restore_from_db(self):
        self._log("restore_from_db: starting restoration...", "INFO")
        
        # 1. Restore Max Offset and Cycle
        state_stmt = 'SELECT MAX("offset") as max_offset, MAX(exhaustion_cycle) as max_cycle FROM markets_2;'
        try:
            restore_task = self._db_cli.fetch(task_id="get_market_restore_state", stmt=state_stmt)
            state_res = await restore_task
            
            if state_res and state_res[0]:
                max_offset = state_res[0].get('max_offset')
                max_cycle = state_res[0].get('max_cycle')
                
                if max_offset is not None: 
                    self._max_seen_offset = int(max_offset)
                    self._discovery_offset = self._max_seen_offset + 1
                
                if max_cycle is not None: 
                    self._exhaustion_cycle = int(max_cycle)
                
                self._log(f"restore_from_db: Max Offset: {self._max_seen_offset}, Cycle: {self._exhaustion_cycle}", "INFO")
        except Exception as e:
            self._log(f"restore_from_db: failed to fetch state '{e}'. Defaulting.", "ERROR")

        # 2. Restore Active Offsets List
        # We need this list to perform Reaping correctly.
        active_stmt = 'SELECT "offset" FROM markets_2 WHERE closed_time IS NULL ORDER BY "offset" ASC;'
        try:
            active_task = self._db_cli.fetch(task_id="get_active_offsets", stmt=active_stmt)
            active_res = await active_task
            
            if active_res:
                self._active_offsets = [r.get('offset') for r in active_res if r.get('offset') is not None]
                self._log(f"restore_from_db: Restored {len(self._active_offsets)} active offsets.", "INFO")
            else:
                self._log("restore_from_db: No active offsets found in DB.", "INFO")
                
        except Exception as e:
            self._log(f"restore_from_db: failed to fetch active offsets '{e}'.", "ERROR")

        # Default to DISCOVERY to sync up or find new items
        self._state = MarketScanState.DISCOVERY

    def _next_request(self) -> Tuple[int, str, Method, Optional[Dict], Optional[bytes]]:
        
        req_offset = 0

        if self._state == MarketScanState.REAPING:
            # If we have run out of active offsets to check, switch to DISCOVERY
            if self._reap_iter_index >= len(self._active_offsets):
                self._state = MarketScanState.DISCOVERY
                # FIX: Start discovery AFTER the last known market index
                self._discovery_offset = self._max_seen_offset + 1
                self._log(f"next_request: Finished REAPING. Switching to DISCOVERY at {self._discovery_offset}", "DEBUG")
                # Fall through to DISCOVERY logic immediately
            else:
                # Get the offset from the current pointer
                req_offset = self._active_offsets[self._reap_iter_index]
                
                # Optimization: Check if proceeding active market offsets reside in this batch (offset -> offset + limit)
                limit_reach = req_offset + self._limit
                
                # Find the index of the first offset that is >= limit_reach
                next_index = bisect.bisect_left(self._active_offsets, limit_reach, lo=self._reap_iter_index)
                
                # Update the pointer to skip the covered offsets
                self._reap_iter_index = next_index
                
                url = f"https://gamma-api.polymarket.com/markets?limit={self._limit}&offset={req_offset}"
                return (req_offset, url, Method.GET, None, None)

        if self._state == MarketScanState.DISCOVERY:
            req_offset = self._discovery_offset
            url = f"https://gamma-api.polymarket.com/markets?limit={self._limit}&offset={req_offset}"
            return (req_offset, url, Method.GET, None, None)

        return (0, "", Method.GET, None, None) # Should not happen
        
    async def _on_response(self, request_id: int, response_content: ResponseContent, status_code: StatusCode, headers: Optional[Dict[str, str]],  rtt: RTT) -> bool:
        if status_code != 200:
            self._log(f"on_response: request_id {request_id} bad status {status_code} with response content '{response_content.decode('utf-8')[0:300]}' and headers {str(headers)[0:300]} ", "WARNING")
            return False
        
        now_ms = time.time_ns() // 1_000_000
        self._rtt_rows.append((now_ms, rtt))
        self._rtt_flag.set()

        try:
            markets = orjson.loads(response_content)
        except Exception as e:
            self._log(f"on_response: request_id {request_id} loading json failed with '{e}' for {response_content[0:300]}", "WARNING")
            return False

        if not isinstance(markets, list):
            self._log(f"on_response: request_id {request_id} non-list response {response_content[:300]}", "WARNING")
            return False
        
        request_offset = request_id 
        
        active_count_in_batch = 0
        closed_removed_count = 0

        for i, market_obj in enumerate(markets):
            if not isinstance(market_obj, dict):
                continue
            
            current_market_offset = request_offset + i
            
            # Update max seen offset to the highest index we've encountered
            if current_market_offset > self._max_seen_offset:
                self._max_seen_offset = current_market_offset

            self._found_index += 1
            
            valid_market = True
            closed = market_obj.get("closed")
            market_id = market_obj.get("id")
            token_ids_str = market_obj.get("clobTokenIds")
            
            try:
                tokens = orjson.loads(token_ids_str) if token_ids_str else []
            except Exception as e:
                tokens = [None]
                valid_market = False

            if not isinstance(closed, bool):
                valid_market = False
            
            # --- Logic for Closed Markets ---
            if closed is True:
                # Always attempt to clean up registry/DB
                if isinstance(tokens, list):
                    for token in tokens:
                        if token:
                            self._orderbook_reg.delete_token(token)
                            self._closed_market_rows.append((now_ms, token))
                
                # Remove from active list if present
                idx = bisect.bisect_left(self._active_offsets, current_market_offset)
                if idx != len(self._active_offsets) and self._active_offsets[idx] == current_market_offset:
                    self._active_offsets.pop(idx)
                    # Adjust reap index if we deleted behind or at the current pointer
                    if idx < self._reap_iter_index:
                        self._reap_iter_index -= 1
                    closed_removed_count += 1

            # --- Logic for Active Markets ---
            elif valid_market and not closed:
                active_count_in_batch += 1
                # Ensure it is in our active list
                idx = bisect.bisect_left(self._active_offsets, current_market_offset)
                if idx == len(self._active_offsets) or self._active_offsets[idx] != current_market_offset:
                    self._active_offsets.insert(idx, current_market_offset)
                    if idx < self._reap_iter_index:
                        self._reap_iter_index += 1

            # Standard Validation for DB Insertion
            if not (isinstance(market_id, str) and market_id):
                valid_market = False
            
            if not isinstance(token_ids_str, str):
                valid_market = False

            if not isinstance(tokens, list):
                valid_market = False

            for token in tokens:
                condition_id = market_obj.get("conditionId")
                question_id = market_obj.get("questionId")
                negrisk_id = market_obj.get("negRiskMarketID")
                dump = orjson.dumps(market_obj).decode('utf-8')
                
                if valid_market and not closed:
                    self._new_market_inserts.append((self._found_index, now_ms, token, self._exhaustion_cycle, market_id, condition_id, question_id, negrisk_id, current_market_offset, dump))
                    self._unexhausted_market_count += 1
                    self._orderbook_reg.insert_token(token)
                
                self._new_market_copies.append((self._found_index, now_ms, token, self._exhaustion_cycle, market_id, condition_id, question_id, negrisk_id, current_market_offset, None, None, dump))
        
        market_count = len(markets)
        
        # --- Improved Logging Logic ---
        if self._state == MarketScanState.DISCOVERY:
            if active_count_in_batch > 0:
                self._log(f"on_response: Discovery: Found {active_count_in_batch} new active markets at offset {request_offset}", "INFO")
        
        elif self._state == MarketScanState.REAPING:
            if closed_removed_count > 0:
                self._log(f"on_response: Reaping: Removed {closed_removed_count} markets from active list at offset {request_offset}", "INFO")

        # --- State Transitions ---
        if self._state == MarketScanState.DISCOVERY:
            if market_count > 0:
                # Found new markets, advance discovery offset
                self._discovery_offset += market_count
                self._http_config.request_break_s = 0.1 # Speed up during discovery
            else:
                # Exhaustion reached in Discovery mode
                self._log(f"on_response: Exhaustion cycle {self._exhaustion_cycle} complete. Found {self._unexhausted_market_count} active markets total.", "INFO")
                
                self.markets_exhausted.set()
                self._exhaustion_cycle += 1
                self._unexhausted_market_count = 0
                
                # Reset to REAPING mode
                self._state = MarketScanState.REAPING
                self._reap_iter_index = 0
                
                self._http_config.request_break_s = 0.1

        elif self._state == MarketScanState.REAPING:
            self._http_config.request_break_s = 0.1

        # Trigger DB flushes
        if len(self._new_market_copies) > 0:
            self._new_market_copy_flag.set()
        if len(self._new_market_inserts) > 0:
            self._new_market_insert_flag.set()
        if len(self._closed_market_rows) > 0:
            self._closed_market_flag.set()

        return True

    async def _on_exception(self, request_id: Any, exception: Exception) -> bool:
        self._log(f"on_exception: request_id {request_id} failed with {exception}", "ERROR")
        return False
    
    async def _on_insert_success(self, task_id: str, params: List[Tuple]):
        if task_id == MARKET_INSERTER_ID: self._new_market_inserts.clear()
        elif task_id == MARKET_COPIER_ID: self._new_market_copies.clear()
        elif task_id == RTT_INSERTER_ID: self._rtt_rows.clear()
        elif task_id == MARKET_CLOSER_ID: self._closed_market_rows.clear()

    async def _on_inserter_failure(self, task_id: str, exception: Exception, params: List[Tuple]):
        self._log(f"on_inserter_failure: {task_id} failed with {exception} on {len(params)} params. stopping.", "FATAL")
        self.stop()