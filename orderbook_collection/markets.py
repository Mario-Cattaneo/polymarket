import asyncio
import orjson
import time
from typing import List, Tuple, Optional, Dict, Any

from orderbook import OrderbookRegistry
from db_cli import DatabaseManager
from http_cli import (HttpTaskConfig, HttpTaskCallbacks, HttpManager, Method, 
                      RequestProducer, ResponseContent, StatusCode, RTT)

MARKET_INSERTER_ID = "market_inserter"
MARKETS_INSERT_STMT = """
    INSERT INTO markets (found_index, found_time_ms, asset_id, exhaustion_cycle, market_id, condition_id, question_id, negrisk_id, "offset", message) 
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) 
    ON CONFLICT (asset_id) DO NOTHING;
"""

MARKET_COPIER_ID = "market_copier"

RTT_INSERTER_ID = "market_rtt_inserters"



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

        self._http_config = HttpTaskConfig(
            base_back_off_s=1.0,
            max_back_off_s=150.0,
            back_off_rate=2.0,
            request_break_s=0.15
        )
        
        self._http_callbacks = HttpTaskCallbacks(
            next_request=self._next_request,
            on_response=self._on_response,
            on_exception=self._on_exception
        )

        self._limit = 500
        self._offset = 0
        self._unexhausted_market_count = 0
        self._exhaustion_cycle = 0
        self.markets_exhausted = asyncio.Event()

        self._getter_task: Optional[RequestProducer] = None
        self._running_db_tasks: Dict[str, asyncio.Task] = {}
    
    def get_buffer_sizes(self) -> Dict[str, int]:
        """
        Returns the current sizes of the internal database buffers.
        """
        return {
            "new_market_inserts": len(self._new_market_inserts),
            "new_market_copies": len(self._new_market_copies),
            "rtt_rows": len(self._rtt_rows),
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
            "market_getter": getter_stats
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
                task_id=RTT_INSERTER_ID, table_name="buffer_markets_rtt", params_buffer=self._rtt_rows,
                signal=self._rtt_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure
            )
            self._running_db_tasks[MARKET_COPIER_ID] = self._db_cli.copy_persistent(
            task_id=MARKET_COPIER_ID, table_name="buffer_markets" , params_buffer=self._new_market_copies,
            signal=self._new_market_copy_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure
        )
            
            self._log(f"start: starting market getter at offset {self._offset}", "INFO")
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
        state_stmt = 'SELECT MAX("offset") as max_offset, MAX(exhaustion_cycle) as max_cycle FROM markets;'
        try:
            restore_task = self._db_cli.fetch(task_id="get_market_restore_state", stmt=state_stmt)
            state_res = await restore_task
            
            if state_res and state_res[0]:
                max_offset = state_res[0].get('max_offset')
                max_cycle = state_res[0].get('max_cycle')
                if max_offset is not None: self._offset = int(max_offset) + 1
                if max_cycle is not None: self._exhaustion_cycle = int(max_cycle)
                self._log(f"restore_from_db: state restored. Offset: {self._offset}, Cycle: {self._exhaustion_cycle}", "INFO")
            
        except Exception as e:
            self._log(f"restore_from_db: failed '{e}'. Defaulting state.", "ERROR")
        
        self._offset = getattr(self, '_offset', 0)
        self._exhaustion_cycle = getattr(self, '_exhaustion_cycle', 0)

    def _next_request(self) -> Tuple[int, str, Method, Optional[Dict], Optional[bytes]]:

        url = f"https://gamma-api.polymarket.com/markets?limit={self._limit}&offset={self._offset}"
        return (self._offset, url, Method.GET, None, None)
        
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
        
        if len(markets):
            self._log(f"on_response: request_id {request_id} with {len(markets)} markets")
   
        request_id -= 1
        for market_obj in markets:
            request_id += 1
            if not isinstance(market_obj, dict):
                self._log(f"on_response: request_id {request_id} non-dict market '{str(market_obj)[:300]}'", "WARNING")
                continue
                
            self._found_index += 1
            
            valid_market = True
            closed = market_obj.get("closed")
            market_id = market_obj.get("id")
            token_ids_str = market_obj.get("clobTokenIds")
            try:
                tokens = orjson.loads(token_ids_str)
            except Exception as e:
                self._log(f"on_response: request_id {request_id} failed to parse 'clobTokenIds' with {e}", "WARNING")
                tokens = [None]
                valid_market = False
                          

            if not isinstance(closed, bool):
                self._log(f"on_response: request_id {request_id} non-bool closed '{str(market_obj)[:300]}'", "WARNING")
                valid_market = False
            elif valid_market:
                valid_market = not closed
            
            if not (isinstance(market_id, str) and market_id):
                self._log(f"on_response: request_id {request_id}  invalid 'id' {str(market_obj)[:300]}", "WARNING")
                valid_market = False
            
            if not isinstance(token_ids_str, str):
                self._log(f"on_response: request_id {request_id} non_str 'clobTokenIds' {token_ids_str}", "WARNING")
                valid_market = False

            if not isinstance(tokens, list):
                self._log(f"on_response: request_id {request_id} non-list tokens {str(token_ids_str)[:300]}", "WARNING")
                valid_market = False

            for token in tokens:
                condition_id = market_obj.get("conditionId")
                question_id = market_obj.get("questionId")
                negrisk_id = market_obj.get("negRiskMarketID")
                dump = orjson.dumps(market_obj).decode('utf-8')
                if valid_market:
                    self._new_market_inserts.append((self._found_index, now_ms, token, self._exhaustion_cycle, market_id, condition_id, question_id, negrisk_id, request_id, dump))
                    self._unexhausted_market_count += 1
                    self._orderbook_reg.insert_token(token)
                self._new_market_copies.append((self._found_index, now_ms, token, self._exhaustion_cycle, market_id, condition_id, question_id, negrisk_id, request_id, None, dump))
        
        unfiltered_market_count = len(markets)
        request_id += 1
        if unfiltered_market_count:
            self._log(f"on_response: request_id {request_id} found {unfiltered_market_count} unfiltered markets", "DEBUG")
            self._http_config.request_break_s = 0.1
            self._offset += unfiltered_market_count
            self._new_market_copy_flag.set()
        if  self._unexhausted_market_count > 0:
            self._log(f"on_response: request_id {request_id} {self._unexhausted_market_count} unexhausted markets.", "INFO")
            self._new_market_insert_flag.set()
        if unfiltered_market_count < self._limit and self._unexhausted_market_count > 0:
            self.markets_exhausted.set()
            self._log(f"on_response: request_id {request_id} completed exhaustion cycle {self._exhaustion_cycle} with {self._unexhausted_market_count} markets")
            self._unexhausted_market_count = 0
            self._http_config.request_break_s = 1
            self._exhaustion_cycle += 1
        return True

    async def _on_exception(self, request_id: Any, exception: Exception) -> bool:
        self._log(f"on_exception: request_id {request_id} failed with {exception}", "ERROR")
        return False
    
    async def _on_insert_success(self, task_id: str, params: List[Tuple]):
        #self._log(f"on_insert_success: {task_id} successfully inserted {len(params)} rows.", "DEBUG")
        return

    async def _on_inserter_failure(self, task_id: str, exception: Exception, params: List[Tuple]):
        self._log(f"on_inserter_failure: {task_id} failed with {exception} on {len(params)} params. stopping.", "FATAL")
        self.stop()