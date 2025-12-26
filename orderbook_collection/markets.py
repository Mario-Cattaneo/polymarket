import asyncio
import orjson
import time
from typing import List, Tuple, Optional, Dict, Any, Set

from orderbook import OrderbookRegistry
from db_cli import DatabaseManager
from http_cli import (HttpTaskConfig, HttpTaskCallbacks, HttpManager, Method,
RequestProducer, ResponseContent, StatusCode, RTT)

MARKET_INSERTER_ID = "market_inserter"
MARKETS_INSERT_STMT = """
INSERT INTO markets_3 (found_index, found_time_ms, asset_id, exhaustion_cycle, market_id, condition_id, question_id, negrisk_id, "offset", message)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (asset_id) DO NOTHING;
"""

MARKET_CLOSER_ID = "market_closer"
MARKET_CLOSE_STMT = "UPDATE markets_3 SET closed_time = $1 WHERE asset_id = $2"

RTT_INSERTER_ID = "market_rtt_inserters"

class Markets:
    def __init__(self, log: callable, http_man: HttpManager, db_man: DatabaseManager, orderbook_reg : OrderbookRegistry):
        self._log = log
        self._db_cli = db_man
        self._http_cli = http_man
        self._orderbook_reg = orderbook_reg

        self._found_index = 0
        self._current_offset = 0
        self._exhaustion_cycle = 0
        
        self._active_assets: Set[str] = set()

        self._new_market_inserts: List[Tuple] = []
        self._new_market_insert_flag = asyncio.Event()
        
        self._rtt_rows: List[Tuple] = []
        self._rtt_flag = asyncio.Event()

        self._closed_market_rows: List[Tuple] = []
        self._closed_market_flag = asyncio.Event()

        self.markets_exhausted = asyncio.Event()

        self._http_config = HttpTaskConfig(
            base_back_off_s=1.0,
            max_back_off_s=150.0,
            back_off_rate=2.0,
            request_break_s=0.04
        )
        
        self._http_callbacks = HttpTaskCallbacks(
            next_request=self._next_request,
            on_response=self._on_response,
            on_exception=self._on_exception
        )

        self._limit = 500
        self._getter_task: Optional[RequestProducer] = None
        self._running_db_tasks: Dict[str, asyncio.Task] = {}

    def get_buffer_sizes(self) -> Dict[str, int]:
        return {
            "new_market_inserts": len(self._new_market_inserts),
            "rtt_rows": len(self._rtt_rows),
            "closed_market_rows": len(self._closed_market_rows),
            "active_assets_count": len(self._active_assets)
        }

    def task_summary(self) -> dict:
        db_tasks_running = len(self._running_db_tasks)
        getter_stats = self._getter_task.to_dict() if self._getter_task else {}
        return {
            "total_running_tasks": db_tasks_running + (1 if self._getter_task else 0),
            "db_tasks": list(self._running_db_tasks.keys()),
            "market_getter": getter_stats,
            "state": {
                "current_offset": self._current_offset,
                "active_assets": len(self._active_assets),
                "cycle": self._exhaustion_cycle
            }
        }

    async def start(self, restore: bool = False):
        try:
            if self._getter_task or self._running_db_tasks:
                self._log("start: tasks already running, stop them first", "WARN")
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
            
            self._log(f"start: starting {MARKET_CLOSER_ID}", "INFO")
            self._running_db_tasks[MARKET_CLOSER_ID] = self._db_cli.exec_persistent(
                task_id=MARKET_CLOSER_ID, stmt=MARKET_CLOSE_STMT, params_buffer=self._closed_market_rows,
                signal=self._closed_market_flag, on_success=self._on_insert_success, on_failure=self._on_inserter_failure
            )
            
            self._log(f"start: starting market getter at offset {self._current_offset}", "INFO")
            self._getter_task = self._http_cli.get_request_producer(
                config=self._http_config, callbacks=self._http_callbacks, max_concurrent_requests=1
            )
            self._getter_task.start()

        except Exception as e:
            self._log(f"start: failed '{type(e).__name__}: {e}'", "FATAL")
            self.stop()

    def stop(self):
        if self._getter_task:
            self._getter_task.stop()
            self._getter_task = None
            self._log("stop: market getter stopped", "INFO")

        for task_id, task in self._running_db_tasks.items():
            task.cancel()
            self._log(f"stop: {task_id} cancelled", "INFO")
        self._running_db_tasks.clear()

    async def _restore_from_db(self):
        self._log("restore_from_db: starting restoration...", "INFO")
        try:
            state_res = await self._db_cli.fetch("restore_cycle", 'SELECT MAX(exhaustion_cycle) as cycle FROM markets_3;')
            if state_res and state_res[0].get('cycle') is not None:
                self._exhaustion_cycle = int(state_res[0]['cycle'])

            active_res = await self._db_cli.fetch("restore_active", 'SELECT asset_id FROM markets_3 WHERE closed_time IS NULL;')
            if active_res:
                self._active_assets = {r['asset_id'] for r in active_res if r['asset_id']}
                self._log(f"restore: {len(self._active_assets)} active assets loaded into local set.", "INFO")
                
                for asset_id in self._active_assets:
                    self._orderbook_reg.insert_token(asset_id)
                self._log(f"restore: {len(self._active_assets)} active assets registered. Cycle: {self._exhaustion_cycle}", "INFO")

                # --- THE FIX ---
                # Manually signal that the restore cycle is complete.
                self._log("restore: Manually signaling market exhaustion after DB restore.", "INFO")
                self.markets_exhausted.set()

        except Exception as e:
            self._log(f"restore_from_db: failed '{e}'", "ERROR")

    def _next_request(self) -> Tuple[int, str, Method, Optional[Dict], Optional[bytes]]:
        url = f"https://gamma-api.polymarket.com/markets?limit={self._limit}&offset={self._current_offset}"
        return (self._current_offset, url, Method.GET, None, None)
        
    async def _on_response(self, request_id: int, response_content: ResponseContent, status_code: StatusCode, headers: Optional[Dict[str, str]], rtt: RTT) -> bool:
        if status_code != 200:
            self._log(f"on_response: request_id {request_id} bad status {status_code}", "WARNING")
            return False
        
        now_ms = time.time_ns() // 1_000_000
        self._rtt_rows.append((now_ms, rtt))
        self._rtt_flag.set()

        try:
            markets = orjson.loads(response_content)
        except Exception as e:
            self._log(f"on_response: request_id {request_id} json parse failed: {e}", "WARNING")
            return False

        if not isinstance(markets, list):
            return False
        
        if not markets:
            self._log(f"Exhaustion: Cycle {self._exhaustion_cycle} complete. Resetting to offset 0.", "INFO")
            self._current_offset = 0
            self._exhaustion_cycle += 1
            self.markets_exhausted.set()
            return True

        new_count = 0
        close_count = 0

        for i, market_obj in enumerate(markets):
            self._found_index += 1
            current_market_offset = request_id + i
            
            is_closed = market_obj.get("closed", False)
            market_id = market_obj.get("id")
            token_ids_str = market_obj.get("clobTokenIds")
            
            try:
                tokens = orjson.loads(token_ids_str) if token_ids_str else []
            except:
                tokens = []

            for token in tokens:
                if not token: continue

                if is_closed:
                    if token in self._active_assets:
                        self._active_assets.remove(token)
                        self._orderbook_reg.delete_token(token)
                        self._closed_market_rows.append((now_ms, token))
                        self._closed_market_flag.set()
                        close_count += 1
                else:
                    if token not in self._active_assets:
                        self._active_assets.add(token)
                        self._orderbook_reg.insert_token(token)
                        
                        condition_id = market_obj.get("conditionId")
                        question_id = market_obj.get("questionId")
                        negrisk_id = market_obj.get("negRiskMarketID")
                        dump = orjson.dumps(market_obj).decode('utf-8')

                        self._new_market_inserts.append((
                            self._found_index, now_ms, token, self._exhaustion_cycle, 
                            market_id, condition_id, question_id, negrisk_id, 
                            current_market_offset, dump
                        ))
                        self._new_market_insert_flag.set()
                        new_count += 1

        if new_count > 0 or close_count > 0:
            self._log(f"on_response: Offset {request_id} processed. New: {new_count}, Closed: {close_count}", "DEBUG")

        self._current_offset += len(markets)
        return True

    async def _on_exception(self, request_id: Any, exception: Exception) -> bool:
        self._log(f"on_exception: request_id {request_id} failed with {type(exception).__name__}: {exception}", "ERROR")
        return False

    async def _on_insert_success(self, task_id: str, params: List[Tuple]):
        if task_id == MARKET_INSERTER_ID: self._new_market_inserts.clear()
        elif task_id == RTT_INSERTER_ID: self._rtt_rows.clear()
        elif task_id == MARKET_CLOSER_ID: self._closed_market_rows.clear()

    async def _on_inserter_failure(self, task_id: str, exception: Exception, params: List[Tuple]):
        self._log(f"on_inserter_failure: {task_id} failed with {exception}. stopping.", "FATAL")
        self.stop()