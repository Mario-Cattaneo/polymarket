import asyncio
import asyncpg
import inspect
from typing import Tuple, Callable, Any, List, Optional, Union, Awaitable
from enum import Enum
from utils import maybe_await

SuccessCallback = Union[Callable[[Any, Any], None], Callable[[Any, Any], Awaitable[None]]]
FailureCallback = Union[Callable[[Any, Exception, Any], None], Callable[[Any, Exception, Any], Awaitable[None]]]

class _SqlInstr(Enum):
    FETCH = 0
    EXEC = 1
    EXEC_MANY = 2
    COPY = 3

class DatabaseManager:
    def __init__(
        self,
        db_user: str,
        db_pass: str,
        db_name: str,
        db_host: str,
        db_port: int = 5432,
        min_pool_size: int = 1,
        max_pool_size: int = 15
    ):
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name
        self.db_host = db_host
        self.db_port = db_port
        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        if self.pool and not self.pool.is_closing():
            return
        self.pool = await asyncpg.create_pool(
            user=self.db_user,
            password=self.db_pass,
            database=self.db_name,
            host=self.db_host,
            port=self.db_port,
            min_size=self.min_pool_size,
            max_size=self.max_pool_size,
        )

    async def disconnect(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    def _validate_common_args(
        self,
        max_attempts: int,
        on_success: Optional[Callable],
        on_failure: Optional[Callable],
    ) -> None:
        if not isinstance(max_attempts, int) or max_attempts < 1:
            raise ValueError("'max_attempts' must be an integer >= 1.")
        if on_failure is not None and not (
            callable(on_failure) and len(inspect.signature(on_failure).parameters) == 3
        ):
            raise ValueError("'on_failure' must be a callable that accepts 3 arguments (task_id, exception, data).")
        if on_success is not None and not (
            callable(on_success) and len(inspect.signature(on_success).parameters) == 2
        ):
            raise ValueError("'on_success' must be a callable that accepts 2 arguments (task_id, data).")

    async def _execute_with_retry(
        self,
        sql_task: _SqlInstr,
        task_id: Any,
        stmt: str,
        params: Any,
        max_attempts: int,
        on_success: Optional[SuccessCallback],
        on_failure: Optional[FailureCallback],
        base_back_off_s: float,
        back_off_rate: float
    ) -> Any:
        failures = 0
        back_off_s = base_back_off_s

        while failures < max_attempts:
            conn = None
            try:
                if self.pool is None:
                    raise RuntimeError("Database pool is not initialized. Call connect() first.")
                conn = await self.pool.acquire()
                result = None

                if sql_task == _SqlInstr.FETCH:
                    result = await conn.fetch(stmt, *params)
                    await maybe_await(on_success, task_id, result)
                    return result

                elif sql_task == _SqlInstr.EXEC:
                    result = await conn.execute(stmt, *params)
                    await maybe_await(on_success, task_id, result)
                    return result

                elif sql_task == _SqlInstr.EXEC_MANY:
                    await conn.executemany(stmt, params)
                    await maybe_await(on_success, task_id, list(params))
                    return None

                elif sql_task == _SqlInstr.COPY:
                    
                    table_name, records = params
                    
                    async with conn.transaction():
                        await conn.copy_records_to_table(table_name, records=records)
                        
                    await maybe_await(on_success, task_id, list(records))
                    return None
                
                else:
                    raise ValueError("Unsupported SQL instruction.")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                failures += 1
                if failures >= max_attempts:
                    await maybe_await(on_failure, task_id, e, params)
                    raise
                await asyncio.sleep(back_off_s)
                back_off_s = min(back_off_s * back_off_rate, 300)
            finally:
                if conn:
                    await self.pool.release(conn)

    async def _sql_persistent_task(
        self,
        sql_instr: _SqlInstr,
        task_id: Any,
        stmt_or_table: str,
        params_buffer: List[Any],
        signal: asyncio.Event,
        prepare_params_fn: Callable[[List[Any]], Any],
        on_success: Optional[SuccessCallback],
        on_failure: Optional[FailureCallback],
        max_attempts: int,
        base_back_off_s: float,
        back_off_rate: float
    ):
        while True:
            try:
                await signal.wait()
                signal.clear()
                if not params_buffer:
                    continue
                batch = list(params_buffer)
                params_buffer.clear()
                params_for_executor = prepare_params_fn(batch)

                await self._execute_with_retry(
                    sql_task=sql_instr,
                    task_id=task_id,
                    stmt=stmt_or_table,
                    params=params_for_executor,
                    max_attempts=max_attempts,
                    on_success=None,
                    on_failure=on_failure,
                    base_back_off_s=base_back_off_s,
                    back_off_rate=back_off_rate
                )

                await maybe_await(on_success, task_id, batch)

            except asyncio.CancelledError:
                break
            except Exception:
                break

    def fetch(
        self, task_id: Any, stmt: str, params: Tuple = (),
        on_success: Optional[SuccessCallback] = None,
        on_failure: Optional[FailureCallback] = None,
        max_attempts: int = 3, base_back_off_s: float = 1.0,
        back_off_rate: float = 2.0
    ) -> asyncio.Task:
        self._validate_common_args(max_attempts, on_success, on_failure)
        coro = self._execute_with_retry(
            _SqlInstr.FETCH, task_id, stmt, params, max_attempts, on_success, on_failure,
            base_back_off_s, back_off_rate
        )
        return asyncio.create_task(coro)

    def exec(
        self, task_id: Any, stmt: str, params: Tuple = (),
        on_success: Optional[SuccessCallback] = None,
        on_failure: Optional[FailureCallback] = None,
        max_attempts: int = 3, base_back_off_s: float = 1.0,
        back_off_rate: float = 2.0
    ) -> asyncio.Task:
        self._validate_common_args(max_attempts, on_success, on_failure)
        coro = self._execute_with_retry(
            _SqlInstr.EXEC, task_id, stmt, params, max_attempts, on_success, on_failure,
            base_back_off_s, back_off_rate
        )
        return asyncio.create_task(coro)

    def exec_batch(
        self, task_id: Any, stmt: str, params_buffer: List[Tuple],
        on_success: Optional[SuccessCallback] = None,
        on_failure: Optional[FailureCallback] = None,
        max_attempts: int = 3, base_back_off_s: float = 1.0,
        back_off_rate: float = 2.0
    ) -> asyncio.Task:
        self._validate_common_args(max_attempts, on_success, on_failure)
        coro = self._execute_with_retry(
            _SqlInstr.EXEC_MANY, task_id, stmt, params_buffer, max_attempts, on_success, on_failure,
            base_back_off_s, back_off_rate
        )
        return asyncio.create_task(coro)

    def exec_persistent(
        self, task_id: Any, stmt: str, params_buffer: List[Tuple],
        signal: asyncio.Event,
        on_success: Optional[SuccessCallback] = None,
        on_failure: Optional[FailureCallback] = None,
        max_attempts: int = 3, base_back_off_s: float = 1.0,
        back_off_rate: float = 2.0
    ) -> asyncio.Task:
        self._validate_common_args(max_attempts, on_success, on_failure)
        if not isinstance(signal, asyncio.Event):
            raise ValueError("exec_persistent requires a valid asyncio.Event 'signal'.")
        coro = self._sql_persistent_task(
            sql_instr=_SqlInstr.EXEC_MANY,
            task_id=task_id,
            stmt_or_table=stmt,
            params_buffer=params_buffer,
            signal=signal,
            prepare_params_fn=lambda batch: batch,
            on_success=on_success,
            on_failure=on_failure,
            max_attempts=max_attempts,
            base_back_off_s=base_back_off_s,
            back_off_rate=back_off_rate
        )
        return asyncio.create_task(coro)

    def copy(
        self, task_id: Any, table_name: str, records: List[Tuple],
        on_success: Optional[SuccessCallback] = None,
        on_failure: Optional[FailureCallback] = None,
        max_attempts: int = 3, base_back_off_s: float = 1.0,
        back_off_rate: float = 2.0
    ) -> asyncio.Task:
        self._validate_common_args(max_attempts, on_success, on_failure)
        params = (table_name, records)
        coro = self._execute_with_retry(
            _SqlInstr.COPY, task_id, table_name, params, max_attempts, on_success, on_failure,
            base_back_off_s, back_off_rate
        )
        return asyncio.create_task(coro)

    def copy_persistent(
        self, task_id: Any, table_name: str, params_buffer: List[Tuple],
        signal: asyncio.Event,
        on_success: Optional[SuccessCallback] = None,
        on_failure: Optional[FailureCallback] = None,
        max_attempts: int = 3, base_back_off_s: float = 1.0,
        back_off_rate: float = 2.0
    ) -> asyncio.Task:
        self._validate_common_args(max_attempts, on_success, on_failure)
        if not isinstance(signal, asyncio.Event):
            raise ValueError("copy_persistent requires a valid asyncio.Event 'signal'.")
        coro = self._sql_persistent_task(
            sql_instr=_SqlInstr.COPY,
            task_id=task_id,
            stmt_or_table=table_name,
            params_buffer=params_buffer,
            signal=signal,
            prepare_params_fn=lambda batch: (table_name, batch),
            on_success=on_success,
            on_failure=on_failure,
            max_attempts=max_attempts,
            base_back_off_s=base_back_off_s,
            back_off_rate=back_off_rate
        )
        return asyncio.create_task(coro)
