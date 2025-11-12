import asyncio
import inspect
import time
import websockets
from typing import TypeAlias, List, Tuple, Callable, Optional, Dict, Any


Message: TypeAlias = bytes | str
TaskOffset: TypeAlias = int
OnReadCallback: TypeAlias = Callable[[TaskOffset, Message], bool]
OnAckCallback: TypeAlias = Optional[Callable[[TaskOffset, Message], bool]]
OnConnectCallback: TypeAlias = Optional[Callable[[TaskOffset], bool]]

OnConnectFailureCallback: TypeAlias = Optional[Callable[
    [
        TaskOffset,         # The offset of the task that failed
        Exception,          # The exception that was raised
        str,                # The URL that was being connected to
        Message             # The initial payload that was sent
    ], 
    bool
]]

OnReadFailureCallback: TypeAlias = Optional[Callable[
    [
        TaskOffset,         # The offset of the task that failed
        Exception,          # The exception that was raised
        str                 # The URL of the connection
    ], 
    bool
]]

class WebSocketTaskConfig:
    """Configuration for all WebSocket managed tasks handled by a manager instance."""
    __slots__ = ('base_back_off_s', 'max_back_off_s', 'back_off_rate', 'max_queue', 
                 'max_msg_size', 'ping_interval', 'timeout')

    def __init__(self, base_back_off_s: float = 1.0, max_back_off_s: float = 90.0, 
                 back_off_rate: float = 2.0, max_queue: int = 256, 
                 max_msg_size: int = 1_048_576, ping_interval: float = 10.0, 
                 timeout: float = 5.0):
        if not isinstance(base_back_off_s, (int, float)) or base_back_off_s < 0: raise ValueError("'base_back_off_s' must be a non-negative number.")
        if not isinstance(max_back_off_s, (int, float)) or max_back_off_s < base_back_off_s: raise ValueError("'max_back_off_s' must be >= base_back_off_s.")
        if not isinstance(back_off_rate, (int, float)) or back_off_rate < 1.0: raise ValueError("'back_off_rate' must be >= 1.0.")
        if not isinstance(max_queue, int) or max_queue <= 0: raise ValueError("'max_queue' must be a positive integer.")
        if not isinstance(max_msg_size, int) or max_msg_size <= 0: raise ValueError("'max_msg_size' must be a positive integer.")
        if not isinstance(ping_interval, (int, float)) or ping_interval <= 0: raise ValueError("'ping_interval' must be a positive number.")
        if not isinstance(timeout, (int, float)) or timeout <= 0: raise ValueError("'timeout' must be a positive number.")
        
        self.base_back_off_s = base_back_off_s
        self.max_back_off_s = max_back_off_s
        self.back_off_rate = back_off_rate
        self.max_queue = max_queue
        self.max_msg_size = max_msg_size
        self.ping_interval = ping_interval
        self.timeout = timeout

class WebSocketTaskCallbacks:
    """Callbacks for all WebSocket managed tasks handled by a manager instance."""
    __slots__ = ('on_read', 'on_acknowledgement', 'on_read_failure', 
                 'on_connect', 'on_connect_failure')

    def __init__(self, on_read: OnReadCallback,
                 on_acknowledgement: OnAckCallback = None,
                 on_read_failure: OnReadFailureCallback = None,
                 on_connect: OnConnectCallback = None,
                 on_connect_failure: OnConnectFailureCallback = None):
        if not (callable(on_read) and len(inspect.signature(on_read).parameters) == 2): raise ValueError("'on_read' must be a callable that accepts 2 arguments.")
        if on_acknowledgement is not None and not (callable(on_acknowledgement) and len(inspect.signature(on_acknowledgement).parameters) == 2): raise ValueError("'on_acknowledgement' must be a callable that accepts 2 arguments.")
        if on_read_failure is not None and not (callable(on_read_failure) and len(inspect.signature(on_read_failure).parameters) == 3): raise ValueError("'on_read_failure' must be a callable that accepts 3 arguments.")
        if on_connect is not None and not (callable(on_connect) and len(inspect.signature(on_connect).parameters) == 1): raise ValueError("'on_connect' must be a callable that accepts 1 argument.")
        if on_connect_failure is not None and not (callable(on_connect_failure) and len(inspect.signature(on_connect_failure).parameters) == 4): raise ValueError("'on_connect_failure' must be a callable that accepts 4 arguments.")

        self.on_read = on_read
        self.on_acknowledgement = on_acknowledgement
        self.on_read_failure = on_read_failure
        self.on_connect = on_connect
        self.on_connect_failure = on_connect_failure

class _WebSocketTaskState:
    """Internal state unique to a single WebSocket managed task."""
    __slots__ = ('url', 'connection_attempts', 'connection_failures', 'messages_received', 
                 'last_activity_time', 'current_back_off_s')
    def __init__(self):
        self.url: str = ""
        self.connection_attempts: int = 0
        self.connection_failures: int = 0
        self.messages_received: int = 0
        self.last_activity_time: float = 0.0
        self.current_back_off_s: float = 1.0

class _ManagedTask:
    """Internal class to bundle the asyncio Task and unique state."""
    __slots__ = ('main_task', 'state', 'wss_cli', 'connection_state')

    def __init__(self, main_task: asyncio.Task, state: _WebSocketTaskState):
        self.main_task = main_task
        self.state = state
        self.wss_cli: Optional[websockets.WebSocketClientProtocol] = None
        self.connection_state: str = "DISCONNECTED"

    def get_stats(self, config: WebSocketTaskConfig) -> Dict[str, Any]:
        if self.main_task.done():
            if self.main_task.cancelled(): status = "cancelled"; exc_info = None
            elif self.main_task.exception(): exc = self.main_task.exception(); status = "failed"; exc_info = f"{type(exc).__name__}: {exc}"
            else: status = "finished"; exc_info = None
        else: status = "running"; exc_info = None
        
        return {
            "task_status": status,
            "task_exception": exc_info,
            # --- MODIFIED: Report our new state instead of relying on wss_cli ---
            "connection_state": self.connection_state,
            "url": self.state.url,
            "config": {k: getattr(config, k) for k in config.__slots__},
            "state": {
                "connection_attempts": self.state.connection_attempts,
                "connection_failures": self.state.connection_failures,
                "messages_received": self.state.messages_received,
                "last_activity_time_monotonic": self.state.last_activity_time,
                "current_back_off_s": self.state.current_back_off_s,
            }
        }

class WebSocketTaskConfig:
    __slots__ = ('base_back_off_s', 'max_back_off_s', 'back_off_rate', 'max_queue', 
                 'max_msg_size', 'ping_interval', 'timeout')

    def __init__(self, base_back_off_s: float = 1.0, max_back_off_s: float = 90.0, 
                 back_off_rate: float = 2.0, max_queue: int = 256, 
                 max_msg_size: int = 1_048_576, ping_interval: float = 10.0, 
                 timeout: float = 5.0):
        if not isinstance(base_back_off_s, (int, float)) or base_back_off_s < 0: raise ValueError("'base_back_off_s' must be a non-negative number.")
        if not isinstance(max_back_off_s, (int, float)) or max_back_off_s < base_back_off_s: raise ValueError("'max_back_off_s' must be >= base_back_off_s.")
        if not isinstance(back_off_rate, (int, float)) or back_off_rate < 1.0: raise ValueError("'back_off_rate' must be >= 1.0.")
        if not isinstance(max_queue, int) or max_queue <= 0: raise ValueError("'max_queue' must be a positive integer.")
        if not isinstance(max_msg_size, int) or max_msg_size <= 0: raise ValueError("'max_msg_size' must be a positive integer.")
        if not isinstance(ping_interval, (int, float)) or ping_interval <= 0: raise ValueError("'ping_interval' must be a positive number.")
        if not isinstance(timeout, (int, float)) or timeout <= 0: raise ValueError("'timeout' must be a positive number.")
        
        self.base_back_off_s = base_back_off_s
        self.max_back_off_s = max_back_off_s
        self.back_off_rate = back_off_rate
        self.max_queue = max_queue
        self.max_msg_size = max_msg_size
        self.ping_interval = ping_interval
        self.timeout = timeout

# In your WebSocketManager class

class WebSocketManager:
    # __init__ and _reader methods are unchanged...
    def __init__(self, callbacks: WebSocketTaskCallbacks, config: WebSocketTaskConfig, request_break_s: float = 0.3,
                 max_concurrent_subscriptions = 3):
        self.callbacks = callbacks
        self.config = config
        self._managed_tasks: Dict[TaskOffset, _ManagedTask] = {}
        self._connection_semaphore = asyncio.Semaphore(max_concurrent_subscriptions)
        self._request_break_s = request_break_s
        self._last_request_time: float = 0.0

    async def _reader(self, offset: TaskOffset, managed_task: _ManagedTask):
        state = managed_task.state
        try:
            while True:
                try:
                    message = await managed_task.wss_cli.recv()
                except Exception as e:
                    if self.callbacks.on_read_failure:
                        if not self.callbacks.on_read_failure(offset, e, state.url):
                            return 
                    break
                state.messages_received += 1
                state.last_activity_time = time.monotonic()
                if not self.callbacks.on_read(offset, message):
                    break
        except asyncio.CancelledError:
            pass
        finally:
            if managed_task.wss_cli:
                await managed_task.wss_cli.close()

    

    # In your WebSocketManager class

class WebSocketManager:
    # __init__ and other methods are unchanged...
    def __init__(self, callbacks: WebSocketTaskCallbacks, config: WebSocketTaskConfig, request_break_s: float = 0.3,
                 max_concurrent_subscriptions = 3):
        self.callbacks = callbacks
        self.config = config
        self._managed_tasks: Dict[TaskOffset, _ManagedTask] = {}
        self._connection_semaphore = asyncio.Semaphore(max_concurrent_subscriptions)
        self._request_break_s = request_break_s
        self._last_request_time: float = 0.0

    # _reader, start_task, etc. are also unchanged.
    async def _reader(self, offset: TaskOffset, managed_task: _ManagedTask):
        state = managed_task.state
        try:
            while True:
                try:
                    message = await managed_task.wss_cli.recv()
                except Exception as e:
                    if self.callbacks.on_read_failure:
                        if not self.callbacks.on_read_failure(offset, e, state.url):
                            return 
                    break
                state.messages_received += 1
                state.last_activity_time = time.monotonic()
                if not self.callbacks.on_read(offset, message):
                    break
        except asyncio.CancelledError:
            pass
        finally:
            if managed_task.wss_cli:
                await managed_task.wss_cli.close()
                
    # --- THIS IS THE FULLY CORRECTED _connector METHOD ---
    async def _connector(self, offset: TaskOffset, payload: Message, managed_task: _ManagedTask, 
                         old_task_to_terminate: Optional[_ManagedTask] = None):
        state = managed_task.state
        state.current_back_off_s = self.config.base_back_off_s
        
        while True:
            managed_task.connection_state = "DISCONNECTED"
            # We must define wss_cli here to access it in the finally block
            wss_cli = None
            try:
                # --- Step 1: Acquire a "connecting" slot ---
                # A task will wait here if 3 other tasks are already connecting.
                async with self._connection_semaphore:
                    managed_task.connection_state = "CONNECTING"
                    
                    now = time.monotonic()
                    elapsed = now - self._last_request_time
                    if elapsed < self._request_break_s:
                        await asyncio.sleep(self._request_break_s - elapsed)
                    self._last_request_time = time.monotonic()

                    state.connection_attempts += 1
                    
                    # --- Step 2: Manually connect and handshake ---
                    # We CANNOT use 'async with' here, as it would close the connection
                    # when the semaphore is released.
                    wss_cli = await websockets.connect(
                        state.url, max_queue=self.config.max_queue, max_size=self.config.max_msg_size,
                        ping_interval=self.config.ping_interval, open_timeout=self.config.timeout
                    )
                    managed_task.wss_cli = wss_cli
                    state.last_activity_time = time.monotonic()
                    
                    await wss_cli.send(payload)
                    ack = await wss_cli.recv()
                    state.last_activity_time = time.monotonic()

                    if self.callbacks.on_acknowledgement and not self.callbacks.on_acknowledgement(offset, ack):
                        return
                    if self.callbacks.on_connect and not self.callbacks.on_connect(offset):
                        return
                    
                managed_task.connection_state = "CONNECTED"
                
                if old_task_to_terminate and old_task_to_terminate.main_task and not old_task_to_terminate.main_task.done():
                    old_task_to_terminate.main_task.cancel()
                
                state.current_back_off_s = self.config.base_back_off_s
                
                # --- Step 4: Run the reader on the open connection ---
                # This will block until the connection is lost.
                await self._reader(offset, managed_task)

            except asyncio.CancelledError:
                managed_task.connection_state = "DISCONNECTED"
                return
            except Exception as e:
                state.connection_failures += 1
                if self.callbacks.on_connect_failure:
                    if not self.callbacks.on_connect_failure(offset, e, state.url, payload):
                        managed_task.connection_state = "DISCONNECTED"
                        return
            finally:
                # --- Step 5: CRITICAL cleanup ---
                # Because we are not using 'async with websockets.connect',
                # we MUST manually close the connection in all cases.
                if wss_cli:
                    await wss_cli.close()
            
            # Backoff logic remains the same.
            managed_task.connection_state = "DISCONNECTED"
            await asyncio.sleep(state.current_back_off_s)
            state.current_back_off_s = min(state.current_back_off_s * self.config.back_off_rate, self.config.max_back_off_s)

    # start_task, terminate_task, purge_tasks, get_stats are unchanged...
    def start_task(self, offset: TaskOffset, url: str, initial_payload: Message):
        old_managed_task = self._managed_tasks.get(offset)
        state = _WebSocketTaskState()
        state.url = url
        state.current_back_off_s = self.config.base_back_off_s
        new_managed_task = _ManagedTask(None, state)
        main_task = asyncio.create_task(
            self._connector(offset, initial_payload, new_managed_task, old_task_to_terminate=old_managed_task)
        )
        new_managed_task.main_task = main_task
        self._managed_tasks[offset] = new_managed_task

    def terminate_task(self, offset: TaskOffset):
        managed_task = self._managed_tasks.get(offset)
        if managed_task and not managed_task.main_task.done():
            managed_task.main_task.cancel()
        if offset in self._managed_tasks:
            del self._managed_tasks[offset]

    def purge_tasks(self):
        for offset in list(self._managed_tasks.keys()):
            self.terminate_task(offset)

    def get_stats(self, offset: TaskOffset) -> Optional[Dict[str, Any]]:
        managed_task = self._managed_tasks.get(offset)
        return managed_task.get_stats(self.config) if managed_task else None

    # --- MODIFIED: get_all_stats method ---
    def get_all_stats(self) -> Dict[str, Any]:
        summary = {
            "connected": 0,
            "connecting": 0,
            "disconnected": 0,
        }
        per_task_stats = {}
        for offset, task in self._managed_tasks.items():
            summary[task.connection_state.lower()] += 1
            per_task_stats[offset] = task.get_stats(self.config)
                
        return {
            "connection_summary": summary,
            "per_task_stats": per_task_stats
        }   
