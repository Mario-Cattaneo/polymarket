import asyncio
import httpx
import inspect
import time
from enum import Enum
from typing import TypeAlias, Callable, Optional, Dict, Any, Tuple, Coroutine, Union, Awaitable, Set

from utils import maybe_await

Request_id: TypeAlias = Any
URL: TypeAlias = str
Headers: TypeAlias = Dict[str, str]
ResponseContent: TypeAlias = bytes
RequestContent: TypeAlias = bytes
StatusCode: TypeAlias = int
RTT: TypeAlias = float

class Method(Enum):
    GET = 0
    POST = 1


Next_request: TypeAlias = Callable[[], Tuple[Request_id, URL, Method, Optional[Headers], Optional[RequestContent]]]

On_response: TypeAlias = Callable[
    [Request_id, ResponseContent, StatusCode, Optional[Headers], RTT], 
    Union[bool, Awaitable[bool]]
]

On_exception: TypeAlias = Callable[
    [Optional[Request_id], Exception], 
    Union[bool, Awaitable[bool]]
]

Prerequest: TypeAlias = Callable[['RequestHandler'], Coroutine[None, None, bool]]


class HttpTaskConfig:
    __slots__ = ('base_back_off_s', 'max_back_off_s', 'back_off_rate', 'request_break_s', 'timeout_s')

    def __init__(self, 
                 base_back_off_s: float = 1.0, 
                 max_back_off_s: float = 90.0, 
                 back_off_rate: float = 2.0, 
                 request_break_s: float = 0.1,
                 timeout_s: float = 30.0):
        if not isinstance(base_back_off_s, (int, float)) or base_back_off_s < 0: raise ValueError(f"'base_back_off_s' must be a non-negative number, but got {base_back_off_s}")
        if not isinstance(max_back_off_s, (int, float)) or max_back_off_s < base_back_off_s: raise ValueError(f"'max_back_off_s' must be a number >= base_back_off_s, but got {max_back_off_s}")
        if not isinstance(back_off_rate, float) or back_off_rate < 1.0: raise ValueError(f"'back_off_rate' must be a float >= 1.0, but got {back_off_rate}")
        if not isinstance(request_break_s, (int, float)) or request_break_s < 0: raise ValueError(f"'request_break_s' must be a non-negative number, but got {request_break_s}")
        if not isinstance(timeout_s, (int, float)) or timeout_s < 0: raise ValueError(f"'timeout_s' must be a non-negative number, but got {timeout_s}")
        self.base_back_off_s = base_back_off_s
        self.max_back_off_s = max_back_off_s
        self.back_off_rate = back_off_rate
        self.request_break_s = request_break_s
        self.timeout_s = timeout_s

    def to_dict(self) -> dict:
        return {attr: getattr(self, attr) for attr in self.__slots__}

    def __repr__(self):
        return str(self.to_dict())
    
class HttpTaskCallbacks:
    __slots__ = ('next_request', 'on_response', 'on_exception', 'prerequest')

    def __init__(self,
                 next_request: Optional[Next_request],
                 on_response: Optional[On_response] = None,
                 on_exception: Optional[On_exception] = None,
                 prerequest: Optional[Prerequest] = None):
        
        if not (callable(on_response) and len(inspect.signature(on_response).parameters) == 5):
            raise ValueError("'on_response' must be a callable that accepts 5 arguments got")
        if on_exception is not None and not (callable(on_exception) and len(inspect.signature(on_exception).parameters) == 2):
            raise ValueError("'on_exception' must be a callable that accepts 2 arguments.")
        if next_request is not None and not (callable(next_request) and len(inspect.signature(next_request).parameters) == 0):
            raise ValueError("'next_request' must be a callable that accepts 0 arguments.")
        if prerequest is not None and not (callable(prerequest) and len(inspect.signature(prerequest).parameters) == 1):
            raise ValueError("'prerequest' must be a callable that accepts 1 argument.")
            
        self.prerequest = prerequest
        self.on_response = on_response
        self.on_exception = on_exception
        self.next_request = next_request

    def to_dict(self) -> dict:
        return {attr: getattr(self, attr, None) for attr in self.__slots__}

    def __repr__(self):
        return str(self.to_dict())
    
class RequestHandler:
    __slots__ = ('handler_task', 'requests_made', 'network_failures', 'last_request_time_s', 
                 'request_id', 'url', 'method', 'payload', 'headers')

    def __init__(self, 
                 request_id: Request_id, 
                 url: URL, 
                 method: Method,
                 headers: Optional[Headers], 
                 payload: Optional[bytes]):
        if not isinstance(url, str): raise ValueError(f"non str url {url}")
        if not isinstance(method, Method): raise ValueError(f"non Method method {method}")
        self.request_id = request_id
        self.url = url
        self.method = method
        self.payload = payload
        self.headers = headers
        self.handler_task: Optional[asyncio.Task] = None
        self.requests_made = 0
        self.network_failures = 0
        self.last_request_time_s = 0.0

    def start(self, 
              client: httpx.AsyncClient, 
              config: HttpTaskConfig, 
              callbacks: HttpTaskCallbacks, 
              done_callback: Callable[['RequestHandler'], None]):
        if self.handler_task:
            raise RuntimeError("Handler has already been started.")
        coro = self._request_lifecycle(client, config, callbacks)
        self.handler_task = asyncio.create_task(coro)
        self.handler_task.add_done_callback(lambda task: done_callback(self))
    
    def stop(self):
        if self.handler_task and not self.handler_task.done():
            self.handler_task.cancel()

    async def _request_lifecycle(self, 
                                 client: httpx.AsyncClient, 
                                 config: HttpTaskConfig, 
                                 callbacks: HttpTaskCallbacks):
        back_off_s = config.base_back_off_s
        handler_task = asyncio.current_task()
        while True:
            response = None
            try:
                if callbacks.prerequest and not await callbacks.prerequest(self):
                    continue
                
                self.last_request_time_s = time.monotonic()
                
                if self.method == Method.GET:
                    response = await client.get(self.url, headers=self.headers, timeout=config.timeout_s)
                elif self.method == Method.POST:
                    response = await client.post(self.url, content=self.payload, headers=self.headers, timeout=config.timeout_s)
      
                if handler_task.cancelled():
                    return

                rtt = time.monotonic() - self.last_request_time_s
                self.requests_made += 1
                
                should_stop = await maybe_await(
                    callbacks.on_response,
                    self.request_id, response.content, response.status_code, response.headers, rtt
                )
                
                if should_stop:
                    return

            except asyncio.CancelledError:
                return
            except Exception as e:
                self.network_failures += 1
                if callbacks.on_exception:
                    should_stop_on_exception = await maybe_await(
                        callbacks.on_exception, self.request_id, e
                    )
                    
                    if should_stop_on_exception:
                        return
            
            await asyncio.sleep(back_off_s)
            back_off_s = min(back_off_s * config.back_off_rate, config.max_back_off_s)

    def to_dict(self) -> dict:
        return {"running": self.handler_task is not None and not self.handler_task.done(),
                "requests_made":self.requests_made,
                "network_failures":self.network_failures,
                "last_request_time_s":self.last_request_time_s}

    def __repr__(self):
        return str(self.to_dict())

class RequestProducer:
    __slots__ = ('producer_task', 'config', 'callbacks', 'requests', 
                 'max_concurrent_requests', 'client',
                 'last_request_time_s', '_requests_change', '_stall')

    def __init__(self, 
                 config: HttpTaskConfig,
                 callbacks: HttpTaskCallbacks,
                 max_concurrent_requests: int,
                 client: httpx.AsyncClient):
        
        if not isinstance(client, httpx.AsyncClient): raise ValueError("invalid client")
        if not isinstance(callbacks, HttpTaskCallbacks): raise ValueError("invalid callbacks")
        if not isinstance(config, HttpTaskConfig): raise ValueError("invalid config")
        if not (isinstance(max_concurrent_requests, int) and max_concurrent_requests > 0): raise ValueError("invalid max_concurrent_requests")
        
        self.producer_task: Optional[asyncio.Task] = None
        self.config = config
        callbacks.prerequest = self._ratelimit
        self.callbacks = callbacks
        self.max_concurrent_requests = max_concurrent_requests
        self.client = client
        self._requests_change = asyncio.Event()
        # This is the single source of truth for all active requests
        self.requests: Dict[asyncio.Task, RequestHandler] = {}
        self.last_request_time_s = 0.0
        self._stall = asyncio.Event()
        self._stall.clear()

    def start(self):
        if self.producer_task and not self.producer_task.done():
            raise RuntimeError("Producer has already been started.")
        self.producer_task = asyncio.create_task(self._request_producer())

    def stop(self):
        """
        Synchronously initiates the cancellation of the producer and all its active request handlers.
        This method is non-blocking and returns immediately.
        """
        if self.producer_task and not self.producer_task.done():
            self.producer_task.cancel()

        for handler in list(self.requests.values()):
            handler.stop()

    def set_max_concurrent_requests(self, max_concurrent_requests: int):
        """Safely updates the concurrency limit and wakes up the producer."""
        self.max_concurrent_requests = max_concurrent_requests
        self._requests_change.set()

    def _requester_done(self, handler: RequestHandler):
        """Callback executed when a RequestHandler finishes its lifecycle."""
        task = handler.handler_task
        if task in self.requests:
            self.requests.pop(task)
        # Signal that a slot has been freed.
        self._requests_change.set()

    def stall(self):
        self._stall.set()

    async def _ratelimit(self, request_handler: RequestHandler) -> bool:
        """Pre-request callback to enforce a delay between requests."""
        now = time.monotonic()
        break_time_s = self.config.request_break_s
        elapsed = now - self.last_request_time_s
        
        if elapsed < break_time_s:
            await asyncio.sleep(break_time_s - elapsed)
            # Returning False tells the handler to retry the prerequest check,
            # but we don't need to add/remove from any sets here.
            return False
        
        self.last_request_time_s = now
        return True
    
    async def _request_producer(self):
        """The main loop that creates and manages request handlers."""
        try:
            while True:
                # This loop waits until a slot is available.
                if len(self.requests) >= self.max_concurrent_requests:  
                    await self._requests_change.wait()
                    # Reset the event so we can wait on it again.
                    self._requests_change.clear()
                    continue

                request_id, url, method, headers, payload = self.callbacks.next_request()
                request_handler = RequestHandler(request_id, url, method, headers, payload)
                
                if self._stall.is_set():
                    await self._stall.wait()

                # Start the handler and add it to our tracking dictionary.
                request_handler.start(self.client, self.config, self.callbacks, self._requester_done)
                self.requests[request_handler.handler_task] = request_handler
                
        except asyncio.CancelledError:
            pass
        finally:
            # When the producer is cancelled, ensure all its children are stopped.
            for handler in self.requests.values():
                handler.stop()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "running": self.producer_task is not None and not self.producer_task.done(),
            "active_request_count": len(self.requests),
            "config": self.config.to_dict(),
            "max_concurrent_requests": self.max_concurrent_requests,
            "requesters": {f"handler_{idx}": handler.to_dict() for idx, handler in enumerate(self.requests.values())}
        }

    def __repr__(self) -> str:
        return str(self.to_dict())

class HttpManager:
    def __init__(self, 
                 timeout: float = 5.0, 
                 keepalive_expiry: float = 10.0,
                 max_connections: int = 100, 
                 max_keepalive_connections: int = 50,
                 transport_retries: int = 3, 
                 enable_http2: bool = True):
        
        timeouts = httpx.Timeout(connect=timeout, read=timeout, write=timeout, pool=timeout)
        limits = httpx.Limits(max_connections=max_connections, max_keepalive_connections=max_keepalive_connections, keepalive_expiry=keepalive_expiry)
        transport = httpx.AsyncHTTPTransport(retries=transport_retries)
        
        self.client = httpx.AsyncClient(http2=enable_http2, timeout=timeouts, limits=limits, transport=transport)

    def get_request_producer(self, 
                             config: HttpTaskConfig, 
                             callbacks: HttpTaskCallbacks, 
                             max_concurrent_requests: int) -> RequestProducer:

        return RequestProducer(
            config=config, 
            callbacks=callbacks, 
            max_concurrent_requests=max_concurrent_requests, 
            client=self.client
        )

    def get_request_handler(self,
                            request_id: Request_id,
                            url: URL,
                            method: Method,
                            headers: Optional[Headers] = None,
                            payload: Optional[bytes] = None) -> RequestHandler:

        return RequestHandler(
            request_id=request_id,
            url=url,
            method=method,
            headers=headers,
            payload=payload
        )

    async def shutdown(self):
        await self.client.aclose()