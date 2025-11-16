import inspect
from typing import Callable, Optional, Awaitable, Any

async def maybe_await(callback: Optional[Callable[..., Any]], *args: Any, **kwargs: Any) -> Any:
    """
    Invokes a callback with given arguments, awaiting it if it's a coroutine,
    and returns the result.

    Args:
        callback: The callable to invoke. Can be sync or async.
        *args: Positional arguments to pass to the callback.
        **kwargs: Keyword arguments to pass to the callback.
    
    Returns:
        The result of the callback.
    """
    if callable(callback):
        result = callback(*args, **kwargs)
        if inspect.isawaitable(result):
            # Return the awaited result
            return await result
        else:
            # Return the synchronous result
            return result
    # Return None if there was no callback
    return None