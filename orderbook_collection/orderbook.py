from typing import Callable, Dict, Set

class OrderbookRegistry:
    """
    A class-based registry to track the set of active tokens (asset_ids).
    
    This registry manages a central set of tokens and provides callback mechanisms
    for other modules to react to the insertion or deletion of tokens.
    """
    _tokens: Set[str] = set()
    _removed_tokens = 0
    _inserted_tokens = 0
    
    Callback_handle = int
    _next_callback_handle = 0
    _delete_callbacks: Dict[Callback_handle, Callable[[str], None]] = {}
    _insert_callbacks: Dict[Callback_handle, Callable[[str], None]] = {}

    @classmethod
    def lifetime_removed(cls) -> int:
        """Returns the total number of tokens removed during the application's lifetime."""
        return cls._removed_tokens
    
    @classmethod
    def lifetime_inserted(cls) -> int:
        """Returns the total number of tokens inserted during the application's lifetime."""
        return cls._inserted_tokens

    @classmethod
    def _register_callback(cls, callback_dict: Dict, callback_func: Callable) -> int:
        """A generic helper to register any callback."""
        handle = cls._next_callback_handle
        callback_dict[handle] = callback_func
        cls._next_callback_handle += 1
        return handle

    @classmethod
    def register_delete_callback(cls, delete_callback: Callable[[str], None]) -> int:
        """Registers a function to be called whenever a token is deleted."""
        return cls._register_callback(cls._delete_callbacks, delete_callback)

    @classmethod
    def register_insert_callback(cls, insert_callback: Callable[[str], None]) -> int:
        """Registers a function to be called whenever a new token is inserted."""
        return cls._register_callback(cls._insert_callbacks, insert_callback)

    @classmethod
    def unregister_delete_callback(cls, handle: int):
        """Safely unregisters a delete callback using its handle."""
        cls._delete_callbacks.pop(handle, None)

    @classmethod
    def unregister_insert_callback(cls, handle: int):
        """Safely unregisters an insert callback using its handle."""
        cls._insert_callbacks.pop(handle, None)

    @classmethod
    def insert_token(cls, token: str) -> bool:
        """
        Adds a new token to the registry.
        
        Returns False if the token already exists, True otherwise.
        Triggers all registered insert callbacks on successful insertion.
        """
        if token in cls._tokens:
            return False
        
        cls._tokens.add(token)
        cls._inserted_tokens += 1
        
        # Iterate over a copy in case a callback modifies the dictionary
        for call_back in list(cls._insert_callbacks.values()):
            call_back(token)
        return True

    @classmethod
    def delete_token(cls, token: str) -> bool:
        """
        Removes a token from the registry.

        Returns False if the token does not exist, True otherwise.
        Triggers all registered delete callbacks on successful deletion.
        """
        if token not in cls._tokens:
            return False
        
        cls._tokens.remove(token)
        cls._removed_tokens += 1
        
        for call_back in list(cls._delete_callbacks.values()):
            call_back(token)
        return True
    
    @classmethod
    def iterate_over_tokens(cls, on_iteration: Callable[[str], bool]):
        """
        Iterates over all registered tokens, calling the provided function for each.
        
        The iteration stops if the on_iteration function returns False.
        """
        for token in list(cls._tokens):
            if not on_iteration(token):
                return

    @classmethod
    def get_token_count(cls) -> int:
        """Returns the current number of active tokens in the registry."""
        return len(cls._tokens)