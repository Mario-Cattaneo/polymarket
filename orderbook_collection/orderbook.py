from typing import Callable, Dict, Set, List

class OrderbookRegistry:
    """
    A class-based registry to track the set of active tokens (asset_ids).
    
    This registry manages a central set of tokens and provides callback mechanisms
    for other modules to react to the insertion or deletion of tokens.
    It also tracks tokens added within a single "exhaustion cycle" for batch processing.
    """
    _tokens: Set[str] = set()
    _exhaustion_cycle_batch: List[str] = []  # NEW: Tracks tokens for the current cycle
    _removed_tokens = 0
    _inserted_tokens = 0
    
    Callback_handle = int
    _next_callback_handle = 0
    _delete_callbacks: Dict[Callback_handle, Callable[[str], None]] = {}
    _insert_callbacks: Dict[Callback_handle, Callable[[str], None]] = {}

    @classmethod
    def lifetime_removed(cls) -> int:
        return cls._removed_tokens
    
    @classmethod
    def lifetime_inserted(cls) -> int:
        return cls._inserted_tokens

    @classmethod
    def _register_callback(cls, callback_dict: Dict, callback_func: Callable) -> int:
        handle = cls._next_callback_handle
        callback_dict[handle] = callback_func
        cls._next_callback_handle += 1
        return handle

    @classmethod
    def register_delete_callback(cls, delete_callback: Callable[[str], None]) -> int:
        return cls._register_callback(cls._delete_callbacks, delete_callback)

    @classmethod
    def register_insert_callback(cls, insert_callback: Callable[[str], None]) -> int:
        return cls._register_callback(cls._insert_callbacks, insert_callback)

    @classmethod
    def unregister_delete_callback(cls, handle: int):
        cls._delete_callbacks.pop(handle, None)

    @classmethod
    def unregister_insert_callback(cls, handle: int):
        cls._insert_callbacks.pop(handle, None)

    @classmethod
    def insert_token(cls, token: str) -> bool:
        if token in cls._tokens:
            return False
        
        cls._tokens.add(token)
        cls._exhaustion_cycle_batch.append(token) # MODIFIED: Add to cycle batch
        cls._inserted_tokens += 1
        
        for call_back in list(cls._insert_callbacks.values()):
            call_back(token)
        return True

    @classmethod
    def delete_token(cls, token: str) -> bool:
        if token not in cls._tokens:
            return False
        
        cls._tokens.remove(token)
        cls._removed_tokens += 1
        
        for call_back in list(cls._delete_callbacks.values()):
            call_back(token)
        return True
    
    @classmethod
    def iterate_over_tokens(cls, on_iteration: Callable[[str], bool]):
        for token in list(cls._tokens):
            if not on_iteration(token):
                return

    @classmethod
    def get_token_count(cls) -> int:
        return len(cls._tokens)

    @classmethod
    def get_all_tokens(cls) -> List[str]:
        """Returns a list of all current tokens for full reserialization."""
        return list(cls._tokens)

    @classmethod
    def consume_exhaustion_cycle_batch(cls) -> List[str]:
        """
        NEW: Atomically retrieves and clears the list of tokens from the last cycle.
        This is the primary method for the Events module to get new tokens.
        """
        if not cls._exhaustion_cycle_batch:
            return []
        
        batch_to_return = list(cls._exhaustion_cycle_batch)
        cls._exhaustion_cycle_batch.clear()
        return batch_to_return