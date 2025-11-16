import numpy as np
from sortedcontainers import SortedDict
from typing import TypeAlias, Callable, Optional, Dict, List, Tuple

class Orderbook:
    # Renamed class variables to avoid conflict with instance slots
    total_tick_changes_count = 0
    total_book_count = 0
    total_price_change_count = 0
    total_last_trade_count = 0

    # __slots__ now correctly refers only to instance attributes without conflict
    __slots__ = ('bids', 'asks', 'tick_size', 'tick_changes_count', 'book_count', 'price_change_count', 'last_trade_count')

    def __init__(self):
        self.bids: SortedDict[float, float] = SortedDict(lambda k: -k)
        self.asks: SortedDict[float, float] = SortedDict()
        self.tick_size: Optional[float] = None
        # These are the instance-specific counters
        self.tick_changes_count = 0
        self.book_count = 0
        self.price_change_count = 0
        self.last_trade_count = 0

    def tick_size_change(self, tick_size: float):
        self.tick_changes_count += 1
        # Corrected to use the new renamed class variable
        Orderbook.total_tick_changes_count += 1
        self.tick_size = tick_size
    
    def new_price_change(self):
        self.price_change_count += 1
        # Corrected to use the new renamed class variable
        Orderbook.total_price_change_count += 1

    def bids_change(self, price: float, size: float):
        if size == 0:
            self.bids.pop(price, None)
        else:
            self.bids[price] = size
    
    def bids_trade(self, price: float, size: float):
        self.last_trade_count += 1
        Orderbook.total_last_trade_count += 1
        if size == 0:
            self.bids.pop(price, None)
        else:
            self.bids[price] = size

    def new_book(self):
        self.book_count += 1
        Orderbook.total_book_count += 1
        # new_book should reset the orderbook sides
        self.bids.clear()
        self.asks.clear()

    def asks_change(self, price: float, size: float):
        if size == 0:
            self.asks.pop(price, None)
        else:
            self.asks[price] = size

    def asks_trade(self, price: float, size: float):
        self.last_trade_count += 1
        Orderbook.total_last_trade_count += 1
        if size == 0:
            # Typo corrected: should be asks.pop, not bids.pop
            self.asks.pop(price, None)
        else:
            # Typo corrected: should be self.asks, not self.bids
            self.asks[price] = size

    def get_book_metrics(
        self, compute_time: int, asset_id: str, server_bids: np.ndarray,
        server_asks: np.ndarray, server_tick_size: float
    ) -> tuple:
        local_bids_array = self._sorted_local_side(self.bids)
        local_asks_array = self._sorted_local_side(self.asks)
        local_bids_vol = self._volume(local_bids_array)
        server_bids_vol = self._volume(server_bids)
        local_asks_vol = self._volume(local_asks_array)
        server_asks_vol = self._volume(server_asks)
        best_worst_bids = self._get_best_worst(self.bids)
        best_worst_asks = self._get_best_worst(self.asks)
        tick_size_dist = abs(self.tick_size - server_tick_size) if self.tick_size and server_tick_size else None

        return (
            compute_time, asset_id, self._distance(local_bids_array, server_bids),
            local_bids_array.shape[1], server_bids.shape[1], abs(local_bids_array.shape[1] - server_bids.shape[1]),
            local_bids_vol, server_bids_vol, abs(local_bids_vol - server_bids_vol),
            *best_worst_bids, self._distance(local_asks_array, server_asks),
            local_asks_array.shape[1], server_asks.shape[1], abs(local_asks_array.shape[1] - server_asks.shape[1]),
            local_asks_vol, server_asks_vol, abs(local_asks_vol - server_asks_vol),
            *best_worst_asks, tick_size_dist)

    @staticmethod
    def _get_best_worst(side: SortedDict) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        if not side:
            return None, None, None, None
        best_price, best_size = side.peekitem(0)
        worst_price, worst_size = side.peekitem(-1)
        return best_price, best_size, worst_price, worst_size

    @staticmethod
    def _volume(side: np.ndarray) -> float:
        if side.shape[1] == 0:
            return 0.0
        return np.sum(side[1, :])

    @staticmethod
    def _sorted_local_side(local_side: SortedDict) -> np.ndarray:
        if not local_side:
            return np.empty((2, 0), dtype=np.float64)
        return np.array([list(local_side.keys()), list(local_side.values())], dtype=np.float64)

    @staticmethod
    def _distance(side1: np.ndarray, side2: np.ndarray) -> float:
        min_len = min(side1.shape[1], side2.shape[1])
        if min_len == 0:
            return 0.0
        price_diff = side1[0, :min_len] - side2[0, :min_len]
        size_diff = side1[1, :min_len] - side2[1, :min_len]
        weights = 1 / (np.arange(min_len) + 1)
        distance = np.sum(weights * (price_diff**2 + size_diff**2))
        return float(distance)

class OrderbookRegistry:
    _token_to_orderbook: Dict[str, 'Orderbook'] = {}
    _removed_tokens = 0
    _inserted_tokens = 0
    Callback_handle = int
    _next_callback_handle = 0
    _delete_callbacks: Dict[Callback_handle, Callable[[str], None]] = {}
    _insert_callbacks: Dict[Callback_handle, Callable[[str], None]] = {}

    @classmethod
    def lifetime_removed(cls)->int:
        return cls._removed_tokens
    
    @classmethod
    def lifetime_inserted(cls)->int:
        return cls._inserted_tokens

    @classmethod
    def get_order_book(cls, token: str) -> Optional['Orderbook']:
        return cls._token_to_orderbook.get(token)

    @classmethod
    def _register_callback(cls, callback_dict: Dict, callback_func: Callable) -> int:
        """A generic helper to register any callback."""
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
        """Safely unregisters a delete callback."""
        cls._delete_callbacks.pop(handle, None)

    @classmethod
    def unregister_insert_callback(cls, handle: int):
        """Safely unregisters an insert callback."""
        cls._insert_callbacks.pop(handle, None)

    @classmethod
    def insert_token(cls, token: str)->bool:
        if token in cls._token_to_orderbook:
            return False
        cls._token_to_orderbook[token] = Orderbook()
        cls._inserted_tokens += 1
        for call_back in list(cls._insert_callbacks.values()):
            call_back(token)
        return True

    @classmethod
    def delete_token(cls, token: str)->bool:
        if token not in list(cls._token_to_orderbook):
            return False
        del cls._token_to_orderbook[token]
        cls._removed_tokens += 1
        for call_back in list(cls._delete_callbacks.values()):
            call_back(token)
        return True
    
    @classmethod
    def iterate_over_tokens(cls, on_iteration: Callable[[str], bool]):
        for token in list(cls._token_to_orderbook.keys()):
            if not on_iteration(token):
                return

    @classmethod
    def get_token_count(cls) -> int:
        return len(cls._token_to_orderbook)