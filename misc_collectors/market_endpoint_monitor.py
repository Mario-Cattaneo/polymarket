#!/usr/bin/env python3

import asyncio
import logging
import orjson
import signal
import statistics
from typing import Tuple, Optional, Dict, Set, List
from enum import Enum

from http_cli import HttpTaskConfig, HttpTaskCallbacks, HttpManager, Method, RequestProducer, ResponseContent, StatusCode, RTT

# Configure logging - suppress verbose modules first
logging.getLogger().setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)

# Now configure our logger at INFO level
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MarketMonitor:
    """
    Monitors market endpoint responses at various concurrency levels.
    
    For each cycle, tracks:
    - Per-offset state: market_id and change counter
    - Bloom filter: detected duplicates within cycle
    - Statistics: min/max/avg/std of change counters
    """
    
    def __init__(self, max_concurrent_requests: int = 30, limit: int = 500):
        self._max_concurrent = max_concurrent_requests
        self._limit = limit
        
        # Request tracking
        self._cycle = 0
        self._next_offset = 0
        self._current_offset = 0
        
        # Per-cycle state: list of (market_id, change_counter) at each offset
        self._local_list: List[Tuple[Optional[str], int]] = []
        
        # Bloom filter for current cycle (set of market_ids seen)
        self._cycle_seen: Set[str] = set()
        
        # Statistics tracking
        self._cycle_duplicates = 0
        
        # HTTP infrastructure
        self._http_config = HttpTaskConfig(
            base_back_off_s=1.0,
            max_back_off_s=150.0,
            back_off_rate=2.0,
            request_break_s=0.05
        )
        
        self._http_callbacks = HttpTaskCallbacks(
            next_request=self._next_request,
            on_response=self._on_response,
            on_exception=self._on_exception
        )
        
        self._http_manager: Optional[HttpManager] = None
        self._getter_task: Optional[RequestProducer] = None
        self._running = False
    
    def _next_request(self) -> Tuple[Tuple[int, int], str, Method, Optional[Dict], Optional[bytes]]:
        """Generate next request with (cycle, offset) tuple as request_id."""
        request_id = (self._cycle, self._next_offset)
        url = f"https://gamma-api.polymarket.com/markets?limit={self._limit}&offset={self._next_offset}"
        self._next_offset += self._limit
        return (request_id, url, Method.GET, None, None)
    
    async def _on_response(self, request_id: Tuple[int, int], response_content: ResponseContent, 
                          status_code: StatusCode, headers: Optional[Dict[str, str]], rtt: RTT) -> bool:
        """Process market endpoint response."""
        request_cycle, request_offset = request_id
        
        if status_code != 200:
            logger.warning(f"Response for cycle {request_cycle}, offset {request_offset} received non-200 status: {status_code}")
            return False
        
        try:
            markets = orjson.loads(response_content)
        except Exception as e:
            logger.warning(f"JSON parsing failed for cycle {request_cycle}, offset {request_offset}. Error: {e}")
            return False
        
        if not isinstance(markets, list):
            logger.warning(f"Expected list of markets but got {type(markets).__name__} for cycle {request_cycle}, offset {request_offset}")
            return False
        
        # Empty response means cycle is exhausted
        if not markets:
            if request_cycle == self._cycle:
                self._print_cycle_summary()
                self._next_offset = 0
                self._cycle += 1
                self._cycle_seen.clear()
                self._cycle_duplicates = 0
                logger.info(f"Cycle {request_cycle} complete. Starting cycle {self._cycle}.")
            return True
        
        # Process market IDs from response
        for i, market_obj in enumerate(markets):
            market_id = market_obj.get("id")
            if not market_id:
                continue
            
            offset_index = request_offset + i
            
            # Check bloom filter for duplicates
            if market_id in self._cycle_seen:
                self._cycle_duplicates += 1
            else:
                self._cycle_seen.add(market_id)
            
            # Extend local list if necessary
            while len(self._local_list) <= offset_index:
                self._local_list.append((None, 0))
            
            # Check if market_id at this offset changed
            prev_market_id, change_counter = self._local_list[offset_index]
            
            # Only count as change if it was previously populated and differs
            if prev_market_id is not None and prev_market_id != market_id:
                change_counter += 1
                self._local_list[offset_index] = (market_id, change_counter)
                logger.info(f"Change at offset {offset_index}: {prev_market_id} -> {market_id} (change_counter={change_counter})")
            elif prev_market_id is None:
                # First population of this offset
                self._local_list[offset_index] = (market_id, 0)
            else:
                # Market ID is the same, no change needed
                pass
        
        return True
    
    async def _on_exception(self, request_id: Tuple[int, int], exception: Exception) -> bool:
        """Handle request exceptions."""
        request_cycle, request_offset = request_id
        logger.error(f"Exception for cycle {request_cycle}, offset {request_offset}: {exception}")
        return False
    
    def _print_cycle_summary(self):
        """Print statistics for the completed cycle."""
        if not self._local_list:
            logger.info(f"Cycle {self._cycle} summary: No markets received.")
            return
        
        # Extract change counters (skip None entries)
        counters = [counter for market_id, counter in self._local_list if market_id is not None]
        
        if not counters:
            logger.info(f"Cycle {self._cycle} summary: No valid market entries.")
            return
        
        min_counter = min(counters)
        max_counter = max(counters)
        avg_counter = sum(counters) / len(counters)
        std_counter = statistics.stdev(counters) if len(counters) > 1 else 0.0
        
        logger.info(f"Cycle {self._cycle} summary:")
        logger.info(f"  Total positions: {len(self._local_list)}")
        logger.info(f"  Populated positions: {len(counters)}")
        logger.info(f"  Duplicate market_ids detected: {self._cycle_duplicates}")
        logger.info(f"  Change counter stats - min={min_counter}, max={max_counter}, avg={avg_counter:.2f}, std={std_counter:.2f}")
    
    def _print_final_stats(self):
        """Print final statistics when monitor stops."""
        logger.info("=== Final Statistics ===")
        
        if not self._local_list:
            logger.info("No data collected.")
            return
        
        # Extract change counters
        counters = [counter for market_id, counter in self._local_list if market_id is not None]
        
        if not counters:
            logger.info("No valid market entries.")
            return
        
        min_counter = min(counters)
        max_counter = max(counters)
        avg_counter = sum(counters) / len(counters)
        std_counter = statistics.stdev(counters) if len(counters) > 1 else 0.0
        
        logger.info(f"Total positions tracked: {len(self._local_list)}")
        logger.info(f"Populated positions: {len(counters)}")
        logger.info(f"Change counter stats - min={min_counter}, max={max_counter}, avg={avg_counter:.2f}, std={std_counter:.2f}")
    
    async def start(self):
        """Start the market monitor."""
        self._running = True
        logger.info(f"Starting MarketMonitor with max_concurrent_requests={self._max_concurrent}, limit={self._limit}")
        
        try:
            self._http_manager = HttpManager()
            
            self._getter_task = self._http_manager.get_request_producer(
                config=self._http_config,
                callbacks=self._http_callbacks,
                max_concurrent_requests=self._max_concurrent
            )
            
            self._getter_task.start()
            
            # Wait for the producer task to complete
            if self._getter_task.producer_task:
                await self._getter_task.producer_task
        
        except asyncio.CancelledError:
            logger.info("Monitor cancelled by user")
        except Exception as e:
            logger.error(f"Error during execution: {e}")
        
        finally:
            self._running = False
            self._print_final_stats()
            if self._http_manager:
                await self._http_manager.shutdown()
    
    def shutdown(self):
        """Gracefully shutdown the monitor."""
        logger.info("Shutdown signal received, stopping monitor...")
        if self._getter_task:
            self._getter_task.stop()


async def main():
    """Main entry point."""
    monitor = MarketMonitor(max_concurrent_requests=1, limit=500)
    
    # Handle graceful shutdown
    def signal_handler(sig, frame):
        monitor.shutdown()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    await monitor.start()


if __name__ == "__main__":
    asyncio.run(main())
