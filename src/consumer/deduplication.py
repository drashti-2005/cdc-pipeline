"""
Event Deduplication - Exactly-Once Semantics
=============================================
Ensures each CDC event is processed exactly once, even if delivered multiple times.

WHY DEDUPLICATION?
------------------
Kafka provides "at-least-once" delivery - events may be delivered multiple times:
1. Consumer crashes after processing but before committing offset
2. Network issues cause retry
3. Rebalancing sends same event to new consumer

Without deduplication, we'd see:
- Duplicate rows in target database
- Double-counted analytics
- Incorrect order totals

HOW IT WORKS
------------
We use an in-memory LRU (Least Recently Used) cache of event_ids.
- On each event, check if event_id is in cache
- If yes → duplicate, skip processing
- If no → process and add to cache

The cache has a size limit (default 100,000 events). When full,
oldest entries are evicted. This works because:
- Duplicates usually arrive close together in time
- Very old events are unlikely to be duplicated
- Memory usage is bounded

FOR INTERVIEW
-------------
Q: What if the consumer restarts?
A: Cache is lost, but worst case is processing a few duplicates.
   Our sinks are idempotent, so duplicates don't cause corruption.

Q: Why not use Redis or a database?
A: Adds latency and another failure point. In-memory is 1000x faster.
   For stricter requirements, we could use Redis with TTL.

Q: How do you size the cache?
A: Based on expected duplicates * time window. 100k events at 1KB each = 100MB.
"""

import logging
from collections import OrderedDict
from threading import Lock
from typing import Optional

from src.metrics import DEDUP_CACHE_SIZE

logger = logging.getLogger(__name__)


class DeduplicationCache:
    """
    LRU cache for tracking processed event IDs.

    Thread-safe implementation using OrderedDict for O(1) operations.
    When capacity is reached, oldest entries are automatically evicted.

    SIMPLE EXPLANATION:
    Think of this like a bouncer with a guest list:
    - Check if this person (event_id) already came in
    - If yes, turn them away (skip processing)
    - If no, let them in and add to the list
    - If the list gets too long, forget the oldest entries
    """

    def __init__(self, max_size: int = 100_000):
        """
        Initialize the deduplication cache.

        Args:
            max_size: Maximum number of event IDs to track.
                     When exceeded, oldest entries are evicted.
        """
        self._max_size = max_size
        self._cache: OrderedDict[str, bool] = OrderedDict()
        self._lock = Lock()

        # Metrics
        self._hits = 0  # Duplicates detected
        self._misses = 0  # New events

        logger.info(f"Deduplication cache initialized with max_size={max_size}")

    def is_duplicate(self, event_id: str) -> bool:
        """
        Check if an event has already been processed.

        If the event_id is in the cache, it's a duplicate.
        If not, we add it to the cache for future checks.

        Args:
            event_id: Unique identifier of the CDC event

        Returns:
            True if this is a duplicate (already seen)
            False if this is a new event
        """
        with self._lock:
            if event_id in self._cache:
                # Move to end (most recently seen)
                self._cache.move_to_end(event_id)
                self._hits += 1
                logger.debug(f"Duplicate detected: {event_id}")
                return True

            # New event - add to cache
            self._cache[event_id] = True
            self._misses += 1

            # Evict oldest if over capacity
            while len(self._cache) > self._max_size:
                evicted_id, _ = self._cache.popitem(last=False)
                logger.debug(f"Evicted from dedup cache: {evicted_id}")

            # Update cache size metric
            DEDUP_CACHE_SIZE.set(len(self._cache))

            return False

    def mark_processed(self, event_id: str) -> None:
        """
        Explicitly mark an event as processed.

        Use this when you want to mark without checking for duplicates.
        """
        with self._lock:
            self._cache[event_id] = True
            self._cache.move_to_end(event_id)

            # Evict oldest if over capacity
            while len(self._cache) > self._max_size:
                self._cache.popitem(last=False)

    def clear(self) -> None:
        """Clear all entries from the cache."""
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0
            logger.info("Deduplication cache cleared")

    # ========================================================
    # Metrics (for monitoring)
    # ========================================================

    @property
    def size(self) -> int:
        """Current number of entries in cache."""
        with self._lock:
            return len(self._cache)

    @property
    def hit_rate(self) -> float:
        """Percentage of lookups that were duplicates."""
        with self._lock:
            total = self._hits + self._misses
            if total == 0:
                return 0.0
            return (self._hits / total) * 100

    def get_stats(self) -> dict:
        """Get cache statistics for monitoring."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100) if total > 0 else 0.0
            return {
                "cache_size": len(self._cache),
                "max_size": self._max_size,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate_percent": hit_rate,
            }
