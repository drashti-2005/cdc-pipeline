"""
Unit Tests for Deduplication Cache
===================================
Tests the LRU deduplication cache for event processing.

Run with: pytest tests/unit/test_deduplication.py -v
"""

import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from src.consumer.deduplication import DeduplicationCache


class TestDeduplicationCache:
    """Tests for DeduplicationCache."""

    def test_create_cache(self):
        """Test creating a deduplication cache."""
        cache = DeduplicationCache(max_size=100)
        
        assert cache._max_size == 100
        assert len(cache._cache) == 0

    def test_default_size(self):
        """Test default cache size."""
        cache = DeduplicationCache()
        
        assert cache._max_size == 100_000

    def test_new_event_not_duplicate(self):
        """Test that new events are not flagged as duplicates."""
        cache = DeduplicationCache(max_size=100)
        event_id = str(uuid.uuid4())
        
        # First time seeing this event
        is_dup = cache.is_duplicate(event_id)
        
        assert is_dup is False

    def test_seen_event_is_duplicate(self):
        """Test that seen events are flagged as duplicates."""
        cache = DeduplicationCache(max_size=100)
        event_id = str(uuid.uuid4())
        
        # First call adds to cache
        cache.is_duplicate(event_id)
        
        # Second call should return True
        is_dup = cache.is_duplicate(event_id)
        
        assert is_dup is True

    def test_multiple_different_events(self):
        """Test tracking multiple different events."""
        cache = DeduplicationCache(max_size=100)
        event_ids = [str(uuid.uuid4()) for _ in range(10)]
        
        # All should be new
        for event_id in event_ids:
            assert cache.is_duplicate(event_id) is False
        
        # All should now be duplicates
        for event_id in event_ids:
            assert cache.is_duplicate(event_id) is True

    def test_cache_eviction(self):
        """Test LRU eviction when cache is full."""
        cache = DeduplicationCache(max_size=3)
        
        # Add 3 events
        id1 = str(uuid.uuid4())
        id2 = str(uuid.uuid4())
        id3 = str(uuid.uuid4())
        
        cache.is_duplicate(id1)  # [id1]
        cache.is_duplicate(id2)  # [id1, id2]
        cache.is_duplicate(id3)  # [id1, id2, id3]
        
        # Add a 4th event - should evict id1 (oldest)
        id4 = str(uuid.uuid4())
        cache.is_duplicate(id4)  # [id2, id3, id4]
        
        # Cache size should still be 3
        assert cache.get_stats()["cache_size"] == 3
        
        # id2, id3, id4 should be duplicates (in cache)
        assert cache.is_duplicate(id2) is True
        assert cache.is_duplicate(id3) is True
        assert cache.is_duplicate(id4) is True
        
        # Add id5 - should evict id2 (oldest after id1)
        id5 = str(uuid.uuid4())
        cache.is_duplicate(id5)  # [id3, id4, id5]
        
        # Now id2 should be evicted, so it's "new" again
        assert cache.is_duplicate(id2) is False

    def test_lru_order_maintained(self):
        """Test that recently used items are kept."""
        cache = DeduplicationCache(max_size=3)
        
        id1 = str(uuid.uuid4())
        id2 = str(uuid.uuid4())
        id3 = str(uuid.uuid4())
        
        cache.is_duplicate(id1)  # [id1]
        cache.is_duplicate(id2)  # [id1, id2]
        cache.is_duplicate(id3)  # [id1, id2, id3]
        
        # Access id1 again - moves it to end
        cache.is_duplicate(id1)  # [id2, id3, id1]
        
        # Add new event - should evict id2 (oldest)
        id4 = str(uuid.uuid4())
        cache.is_duplicate(id4)  # [id3, id1, id4]
        
        # id2 should be evicted, not id1
        assert cache.is_duplicate(id2) is False  # Evicted
        assert cache.is_duplicate(id1) is True   # Still there

    def test_mark_processed(self):
        """Test explicitly marking an event as processed."""
        cache = DeduplicationCache(max_size=100)
        event_id = str(uuid.uuid4())
        
        # Mark as processed (without checking first)
        cache.mark_processed(event_id)
        
        # Should now be detected as duplicate
        assert cache.is_duplicate(event_id) is True

    def test_clear_cache(self):
        """Test clearing the cache."""
        cache = DeduplicationCache(max_size=100)
        
        # Add some events
        for _ in range(10):
            cache.is_duplicate(str(uuid.uuid4()))
        
        assert len(cache._cache) == 10
        
        # Clear
        cache.clear()
        
        assert len(cache._cache) == 0
        assert cache._hits == 0
        assert cache._misses == 0

    def test_metrics_tracking(self):
        """Test that hits and misses are tracked."""
        cache = DeduplicationCache(max_size=100)
        event_id = str(uuid.uuid4())
        
        # First call = miss
        cache.is_duplicate(event_id)
        assert cache._misses == 1
        assert cache._hits == 0
        
        # Second call = hit
        cache.is_duplicate(event_id)
        assert cache._misses == 1
        assert cache._hits == 1
        
        # Third call = hit
        cache.is_duplicate(event_id)
        assert cache._misses == 1
        assert cache._hits == 2

    def test_thread_safety(self):
        """Test cache is thread-safe."""
        cache = DeduplicationCache(max_size=1000)
        event_ids = [str(uuid.uuid4()) for _ in range(100)]
        
        def process_events(ids):
            results = []
            for id in ids:
                results.append(cache.is_duplicate(id))
            return results
        
        # Run from multiple threads
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(process_events, event_ids)
                for _ in range(4)
            ]
            results = [f.result() for f in futures]
        
        # First thread to process each ID should get False
        # Subsequent threads should get True
        total_new = sum(sum(1 for r in result if not r) for result in results)
        total_dup = sum(sum(1 for r in result if r) for result in results)
        
        # Each event should be new once and duplicate 3 times (4 threads)
        assert total_new == 100  # Each ID new once
        assert total_dup == 300  # Each ID duplicate 3 times

    def test_large_event_ids(self):
        """Test with very long event IDs."""
        cache = DeduplicationCache(max_size=100)
        long_id = "a" * 1000
        
        assert cache.is_duplicate(long_id) is False
        assert cache.is_duplicate(long_id) is True

    def test_special_characters_in_id(self):
        """Test event IDs with special characters."""
        cache = DeduplicationCache(max_size=100)
        special_ids = [
            "event-with-dashes",
            "event.with.dots",
            "event_with_underscores",
            "event/with/slashes",
            "event:with:colons",
            "event with spaces",
        ]
        
        for event_id in special_ids:
            assert cache.is_duplicate(event_id) is False
            assert cache.is_duplicate(event_id) is True


class TestDeduplicationPerformance:
    """Performance tests for DeduplicationCache."""

    @pytest.mark.slow
    def test_large_cache_performance(self):
        """Test performance with large cache."""
        import time
        
        cache = DeduplicationCache(max_size=100_000)
        event_ids = [str(uuid.uuid4()) for _ in range(10_000)]
        
        # Measure insert time
        start = time.time()
        for event_id in event_ids:
            cache.is_duplicate(event_id)
        insert_time = time.time() - start
        
        # Measure lookup time
        start = time.time()
        for event_id in event_ids:
            cache.is_duplicate(event_id)
        lookup_time = time.time() - start
        
        # Should be fast (< 1s for 10k operations)
        assert insert_time < 1.0, f"Insert too slow: {insert_time}s"
        assert lookup_time < 1.0, f"Lookup too slow: {lookup_time}s"
