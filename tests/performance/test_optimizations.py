"""
Unit Tests for Optimization Utilities

Tests for batching, pooling, circuit breaker, rate limiting, and retry.
"""

import time
import threading
import pytest
from src.performance.optimizations import (
    BatchProcessor,
    ObjectPool,
    CircuitBreaker,
    CircuitState,
    CircuitOpenError,
    RateLimiter,
    RetryWithBackoff,
    BufferedWriter,
)


class TestBatchProcessor:
    """Tests for BatchProcessor class."""
    
    def test_batch_collects_items(self):
        """Processor should collect items into batches."""
        batches = []
        processor = BatchProcessor(
            batch_size=5,
            process_fn=lambda b: batches.append(list(b)),
        )
        
        for i in range(5):
            processor.add(i)
        
        assert len(batches) == 1
        assert batches[0] == [0, 1, 2, 3, 4]
    
    def test_batch_auto_flush_on_size(self):
        """Processor should auto-flush when batch size reached."""
        batches = []
        processor = BatchProcessor(
            batch_size=3,
            process_fn=lambda b: batches.append(len(b)),
        )
        
        for i in range(7):
            processor.add(i)
        
        # Should have 2 full batches, 1 remaining
        assert len(batches) == 2
        assert processor.buffer_size == 1
    
    def test_batch_manual_flush(self):
        """Processor should support manual flush."""
        batches = []
        processor = BatchProcessor(
            batch_size=100,
            process_fn=lambda b: batches.append(len(b)),
        )
        
        processor.add(1)
        processor.add(2)
        count = processor.flush()
        
        assert count == 2
        assert len(batches) == 1
    
    def test_batch_backpressure(self):
        """Processor should apply backpressure when buffer full."""
        processor = BatchProcessor(
            batch_size=100,
            max_buffer_size=5,
        )
        
        for i in range(5):
            assert processor.add(i) is True
        
        # Buffer full
        assert processor.add(6) is False
    
    def test_batch_stats(self):
        """Processor should track statistics."""
        batches = []
        processor = BatchProcessor(
            batch_size=5,
            process_fn=lambda b: batches.append(b),
        )
        
        for i in range(10):
            processor.add(i)
        
        stats = processor.stats
        assert stats["items_processed"] == 10
        assert stats["batches_processed"] == 2
        assert stats["avg_batch_size"] == 5


class TestObjectPool:
    """Tests for ObjectPool class."""
    
    def test_pool_creates_objects(self):
        """Pool should create objects using factory."""
        pool = ObjectPool(factory=lambda: {"value": 0})
        
        obj = pool.acquire()
        assert obj == {"value": 0}
    
    def test_pool_reuses_objects(self):
        """Pool should reuse released objects."""
        created = [0]
        
        def factory():
            created[0] += 1
            return {"id": created[0]}
        
        pool = ObjectPool(factory=factory, max_size=2)
        
        obj1 = pool.acquire()
        pool.release(obj1)
        obj2 = pool.acquire()
        
        # Should reuse, not create new
        assert obj1 is obj2
        assert created[0] == 1
    
    def test_pool_context_manager(self):
        """Pool should work as context manager."""
        pool = ObjectPool(factory=lambda: [])
        
        with pool.get() as obj:
            obj.append(1)
        
        # Object should be back in pool
        assert pool.available == 1
    
    def test_pool_reset_function(self):
        """Pool should reset objects before reuse."""
        pool = ObjectPool(
            factory=lambda: {"count": 0},
            reset_fn=lambda obj: obj.update({"count": 0}),
        )
        
        obj1 = pool.acquire()
        obj1["count"] = 5
        pool.release(obj1)
        
        obj2 = pool.acquire()
        assert obj2["count"] == 0
    
    def test_pool_stats(self):
        """Pool should track statistics."""
        pool = ObjectPool(factory=lambda: {}, max_size=5)
        
        obj = pool.acquire()
        pool.release(obj)
        pool.acquire()  # Reuse
        
        stats = pool.stats
        assert stats["created"] == 1
        assert stats["acquired"] == 2
        assert stats["hit_rate"] == 0.5


class TestCircuitBreaker:
    """Tests for CircuitBreaker class."""
    
    def test_circuit_closed_by_default(self):
        """Circuit should start closed."""
        breaker = CircuitBreaker()
        assert breaker.state == CircuitState.CLOSED
        assert breaker.is_closed
    
    def test_circuit_opens_after_failures(self):
        """Circuit should open after threshold failures."""
        breaker = CircuitBreaker(failure_threshold=3)
        
        @breaker
        def failing():
            raise ValueError("fail")
        
        for _ in range(3):
            with pytest.raises(ValueError):
                failing()
        
        assert breaker.state == CircuitState.OPEN
    
    def test_circuit_rejects_when_open(self):
        """Circuit should reject calls when open."""
        breaker = CircuitBreaker(failure_threshold=1)
        
        @breaker
        def failing():
            raise ValueError("fail")
        
        with pytest.raises(ValueError):
            failing()
        
        with pytest.raises(CircuitOpenError):
            failing()
    
    def test_circuit_half_open_after_timeout(self):
        """Circuit should enter half-open after recovery timeout."""
        breaker = CircuitBreaker(
            failure_threshold=1,
            recovery_timeout=0.1,
        )
        
        @breaker
        def failing():
            raise ValueError("fail")
        
        with pytest.raises(ValueError):
            failing()
        
        assert breaker.state == CircuitState.OPEN
        
        time.sleep(0.15)
        
        # Attempt should be allowed (half-open)
        with pytest.raises(ValueError):
            failing()
    
    def test_circuit_closes_on_success(self):
        """Circuit should close after successful calls in half-open."""
        breaker = CircuitBreaker(
            failure_threshold=1,
            recovery_timeout=0.05,
            success_threshold=2,
        )
        
        call_count = [0]
        
        @breaker
        def sometimes_fail():
            call_count[0] += 1
            if call_count[0] == 1:
                raise ValueError("fail")
            return "success"
        
        # First call fails, opens circuit
        with pytest.raises(ValueError):
            sometimes_fail()
        
        time.sleep(0.1)
        
        # Next calls succeed, close circuit
        sometimes_fail()
        sometimes_fail()
        
        assert breaker.state == CircuitState.CLOSED
    
    def test_circuit_execute_method(self):
        """Circuit should work with execute method."""
        breaker = CircuitBreaker()
        
        result = breaker.execute(lambda: "success")
        assert result == "success"
    
    def test_circuit_reset(self):
        """Circuit should reset to closed."""
        breaker = CircuitBreaker(failure_threshold=1)
        
        @breaker
        def failing():
            raise ValueError()
        
        with pytest.raises(ValueError):
            failing()
        
        assert breaker.state == CircuitState.OPEN
        
        breaker.reset()
        assert breaker.state == CircuitState.CLOSED


class TestRateLimiter:
    """Tests for RateLimiter class."""
    
    def test_limiter_allows_burst(self):
        """Limiter should allow burst capacity."""
        limiter = RateLimiter(rate=10, burst=5)
        
        # Should allow 5 immediately
        for _ in range(5):
            assert limiter.acquire(block=False) is True
        
        # 6th should fail (no blocking)
        assert limiter.acquire(block=False) is False
    
    def test_limiter_refills_over_time(self):
        """Limiter should refill tokens over time."""
        limiter = RateLimiter(rate=100, burst=1)
        
        limiter.acquire()
        time.sleep(0.02)  # Wait for refill
        
        assert limiter.acquire(block=False) is True
    
    def test_limiter_context_manager(self):
        """Limiter should work as context manager."""
        limiter = RateLimiter(rate=100, burst=5)
        
        with limiter.limit():
            pass  # Should not raise


class TestRetryWithBackoff:
    """Tests for RetryWithBackoff class."""
    
    def test_retry_succeeds_first_try(self):
        """Retry should return immediately on success."""
        retry = RetryWithBackoff(max_attempts=3)
        
        @retry
        def success():
            return "ok"
        
        assert success() == "ok"
    
    def test_retry_retries_on_failure(self):
        """Retry should retry on failure."""
        attempts = [0]
        
        retry = RetryWithBackoff(
            max_attempts=3,
            base_delay=0.01,
        )
        
        @retry
        def flaky():
            attempts[0] += 1
            if attempts[0] < 3:
                raise ValueError("fail")
            return "ok"
        
        assert flaky() == "ok"
        assert attempts[0] == 3
    
    def test_retry_gives_up_after_max_attempts(self):
        """Retry should give up after max attempts."""
        retry = RetryWithBackoff(
            max_attempts=2,
            base_delay=0.01,
        )
        
        @retry
        def always_fail():
            raise ValueError("fail")
        
        with pytest.raises(ValueError):
            always_fail()
    
    def test_retry_respects_exception_types(self):
        """Retry should only retry on specified exceptions."""
        retry = RetryWithBackoff(
            max_attempts=3,
            retryable_exceptions=(ValueError,),
        )
        
        @retry
        def type_error():
            raise TypeError("not retryable")
        
        with pytest.raises(TypeError):
            type_error()


class TestBufferedWriter:
    """Tests for BufferedWriter class."""
    
    def test_writer_buffers_data(self):
        """Writer should buffer data."""
        written = []
        writer = BufferedWriter(
            write_fn=lambda d: written.append(d),
            buffer_size=100,
        )
        
        writer.write(b"hello")
        assert len(written) == 0  # Still buffered
        
        writer.flush()
        assert len(written) == 1
        assert written[0] == b"hello"
    
    def test_writer_flushes_on_size(self):
        """Writer should flush when buffer size exceeded."""
        written = []
        writer = BufferedWriter(
            write_fn=lambda d: written.append(d),
            buffer_size=10,
        )
        
        writer.write(b"hello world!")  # 12 bytes > 10
        assert len(written) == 1
    
    def test_writer_flush_on_newline(self):
        """Writer should flush on newline when configured."""
        written = []
        writer = BufferedWriter(
            write_fn=lambda d: written.append(d),
            buffer_size=100,
            flush_on_newline=True,
        )
        
        writer.write(b"line\n")
        assert len(written) == 1
    
    def test_writer_context_manager(self):
        """Writer should flush on context exit."""
        written = []
        
        with BufferedWriter(
            write_fn=lambda d: written.append(d),
            buffer_size=100,
        ) as writer:
            writer.write(b"data")
        
        assert len(written) == 1
    
    def test_writer_stats(self):
        """Writer should track statistics."""
        written = []
        writer = BufferedWriter(
            write_fn=lambda d: written.append(d),
            buffer_size=5,
        )
        
        writer.write(b"12345678")  # All 8 bytes buffered, flushes once when > 5
        writer.flush()  # No-op if already flushed
        
        stats = writer.stats
        assert stats["bytes_written"] == 8
        assert stats["flush_count"] >= 1


class TestConcurrentBatchProcessor:
    """Tests for thread safety of BatchProcessor."""
    
    def test_concurrent_adds(self):
        """Processor should handle concurrent adds."""
        batches = []
        lock = threading.Lock()
        
        def process(batch):
            with lock:
                batches.append(len(batch))
        
        processor = BatchProcessor(
            batch_size=10,
            process_fn=process,
        )
        
        def add_items():
            for i in range(100):
                processor.add(i)
        
        threads = [threading.Thread(target=add_items) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        processor.flush()
        
        total = sum(batches)
        assert total == 1000


class TestConcurrentObjectPool:
    """Tests for thread safety of ObjectPool."""
    
    def test_concurrent_acquire_release(self):
        """Pool should handle concurrent access."""
        pool = ObjectPool(
            factory=lambda: {},
            max_size=5,
        )
        
        def use_object():
            for _ in range(100):
                obj = pool.acquire()
                time.sleep(0.0001)
                pool.release(obj)
        
        threads = [threading.Thread(target=use_object) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Pool should be consistent
        assert pool.stats["released"] >= pool.stats["acquired"] - pool.available
