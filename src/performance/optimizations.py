"""
Performance Optimization Utilities

Provides optimization patterns for CDC pipeline:
- Batch processing utilities
- Object pooling
- Async processing helpers
- Buffer management
- Circuit breaker pattern
"""

import time
import threading
import logging
from dataclasses import dataclass, field
from typing import TypeVar, Generic, Callable, List, Optional, Any, Deque
from collections import deque
from contextlib import contextmanager
from enum import Enum
import queue

logger = logging.getLogger(__name__)

T = TypeVar('T')


class BatchProcessor(Generic[T]):
    """
    Batch items for efficient bulk processing.
    
    Collects items until batch size or time threshold is reached,
    then processes them together.
    
    Usage:
        def process_batch(events):
            bulk_insert(events)
        
        processor = BatchProcessor(
            batch_size=100,
            flush_interval_sec=5.0,
            process_fn=process_batch,
        )
        
        for event in events:
            processor.add(event)
        
        processor.flush()  # Process remaining
    """
    
    def __init__(
        self,
        batch_size: int = 100,
        flush_interval_sec: float = 5.0,
        process_fn: Optional[Callable[[List[T]], None]] = None,
        max_buffer_size: int = 10000,
    ):
        """
        Initialize batch processor.
        
        Args:
            batch_size: Items per batch
            flush_interval_sec: Max time before flush
            process_fn: Function to process batches
            max_buffer_size: Max items to buffer (backpressure)
        """
        self.batch_size = batch_size
        self.flush_interval_sec = flush_interval_sec
        self.process_fn = process_fn
        self.max_buffer_size = max_buffer_size
        
        self._buffer: List[T] = []
        self._last_flush: float = time.time()
        self._lock = threading.Lock()
        
        self._items_processed: int = 0
        self._batches_processed: int = 0
    
    def add(self, item: T) -> bool:
        """
        Add item to batch.
        
        Args:
            item: Item to add
            
        Returns:
            True if item was added, False if buffer full
        """
        with self._lock:
            if len(self._buffer) >= self.max_buffer_size:
                logger.warning("Batch buffer full, applying backpressure")
                return False
            
            self._buffer.append(item)
            
            # Check if we should flush
            if len(self._buffer) >= self.batch_size:
                self._flush_locked()
            elif time.time() - self._last_flush >= self.flush_interval_sec:
                self._flush_locked()
            
            return True
    
    def add_many(self, items: List[T]) -> int:
        """
        Add multiple items.
        
        Returns:
            Number of items added
        """
        added = 0
        for item in items:
            if self.add(item):
                added += 1
            else:
                break
        return added
    
    def flush(self) -> int:
        """
        Force flush of current batch.
        
        Returns:
            Number of items processed
        """
        with self._lock:
            return self._flush_locked()
    
    def _flush_locked(self) -> int:
        """Flush while holding lock."""
        if not self._buffer:
            return 0
        
        batch = self._buffer
        self._buffer = []
        self._last_flush = time.time()
        
        if self.process_fn:
            try:
                self.process_fn(batch)
                self._batches_processed += 1
                self._items_processed += len(batch)
            except Exception as e:
                logger.error(f"Batch processing failed: {e}")
                # Put items back in buffer for retry
                self._buffer = batch + self._buffer
                raise
        
        return len(batch)
    
    @property
    def buffer_size(self) -> int:
        """Current buffer size."""
        return len(self._buffer)
    
    @property
    def stats(self) -> dict:
        """Get processing statistics."""
        return {
            "buffer_size": len(self._buffer),
            "items_processed": self._items_processed,
            "batches_processed": self._batches_processed,
            "avg_batch_size": (
                self._items_processed / self._batches_processed
                if self._batches_processed > 0 else 0
            ),
        }


class ObjectPool(Generic[T]):
    """
    Pool reusable objects to reduce allocation overhead.
    
    Usage:
        def create_buffer():
            return bytearray(4096)
        
        pool = ObjectPool(create_buffer, max_size=10)
        
        buffer = pool.acquire()
        try:
            # Use buffer
            buffer[:] = data
        finally:
            pool.release(buffer)
        
        # Or use context manager
        with pool.get() as buffer:
            buffer[:] = data
    """
    
    def __init__(
        self,
        factory: Callable[[], T],
        max_size: int = 10,
        reset_fn: Optional[Callable[[T], None]] = None,
    ):
        """
        Initialize object pool.
        
        Args:
            factory: Function to create new objects
            max_size: Maximum pool size
            reset_fn: Optional function to reset objects before reuse
        """
        self.factory = factory
        self.max_size = max_size
        self.reset_fn = reset_fn
        
        self._pool: Deque[T] = deque(maxlen=max_size)
        self._lock = threading.Lock()
        
        self._created: int = 0
        self._acquired: int = 0
        self._released: int = 0
    
    def acquire(self) -> T:
        """
        Acquire object from pool or create new one.
        
        Returns:
            Object from pool or newly created
        """
        with self._lock:
            self._acquired += 1
            
            if self._pool:
                obj = self._pool.pop()
                if self.reset_fn:
                    self.reset_fn(obj)
                return obj
            
            self._created += 1
            return self.factory()
    
    def release(self, obj: T) -> None:
        """
        Return object to pool.
        
        Args:
            obj: Object to return
        """
        with self._lock:
            self._released += 1
            
            if len(self._pool) < self.max_size:
                self._pool.append(obj)
    
    @contextmanager
    def get(self):
        """
        Context manager for pool object.
        
        Usage:
            with pool.get() as obj:
                use(obj)
        """
        obj = self.acquire()
        try:
            yield obj
        finally:
            self.release(obj)
    
    @property
    def available(self) -> int:
        """Number of available objects in pool."""
        return len(self._pool)
    
    @property
    def stats(self) -> dict:
        """Get pool statistics."""
        return {
            "available": len(self._pool),
            "max_size": self.max_size,
            "created": self._created,
            "acquired": self._acquired,
            "released": self._released,
            "hit_rate": (
                (self._acquired - self._created) / self._acquired
                if self._acquired > 0 else 0
            ),
        }


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject calls
    HALF_OPEN = "half_open"  # Testing recovery


class CircuitBreaker:
    """
    Circuit breaker for fault tolerance.
    
    Prevents cascading failures by stopping calls to failing services.
    
    Usage:
        breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=30,
        )
        
        @breaker
        def call_external_service():
            return requests.get(url)
        
        try:
            result = call_external_service()
        except CircuitOpenError:
            # Circuit is open, use fallback
            result = get_cached_value()
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        success_threshold: int = 2,
    ):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Failures before opening
            recovery_timeout: Seconds before trying again
            success_threshold: Successes needed to close
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        
        self._state = CircuitState.CLOSED
        self._failure_count: int = 0
        self._success_count: int = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.Lock()
    
    @property
    def state(self) -> CircuitState:
        """Current circuit state."""
        return self._state
    
    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed (accepting calls)."""
        return self._state == CircuitState.CLOSED
    
    def _should_attempt(self) -> bool:
        """Check if we should attempt the call."""
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True
            
            if self._state == CircuitState.OPEN:
                # Check if recovery timeout has passed
                if self._last_failure_time:
                    elapsed = time.time() - self._last_failure_time
                    if elapsed >= self.recovery_timeout:
                        self._state = CircuitState.HALF_OPEN
                        self._success_count = 0
                        logger.info("Circuit breaker entering half-open state")
                        return True
                return False
            
            # HALF_OPEN - allow attempt
            return True
    
    def _record_success(self) -> None:
        """Record successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    logger.info("Circuit breaker closed")
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0
    
    def _record_failure(self) -> None:
        """Record failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                logger.warning("Circuit breaker opened (half-open failure)")
            elif self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                logger.warning(f"Circuit breaker opened after {self._failure_count} failures")
    
    def __call__(self, func: Callable) -> Callable:
        """Decorator to wrap function with circuit breaker."""
        def wrapper(*args, **kwargs):
            if not self._should_attempt():
                raise CircuitOpenError("Circuit breaker is open")
            
            try:
                result = func(*args, **kwargs)
                self._record_success()
                return result
            except Exception as e:
                self._record_failure()
                raise
        
        return wrapper
    
    def execute(self, func: Callable[[], T]) -> T:
        """
        Execute function with circuit breaker protection.
        
        Args:
            func: Function to execute
            
        Returns:
            Function result
            
        Raises:
            CircuitOpenError: If circuit is open
        """
        if not self._should_attempt():
            raise CircuitOpenError("Circuit breaker is open")
        
        try:
            result = func()
            self._record_success()
            return result
        except Exception:
            self._record_failure()
            raise
    
    def reset(self) -> None:
        """Reset circuit breaker to closed state."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


@dataclass
class RateLimiter:
    """
    Rate limiter using token bucket algorithm.
    
    Controls the rate of operations to prevent overload.
    
    Usage:
        limiter = RateLimiter(rate=100, burst=10)
        
        for item in items:
            limiter.acquire()  # Blocks if rate exceeded
            process(item)
    """
    
    rate: float  # Tokens per second
    burst: int = 1  # Max tokens (bucket size)
    
    _tokens: float = field(default=0, init=False)
    _last_time: float = field(default_factory=time.time, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    
    def __post_init__(self):
        self._tokens = float(self.burst)
    
    def acquire(self, tokens: int = 1, block: bool = True) -> bool:
        """
        Acquire tokens from bucket.
        
        Args:
            tokens: Number of tokens needed
            block: Whether to block until available
            
        Returns:
            True if acquired, False if non-blocking and unavailable
        """
        with self._lock:
            while True:
                now = time.time()
                elapsed = now - self._last_time
                self._last_time = now
                
                # Add tokens based on elapsed time
                self._tokens = min(
                    self.burst,
                    self._tokens + elapsed * self.rate
                )
                
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
                
                if not block:
                    return False
                
                # Wait for tokens
                wait_time = (tokens - self._tokens) / self.rate
                time.sleep(min(wait_time, 0.1))
    
    @contextmanager
    def limit(self, tokens: int = 1):
        """
        Context manager for rate limiting.
        
        Usage:
            with limiter.limit():
                process()
        """
        self.acquire(tokens)
        yield


class RetryWithBackoff:
    """
    Retry failed operations with exponential backoff.
    
    Usage:
        retry = RetryWithBackoff(max_attempts=3, base_delay=1.0)
        
        @retry
        def flaky_operation():
            return call_service()
        
        result = flaky_operation()
    """
    
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: tuple = (Exception,),
    ):
        """
        Initialize retry handler.
        
        Args:
            max_attempts: Maximum retry attempts
            base_delay: Initial delay in seconds
            max_delay: Maximum delay between retries
            exponential_base: Base for exponential backoff
            jitter: Add randomness to delay
            retryable_exceptions: Exceptions to retry on
        """
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions
    
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for attempt."""
        import random
        
        delay = self.base_delay * (self.exponential_base ** attempt)
        delay = min(delay, self.max_delay)
        
        if self.jitter:
            delay = delay * (0.5 + random.random())
        
        return delay
    
    def __call__(self, func: Callable) -> Callable:
        """Decorator to add retry logic."""
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(self.max_attempts):
                try:
                    return func(*args, **kwargs)
                except self.retryable_exceptions as e:
                    last_exception = e
                    
                    if attempt < self.max_attempts - 1:
                        delay = self._calculate_delay(attempt)
                        logger.warning(
                            f"Attempt {attempt + 1} failed: {e}. "
                            f"Retrying in {delay:.2f}s"
                        )
                        time.sleep(delay)
            
            raise last_exception
        
        return wrapper
    
    def execute(self, func: Callable[[], T]) -> T:
        """
        Execute function with retry logic.
        
        Args:
            func: Function to execute
            
        Returns:
            Function result
        """
        wrapped = self(lambda: func())
        return wrapped()


class BufferedWriter:
    """
    Buffer writes for efficient I/O.
    
    Accumulates data and writes in larger chunks.
    
    Usage:
        with BufferedWriter(file_path, buffer_size=8192) as writer:
            for data in chunks:
                writer.write(data)
    """
    
    def __init__(
        self,
        write_fn: Callable[[bytes], None],
        buffer_size: int = 8192,
        flush_on_newline: bool = False,
    ):
        """
        Initialize buffered writer.
        
        Args:
            write_fn: Function to write data
            buffer_size: Buffer size in bytes
            flush_on_newline: Flush on newline characters
        """
        self.write_fn = write_fn
        self.buffer_size = buffer_size
        self.flush_on_newline = flush_on_newline
        
        self._buffer = bytearray()
        self._bytes_written: int = 0
        self._flush_count: int = 0
    
    def write(self, data: bytes) -> None:
        """
        Write data to buffer.
        
        Args:
            data: Data to write
        """
        self._buffer.extend(data)
        
        if len(self._buffer) >= self.buffer_size:
            self.flush()
        elif self.flush_on_newline and b'\n' in data:
            self.flush()
    
    def flush(self) -> None:
        """Flush buffer to underlying writer."""
        if self._buffer:
            self.write_fn(bytes(self._buffer))
            self._bytes_written += len(self._buffer)
            self._flush_count += 1
            self._buffer.clear()
    
    @property
    def stats(self) -> dict:
        """Get writer statistics."""
        return {
            "bytes_written": self._bytes_written,
            "flush_count": self._flush_count,
            "buffer_size": len(self._buffer),
            "avg_flush_size": (
                self._bytes_written / self._flush_count
                if self._flush_count > 0 else 0
            ),
        }
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.flush()
