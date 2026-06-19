"""
Benchmarking Framework for CDC Pipeline

Provides utilities for measuring and analyzing pipeline performance:
- Timer context manager for precise timing
- Benchmark class for repeated measurements
- BenchmarkSuite for organizing multiple benchmarks
- Statistical analysis of results
"""

import time
import statistics
import logging
from dataclasses import dataclass, field
from typing import Callable, Any, Optional, List, Dict
from contextlib import contextmanager
from functools import wraps

logger = logging.getLogger(__name__)


@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""
    
    name: str
    iterations: int
    total_time_ms: float
    times_ms: List[float] = field(default_factory=list)
    
    @property
    def mean_ms(self) -> float:
        """Average time per iteration in milliseconds."""
        return statistics.mean(self.times_ms) if self.times_ms else 0
    
    @property
    def median_ms(self) -> float:
        """Median time per iteration in milliseconds."""
        return statistics.median(self.times_ms) if self.times_ms else 0
    
    @property
    def min_ms(self) -> float:
        """Minimum time per iteration in milliseconds."""
        return min(self.times_ms) if self.times_ms else 0
    
    @property
    def max_ms(self) -> float:
        """Maximum time per iteration in milliseconds."""
        return max(self.times_ms) if self.times_ms else 0
    
    @property
    def stdev_ms(self) -> float:
        """Standard deviation in milliseconds."""
        if len(self.times_ms) < 2:
            return 0
        return statistics.stdev(self.times_ms)
    
    @property
    def p50_ms(self) -> float:
        """50th percentile (same as median)."""
        return self.percentile(50)
    
    @property
    def p95_ms(self) -> float:
        """95th percentile latency."""
        return self.percentile(95)
    
    @property
    def p99_ms(self) -> float:
        """99th percentile latency."""
        return self.percentile(99)
    
    @property
    def throughput_per_sec(self) -> float:
        """Operations per second."""
        if self.total_time_ms == 0:
            return 0
        return (self.iterations / self.total_time_ms) * 1000
    
    def percentile(self, p: float) -> float:
        """Calculate percentile (0-100)."""
        if not self.times_ms:
            return 0
        sorted_times = sorted(self.times_ms)
        idx = int(len(sorted_times) * p / 100)
        idx = min(idx, len(sorted_times) - 1)
        return sorted_times[idx]
    
    def summary(self) -> str:
        """Human-readable summary of results."""
        return (
            f"Benchmark: {self.name}\n"
            f"  Iterations: {self.iterations}\n"
            f"  Total Time: {self.total_time_ms:.2f} ms\n"
            f"  Throughput: {self.throughput_per_sec:.2f} ops/sec\n"
            f"  Mean:   {self.mean_ms:.3f} ms\n"
            f"  Median: {self.median_ms:.3f} ms\n"
            f"  Min:    {self.min_ms:.3f} ms\n"
            f"  Max:    {self.max_ms:.3f} ms\n"
            f"  StdDev: {self.stdev_ms:.3f} ms\n"
            f"  P50:    {self.p50_ms:.3f} ms\n"
            f"  P95:    {self.p95_ms:.3f} ms\n"
            f"  P99:    {self.p99_ms:.3f} ms"
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "iterations": self.iterations,
            "total_time_ms": self.total_time_ms,
            "throughput_per_sec": self.throughput_per_sec,
            "mean_ms": self.mean_ms,
            "median_ms": self.median_ms,
            "min_ms": self.min_ms,
            "max_ms": self.max_ms,
            "stdev_ms": self.stdev_ms,
            "p50_ms": self.p50_ms,
            "p95_ms": self.p95_ms,
            "p99_ms": self.p99_ms,
        }


class Timer:
    """
    High-precision timer for measuring code execution time.
    
    Usage:
        timer = Timer()
        timer.start()
        # ... code to measure ...
        elapsed = timer.stop()
        
        # Or as context manager:
        with Timer() as t:
            # ... code to measure ...
        print(t.elapsed_ms)
    """
    
    def __init__(self):
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None
    
    def start(self) -> "Timer":
        """Start the timer."""
        self._start_time = time.perf_counter()
        self._end_time = None
        return self
    
    def stop(self) -> float:
        """Stop the timer and return elapsed time in milliseconds."""
        self._end_time = time.perf_counter()
        return self.elapsed_ms
    
    @property
    def elapsed_ms(self) -> float:
        """Elapsed time in milliseconds."""
        if self._start_time is None:
            return 0
        end = self._end_time if self._end_time else time.perf_counter()
        return (end - self._start_time) * 1000
    
    @property
    def elapsed_sec(self) -> float:
        """Elapsed time in seconds."""
        return self.elapsed_ms / 1000
    
    def __enter__(self) -> "Timer":
        self.start()
        return self
    
    def __exit__(self, *args) -> None:
        self.stop()


@contextmanager
def measure_time(name: str = "operation"):
    """
    Context manager for measuring execution time with logging.
    
    Usage:
        with measure_time("serialize_event"):
            serialize(event)
    """
    timer = Timer()
    timer.start()
    try:
        yield timer
    finally:
        elapsed = timer.stop()
        logger.debug(f"{name} completed in {elapsed:.3f} ms")


def timed(func: Callable) -> Callable:
    """
    Decorator to measure function execution time.
    
    Usage:
        @timed
        def process_event(event):
            ...
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        with Timer() as t:
            result = func(*args, **kwargs)
        logger.debug(f"{func.__name__} completed in {t.elapsed_ms:.3f} ms")
        return result
    return wrapper


class Benchmark:
    """
    Benchmark runner for measuring operation performance.
    
    Usage:
        benchmark = Benchmark("serialize_events")
        result = benchmark.run(serialize_function, iterations=1000)
        print(result.summary())
    """
    
    def __init__(
        self,
        name: str,
        warmup_iterations: int = 10,
        setup: Optional[Callable[[], Any]] = None,
        teardown: Optional[Callable[[], None]] = None,
    ):
        self.name = name
        self.warmup_iterations = warmup_iterations
        self.setup = setup
        self.teardown = teardown
    
    def run(
        self,
        func: Callable[[], Any],
        iterations: int = 100,
    ) -> BenchmarkResult:
        """
        Run the benchmark.
        
        Args:
            func: Function to benchmark (no arguments)
            iterations: Number of iterations to run
            
        Returns:
            BenchmarkResult with timing statistics
        """
        # Setup
        if self.setup:
            self.setup()
        
        # Warmup - let JIT/caches warm up
        logger.debug(f"Warming up {self.name} with {self.warmup_iterations} iterations")
        for _ in range(self.warmup_iterations):
            func()
        
        # Run benchmark
        logger.info(f"Running benchmark {self.name} with {iterations} iterations")
        times_ms: List[float] = []
        
        total_timer = Timer()
        total_timer.start()
        
        for _ in range(iterations):
            timer = Timer()
            timer.start()
            func()
            times_ms.append(timer.stop())
        
        total_time = total_timer.stop()
        
        # Teardown
        if self.teardown:
            self.teardown()
        
        result = BenchmarkResult(
            name=self.name,
            iterations=iterations,
            total_time_ms=total_time,
            times_ms=times_ms,
        )
        
        logger.info(f"Benchmark complete: {result.throughput_per_sec:.2f} ops/sec")
        return result
    
    def run_with_args(
        self,
        func: Callable[..., Any],
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        iterations: int = 100,
    ) -> BenchmarkResult:
        """
        Run benchmark with function arguments.
        
        Args:
            func: Function to benchmark
            args: Positional arguments
            kwargs: Keyword arguments
            iterations: Number of iterations
            
        Returns:
            BenchmarkResult with timing statistics
        """
        kwargs = kwargs or {}
        return self.run(lambda: func(*args, **kwargs), iterations)


class BenchmarkSuite:
    """
    Suite for organizing and running multiple benchmarks.
    
    Usage:
        suite = BenchmarkSuite("CDC Pipeline Benchmarks")
        suite.add("serialize", lambda: serializer.serialize(event))
        suite.add("deserialize", lambda: serializer.deserialize(data))
        results = suite.run_all()
        print(suite.summary())
    """
    
    def __init__(self, name: str):
        self.name = name
        self._benchmarks: List[tuple] = []  # (name, func, iterations)
        self._results: List[BenchmarkResult] = []
    
    def add(
        self,
        name: str,
        func: Callable[[], Any],
        iterations: int = 100,
        warmup: int = 10,
    ) -> "BenchmarkSuite":
        """Add a benchmark to the suite."""
        self._benchmarks.append((name, func, iterations, warmup))
        return self
    
    def run_all(self) -> List[BenchmarkResult]:
        """Run all benchmarks in the suite."""
        logger.info(f"Running benchmark suite: {self.name}")
        self._results = []
        
        for name, func, iterations, warmup in self._benchmarks:
            benchmark = Benchmark(name, warmup_iterations=warmup)
            result = benchmark.run(func, iterations)
            self._results.append(result)
        
        logger.info(f"Suite complete: {len(self._results)} benchmarks")
        return self._results
    
    @property
    def results(self) -> List[BenchmarkResult]:
        """Get benchmark results."""
        return self._results
    
    def summary(self) -> str:
        """Generate summary report of all benchmarks."""
        lines = [
            f"=== {self.name} ===",
            f"Benchmarks: {len(self._results)}",
            "",
        ]
        
        for result in self._results:
            lines.append(f"[{result.name}]")
            lines.append(f"  Throughput: {result.throughput_per_sec:,.2f} ops/sec")
            lines.append(f"  Mean: {result.mean_ms:.3f} ms | P95: {result.p95_ms:.3f} ms | P99: {result.p99_ms:.3f} ms")
            lines.append("")
        
        return "\n".join(lines)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert all results to dictionary."""
        return {
            "suite_name": self.name,
            "benchmarks": [r.to_dict() for r in self._results],
        }


class LatencyTracker:
    """
    Track latencies for ongoing operations with rolling statistics.
    
    Usage:
        tracker = LatencyTracker("event_processing")
        
        # Record latencies
        tracker.record(5.2)
        tracker.record(3.1)
        
        # Get stats
        print(tracker.stats())
    """
    
    def __init__(self, name: str, max_samples: int = 10000):
        self.name = name
        self.max_samples = max_samples
        self._latencies: List[float] = []
        self._total_count: int = 0
        self._total_sum: float = 0
    
    def record(self, latency_ms: float) -> None:
        """Record a latency measurement."""
        self._latencies.append(latency_ms)
        self._total_count += 1
        self._total_sum += latency_ms
        
        # Keep only recent samples for percentile calculations
        if len(self._latencies) > self.max_samples:
            self._latencies = self._latencies[-self.max_samples:]
    
    @contextmanager
    def measure(self):
        """Context manager to measure and record latency."""
        timer = Timer()
        timer.start()
        try:
            yield timer
        finally:
            self.record(timer.stop())
    
    @property
    def count(self) -> int:
        """Total number of measurements."""
        return self._total_count
    
    @property
    def mean(self) -> float:
        """Mean latency across all measurements."""
        return self._total_sum / self._total_count if self._total_count > 0 else 0
    
    @property
    def p50(self) -> float:
        """50th percentile of recent samples."""
        return self._percentile(50)
    
    @property
    def p95(self) -> float:
        """95th percentile of recent samples."""
        return self._percentile(95)
    
    @property
    def p99(self) -> float:
        """99th percentile of recent samples."""
        return self._percentile(99)
    
    def _percentile(self, p: float) -> float:
        """Calculate percentile of recent samples."""
        if not self._latencies:
            return 0
        sorted_latencies = sorted(self._latencies)
        idx = int(len(sorted_latencies) * p / 100)
        idx = min(idx, len(sorted_latencies) - 1)
        return sorted_latencies[idx]
    
    def stats(self) -> Dict[str, float]:
        """Get current statistics."""
        return {
            "name": self.name,
            "count": self._total_count,
            "mean_ms": self.mean,
            "p50_ms": self.p50,
            "p95_ms": self.p95,
            "p99_ms": self.p99,
        }
    
    def reset(self) -> None:
        """Reset all measurements."""
        self._latencies.clear()
        self._total_count = 0
        self._total_sum = 0
