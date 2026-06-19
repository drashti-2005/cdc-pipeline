"""
Performance Metrics Collection for CDC Pipeline

Provides real-time metrics collection and analysis:
- Latency histograms with percentiles
- Throughput tracking with windowed rates
- Resource monitoring (CPU, memory)
- Pipeline-specific metrics aggregation
"""

import time
import threading
import logging
import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Deque
from collections import deque
from datetime import datetime, timezone
from contextlib import contextmanager

logger = logging.getLogger(__name__)

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.debug("psutil not available, resource monitoring disabled")


@dataclass
class LatencyBucket:
    """A bucket in the latency histogram."""
    
    upper_bound_ms: float
    count: int = 0
    
    def record(self) -> None:
        """Increment bucket count."""
        self.count += 1


class LatencyHistogram:
    """
    Histogram for tracking latency distribution.
    
    Pre-defined buckets aligned with common SLA thresholds.
    
    Usage:
        histogram = LatencyHistogram("processing_time")
        histogram.observe(5.2)
        histogram.observe(12.8)
        print(histogram.summary())
    """
    
    # Default bucket boundaries in milliseconds
    DEFAULT_BUCKETS = [0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]
    
    def __init__(
        self,
        name: str,
        buckets: Optional[List[float]] = None,
        max_samples: int = 100000,
    ):
        """
        Initialize histogram.
        
        Args:
            name: Metric name
            buckets: Custom bucket boundaries (ms)
            max_samples: Max samples to keep for percentile calculation
        """
        self.name = name
        self.bucket_bounds = sorted(buckets or self.DEFAULT_BUCKETS)
        self.max_samples = max_samples
        
        # Buckets track count at each boundary
        self._buckets: List[LatencyBucket] = [
            LatencyBucket(upper_bound_ms=b) for b in self.bucket_bounds
        ]
        self._buckets.append(LatencyBucket(upper_bound_ms=float("inf")))
        
        # Keep samples for accurate percentiles
        self._samples: Deque[float] = deque(maxlen=max_samples)
        
        # Running statistics
        self._count: int = 0
        self._sum: float = 0
        self._min: float = float("inf")
        self._max: float = 0
        
        self._lock = threading.Lock()
    
    def observe(self, latency_ms: float) -> None:
        """
        Record a latency observation.
        
        Args:
            latency_ms: Latency in milliseconds
        """
        with self._lock:
            # Update running stats
            self._count += 1
            self._sum += latency_ms
            self._min = min(self._min, latency_ms)
            self._max = max(self._max, latency_ms)
            
            # Store sample
            self._samples.append(latency_ms)
            
            # Update buckets
            for bucket in self._buckets:
                if latency_ms <= bucket.upper_bound_ms:
                    bucket.record()
                    break
    
    @contextmanager
    def time(self):
        """
        Context manager to time and record a block.
        
        Usage:
            with histogram.time():
                process_event()
        """
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed = (time.perf_counter() - start) * 1000
            self.observe(elapsed)
    
    @property
    def count(self) -> int:
        """Total observations."""
        return self._count
    
    @property
    def sum_ms(self) -> float:
        """Sum of all observations."""
        return self._sum
    
    @property
    def mean_ms(self) -> float:
        """Mean latency."""
        return self._sum / self._count if self._count > 0 else 0
    
    @property
    def min_ms(self) -> float:
        """Minimum latency."""
        return self._min if self._count > 0 else 0
    
    @property
    def max_ms(self) -> float:
        """Maximum latency."""
        return self._max if self._count > 0 else 0
    
    def percentile(self, p: float) -> float:
        """
        Calculate percentile from samples.
        
        Args:
            p: Percentile (0-100)
            
        Returns:
            Latency at percentile in ms
        """
        with self._lock:
            if not self._samples:
                return 0
            sorted_samples = sorted(self._samples)
            idx = int(len(sorted_samples) * p / 100)
            idx = min(idx, len(sorted_samples) - 1)
            return sorted_samples[idx]
    
    @property
    def p50_ms(self) -> float:
        """50th percentile (median)."""
        return self.percentile(50)
    
    @property
    def p90_ms(self) -> float:
        """90th percentile."""
        return self.percentile(90)
    
    @property
    def p95_ms(self) -> float:
        """95th percentile."""
        return self.percentile(95)
    
    @property
    def p99_ms(self) -> float:
        """99th percentile."""
        return self.percentile(99)
    
    @property
    def p999_ms(self) -> float:
        """99.9th percentile."""
        return self.percentile(99.9)
    
    def bucket_counts(self) -> Dict[str, int]:
        """Get count per bucket."""
        result = {}
        for bucket in self._buckets:
            if bucket.upper_bound_ms == float("inf"):
                key = f">{self.bucket_bounds[-1]}"
            else:
                key = f"<={bucket.upper_bound_ms}"
            result[key] = bucket.count
        return result
    
    def summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        return {
            "name": self.name,
            "count": self._count,
            "sum_ms": self._sum,
            "mean_ms": self.mean_ms,
            "min_ms": self.min_ms,
            "max_ms": self.max_ms,
            "p50_ms": self.p50_ms,
            "p90_ms": self.p90_ms,
            "p95_ms": self.p95_ms,
            "p99_ms": self.p99_ms,
            "p999_ms": self.p999_ms,
        }
    
    def reset(self) -> None:
        """Reset all counters."""
        with self._lock:
            self._count = 0
            self._sum = 0
            self._min = float("inf")
            self._max = 0
            self._samples.clear()
            for bucket in self._buckets:
                bucket.count = 0


class ThroughputTracker:
    """
    Track throughput with windowed rate calculation.
    
    Maintains rolling windows for accurate rate measurement.
    
    Usage:
        tracker = ThroughputTracker("events_processed")
        
        for event in events:
            process(event)
            tracker.record()
        
        print(f"Rate: {tracker.rate_per_second} events/sec")
    """
    
    def __init__(
        self,
        name: str,
        window_size_sec: float = 60.0,
        bucket_count: int = 60,
    ):
        """
        Initialize throughput tracker.
        
        Args:
            name: Metric name
            window_size_sec: Total window size
            bucket_count: Number of time buckets
        """
        self.name = name
        self.window_size_sec = window_size_sec
        self.bucket_count = bucket_count
        self.bucket_duration_sec = window_size_sec / bucket_count
        
        self._buckets: Deque[tuple] = deque(maxlen=bucket_count)
        self._current_bucket_start: float = time.time()
        self._current_bucket_count: int = 0
        
        self._total_count: int = 0
        self._start_time: float = time.time()
        
        self._lock = threading.Lock()
    
    def record(self, count: int = 1) -> None:
        """
        Record events.
        
        Args:
            count: Number of events to record
        """
        now = time.time()
        
        with self._lock:
            self._total_count += count
            
            # Check if we need to rotate buckets
            bucket_age = now - self._current_bucket_start
            if bucket_age >= self.bucket_duration_sec:
                # Save current bucket
                self._buckets.append((
                    self._current_bucket_start,
                    self._current_bucket_count,
                ))
                
                # Start new bucket
                self._current_bucket_start = now
                self._current_bucket_count = count
            else:
                self._current_bucket_count += count
    
    @property
    def total_count(self) -> int:
        """Total events recorded."""
        return self._total_count
    
    @property
    def rate_per_second(self) -> float:
        """Current rate per second (from recent window)."""
        now = time.time()
        
        with self._lock:
            # Include current bucket
            total = self._current_bucket_count
            window_start = now - self.window_size_sec
            
            for bucket_start, count in self._buckets:
                if bucket_start >= window_start:
                    total += count
            
            elapsed = min(now - self._start_time, self.window_size_sec)
            return total / elapsed if elapsed > 0 else 0
    
    @property
    def average_rate_per_second(self) -> float:
        """Average rate since start."""
        elapsed = time.time() - self._start_time
        return self._total_count / elapsed if elapsed > 0 else 0
    
    @property
    def rate_per_minute(self) -> float:
        """Current rate per minute."""
        return self.rate_per_second * 60
    
    def summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        return {
            "name": self.name,
            "total_count": self._total_count,
            "current_rate_per_sec": self.rate_per_second,
            "average_rate_per_sec": self.average_rate_per_second,
            "window_size_sec": self.window_size_sec,
        }
    
    def reset(self) -> None:
        """Reset all counters."""
        with self._lock:
            self._buckets.clear()
            self._current_bucket_start = time.time()
            self._current_bucket_count = 0
            self._total_count = 0
            self._start_time = time.time()


@dataclass
class ResourceSnapshot:
    """Point-in-time resource utilization snapshot."""
    
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_mb: float
    thread_count: int
    open_files: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "cpu_percent": self.cpu_percent,
            "memory_percent": self.memory_percent,
            "memory_mb": self.memory_mb,
            "thread_count": self.thread_count,
            "open_files": self.open_files,
        }


class ResourceMonitor:
    """
    Monitor system resource utilization.
    
    Tracks CPU, memory, threads, and file handles.
    
    Usage:
        monitor = ResourceMonitor()
        monitor.start()
        
        # ... run workload ...
        
        monitor.stop()
        print(monitor.summary())
    """
    
    def __init__(self, interval_sec: float = 1.0, max_snapshots: int = 3600):
        """
        Initialize resource monitor.
        
        Args:
            interval_sec: Sampling interval
            max_snapshots: Maximum snapshots to retain
        """
        self.interval_sec = interval_sec
        self.max_snapshots = max_snapshots
        
        self._snapshots: Deque[ResourceSnapshot] = deque(maxlen=max_snapshots)
        self._running = False
        self._thread: Optional[threading.Thread] = None
        
        if PSUTIL_AVAILABLE:
            self._process = psutil.Process()
        else:
            self._process = None
    
    def _sample(self) -> ResourceSnapshot:
        """Take a resource snapshot."""
        if self._process:
            try:
                with self._process.oneshot():
                    cpu = self._process.cpu_percent()
                    memory_info = self._process.memory_info()
                    memory_mb = memory_info.rss / (1024 * 1024)
                    memory_percent = self._process.memory_percent()
                    threads = self._process.num_threads()
                    try:
                        open_files = len(self._process.open_files())
                    except (psutil.AccessDenied, psutil.NoSuchProcess):
                        open_files = 0
            except Exception as e:
                logger.warning(f"Error sampling resources: {e}")
                cpu = 0
                memory_mb = 0
                memory_percent = 0
                threads = threading.active_count()
                open_files = 0
        else:
            # Fallback without psutil
            cpu = 0
            memory_mb = 0
            memory_percent = 0
            threads = threading.active_count()
            open_files = 0
        
        return ResourceSnapshot(
            timestamp=datetime.now(timezone.utc),
            cpu_percent=cpu,
            memory_percent=memory_percent,
            memory_mb=memory_mb,
            thread_count=threads,
            open_files=open_files,
        )
    
    def _monitor_loop(self) -> None:
        """Background monitoring loop."""
        while self._running:
            snapshot = self._sample()
            self._snapshots.append(snapshot)
            time.sleep(self.interval_sec)
    
    def start(self) -> None:
        """Start background monitoring."""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        logger.debug("Resource monitoring started")
    
    def stop(self) -> None:
        """Stop background monitoring."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
        logger.debug("Resource monitoring stopped")
    
    def snapshot(self) -> ResourceSnapshot:
        """Take an immediate snapshot."""
        return self._sample()
    
    @property
    def snapshots(self) -> List[ResourceSnapshot]:
        """Get all snapshots."""
        return list(self._snapshots)
    
    def summary(self) -> Dict[str, Any]:
        """Get summary of resource utilization."""
        if not self._snapshots:
            return {"error": "No snapshots available"}
        
        cpu_values = [s.cpu_percent for s in self._snapshots]
        memory_values = [s.memory_mb for s in self._snapshots]
        
        return {
            "snapshot_count": len(self._snapshots),
            "cpu": {
                "mean_percent": statistics.mean(cpu_values),
                "max_percent": max(cpu_values),
                "min_percent": min(cpu_values),
            },
            "memory": {
                "mean_mb": statistics.mean(memory_values),
                "max_mb": max(memory_values),
                "min_mb": min(memory_values),
                "current_mb": memory_values[-1],
            },
            "threads": {
                "current": self._snapshots[-1].thread_count,
                "max": max(s.thread_count for s in self._snapshots),
            },
        }
    
    def reset(self) -> None:
        """Clear all snapshots."""
        self._snapshots.clear()


@dataclass
class PerformanceMetrics:
    """
    Aggregated performance metrics for CDC pipeline.
    
    Combines latency, throughput, and resource metrics.
    
    Usage:
        metrics = PerformanceMetrics()
        
        # Record various metrics
        with metrics.serialization_latency.time():
            serialize(event)
        
        metrics.events_processed.record()
        
        # Get report
        print(metrics.report())
    """
    
    # Latency histograms
    serialization_latency: LatencyHistogram = field(
        default_factory=lambda: LatencyHistogram("serialization_latency_ms")
    )
    deserialization_latency: LatencyHistogram = field(
        default_factory=lambda: LatencyHistogram("deserialization_latency_ms")
    )
    kafka_produce_latency: LatencyHistogram = field(
        default_factory=lambda: LatencyHistogram("kafka_produce_latency_ms")
    )
    kafka_consume_latency: LatencyHistogram = field(
        default_factory=lambda: LatencyHistogram("kafka_consume_latency_ms")
    )
    processing_latency: LatencyHistogram = field(
        default_factory=lambda: LatencyHistogram("processing_latency_ms")
    )
    end_to_end_latency: LatencyHistogram = field(
        default_factory=lambda: LatencyHistogram("end_to_end_latency_ms")
    )
    
    # Throughput trackers
    events_produced: ThroughputTracker = field(
        default_factory=lambda: ThroughputTracker("events_produced")
    )
    events_consumed: ThroughputTracker = field(
        default_factory=lambda: ThroughputTracker("events_consumed")
    )
    events_processed: ThroughputTracker = field(
        default_factory=lambda: ThroughputTracker("events_processed")
    )
    events_written: ThroughputTracker = field(
        default_factory=lambda: ThroughputTracker("events_written")
    )
    
    # Resource monitoring
    resource_monitor: ResourceMonitor = field(
        default_factory=lambda: ResourceMonitor()
    )
    
    # Timestamps
    start_time: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    
    def start_monitoring(self) -> None:
        """Start resource monitoring."""
        self.resource_monitor.start()
    
    def stop_monitoring(self) -> None:
        """Stop resource monitoring."""
        self.resource_monitor.stop()
    
    def report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report."""
        elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        
        return {
            "duration_seconds": elapsed,
            "latency": {
                "serialization": self.serialization_latency.summary(),
                "deserialization": self.deserialization_latency.summary(),
                "kafka_produce": self.kafka_produce_latency.summary(),
                "kafka_consume": self.kafka_consume_latency.summary(),
                "processing": self.processing_latency.summary(),
                "end_to_end": self.end_to_end_latency.summary(),
            },
            "throughput": {
                "produced": self.events_produced.summary(),
                "consumed": self.events_consumed.summary(),
                "processed": self.events_processed.summary(),
                "written": self.events_written.summary(),
            },
            "resources": self.resource_monitor.summary(),
        }
    
    def print_report(self) -> None:
        """Print formatted performance report."""
        report = self.report()
        
        print("\n" + "=" * 60)
        print("PERFORMANCE REPORT")
        print("=" * 60)
        print(f"Duration: {report['duration_seconds']:.2f} seconds\n")
        
        print("LATENCY (milliseconds)")
        print("-" * 40)
        for name, stats in report["latency"].items():
            if stats["count"] > 0:
                print(f"  {name}:")
                print(f"    Count: {stats['count']:,}")
                print(f"    Mean:  {stats['mean_ms']:.3f} ms")
                print(f"    P50:   {stats['p50_ms']:.3f} ms")
                print(f"    P95:   {stats['p95_ms']:.3f} ms")
                print(f"    P99:   {stats['p99_ms']:.3f} ms")
        
        print("\nTHROUGHPUT")
        print("-" * 40)
        for name, stats in report["throughput"].items():
            if stats["total_count"] > 0:
                print(f"  {name}:")
                print(f"    Total:        {stats['total_count']:,}")
                print(f"    Current Rate: {stats['current_rate_per_sec']:,.2f}/sec")
        
        if "error" not in report["resources"]:
            print("\nRESOURCES")
            print("-" * 40)
            print(f"  CPU:    {report['resources']['cpu']['mean_percent']:.1f}% avg")
            print(f"  Memory: {report['resources']['memory']['current_mb']:.1f} MB")
            print(f"  Threads: {report['resources']['threads']['current']}")
        
        print("=" * 60 + "\n")
    
    def reset(self) -> None:
        """Reset all metrics."""
        self.serialization_latency.reset()
        self.deserialization_latency.reset()
        self.kafka_produce_latency.reset()
        self.kafka_consume_latency.reset()
        self.processing_latency.reset()
        self.end_to_end_latency.reset()
        self.events_produced.reset()
        self.events_consumed.reset()
        self.events_processed.reset()
        self.events_written.reset()
        self.resource_monitor.reset()
        self.start_time = datetime.now(timezone.utc)
