"""
Metrics Collection Module

Provides metric types and registry for monitoring:
- Counter: Cumulative values that only go up
- Gauge: Values that can go up and down
- Histogram: Distribution of values
- Timer: Duration measurements

SIMPLE EXPLANATION:
Metrics are like a dashboard in your car:
- Speedometer (Gauge): Current speed, goes up and down
- Odometer (Counter): Total miles, only goes up
- Trip history (Histogram): Distribution of trip distances
- Lap timer (Timer): How long each trip takes
"""

import logging
import threading
import time
import statistics
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Dict, List, Optional, Any, Callable, Tuple

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics."""
    
    COUNTER = auto()
    GAUGE = auto()
    HISTOGRAM = auto()
    TIMER = auto()


@dataclass
class MetricValue:
    """A metric value with timestamp."""
    
    value: float
    timestamp: datetime
    labels: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "labels": self.labels,
        }


class Metric(ABC):
    """
    Base metric class.
    
    All metrics have a name, optional labels, and description.
    """
    
    def __init__(
        self,
        name: str,
        description: str = "",
        labels: Optional[List[str]] = None,
    ):
        """
        Initialize metric.
        
        Args:
            name: Metric name (e.g., "requests_total")
            description: Human-readable description
            labels: Label names for this metric
        """
        self.name = name
        self.description = description
        self.label_names = labels or []
        self._lock = threading.RLock()  # RLock allows nested locking in same thread
    
    @abstractmethod
    def get_type(self) -> MetricType:
        """Get metric type."""
        pass
    
    @abstractmethod
    def get_value(self, labels: Optional[Dict[str, str]] = None) -> Any:
        """Get current metric value."""
        pass
    
    @abstractmethod
    def reset(self, labels: Optional[Dict[str, str]] = None) -> None:
        """Reset metric to initial state."""
        pass
    
    def _label_key(self, labels: Optional[Dict[str, str]] = None) -> str:
        """Create a key from labels for storage."""
        if not labels:
            return ""
        
        # Sort labels for consistent key
        sorted_items = sorted(labels.items())
        return "|".join(f"{k}={v}" for k, v in sorted_items)


class Counter(Metric):
    """
    Counter metric - cumulative value that only increases.
    
    Use for: request counts, errors, bytes processed, etc.
    
    USAGE:
        requests = Counter("requests_total", labels=["method", "status"])
        
        requests.inc()  # Increment by 1
        requests.inc(5)  # Increment by 5
        requests.inc(labels={"method": "GET", "status": "200"})
    """
    
    def __init__(
        self,
        name: str,
        description: str = "",
        labels: Optional[List[str]] = None,
    ):
        super().__init__(name, description, labels)
        self._values: Dict[str, float] = defaultdict(float)
    
    def get_type(self) -> MetricType:
        return MetricType.COUNTER
    
    def inc(
        self,
        amount: float = 1.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Increment counter.
        
        Args:
            amount: Amount to increment (must be positive)
            labels: Label values
        """
        if amount < 0:
            raise ValueError("Counter can only be incremented")
        
        key = self._label_key(labels)
        
        with self._lock:
            self._values[key] += amount
    
    def get_value(self, labels: Optional[Dict[str, str]] = None) -> float:
        """Get current counter value."""
        key = self._label_key(labels)
        
        with self._lock:
            return self._values.get(key, 0.0)
    
    def get_all_values(self) -> Dict[str, float]:
        """Get all counter values with labels."""
        with self._lock:
            return dict(self._values)
    
    def reset(self, labels: Optional[Dict[str, str]] = None) -> None:
        """Reset counter to zero."""
        key = self._label_key(labels)
        
        with self._lock:
            if key in self._values:
                self._values[key] = 0.0


class Gauge(Metric):
    """
    Gauge metric - value that can go up and down.
    
    Use for: temperature, queue size, active connections, etc.
    
    USAGE:
        queue_size = Gauge("queue_size", labels=["queue_name"])
        
        queue_size.set(100)
        queue_size.inc(5)   # Now 105
        queue_size.dec(10)  # Now 95
    """
    
    def __init__(
        self,
        name: str,
        description: str = "",
        labels: Optional[List[str]] = None,
    ):
        super().__init__(name, description, labels)
        self._values: Dict[str, float] = defaultdict(float)
    
    def get_type(self) -> MetricType:
        return MetricType.GAUGE
    
    def set(
        self,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Set gauge to a specific value."""
        key = self._label_key(labels)
        
        with self._lock:
            self._values[key] = value
    
    def inc(
        self,
        amount: float = 1.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Increment gauge."""
        key = self._label_key(labels)
        
        with self._lock:
            self._values[key] += amount
    
    def dec(
        self,
        amount: float = 1.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Decrement gauge."""
        key = self._label_key(labels)
        
        with self._lock:
            self._values[key] -= amount
    
    def get_value(self, labels: Optional[Dict[str, str]] = None) -> float:
        """Get current gauge value."""
        key = self._label_key(labels)
        
        with self._lock:
            return self._values.get(key, 0.0)
    
    def get_all_values(self) -> Dict[str, float]:
        """Get all gauge values with labels."""
        with self._lock:
            return dict(self._values)
    
    def reset(self, labels: Optional[Dict[str, str]] = None) -> None:
        """Reset gauge to zero."""
        key = self._label_key(labels)
        
        with self._lock:
            if key in self._values:
                self._values[key] = 0.0


@dataclass
class HistogramBuckets:
    """Histogram bucket boundaries and counts."""
    
    boundaries: List[float]
    counts: List[int]
    
    def get_bucket_index(self, value: float) -> int:
        """Get the bucket index for a value."""
        for i, boundary in enumerate(self.boundaries):
            if value <= boundary:
                return i
        return len(self.boundaries)  # +Inf bucket


class Histogram(Metric):
    """
    Histogram metric - distribution of values in buckets.
    
    Use for: request latencies, response sizes, etc.
    
    USAGE:
        latency = Histogram(
            "request_latency_seconds",
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
        )
        
        latency.observe(0.123)
        
        print(latency.get_percentile(0.95))  # p95 latency
    """
    
    # Default bucket boundaries for latencies
    DEFAULT_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    
    def __init__(
        self,
        name: str,
        description: str = "",
        labels: Optional[List[str]] = None,
        buckets: Optional[List[float]] = None,
    ):
        super().__init__(name, description, labels)
        self.buckets = sorted(buckets or self.DEFAULT_BUCKETS)
        
        # Per-label storage
        self._bucket_counts: Dict[str, List[int]] = {}
        self._sums: Dict[str, float] = defaultdict(float)
        self._counts: Dict[str, int] = defaultdict(int)
        self._values: Dict[str, List[float]] = {}  # For percentile calculation
    
    def get_type(self) -> MetricType:
        return MetricType.HISTOGRAM
    
    def _ensure_label_storage(self, key: str) -> None:
        """Ensure storage exists for label key."""
        if key not in self._bucket_counts:
            self._bucket_counts[key] = [0] * (len(self.buckets) + 1)
            self._values[key] = []
    
    def observe(
        self,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Record an observation.
        
        Args:
            value: Value to record
            labels: Label values
        """
        key = self._label_key(labels)
        
        with self._lock:
            self._ensure_label_storage(key)
            
            # Update bucket counts
            for i, boundary in enumerate(self.buckets):
                if value <= boundary:
                    self._bucket_counts[key][i] += 1
                    break
            else:
                # +Inf bucket
                self._bucket_counts[key][-1] += 1
            
            # Update sum and count
            self._sums[key] += value
            self._counts[key] += 1
            
            # Store value for percentile calculation
            self._values[key].append(value)
            
            # Limit stored values to prevent memory issues
            if len(self._values[key]) > 10000:
                self._values[key] = self._values[key][-5000:]
    
    def get_value(self, labels: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Get histogram statistics."""
        key = self._label_key(labels)
        
        with self._lock:
            if key not in self._counts or self._counts[key] == 0:
                return {"count": 0, "sum": 0.0, "avg": 0.0}
            
            count = self._counts[key]
            total = self._sums[key]
            
            return {
                "count": count,
                "sum": total,
                "avg": total / count if count > 0 else 0.0,
                "buckets": dict(zip(
                    [str(b) for b in self.buckets] + ["+Inf"],
                    self._bucket_counts.get(key, [])
                )),
            }
    
    def get_percentile(
        self,
        percentile: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> float:
        """
        Get percentile value.
        
        Args:
            percentile: Percentile (0.0 to 1.0)
            labels: Label values
            
        Returns:
            Percentile value
        """
        key = self._label_key(labels)
        
        with self._lock:
            values = self._values.get(key, [])
            if not values:
                return 0.0
            
            sorted_values = sorted(values)
            index = int(percentile * len(sorted_values))
            index = min(index, len(sorted_values) - 1)
            return sorted_values[index]
    
    def get_statistics(
        self,
        labels: Optional[Dict[str, str]] = None,
    ) -> Dict[str, float]:
        """Get statistical summary."""
        key = self._label_key(labels)
        
        with self._lock:
            values = self._values.get(key, [])
            if not values:
                return {
                    "count": 0,
                    "min": 0.0, "max": 0.0, "avg": 0.0,
                    "p50": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0,
                }
            
            return {
                "count": len(values),
                "min": min(values),
                "max": max(values),
                "avg": statistics.mean(values),
                "p50": self.get_percentile(0.50, labels),
                "p90": self.get_percentile(0.90, labels),
                "p95": self.get_percentile(0.95, labels),
                "p99": self.get_percentile(0.99, labels),
            }
    
    def reset(self, labels: Optional[Dict[str, str]] = None) -> None:
        """Reset histogram."""
        key = self._label_key(labels)
        
        with self._lock:
            if key in self._bucket_counts:
                self._bucket_counts[key] = [0] * (len(self.buckets) + 1)
            self._sums[key] = 0.0
            self._counts[key] = 0
            if key in self._values:
                self._values[key] = []


class Timer:
    """
    Timer context manager for measuring durations.
    
    Records durations in a Histogram metric.
    
    USAGE:
        timer = Timer(histogram)
        
        with timer:
            do_work()
        
        # Or manually:
        timer.start()
        do_work()
        timer.stop()
    """
    
    def __init__(
        self,
        histogram: Histogram,
        labels: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize timer.
        
        Args:
            histogram: Histogram to record durations
            labels: Label values
        """
        self.histogram = histogram
        self.labels = labels
        self._start_time: Optional[float] = None
    
    def start(self) -> "Timer":
        """Start the timer."""
        self._start_time = time.perf_counter()
        return self
    
    def stop(self) -> float:
        """Stop the timer and record duration."""
        if self._start_time is None:
            raise RuntimeError("Timer not started")
        
        duration = time.perf_counter() - self._start_time
        self.histogram.observe(duration, self.labels)
        self._start_time = None
        
        return duration
    
    def __enter__(self) -> "Timer":
        """Context manager entry."""
        return self.start()
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.stop()


class MetricsRegistry:
    """
    Registry for all metrics.
    
    Central location to register and retrieve metrics.
    
    USAGE:
        registry = MetricsRegistry()
        
        # Register metrics
        requests = registry.counter("requests_total")
        errors = registry.counter("errors_total")
        latency = registry.histogram("request_latency")
        
        # Use metrics
        requests.inc()
        
        # Get all metrics
        all_metrics = registry.collect()
    """
    
    def __init__(self, prefix: str = ""):
        """
        Initialize registry.
        
        Args:
            prefix: Prefix for all metric names
        """
        self.prefix = prefix
        self._metrics: Dict[str, Metric] = {}
        self._lock = threading.RLock()  # RLock for consistency
    
    def _full_name(self, name: str) -> str:
        """Get full metric name with prefix."""
        if self.prefix:
            return f"{self.prefix}_{name}"
        return name
    
    def counter(
        self,
        name: str,
        description: str = "",
        labels: Optional[List[str]] = None,
    ) -> Counter:
        """
        Get or create a Counter metric.
        
        Args:
            name: Metric name
            description: Description
            labels: Label names
            
        Returns:
            Counter metric
        """
        full_name = self._full_name(name)
        
        with self._lock:
            if full_name not in self._metrics:
                self._metrics[full_name] = Counter(full_name, description, labels)
            return self._metrics[full_name]
    
    def gauge(
        self,
        name: str,
        description: str = "",
        labels: Optional[List[str]] = None,
    ) -> Gauge:
        """
        Get or create a Gauge metric.
        
        Args:
            name: Metric name
            description: Description
            labels: Label names
            
        Returns:
            Gauge metric
        """
        full_name = self._full_name(name)
        
        with self._lock:
            if full_name not in self._metrics:
                self._metrics[full_name] = Gauge(full_name, description, labels)
            return self._metrics[full_name]
    
    def histogram(
        self,
        name: str,
        description: str = "",
        labels: Optional[List[str]] = None,
        buckets: Optional[List[float]] = None,
    ) -> Histogram:
        """
        Get or create a Histogram metric.
        
        Args:
            name: Metric name
            description: Description
            labels: Label names
            buckets: Bucket boundaries
            
        Returns:
            Histogram metric
        """
        full_name = self._full_name(name)
        
        with self._lock:
            if full_name not in self._metrics:
                self._metrics[full_name] = Histogram(
                    full_name, description, labels, buckets
                )
            return self._metrics[full_name]
    
    def timer(
        self,
        name: str,
        description: str = "",
        labels: Optional[List[str]] = None,
        buckets: Optional[List[float]] = None,
    ) -> Tuple[Histogram, Callable]:
        """
        Get or create a Timer (backed by Histogram).
        
        Returns histogram and a factory function for creating timers.
        
        Args:
            name: Metric name
            description: Description
            labels: Label names
            buckets: Bucket boundaries
            
        Returns:
            Tuple of (Histogram, timer_factory)
        """
        histogram = self.histogram(name, description, labels, buckets)
        
        def create_timer(labels: Optional[Dict[str, str]] = None) -> Timer:
            return Timer(histogram, labels)
        
        return histogram, create_timer
    
    def get(self, name: str) -> Optional[Metric]:
        """Get a metric by name."""
        full_name = self._full_name(name)
        
        with self._lock:
            return self._metrics.get(full_name)
    
    def unregister(self, name: str) -> bool:
        """Unregister a metric."""
        full_name = self._full_name(name)
        
        with self._lock:
            if full_name in self._metrics:
                del self._metrics[full_name]
                return True
            return False
    
    def collect(self) -> Dict[str, Dict[str, Any]]:
        """
        Collect all metric values.
        
        Returns:
            Dictionary of metric name → values
        """
        result = {}
        
        with self._lock:
            for name, metric in self._metrics.items():
                metric_data = {
                    "type": metric.get_type().name,
                    "description": metric.description,
                }
                
                if isinstance(metric, Counter):
                    metric_data["values"] = metric.get_all_values()
                elif isinstance(metric, Gauge):
                    metric_data["values"] = metric.get_all_values()
                elif isinstance(metric, Histogram):
                    metric_data["statistics"] = metric.get_statistics()
                    metric_data["value"] = metric.get_value()
                
                result[name] = metric_data
        
        return result
    
    def to_prometheus(self) -> str:
        """
        Export metrics in Prometheus format.
        
        Returns:
            Prometheus text format metrics
        """
        lines = []
        
        with self._lock:
            for name, metric in self._metrics.items():
                # Add HELP and TYPE
                if metric.description:
                    lines.append(f"# HELP {name} {metric.description}")
                lines.append(f"# TYPE {name} {metric.get_type().name.lower()}")
                
                if isinstance(metric, Counter):
                    for label_key, value in metric.get_all_values().items():
                        if label_key:
                            labels = "{" + label_key.replace("|", ",") + "}"
                            lines.append(f"{name}{labels} {value}")
                        else:
                            lines.append(f"{name} {value}")
                
                elif isinstance(metric, Gauge):
                    for label_key, value in metric.get_all_values().items():
                        if label_key:
                            labels = "{" + label_key.replace("|", ",") + "}"
                            lines.append(f"{name}{labels} {value}")
                        else:
                            lines.append(f"{name} {value}")
                
                elif isinstance(metric, Histogram):
                    stats = metric.get_value()
                    lines.append(f"{name}_count {stats.get('count', 0)}")
                    lines.append(f"{name}_sum {stats.get('sum', 0)}")
                    
                    for bucket, count in stats.get("buckets", {}).items():
                        lines.append(f'{name}_bucket{{le="{bucket}"}} {count}')
                
                lines.append("")
        
        return "\n".join(lines)
    
    def reset_all(self) -> None:
        """Reset all metrics."""
        with self._lock:
            for metric in self._metrics.values():
                metric.reset()


# Global registry
_global_registry: Optional[MetricsRegistry] = None


def configure_metrics(registry: Optional[MetricsRegistry] = None, prefix: str = "") -> MetricsRegistry:
    """
    Configure the global metrics registry.
    
    Args:
        registry: Registry to use (creates new if None)
        prefix: Prefix for metric names
        
    Returns:
        The configured registry
    """
    global _global_registry
    
    if registry:
        _global_registry = registry
    else:
        _global_registry = MetricsRegistry(prefix)
    
    return _global_registry


def get_metrics_registry() -> MetricsRegistry:
    """Get the global metrics registry."""
    global _global_registry
    
    if _global_registry is None:
        _global_registry = MetricsRegistry()
    
    return _global_registry


# Pipeline-specific metrics
def create_pipeline_metrics(registry: Optional[MetricsRegistry] = None) -> Dict[str, Metric]:
    """
    Create standard CDC pipeline metrics.
    
    Returns:
        Dictionary of metric name → metric
    """
    registry = registry or get_metrics_registry()
    
    return {
        # Event metrics
        "events_processed": registry.counter(
            "cdc_events_processed_total",
            "Total number of CDC events processed",
            labels=["table", "operation"]
        ),
        "events_failed": registry.counter(
            "cdc_events_failed_total",
            "Total number of failed CDC events",
            labels=["table", "error_type"]
        ),
        
        # Latency metrics
        "event_latency": registry.histogram(
            "cdc_event_latency_seconds",
            "CDC event processing latency",
            labels=["table"],
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
        ),
        "replication_lag": registry.histogram(
            "cdc_replication_lag_seconds",
            "Time between event creation and processing",
            labels=["source"],
            buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]
        ),
        
        # Queue metrics
        "queue_size": registry.gauge(
            "cdc_queue_size",
            "Current number of events in queue",
            labels=["queue_name"]
        ),
        "kafka_lag": registry.gauge(
            "cdc_kafka_consumer_lag",
            "Kafka consumer lag",
            labels=["topic", "partition"]
        ),
        
        # Connection metrics
        "active_connections": registry.gauge(
            "cdc_active_connections",
            "Number of active connections",
            labels=["type"]
        ),
        "connection_errors": registry.counter(
            "cdc_connection_errors_total",
            "Total connection errors",
            labels=["type", "error"]
        ),
        
        # Throughput metrics
        "bytes_processed": registry.counter(
            "cdc_bytes_processed_total",
            "Total bytes processed",
            labels=["direction"]
        ),
    }
