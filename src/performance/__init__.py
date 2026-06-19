"""
Performance Testing and Benchmarking Module

This module provides utilities for:
- Benchmarking CDC pipeline throughput
- Load testing with configurable event rates
- Latency measurement and analysis
- Resource utilization monitoring
- Performance profiling
- Optimization patterns (batching, pooling, circuit breaker)
"""

from src.performance.benchmark import (
    Benchmark,
    BenchmarkResult,
    BenchmarkSuite,
    Timer,
    LatencyTracker,
    measure_time,
    timed,
)
from src.performance.load_generator import (
    LoadGenerator,
    LoadProfile,
    EventBatch,
    generate_customer_event,
    generate_order_event,
    generate_product_event,
)
from src.performance.metrics_collector import (
    PerformanceMetrics,
    LatencyHistogram,
    ThroughputTracker,
    ResourceMonitor,
)
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

__all__ = [
    # Benchmark
    "Benchmark",
    "BenchmarkResult",
    "BenchmarkSuite",
    "Timer",
    "LatencyTracker",
    "measure_time",
    "timed",
    # Load Generator
    "LoadGenerator",
    "LoadProfile",
    "EventBatch",
    "generate_customer_event",
    "generate_order_event",
    "generate_product_event",
    # Metrics
    "PerformanceMetrics",
    "LatencyHistogram",
    "ThroughputTracker",
    "ResourceMonitor",
    # Optimizations
    "BatchProcessor",
    "ObjectPool",
    "CircuitBreaker",
    "CircuitState",
    "CircuitOpenError",
    "RateLimiter",
    "RetryWithBackoff",
    "BufferedWriter",
]
