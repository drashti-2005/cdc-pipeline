"""
Performance Test Configuration

Shared fixtures and utilities for performance tests.
"""

import pytest
import logging
from typing import Generator

from src.performance.benchmark import Benchmark, BenchmarkSuite, Timer
from src.performance.load_generator import LoadGenerator, LoadProfile
from src.performance.metrics_collector import (
    PerformanceMetrics,
    LatencyHistogram,
    ThroughputTracker,
    ResourceMonitor,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def benchmark() -> Generator[Benchmark, None, None]:
    """Provide a benchmark runner."""
    yield Benchmark("test_benchmark", warmup_iterations=5)


@pytest.fixture
def benchmark_suite() -> Generator[BenchmarkSuite, None, None]:
    """Provide a benchmark suite."""
    yield BenchmarkSuite("Test Suite")


@pytest.fixture
def load_generator() -> Generator[LoadGenerator, None, None]:
    """Provide a load generator."""
    gen = LoadGenerator(
        events_per_second=100,
        batch_size=10,
        tables=["customers", "products", "orders"],
    )
    yield gen
    gen.stop()


@pytest.fixture
def performance_metrics() -> Generator[PerformanceMetrics, None, None]:
    """Provide performance metrics collector."""
    metrics = PerformanceMetrics()
    yield metrics
    metrics.stop_monitoring()


@pytest.fixture
def latency_histogram() -> LatencyHistogram:
    """Provide a latency histogram."""
    return LatencyHistogram("test_latency")


@pytest.fixture
def throughput_tracker() -> ThroughputTracker:
    """Provide a throughput tracker."""
    return ThroughputTracker("test_throughput")


@pytest.fixture
def resource_monitor() -> Generator[ResourceMonitor, None, None]:
    """Provide a resource monitor."""
    monitor = ResourceMonitor(interval_sec=0.1)
    yield monitor
    monitor.stop()
