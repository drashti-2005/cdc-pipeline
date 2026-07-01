"""
Unit Tests for Benchmark Module

Tests for Timer, Benchmark, BenchmarkResult, and BenchmarkSuite.
"""

import time
import pytest
from performance.benchmark import (
    Timer,
    Benchmark,
    BenchmarkResult,
    BenchmarkSuite,
    LatencyTracker,
    measure_time,
    timed,
)


class TestTimer:
    """Tests for Timer class."""
    
    def test_timer_measures_time(self):
        """Timer should measure elapsed time."""
        timer = Timer()
        timer.start()
        time.sleep(0.01)  # 10ms
        elapsed = timer.stop()
        
        assert elapsed > 5  # At least 5ms
        assert elapsed < 50  # Less than 50ms
    
    def test_timer_context_manager(self):
        """Timer should work as context manager."""
        with Timer() as t:
            time.sleep(0.01)
        
        assert t.elapsed_ms > 5
        assert t.elapsed_ms < 50
    
    def test_timer_elapsed_during_run(self):
        """Timer should report elapsed time while running."""
        timer = Timer()
        timer.start()
        time.sleep(0.01)
        
        # Check elapsed before stop
        elapsed = timer.elapsed_ms
        assert elapsed > 5
        
        timer.stop()
        assert timer.elapsed_ms >= elapsed
    
    def test_timer_elapsed_sec(self):
        """Timer should convert to seconds."""
        with Timer() as t:
            time.sleep(0.01)
        
        assert t.elapsed_sec > 0.005
        assert t.elapsed_sec < 0.05


class TestBenchmarkResult:
    """Tests for BenchmarkResult class."""
    
    def test_result_statistics(self):
        """Result should calculate correct statistics."""
        result = BenchmarkResult(
            name="test",
            iterations=5,
            total_time_ms=100,
            times_ms=[10, 20, 30, 20, 20],
        )
        
        assert result.mean_ms == 20
        assert result.median_ms == 20
        assert result.min_ms == 10
        assert result.max_ms == 30
    
    def test_result_percentiles(self):
        """Result should calculate percentiles."""
        # 100 values from 1 to 100
        times = list(range(1, 101))
        result = BenchmarkResult(
            name="test",
            iterations=100,
            total_time_ms=5050,
            times_ms=times,
        )
        
        # Index-based percentile: p50 of [1..100] is at index 50 = value 51
        assert result.p50_ms == 51
        assert result.p95_ms == 96
        assert result.p99_ms == 100
    
    def test_result_throughput(self):
        """Result should calculate throughput."""
        result = BenchmarkResult(
            name="test",
            iterations=1000,
            total_time_ms=1000,  # 1 second
            times_ms=[1] * 1000,
        )
        
        assert result.throughput_per_sec == 1000
    
    def test_result_summary(self):
        """Result should generate summary string."""
        result = BenchmarkResult(
            name="test_bench",
            iterations=10,
            total_time_ms=100,
            times_ms=[10] * 10,
        )
        
        summary = result.summary()
        assert "test_bench" in summary
        assert "Iterations: 10" in summary
        assert "ops/sec" in summary
    
    def test_result_to_dict(self):
        """Result should serialize to dict."""
        result = BenchmarkResult(
            name="test",
            iterations=10,
            total_time_ms=100,
            times_ms=[10] * 10,
        )
        
        d = result.to_dict()
        assert d["name"] == "test"
        assert d["iterations"] == 10
        assert "mean_ms" in d
        assert "p99_ms" in d


class TestBenchmark:
    """Tests for Benchmark class."""
    
    def test_benchmark_run(self):
        """Benchmark should run function and collect stats."""
        counter = {"count": 0}
        
        def work():
            counter["count"] += 1
            time.sleep(0.001)
        
        benchmark = Benchmark("test", warmup_iterations=2)
        result = benchmark.run(work, iterations=10)
        
        assert result.name == "test"
        assert result.iterations == 10
        assert counter["count"] == 12  # 2 warmup + 10 iterations
        assert len(result.times_ms) == 10
    
    def test_benchmark_with_args(self):
        """Benchmark should pass arguments to function."""
        results = []
        
        def work(x, y, z=None):
            results.append((x, y, z))
        
        benchmark = Benchmark("test", warmup_iterations=0)
        benchmark.run_with_args(work, args=(1, 2), kwargs={"z": 3}, iterations=5)
        
        assert len(results) == 5
        assert all(r == (1, 2, 3) for r in results)
    
    def test_benchmark_setup_teardown(self):
        """Benchmark should call setup and teardown."""
        state = {"setup": False, "teardown": False}
        
        def setup():
            state["setup"] = True
        
        def teardown():
            state["teardown"] = True
        
        benchmark = Benchmark("test", warmup_iterations=0, setup=setup, teardown=teardown)
        benchmark.run(lambda: None, iterations=1)
        
        assert state["setup"] is True
        assert state["teardown"] is True


class TestBenchmarkSuite:
    """Tests for BenchmarkSuite class."""
    
    def test_suite_run_all(self):
        """Suite should run all benchmarks."""
        suite = BenchmarkSuite("Test Suite")
        
        suite.add("fast", lambda: time.sleep(0.001), iterations=5, warmup=1)
        suite.add("slow", lambda: time.sleep(0.002), iterations=5, warmup=1)
        
        results = suite.run_all()
        
        assert len(results) == 2
        assert results[0].name == "fast"
        assert results[1].name == "slow"
    
    def test_suite_summary(self):
        """Suite should generate summary."""
        suite = BenchmarkSuite("Test Suite")
        suite.add("test1", lambda: None, iterations=10, warmup=0)
        suite.run_all()
        
        summary = suite.summary()
        assert "Test Suite" in summary
        assert "test1" in summary
        assert "ops/sec" in summary
    
    def test_suite_to_dict(self):
        """Suite should serialize to dict."""
        suite = BenchmarkSuite("Test Suite")
        suite.add("test1", lambda: None, iterations=5, warmup=0)
        suite.run_all()
        
        d = suite.to_dict()
        assert d["suite_name"] == "Test Suite"
        assert len(d["benchmarks"]) == 1


class TestLatencyTracker:
    """Tests for LatencyTracker class."""
    
    def test_tracker_records_latencies(self):
        """Tracker should record latency values."""
        tracker = LatencyTracker("test")
        
        tracker.record(10)
        tracker.record(20)
        tracker.record(30)
        
        assert tracker.count == 3
        assert tracker.mean == 20
    
    def test_tracker_percentiles(self):
        """Tracker should calculate percentiles."""
        tracker = LatencyTracker("test")
        
        for i in range(1, 101):
            tracker.record(i)
        
        # Index-based percentile calculation
        assert tracker.p50 == 51
        assert tracker.p95 == 96
        assert tracker.p99 == 100
    
    def test_tracker_measure_context(self):
        """Tracker should measure with context manager."""
        tracker = LatencyTracker("test")
        
        with tracker.measure():
            time.sleep(0.01)
        
        assert tracker.count == 1
        assert tracker.mean > 5  # At least 5ms
    
    def test_tracker_max_samples(self):
        """Tracker should limit sample storage."""
        tracker = LatencyTracker("test", max_samples=10)
        
        for i in range(100):
            tracker.record(i)
        
        assert tracker.count == 100  # Total count preserved
        assert len(tracker._latencies) == 10  # Only recent samples
    
    def test_tracker_stats(self):
        """Tracker should return stats dict."""
        tracker = LatencyTracker("test_latency")
        tracker.record(10)
        
        stats = tracker.stats()
        assert stats["name"] == "test_latency"
        assert stats["count"] == 1
        assert "mean_ms" in stats
    
    def test_tracker_reset(self):
        """Tracker should reset all data."""
        tracker = LatencyTracker("test")
        tracker.record(10)
        tracker.reset()
        
        assert tracker.count == 0
        assert tracker.mean == 0


class TestMeasureTime:
    """Tests for measure_time context manager."""
    
    def test_measure_time_yields_timer(self):
        """measure_time should yield working timer."""
        with measure_time("test") as timer:
            time.sleep(0.01)
        
        assert timer.elapsed_ms > 5


class TestTimedDecorator:
    """Tests for @timed decorator."""
    
    def test_timed_decorator(self):
        """Decorator should not affect function result."""
        @timed
        def add(a, b):
            return a + b
        
        result = add(2, 3)
        assert result == 5
