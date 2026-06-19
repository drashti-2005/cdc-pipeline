"""
Unit Tests for Metrics Collector Module

Tests for latency histograms, throughput tracking, and resource monitoring.
"""

import time
import pytest
from src.performance.metrics_collector import (
    LatencyHistogram,
    ThroughputTracker,
    ResourceMonitor,
    PerformanceMetrics,
    ResourceSnapshot,
)


class TestLatencyHistogram:
    """Tests for LatencyHistogram class."""
    
    def test_histogram_observe(self):
        """Histogram should record observations."""
        hist = LatencyHistogram("test")
        
        hist.observe(10)
        hist.observe(20)
        hist.observe(30)
        
        assert hist.count == 3
        assert hist.sum_ms == 60
        assert hist.mean_ms == 20
    
    def test_histogram_min_max(self):
        """Histogram should track min/max."""
        hist = LatencyHistogram("test")
        
        hist.observe(5)
        hist.observe(15)
        hist.observe(10)
        
        assert hist.min_ms == 5
        assert hist.max_ms == 15
    
    def test_histogram_percentiles(self):
        """Histogram should calculate percentiles."""
        hist = LatencyHistogram("test")
        
        for i in range(1, 101):
            hist.observe(i)
        
        # Index-based percentile
        assert hist.p50_ms == 51
        assert hist.p90_ms == 91
        assert hist.p95_ms == 96
        assert hist.p99_ms == 100
    
    def test_histogram_time_context(self):
        """Histogram should time context blocks."""
        hist = LatencyHistogram("test")
        
        with hist.time():
            time.sleep(0.01)
        
        assert hist.count == 1
        assert hist.mean_ms > 5
    
    def test_histogram_buckets(self):
        """Histogram should populate buckets."""
        hist = LatencyHistogram("test", buckets=[10, 50, 100])
        
        hist.observe(5)   # <=10
        hist.observe(30)  # <=50
        hist.observe(150) # >100
        
        buckets = hist.bucket_counts()
        assert buckets["<=10"] == 1
        assert buckets["<=50"] == 1
        assert buckets[">100"] == 1
    
    def test_histogram_summary(self):
        """Histogram should return summary dict."""
        hist = LatencyHistogram("test_latency")
        hist.observe(10)
        
        summary = hist.summary()
        assert summary["name"] == "test_latency"
        assert summary["count"] == 1
        assert summary["mean_ms"] == 10
        assert "p99_ms" in summary
    
    def test_histogram_reset(self):
        """Histogram should reset all data."""
        hist = LatencyHistogram("test")
        hist.observe(10)
        hist.reset()
        
        assert hist.count == 0
        assert hist.sum_ms == 0
        assert hist.min_ms == 0


class TestThroughputTracker:
    """Tests for ThroughputTracker class."""
    
    def test_tracker_record(self):
        """Tracker should record events."""
        tracker = ThroughputTracker("test")
        
        tracker.record()
        tracker.record(5)
        
        assert tracker.total_count == 6
    
    def test_tracker_rate(self):
        """Tracker should calculate rate."""
        tracker = ThroughputTracker("test", window_size_sec=1.0)
        
        # Record 100 events
        tracker.record(100)
        
        # Rate should be high initially
        assert tracker.rate_per_second > 0
    
    def test_tracker_rate_per_minute(self):
        """Tracker should calculate per-minute rate."""
        tracker = ThroughputTracker("test")
        tracker.record(60)
        
        # Per-minute should be approximately 60x per-second
        # Using approximate check due to timing variations
        assert tracker.rate_per_minute > 0
        assert abs(tracker.rate_per_minute - tracker.rate_per_second * 60) < tracker.rate_per_minute * 0.5
    
    def test_tracker_summary(self):
        """Tracker should return summary dict."""
        tracker = ThroughputTracker("test_throughput")
        tracker.record(10)
        
        summary = tracker.summary()
        assert summary["name"] == "test_throughput"
        assert summary["total_count"] == 10
        assert "current_rate_per_sec" in summary
    
    def test_tracker_reset(self):
        """Tracker should reset all data."""
        tracker = ThroughputTracker("test")
        tracker.record(100)
        tracker.reset()
        
        assert tracker.total_count == 0


class TestResourceMonitor:
    """Tests for ResourceMonitor class."""
    
    def test_monitor_snapshot(self):
        """Monitor should take snapshots."""
        monitor = ResourceMonitor()
        
        snapshot = monitor.snapshot()
        
        assert isinstance(snapshot, ResourceSnapshot)
        assert snapshot.timestamp is not None
        assert snapshot.thread_count >= 1
    
    def test_monitor_start_stop(self):
        """Monitor should start and stop cleanly."""
        monitor = ResourceMonitor(interval_sec=0.05)
        
        monitor.start()
        time.sleep(0.2)
        monitor.stop()
        
        # Should have collected some snapshots
        assert len(monitor.snapshots) > 0
    
    def test_monitor_summary(self):
        """Monitor should generate summary."""
        monitor = ResourceMonitor(interval_sec=0.02)
        
        monitor.start()
        time.sleep(0.1)
        monitor.stop()
        
        summary = monitor.summary()
        assert "snapshot_count" in summary
        assert "cpu" in summary
        assert "memory" in summary
    
    def test_monitor_reset(self):
        """Monitor should reset snapshots."""
        monitor = ResourceMonitor()
        monitor._snapshots.append(monitor.snapshot())
        
        monitor.reset()
        
        assert len(monitor.snapshots) == 0
    
    def test_snapshot_to_dict(self):
        """Snapshot should serialize to dict."""
        monitor = ResourceMonitor()
        snapshot = monitor.snapshot()
        
        d = snapshot.to_dict()
        assert "timestamp" in d
        assert "cpu_percent" in d
        assert "memory_mb" in d


class TestPerformanceMetrics:
    """Tests for PerformanceMetrics class."""
    
    def test_metrics_initialization(self):
        """Metrics should initialize with all components."""
        metrics = PerformanceMetrics()
        
        assert metrics.serialization_latency is not None
        assert metrics.events_produced is not None
        assert metrics.resource_monitor is not None
    
    def test_metrics_record_latency(self):
        """Metrics should record latencies."""
        metrics = PerformanceMetrics()
        
        with metrics.serialization_latency.time():
            time.sleep(0.01)
        
        assert metrics.serialization_latency.count == 1
    
    def test_metrics_record_throughput(self):
        """Metrics should record throughput."""
        metrics = PerformanceMetrics()
        
        metrics.events_processed.record(100)
        
        assert metrics.events_processed.total_count == 100
    
    def test_metrics_report(self):
        """Metrics should generate report."""
        metrics = PerformanceMetrics()
        metrics.events_processed.record(100)
        
        report = metrics.report()
        
        assert "duration_seconds" in report
        assert "latency" in report
        assert "throughput" in report
        assert "resources" in report
    
    def test_metrics_reset(self):
        """Metrics should reset all data."""
        metrics = PerformanceMetrics()
        metrics.events_processed.record(100)
        metrics.reset()
        
        assert metrics.events_processed.total_count == 0
    
    def test_metrics_monitoring_lifecycle(self):
        """Metrics should manage monitoring lifecycle."""
        metrics = PerformanceMetrics()
        
        metrics.start_monitoring()
        time.sleep(0.1)
        metrics.stop_monitoring()
        
        # Should not raise


class TestLatencyHistogramThreadSafety:
    """Tests for thread safety of LatencyHistogram."""
    
    def test_concurrent_observe(self):
        """Histogram should handle concurrent observations."""
        import threading
        
        hist = LatencyHistogram("test")
        threads = []
        
        def observe_many():
            for _ in range(100):
                hist.observe(1)
        
        for _ in range(10):
            t = threading.Thread(target=observe_many)
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        assert hist.count == 1000


class TestThroughputTrackerThreadSafety:
    """Tests for thread safety of ThroughputTracker."""
    
    def test_concurrent_record(self):
        """Tracker should handle concurrent records."""
        import threading
        
        tracker = ThroughputTracker("test")
        threads = []
        
        def record_many():
            for _ in range(100):
                tracker.record()
        
        for _ in range(10):
            t = threading.Thread(target=record_many)
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        assert tracker.total_count == 1000
