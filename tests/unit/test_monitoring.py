"""
Monitoring Module Tests

Tests for metrics, alerting, health checks, and dashboards.
"""

import json
import socket
import tempfile
import threading
import time
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from monitoring.metrics import (
    MetricType,
    Counter,
    Gauge,
    Histogram,
    Timer,
    MetricsRegistry,
    configure_metrics,
    get_metrics_registry,
    create_pipeline_metrics,
)
from monitoring.alerting import (
    AlertLevel,
    AlertState,
    Alert,
    AlertRule,
    AlertManager,
    LogAlertChannel,
    WebhookAlertChannel,
    create_pipeline_alert_rules,
)
from monitoring.health import (
    HealthStatus,
    HealthResult,
    HealthCheck,
    TCPHealthCheck,
    HTTPHealthCheck,
    CallableHealthCheck,
    HealthChecker,
    create_tcp_check,
    create_http_check,
)
from monitoring.dashboard import (
    PanelType,
    TimeRange,
    Threshold,
    DashboardPanel,
    DashboardRow,
    Dashboard,
    DashboardManager,
    create_pipeline_dashboard,
    create_simple_text_dashboard,
)


# =============================================================================
# Metrics Tests
# =============================================================================

class TestCounter:
    """Test Counter metric."""
    
    def test_create_counter(self):
        """Test creating a counter."""
        counter = Counter("requests_total", "Total requests")
        
        assert counter.name == "requests_total"
        assert counter.get_type() == MetricType.COUNTER
    
    def test_increment(self):
        """Test counter increment."""
        counter = Counter("requests_total")
        
        counter.inc()
        assert counter.get_value() == 1.0
        
        counter.inc(5)
        assert counter.get_value() == 6.0
    
    def test_increment_with_labels(self):
        """Test counter with labels."""
        counter = Counter("requests_total", labels=["method", "status"])
        
        counter.inc(labels={"method": "GET", "status": "200"})
        counter.inc(labels={"method": "POST", "status": "201"})
        counter.inc(2, labels={"method": "GET", "status": "200"})
        
        assert counter.get_value({"method": "GET", "status": "200"}) == 3.0
        assert counter.get_value({"method": "POST", "status": "201"}) == 1.0
    
    def test_negative_increment_raises(self):
        """Test that negative increment raises error."""
        counter = Counter("requests_total")
        
        with pytest.raises(ValueError):
            counter.inc(-1)
    
    def test_reset(self):
        """Test counter reset."""
        counter = Counter("requests_total")
        counter.inc(10)
        counter.reset()
        
        assert counter.get_value() == 0.0


class TestGauge:
    """Test Gauge metric."""
    
    def test_create_gauge(self):
        """Test creating a gauge."""
        gauge = Gauge("temperature", "Current temperature")
        
        assert gauge.name == "temperature"
        assert gauge.get_type() == MetricType.GAUGE
    
    def test_set_value(self):
        """Test setting gauge value."""
        gauge = Gauge("temperature")
        
        gauge.set(25.5)
        assert gauge.get_value() == 25.5
        
        gauge.set(30.0)
        assert gauge.get_value() == 30.0
    
    def test_increment_decrement(self):
        """Test gauge increment and decrement."""
        gauge = Gauge("queue_size")
        
        gauge.set(100)
        gauge.inc(10)
        assert gauge.get_value() == 110
        
        gauge.dec(20)
        assert gauge.get_value() == 90
    
    def test_gauge_with_labels(self):
        """Test gauge with labels."""
        gauge = Gauge("connections", labels=["type"])
        
        gauge.set(5, labels={"type": "http"})
        gauge.set(3, labels={"type": "database"})
        
        assert gauge.get_value({"type": "http"}) == 5
        assert gauge.get_value({"type": "database"}) == 3


class TestHistogram:
    """Test Histogram metric."""
    
    def test_create_histogram(self):
        """Test creating a histogram."""
        histogram = Histogram("latency_seconds")
        
        assert histogram.name == "latency_seconds"
        assert histogram.get_type() == MetricType.HISTOGRAM
    
    def test_observe_values(self):
        """Test observing values."""
        histogram = Histogram("latency_seconds")
        
        histogram.observe(0.1)
        histogram.observe(0.2)
        histogram.observe(0.3)
        
        stats = histogram.get_value()
        
        assert stats["count"] == 3
        assert stats["sum"] == pytest.approx(0.6)
        assert stats["avg"] == pytest.approx(0.2)
    
    def test_percentiles(self):
        """Test percentile calculation."""
        histogram = Histogram("latency_seconds")
        
        for i in range(100):
            histogram.observe(i / 100.0)
        
        assert histogram.get_percentile(0.50) == pytest.approx(0.50, abs=0.02)
        assert histogram.get_percentile(0.95) == pytest.approx(0.95, abs=0.02)
    
    def test_statistics(self):
        """Test statistical summary."""
        histogram = Histogram("latency_seconds")
        
        values = [0.1, 0.2, 0.3, 0.4, 0.5]
        for v in values:
            histogram.observe(v)
        
        stats = histogram.get_statistics()
        
        assert stats["min"] == 0.1
        assert stats["max"] == 0.5
        assert stats["avg"] == pytest.approx(0.3)
    
    def test_custom_buckets(self):
        """Test histogram with custom buckets."""
        histogram = Histogram(
            "latency_seconds",
            buckets=[0.1, 0.5, 1.0, 5.0]
        )
        
        histogram.observe(0.05)
        histogram.observe(0.3)
        histogram.observe(2.0)
        
        value = histogram.get_value()
        assert "buckets" in value


class TestTimer:
    """Test Timer context manager."""
    
    def test_timer_context_manager(self):
        """Test timer as context manager."""
        histogram = Histogram("operation_seconds")
        
        with Timer(histogram):
            time.sleep(0.05)
        
        stats = histogram.get_statistics()
        assert stats["count"] == 1
        assert stats["min"] >= 0.05
    
    def test_timer_manual(self):
        """Test manual timer start/stop."""
        histogram = Histogram("operation_seconds")
        timer = Timer(histogram)
        
        timer.start()
        time.sleep(0.05)
        duration = timer.stop()
        
        assert duration >= 0.05
        assert histogram.get_statistics()["count"] == 1


class TestMetricsRegistry:
    """Test MetricsRegistry."""
    
    def test_create_registry(self):
        """Test creating a registry."""
        registry = MetricsRegistry(prefix="app")
        
        assert registry.prefix == "app"
    
    def test_register_metrics(self):
        """Test registering metrics."""
        registry = MetricsRegistry()
        
        counter = registry.counter("requests", "Total requests")
        gauge = registry.gauge("connections", "Active connections")
        histogram = registry.histogram("latency", "Request latency")
        
        assert counter is not None
        assert gauge is not None
        assert histogram is not None
    
    def test_get_same_metric(self):
        """Test getting same metric returns same instance."""
        registry = MetricsRegistry()
        
        counter1 = registry.counter("requests")
        counter2 = registry.counter("requests")
        
        assert counter1 is counter2
    
    def test_collect_metrics(self):
        """Test collecting all metrics."""
        registry = MetricsRegistry()
        
        registry.counter("requests").inc(10)
        registry.gauge("connections").set(5)
        
        collected = registry.collect()
        
        assert "requests" in collected
        assert "connections" in collected
    
    def test_prometheus_format(self):
        """Test Prometheus text format export."""
        registry = MetricsRegistry()
        
        registry.counter("requests_total", "Total requests").inc(100)
        
        output = registry.to_prometheus()
        
        assert "# HELP requests_total" in output
        assert "# TYPE requests_total counter" in output
        assert "requests_total 100" in output
    
    def test_timer_factory(self):
        """Test timer factory from registry."""
        registry = MetricsRegistry()
        
        histogram, create_timer = registry.timer("operation_seconds")
        
        timer = create_timer()
        with timer:
            time.sleep(0.01)
        
        assert histogram.get_statistics()["count"] == 1


class TestGlobalRegistry:
    """Test global registry functions."""
    
    def test_configure_and_get(self):
        """Test configuring global registry."""
        registry = MetricsRegistry(prefix="test")
        configure_metrics(registry)
        
        retrieved = get_metrics_registry()
        assert retrieved is registry
    
    def test_auto_create_registry(self):
        """Test auto-creation of global registry."""
        # Reset global
        import src.monitoring.metrics as metrics_module
        metrics_module._global_registry = None
        
        registry = get_metrics_registry()
        assert registry is not None


class TestPipelineMetrics:
    """Test pipeline-specific metrics."""
    
    def test_create_pipeline_metrics(self):
        """Test creating pipeline metrics."""
        registry = MetricsRegistry()
        metrics = create_pipeline_metrics(registry)
        
        assert "events_processed" in metrics
        assert "events_failed" in metrics
        assert "event_latency" in metrics
        assert "queue_size" in metrics


# =============================================================================
# Alerting Tests
# =============================================================================

class TestAlertLevel:
    """Test AlertLevel enum."""
    
    def test_level_comparison(self):
        """Test alert level comparison."""
        assert AlertLevel.INFO < AlertLevel.WARNING
        assert AlertLevel.WARNING < AlertLevel.CRITICAL


class TestAlert:
    """Test Alert class."""
    
    def test_create_alert(self):
        """Test creating an alert."""
        alert = Alert(
            name="high_cpu",
            level=AlertLevel.WARNING,
            message="CPU usage is high",
        )
        
        assert alert.name == "high_cpu"
        assert alert.level == AlertLevel.WARNING
        assert alert.state == AlertState.FIRING
        assert alert.alert_id is not None
    
    def test_alert_resolve(self):
        """Test resolving an alert."""
        alert = Alert(
            name="high_cpu",
            level=AlertLevel.WARNING,
            message="CPU usage is high",
        )
        
        alert.resolve()
        
        assert alert.state == AlertState.RESOLVED
        assert alert.resolved_at is not None
    
    def test_alert_duration(self):
        """Test alert duration calculation."""
        alert = Alert(
            name="test",
            level=AlertLevel.INFO,
            message="Test",
        )
        
        time.sleep(0.1)
        
        assert alert.duration >= timedelta(milliseconds=100)
    
    def test_alert_to_dict(self):
        """Test alert serialization."""
        alert = Alert(
            name="test",
            level=AlertLevel.WARNING,
            message="Test message",
            labels={"host": "server1"},
        )
        
        data = alert.to_dict()
        
        assert data["name"] == "test"
        assert data["level"] == "WARNING"
        assert data["labels"]["host"] == "server1"


class TestAlertRule:
    """Test AlertRule class."""
    
    def test_create_rule(self):
        """Test creating an alert rule."""
        rule = AlertRule(
            name="high_cpu",
            condition=lambda m: m.get("cpu", 0) > 90,
            level=AlertLevel.WARNING,
            message="CPU usage exceeds 90%",
        )
        
        assert rule.name == "high_cpu"
        assert rule.enabled is True
    
    def test_evaluate_condition_met(self):
        """Test rule evaluation when condition is met."""
        rule = AlertRule(
            name="high_cpu",
            condition=lambda m: m.get("cpu", 0) > 90,
            level=AlertLevel.WARNING,
            message="CPU high",
        )
        
        alert = rule.evaluate({"cpu": 95})
        
        assert alert is not None
        assert alert.state == AlertState.FIRING
    
    def test_evaluate_condition_not_met(self):
        """Test rule evaluation when condition is not met."""
        rule = AlertRule(
            name="high_cpu",
            condition=lambda m: m.get("cpu", 0) > 90,
            level=AlertLevel.WARNING,
            message="CPU high",
        )
        
        alert = rule.evaluate({"cpu": 50})
        
        assert alert is None
    
    def test_for_duration(self):
        """Test for_duration threshold."""
        rule = AlertRule(
            name="high_cpu",
            condition=lambda m: m.get("cpu", 0) > 90,
            for_duration=timedelta(milliseconds=100),
        )
        
        # First evaluation - pending
        alert = rule.evaluate({"cpu": 95})
        assert alert is None  # Not yet past duration
        
        # Wait for duration
        time.sleep(0.15)
        
        # Second evaluation - should fire
        alert = rule.evaluate({"cpu": 95})
        assert alert is not None
        assert alert.state == AlertState.FIRING
    
    def test_alert_resolve_on_recovery(self):
        """Test alert resolves when condition is no longer met."""
        rule = AlertRule(
            name="high_cpu",
            condition=lambda m: m.get("cpu", 0) > 90,
        )
        
        # Fire alert
        rule.evaluate({"cpu": 95})
        alert = rule.evaluate({"cpu": 95})
        assert alert.state == AlertState.FIRING
        
        # Recover
        alert = rule.evaluate({"cpu": 50})
        assert alert.state == AlertState.RESOLVED


class TestLogAlertChannel:
    """Test LogAlertChannel."""
    
    def test_send_alert(self):
        """Test sending alert to log."""
        channel = LogAlertChannel()
        
        alert = Alert(
            name="test",
            level=AlertLevel.WARNING,
            message="Test alert",
        )
        
        result = channel.send(alert)
        assert result is True
    
    def test_send_resolved(self):
        """Test sending resolution to log."""
        channel = LogAlertChannel()
        
        alert = Alert(
            name="test",
            level=AlertLevel.INFO,
            message="Test",
        )
        alert.resolve()
        
        result = channel.send_resolved(alert)
        assert result is True


class TestAlertManager:
    """Test AlertManager."""
    
    def test_add_rule(self):
        """Test adding an alert rule."""
        manager = AlertManager()
        
        rule = AlertRule(
            name="test",
            condition=lambda m: True,
        )
        
        manager.add_rule(rule)
        
        assert len(manager.get_rules()) == 1
    
    def test_add_channel(self):
        """Test adding a channel."""
        manager = AlertManager()
        manager.add_channel(LogAlertChannel())
        
        # Channel added (no direct getter, but should work)
    
    def test_evaluate_rules(self):
        """Test evaluating all rules."""
        manager = AlertManager()
        manager.add_channel(LogAlertChannel())
        
        manager.add_rule(AlertRule(
            name="high_cpu",
            condition=lambda m: m.get("cpu", 0) > 90,
        ))
        
        changed = manager.evaluate({"cpu": 95})
        
        assert len(changed) == 1
        assert changed[0].name == "high_cpu"
    
    def test_active_alerts(self):
        """Test getting active alerts."""
        manager = AlertManager()
        
        manager.add_rule(AlertRule(
            name="alert1",
            condition=lambda m: True,
        ))
        manager.add_rule(AlertRule(
            name="alert2",
            condition=lambda m: False,
        ))
        
        manager.evaluate({})
        
        active = manager.get_active_alerts()
        assert len(active) == 1
        assert active[0].name == "alert1"
    
    def test_silence(self):
        """Test silencing alerts."""
        manager = AlertManager()
        manager.add_channel(LogAlertChannel())
        
        manager.add_rule(AlertRule(
            name="silenced_alert",
            condition=lambda m: True,
            labels={"host": "server1"},
        ))
        
        # Add silence
        manager.add_silence(
            matchers={"alertname": "silenced_alert"},
            duration=timedelta(hours=1),
            comment="Maintenance",
        )
        
        # Evaluate - should not notify due to silence
        manager.evaluate({})
        
        # Alert is still active but silenced
        active = manager.get_active_alerts()
        assert len(active) == 1


class TestPipelineAlertRules:
    """Test pipeline alert rules."""
    
    def test_create_rules(self):
        """Test creating pipeline alert rules."""
        rules = create_pipeline_alert_rules()
        
        assert len(rules) > 0
        
        # Check some expected rules
        rule_names = [r.name for r in rules]
        assert "high_replication_lag" in rule_names
        assert "high_error_rate" in rule_names


# =============================================================================
# Health Check Tests
# =============================================================================

class TestHealthStatus:
    """Test HealthStatus enum."""
    
    def test_is_ok(self):
        """Test is_ok property."""
        assert HealthStatus.HEALTHY.is_ok is True
        assert HealthStatus.DEGRADED.is_ok is True
        assert HealthStatus.UNHEALTHY.is_ok is False


class TestHealthResult:
    """Test HealthResult class."""
    
    def test_create_result(self):
        """Test creating a health result."""
        result = HealthResult(
            status=HealthStatus.HEALTHY,
            message="All good",
            latency_ms=5.0,
        )
        
        assert result.status == HealthStatus.HEALTHY
        assert result.latency_ms == 5.0
    
    def test_to_dict(self):
        """Test result serialization."""
        result = HealthResult(
            status=HealthStatus.UNHEALTHY,
            message="Connection failed",
        )
        
        data = result.to_dict()
        
        assert data["status"] == "UNHEALTHY"
        assert data["message"] == "Connection failed"


class TestTCPHealthCheck:
    """Test TCPHealthCheck."""
    
    def test_healthy_connection(self):
        """Test checking a healthy connection."""
        # Start a simple TCP server
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('localhost', 0))
        server_socket.listen(1)
        port = server_socket.getsockname()[1]
        
        try:
            check = TCPHealthCheck("test", "localhost", port, timeout=1.0)
            result = check.run()
            
            assert result.status == HealthStatus.HEALTHY
            assert result.latency_ms > 0
        finally:
            server_socket.close()
    
    def test_unhealthy_connection(self):
        """Test checking an unhealthy connection."""
        # Use a port that's likely not in use
        check = TCPHealthCheck("test", "localhost", 59999, timeout=0.5)
        result = check.run()
        
        assert result.status == HealthStatus.UNHEALTHY


class TestHTTPHealthCheck:
    """Test HTTPHealthCheck."""
    
    def test_healthy_endpoint(self):
        """Test checking a healthy HTTP endpoint."""
        # Start a simple HTTP server
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK")
            
            def log_message(self, format, *args):
                pass  # Suppress logs
        
        server = HTTPServer(('localhost', 0), Handler)
        port = server.server_address[1]
        
        thread = threading.Thread(target=server.handle_request)
        thread.daemon = True
        thread.start()
        
        try:
            check = HTTPHealthCheck("test", f"http://localhost:{port}/health")
            result = check.run()
            
            assert result.status == HealthStatus.HEALTHY
        finally:
            server.server_close()
    
    def test_unhealthy_endpoint(self):
        """Test checking an unreachable HTTP endpoint."""
        check = HTTPHealthCheck("test", "http://localhost:59998/health", timeout=0.5)
        result = check.run()
        
        assert result.status == HealthStatus.UNHEALTHY


class TestCallableHealthCheck:
    """Test CallableHealthCheck."""
    
    def test_callable_check(self):
        """Test custom callable check."""
        def my_check():
            return HealthResult(
                status=HealthStatus.HEALTHY,
                message="Custom check passed",
            )
        
        check = CallableHealthCheck("custom", my_check)
        result = check.run()
        
        assert result.status == HealthStatus.HEALTHY
        assert result.message == "Custom check passed"
    
    def test_callable_with_exception(self):
        """Test callable that raises exception."""
        def failing_check():
            raise ValueError("Check failed")
        
        check = CallableHealthCheck("failing", failing_check)
        result = check.run()
        
        assert result.status == HealthStatus.UNHEALTHY


class TestHealthChecker:
    """Test HealthChecker."""
    
    def test_add_check(self):
        """Test adding health checks."""
        checker = HealthChecker()
        
        checker.add_check(CallableHealthCheck(
            "test",
            lambda: HealthResult(status=HealthStatus.HEALTHY),
        ))
        
        assert "test" in checker.get_checks()
    
    def test_check_all(self):
        """Test running all checks."""
        checker = HealthChecker("test-system")
        
        checker.add_check(CallableHealthCheck(
            "healthy",
            lambda: HealthResult(status=HealthStatus.HEALTHY),
        ))
        checker.add_check(CallableHealthCheck(
            "degraded",
            lambda: HealthResult(status=HealthStatus.DEGRADED),
        ))
        
        result = checker.check_all()
        
        assert result["name"] == "test-system"
        assert "healthy" in result["checks"]
        assert "degraded" in result["checks"]
    
    def test_overall_status(self):
        """Test overall status calculation."""
        checker = HealthChecker()
        
        # One critical unhealthy check
        checker.add_check(CallableHealthCheck(
            "critical",
            lambda: HealthResult(status=HealthStatus.UNHEALTHY),
            critical=True,
        ))
        
        result = checker.check_all()
        
        assert result["status"] == "UNHEALTHY"
        assert result["healthy"] is False
    
    def test_non_critical_check(self):
        """Test non-critical check doesn't affect overall status."""
        checker = HealthChecker()
        
        checker.add_check(CallableHealthCheck(
            "critical",
            lambda: HealthResult(status=HealthStatus.HEALTHY),
            critical=True,
        ))
        checker.add_check(CallableHealthCheck(
            "non_critical",
            lambda: HealthResult(status=HealthStatus.UNHEALTHY),
            critical=False,
        ))
        
        result = checker.check_all()
        
        assert result["status"] == "HEALTHY"


# =============================================================================
# Dashboard Tests
# =============================================================================

class TestTimeRange:
    """Test TimeRange class."""
    
    def test_relative_range(self):
        """Test relative time range."""
        range = TimeRange.last_hours(1)
        start, end = range.get_range()
        
        assert end - start == timedelta(hours=1)
    
    def test_absolute_range(self):
        """Test absolute time range."""
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)
        
        range = TimeRange(start=start, end=end)
        s, e = range.get_range()
        
        assert s == start
        assert e == end
    
    def test_to_dict(self):
        """Test time range serialization."""
        range = TimeRange.last_minutes(30)
        data = range.to_dict()
        
        assert "start" in data
        assert "end" in data
        assert data["relative_seconds"] == 1800


class TestDashboardPanel:
    """Test DashboardPanel class."""
    
    def test_create_panel(self):
        """Test creating a panel."""
        panel = DashboardPanel(
            id="latency",
            title="Request Latency",
            panel_type=PanelType.GAUGE,
            metric="request_latency_p95",
        )
        
        assert panel.id == "latency"
        assert panel.panel_type == PanelType.GAUGE
    
    def test_get_data(self):
        """Test getting panel data."""
        panel = DashboardPanel(
            id="events",
            title="Events",
            panel_type=PanelType.COUNTER,
            metric="events_total",
        )
        
        metrics = {"events_total": 1000}
        data = panel.get_data(metrics)
        
        assert data == 1000
    
    def test_threshold_color(self):
        """Test threshold-based coloring."""
        panel = DashboardPanel(
            id="cpu",
            title="CPU",
            panel_type=PanelType.GAUGE,
            thresholds=[
                Threshold(90, "red"),
                Threshold(70, "yellow"),
                Threshold(0, "green"),
            ],
        )
        
        assert panel.get_color(95) == "red"
        assert panel.get_color(80) == "yellow"
        assert panel.get_color(50) == "green"
    
    def test_to_dict(self):
        """Test panel serialization."""
        panel = DashboardPanel(
            id="test",
            title="Test Panel",
            panel_type=PanelType.STAT,
            unit="ms",
        )
        
        data = panel.to_dict()
        
        assert data["id"] == "test"
        assert data["type"] == "STAT"
        assert data["unit"] == "ms"


class TestDashboard:
    """Test Dashboard class."""
    
    def test_create_dashboard(self):
        """Test creating a dashboard."""
        dashboard = Dashboard(
            id="overview",
            title="System Overview",
            description="Main monitoring dashboard",
        )
        
        assert dashboard.id == "overview"
        assert dashboard.title == "System Overview"
    
    def test_add_panel(self):
        """Test adding panels."""
        dashboard = Dashboard(id="test", title="Test")
        
        dashboard.add_panel(DashboardPanel(
            id="events",
            title="Events",
            panel_type=PanelType.COUNTER,
        ))
        
        assert len(dashboard.panels) == 1
    
    def test_add_row(self):
        """Test adding rows."""
        dashboard = Dashboard(id="test", title="Test")
        
        row = DashboardRow(title="Overview")
        row.add_panel(DashboardPanel(
            id="events",
            title="Events",
            panel_type=PanelType.COUNTER,
        ))
        
        dashboard.add_row(row)
        
        assert len(dashboard.rows) == 1
    
    def test_get_panel(self):
        """Test getting panel by ID."""
        dashboard = Dashboard(id="test", title="Test")
        dashboard.add_panel(DashboardPanel(
            id="events",
            title="Events",
            panel_type=PanelType.COUNTER,
        ))
        
        panel = dashboard.get_panel("events")
        
        assert panel is not None
        assert panel.id == "events"
    
    def test_render(self):
        """Test rendering dashboard."""
        dashboard = Dashboard(id="test", title="Test")
        dashboard.add_panel(DashboardPanel(
            id="events",
            title="Events",
            panel_type=PanelType.COUNTER,
            metric="events_total",
        ))
        
        metrics = {"events_total": 1000}
        rendered = dashboard.render(metrics)
        
        assert rendered["id"] == "test"
        assert len(rendered["panels"]) == 1
        assert rendered["panels"][0]["data"] == 1000
    
    def test_to_json(self):
        """Test JSON export."""
        dashboard = Dashboard(
            id="test",
            title="Test Dashboard",
            tags=["monitoring"],
        )
        
        json_str = dashboard.to_json()
        data = json.loads(json_str)
        
        assert data["id"] == "test"
        assert "monitoring" in data["tags"]


class TestDashboardManager:
    """Test DashboardManager."""
    
    def test_create_dashboard(self):
        """Test creating dashboard via manager."""
        manager = DashboardManager()
        
        dashboard = manager.create_dashboard(
            id="overview",
            title="Overview",
            created_by="test",
        )
        
        assert dashboard.id == "overview"
        assert manager.get_dashboard("overview") is dashboard
    
    def test_list_dashboards(self):
        """Test listing dashboards."""
        manager = DashboardManager()
        
        manager.create_dashboard("d1", "Dashboard 1")
        manager.create_dashboard("d2", "Dashboard 2")
        
        dashboards = manager.list_dashboards()
        
        assert len(dashboards) == 2
    
    def test_delete_dashboard(self):
        """Test deleting dashboard."""
        manager = DashboardManager()
        manager.create_dashboard("test", "Test")
        
        result = manager.delete_dashboard("test")
        
        assert result is True
        assert manager.get_dashboard("test") is None
    
    def test_search_dashboards(self):
        """Test searching dashboards."""
        manager = DashboardManager()
        
        d1 = manager.create_dashboard("cdc", "CDC Overview")
        d1.tags = ["cdc", "monitoring"]
        
        d2 = manager.create_dashboard("api", "API Metrics")
        d2.tags = ["api"]
        
        results = manager.search_dashboards("cdc")
        
        assert len(results) == 1
        assert results[0].id == "cdc"
    
    def test_export_import(self):
        """Test export and import."""
        manager = DashboardManager()
        
        dashboard = manager.create_dashboard("test", "Test")
        dashboard.add_panel(DashboardPanel(
            id="events",
            title="Events",
            panel_type=PanelType.COUNTER,
        ))
        
        # Export
        json_str = manager.export_dashboard("test")
        
        # Delete
        manager.delete_dashboard("test")
        
        # Import
        imported = manager.import_dashboard(json_str)
        
        assert imported.id == "test"
        assert len(imported.panels) == 1


class TestPipelineDashboard:
    """Test pipeline dashboard creation."""
    
    def test_create_pipeline_dashboard(self):
        """Test creating pipeline dashboard."""
        dashboard = create_pipeline_dashboard()
        
        assert dashboard.id == "cdc-pipeline"
        assert len(dashboard.rows) > 0
        
        # Check for expected panels
        panel_ids = []
        for row in dashboard.rows:
            for panel in row.panels:
                panel_ids.append(panel.id)
        
        assert "events_total" in panel_ids
        assert "error_rate" in panel_ids


class TestTextDashboard:
    """Test text dashboard creation."""
    
    def test_create_text_dashboard(self):
        """Test creating text dashboard."""
        metrics = {
            "events_processed_total": 10000,
            "events_failed_total": 5,
            "latency_statistics": {
                "p50": 0.01,
                "p95": 0.05,
                "p99": 0.1,
                "max": 0.5,
            },
            "active_connections": 10,
            "kafka_consumer_lag": 100,
            "queue_size": 50,
        }
        
        output = create_simple_text_dashboard(metrics)
        
        assert "CDC PIPELINE DASHBOARD" in output
        assert "Events Processed: 10,000" in output
        assert "Error Rate:" in output


# =============================================================================
# Integration Tests
# =============================================================================

class TestMonitoringIntegration:
    """Integration tests for monitoring components."""
    
    def test_metrics_to_alerts(self):
        """Test metrics triggering alerts."""
        # Setup
        registry = MetricsRegistry()
        error_counter = registry.counter("errors")
        total_counter = registry.counter("requests")
        
        manager = AlertManager()
        manager.add_channel(LogAlertChannel())
        
        manager.add_rule(AlertRule(
            name="high_error_rate",
            condition=lambda m: m.get("error_rate", 0) > 0.05,
            level=AlertLevel.WARNING,
        ))
        
        # Simulate traffic
        total_counter.inc(1000)
        error_counter.inc(100)  # 10% error rate
        
        # Calculate metrics
        metrics = {
            "error_rate": error_counter.get_value() / total_counter.get_value(),
        }
        
        # Evaluate alerts
        changed = manager.evaluate(metrics)
        
        assert len(changed) == 1
        assert changed[0].name == "high_error_rate"
    
    def test_health_to_dashboard(self):
        """Test health checks appearing in dashboard."""
        # Setup health checker
        checker = HealthChecker()
        checker.add_check(CallableHealthCheck(
            "database",
            lambda: HealthResult(status=HealthStatus.HEALTHY),
        ))
        checker.add_check(CallableHealthCheck(
            "kafka",
            lambda: HealthResult(status=HealthStatus.DEGRADED),
        ))
        
        # Run health checks
        health_data = checker.check_all()
        
        # Create dashboard with health data
        dashboard = Dashboard(id="health", title="Health")
        dashboard.add_panel(DashboardPanel(
            id="health_grid",
            title="Component Health",
            panel_type=PanelType.HEALTH_GRID,
            data_source=lambda: health_data,
        ))
        
        # Render
        rendered = dashboard.render({})
        
        assert rendered["panels"][0]["data"]["status"] == "DEGRADED"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
