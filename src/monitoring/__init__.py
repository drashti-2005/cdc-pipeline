"""
Monitoring Module

Provides observability infrastructure for the CDC pipeline:
- Metrics: Collect and expose performance data
- Alerting: Trigger notifications based on conditions
- Health Checks: Monitor component availability
- Dashboards: Visualize system state
"""

from .metrics import (
    MetricsRegistry,
    Counter,
    Gauge,
    Histogram,
    Timer,
    MetricType,
    get_metrics_registry,
    configure_metrics,
)
from .alerting import (
    Alert,
    AlertLevel,
    AlertState,
    AlertRule,
    AlertManager,
    AlertChannel,
    LogAlertChannel,
    WebhookAlertChannel,
    EmailAlertChannel,
)
from .health import (
    HealthStatus,
    HealthCheck,
    HealthResult,
    HealthChecker,
    ComponentHealth,
    create_tcp_check,
    create_http_check,
    create_database_check,
    create_kafka_check,
)
from .dashboard import (
    DashboardPanel,
    Dashboard,
    DashboardManager,
    PanelType,
    TimeRange,
)

__all__ = [
    # Metrics
    "MetricsRegistry",
    "Counter",
    "Gauge",
    "Histogram",
    "Timer",
    "MetricType",
    "get_metrics_registry",
    "configure_metrics",
    # Alerting
    "Alert",
    "AlertLevel",
    "AlertState",
    "AlertRule",
    "AlertManager",
    "AlertChannel",
    "LogAlertChannel",
    "WebhookAlertChannel",
    "EmailAlertChannel",
    # Health
    "HealthStatus",
    "HealthCheck",
    "HealthResult",
    "HealthChecker",
    "ComponentHealth",
    "create_tcp_check",
    "create_http_check",
    "create_database_check",
    "create_kafka_check",
    # Dashboard
    "DashboardPanel",
    "Dashboard",
    "DashboardManager",
    "PanelType",
    "TimeRange",
]
