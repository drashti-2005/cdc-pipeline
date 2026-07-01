"""
Dashboard Module

Provides dashboard and visualization components:
- Dashboard panels for different metric types
- Time range selection
- Dashboard management

SIMPLE EXPLANATION:
A dashboard is like a car's instrument panel:
- Shows important information at a glance
- Different gauges for different things
- Updates in real-time
"""

import json
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
from typing import Dict, List, Optional, Any, Callable, Union

logger = logging.getLogger(__name__)


class PanelType(Enum):
    """Types of dashboard panels."""
    
    GAUGE = auto()          # Single value with optional threshold
    COUNTER = auto()        # Cumulative count
    GRAPH = auto()          # Time-series line graph
    BAR = auto()            # Bar chart
    TABLE = auto()          # Tabular data
    STAT = auto()           # Big number with sparkline
    HEATMAP = auto()        # Heatmap visualization
    TEXT = auto()           # Text/markdown panel
    ALERT_LIST = auto()     # List of alerts
    HEALTH_GRID = auto()    # Health status grid


@dataclass
class TimeRange:
    """
    Time range for dashboard queries.
    
    Can be absolute (start/end) or relative (last N hours).
    """
    
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    relative: Optional[timedelta] = None
    
    def get_range(self) -> tuple:
        """Get absolute start and end times."""
        if self.relative:
            end = datetime.now(timezone.utc)
            start = end - self.relative
            return start, end
        
        return self.start, self.end
    
    @classmethod
    def last_minutes(cls, minutes: int) -> "TimeRange":
        """Create relative time range for last N minutes."""
        return cls(relative=timedelta(minutes=minutes))
    
    @classmethod
    def last_hours(cls, hours: int) -> "TimeRange":
        """Create relative time range for last N hours."""
        return cls(relative=timedelta(hours=hours))
    
    @classmethod
    def last_days(cls, days: int) -> "TimeRange":
        """Create relative time range for last N days."""
        return cls(relative=timedelta(days=days))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        start, end = self.get_range()
        return {
            "start": start.isoformat() if start else None,
            "end": end.isoformat() if end else None,
            "relative_seconds": self.relative.total_seconds() if self.relative else None,
        }


@dataclass
class Threshold:
    """Threshold for visual indicators."""
    
    value: float
    color: str = "red"
    label: str = ""


@dataclass
class DashboardPanel:
    """
    A panel in a dashboard.
    
    Represents a single visualization element.
    
    USAGE:
        panel = DashboardPanel(
            id="latency",
            title="Request Latency (p95)",
            panel_type=PanelType.GAUGE,
            metric="request_latency_p95",
            thresholds=[
                Threshold(0.1, "green", "Good"),
                Threshold(0.5, "yellow", "Warning"),
                Threshold(1.0, "red", "Critical"),
            ]
        )
    """
    
    id: str
    title: str
    panel_type: PanelType
    
    # Data source
    metric: Optional[str] = None
    query: Optional[str] = None
    data_source: Optional[Callable[[], Any]] = None
    
    # Display options
    width: int = 4           # Grid units (out of 12)
    height: int = 4          # Grid units
    position_x: int = 0      # Grid position
    position_y: int = 0      # Grid position
    
    # Visual options
    unit: str = ""           # Unit label (e.g., "ms", "%")
    decimals: int = 2        # Decimal places
    thresholds: List[Threshold] = field(default_factory=list)
    colors: List[str] = field(default_factory=list)
    
    # Time range (optional override)
    time_range: Optional[TimeRange] = None
    
    # Refresh
    refresh_interval: Optional[int] = None  # Seconds
    
    # Metadata
    description: str = ""
    
    def get_data(self, metrics_data: Dict[str, Any]) -> Any:
        """
        Get data for this panel.
        
        Args:
            metrics_data: Current metrics data
            
        Returns:
            Panel-specific data
        """
        if self.data_source:
            return self.data_source()
        
        if self.metric and self.metric in metrics_data:
            return metrics_data[self.metric]
        
        return None
    
    def get_color(self, value: float) -> str:
        """Get color based on value and thresholds."""
        if not self.thresholds:
            return self.colors[0] if self.colors else "green"
        
        for threshold in sorted(self.thresholds, key=lambda t: t.value, reverse=True):
            if value >= threshold.value:
                return threshold.color
        
        return "green"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "title": self.title,
            "type": self.panel_type.name,
            "metric": self.metric,
            "width": self.width,
            "height": self.height,
            "position_x": self.position_x,
            "position_y": self.position_y,
            "unit": self.unit,
            "decimals": self.decimals,
            "thresholds": [
                {"value": t.value, "color": t.color, "label": t.label}
                for t in self.thresholds
            ],
            "description": self.description,
            "refresh_interval": self.refresh_interval,
        }


@dataclass
class DashboardRow:
    """A row of panels in a dashboard."""
    
    title: str = ""
    collapsed: bool = False
    panels: List[DashboardPanel] = field(default_factory=list)
    
    def add_panel(self, panel: DashboardPanel) -> None:
        """Add a panel to this row."""
        self.panels.append(panel)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "title": self.title,
            "collapsed": self.collapsed,
            "panels": [p.to_dict() for p in self.panels],
        }


@dataclass
class Dashboard:
    """
    A complete dashboard.
    
    Contains multiple panels organized in rows.
    
    USAGE:
        dashboard = Dashboard(
            id="cdc-overview",
            title="CDC Pipeline Overview",
            description="Real-time CDC pipeline monitoring"
        )
        
        # Add panels
        dashboard.add_panel(DashboardPanel(
            id="events",
            title="Events Processed",
            panel_type=PanelType.COUNTER,
            metric="events_processed_total"
        ))
        
        # Render
        data = dashboard.render(metrics)
    """
    
    id: str
    title: str
    description: str = ""
    
    # Time range
    time_range: TimeRange = field(default_factory=lambda: TimeRange.last_hours(1))
    
    # Refresh
    refresh_interval: int = 30  # Seconds
    auto_refresh: bool = True
    
    # Content
    rows: List[DashboardRow] = field(default_factory=list)
    panels: List[DashboardPanel] = field(default_factory=list)
    
    # Metadata
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    created_by: str = ""
    tags: List[str] = field(default_factory=list)
    
    # Variables (for templating)
    variables: Dict[str, Any] = field(default_factory=dict)
    
    def add_panel(self, panel: DashboardPanel) -> None:
        """Add a panel to the dashboard."""
        self.panels.append(panel)
        self.updated_at = datetime.now(timezone.utc)
    
    def add_row(self, row: DashboardRow) -> None:
        """Add a row to the dashboard."""
        self.rows.append(row)
        self.updated_at = datetime.now(timezone.utc)
    
    def get_panel(self, panel_id: str) -> Optional[DashboardPanel]:
        """Get a panel by ID."""
        for panel in self.panels:
            if panel.id == panel_id:
                return panel
        
        for row in self.rows:
            for panel in row.panels:
                if panel.id == panel_id:
                    return panel
        
        return None
    
    def render(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Render the dashboard with current data.
        
        Args:
            metrics_data: Current metrics values
            
        Returns:
            Dashboard data ready for display
        """
        rendered_panels = []
        
        # Render standalone panels
        for panel in self.panels:
            rendered_panels.append({
                **panel.to_dict(),
                "data": panel.get_data(metrics_data),
            })
        
        # Render row panels
        rendered_rows = []
        for row in self.rows:
            rendered_row = {
                "title": row.title,
                "collapsed": row.collapsed,
                "panels": [],
            }
            
            for panel in row.panels:
                rendered_row["panels"].append({
                    **panel.to_dict(),
                    "data": panel.get_data(metrics_data),
                })
            
            rendered_rows.append(rendered_row)
        
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "time_range": self.time_range.to_dict(),
            "refresh_interval": self.refresh_interval,
            "panels": rendered_panels,
            "rows": rendered_rows,
            "rendered_at": datetime.now(timezone.utc).isoformat(),
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (for persistence)."""
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "time_range": self.time_range.to_dict(),
            "refresh_interval": self.refresh_interval,
            "auto_refresh": self.auto_refresh,
            "panels": [p.to_dict() for p in self.panels],
            "rows": [r.to_dict() for r in self.rows],
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "created_by": self.created_by,
            "tags": self.tags,
            "variables": self.variables,
        }
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


class DashboardManager:
    """
    Manages multiple dashboards.
    
    USAGE:
        manager = DashboardManager()
        
        # Create dashboard
        dashboard = manager.create_dashboard("overview", "System Overview")
        
        # Add panels
        dashboard.add_panel(...)
        
        # Save
        manager.save_dashboard(dashboard)
        
        # List dashboards
        for d in manager.list_dashboards():
            print(d.title)
    """
    
    def __init__(self):
        self._dashboards: Dict[str, Dashboard] = {}
        self._lock = threading.Lock()
    
    def create_dashboard(
        self,
        id: str,
        title: str,
        description: str = "",
        created_by: str = "",
    ) -> Dashboard:
        """Create a new dashboard."""
        dashboard = Dashboard(
            id=id,
            title=title,
            description=description,
            created_by=created_by,
        )
        
        with self._lock:
            self._dashboards[id] = dashboard
        
        logger.info(f"Created dashboard: {id}")
        return dashboard
    
    def get_dashboard(self, id: str) -> Optional[Dashboard]:
        """Get a dashboard by ID."""
        with self._lock:
            return self._dashboards.get(id)
    
    def save_dashboard(self, dashboard: Dashboard) -> None:
        """Save a dashboard."""
        dashboard.updated_at = datetime.now(timezone.utc)
        
        with self._lock:
            self._dashboards[dashboard.id] = dashboard
        
        logger.info(f"Saved dashboard: {dashboard.id}")
    
    def delete_dashboard(self, id: str) -> bool:
        """Delete a dashboard."""
        with self._lock:
            if id in self._dashboards:
                del self._dashboards[id]
                logger.info(f"Deleted dashboard: {id}")
                return True
        return False
    
    def list_dashboards(self) -> List[Dashboard]:
        """List all dashboards."""
        with self._lock:
            return list(self._dashboards.values())
    
    def search_dashboards(self, query: str) -> List[Dashboard]:
        """Search dashboards by title or tags."""
        query = query.lower()
        results = []
        
        with self._lock:
            for dashboard in self._dashboards.values():
                if query in dashboard.title.lower():
                    results.append(dashboard)
                elif any(query in tag.lower() for tag in dashboard.tags):
                    results.append(dashboard)
        
        return results
    
    def export_dashboard(self, id: str) -> Optional[str]:
        """Export dashboard to JSON."""
        dashboard = self.get_dashboard(id)
        if dashboard:
            return dashboard.to_json()
        return None
    
    def import_dashboard(self, json_str: str) -> Dashboard:
        """Import dashboard from JSON."""
        data = json.loads(json_str)
        
        dashboard = Dashboard(
            id=data["id"],
            title=data["title"],
            description=data.get("description", ""),
            refresh_interval=data.get("refresh_interval", 30),
            auto_refresh=data.get("auto_refresh", True),
            created_by=data.get("created_by", ""),
            tags=data.get("tags", []),
            variables=data.get("variables", {}),
        )
        
        # Import panels
        for panel_data in data.get("panels", []):
            panel = DashboardPanel(
                id=panel_data["id"],
                title=panel_data["title"],
                panel_type=PanelType[panel_data["type"]],
                metric=panel_data.get("metric"),
                width=panel_data.get("width", 4),
                height=panel_data.get("height", 4),
                unit=panel_data.get("unit", ""),
                description=panel_data.get("description", ""),
            )
            dashboard.add_panel(panel)
        
        self.save_dashboard(dashboard)
        return dashboard


def create_pipeline_dashboard() -> Dashboard:
    """
    Create a standard CDC pipeline monitoring dashboard.
    
    Returns:
        Configured Dashboard object
    """
    dashboard = Dashboard(
        id="cdc-pipeline",
        title="CDC Pipeline Dashboard",
        description="Real-time monitoring for CDC data pipeline",
        tags=["cdc", "pipeline", "monitoring"],
    )
    
    # Overview row
    overview_row = DashboardRow(title="Overview")
    
    overview_row.add_panel(DashboardPanel(
        id="events_total",
        title="Events Processed",
        panel_type=PanelType.STAT,
        metric="cdc_events_processed_total",
        width=3,
        height=2,
        unit="events",
    ))
    
    overview_row.add_panel(DashboardPanel(
        id="events_rate",
        title="Events/sec",
        panel_type=PanelType.GAUGE,
        metric="cdc_events_per_second",
        width=3,
        height=2,
        unit="events/s",
        thresholds=[
            Threshold(1000, "green", "Normal"),
            Threshold(500, "yellow", "Low"),
            Threshold(100, "red", "Critical"),
        ],
    ))
    
    overview_row.add_panel(DashboardPanel(
        id="error_rate",
        title="Error Rate",
        panel_type=PanelType.GAUGE,
        metric="cdc_error_rate",
        width=3,
        height=2,
        unit="%",
        thresholds=[
            Threshold(5, "red", "Critical"),
            Threshold(1, "yellow", "Warning"),
            Threshold(0, "green", "Good"),
        ],
    ))
    
    overview_row.add_panel(DashboardPanel(
        id="replication_lag",
        title="Replication Lag",
        panel_type=PanelType.GAUGE,
        metric="cdc_replication_lag_seconds",
        width=3,
        height=2,
        unit="seconds",
        thresholds=[
            Threshold(60, "red", "Critical"),
            Threshold(10, "yellow", "Warning"),
            Threshold(0, "green", "Good"),
        ],
    ))
    
    dashboard.add_row(overview_row)
    
    # Latency row
    latency_row = DashboardRow(title="Latency")
    
    latency_row.add_panel(DashboardPanel(
        id="latency_p50",
        title="Latency p50",
        panel_type=PanelType.STAT,
        metric="cdc_event_latency_p50",
        width=3,
        height=2,
        unit="ms",
    ))
    
    latency_row.add_panel(DashboardPanel(
        id="latency_p95",
        title="Latency p95",
        panel_type=PanelType.STAT,
        metric="cdc_event_latency_p95",
        width=3,
        height=2,
        unit="ms",
    ))
    
    latency_row.add_panel(DashboardPanel(
        id="latency_p99",
        title="Latency p99",
        panel_type=PanelType.STAT,
        metric="cdc_event_latency_p99",
        width=3,
        height=2,
        unit="ms",
    ))
    
    latency_row.add_panel(DashboardPanel(
        id="latency_graph",
        title="Latency Over Time",
        panel_type=PanelType.GRAPH,
        metric="cdc_event_latency",
        width=3,
        height=2,
        unit="ms",
    ))
    
    dashboard.add_row(latency_row)
    
    # Connections row
    connections_row = DashboardRow(title="Connections")
    
    connections_row.add_panel(DashboardPanel(
        id="active_connections",
        title="Active Connections",
        panel_type=PanelType.STAT,
        metric="cdc_active_connections",
        width=4,
        height=2,
    ))
    
    connections_row.add_panel(DashboardPanel(
        id="kafka_lag",
        title="Kafka Consumer Lag",
        panel_type=PanelType.GAUGE,
        metric="cdc_kafka_consumer_lag",
        width=4,
        height=2,
        unit="messages",
        thresholds=[
            Threshold(10000, "red", "Critical"),
            Threshold(1000, "yellow", "Warning"),
            Threshold(0, "green", "Good"),
        ],
    ))
    
    connections_row.add_panel(DashboardPanel(
        id="queue_size",
        title="Queue Size",
        panel_type=PanelType.GAUGE,
        metric="cdc_queue_size",
        width=4,
        height=2,
        unit="events",
    ))
    
    dashboard.add_row(connections_row)
    
    # Health row
    health_row = DashboardRow(title="Health")
    
    health_row.add_panel(DashboardPanel(
        id="health_grid",
        title="Component Health",
        panel_type=PanelType.HEALTH_GRID,
        width=6,
        height=3,
    ))
    
    health_row.add_panel(DashboardPanel(
        id="alert_list",
        title="Active Alerts",
        panel_type=PanelType.ALERT_LIST,
        width=6,
        height=3,
    ))
    
    dashboard.add_row(health_row)
    
    return dashboard


def create_simple_text_dashboard(
    metrics_data: Dict[str, Any],
    width: int = 80,
) -> str:
    """
    Create a simple text-based dashboard for terminal display.
    
    Args:
        metrics_data: Current metrics values
        width: Terminal width
        
    Returns:
        Text dashboard string
    """
    lines = []
    separator = "=" * width
    
    lines.append(separator)
    lines.append("CDC PIPELINE DASHBOARD".center(width))
    lines.append(f"Updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}".center(width))
    lines.append(separator)
    lines.append("")
    
    # Overview section
    lines.append("OVERVIEW")
    lines.append("-" * 40)
    
    events = metrics_data.get("events_processed_total", 0)
    errors = metrics_data.get("events_failed_total", 0)
    error_rate = (errors / events * 100) if events > 0 else 0
    
    lines.append(f"  Events Processed: {events:,}")
    lines.append(f"  Events Failed:    {errors:,}")
    lines.append(f"  Error Rate:       {error_rate:.2f}%")
    lines.append("")
    
    # Latency section
    lines.append("LATENCY")
    lines.append("-" * 40)
    
    latency = metrics_data.get("latency_statistics", {})
    lines.append(f"  p50:  {latency.get('p50', 0)*1000:.2f} ms")
    lines.append(f"  p95:  {latency.get('p95', 0)*1000:.2f} ms")
    lines.append(f"  p99:  {latency.get('p99', 0)*1000:.2f} ms")
    lines.append(f"  Max:  {latency.get('max', 0)*1000:.2f} ms")
    lines.append("")
    
    # Connections section
    lines.append("CONNECTIONS")
    lines.append("-" * 40)
    
    lines.append(f"  Active Connections: {metrics_data.get('active_connections', 0)}")
    lines.append(f"  Kafka Lag:          {metrics_data.get('kafka_consumer_lag', 0):,}")
    lines.append(f"  Queue Size:         {metrics_data.get('queue_size', 0):,}")
    lines.append("")
    
    lines.append(separator)
    
    return "\n".join(lines)
