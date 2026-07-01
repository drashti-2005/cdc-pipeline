"""
Alerting Module

Provides alerting infrastructure:
- Alert rules: Define conditions that trigger alerts
- Alert channels: Where to send alerts (log, webhook, email)
- Alert manager: Coordinate alerts and avoid spam

SIMPLE EXPLANATION:
Alerting is like a smoke detector for your system:
- Rules define what to watch for (smoke)
- Channels define how to notify (alarm sound)
- Manager prevents constant beeping (silencing)
"""

import json
import logging
import smtplib
import threading
import time
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from enum import Enum, auto
from typing import Dict, List, Optional, Any, Callable

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Alert severity levels."""
    
    INFO = auto()       # Informational
    WARNING = auto()    # Warning, may need attention
    CRITICAL = auto()   # Critical, needs immediate attention
    
    def __str__(self) -> str:
        return self.name
    
    def __lt__(self, other: "AlertLevel") -> bool:
        return self.value < other.value


class AlertState(Enum):
    """Alert state."""
    
    PENDING = auto()    # Condition met, waiting for threshold
    FIRING = auto()     # Alert is active
    RESOLVED = auto()   # Alert condition no longer met
    SILENCED = auto()   # Alert is silenced


@dataclass
class Alert:
    """
    An alert instance.
    
    Represents a specific alert that has been triggered.
    """
    
    name: str
    level: AlertLevel
    message: str
    state: AlertState = AlertState.FIRING
    
    # When
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    resolved_at: Optional[datetime] = None
    last_notified_at: Optional[datetime] = None
    
    # Context
    source: Optional[str] = None
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    value: Optional[float] = None
    
    # Tracking
    alert_id: Optional[str] = None
    notification_count: int = 0
    
    def __post_init__(self):
        if not self.alert_id:
            import secrets
            self.alert_id = f"alert-{secrets.token_hex(8)}"
    
    @property
    def duration(self) -> timedelta:
        """Get alert duration."""
        end = self.resolved_at or datetime.now(timezone.utc)
        return end - self.started_at
    
    def resolve(self) -> None:
        """Mark alert as resolved."""
        self.state = AlertState.RESOLVED
        self.resolved_at = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "alert_id": self.alert_id,
            "name": self.name,
            "level": str(self.level),
            "state": str(self.state.name),
            "message": self.message,
            "source": self.source,
            "labels": self.labels,
            "annotations": self.annotations,
            "value": self.value,
            "started_at": self.started_at.isoformat(),
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "duration_seconds": self.duration.total_seconds(),
        }


class AlertChannel(ABC):
    """
    Abstract alert notification channel.
    
    Implement this to add new notification methods.
    """
    
    @abstractmethod
    def send(self, alert: Alert) -> bool:
        """
        Send an alert notification.
        
        Args:
            alert: Alert to send
            
        Returns:
            True if sent successfully
        """
        pass
    
    @abstractmethod
    def send_resolved(self, alert: Alert) -> bool:
        """
        Send a resolution notification.
        
        Args:
            alert: Resolved alert
            
        Returns:
            True if sent successfully
        """
        pass


class LogAlertChannel(AlertChannel):
    """
    Log-based alert channel.
    
    Writes alerts to the application log.
    Good for development and as a fallback.
    """
    
    def __init__(self, log_level: int = logging.WARNING):
        """
        Initialize log channel.
        
        Args:
            log_level: Logging level to use
        """
        self.log_level = log_level
        self._logger = logging.getLogger("alerts")
    
    def send(self, alert: Alert) -> bool:
        """Log the alert."""
        level_map = {
            AlertLevel.INFO: logging.INFO,
            AlertLevel.WARNING: logging.WARNING,
            AlertLevel.CRITICAL: logging.CRITICAL,
        }
        
        log_level = level_map.get(alert.level, logging.WARNING)
        
        self._logger.log(
            log_level,
            f"[{alert.level}] {alert.name}: {alert.message}"
            + (f" (value={alert.value})" if alert.value is not None else "")
        )
        return True
    
    def send_resolved(self, alert: Alert) -> bool:
        """Log the resolution."""
        self._logger.info(
            f"[RESOLVED] {alert.name}: {alert.message} "
            f"(duration={alert.duration})"
        )
        return True


class WebhookAlertChannel(AlertChannel):
    """
    Webhook-based alert channel.
    
    Sends alerts to a webhook URL (Slack, Teams, PagerDuty, etc.).
    
    USAGE:
        channel = WebhookAlertChannel(
            url="https://hooks.slack.com/services/...",
            headers={"Content-Type": "application/json"}
        )
    """
    
    def __init__(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
        payload_formatter: Optional[Callable[[Alert], Dict[str, Any]]] = None,
    ):
        """
        Initialize webhook channel.
        
        Args:
            url: Webhook URL
            headers: HTTP headers
            timeout: Request timeout in seconds
            payload_formatter: Custom payload formatter
        """
        self.url = url
        self.headers = headers or {"Content-Type": "application/json"}
        self.timeout = timeout
        self.payload_formatter = payload_formatter or self._default_format
    
    def _default_format(self, alert: Alert) -> Dict[str, Any]:
        """Default payload format."""
        return alert.to_dict()
    
    def _send_request(self, payload: Dict[str, Any]) -> bool:
        """Send HTTP request."""
        try:
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                self.url,
                data=data,
                headers=self.headers,
                method='POST'
            )
            
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                return response.status == 200
        
        except Exception as e:
            logger.error(f"Webhook send failed: {e}")
            return False
    
    def send(self, alert: Alert) -> bool:
        """Send alert to webhook."""
        payload = self.payload_formatter(alert)
        payload["action"] = "firing"
        return self._send_request(payload)
    
    def send_resolved(self, alert: Alert) -> bool:
        """Send resolution to webhook."""
        payload = self.payload_formatter(alert)
        payload["action"] = "resolved"
        return self._send_request(payload)


class EmailAlertChannel(AlertChannel):
    """
    Email-based alert channel.
    
    Sends alerts via SMTP.
    
    USAGE:
        channel = EmailAlertChannel(
            smtp_host="smtp.gmail.com",
            smtp_port=587,
            username="alerts@example.com",
            password="...",
            from_addr="alerts@example.com",
            to_addrs=["ops@example.com"]
        )
    """
    
    def __init__(
        self,
        smtp_host: str,
        smtp_port: int = 587,
        username: Optional[str] = None,
        password: Optional[str] = None,
        from_addr: str = "",
        to_addrs: Optional[List[str]] = None,
        use_tls: bool = True,
    ):
        """
        Initialize email channel.
        
        Args:
            smtp_host: SMTP server host
            smtp_port: SMTP server port
            username: SMTP username
            password: SMTP password
            from_addr: From email address
            to_addrs: List of recipient addresses
            use_tls: Use TLS encryption
        """
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.from_addr = from_addr
        self.to_addrs = to_addrs or []
        self.use_tls = use_tls
    
    def _send_email(self, subject: str, body: str) -> bool:
        """Send email."""
        try:
            msg = MIMEText(body)
            msg['Subject'] = subject
            msg['From'] = self.from_addr
            msg['To'] = ', '.join(self.to_addrs)
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                
                if self.username and self.password:
                    server.login(self.username, self.password)
                
                server.send_message(msg)
            
            return True
        
        except Exception as e:
            logger.error(f"Email send failed: {e}")
            return False
    
    def send(self, alert: Alert) -> bool:
        """Send alert email."""
        subject = f"[{alert.level}] Alert: {alert.name}"
        body = f"""
Alert: {alert.name}
Level: {alert.level}
State: FIRING
Message: {alert.message}
Started: {alert.started_at}
Value: {alert.value}
Source: {alert.source}

Labels: {json.dumps(alert.labels, indent=2)}
        """.strip()
        
        return self._send_email(subject, body)
    
    def send_resolved(self, alert: Alert) -> bool:
        """Send resolution email."""
        subject = f"[RESOLVED] Alert: {alert.name}"
        body = f"""
Alert: {alert.name}
State: RESOLVED
Message: {alert.message}
Duration: {alert.duration}
        """.strip()
        
        return self._send_email(subject, body)


@dataclass
class AlertRule:
    """
    Defines when an alert should be triggered.
    
    SIMPLE EXPLANATION:
    An alert rule is like a "if this, then that":
    - Condition: What to check (CPU > 90%)
    - Duration: How long before alerting (5 minutes)
    - Level: How serious is it (WARNING, CRITICAL)
    
    USAGE:
        rule = AlertRule(
            name="high_cpu",
            condition=lambda metrics: metrics.get("cpu") > 90,
            level=AlertLevel.WARNING,
            message="CPU usage is high",
            for_duration=timedelta(minutes=5)
        )
    """
    
    name: str
    condition: Callable[[Dict[str, Any]], bool]
    level: AlertLevel = AlertLevel.WARNING
    message: str = ""
    
    # Thresholds
    for_duration: timedelta = timedelta(seconds=0)  # Must be true for this long
    
    # Notification control
    repeat_interval: timedelta = timedelta(hours=4)  # Repeat notifications
    
    # Metadata
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    enabled: bool = True
    
    # State
    _pending_since: Optional[datetime] = field(default=None, repr=False)
    _current_alert: Optional[Alert] = field(default=None, repr=False)
    
    def evaluate(self, metrics: Dict[str, Any]) -> Optional[Alert]:
        """
        Evaluate the rule against metrics.
        
        Args:
            metrics: Current metric values
            
        Returns:
            Alert if condition is met, None otherwise
        """
        if not self.enabled:
            return None
        
        try:
            condition_met = self.condition(metrics)
        except Exception as e:
            logger.warning(f"Rule {self.name} evaluation failed: {e}")
            return None
        
        now = datetime.now(timezone.utc)
        
        if condition_met:
            # Condition is met
            if self._pending_since is None:
                self._pending_since = now
            
            # Check if condition has been true long enough
            duration = now - self._pending_since
            
            if duration >= self.for_duration:
                if self._current_alert is None:
                    # Create new alert
                    self._current_alert = Alert(
                        name=self.name,
                        level=self.level,
                        message=self.message,
                        labels=self.labels.copy(),
                        annotations=self.annotations.copy(),
                        started_at=self._pending_since,
                    )
                
                return self._current_alert
        else:
            # Condition not met - resolve if we have an alert
            if self._current_alert is not None:
                self._current_alert.resolve()
                resolved = self._current_alert
                self._current_alert = None
                self._pending_since = None
                return resolved
            
            self._pending_since = None
        
        return None
    
    def should_notify(self) -> bool:
        """Check if we should send a notification."""
        if self._current_alert is None:
            return False
        
        alert = self._current_alert
        
        # First notification
        if alert.last_notified_at is None:
            return True
        
        # Repeat interval
        since_last = datetime.now(timezone.utc) - alert.last_notified_at
        return since_last >= self.repeat_interval


@dataclass
class Silence:
    """
    Silence period for alerts.
    
    Suppresses notifications for matching alerts during a time period.
    """
    
    matchers: Dict[str, str]  # Label matchers
    starts_at: datetime
    ends_at: datetime
    comment: str = ""
    created_by: str = ""
    
    def matches(self, alert: Alert) -> bool:
        """Check if silence matches an alert."""
        now = datetime.now(timezone.utc)
        
        # Check time range
        if now < self.starts_at or now > self.ends_at:
            return False
        
        # Check label matchers
        for key, value in self.matchers.items():
            if key == "alertname":
                if alert.name != value:
                    return False
            elif alert.labels.get(key) != value:
                return False
        
        return True


class AlertManager:
    """
    Manages alerts and notifications.
    
    Coordinates alert evaluation, notification, and silencing.
    
    SIMPLE EXPLANATION:
    The AlertManager is like a control center:
    - Watches for problems (evaluates rules)
    - Sends notifications (through channels)
    - Prevents spam (grouping and silencing)
    
    USAGE:
        manager = AlertManager()
        
        # Add channels
        manager.add_channel(LogAlertChannel())
        manager.add_channel(WebhookAlertChannel(url="..."))
        
        # Add rules
        manager.add_rule(AlertRule(
            name="high_latency",
            condition=lambda m: m.get("p99_latency", 0) > 1.0,
            level=AlertLevel.WARNING,
            message="Request latency is high"
        ))
        
        # Evaluate periodically
        while True:
            metrics = collect_metrics()
            manager.evaluate(metrics)
            time.sleep(30)
    """
    
    def __init__(
        self,
        group_wait: timedelta = timedelta(seconds=30),
        group_interval: timedelta = timedelta(minutes=5),
    ):
        """
        Initialize alert manager.
        
        Args:
            group_wait: Wait before sending first notification
            group_interval: Interval between grouped notifications
        """
        self.group_wait = group_wait
        self.group_interval = group_interval
        
        self._rules: Dict[str, AlertRule] = {}
        self._channels: List[AlertChannel] = []
        self._silences: List[Silence] = []
        self._active_alerts: Dict[str, Alert] = {}
        self._lock = threading.Lock()
    
    def add_rule(self, rule: AlertRule) -> None:
        """Add an alert rule."""
        with self._lock:
            self._rules[rule.name] = rule
        logger.info(f"Added alert rule: {rule.name}")
    
    def remove_rule(self, name: str) -> bool:
        """Remove an alert rule."""
        with self._lock:
            if name in self._rules:
                del self._rules[name]
                logger.info(f"Removed alert rule: {name}")
                return True
        return False
    
    def add_channel(self, channel: AlertChannel) -> None:
        """Add a notification channel."""
        with self._lock:
            self._channels.append(channel)
        logger.info(f"Added alert channel: {type(channel).__name__}")
    
    def add_silence(
        self,
        matchers: Dict[str, str],
        duration: timedelta,
        comment: str = "",
        created_by: str = "",
    ) -> Silence:
        """
        Add a silence.
        
        Args:
            matchers: Label matchers
            duration: Silence duration
            comment: Reason for silencing
            created_by: Who created the silence
            
        Returns:
            Created Silence object
        """
        now = datetime.now(timezone.utc)
        silence = Silence(
            matchers=matchers,
            starts_at=now,
            ends_at=now + duration,
            comment=comment,
            created_by=created_by,
        )
        
        with self._lock:
            self._silences.append(silence)
        
        logger.info(f"Added silence for {matchers} until {silence.ends_at}")
        return silence
    
    def _is_silenced(self, alert: Alert) -> bool:
        """Check if alert is silenced."""
        for silence in self._silences:
            if silence.matches(alert):
                return True
        return False
    
    def _cleanup_silences(self) -> None:
        """Remove expired silences."""
        now = datetime.now(timezone.utc)
        self._silences = [s for s in self._silences if s.ends_at > now]
    
    def evaluate(self, metrics: Dict[str, Any]) -> List[Alert]:
        """
        Evaluate all rules against metrics.
        
        Args:
            metrics: Current metric values
            
        Returns:
            List of alerts that changed state
        """
        changed_alerts = []
        
        with self._lock:
            self._cleanup_silences()
            
            for name, rule in self._rules.items():
                alert = rule.evaluate(metrics)
                
                if alert is None:
                    continue
                
                # Handle state changes
                if alert.state == AlertState.FIRING:
                    if alert.alert_id not in self._active_alerts:
                        # New alert
                        self._active_alerts[alert.alert_id] = alert
                        changed_alerts.append(alert)
                        
                        if not self._is_silenced(alert):
                            self._notify(alert)
                    
                    elif rule.should_notify():
                        # Repeat notification
                        if not self._is_silenced(alert):
                            self._notify(alert)
                
                elif alert.state == AlertState.RESOLVED:
                    if alert.alert_id in self._active_alerts:
                        del self._active_alerts[alert.alert_id]
                        changed_alerts.append(alert)
                        
                        if not self._is_silenced(alert):
                            self._notify_resolved(alert)
        
        return changed_alerts
    
    def _notify(self, alert: Alert) -> None:
        """Send alert notification."""
        for channel in self._channels:
            try:
                channel.send(alert)
            except Exception as e:
                logger.error(f"Failed to send alert via {type(channel).__name__}: {e}")
        
        alert.last_notified_at = datetime.now(timezone.utc)
        alert.notification_count += 1
    
    def _notify_resolved(self, alert: Alert) -> None:
        """Send resolution notification."""
        for channel in self._channels:
            try:
                channel.send_resolved(alert)
            except Exception as e:
                logger.error(f"Failed to send resolution via {type(channel).__name__}: {e}")
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts."""
        with self._lock:
            return list(self._active_alerts.values())
    
    def get_alert_count(self, level: Optional[AlertLevel] = None) -> int:
        """Get count of active alerts."""
        with self._lock:
            if level is None:
                return len(self._active_alerts)
            return sum(
                1 for a in self._active_alerts.values()
                if a.level == level
            )
    
    def get_rules(self) -> List[AlertRule]:
        """Get all rules."""
        with self._lock:
            return list(self._rules.values())
    
    def get_silences(self) -> List[Silence]:
        """Get active silences."""
        with self._lock:
            self._cleanup_silences()
            return list(self._silences)


def create_pipeline_alert_rules() -> List[AlertRule]:
    """
    Create standard CDC pipeline alert rules.
    
    Returns:
        List of AlertRule objects
    """
    return [
        AlertRule(
            name="high_replication_lag",
            condition=lambda m: m.get("replication_lag_seconds", 0) > 60,
            level=AlertLevel.WARNING,
            message="Replication lag exceeds 60 seconds",
            for_duration=timedelta(minutes=5),
            labels={"category": "performance"},
        ),
        AlertRule(
            name="critical_replication_lag",
            condition=lambda m: m.get("replication_lag_seconds", 0) > 300,
            level=AlertLevel.CRITICAL,
            message="Replication lag exceeds 5 minutes",
            for_duration=timedelta(minutes=2),
            labels={"category": "performance"},
        ),
        AlertRule(
            name="high_error_rate",
            condition=lambda m: m.get("error_rate", 0) > 0.05,
            level=AlertLevel.WARNING,
            message="Error rate exceeds 5%",
            for_duration=timedelta(minutes=5),
            labels={"category": "reliability"},
        ),
        AlertRule(
            name="kafka_consumer_lag",
            condition=lambda m: m.get("kafka_consumer_lag", 0) > 10000,
            level=AlertLevel.WARNING,
            message="Kafka consumer lag exceeds 10,000 messages",
            for_duration=timedelta(minutes=10),
            labels={"category": "backpressure"},
        ),
        AlertRule(
            name="database_connection_errors",
            condition=lambda m: m.get("db_connection_errors", 0) > 10,
            level=AlertLevel.CRITICAL,
            message="Multiple database connection failures",
            for_duration=timedelta(minutes=2),
            labels={"category": "connectivity"},
        ),
        AlertRule(
            name="low_throughput",
            condition=lambda m: m.get("events_per_second", float("inf")) < 10,
            level=AlertLevel.WARNING,
            message="Event throughput is unusually low",
            for_duration=timedelta(minutes=15),
            labels={"category": "performance"},
        ),
        AlertRule(
            name="high_memory_usage",
            condition=lambda m: m.get("memory_usage_percent", 0) > 90,
            level=AlertLevel.WARNING,
            message="Memory usage exceeds 90%",
            for_duration=timedelta(minutes=5),
            labels={"category": "resources"},
        ),
    ]
