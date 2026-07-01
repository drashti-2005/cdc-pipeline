"""
Failover and Health Monitoring

Provides automatic failover between regions:
- Health checking: Monitor region availability
- Failover triggers: Detect failures and initiate failover
- Failback: Return to primary when recovered

SIMPLE EXPLANATION:
Failover is like having a backup generator:
- Monitor if main power is working
- If it fails, switch to backup automatically
- When main power returns, switch back
"""

import asyncio
import logging
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Callable, Set

from src.multiregion.config import (
    Region,
    RegionConfig,
    MultiRegionConfig,
    RegionStatus,
)

logger = logging.getLogger(__name__)


class FailoverStrategy(Enum):
    """
    Strategy for handling failover.
    
    AUTOMATIC: Automatic failover when health check fails
    MANUAL: Require manual intervention
    SEMI_AUTOMATIC: Auto failover, manual failback
    """
    
    AUTOMATIC = "automatic"
    MANUAL = "manual"
    SEMI_AUTOMATIC = "semi_automatic"


class HealthStatus(Enum):
    """Health status of a region."""
    
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class RegionHealth:
    """
    Health information for a region.
    
    Tracks current health status and history.
    """
    
    region_name: str
    status: HealthStatus
    last_check: datetime
    latency_ms: Optional[float] = None
    error_rate: float = 0.0
    consecutive_failures: int = 0
    last_error: Optional[str] = None
    checks_performed: int = 0
    checks_passed: int = 0
    
    @property
    def success_rate(self) -> float:
        """Get health check success rate."""
        if self.checks_performed == 0:
            return 0.0
        return self.checks_passed / self.checks_performed
    
    @property
    def is_healthy(self) -> bool:
        """Check if region is healthy."""
        return self.status == HealthStatus.HEALTHY
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "region_name": self.region_name,
            "status": self.status.value,
            "last_check": self.last_check.isoformat(),
            "latency_ms": self.latency_ms,
            "error_rate": self.error_rate,
            "consecutive_failures": self.consecutive_failures,
            "last_error": self.last_error,
            "success_rate": self.success_rate,
        }


@dataclass
class FailoverEvent:
    """
    Record of a failover event.
    
    Tracks when and why failover occurred.
    """
    
    event_id: str
    event_type: str  # "failover", "failback", "manual"
    from_region: str
    to_region: str
    reason: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    duration_ms: Optional[float] = None
    success: bool = True
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "from_region": self.from_region,
            "to_region": self.to_region,
            "reason": self.reason,
            "timestamp": self.timestamp.isoformat(),
            "duration_ms": self.duration_ms,
            "success": self.success,
            "error": self.error,
        }


class HealthChecker:
    """
    Health checker for regions.
    
    Performs periodic health checks and maintains status.
    
    USAGE:
        checker = HealthChecker(config)
        checker.add_check("kafka", kafka_health_check)
        checker.add_check("database", db_health_check)
        checker.start()
        
        health = checker.get_health("us-east-1")
        print(f"Status: {health.status.value}")
    """
    
    def __init__(
        self,
        config: MultiRegionConfig,
        check_interval_seconds: float = 10.0,
        failure_threshold: int = 3,
        recovery_threshold: int = 2,
    ):
        """
        Initialize health checker.
        
        Args:
            config: Multi-region configuration
            check_interval_seconds: Interval between checks
            failure_threshold: Consecutive failures to mark unhealthy
            recovery_threshold: Consecutive successes to mark recovered
        """
        self.config = config
        self.check_interval = check_interval_seconds
        self.failure_threshold = failure_threshold
        self.recovery_threshold = recovery_threshold
        
        # Health check functions
        self._checks: Dict[str, Callable[[RegionConfig], bool]] = {}
        
        # Health status
        self._health: Dict[str, RegionHealth] = {}
        self._consecutive_successes: Dict[str, int] = {}
        
        # Threading
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        
        # Callbacks
        self._on_healthy: List[Callable[[str], None]] = []
        self._on_unhealthy: List[Callable[[str], None]] = []
        
        # Initialize health for all regions
        for name in config.region_names:
            self._health[name] = RegionHealth(
                region_name=name,
                status=HealthStatus.UNKNOWN,
                last_check=datetime.now(timezone.utc),
            )
    
    def add_check(
        self,
        name: str,
        check_fn: Callable[[RegionConfig], bool],
    ) -> None:
        """
        Add a health check function.
        
        Args:
            name: Check name (e.g., "kafka", "database")
            check_fn: Function that returns True if healthy
        """
        self._checks[name] = check_fn
        logger.info(f"Added health check: {name}")
    
    def on_healthy(self, callback: Callable[[str], None]) -> None:
        """Register callback for when region becomes healthy."""
        self._on_healthy.append(callback)
    
    def on_unhealthy(self, callback: Callable[[str], None]) -> None:
        """Register callback for when region becomes unhealthy."""
        self._on_unhealthy.append(callback)
    
    def start(self) -> None:
        """Start health checking."""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._check_loop, daemon=True)
        self._thread.start()
        logger.info("Health checker started")
    
    def stop(self) -> None:
        """Stop health checking."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None
        logger.info("Health checker stopped")
    
    def _check_loop(self) -> None:
        """Main health check loop."""
        while self._running:
            try:
                self._perform_checks()
            except Exception as e:
                logger.error(f"Health check error: {e}")
            
            time.sleep(self.check_interval)
    
    def _perform_checks(self) -> None:
        """Perform health checks on all regions."""
        for name, region_config in self.config.regions.items():
            self._check_region(region_config)
    
    def _check_region(self, region_config: RegionConfig) -> None:
        """Check health of a single region."""
        name = region_config.region.name
        start = time.time()
        
        with self._lock:
            health = self._health.get(name)
            if not health:
                health = RegionHealth(
                    region_name=name,
                    status=HealthStatus.UNKNOWN,
                    last_check=datetime.now(timezone.utc),
                )
                self._health[name] = health
        
        # Run all checks
        all_passed = True
        errors = []
        
        if not self._checks:
            # No checks configured, do simple connectivity check
            all_passed = region_config.region.status == RegionStatus.ACTIVE
        else:
            for check_name, check_fn in self._checks.items():
                try:
                    if not check_fn(region_config):
                        all_passed = False
                        errors.append(f"{check_name}: failed")
                except Exception as e:
                    all_passed = False
                    errors.append(f"{check_name}: {str(e)}")
        
        latency = (time.time() - start) * 1000
        
        # Update health status
        with self._lock:
            health.checks_performed += 1
            health.last_check = datetime.now(timezone.utc)
            health.latency_ms = latency
            
            old_status = health.status
            
            if all_passed:
                health.checks_passed += 1
                health.consecutive_failures = 0
                self._consecutive_successes[name] = \
                    self._consecutive_successes.get(name, 0) + 1
                
                if self._consecutive_successes[name] >= self.recovery_threshold:
                    health.status = HealthStatus.HEALTHY
                    health.error_rate = 0.0
            else:
                health.consecutive_failures += 1
                self._consecutive_successes[name] = 0
                health.last_error = "; ".join(errors)
                
                if health.consecutive_failures >= self.failure_threshold:
                    health.status = HealthStatus.UNHEALTHY
                else:
                    health.status = HealthStatus.DEGRADED
                
                health.error_rate = 1.0 - health.success_rate
            
            # Trigger callbacks on status change
            if old_status != health.status:
                if health.status == HealthStatus.HEALTHY:
                    for cb in self._on_healthy:
                        try:
                            cb(name)
                        except Exception as e:
                            logger.error(f"Healthy callback error: {e}")
                elif health.status == HealthStatus.UNHEALTHY:
                    for cb in self._on_unhealthy:
                        try:
                            cb(name)
                        except Exception as e:
                            logger.error(f"Unhealthy callback error: {e}")
    
    def check_now(self, region_name: str) -> RegionHealth:
        """Force immediate health check for a region."""
        region_config = self.config.get_region(region_name)
        if not region_config:
            raise ValueError(f"Region not found: {region_name}")
        
        self._check_region(region_config)
        return self._health[region_name]
    
    def get_health(self, region_name: str) -> Optional[RegionHealth]:
        """Get health status for a region."""
        with self._lock:
            return self._health.get(region_name)
    
    def get_all_health(self) -> Dict[str, RegionHealth]:
        """Get health status for all regions."""
        with self._lock:
            return dict(self._health)
    
    def get_healthy_regions(self) -> List[str]:
        """Get list of healthy region names."""
        with self._lock:
            return [
                name for name, health in self._health.items()
                if health.status == HealthStatus.HEALTHY
            ]
    
    def is_healthy(self, region_name: str) -> bool:
        """Check if a region is healthy."""
        health = self.get_health(region_name)
        return health and health.status == HealthStatus.HEALTHY


class FailoverManager:
    """
    Manages automatic failover between regions.
    
    SIMPLE EXPLANATION:
    This is the "decision maker" that:
    - Watches health of all regions
    - Decides when to failover
    - Coordinates the switch to backup
    - Handles failback when primary recovers
    
    USAGE:
        manager = FailoverManager(config, strategy=FailoverStrategy.AUTOMATIC)
        manager.start()
        
        # Get current active region
        active = manager.get_active_region()
        
        # Manual failover (if needed)
        manager.failover_to("eu-west-1", reason="Manual maintenance")
        
        # Stop
        manager.stop()
    """
    
    def __init__(
        self,
        config: MultiRegionConfig,
        strategy: FailoverStrategy = FailoverStrategy.AUTOMATIC,
        health_checker: Optional[HealthChecker] = None,
        failover_timeout_seconds: float = 30.0,
        failback_delay_seconds: float = 60.0,
    ):
        """
        Initialize failover manager.
        
        Args:
            config: Multi-region configuration
            strategy: Failover strategy
            health_checker: Health checker instance (creates one if not provided)
            failover_timeout_seconds: Timeout for failover operations
            failback_delay_seconds: Delay before automatic failback
        """
        self.config = config
        self.strategy = strategy
        self.failover_timeout = failover_timeout_seconds
        self.failback_delay = failback_delay_seconds
        
        # Health checker
        self.health_checker = health_checker or HealthChecker(config)
        
        # State
        self._active_region: Optional[str] = None
        self._original_primary: Optional[str] = None
        self._in_failover = False
        self._failback_scheduled: Optional[datetime] = None
        
        # Events
        self._events: List[FailoverEvent] = []
        self._event_counter = 0
        self._lock = threading.Lock()
        
        # Callbacks
        self._on_failover: List[Callable[[FailoverEvent], None]] = []
        self._on_failback: List[Callable[[FailoverEvent], None]] = []
        
        # Initialize active region
        primary = config.get_primary()
        if primary:
            self._active_region = primary.region.name
            self._original_primary = primary.region.name
        
        # Register health callbacks
        if strategy == FailoverStrategy.AUTOMATIC:
            self.health_checker.on_unhealthy(self._handle_unhealthy)
            self.health_checker.on_healthy(self._handle_healthy)
        
        logger.info(f"FailoverManager initialized: strategy={strategy.value}, active={self._active_region}")
    
    def _generate_event_id(self) -> str:
        """Generate unique event ID."""
        with self._lock:
            self._event_counter += 1
            return f"failover-{self._event_counter:04d}"
    
    def on_failover(self, callback: Callable[[FailoverEvent], None]) -> None:
        """Register callback for failover events."""
        self._on_failover.append(callback)
    
    def on_failback(self, callback: Callable[[FailoverEvent], None]) -> None:
        """Register callback for failback events."""
        self._on_failback.append(callback)
    
    def start(self) -> None:
        """Start failover manager."""
        self.health_checker.start()
        logger.info("Failover manager started")
    
    def stop(self) -> None:
        """Stop failover manager."""
        self.health_checker.stop()
        logger.info("Failover manager stopped")
    
    def get_active_region(self) -> Optional[RegionConfig]:
        """Get currently active region."""
        if not self._active_region:
            return None
        return self.config.get_region(self._active_region)
    
    def get_active_region_name(self) -> Optional[str]:
        """Get name of currently active region."""
        return self._active_region
    
    def is_in_failover(self) -> bool:
        """Check if currently in failover state."""
        return self._in_failover
    
    def _handle_unhealthy(self, region_name: str) -> None:
        """Handle region becoming unhealthy."""
        if region_name != self._active_region:
            logger.info(f"Non-active region {region_name} became unhealthy, ignoring")
            return
        
        logger.warning(f"Active region {region_name} became unhealthy, initiating failover")
        
        # Find best failover target
        healthy = self.health_checker.get_healthy_regions()
        targets = [
            self.config.get_region(name) for name in healthy
            if name != region_name
        ]
        
        if not targets:
            logger.error("No healthy regions available for failover!")
            return
        
        # Sort by priority
        targets.sort(key=lambda r: r.region.priority)
        target = targets[0]
        
        self.failover_to(
            target.region.name,
            reason=f"Automatic failover: {region_name} unhealthy",
        )
    
    def _handle_healthy(self, region_name: str) -> None:
        """Handle region becoming healthy."""
        if region_name != self._original_primary:
            return
        
        if self._active_region == self._original_primary:
            return
        
        logger.info(f"Primary region {region_name} recovered")
        
        if self.strategy == FailoverStrategy.AUTOMATIC:
            # Schedule failback after delay
            self._failback_scheduled = datetime.now(timezone.utc) + \
                timedelta(seconds=self.failback_delay)
            logger.info(f"Failback scheduled for {self._failback_scheduled}")
            
            # Start failback timer
            timer = threading.Timer(
                self.failback_delay,
                self._attempt_failback,
            )
            timer.daemon = True
            timer.start()
    
    def _attempt_failback(self) -> None:
        """Attempt automatic failback to primary."""
        if not self._original_primary:
            return
        
        if self._active_region == self._original_primary:
            return
        
        # Verify primary is still healthy
        if not self.health_checker.is_healthy(self._original_primary):
            logger.warning(f"Primary {self._original_primary} not healthy, canceling failback")
            self._failback_scheduled = None
            return
        
        self.failback(reason="Automatic failback: primary recovered")
    
    def failover_to(
        self,
        target_region: str,
        reason: str = "Manual failover",
    ) -> FailoverEvent:
        """
        Failover to specified region.
        
        Args:
            target_region: Target region name
            reason: Reason for failover
            
        Returns:
            FailoverEvent recording the failover
        """
        if self._in_failover:
            raise RuntimeError("Failover already in progress")
        
        target = self.config.get_region(target_region)
        if not target:
            raise ValueError(f"Target region not found: {target_region}")
        
        from_region = self._active_region or "none"
        
        logger.warning(f"Initiating failover: {from_region} → {target_region}")
        
        start = time.time()
        self._in_failover = True
        
        event = FailoverEvent(
            event_id=self._generate_event_id(),
            event_type="failover",
            from_region=from_region,
            to_region=target_region,
            reason=reason,
        )
        
        try:
            # Update active region
            old_active = self._active_region
            self._active_region = target_region
            
            # Update region statuses
            if old_active:
                old_config = self.config.get_region(old_active)
                if old_config:
                    old_config.region.status = RegionStatus.INACTIVE
            
            target.region.status = RegionStatus.ACTIVE
            
            event.success = True
            event.duration_ms = (time.time() - start) * 1000
            
            logger.warning(
                f"Failover complete: {from_region} → {target_region} "
                f"({event.duration_ms:.0f}ms)"
            )
            
            # Trigger callbacks
            for cb in self._on_failover:
                try:
                    cb(event)
                except Exception as e:
                    logger.error(f"Failover callback error: {e}")
                    
        except Exception as e:
            event.success = False
            event.error = str(e)
            logger.error(f"Failover failed: {e}")
            raise
        finally:
            self._in_failover = False
            self._events.append(event)
        
        return event
    
    def failback(self, reason: str = "Manual failback") -> FailoverEvent:
        """
        Failback to original primary region.
        
        Args:
            reason: Reason for failback
            
        Returns:
            FailoverEvent recording the failback
        """
        if not self._original_primary:
            raise RuntimeError("No original primary to failback to")
        
        if self._active_region == self._original_primary:
            raise RuntimeError("Already on primary region")
        
        from_region = self._active_region or "none"
        
        logger.info(f"Initiating failback: {from_region} → {self._original_primary}")
        
        start = time.time()
        
        event = FailoverEvent(
            event_id=self._generate_event_id(),
            event_type="failback",
            from_region=from_region,
            to_region=self._original_primary,
            reason=reason,
        )
        
        try:
            # Update active region
            old_active = self._active_region
            self._active_region = self._original_primary
            
            # Update region statuses
            if old_active:
                old_config = self.config.get_region(old_active)
                if old_config:
                    old_config.region.status = RegionStatus.ACTIVE  # Keep as active replica
            
            primary = self.config.get_region(self._original_primary)
            if primary:
                primary.region.status = RegionStatus.ACTIVE
            
            event.success = True
            event.duration_ms = (time.time() - start) * 1000
            
            logger.info(
                f"Failback complete: {from_region} → {self._original_primary} "
                f"({event.duration_ms:.0f}ms)"
            )
            
            # Clear failback schedule
            self._failback_scheduled = None
            
            # Trigger callbacks
            for cb in self._on_failback:
                try:
                    cb(event)
                except Exception as e:
                    logger.error(f"Failback callback error: {e}")
                    
        except Exception as e:
            event.success = False
            event.error = str(e)
            logger.error(f"Failback failed: {e}")
            raise
        finally:
            self._events.append(event)
        
        return event
    
    def get_events(self, limit: int = 100) -> List[FailoverEvent]:
        """Get recent failover events."""
        return self._events[-limit:]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get failover statistics."""
        return {
            "strategy": self.strategy.value,
            "active_region": self._active_region,
            "original_primary": self._original_primary,
            "in_failover": self._in_failover,
            "failback_scheduled": self._failback_scheduled.isoformat() if self._failback_scheduled else None,
            "total_events": len(self._events),
            "health": {
                name: health.to_dict()
                for name, health in self.health_checker.get_all_health().items()
            },
        }


# Common health check functions
def create_http_health_check(
    health_endpoint: str,
    timeout_seconds: float = 5.0,
) -> Callable[[RegionConfig], bool]:
    """
    Create HTTP health check function.
    
    Args:
        health_endpoint: Health endpoint path (e.g., "/health")
        timeout_seconds: Request timeout
        
    Returns:
        Health check function
    """
    import urllib.request
    import urllib.error
    
    def check(region_config: RegionConfig) -> bool:
        # Build URL from region config
        # This is a simplified example
        url = f"http://{region_config.kafka.bootstrap_servers.split(',')[0]}{health_endpoint}"
        
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
                return response.status == 200
        except Exception:
            return False
    
    return check


def create_tcp_health_check(
    port: int,
    timeout_seconds: float = 5.0,
) -> Callable[[RegionConfig], bool]:
    """
    Create TCP connectivity health check.
    
    Args:
        port: Port to check
        timeout_seconds: Connection timeout
        
    Returns:
        Health check function
    """
    import socket
    
    def check(region_config: RegionConfig) -> bool:
        host = region_config.kafka.bootstrap_servers.split(",")[0].split(":")[0]
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout_seconds)
            result = sock.connect_ex((host, port))
            sock.close()
            return result == 0
        except Exception:
            return False
    
    return check
