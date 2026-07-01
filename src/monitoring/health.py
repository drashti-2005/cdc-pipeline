"""
Health Check Module

Provides health monitoring for system components:
- TCP connectivity checks
- HTTP endpoint checks
- Database connection checks
- Kafka broker checks

SIMPLE EXPLANATION:
Health checks are like a doctor's checkup:
- Check heartbeat (is it running?)
- Check vital signs (is it healthy?)
- Report status (healthy, degraded, unhealthy)
"""

import asyncio
import logging
import socket
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
from typing import Dict, List, Optional, Any, Callable
from urllib.request import urlopen
from urllib.error import URLError

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health status levels."""
    
    HEALTHY = auto()    # Everything is working
    DEGRADED = auto()   # Some issues but functional
    UNHEALTHY = auto()  # Not working
    UNKNOWN = auto()    # Cannot determine status
    
    def __str__(self) -> str:
        return self.name
    
    @property
    def is_ok(self) -> bool:
        """Check if status is acceptable."""
        return self in (HealthStatus.HEALTHY, HealthStatus.DEGRADED)


@dataclass
class HealthResult:
    """
    Result of a health check.
    
    Contains status, timing, and optional details.
    """
    
    status: HealthStatus
    message: str = ""
    latency_ms: float = 0.0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "status": str(self.status),
            "message": self.message,
            "latency_ms": self.latency_ms,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details,
        }


class HealthCheck(ABC):
    """
    Abstract health check.
    
    Implement this to add new health check types.
    """
    
    def __init__(
        self,
        name: str,
        timeout: float = 5.0,
        critical: bool = True,
    ):
        """
        Initialize health check.
        
        Args:
            name: Check name
            timeout: Check timeout in seconds
            critical: If True, failure affects overall health
        """
        self.name = name
        self.timeout = timeout
        self.critical = critical
        
        # History
        self._last_result: Optional[HealthResult] = None
        self._consecutive_failures: int = 0
    
    @abstractmethod
    def check(self) -> HealthResult:
        """
        Perform the health check.
        
        Returns:
            HealthResult with status and details
        """
        pass
    
    def run(self) -> HealthResult:
        """Run the check with timing."""
        start = time.perf_counter()
        
        try:
            result = self.check()
            result.latency_ms = (time.perf_counter() - start) * 1000
            
            if result.status == HealthStatus.HEALTHY:
                self._consecutive_failures = 0
            else:
                self._consecutive_failures += 1
            
        except Exception as e:
            result = HealthResult(
                status=HealthStatus.UNHEALTHY,
                message=str(e),
                latency_ms=(time.perf_counter() - start) * 1000,
            )
            self._consecutive_failures += 1
        
        self._last_result = result
        return result
    
    @property
    def last_result(self) -> Optional[HealthResult]:
        """Get last check result."""
        return self._last_result
    
    @property
    def consecutive_failures(self) -> int:
        """Get number of consecutive failures."""
        return self._consecutive_failures


class TCPHealthCheck(HealthCheck):
    """
    TCP connectivity health check.
    
    Checks if a TCP port is accepting connections.
    
    USAGE:
        check = TCPHealthCheck("postgres", "localhost", 5432)
        result = check.run()
        print(f"Status: {result.status}")
    """
    
    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        timeout: float = 5.0,
        critical: bool = True,
    ):
        """
        Initialize TCP check.
        
        Args:
            name: Check name
            host: Host to connect to
            port: Port to connect to
            timeout: Connection timeout
            critical: If True, failure affects overall health
        """
        super().__init__(name, timeout, critical)
        self.host = host
        self.port = port
    
    def check(self) -> HealthResult:
        """Check TCP connectivity."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            
            result = sock.connect_ex((self.host, self.port))
            sock.close()
            
            if result == 0:
                return HealthResult(
                    status=HealthStatus.HEALTHY,
                    message=f"Connected to {self.host}:{self.port}",
                    details={"host": self.host, "port": self.port},
                )
            else:
                return HealthResult(
                    status=HealthStatus.UNHEALTHY,
                    message=f"Cannot connect to {self.host}:{self.port}",
                    details={"host": self.host, "port": self.port, "error_code": result},
                )
        
        except socket.timeout:
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                message=f"Connection timeout to {self.host}:{self.port}",
            )
        except Exception as e:
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                message=str(e),
            )


class HTTPHealthCheck(HealthCheck):
    """
    HTTP endpoint health check.
    
    Checks if an HTTP endpoint returns expected status.
    
    USAGE:
        check = HTTPHealthCheck("api", "http://localhost:8080/health")
        result = check.run()
    """
    
    def __init__(
        self,
        name: str,
        url: str,
        expected_status: int = 200,
        timeout: float = 5.0,
        headers: Optional[Dict[str, str]] = None,
        critical: bool = True,
    ):
        """
        Initialize HTTP check.
        
        Args:
            name: Check name
            url: URL to check
            expected_status: Expected HTTP status code
            timeout: Request timeout
            headers: HTTP headers
            critical: If True, failure affects overall health
        """
        super().__init__(name, timeout, critical)
        self.url = url
        self.expected_status = expected_status
        self.headers = headers or {}
    
    def check(self) -> HealthResult:
        """Check HTTP endpoint."""
        try:
            import urllib.request
            
            req = urllib.request.Request(self.url, headers=self.headers)
            
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                status = response.status
                
                if status == self.expected_status:
                    return HealthResult(
                        status=HealthStatus.HEALTHY,
                        message=f"HTTP {status} from {self.url}",
                        details={"url": self.url, "status_code": status},
                    )
                else:
                    return HealthResult(
                        status=HealthStatus.DEGRADED,
                        message=f"Unexpected status {status} from {self.url}",
                        details={"url": self.url, "status_code": status},
                    )
        
        except URLError as e:
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                message=f"HTTP error: {e.reason}",
                details={"url": self.url},
            )
        except Exception as e:
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                message=str(e),
            )


class DatabaseHealthCheck(HealthCheck):
    """
    Database connection health check.
    
    Executes a simple query to verify database connectivity.
    
    USAGE:
        check = DatabaseHealthCheck(
            "postgres",
            connection_factory=lambda: psycopg2.connect(...)
        )
        result = check.run()
    """
    
    def __init__(
        self,
        name: str,
        connection_factory: Callable,
        query: str = "SELECT 1",
        timeout: float = 5.0,
        critical: bool = True,
    ):
        """
        Initialize database check.
        
        Args:
            name: Check name
            connection_factory: Function that returns a database connection
            query: Query to execute for health check
            timeout: Query timeout
            critical: If True, failure affects overall health
        """
        super().__init__(name, timeout, critical)
        self.connection_factory = connection_factory
        self.query = query
    
    def check(self) -> HealthResult:
        """Check database connectivity."""
        conn = None
        
        try:
            conn = self.connection_factory()
            cursor = conn.cursor()
            cursor.execute(self.query)
            cursor.fetchone()
            cursor.close()
            
            return HealthResult(
                status=HealthStatus.HEALTHY,
                message="Database query successful",
                details={"query": self.query},
            )
        
        except Exception as e:
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                message=f"Database error: {e}",
            )
        
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass


class KafkaHealthCheck(HealthCheck):
    """
    Kafka broker health check.
    
    Checks Kafka broker connectivity and topic availability.
    
    USAGE:
        check = KafkaHealthCheck(
            "kafka",
            bootstrap_servers="localhost:9092",
            topic="health-check"
        )
        result = check.run()
    """
    
    def __init__(
        self,
        name: str,
        bootstrap_servers: str,
        topic: Optional[str] = None,
        timeout: float = 10.0,
        critical: bool = True,
    ):
        """
        Initialize Kafka check.
        
        Args:
            name: Check name
            bootstrap_servers: Kafka bootstrap servers
            topic: Optional topic to check (verifies exists)
            timeout: Connection timeout
            critical: If True, failure affects overall health
        """
        super().__init__(name, timeout, critical)
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
    
    def check(self) -> HealthResult:
        """Check Kafka connectivity."""
        try:
            from kafka import KafkaAdminClient
            from kafka.errors import KafkaError
            
            admin = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                request_timeout_ms=int(self.timeout * 1000),
            )
            
            # Get cluster metadata
            topics = admin.list_topics()
            
            if self.topic and self.topic not in topics:
                admin.close()
                return HealthResult(
                    status=HealthStatus.DEGRADED,
                    message=f"Topic '{self.topic}' not found",
                    details={"available_topics": len(topics)},
                )
            
            admin.close()
            
            return HealthResult(
                status=HealthStatus.HEALTHY,
                message="Kafka cluster is healthy",
                details={"topics": len(topics)},
            )
        
        except ImportError:
            # kafka-python not installed, try TCP check
            return self._tcp_fallback()
        
        except Exception as e:
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                message=f"Kafka error: {e}",
            )
    
    def _tcp_fallback(self) -> HealthResult:
        """Fallback to TCP check if kafka-python not available."""
        # Parse bootstrap servers
        servers = self.bootstrap_servers.split(",")
        
        for server in servers:
            parts = server.strip().split(":")
            host = parts[0]
            port = int(parts[1]) if len(parts) > 1 else 9092
            
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.timeout)
                result = sock.connect_ex((host, port))
                sock.close()
                
                if result == 0:
                    return HealthResult(
                        status=HealthStatus.HEALTHY,
                        message=f"Kafka broker reachable at {host}:{port}",
                    )
            except Exception:
                continue
        
        return HealthResult(
            status=HealthStatus.UNHEALTHY,
            message="No Kafka brokers reachable",
        )


class CallableHealthCheck(HealthCheck):
    """
    Custom health check using a callable.
    
    USAGE:
        def my_check():
            # Custom check logic
            return HealthResult(status=HealthStatus.HEALTHY)
        
        check = CallableHealthCheck("custom", my_check)
    """
    
    def __init__(
        self,
        name: str,
        check_func: Callable[[], HealthResult],
        timeout: float = 5.0,
        critical: bool = True,
    ):
        """
        Initialize callable check.
        
        Args:
            name: Check name
            check_func: Function that returns HealthResult
            timeout: Check timeout
            critical: If True, failure affects overall health
        """
        super().__init__(name, timeout, critical)
        self.check_func = check_func
    
    def check(self) -> HealthResult:
        """Run the custom check function."""
        return self.check_func()


@dataclass
class ComponentHealth:
    """
    Health status of a component.
    
    Aggregates multiple health check results.
    """
    
    name: str
    status: HealthStatus
    checks: Dict[str, HealthResult]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "status": str(self.status),
            "checks": {k: v.to_dict() for k, v in self.checks.items()},
            "timestamp": self.timestamp.isoformat(),
        }


class HealthChecker:
    """
    Coordinates health checks for a system.
    
    SIMPLE EXPLANATION:
    The HealthChecker is like a hospital:
    - Registers patients (components)
    - Runs checkups (health checks)
    - Reports overall health (aggregate status)
    
    USAGE:
        checker = HealthChecker()
        
        # Add health checks
        checker.add_check(TCPHealthCheck("postgres", "localhost", 5432))
        checker.add_check(HTTPHealthCheck("api", "http://localhost:8080/health"))
        checker.add_check(KafkaHealthCheck("kafka", "localhost:9092"))
        
        # Run all checks
        health = checker.check_all()
        print(f"Overall status: {health['status']}")
        
        # Start background checking
        checker.start(interval=30)
    """
    
    def __init__(self, name: str = "system"):
        """
        Initialize health checker.
        
        Args:
            name: System name
        """
        self.name = name
        self._checks: Dict[str, HealthCheck] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
    
    def add_check(self, check: HealthCheck) -> None:
        """Add a health check."""
        with self._lock:
            self._checks[check.name] = check
        logger.info(f"Added health check: {check.name}")
    
    def remove_check(self, name: str) -> bool:
        """Remove a health check."""
        with self._lock:
            if name in self._checks:
                del self._checks[name]
                logger.info(f"Removed health check: {name}")
                return True
        return False
    
    def check_one(self, name: str) -> Optional[HealthResult]:
        """Run a single check by name."""
        with self._lock:
            check = self._checks.get(name)
        
        if check:
            return check.run()
        return None
    
    def check_all(self) -> Dict[str, Any]:
        """
        Run all health checks.
        
        Returns:
            Dictionary with overall status and individual results
        """
        results: Dict[str, HealthResult] = {}
        overall_status = HealthStatus.HEALTHY
        
        with self._lock:
            checks = list(self._checks.items())
        
        for name, check in checks:
            result = check.run()
            results[name] = result
            
            # Update overall status
            if check.critical:
                if result.status == HealthStatus.UNHEALTHY:
                    overall_status = HealthStatus.UNHEALTHY
                elif result.status == HealthStatus.DEGRADED:
                    if overall_status != HealthStatus.UNHEALTHY:
                        overall_status = HealthStatus.DEGRADED
        
        return {
            "name": self.name,
            "status": str(overall_status),
            "healthy": overall_status == HealthStatus.HEALTHY,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "checks": {k: v.to_dict() for k, v in results.items()},
        }
    
    def get_status(self) -> HealthStatus:
        """Get current overall status from cached results."""
        with self._lock:
            for check in self._checks.values():
                if check.critical and check.last_result:
                    if check.last_result.status == HealthStatus.UNHEALTHY:
                        return HealthStatus.UNHEALTHY
        
        return HealthStatus.HEALTHY
    
    def start(self, interval: float = 30.0) -> None:
        """
        Start background health checking.
        
        Args:
            interval: Check interval in seconds
        """
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(
            target=self._check_loop,
            args=(interval,),
            daemon=True,
        )
        self._thread.start()
        logger.info(f"Started health checker with {interval}s interval")
    
    def stop(self) -> None:
        """Stop background health checking."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Stopped health checker")
    
    def _check_loop(self, interval: float) -> None:
        """Background check loop."""
        while self._running:
            try:
                self.check_all()
            except Exception as e:
                logger.error(f"Health check error: {e}")
            
            time.sleep(interval)
    
    def get_checks(self) -> List[str]:
        """Get list of check names."""
        with self._lock:
            return list(self._checks.keys())


# Factory functions

def create_tcp_check(name: str, host: str, port: int, **kwargs) -> TCPHealthCheck:
    """Create a TCP health check."""
    return TCPHealthCheck(name, host, port, **kwargs)


def create_http_check(name: str, url: str, **kwargs) -> HTTPHealthCheck:
    """Create an HTTP health check."""
    return HTTPHealthCheck(name, url, **kwargs)


def create_database_check(
    name: str,
    connection_factory: Callable,
    **kwargs
) -> DatabaseHealthCheck:
    """Create a database health check."""
    return DatabaseHealthCheck(name, connection_factory, **kwargs)


def create_kafka_check(
    name: str,
    bootstrap_servers: str,
    **kwargs
) -> KafkaHealthCheck:
    """Create a Kafka health check."""
    return KafkaHealthCheck(name, bootstrap_servers, **kwargs)


def create_pipeline_health_checks() -> List[HealthCheck]:
    """
    Create standard CDC pipeline health checks.
    
    Returns:
        List of HealthCheck objects
    """
    return [
        TCPHealthCheck("source_db", "localhost", 5434, critical=True),
        TCPHealthCheck("target_db", "localhost", 5435, critical=True),
        KafkaHealthCheck("kafka", "localhost:9092", critical=True),
    ]
