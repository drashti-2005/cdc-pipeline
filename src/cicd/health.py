"""
Deployment Health Check Module

Provides health checking for deployed services.
"""

import logging
import socket
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from urllib.request import urlopen
from urllib.error import URLError

from src.cicd.config import DeploymentConfig

logger = logging.getLogger(__name__)


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    
    name: str
    healthy: bool
    message: str = ""
    latency_ms: float = 0.0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "healthy": self.healthy,
            "message": self.message,
            "latency_ms": self.latency_ms,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details,
        }


class HealthCheck:
    """
    Base health check class.
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
            name: Name of the check
            timeout: Timeout in seconds
            critical: If True, failure marks overall health as unhealthy
        """
        self.name = name
        self.timeout = timeout
        self.critical = critical
    
    def run(self) -> HealthCheckResult:
        """Run the health check."""
        raise NotImplementedError


class TCPHealthCheck(HealthCheck):
    """Check if a TCP port is open."""
    
    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        timeout: float = 5.0,
        critical: bool = True,
    ):
        """
        Initialize TCP health check.
        
        Args:
            name: Check name
            host: Host to connect to
            port: Port to check
            timeout: Connection timeout
            critical: If critical for overall health
        """
        super().__init__(name, timeout, critical)
        self.host = host
        self.port = port
    
    def run(self) -> HealthCheckResult:
        """Run TCP connection check."""
        start = time.time()
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            
            result = sock.connect_ex((self.host, self.port))
            sock.close()
            
            latency = (time.time() - start) * 1000
            
            if result == 0:
                return HealthCheckResult(
                    name=self.name,
                    healthy=True,
                    message=f"Connected to {self.host}:{self.port}",
                    latency_ms=latency,
                )
            else:
                return HealthCheckResult(
                    name=self.name,
                    healthy=False,
                    message=f"Cannot connect to {self.host}:{self.port}",
                    latency_ms=latency,
                )
                
        except socket.timeout:
            return HealthCheckResult(
                name=self.name,
                healthy=False,
                message=f"Connection timeout to {self.host}:{self.port}",
                latency_ms=self.timeout * 1000,
            )
        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                healthy=False,
                message=f"Error: {str(e)}",
            )


class HTTPHealthCheck(HealthCheck):
    """Check HTTP endpoint health."""
    
    def __init__(
        self,
        name: str,
        url: str,
        expected_status: int = 200,
        timeout: float = 5.0,
        critical: bool = True,
    ):
        """
        Initialize HTTP health check.
        
        Args:
            name: Check name
            url: URL to check
            expected_status: Expected HTTP status code
            timeout: Request timeout
            critical: If critical for overall health
        """
        super().__init__(name, timeout, critical)
        self.url = url
        self.expected_status = expected_status
    
    def run(self) -> HealthCheckResult:
        """Run HTTP health check."""
        start = time.time()
        
        try:
            response = urlopen(self.url, timeout=self.timeout)
            latency = (time.time() - start) * 1000
            status = response.getcode()
            
            healthy = status == self.expected_status
            
            return HealthCheckResult(
                name=self.name,
                healthy=healthy,
                message=f"HTTP {status}" if healthy else f"Expected {self.expected_status}, got {status}",
                latency_ms=latency,
                details={"status_code": status},
            )
            
        except URLError as e:
            return HealthCheckResult(
                name=self.name,
                healthy=False,
                message=f"Connection failed: {str(e)}",
            )
        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                healthy=False,
                message=f"Error: {str(e)}",
            )


class CallableHealthCheck(HealthCheck):
    """Health check using a custom callable."""
    
    def __init__(
        self,
        name: str,
        check_fn: Callable[[], bool],
        timeout: float = 5.0,
        critical: bool = True,
    ):
        """
        Initialize callable health check.
        
        Args:
            name: Check name
            check_fn: Function that returns True if healthy
            timeout: Check timeout
            critical: If critical for overall health
        """
        super().__init__(name, timeout, critical)
        self.check_fn = check_fn
    
    def run(self) -> HealthCheckResult:
        """Run custom health check."""
        start = time.time()
        
        try:
            healthy = self.check_fn()
            latency = (time.time() - start) * 1000
            
            return HealthCheckResult(
                name=self.name,
                healthy=healthy,
                message="Check passed" if healthy else "Check failed",
                latency_ms=latency,
            )
            
        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                healthy=False,
                message=f"Error: {str(e)}",
            )


class DeploymentHealthChecker:
    """
    Health checker for deployment verification.
    """
    
    def __init__(
        self,
        config: DeploymentConfig,
        checks: Optional[List[HealthCheck]] = None,
    ):
        """
        Initialize deployment health checker.
        
        Args:
            config: Deployment configuration
            checks: Optional list of health checks
        """
        self.config = config
        self.checks: List[HealthCheck] = checks or []
        self.logger = logging.getLogger(__name__)
    
    def add_check(self, check: HealthCheck) -> None:
        """Add a health check."""
        self.checks.append(check)
        self.logger.info(f"Added health check: {check.name}")
    
    def add_default_checks(self) -> None:
        """Add default health checks based on configuration."""
        # Source database
        self.add_check(TCPHealthCheck(
            name="source_database",
            host=self.config.source_db.host,
            port=self.config.source_db.port,
            critical=True,
        ))
        
        # Target database
        self.add_check(TCPHealthCheck(
            name="target_database",
            host=self.config.target_db.host,
            port=self.config.target_db.port,
            critical=True,
        ))
        
        # Kafka
        kafka_host = self.config.kafka.bootstrap_servers.split(",")[0]
        host, port = kafka_host.split(":") if ":" in kafka_host else (kafka_host, "9092")
        
        self.add_check(TCPHealthCheck(
            name="kafka",
            host=host,
            port=int(port),
            critical=True,
        ))
        
        # Metrics endpoint
        if self.config.monitoring.enabled:
            self.add_check(HTTPHealthCheck(
                name="metrics_endpoint",
                url=f"http://localhost:{self.config.monitoring.metrics_port}/health",
                expected_status=200,
                critical=False,
            ))
    
    def run_all(self) -> Dict[str, Any]:
        """
        Run all health checks.
        
        Returns:
            Health check results
        """
        results = []
        all_healthy = True
        critical_healthy = True
        
        for check in self.checks:
            self.logger.info(f"Running health check: {check.name}")
            result = check.run()
            results.append(result)
            
            if not result.healthy:
                all_healthy = False
                if check.critical:
                    critical_healthy = False
                    self.logger.error(f"Critical health check failed: {check.name}")
                else:
                    self.logger.warning(f"Non-critical health check failed: {check.name}")
        
        return {
            "healthy": critical_healthy,
            "all_healthy": all_healthy,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "environment": self.config.environment.value,
            "checks": [r.to_dict() for r in results],
            "summary": {
                "total": len(results),
                "passed": sum(1 for r in results if r.healthy),
                "failed": sum(1 for r in results if not r.healthy),
            },
        }
    
    def wait_for_healthy(
        self,
        timeout: float = 300.0,
        interval: float = 5.0,
    ) -> bool:
        """
        Wait for all critical health checks to pass.
        
        Args:
            timeout: Maximum time to wait in seconds
            interval: Time between checks in seconds
            
        Returns:
            True if healthy within timeout
        """
        start = time.time()
        
        while (time.time() - start) < timeout:
            result = self.run_all()
            
            if result["healthy"]:
                self.logger.info("All critical health checks passed")
                return True
            
            self.logger.info(
                f"Health checks not passing yet "
                f"({result['summary']['passed']}/{result['summary']['total']}), "
                f"retrying in {interval}s..."
            )
            time.sleep(interval)
        
        self.logger.error(f"Health checks did not pass within {timeout}s")
        return False


def check_deployment_health(
    config: DeploymentConfig,
    timeout: float = 60.0,
) -> Dict[str, Any]:
    """
    Convenience function to check deployment health.
    
    Args:
        config: Deployment configuration
        timeout: Timeout for health checks
        
    Returns:
        Health check results
    """
    checker = DeploymentHealthChecker(config)
    checker.add_default_checks()
    return checker.run_all()


def create_smoke_tests(config: DeploymentConfig) -> List[HealthCheck]:
    """
    Create smoke test health checks for post-deployment verification.
    
    Args:
        config: Deployment configuration
        
    Returns:
        List of health checks for smoke testing
    """
    checks = []
    
    # Application health endpoint
    checks.append(HTTPHealthCheck(
        name="application_health",
        url=f"http://localhost:{config.monitoring.metrics_port}/health",
        expected_status=200,
        timeout=10.0,
    ))
    
    # Readiness endpoint
    checks.append(HTTPHealthCheck(
        name="application_ready",
        url=f"http://localhost:{config.monitoring.metrics_port}/ready",
        expected_status=200,
        timeout=10.0,
    ))
    
    # Database connectivity
    checks.append(TCPHealthCheck(
        name="source_db_connectivity",
        host=config.source_db.host,
        port=config.source_db.port,
    ))
    
    checks.append(TCPHealthCheck(
        name="target_db_connectivity",
        host=config.target_db.host,
        port=config.target_db.port,
    ))
    
    return checks
