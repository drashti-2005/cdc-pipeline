"""
Multi-Region Support Module

Provides infrastructure for running CDC pipeline across multiple regions:
- Region configuration and management
- Region-aware routing
- Cross-region replication
- Failover and health monitoring
"""

from .config import (
    Region,
    RegionConfig,
    MultiRegionConfig,
    RegionStatus,
    load_region_config,
)
from .routing import (
    RegionRouter,
    RoutingStrategy,
    RoutingDecision,
    LatencyBasedRouter,
    GeographicRouter,
)
from .replication import (
    ReplicationManager,
    ReplicationMode,
    ReplicationStatus,
    ReplicationLag,
)
from .failover import (
    FailoverManager,
    FailoverStrategy,
    FailoverEvent,
    HealthChecker,
    RegionHealth,
)

__all__ = [
    # Config
    "Region",
    "RegionConfig",
    "MultiRegionConfig",
    "RegionStatus",
    "load_region_config",
    # Routing
    "RegionRouter",
    "RoutingStrategy",
    "RoutingDecision",
    "LatencyBasedRouter",
    "GeographicRouter",
    # Replication
    "ReplicationManager",
    "ReplicationMode",
    "ReplicationStatus",
    "ReplicationLag",
    # Failover
    "FailoverManager",
    "FailoverStrategy",
    "FailoverEvent",
    "HealthChecker",
    "RegionHealth",
]
