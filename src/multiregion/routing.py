"""
Region-Aware Routing

Routes requests to the optimal region based on:
- Latency: Route to fastest responding region
- Geography: Route to nearest region
- Load: Route to least loaded region
- Affinity: Route based on data affinity rules

SIMPLE EXPLANATION:
Routing is like choosing which restaurant to go to:
- Nearest: Go to the closest one
- Fastest: Go to the one with shortest wait
- Least busy: Go to the one with most free tables
"""

import logging
import math
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Any, Callable

from .config import (
    Region,
    RegionConfig,
    MultiRegionConfig,
    RegionStatus,
)

logger = logging.getLogger(__name__)


class RoutingStrategy(Enum):
    """
    Routing strategy for selecting regions.
    
    ROUND_ROBIN: Rotate through regions
    LATENCY: Route to lowest latency region
    GEOGRAPHIC: Route to geographically nearest region
    WEIGHTED: Route based on configured weights
    RANDOM: Random selection among healthy regions
    AFFINITY: Route based on data affinity (e.g., same user → same region)
    PRIMARY_ONLY: Always route to primary region
    FAILOVER: Route to primary, failover to replicas
    """
    
    ROUND_ROBIN = "round_robin"
    LATENCY = "latency"
    GEOGRAPHIC = "geographic"
    WEIGHTED = "weighted"
    RANDOM = "random"
    AFFINITY = "affinity"
    PRIMARY_ONLY = "primary_only"
    FAILOVER = "failover"


@dataclass
class RoutingDecision:
    """
    Result of a routing decision.
    
    Contains the selected region and metadata about the decision.
    """
    
    region: RegionConfig
    strategy: RoutingStrategy
    reason: str
    latency_ms: Optional[float] = None
    distance_km: Optional[float] = None
    alternatives: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "region": self.region.region.name,
            "strategy": self.strategy.value,
            "reason": self.reason,
            "latency_ms": self.latency_ms,
            "distance_km": self.distance_km,
            "alternatives": self.alternatives,
            "timestamp": self.timestamp.isoformat(),
        }


class RegionRouter(ABC):
    """
    Abstract base class for region routers.
    
    USAGE:
        router = LatencyBasedRouter(config)
        decision = router.route(request_context)
        print(f"Selected region: {decision.region.region.name}")
    """
    
    def __init__(self, config: MultiRegionConfig):
        self.config = config
        self._last_decision: Optional[RoutingDecision] = None
    
    @abstractmethod
    def route(self, context: Optional[Dict[str, Any]] = None) -> RoutingDecision:
        """
        Select a region for the request.
        
        Args:
            context: Optional request context (user info, data keys, etc.)
            
        Returns:
            RoutingDecision with selected region
        """
        pass
    
    def get_healthy_regions(self) -> List[RegionConfig]:
        """Get all healthy (active) regions."""
        return [
            config for config in self.config.regions.values()
            if config.region.status in (RegionStatus.ACTIVE, RegionStatus.DEGRADED)
        ]
    
    @property
    def last_decision(self) -> Optional[RoutingDecision]:
        """Get the last routing decision."""
        return self._last_decision


class RoundRobinRouter(RegionRouter):
    """
    Round-robin routing across healthy regions.
    
    Simple load distribution by rotating through regions.
    """
    
    def __init__(self, config: MultiRegionConfig):
        super().__init__(config)
        self._index = 0
    
    def route(self, context: Optional[Dict[str, Any]] = None) -> RoutingDecision:
        """Select next region in rotation."""
        healthy = self.get_healthy_regions()
        
        if not healthy:
            raise RuntimeError("No healthy regions available")
        
        # Select next region
        selected = healthy[self._index % len(healthy)]
        self._index += 1
        
        # Get alternatives
        alternatives = [r.region.name for r in healthy if r != selected]
        
        decision = RoutingDecision(
            region=selected,
            strategy=RoutingStrategy.ROUND_ROBIN,
            reason=f"Round robin index {self._index - 1}",
            alternatives=alternatives,
        )
        
        self._last_decision = decision
        return decision


class LatencyBasedRouter(RegionRouter):
    """
    Route to the region with lowest measured latency.
    
    Maintains latency measurements and routes to fastest region.
    
    SIMPLE EXPLANATION:
    Like choosing the fastest checkout line:
    - Measure how long each line takes
    - Go to the shortest one
    - Keep measuring to adapt to changes
    """
    
    def __init__(
        self,
        config: MultiRegionConfig,
        latency_check_fn: Optional[Callable[[RegionConfig], float]] = None,
    ):
        """
        Initialize latency-based router.
        
        Args:
            config: Multi-region configuration
            latency_check_fn: Function to measure latency to a region (returns ms)
        """
        super().__init__(config)
        self._latencies: Dict[str, float] = {}
        self._latency_check_fn = latency_check_fn or self._default_latency_check
        self._last_check: Dict[str, float] = {}
        self._check_interval_seconds = 30.0
    
    def _default_latency_check(self, region: RegionConfig) -> float:
        """Default latency check (simulated)."""
        # In production, this would ping the region's health endpoint
        # For now, simulate based on priority (lower priority = lower latency)
        base_latency = 10.0 + (region.region.priority * 5)
        jitter = random.uniform(-5, 5)
        return max(1.0, base_latency + jitter)
    
    def measure_latency(self, region: RegionConfig) -> float:
        """
        Measure latency to a region.
        
        Args:
            region: Region to measure
            
        Returns:
            Latency in milliseconds
        """
        latency = self._latency_check_fn(region)
        self._latencies[region.region.name] = latency
        self._last_check[region.region.name] = time.time()
        return latency
    
    def get_latency(self, region: RegionConfig) -> float:
        """Get cached or fresh latency measurement."""
        name = region.region.name
        now = time.time()
        
        # Check if we need fresh measurement
        if name not in self._latencies or \
           (now - self._last_check.get(name, 0)) > self._check_interval_seconds:
            return self.measure_latency(region)
        
        return self._latencies[name]
    
    def route(self, context: Optional[Dict[str, Any]] = None) -> RoutingDecision:
        """Select region with lowest latency."""
        healthy = self.get_healthy_regions()
        
        if not healthy:
            raise RuntimeError("No healthy regions available")
        
        # Measure latencies
        latencies = {r.region.name: self.get_latency(r) for r in healthy}
        
        # Select lowest latency region
        best_name = min(latencies, key=latencies.get)
        best_region = self.config.get_region(best_name)
        
        # Get alternatives sorted by latency
        sorted_regions = sorted(latencies.keys(), key=lambda n: latencies[n])
        alternatives = [n for n in sorted_regions if n != best_name]
        
        decision = RoutingDecision(
            region=best_region,
            strategy=RoutingStrategy.LATENCY,
            reason=f"Lowest latency: {latencies[best_name]:.1f}ms",
            latency_ms=latencies[best_name],
            alternatives=alternatives,
        )
        
        self._last_decision = decision
        logger.debug(f"Latency routing selected {best_name}: {latencies[best_name]:.1f}ms")
        return decision


class GeographicRouter(RegionRouter):
    """
    Route to geographically nearest region.
    
    Uses Haversine formula to calculate distance.
    
    SIMPLE EXPLANATION:
    Like finding the nearest store:
    - Know where you are (your coordinates)
    - Know where each store is
    - Go to the closest one
    """
    
    def __init__(self, config: MultiRegionConfig):
        super().__init__(config)
        self._client_location: Optional[tuple] = None
    
    def set_client_location(self, latitude: float, longitude: float) -> None:
        """Set client location for distance calculations."""
        self._client_location = (latitude, longitude)
    
    @staticmethod
    def haversine_distance(
        lat1: float, lon1: float,
        lat2: float, lon2: float,
    ) -> float:
        """
        Calculate distance between two points using Haversine formula.
        
        Args:
            lat1, lon1: First point coordinates
            lat2, lon2: Second point coordinates
            
        Returns:
            Distance in kilometers
        """
        R = 6371  # Earth's radius in km
        
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        
        a = (
            math.sin(delta_lat / 2) ** 2 +
            math.cos(lat1_rad) * math.cos(lat2_rad) *
            math.sin(delta_lon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return R * c
    
    def get_distance(self, region: RegionConfig) -> float:
        """Get distance to region in kilometers."""
        if not self._client_location:
            # Default to 0 if no client location set
            return 0.0
        
        return self.haversine_distance(
            self._client_location[0],
            self._client_location[1],
            region.region.latitude,
            region.region.longitude,
        )
    
    def route(self, context: Optional[Dict[str, Any]] = None) -> RoutingDecision:
        """Select geographically nearest region."""
        healthy = self.get_healthy_regions()
        
        if not healthy:
            raise RuntimeError("No healthy regions available")
        
        # Check for client location in context
        if context and "latitude" in context and "longitude" in context:
            self.set_client_location(context["latitude"], context["longitude"])
        
        # Calculate distances
        distances = {r.region.name: self.get_distance(r) for r in healthy}
        
        # Select nearest region
        nearest_name = min(distances, key=distances.get)
        nearest_region = self.config.get_region(nearest_name)
        
        # Get alternatives sorted by distance
        sorted_regions = sorted(distances.keys(), key=lambda n: distances[n])
        alternatives = [n for n in sorted_regions if n != nearest_name]
        
        decision = RoutingDecision(
            region=nearest_region,
            strategy=RoutingStrategy.GEOGRAPHIC,
            reason=f"Nearest region: {distances[nearest_name]:.0f}km",
            distance_km=distances[nearest_name],
            alternatives=alternatives,
        )
        
        self._last_decision = decision
        return decision


class WeightedRouter(RegionRouter):
    """
    Route based on configured weights.
    
    Higher weight = higher probability of selection.
    
    USAGE:
        router = WeightedRouter(config)
        router.set_weight("us-east-1", 70)  # 70% traffic
        router.set_weight("eu-west-1", 30)  # 30% traffic
    """
    
    def __init__(self, config: MultiRegionConfig):
        super().__init__(config)
        self._weights: Dict[str, int] = {}
        
        # Default weights from priority (inverse)
        for region_config in config.regions.values():
            # Higher priority (lower number) = higher weight
            self._weights[region_config.region.name] = 100 - region_config.region.priority
    
    def set_weight(self, region_name: str, weight: int) -> None:
        """Set weight for a region (0-100)."""
        self._weights[region_name] = max(0, min(100, weight))
    
    def get_weight(self, region_name: str) -> int:
        """Get weight for a region."""
        return self._weights.get(region_name, 50)
    
    def route(self, context: Optional[Dict[str, Any]] = None) -> RoutingDecision:
        """Select region based on weights."""
        healthy = self.get_healthy_regions()
        
        if not healthy:
            raise RuntimeError("No healthy regions available")
        
        # Build weighted list
        weighted_regions = []
        for region_config in healthy:
            weight = self._weights.get(region_config.region.name, 50)
            weighted_regions.extend([region_config] * weight)
        
        if not weighted_regions:
            # Fallback to random
            selected = random.choice(healthy)
        else:
            selected = random.choice(weighted_regions)
        
        alternatives = [r.region.name for r in healthy if r != selected]
        
        decision = RoutingDecision(
            region=selected,
            strategy=RoutingStrategy.WEIGHTED,
            reason=f"Weight: {self._weights.get(selected.region.name, 50)}%",
            alternatives=alternatives,
        )
        
        self._last_decision = decision
        return decision


class AffinityRouter(RegionRouter):
    """
    Route based on data affinity.
    
    Ensures related data goes to the same region.
    
    SIMPLE EXPLANATION:
    Like keeping a customer's files in the same cabinet:
    - Hash the customer ID
    - Always go to the same region for that customer
    - Ensures data locality
    """
    
    def __init__(self, config: MultiRegionConfig, affinity_key: str = "user_id"):
        """
        Initialize affinity router.
        
        Args:
            config: Multi-region configuration
            affinity_key: Key in context to use for affinity (default: user_id)
        """
        super().__init__(config)
        self.affinity_key = affinity_key
        self._affinity_cache: Dict[str, str] = {}  # key_value → region_name
    
    def _hash_to_region(self, key_value: str, regions: List[RegionConfig]) -> RegionConfig:
        """Hash key value to a region."""
        # Simple hash-based selection
        hash_value = hash(key_value)
        index = hash_value % len(regions)
        return regions[index]
    
    def set_affinity(self, key_value: str, region_name: str) -> None:
        """Manually set affinity for a key."""
        self._affinity_cache[key_value] = region_name
    
    def get_affinity(self, key_value: str) -> Optional[str]:
        """Get cached affinity for a key."""
        return self._affinity_cache.get(key_value)
    
    def route(self, context: Optional[Dict[str, Any]] = None) -> RoutingDecision:
        """Select region based on affinity key."""
        healthy = self.get_healthy_regions()
        
        if not healthy:
            raise RuntimeError("No healthy regions available")
        
        # Get affinity key from context
        key_value = None
        if context and self.affinity_key in context:
            key_value = str(context[self.affinity_key])
        
        if key_value:
            # Check cache first
            if key_value in self._affinity_cache:
                region_name = self._affinity_cache[key_value]
                region = self.config.get_region(region_name)
                
                # Verify region is still healthy
                if region and region.region.status == RegionStatus.ACTIVE:
                    return RoutingDecision(
                        region=region,
                        strategy=RoutingStrategy.AFFINITY,
                        reason=f"Cached affinity: {self.affinity_key}={key_value}",
                        alternatives=[r.region.name for r in healthy if r != region],
                    )
            
            # Hash to region
            selected = self._hash_to_region(key_value, healthy)
            self._affinity_cache[key_value] = selected.region.name
            
            decision = RoutingDecision(
                region=selected,
                strategy=RoutingStrategy.AFFINITY,
                reason=f"Hash affinity: {self.affinity_key}={key_value}",
                alternatives=[r.region.name for r in healthy if r != selected],
            )
        else:
            # No affinity key, fall back to random
            selected = random.choice(healthy)
            decision = RoutingDecision(
                region=selected,
                strategy=RoutingStrategy.RANDOM,
                reason="No affinity key, random selection",
                alternatives=[r.region.name for r in healthy if r != selected],
            )
        
        self._last_decision = decision
        return decision


class FailoverRouter(RegionRouter):
    """
    Route to primary with automatic failover.
    
    Always tries primary first, fails over to replicas if unavailable.
    
    SIMPLE EXPLANATION:
    Like having a backup plan:
    - Try the main office first
    - If closed, go to backup office #1
    - If that's closed too, go to backup #2
    """
    
    def __init__(self, config: MultiRegionConfig):
        super().__init__(config)
        self._failed_regions: Dict[str, float] = {}  # region → failure timestamp
        self._retry_after_seconds = 60.0
    
    def mark_failed(self, region_name: str) -> None:
        """Mark a region as failed."""
        self._failed_regions[region_name] = time.time()
        logger.warning(f"Region marked as failed: {region_name}")
    
    def mark_recovered(self, region_name: str) -> None:
        """Mark a region as recovered."""
        if region_name in self._failed_regions:
            del self._failed_regions[region_name]
            logger.info(f"Region marked as recovered: {region_name}")
    
    def is_available(self, region: RegionConfig) -> bool:
        """Check if region is available."""
        # Check region status
        if region.region.status not in (RegionStatus.ACTIVE, RegionStatus.DEGRADED):
            return False
        
        # Check failure cache
        name = region.region.name
        if name in self._failed_regions:
            elapsed = time.time() - self._failed_regions[name]
            if elapsed < self._retry_after_seconds:
                return False
            # Retry period elapsed, remove from failed
            del self._failed_regions[name]
        
        return True
    
    def route(self, context: Optional[Dict[str, Any]] = None) -> RoutingDecision:
        """Route to primary with failover."""
        # Try primary first
        primary = self.config.get_primary()
        if primary and self.is_available(primary):
            return RoutingDecision(
                region=primary,
                strategy=RoutingStrategy.FAILOVER,
                reason="Primary region available",
                alternatives=[
                    r.region.name for r in self.config.get_replica_regions()
                    if self.is_available(r)
                ],
            )
        
        # Failover to replicas by priority
        replicas = sorted(
            self.config.get_replica_regions(),
            key=lambda r: r.region.priority
        )
        
        for replica in replicas:
            if self.is_available(replica):
                logger.warning(f"Failover to replica: {replica.region.name}")
                return RoutingDecision(
                    region=replica,
                    strategy=RoutingStrategy.FAILOVER,
                    reason=f"Failover: primary unavailable, using {replica.region.name}",
                    alternatives=[
                        r.region.name for r in replicas
                        if r != replica and self.is_available(r)
                    ],
                )
        
        raise RuntimeError("No available regions for failover")


def create_router(
    config: MultiRegionConfig,
    strategy: RoutingStrategy,
    **kwargs,
) -> RegionRouter:
    """
    Factory function to create a router.
    
    Args:
        config: Multi-region configuration
        strategy: Routing strategy
        **kwargs: Additional router-specific arguments
        
    Returns:
        RegionRouter instance
    """
    routers = {
        RoutingStrategy.ROUND_ROBIN: RoundRobinRouter,
        RoutingStrategy.LATENCY: LatencyBasedRouter,
        RoutingStrategy.GEOGRAPHIC: GeographicRouter,
        RoutingStrategy.WEIGHTED: WeightedRouter,
        RoutingStrategy.AFFINITY: AffinityRouter,
        RoutingStrategy.FAILOVER: FailoverRouter,
    }
    
    router_class = routers.get(strategy)
    if not router_class:
        raise ValueError(f"Unknown routing strategy: {strategy}")
    
    return router_class(config, **kwargs)
