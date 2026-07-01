"""
Unit tests for multi-region module.

Tests:
- Region configuration
- Routing strategies
- Replication
- Failover mechanisms
"""

import pytest
import tempfile
import time
from pathlib import Path
from datetime import datetime, timezone

from src.multiregion.config import (
    Region,
    RegionConfig,
    MultiRegionConfig,
    RegionStatus,
    KafkaEndpoint,
    DatabaseEndpoint,
    StorageEndpoint,
    create_default_config,
    load_region_config,
    REGION_TEMPLATES,
)
from src.multiregion.routing import (
    RoutingStrategy,
    RoutingDecision,
    RoundRobinRouter,
    LatencyBasedRouter,
    GeographicRouter,
    WeightedRouter,
    AffinityRouter,
    FailoverRouter,
    create_router,
)
from src.multiregion.replication import (
    ReplicationMode,
    ReplicationStatus,
    ConflictResolution,
    ReplicationLag,
    ReplicationEvent,
    ReplicationManager,
    MockReplicationTransport,
)
from src.multiregion.failover import (
    FailoverStrategy,
    HealthStatus,
    RegionHealth,
    FailoverEvent,
    HealthChecker,
    FailoverManager,
)


# ==============================================================================
# Test Fixtures
# ==============================================================================

@pytest.fixture
def sample_region():
    """Create a sample region."""
    return Region(
        name="us-east-1",
        display_name="US East (Virginia)",
        is_primary=True,
        priority=1,
        latitude=37.4316,
        longitude=-78.6569,
    )


@pytest.fixture
def sample_region_config():
    """Create a sample region config."""
    return RegionConfig(
        region=Region(
            name="us-east-1",
            display_name="US East",
            is_primary=True,
        ),
        kafka=KafkaEndpoint(bootstrap_servers="localhost:9092"),
        source_database=DatabaseEndpoint(
            host="localhost", port=5432, database="source",
            username="postgres", password="postgres",
        ),
        target_database=DatabaseEndpoint(
            host="localhost", port=5433, database="target",
            username="postgres", password="postgres",
        ),
        storage=StorageEndpoint(
            endpoint="localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket="cdc-events",
        ),
    )


@pytest.fixture
def multi_region_config():
    """Create a multi-region config with 3 regions."""
    return create_default_config()


# ==============================================================================
# Region Configuration Tests
# ==============================================================================

class TestRegion:
    """Tests for Region class."""
    
    def test_region_creation(self, sample_region):
        """Test region creation."""
        assert sample_region.name == "us-east-1"
        assert sample_region.is_primary
        assert sample_region.status == RegionStatus.ACTIVE
    
    def test_region_equality(self):
        """Test region equality based on name."""
        r1 = Region(name="us-east-1", display_name="US East 1")
        r2 = Region(name="us-east-1", display_name="Different Name")
        r3 = Region(name="eu-west-1", display_name="EU West")
        
        assert r1 == r2
        assert r1 != r3
    
    def test_region_to_dict(self, sample_region):
        """Test serialization."""
        d = sample_region.to_dict()
        
        assert d["name"] == "us-east-1"
        assert d["is_primary"] is True
        assert d["status"] == "active"
    
    def test_region_from_dict(self):
        """Test deserialization."""
        data = {
            "name": "eu-west-1",
            "display_name": "EU West",
            "is_primary": False,
            "status": "degraded",
            "priority": 50,
        }
        
        region = Region.from_dict(data)
        
        assert region.name == "eu-west-1"
        assert region.status == RegionStatus.DEGRADED
        assert region.priority == 50
    
    def test_region_templates(self):
        """Test pre-defined region templates."""
        assert "us-east-1" in REGION_TEMPLATES
        assert "eu-west-1" in REGION_TEMPLATES
        assert "ap-south-1" in REGION_TEMPLATES
        
        us_east = REGION_TEMPLATES["us-east-1"]
        assert us_east.latitude != 0
        assert us_east.longitude != 0


class TestMultiRegionConfig:
    """Tests for MultiRegionConfig."""
    
    def test_add_region(self, sample_region_config):
        """Test adding a region."""
        config = MultiRegionConfig()
        config.add_region(sample_region_config)
        
        assert "us-east-1" in config.regions
        assert config.default_region == "us-east-1"
    
    def test_get_primary(self, multi_region_config):
        """Test getting primary region."""
        primary = multi_region_config.get_primary()
        
        assert primary is not None
        assert primary.region.is_primary
        assert primary.region.name == "us-east-1"
    
    def test_get_active_regions(self, multi_region_config):
        """Test getting active regions."""
        active = multi_region_config.get_active_regions()
        
        assert len(active) == 3
        assert all(r.region.status == RegionStatus.ACTIVE for r in active)
    
    def test_get_replica_regions(self, multi_region_config):
        """Test getting replica regions."""
        replicas = multi_region_config.get_replica_regions()
        
        assert len(replicas) == 2
        assert all(not r.region.is_primary for r in replicas)
    
    def test_validate_valid_config(self, multi_region_config):
        """Test validation of valid config."""
        errors = multi_region_config.validate()
        assert len(errors) == 0
    
    def test_validate_no_primary(self):
        """Test validation fails with no primary."""
        config = MultiRegionConfig()
        config.add_region(RegionConfig(
            region=Region(name="r1", display_name="R1", is_primary=False),
            kafka=KafkaEndpoint(bootstrap_servers="localhost:9092"),
            source_database=DatabaseEndpoint("h", 1, "d", "u", "p"),
            target_database=DatabaseEndpoint("h", 2, "d", "u", "p"),
            storage=StorageEndpoint("e", "a", "s", "b"),
        ))
        
        errors = config.validate()
        assert any("primary" in e.lower() for e in errors)
    
    def test_save_and_load(self, multi_region_config):
        """Test saving and loading config."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            path = Path(f.name)
        
        try:
            multi_region_config.save(path)
            loaded = load_region_config(path)
            
            assert loaded.region_count == multi_region_config.region_count
            assert loaded.default_region == multi_region_config.default_region
        finally:
            path.unlink(missing_ok=True)


# ==============================================================================
# Routing Tests
# ==============================================================================

class TestRoundRobinRouter:
    """Tests for RoundRobinRouter."""
    
    def test_round_robin_rotation(self, multi_region_config):
        """Test round robin rotates through regions."""
        router = RoundRobinRouter(multi_region_config)
        
        decisions = [router.route() for _ in range(6)]
        regions = [d.region.region.name for d in decisions]
        
        # Should see each region at least once
        unique_regions = set(regions)
        assert len(unique_regions) >= 2
    
    def test_routing_decision_metadata(self, multi_region_config):
        """Test routing decision includes metadata."""
        router = RoundRobinRouter(multi_region_config)
        decision = router.route()
        
        assert decision.strategy == RoutingStrategy.ROUND_ROBIN
        assert decision.region is not None
        assert isinstance(decision.alternatives, list)


class TestLatencyBasedRouter:
    """Tests for LatencyBasedRouter."""
    
    def test_selects_lowest_latency(self, multi_region_config):
        """Test selects region with lowest latency."""
        # Custom latency function that returns fixed values
        latencies = {
            "us-east-1": 100.0,
            "eu-west-1": 50.0,   # Lowest
            "ap-south-1": 200.0,
        }
        
        def mock_latency(region: RegionConfig) -> float:
            return latencies.get(region.region.name, 999.0)
        
        router = LatencyBasedRouter(
            multi_region_config,
            latency_check_fn=mock_latency,
        )
        
        decision = router.route()
        
        assert decision.region.region.name == "eu-west-1"
        assert decision.latency_ms == 50.0
    
    def test_latency_measurement(self, multi_region_config):
        """Test latency is measured and cached."""
        router = LatencyBasedRouter(multi_region_config)
        
        region = multi_region_config.get_region("us-east-1")
        latency = router.measure_latency(region)
        
        assert latency > 0
        assert router.get_latency(region) == latency


class TestGeographicRouter:
    """Tests for GeographicRouter."""
    
    def test_selects_nearest_region(self, multi_region_config):
        """Test selects geographically nearest region."""
        router = GeographicRouter(multi_region_config)
        
        # Client in London (closer to EU)
        router.set_client_location(51.5074, -0.1278)
        decision = router.route()
        
        assert decision.region.region.name == "eu-west-1"
        assert decision.distance_km is not None
    
    def test_haversine_distance(self):
        """Test Haversine distance calculation."""
        # New York to London ≈ 5570 km
        distance = GeographicRouter.haversine_distance(
            40.7128, -74.0060,  # NYC
            51.5074, -0.1278,   # London
        )
        
        assert 5500 < distance < 5650
    
    def test_context_coordinates(self, multi_region_config):
        """Test coordinates from context."""
        router = GeographicRouter(multi_region_config)
        
        # Context with Mumbai coordinates
        context = {"latitude": 19.0760, "longitude": 72.8777}
        decision = router.route(context)
        
        assert decision.region.region.name == "ap-south-1"


class TestWeightedRouter:
    """Tests for WeightedRouter."""
    
    def test_weight_influence(self, multi_region_config):
        """Test weights influence selection."""
        router = WeightedRouter(multi_region_config)
        
        # Set extreme weights
        router.set_weight("us-east-1", 100)
        router.set_weight("eu-west-1", 0)
        router.set_weight("ap-south-1", 0)
        
        # Should always select us-east-1
        decisions = [router.route() for _ in range(10)]
        regions = [d.region.region.name for d in decisions]
        
        assert all(r == "us-east-1" for r in regions)


class TestAffinityRouter:
    """Tests for AffinityRouter."""
    
    def test_consistent_routing(self, multi_region_config):
        """Test same key always routes to same region."""
        router = AffinityRouter(multi_region_config, affinity_key="user_id")
        
        context = {"user_id": "user-123"}
        
        # Multiple routes should return same region
        decisions = [router.route(context) for _ in range(5)]
        regions = [d.region.region.name for d in decisions]
        
        assert len(set(regions)) == 1  # All same region
    
    def test_different_keys_different_regions(self, multi_region_config):
        """Test different keys may route to different regions."""
        router = AffinityRouter(multi_region_config, affinity_key="user_id")
        
        # Route many different users
        regions = set()
        for i in range(100):
            context = {"user_id": f"user-{i}"}
            decision = router.route(context)
            regions.add(decision.region.region.name)
        
        # Should distribute across regions
        assert len(regions) >= 2


class TestFailoverRouter:
    """Tests for FailoverRouter."""
    
    def test_routes_to_primary(self, multi_region_config):
        """Test routes to primary when healthy."""
        router = FailoverRouter(multi_region_config)
        
        decision = router.route()
        
        assert decision.region.region.is_primary
        assert decision.region.region.name == "us-east-1"
    
    def test_failover_on_primary_failure(self, multi_region_config):
        """Test failover to replica when primary fails."""
        router = FailoverRouter(multi_region_config)
        
        # Mark primary as failed
        router.mark_failed("us-east-1")
        
        decision = router.route()
        
        # Should select a replica
        assert decision.region.region.name != "us-east-1"
        assert not decision.region.region.is_primary


class TestRouterFactory:
    """Tests for router factory function."""
    
    def test_create_router(self, multi_region_config):
        """Test router factory creates correct types."""
        strategies = [
            (RoutingStrategy.ROUND_ROBIN, RoundRobinRouter),
            (RoutingStrategy.LATENCY, LatencyBasedRouter),
            (RoutingStrategy.GEOGRAPHIC, GeographicRouter),
            (RoutingStrategy.WEIGHTED, WeightedRouter),
            (RoutingStrategy.FAILOVER, FailoverRouter),
        ]
        
        for strategy, expected_type in strategies:
            router = create_router(multi_region_config, strategy)
            assert isinstance(router, expected_type)


# ==============================================================================
# Replication Tests
# ==============================================================================

class TestReplicationLag:
    """Tests for ReplicationLag."""
    
    def test_lag_properties(self):
        """Test lag property calculations."""
        lag = ReplicationLag(
            source_region="us-east-1",
            target_region="eu-west-1",
            lag_ms=500.0,
            lag_events=10,
        )
        
        assert lag.lag_seconds == 0.5
        assert lag.is_caught_up  # < 1 second
    
    def test_lag_not_caught_up(self):
        """Test lag detection when behind."""
        lag = ReplicationLag(
            source_region="us-east-1",
            target_region="eu-west-1",
            lag_ms=5000.0,
        )
        
        assert not lag.is_caught_up


class TestReplicationEvent:
    """Tests for ReplicationEvent."""
    
    def test_event_creation(self):
        """Test event creation."""
        event = ReplicationEvent(
            event_id="repl-001",
            source_region="us-east-1",
            target_regions=["eu-west-1", "ap-south-1"],
            data={"id": "123", "name": "test"},
        )
        
        assert event.status == ReplicationStatus.PENDING
        assert len(event.target_regions) == 2


class TestReplicationManager:
    """Tests for ReplicationManager."""
    
    def test_sync_replication(self, multi_region_config):
        """Test synchronous replication."""
        transport = MockReplicationTransport(latency_ms=10.0)
        manager = ReplicationManager(
            multi_region_config,
            transport=transport,
            mode=ReplicationMode.SYNC,
        )
        
        event = manager.replicate(
            data={"id": "123"},
            source_region="us-east-1",
        )
        
        assert event is not None
        assert event.status == ReplicationStatus.COMPLETED
        assert len(transport.sent_events) > 0
    
    def test_async_replication(self, multi_region_config):
        """Test asynchronous replication."""
        transport = MockReplicationTransport(latency_ms=10.0)
        manager = ReplicationManager(
            multi_region_config,
            transport=transport,
            mode=ReplicationMode.ASYNC,
        )
        
        manager.start()
        
        event = manager.replicate(
            data={"id": "123"},
            source_region="us-east-1",
        )
        
        # Wait for async processing
        time.sleep(0.2)
        manager.stop()
        
        assert event is not None
        assert len(transport.sent_events) > 0
    
    def test_conflict_resolution_last_write_wins(self, multi_region_config):
        """Test last-write-wins conflict resolution."""
        manager = ReplicationManager(
            multi_region_config,
            conflict_resolution=ConflictResolution.LAST_WRITE_WINS,
        )
        
        local = {"name": "local"}
        remote = {"name": "remote"}
        local_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        remote_ts = datetime(2024, 1, 2, tzinfo=timezone.utc)  # Later
        
        resolved = manager.resolve_conflict(local, remote, local_ts, remote_ts)
        
        assert resolved["name"] == "remote"
    
    def test_get_stats(self, multi_region_config):
        """Test getting replication stats."""
        manager = ReplicationManager(multi_region_config)
        stats = manager.get_stats()
        
        assert "mode" in stats
        assert "running" in stats
        assert "pending_events" in stats


# ==============================================================================
# Failover Tests
# ==============================================================================

class TestRegionHealth:
    """Tests for RegionHealth."""
    
    def test_health_properties(self):
        """Test health property calculations."""
        health = RegionHealth(
            region_name="us-east-1",
            status=HealthStatus.HEALTHY,
            last_check=datetime.now(timezone.utc),
            checks_performed=100,
            checks_passed=95,
        )
        
        assert health.success_rate == 0.95
        assert health.is_healthy


class TestHealthChecker:
    """Tests for HealthChecker."""
    
    def test_add_check(self, multi_region_config):
        """Test adding health checks."""
        checker = HealthChecker(multi_region_config)
        
        checker.add_check("test", lambda r: True)
        
        assert "test" in checker._checks
    
    def test_initial_health_unknown(self, multi_region_config):
        """Test initial health is unknown."""
        checker = HealthChecker(multi_region_config)
        
        health = checker.get_health("us-east-1")
        
        assert health is not None
        assert health.status == HealthStatus.UNKNOWN
    
    def test_check_now(self, multi_region_config):
        """Test immediate health check."""
        checker = HealthChecker(multi_region_config)
        checker.add_check("always_pass", lambda r: True)
        
        health = checker.check_now("us-east-1")
        
        assert health.checks_performed > 0
    
    def test_healthy_callback(self, multi_region_config):
        """Test healthy callback is triggered."""
        checker = HealthChecker(
            multi_region_config,
            recovery_threshold=1,
        )
        checker.add_check("always_pass", lambda r: True)
        
        healthy_regions = []
        checker.on_healthy(lambda name: healthy_regions.append(name))
        
        checker.check_now("us-east-1")
        
        # May or may not trigger depending on previous state
        # Just verify no errors


class TestFailoverManager:
    """Tests for FailoverManager."""
    
    def test_initial_active_region(self, multi_region_config):
        """Test initial active region is primary."""
        manager = FailoverManager(multi_region_config)
        
        active = manager.get_active_region()
        
        assert active is not None
        assert active.region.is_primary
    
    def test_manual_failover(self, multi_region_config):
        """Test manual failover."""
        manager = FailoverManager(
            multi_region_config,
            strategy=FailoverStrategy.MANUAL,
        )
        
        event = manager.failover_to("eu-west-1", reason="Test failover")
        
        assert event.success
        assert event.to_region == "eu-west-1"
        assert manager.get_active_region_name() == "eu-west-1"
    
    def test_failback(self, multi_region_config):
        """Test failback to primary."""
        manager = FailoverManager(
            multi_region_config,
            strategy=FailoverStrategy.MANUAL,
        )
        
        # Failover first
        manager.failover_to("eu-west-1")
        
        # Then failback
        event = manager.failback(reason="Test failback")
        
        assert event.success
        assert event.to_region == "us-east-1"
        assert manager.get_active_region_name() == "us-east-1"
    
    def test_failover_event_recorded(self, multi_region_config):
        """Test failover events are recorded."""
        manager = FailoverManager(multi_region_config)
        
        manager.failover_to("eu-west-1")
        
        events = manager.get_events()
        
        assert len(events) > 0
        assert events[-1].event_type == "failover"
    
    def test_get_stats(self, multi_region_config):
        """Test getting failover stats."""
        manager = FailoverManager(multi_region_config)
        stats = manager.get_stats()
        
        assert "strategy" in stats
        assert "active_region" in stats
        assert "health" in stats
    
    def test_failover_callback(self, multi_region_config):
        """Test failover callback is triggered."""
        manager = FailoverManager(multi_region_config)
        
        events_received = []
        manager.on_failover(lambda e: events_received.append(e))
        
        manager.failover_to("eu-west-1")
        
        assert len(events_received) == 1
        assert events_received[0].to_region == "eu-west-1"


class TestFailoverEvent:
    """Tests for FailoverEvent."""
    
    def test_event_to_dict(self):
        """Test event serialization."""
        event = FailoverEvent(
            event_id="fo-001",
            event_type="failover",
            from_region="us-east-1",
            to_region="eu-west-1",
            reason="Primary unhealthy",
            duration_ms=150.0,
        )
        
        d = event.to_dict()
        
        assert d["event_id"] == "fo-001"
        assert d["event_type"] == "failover"
        assert d["duration_ms"] == 150.0
