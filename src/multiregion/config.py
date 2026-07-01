"""
Multi-Region Configuration

Defines region configuration and management:
- Region: Single region definition
- RegionConfig: Configuration for a region
- MultiRegionConfig: Global multi-region settings

SIMPLE EXPLANATION:
Think of regions like branch offices:
- Each office has its own address and resources
- Some offices are "primary" (main headquarters)
- Others are "replicas" (backup offices)
- We need to know which office is closest to each customer
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Set

logger = logging.getLogger(__name__)


class RegionStatus(Enum):
    """
    Status of a region.
    
    ACTIVE: Region is healthy and accepting traffic
    DEGRADED: Region has issues but still operational
    INACTIVE: Region is not accepting traffic
    FAILING_OVER: Region is in failover process
    MAINTENANCE: Region is under planned maintenance
    """
    
    ACTIVE = "active"
    DEGRADED = "degraded"
    INACTIVE = "inactive"
    FAILING_OVER = "failing_over"
    MAINTENANCE = "maintenance"


@dataclass
class Region:
    """
    A geographic region for the CDC pipeline.
    
    SIMPLE EXPLANATION:
    A region is like a data center location:
    - us-east-1: Virginia, USA
    - eu-west-1: Ireland, Europe
    - ap-south-1: Mumbai, India
    
    Each region can run independently and replicate to others.
    """
    
    name: str                          # e.g., "us-east-1", "eu-west-1"
    display_name: str                  # e.g., "US East (Virginia)"
    is_primary: bool = False           # Is this the primary region?
    status: RegionStatus = RegionStatus.ACTIVE
    priority: int = 100                # Lower = higher priority for failover
    latitude: float = 0.0              # Geographic coordinates
    longitude: float = 0.0
    tags: Dict[str, str] = field(default_factory=dict)
    
    def __hash__(self) -> int:
        return hash(self.name)
    
    def __eq__(self, other: object) -> bool:
        if isinstance(other, Region):
            return self.name == other.name
        return False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "display_name": self.display_name,
            "is_primary": self.is_primary,
            "status": self.status.value,
            "priority": self.priority,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "tags": self.tags,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Region":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            display_name=data.get("display_name", data["name"]),
            is_primary=data.get("is_primary", False),
            status=RegionStatus(data.get("status", "active")),
            priority=data.get("priority", 100),
            latitude=data.get("latitude", 0.0),
            longitude=data.get("longitude", 0.0),
            tags=data.get("tags", {}),
        )


@dataclass
class KafkaEndpoint:
    """Kafka connection endpoint for a region."""
    
    bootstrap_servers: str
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for kafka-python."""
        config = {
            "bootstrap_servers": self.bootstrap_servers,
            "security_protocol": self.security_protocol,
        }
        
        if self.sasl_mechanism:
            config["sasl_mechanism"] = self.sasl_mechanism
            config["sasl_plain_username"] = self.sasl_username
            config["sasl_plain_password"] = self.sasl_password
        
        if self.ssl_cafile:
            config["ssl_cafile"] = self.ssl_cafile
        if self.ssl_certfile:
            config["ssl_certfile"] = self.ssl_certfile
        if self.ssl_keyfile:
            config["ssl_keyfile"] = self.ssl_keyfile
        
        return config


@dataclass
class DatabaseEndpoint:
    """Database connection endpoint for a region."""
    
    host: str
    port: int
    database: str
    username: str
    password: str
    ssl_mode: str = "prefer"
    
    def connection_string(self) -> str:
        """Get connection string."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?sslmode={self.ssl_mode}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "username": self.username,
            "password": self.password,
            "ssl_mode": self.ssl_mode,
        }


@dataclass
class StorageEndpoint:
    """Object storage endpoint for a region (MinIO/S3)."""
    
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    secure: bool = True
    region: str = "us-east-1"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "endpoint": self.endpoint,
            "access_key": self.access_key,
            "secret_key": self.secret_key,
            "bucket": self.bucket,
            "secure": self.secure,
            "region": self.region,
        }


@dataclass
class RegionConfig:
    """
    Complete configuration for a region.
    
    Includes all endpoints and settings needed to operate in this region.
    """
    
    region: Region
    kafka: KafkaEndpoint
    source_database: DatabaseEndpoint
    target_database: DatabaseEndpoint
    storage: StorageEndpoint
    
    # Replication settings
    replication_enabled: bool = True
    replication_targets: List[str] = field(default_factory=list)  # Region names
    
    # Performance settings
    max_connections: int = 100
    connection_timeout_ms: int = 30000
    request_timeout_ms: int = 60000
    
    # Monitoring
    metrics_enabled: bool = True
    metrics_port: int = 9090
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "region": self.region.to_dict(),
            "kafka": {
                "bootstrap_servers": self.kafka.bootstrap_servers,
                "security_protocol": self.kafka.security_protocol,
            },
            "source_database": self.source_database.to_dict(),
            "target_database": self.target_database.to_dict(),
            "storage": self.storage.to_dict(),
            "replication_enabled": self.replication_enabled,
            "replication_targets": self.replication_targets,
            "max_connections": self.max_connections,
            "connection_timeout_ms": self.connection_timeout_ms,
            "request_timeout_ms": self.request_timeout_ms,
            "metrics_enabled": self.metrics_enabled,
            "metrics_port": self.metrics_port,
        }


@dataclass
class MultiRegionConfig:
    """
    Global multi-region configuration.
    
    Manages all regions and provides lookup utilities.
    
    SIMPLE EXPLANATION:
    This is like the company directory:
    - Lists all offices (regions)
    - Knows which is headquarters (primary)
    - Tracks relationships between offices
    """
    
    regions: Dict[str, RegionConfig] = field(default_factory=dict)
    default_region: Optional[str] = None
    replication_mode: str = "async"  # async, sync, none
    failover_enabled: bool = True
    failover_timeout_seconds: int = 30
    health_check_interval_seconds: int = 10
    
    def add_region(self, config: RegionConfig) -> None:
        """Add a region configuration."""
        self.regions[config.region.name] = config
        
        # Set as default if primary
        if config.region.is_primary:
            self.default_region = config.region.name
        
        logger.info(f"Added region: {config.region.name}")
    
    def get_region(self, name: str) -> Optional[RegionConfig]:
        """Get region configuration by name."""
        return self.regions.get(name)
    
    def get_primary(self) -> Optional[RegionConfig]:
        """Get primary region configuration."""
        for config in self.regions.values():
            if config.region.is_primary:
                return config
        return None
    
    def get_active_regions(self) -> List[RegionConfig]:
        """Get all active regions."""
        return [
            config for config in self.regions.values()
            if config.region.status == RegionStatus.ACTIVE
        ]
    
    def get_replica_regions(self) -> List[RegionConfig]:
        """Get all replica (non-primary) regions."""
        return [
            config for config in self.regions.values()
            if not config.region.is_primary
        ]
    
    def get_regions_by_priority(self) -> List[RegionConfig]:
        """Get regions sorted by priority (lowest first)."""
        return sorted(
            self.regions.values(),
            key=lambda c: c.region.priority
        )
    
    @property
    def region_names(self) -> Set[str]:
        """Get all region names."""
        return set(self.regions.keys())
    
    @property
    def region_count(self) -> int:
        """Get number of regions."""
        return len(self.regions)
    
    def validate(self) -> List[str]:
        """
        Validate configuration.
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        if not self.regions:
            errors.append("No regions configured")
        
        # Check for exactly one primary
        primaries = [c for c in self.regions.values() if c.region.is_primary]
        if len(primaries) == 0:
            errors.append("No primary region configured")
        elif len(primaries) > 1:
            errors.append(f"Multiple primary regions: {[p.region.name for p in primaries]}")
        
        # Check replication targets exist
        for config in self.regions.values():
            for target in config.replication_targets:
                if target not in self.regions:
                    errors.append(
                        f"Region {config.region.name} has invalid replication target: {target}"
                    )
        
        return errors
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "regions": {name: config.to_dict() for name, config in self.regions.items()},
            "default_region": self.default_region,
            "replication_mode": self.replication_mode,
            "failover_enabled": self.failover_enabled,
            "failover_timeout_seconds": self.failover_timeout_seconds,
            "health_check_interval_seconds": self.health_check_interval_seconds,
        }
    
    def save(self, path: Path) -> None:
        """Save configuration to file."""
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info(f"Saved multi-region config to {path}")


def load_region_config(path: Path) -> MultiRegionConfig:
    """
    Load multi-region configuration from file.
    
    Args:
        path: Path to configuration file (JSON)
        
    Returns:
        MultiRegionConfig instance
    """
    with open(path) as f:
        data = json.load(f)
    
    config = MultiRegionConfig(
        default_region=data.get("default_region"),
        replication_mode=data.get("replication_mode", "async"),
        failover_enabled=data.get("failover_enabled", True),
        failover_timeout_seconds=data.get("failover_timeout_seconds", 30),
        health_check_interval_seconds=data.get("health_check_interval_seconds", 10),
    )
    
    # Load regions
    for name, region_data in data.get("regions", {}).items():
        region = Region.from_dict(region_data.get("region", {"name": name}))
        
        kafka_data = region_data.get("kafka", {})
        kafka = KafkaEndpoint(
            bootstrap_servers=kafka_data.get("bootstrap_servers", "localhost:9092"),
            security_protocol=kafka_data.get("security_protocol", "PLAINTEXT"),
        )
        
        src_db = region_data.get("source_database", {})
        source_db = DatabaseEndpoint(
            host=src_db.get("host", "localhost"),
            port=src_db.get("port", 5432),
            database=src_db.get("database", "source"),
            username=src_db.get("username", "postgres"),
            password=src_db.get("password", ""),
        )
        
        tgt_db = region_data.get("target_database", {})
        target_db = DatabaseEndpoint(
            host=tgt_db.get("host", "localhost"),
            port=tgt_db.get("port", 5433),
            database=tgt_db.get("database", "target"),
            username=tgt_db.get("username", "postgres"),
            password=tgt_db.get("password", ""),
        )
        
        storage_data = region_data.get("storage", {})
        storage = StorageEndpoint(
            endpoint=storage_data.get("endpoint", "localhost:9000"),
            access_key=storage_data.get("access_key", ""),
            secret_key=storage_data.get("secret_key", ""),
            bucket=storage_data.get("bucket", "cdc-events"),
        )
        
        region_config = RegionConfig(
            region=region,
            kafka=kafka,
            source_database=source_db,
            target_database=target_db,
            storage=storage,
            replication_enabled=region_data.get("replication_enabled", True),
            replication_targets=region_data.get("replication_targets", []),
        )
        
        config.add_region(region_config)
    
    logger.info(f"Loaded multi-region config from {path}: {config.region_count} regions")
    return config


# Pre-defined region templates
REGION_TEMPLATES = {
    "us-east-1": Region(
        name="us-east-1",
        display_name="US East (N. Virginia)",
        latitude=37.4316,
        longitude=-78.6569,
    ),
    "us-west-2": Region(
        name="us-west-2",
        display_name="US West (Oregon)",
        latitude=45.8399,
        longitude=-119.7006,
    ),
    "eu-west-1": Region(
        name="eu-west-1",
        display_name="EU West (Ireland)",
        latitude=53.3331,
        longitude=-6.2489,
    ),
    "eu-central-1": Region(
        name="eu-central-1",
        display_name="EU Central (Frankfurt)",
        latitude=50.1109,
        longitude=8.6821,
    ),
    "ap-south-1": Region(
        name="ap-south-1",
        display_name="Asia Pacific (Mumbai)",
        latitude=19.0760,
        longitude=72.8777,
    ),
    "ap-northeast-1": Region(
        name="ap-northeast-1",
        display_name="Asia Pacific (Tokyo)",
        latitude=35.6762,
        longitude=139.6503,
    ),
    "ap-southeast-1": Region(
        name="ap-southeast-1",
        display_name="Asia Pacific (Singapore)",
        latitude=1.3521,
        longitude=103.8198,
    ),
}


def create_default_config() -> MultiRegionConfig:
    """
    Create a default multi-region configuration for development.
    
    Sets up three regions:
    - us-east-1 (primary)
    - eu-west-1 (replica)
    - ap-south-1 (replica)
    """
    config = MultiRegionConfig(
        replication_mode="async",
        failover_enabled=True,
    )
    
    # US East - Primary
    us_east = RegionConfig(
        region=Region(
            name="us-east-1",
            display_name="US East (N. Virginia)",
            is_primary=True,
            priority=1,
            latitude=37.4316,
            longitude=-78.6569,
        ),
        kafka=KafkaEndpoint(bootstrap_servers="localhost:9092"),
        source_database=DatabaseEndpoint(
            host="localhost", port=5434, database="source_db",
            username="postgres", password="postgres"
        ),
        target_database=DatabaseEndpoint(
            host="localhost", port=5435, database="target_db",
            username="postgres", password="postgres"
        ),
        storage=StorageEndpoint(
            endpoint="localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket="cdc-events-us-east",
        ),
        replication_targets=["eu-west-1", "ap-south-1"],
    )
    config.add_region(us_east)
    
    # EU West - Replica
    eu_west = RegionConfig(
        region=Region(
            name="eu-west-1",
            display_name="EU West (Ireland)",
            is_primary=False,
            priority=2,
            latitude=53.3331,
            longitude=-6.2489,
        ),
        kafka=KafkaEndpoint(bootstrap_servers="localhost:9093"),
        source_database=DatabaseEndpoint(
            host="localhost", port=5436, database="source_db",
            username="postgres", password="postgres"
        ),
        target_database=DatabaseEndpoint(
            host="localhost", port=5437, database="target_db",
            username="postgres", password="postgres"
        ),
        storage=StorageEndpoint(
            endpoint="localhost:9001",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket="cdc-events-eu-west",
        ),
        replication_targets=["us-east-1"],
    )
    config.add_region(eu_west)
    
    # AP South - Replica
    ap_south = RegionConfig(
        region=Region(
            name="ap-south-1",
            display_name="Asia Pacific (Mumbai)",
            is_primary=False,
            priority=3,
            latitude=19.0760,
            longitude=72.8777,
        ),
        kafka=KafkaEndpoint(bootstrap_servers="localhost:9094"),
        source_database=DatabaseEndpoint(
            host="localhost", port=5438, database="source_db",
            username="postgres", password="postgres"
        ),
        target_database=DatabaseEndpoint(
            host="localhost", port=5439, database="target_db",
            username="postgres", password="postgres"
        ),
        storage=StorageEndpoint(
            endpoint="localhost:9002",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket="cdc-events-ap-south",
        ),
        replication_targets=["us-east-1"],
    )
    config.add_region(ap_south)
    
    return config
