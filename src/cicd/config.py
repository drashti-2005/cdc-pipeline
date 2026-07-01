"""
Deployment Configuration Module

Manages configuration for different deployment environments.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class Environment(Enum):
    """Deployment environment types."""
    LOCAL = "local"
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    
    @classmethod
    def from_string(cls, value: str) -> "Environment":
        """Create from string value."""
        value = value.lower().strip()
        
        # Handle aliases
        aliases = {
            "dev": cls.DEVELOPMENT,
            "stage": cls.STAGING,
            "prod": cls.PRODUCTION,
        }
        
        if value in aliases:
            return aliases[value]
        
        try:
            return cls(value)
        except ValueError:
            raise ValueError(
                f"Invalid environment: {value}. "
                f"Valid values: {[e.value for e in cls]}"
            )


@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = ""
    database: str = "postgres"
    ssl_mode: str = "prefer"
    pool_size: int = 5
    
    def connection_string(self, include_password: bool = False) -> str:
        """Get connection string."""
        password = self.password if include_password else "***"
        return (
            f"postgresql://{self.user}:{password}@"
            f"{self.host}:{self.port}/{self.database}"
            f"?sslmode={self.ssl_mode}"
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (without password)."""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "database": self.database,
            "ssl_mode": self.ssl_mode,
            "pool_size": self.pool_size,
        }


@dataclass
class KafkaConfig:
    """Kafka configuration."""
    bootstrap_servers: str = "localhost:9092"
    topic_prefix: str = "cdc"
    num_partitions: int = 3
    replication_factor: int = 1
    compression_type: str = "lz4"
    acks: str = "all"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "topic_prefix": self.topic_prefix,
            "num_partitions": self.num_partitions,
            "replication_factor": self.replication_factor,
            "compression_type": self.compression_type,
            "acks": self.acks,
        }


@dataclass
class MonitoringConfig:
    """Monitoring configuration."""
    enabled: bool = True
    metrics_port: int = 8080
    health_check_interval: int = 30
    log_level: str = "INFO"
    log_format: str = "json"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "enabled": self.enabled,
            "metrics_port": self.metrics_port,
            "health_check_interval": self.health_check_interval,
            "log_level": self.log_level,
            "log_format": self.log_format,
        }


@dataclass
class DeploymentConfig:
    """
    Complete deployment configuration.
    
    Contains all settings for a specific environment.
    """
    
    environment: Environment
    source_db: DatabaseConfig = field(default_factory=DatabaseConfig)
    target_db: DatabaseConfig = field(default_factory=DatabaseConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    
    # Pipeline settings
    batch_size: int = 1000
    poll_interval_seconds: float = 1.0
    max_retries: int = 3
    retry_delay_seconds: float = 5.0
    
    # Deployment settings
    replicas: int = 1
    image: str = "cdc-pipeline:latest"
    namespace: str = "cdc"
    
    # Feature flags
    features: Dict[str, bool] = field(default_factory=dict)
    
    def __post_init__(self):
        """Post-initialization setup."""
        # Set environment-specific defaults
        if self.environment == Environment.PRODUCTION:
            self.monitoring.log_level = "WARNING"
            self.replicas = 3
        elif self.environment == Environment.STAGING:
            self.replicas = 2
    
    def validate(self) -> List[str]:
        """
        Validate configuration.
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Check database passwords
        if not self.source_db.password:
            errors.append("Source database password is not set")
        if not self.target_db.password:
            errors.append("Target database password is not set")
        
        # Check Kafka
        if not self.kafka.bootstrap_servers:
            errors.append("Kafka bootstrap servers not configured")
        
        # Production-specific checks
        if self.environment == Environment.PRODUCTION:
            if self.kafka.replication_factor < 2:
                errors.append("Production should have replication_factor >= 2")
            if self.replicas < 2:
                errors.append("Production should have at least 2 replicas")
        
        return errors
    
    def is_valid(self) -> bool:
        """Check if configuration is valid."""
        return len(self.validate()) == 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "environment": self.environment.value,
            "source_db": self.source_db.to_dict(),
            "target_db": self.target_db.to_dict(),
            "kafka": self.kafka.to_dict(),
            "monitoring": self.monitoring.to_dict(),
            "batch_size": self.batch_size,
            "poll_interval_seconds": self.poll_interval_seconds,
            "max_retries": self.max_retries,
            "retry_delay_seconds": self.retry_delay_seconds,
            "replicas": self.replicas,
            "image": self.image,
            "namespace": self.namespace,
            "features": self.features,
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)
    
    def to_env_vars(self) -> Dict[str, str]:
        """Convert to environment variables."""
        return {
            "CDC_ENVIRONMENT": self.environment.value,
            "SOURCE_DB_HOST": self.source_db.host,
            "SOURCE_DB_PORT": str(self.source_db.port),
            "SOURCE_DB_USER": self.source_db.user,
            "SOURCE_DB_PASSWORD": self.source_db.password,
            "SOURCE_DB_NAME": self.source_db.database,
            "TARGET_DB_HOST": self.target_db.host,
            "TARGET_DB_PORT": str(self.target_db.port),
            "TARGET_DB_USER": self.target_db.user,
            "TARGET_DB_PASSWORD": self.target_db.password,
            "TARGET_DB_NAME": self.target_db.database,
            "KAFKA_BOOTSTRAP_SERVERS": self.kafka.bootstrap_servers,
            "KAFKA_TOPIC_PREFIX": self.kafka.topic_prefix,
            "CDC_BATCH_SIZE": str(self.batch_size),
            "CDC_POLL_INTERVAL": str(self.poll_interval_seconds),
            "CDC_LOG_LEVEL": self.monitoring.log_level,
            "CDC_LOG_FORMAT": self.monitoring.log_format,
        }


def get_environment() -> Environment:
    """
    Get current environment from environment variable.
    
    Returns:
        Current Environment
    """
    env_value = os.environ.get("CDC_ENVIRONMENT", "local")
    return Environment.from_string(env_value)


def load_config(
    config_path: Optional[Path] = None,
    environment: Optional[Environment] = None,
) -> DeploymentConfig:
    """
    Load configuration from file or environment.
    
    Args:
        config_path: Optional path to config file
        environment: Optional environment override
        
    Returns:
        DeploymentConfig instance
    """
    env = environment or get_environment()
    
    # Start with defaults for environment
    config = DeploymentConfig(environment=env)
    
    # Load from config file if provided
    if config_path and config_path.exists():
        logger.info(f"Loading config from {config_path}")
        with open(config_path) as f:
            data = json.load(f)
        
        # Update from file
        if "source_db" in data:
            config.source_db = DatabaseConfig(**data["source_db"])
        if "target_db" in data:
            config.target_db = DatabaseConfig(**data["target_db"])
        if "kafka" in data:
            config.kafka = KafkaConfig(**data["kafka"])
        if "monitoring" in data:
            config.monitoring = MonitoringConfig(**data["monitoring"])
        
        for key in ["batch_size", "poll_interval_seconds", "max_retries",
                    "retry_delay_seconds", "replicas", "image", "namespace"]:
            if key in data:
                setattr(config, key, data[key])
    
    # Override from environment variables
    config = _apply_env_overrides(config)
    
    return config


def _apply_env_overrides(config: DeploymentConfig) -> DeploymentConfig:
    """Apply environment variable overrides to config."""
    # Source database
    if os.environ.get("SOURCE_DB_HOST"):
        config.source_db.host = os.environ["SOURCE_DB_HOST"]
    if os.environ.get("SOURCE_DB_PORT"):
        config.source_db.port = int(os.environ["SOURCE_DB_PORT"])
    if os.environ.get("SOURCE_DB_USER"):
        config.source_db.user = os.environ["SOURCE_DB_USER"]
    if os.environ.get("SOURCE_DB_PASSWORD"):
        config.source_db.password = os.environ["SOURCE_DB_PASSWORD"]
    if os.environ.get("SOURCE_DB_NAME"):
        config.source_db.database = os.environ["SOURCE_DB_NAME"]
    
    # Target database
    if os.environ.get("TARGET_DB_HOST"):
        config.target_db.host = os.environ["TARGET_DB_HOST"]
    if os.environ.get("TARGET_DB_PORT"):
        config.target_db.port = int(os.environ["TARGET_DB_PORT"])
    if os.environ.get("TARGET_DB_USER"):
        config.target_db.user = os.environ["TARGET_DB_USER"]
    if os.environ.get("TARGET_DB_PASSWORD"):
        config.target_db.password = os.environ["TARGET_DB_PASSWORD"]
    if os.environ.get("TARGET_DB_NAME"):
        config.target_db.database = os.environ["TARGET_DB_NAME"]
    
    # Kafka
    if os.environ.get("KAFKA_BOOTSTRAP_SERVERS"):
        config.kafka.bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    if os.environ.get("KAFKA_TOPIC_PREFIX"):
        config.kafka.topic_prefix = os.environ["KAFKA_TOPIC_PREFIX"]
    
    # Pipeline settings
    if os.environ.get("CDC_BATCH_SIZE"):
        config.batch_size = int(os.environ["CDC_BATCH_SIZE"])
    if os.environ.get("CDC_POLL_INTERVAL"):
        config.poll_interval_seconds = float(os.environ["CDC_POLL_INTERVAL"])
    
    # Monitoring
    if os.environ.get("CDC_LOG_LEVEL"):
        config.monitoring.log_level = os.environ["CDC_LOG_LEVEL"]
    if os.environ.get("CDC_LOG_FORMAT"):
        config.monitoring.log_format = os.environ["CDC_LOG_FORMAT"]
    
    return config


def create_config_file(
    config: DeploymentConfig,
    output_path: Path,
) -> None:
    """
    Write configuration to file.
    
    Args:
        config: Configuration to write
        output_path: Path to output file
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(config.to_json())
    logger.info(f"Wrote configuration to {output_path}")
