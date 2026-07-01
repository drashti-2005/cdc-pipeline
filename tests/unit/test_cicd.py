"""
CI/CD Module Tests

Tests for version management, configuration, deployment, health checks, and rollback.
"""

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.cicd.version import (
    SemanticVersion,
    VersionBumpType,
    parse_version,
    get_version,
    bump_version,
    compare_versions,
    get_version_info,
)
from src.cicd.config import (
    Environment,
    DatabaseConfig,
    KafkaConfig,
    MonitoringConfig,
    DeploymentConfig,
    get_environment,
    load_config,
)
from src.cicd.deploy import (
    DeploymentStatus,
    DeploymentResult,
    Deployer,
    DockerDeployer,
    KubernetesDeployer,
    create_deployer,
)
from src.cicd.health import (
    HealthCheckResult,
    HealthCheck,
    TCPHealthCheck,
    HTTPHealthCheck,
    CallableHealthCheck,
    DeploymentHealthChecker,
    check_deployment_health,
)
from src.cicd.rollback import (
    Backup,
    RollbackManager,
    create_backup,
)


# =============================================================================
# Version Tests
# =============================================================================

class TestSemanticVersion:
    """Test SemanticVersion class."""
    
    def test_create_version(self):
        """Test creating a version."""
        v = SemanticVersion(1, 2, 3)
        
        assert v.major == 1
        assert v.minor == 2
        assert v.patch == 3
        assert str(v) == "1.2.3"
    
    def test_version_with_prerelease(self):
        """Test version with prerelease tag."""
        v = SemanticVersion(1, 0, 0, prerelease="alpha.1")
        
        assert str(v) == "1.0.0-alpha.1"
        assert v.is_prerelease() is True
        assert v.is_stable() is False
    
    def test_version_with_build(self):
        """Test version with build metadata."""
        v = SemanticVersion(1, 0, 0, build="build.123")
        
        assert str(v) == "1.0.0+build.123"
    
    def test_version_comparison(self):
        """Test version comparison."""
        v1 = SemanticVersion(1, 0, 0)
        v2 = SemanticVersion(1, 1, 0)
        v3 = SemanticVersion(2, 0, 0)
        
        assert v1 < v2
        assert v2 < v3
        assert v1 < v3
    
    def test_prerelease_lower_than_release(self):
        """Test that prerelease is lower than release."""
        pre = SemanticVersion(1, 0, 0, prerelease="alpha.1")
        rel = SemanticVersion(1, 0, 0)
        
        assert pre < rel
    
    def test_bump_major(self):
        """Test major version bump."""
        v = SemanticVersion(1, 2, 3)
        bumped = v.bump(VersionBumpType.MAJOR)
        
        assert str(bumped) == "2.0.0"
    
    def test_bump_minor(self):
        """Test minor version bump."""
        v = SemanticVersion(1, 2, 3)
        bumped = v.bump(VersionBumpType.MINOR)
        
        assert str(bumped) == "1.3.0"
    
    def test_bump_patch(self):
        """Test patch version bump."""
        v = SemanticVersion(1, 2, 3)
        bumped = v.bump(VersionBumpType.PATCH)
        
        assert str(bumped) == "1.2.4"
    
    def test_bump_prerelease(self):
        """Test prerelease bump."""
        v = SemanticVersion(1, 0, 0, prerelease="alpha.1")
        bumped = v.bump(VersionBumpType.PRERELEASE)
        
        assert str(bumped) == "1.0.0-alpha.2"
    
    def test_to_dict(self):
        """Test converting to dictionary."""
        v = SemanticVersion(1, 2, 3, prerelease="beta.1")
        d = v.to_dict()
        
        assert d["major"] == 1
        assert d["minor"] == 2
        assert d["patch"] == 3
        assert d["prerelease"] == "beta.1"
        assert d["version"] == "1.2.3-beta.1"


class TestParseVersion:
    """Test version parsing."""
    
    def test_parse_simple(self):
        """Test parsing simple version."""
        v = parse_version("1.2.3")
        
        assert v.major == 1
        assert v.minor == 2
        assert v.patch == 3
    
    def test_parse_with_v_prefix(self):
        """Test parsing with v prefix."""
        v = parse_version("v2.0.0")
        
        assert v.major == 2
    
    def test_parse_prerelease(self):
        """Test parsing prerelease."""
        v = parse_version("1.0.0-alpha.1")
        
        assert v.prerelease == "alpha.1"
    
    def test_parse_build(self):
        """Test parsing build metadata."""
        v = parse_version("1.0.0+build.123")
        
        assert v.build == "build.123"
    
    def test_parse_full(self):
        """Test parsing full version string."""
        v = parse_version("1.2.3-rc.1+build.456")
        
        assert v.major == 1
        assert v.minor == 2
        assert v.patch == 3
        assert v.prerelease == "rc.1"
        assert v.build == "build.456"
    
    def test_parse_invalid_raises(self):
        """Test that invalid version raises."""
        with pytest.raises(ValueError):
            parse_version("invalid")
        
        with pytest.raises(ValueError):
            parse_version("1.2")


class TestCompareVersions:
    """Test version comparison function."""
    
    def test_compare_less(self):
        """Test comparing less than."""
        assert compare_versions("1.0.0", "2.0.0") == -1
    
    def test_compare_equal(self):
        """Test comparing equal."""
        assert compare_versions("1.0.0", "1.0.0") == 0
    
    def test_compare_greater(self):
        """Test comparing greater than."""
        assert compare_versions("2.0.0", "1.0.0") == 1


# =============================================================================
# Config Tests
# =============================================================================

class TestEnvironment:
    """Test Environment enum."""
    
    def test_from_string(self):
        """Test creating from string."""
        assert Environment.from_string("local") == Environment.LOCAL
        assert Environment.from_string("production") == Environment.PRODUCTION
    
    def test_from_alias(self):
        """Test creating from alias."""
        assert Environment.from_string("dev") == Environment.DEVELOPMENT
        assert Environment.from_string("prod") == Environment.PRODUCTION
    
    def test_invalid_raises(self):
        """Test invalid environment raises."""
        with pytest.raises(ValueError):
            Environment.from_string("invalid")


class TestDatabaseConfig:
    """Test DatabaseConfig."""
    
    def test_defaults(self):
        """Test default values."""
        config = DatabaseConfig()
        
        assert config.host == "localhost"
        assert config.port == 5432
    
    def test_connection_string(self):
        """Test connection string generation."""
        config = DatabaseConfig(
            host="db.example.com",
            port=5432,
            user="user",
            password="pass",
            database="mydb",
        )
        
        conn = config.connection_string(include_password=False)
        assert "***" in conn
        assert "pass" not in conn
        
        conn = config.connection_string(include_password=True)
        assert "pass" in conn
    
    def test_to_dict(self):
        """Test converting to dict (no password)."""
        config = DatabaseConfig(password="secret")
        d = config.to_dict()
        
        assert "password" not in d


class TestDeploymentConfig:
    """Test DeploymentConfig."""
    
    def test_create_config(self):
        """Test creating configuration."""
        config = DeploymentConfig(environment=Environment.LOCAL)
        
        assert config.environment == Environment.LOCAL
        assert config.batch_size == 1000
    
    def test_production_defaults(self):
        """Test production-specific defaults."""
        config = DeploymentConfig(environment=Environment.PRODUCTION)
        
        assert config.monitoring.log_level == "WARNING"
        assert config.replicas == 3
    
    def test_validate_missing_password(self):
        """Test validation catches missing password."""
        config = DeploymentConfig(environment=Environment.LOCAL)
        
        errors = config.validate()
        
        assert len(errors) > 0
        assert any("password" in e.lower() for e in errors)
    
    def test_validate_production_replication(self):
        """Test validation catches low replication factor."""
        config = DeploymentConfig(environment=Environment.PRODUCTION)
        config.source_db.password = "test"
        config.target_db.password = "test"
        config.kafka.replication_factor = 1
        
        errors = config.validate()
        
        assert any("replication" in e.lower() for e in errors)
    
    def test_to_env_vars(self):
        """Test converting to environment variables."""
        config = DeploymentConfig(environment=Environment.LOCAL)
        env = config.to_env_vars()
        
        assert "SOURCE_DB_HOST" in env
        assert "KAFKA_BOOTSTRAP_SERVERS" in env
        assert env["CDC_ENVIRONMENT"] == "local"
    
    def test_to_json(self):
        """Test JSON serialization."""
        config = DeploymentConfig(environment=Environment.LOCAL)
        json_str = config.to_json()
        
        data = json.loads(json_str)
        assert data["environment"] == "local"


class TestLoadConfig:
    """Test configuration loading."""
    
    def test_load_defaults(self):
        """Test loading default config."""
        config = load_config()
        
        assert config is not None
        assert config.environment is not None
    
    def test_load_from_environment(self):
        """Test loading from environment variables."""
        with patch.dict(os.environ, {
            "CDC_ENVIRONMENT": "staging",
            "SOURCE_DB_HOST": "db.test.com",
        }):
            config = load_config()
            
            assert config.environment == Environment.STAGING
            assert config.source_db.host == "db.test.com"


# =============================================================================
# Deploy Tests
# =============================================================================

class TestDeploymentStatus:
    """Test DeploymentStatus enum."""
    
    def test_status_values(self):
        """Test status values exist."""
        assert DeploymentStatus.PENDING
        assert DeploymentStatus.IN_PROGRESS
        assert DeploymentStatus.COMPLETED
        assert DeploymentStatus.FAILED
        assert DeploymentStatus.ROLLED_BACK


class TestDeploymentResult:
    """Test DeploymentResult."""
    
    def test_create_result(self):
        """Test creating result."""
        result = DeploymentResult(
            status=DeploymentStatus.IN_PROGRESS,
            environment=Environment.LOCAL,
            version="1.0.0",
        )
        
        assert result.status == DeploymentStatus.IN_PROGRESS
        assert result.version == "1.0.0"
    
    def test_complete_success(self):
        """Test marking as complete."""
        result = DeploymentResult(
            status=DeploymentStatus.IN_PROGRESS,
            environment=Environment.LOCAL,
            version="1.0.0",
        )
        
        result.complete(DeploymentStatus.COMPLETED, "Done")
        
        assert result.status == DeploymentStatus.COMPLETED
        assert result.completed_at is not None
        assert result.duration_seconds is not None
        assert result.is_success()
    
    def test_complete_failure(self):
        """Test marking as failed."""
        result = DeploymentResult(
            status=DeploymentStatus.IN_PROGRESS,
            environment=Environment.LOCAL,
            version="1.0.0",
        )
        
        result.complete(
            DeploymentStatus.FAILED,
            "Error occurred",
            errors=["Error 1", "Error 2"],
        )
        
        assert result.status == DeploymentStatus.FAILED
        assert not result.is_success()
        assert len(result.errors) == 2
    
    def test_to_dict(self):
        """Test converting to dictionary."""
        result = DeploymentResult(
            status=DeploymentStatus.COMPLETED,
            environment=Environment.LOCAL,
            version="1.0.0",
        )
        
        d = result.to_dict()
        
        assert d["status"] == "completed"
        assert d["version"] == "1.0.0"


class TestCreateDeployer:
    """Test deployer factory."""
    
    def test_create_local_deployer(self):
        """Test creating local deployer."""
        config = DeploymentConfig(environment=Environment.LOCAL)
        deployer = create_deployer(config)
        
        assert isinstance(deployer, DockerDeployer)
    
    def test_create_staging_deployer(self):
        """Test creating staging deployer."""
        config = DeploymentConfig(environment=Environment.STAGING)
        deployer = create_deployer(config)
        
        assert isinstance(deployer, KubernetesDeployer)


# =============================================================================
# Health Tests
# =============================================================================

class TestHealthCheckResult:
    """Test HealthCheckResult."""
    
    def test_create_result(self):
        """Test creating result."""
        result = HealthCheckResult(
            name="test",
            healthy=True,
            message="OK",
            latency_ms=5.0,
        )
        
        assert result.name == "test"
        assert result.healthy is True
    
    def test_to_dict(self):
        """Test converting to dict."""
        result = HealthCheckResult(
            name="test",
            healthy=False,
            message="Failed",
        )
        
        d = result.to_dict()
        
        assert d["name"] == "test"
        assert d["healthy"] is False


class TestTCPHealthCheck:
    """Test TCPHealthCheck."""
    
    def test_check_closed_port(self):
        """Test checking a closed port."""
        check = TCPHealthCheck(
            name="test",
            host="localhost",
            port=59999,  # Unlikely to be open
            timeout=0.5,
        )
        
        result = check.run()
        
        assert result.healthy is False
    
    def test_check_attributes(self):
        """Test check attributes."""
        check = TCPHealthCheck(
            name="test",
            host="localhost",
            port=5432,
            timeout=5.0,
            critical=True,
        )
        
        assert check.name == "test"
        assert check.critical is True


class TestCallableHealthCheck:
    """Test CallableHealthCheck."""
    
    def test_callable_returns_true(self):
        """Test callable that returns True."""
        check = CallableHealthCheck(
            name="test",
            check_fn=lambda: True,
        )
        
        result = check.run()
        
        assert result.healthy is True
    
    def test_callable_returns_false(self):
        """Test callable that returns False."""
        check = CallableHealthCheck(
            name="test",
            check_fn=lambda: False,
        )
        
        result = check.run()
        
        assert result.healthy is False
    
    def test_callable_raises_exception(self):
        """Test callable that raises exception."""
        def failing_check():
            raise RuntimeError("Test error")
        
        check = CallableHealthCheck(
            name="test",
            check_fn=failing_check,
        )
        
        result = check.run()
        
        assert result.healthy is False
        assert "error" in result.message.lower()


class TestDeploymentHealthChecker:
    """Test DeploymentHealthChecker."""
    
    def test_add_check(self):
        """Test adding health checks."""
        config = DeploymentConfig(environment=Environment.LOCAL)
        checker = DeploymentHealthChecker(config)
        
        checker.add_check(CallableHealthCheck("test", lambda: True))
        
        assert len(checker.checks) == 1
    
    def test_run_all(self):
        """Test running all checks."""
        config = DeploymentConfig(environment=Environment.LOCAL)
        checker = DeploymentHealthChecker(config)
        
        checker.add_check(CallableHealthCheck("healthy", lambda: True, critical=True))
        checker.add_check(CallableHealthCheck("unhealthy", lambda: False, critical=False))
        
        result = checker.run_all()
        
        assert result["healthy"] is True  # Critical check passed
        assert result["all_healthy"] is False
        assert result["summary"]["passed"] == 1
        assert result["summary"]["failed"] == 1
    
    def test_critical_failure(self):
        """Test critical check failure."""
        config = DeploymentConfig(environment=Environment.LOCAL)
        checker = DeploymentHealthChecker(config)
        
        checker.add_check(CallableHealthCheck("critical", lambda: False, critical=True))
        
        result = checker.run_all()
        
        assert result["healthy"] is False


# =============================================================================
# Rollback Tests
# =============================================================================

class TestBackup:
    """Test Backup class."""
    
    def test_create_backup(self):
        """Test creating backup."""
        backup = Backup(
            backup_id="backup_001",
            version="1.0.0",
            environment=Environment.LOCAL,
        )
        
        assert backup.backup_id == "backup_001"
        assert backup.version == "1.0.0"
    
    def test_to_dict(self):
        """Test converting to dict."""
        backup = Backup(
            backup_id="backup_001",
            version="1.0.0",
            environment=Environment.LOCAL,
        )
        
        d = backup.to_dict()
        
        assert d["backup_id"] == "backup_001"
        assert d["environment"] == "local"
    
    def test_from_dict(self):
        """Test creating from dict."""
        data = {
            "backup_id": "backup_001",
            "version": "1.0.0",
            "environment": "staging",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        
        backup = Backup.from_dict(data)
        
        assert backup.backup_id == "backup_001"
        assert backup.environment == Environment.STAGING


class TestRollbackManager:
    """Test RollbackManager."""
    
    def test_create_manager(self):
        """Test creating manager."""
        config = DeploymentConfig(environment=Environment.LOCAL)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = RollbackManager(config, backup_dir=Path(tmpdir))
            
            assert manager.backup_dir.exists()
    
    def test_create_and_list_backup(self):
        """Test creating and listing backups."""
        config = DeploymentConfig(environment=Environment.LOCAL)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = RollbackManager(config, backup_dir=Path(tmpdir))
            
            # Create backup
            backup = manager.create_backup("1.0.0", "Test backup")
            
            # List backups
            backups = manager.list_backups()
            
            assert len(backups) == 1
            assert backups[0].version == "1.0.0"
    
    def test_get_backup(self):
        """Test getting specific backup."""
        config = DeploymentConfig(environment=Environment.LOCAL)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = RollbackManager(config, backup_dir=Path(tmpdir))
            
            backup = manager.create_backup("1.0.0")
            retrieved = manager.get_backup(backup.backup_id)
            
            assert retrieved is not None
            assert retrieved.backup_id == backup.backup_id
    
    def test_get_latest_backup(self):
        """Test getting latest backup."""
        config = DeploymentConfig(environment=Environment.LOCAL)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = RollbackManager(config, backup_dir=Path(tmpdir))
            
            manager.create_backup("1.0.0")
            manager.create_backup("2.0.0")
            
            latest = manager.get_latest_backup()
            
            assert latest is not None
            assert latest.version == "2.0.0"
    
    def test_delete_backup(self):
        """Test deleting backup."""
        config = DeploymentConfig(environment=Environment.LOCAL)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = RollbackManager(config, backup_dir=Path(tmpdir))
            
            backup = manager.create_backup("1.0.0")
            manager.delete_backup(backup.backup_id)
            
            assert manager.get_backup(backup.backup_id) is None
    
    def test_cleanup_old_backups(self):
        """Test automatic cleanup of old backups."""
        config = DeploymentConfig(environment=Environment.LOCAL)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = RollbackManager(
                config,
                backup_dir=Path(tmpdir),
                max_backups=2,
            )
            
            # Create 3 backups
            manager.create_backup("1.0.0")
            manager.create_backup("2.0.0")
            manager.create_backup("3.0.0")
            
            backups = manager.list_backups()
            
            # Should only keep 2 most recent
            assert len(backups) == 2
            versions = [b.version for b in backups]
            assert "3.0.0" in versions
            assert "2.0.0" in versions
            assert "1.0.0" not in versions


# =============================================================================
# Integration Tests
# =============================================================================

class TestCICDIntegration:
    """Integration tests for CI/CD module."""
    
    def test_full_deployment_workflow(self):
        """Test complete deployment workflow."""
        config = DeploymentConfig(environment=Environment.LOCAL)
        config.source_db.password = "test"
        config.target_db.password = "test"
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create rollback manager
            manager = RollbackManager(config, backup_dir=Path(tmpdir))
            
            # Create initial backup
            backup = manager.create_backup("1.0.0", "Before deployment")
            
            # Simulate deployment
            result = DeploymentResult(
                status=DeploymentStatus.IN_PROGRESS,
                environment=config.environment,
                version="2.0.0",
            )
            
            # Run health checks
            checker = DeploymentHealthChecker(config)
            checker.add_check(CallableHealthCheck("app", lambda: True))
            health = checker.run_all()
            
            if health["healthy"]:
                result.complete(DeploymentStatus.COMPLETED, "Success")
            else:
                result.complete(DeploymentStatus.FAILED, "Health checks failed")
            
            assert result.is_success()
    
    def test_version_to_config_flow(self):
        """Test version information in config."""
        version = parse_version("1.2.3-rc.1")
        
        config = DeploymentConfig(environment=Environment.LOCAL)
        config.image = f"cdc-pipeline:{version}"
        
        assert "1.2.3-rc.1" in config.image


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
