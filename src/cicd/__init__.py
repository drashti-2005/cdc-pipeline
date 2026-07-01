"""
CI/CD Module

Provides utilities for continuous integration and deployment,
including version management, deployment configuration, and
environment management.
"""

from .version import (
    SemanticVersion,
    get_version,
    bump_version,
    parse_version,
)
from .config import (
    Environment,
    DeploymentConfig,
    load_config,
    get_environment,
)
from .deploy import (
    DeploymentStatus,
    DeploymentResult,
    Deployer,
    DockerDeployer,
    KubernetesDeployer,
)
from .health import (
    HealthCheck,
    DeploymentHealthChecker,
    check_deployment_health,
)
from .rollback import (
    RollbackManager,
    create_backup,
    restore_backup,
)

__all__ = [
    # Version
    "SemanticVersion",
    "get_version",
    "bump_version",
    "parse_version",
    # Config
    "Environment",
    "DeploymentConfig",
    "load_config",
    "get_environment",
    # Deploy
    "DeploymentStatus",
    "DeploymentResult",
    "Deployer",
    "DockerDeployer",
    "KubernetesDeployer",
    # Health
    "HealthCheck",
    "DeploymentHealthChecker",
    "check_deployment_health",
    # Rollback
    "RollbackManager",
    "create_backup",
    "restore_backup",
]
