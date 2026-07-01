"""
Deployment Module

Provides deployment abstractions for different platforms.
"""

import json
import logging
import subprocess
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from cicd.config import DeploymentConfig, Environment

logger = logging.getLogger(__name__)


class DeploymentStatus(Enum):
    """Deployment status states."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass
class DeploymentResult:
    """
    Result of a deployment operation.
    """
    
    status: DeploymentStatus
    environment: Environment
    version: str
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    
    def complete(
        self,
        status: DeploymentStatus,
        message: str = "",
        errors: Optional[List[str]] = None,
    ) -> None:
        """Mark deployment as complete."""
        self.status = status
        self.completed_at = datetime.now(timezone.utc)
        self.duration_seconds = (self.completed_at - self.started_at).total_seconds()
        self.message = message
        
        if errors:
            self.errors = errors
    
    def is_success(self) -> bool:
        """Check if deployment was successful."""
        return self.status == DeploymentStatus.COMPLETED
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "status": self.status.value,
            "environment": self.environment.value,
            "version": self.version,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "message": self.message,
            "details": self.details,
            "errors": self.errors,
        }


class Deployer(ABC):
    """
    Abstract base class for deployers.
    """
    
    def __init__(self, config: DeploymentConfig):
        """
        Initialize deployer.
        
        Args:
            config: Deployment configuration
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def deploy(self, version: str) -> DeploymentResult:
        """
        Deploy a specific version.
        
        Args:
            version: Version to deploy
            
        Returns:
            DeploymentResult
        """
        pass
    
    @abstractmethod
    def rollback(self, version: str) -> DeploymentResult:
        """
        Rollback to a specific version.
        
        Args:
            version: Version to rollback to
            
        Returns:
            DeploymentResult
        """
        pass
    
    @abstractmethod
    def get_current_version(self) -> Optional[str]:
        """
        Get currently deployed version.
        
        Returns:
            Current version or None
        """
        pass
    
    @abstractmethod
    def get_status(self) -> Dict[str, Any]:
        """
        Get deployment status.
        
        Returns:
            Status dictionary
        """
        pass
    
    def pre_deploy_checks(self) -> List[str]:
        """
        Run pre-deployment checks.
        
        Returns:
            List of errors (empty if all checks pass)
        """
        errors = self.config.validate()
        
        if errors:
            self.logger.warning(f"Configuration validation errors: {errors}")
        
        return errors


class DockerDeployer(Deployer):
    """
    Deployer for Docker/Docker Compose environments.
    """
    
    def __init__(
        self,
        config: DeploymentConfig,
        compose_file: Path = Path("docker/docker-compose.yml"),
        project_name: str = "cdc-pipeline",
    ):
        """
        Initialize Docker deployer.
        
        Args:
            config: Deployment configuration
            compose_file: Path to docker-compose.yml
            project_name: Docker Compose project name
        """
        super().__init__(config)
        self.compose_file = compose_file
        self.project_name = project_name
    
    def _run_compose(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        """Run docker-compose command."""
        cmd = [
            "docker-compose",
            "-f", str(self.compose_file),
            "-p", self.project_name,
            *args,
        ]
        
        self.logger.info(f"Running: {' '.join(cmd)}")
        
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=check,
        )
    
    def deploy(self, version: str) -> DeploymentResult:
        """Deploy using Docker Compose."""
        result = DeploymentResult(
            status=DeploymentStatus.IN_PROGRESS,
            environment=self.config.environment,
            version=version,
        )
        
        try:
            # Pre-deploy checks
            errors = self.pre_deploy_checks()
            if errors:
                result.complete(
                    DeploymentStatus.FAILED,
                    "Pre-deployment checks failed",
                    errors,
                )
                return result
            
            # Pull latest images
            self.logger.info("Pulling latest images...")
            self._run_compose("pull")
            
            # Stop existing containers
            self.logger.info("Stopping existing containers...")
            self._run_compose("down", "--remove-orphans", check=False)
            
            # Start new containers
            self.logger.info("Starting containers...")
            self._run_compose("up", "-d", "--build")
            
            # Wait for health
            self.logger.info("Waiting for services to be healthy...")
            time.sleep(10)
            
            # Check status
            ps_result = self._run_compose("ps", "--format", "json", check=False)
            
            result.details["containers"] = ps_result.stdout
            result.complete(
                DeploymentStatus.COMPLETED,
                f"Successfully deployed version {version}",
            )
            
        except subprocess.CalledProcessError as e:
            result.complete(
                DeploymentStatus.FAILED,
                f"Deployment failed: {e.stderr}",
                [str(e)],
            )
        except Exception as e:
            result.complete(
                DeploymentStatus.FAILED,
                f"Unexpected error: {str(e)}",
                [str(e)],
            )
        
        return result
    
    def rollback(self, version: str) -> DeploymentResult:
        """Rollback Docker deployment."""
        # For Docker, rollback is essentially a deploy of the previous version
        self.logger.info(f"Rolling back to version {version}")
        return self.deploy(version)
    
    def get_current_version(self) -> Optional[str]:
        """Get current deployed version from container labels."""
        try:
            result = subprocess.run(
                [
                    "docker", "inspect",
                    f"{self.project_name}-cdc-pipeline-1",
                    "--format", "{{.Config.Labels.version}}",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip() or None
        except subprocess.CalledProcessError:
            return None
    
    def get_status(self) -> Dict[str, Any]:
        """Get Docker deployment status."""
        try:
            result = self._run_compose("ps", "--format", "json", check=False)
            
            return {
                "running": "Up" in result.stdout,
                "containers": result.stdout,
                "environment": self.config.environment.value,
            }
        except Exception as e:
            return {
                "running": False,
                "error": str(e),
            }


class KubernetesDeployer(Deployer):
    """
    Deployer for Kubernetes environments.
    """
    
    def __init__(
        self,
        config: DeploymentConfig,
        manifest_dir: Optional[Path] = None,
        kubeconfig: Optional[Path] = None,
    ):
        """
        Initialize Kubernetes deployer.
        
        Args:
            config: Deployment configuration
            manifest_dir: Path to Kubernetes manifests
            kubeconfig: Path to kubeconfig file
        """
        super().__init__(config)
        
        self.manifest_dir = manifest_dir or Path(
            f"deploy/k8s/{config.environment.value}"
        )
        self.kubeconfig = kubeconfig
        self.namespace = config.namespace
    
    def _run_kubectl(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        """Run kubectl command."""
        cmd = ["kubectl"]
        
        if self.kubeconfig:
            cmd.extend(["--kubeconfig", str(self.kubeconfig)])
        
        cmd.extend(["-n", self.namespace])
        cmd.extend(args)
        
        self.logger.info(f"Running: {' '.join(cmd)}")
        
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=check,
        )
    
    def deploy(self, version: str) -> DeploymentResult:
        """Deploy to Kubernetes."""
        result = DeploymentResult(
            status=DeploymentStatus.IN_PROGRESS,
            environment=self.config.environment,
            version=version,
        )
        
        try:
            # Pre-deploy checks
            errors = self.pre_deploy_checks()
            if errors:
                result.complete(
                    DeploymentStatus.FAILED,
                    "Pre-deployment checks failed",
                    errors,
                )
                return result
            
            # Apply manifests
            self.logger.info("Applying Kubernetes manifests...")
            
            if self.manifest_dir.exists():
                self._run_kubectl("apply", "-f", str(self.manifest_dir))
            
            # Update image
            image = f"{self.config.image.split(':')[0]}:{version}"
            self.logger.info(f"Updating image to {image}...")
            
            self._run_kubectl(
                "set", "image",
                "deployment/cdc-pipeline",
                f"cdc-pipeline={image}",
                check=False,
            )
            
            # Wait for rollout
            self.logger.info("Waiting for rollout...")
            rollout_result = self._run_kubectl(
                "rollout", "status",
                "deployment/cdc-pipeline",
                "--timeout=300s",
                check=False,
            )
            
            if rollout_result.returncode == 0:
                result.complete(
                    DeploymentStatus.COMPLETED,
                    f"Successfully deployed version {version}",
                )
            else:
                result.complete(
                    DeploymentStatus.FAILED,
                    "Rollout failed or timed out",
                    [rollout_result.stderr],
                )
            
        except subprocess.CalledProcessError as e:
            result.complete(
                DeploymentStatus.FAILED,
                f"Deployment failed: {e.stderr}",
                [str(e)],
            )
        except Exception as e:
            result.complete(
                DeploymentStatus.FAILED,
                f"Unexpected error: {str(e)}",
                [str(e)],
            )
        
        return result
    
    def rollback(self, version: str) -> DeploymentResult:
        """Rollback Kubernetes deployment."""
        result = DeploymentResult(
            status=DeploymentStatus.IN_PROGRESS,
            environment=self.config.environment,
            version=version,
        )
        
        try:
            self.logger.info("Rolling back deployment...")
            
            # Use kubectl rollout undo or deploy specific version
            if version == "previous":
                self._run_kubectl(
                    "rollout", "undo",
                    "deployment/cdc-pipeline",
                )
            else:
                # Deploy specific version
                return self.deploy(version)
            
            # Wait for rollback
            rollout_result = self._run_kubectl(
                "rollout", "status",
                "deployment/cdc-pipeline",
                "--timeout=300s",
                check=False,
            )
            
            if rollout_result.returncode == 0:
                result.complete(
                    DeploymentStatus.ROLLED_BACK,
                    f"Successfully rolled back to {version}",
                )
            else:
                result.complete(
                    DeploymentStatus.FAILED,
                    "Rollback failed",
                    [rollout_result.stderr],
                )
            
        except Exception as e:
            result.complete(
                DeploymentStatus.FAILED,
                f"Rollback failed: {str(e)}",
                [str(e)],
            )
        
        return result
    
    def get_current_version(self) -> Optional[str]:
        """Get current deployed version from deployment."""
        try:
            result = self._run_kubectl(
                "get", "deployment", "cdc-pipeline",
                "-o", "jsonpath={.spec.template.spec.containers[0].image}",
                check=False,
            )
            
            if result.returncode == 0 and result.stdout:
                # Extract version from image tag
                image = result.stdout.strip()
                if ":" in image:
                    return image.split(":")[-1]
            
            return None
        except Exception:
            return None
    
    def get_status(self) -> Dict[str, Any]:
        """Get Kubernetes deployment status."""
        try:
            # Get deployment status
            result = self._run_kubectl(
                "get", "deployment", "cdc-pipeline",
                "-o", "json",
                check=False,
            )
            
            if result.returncode == 0:
                deployment = json.loads(result.stdout)
                status = deployment.get("status", {})
                
                return {
                    "running": status.get("readyReplicas", 0) > 0,
                    "ready_replicas": status.get("readyReplicas", 0),
                    "desired_replicas": status.get("replicas", 0),
                    "available_replicas": status.get("availableReplicas", 0),
                    "environment": self.config.environment.value,
                    "namespace": self.namespace,
                }
            
            return {
                "running": False,
                "error": result.stderr,
            }
            
        except Exception as e:
            return {
                "running": False,
                "error": str(e),
            }
    
    def scale(self, replicas: int) -> bool:
        """
        Scale deployment.
        
        Args:
            replicas: Number of replicas
            
        Returns:
            True if successful
        """
        try:
            self._run_kubectl(
                "scale", "deployment", "cdc-pipeline",
                f"--replicas={replicas}",
            )
            return True
        except subprocess.CalledProcessError:
            return False


def create_deployer(config: DeploymentConfig) -> Deployer:
    """
    Factory function to create appropriate deployer.
    
    Args:
        config: Deployment configuration
        
    Returns:
        Appropriate Deployer instance
    """
    if config.environment == Environment.LOCAL:
        return DockerDeployer(config)
    else:
        return KubernetesDeployer(config)
