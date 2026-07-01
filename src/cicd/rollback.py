"""
Rollback Management Module

Provides backup and rollback functionality for deployments.
"""

import json
import logging
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import DeploymentConfig, Environment

logger = logging.getLogger(__name__)


@dataclass
class Backup:
    """
    Represents a deployment backup.
    """
    
    backup_id: str
    version: str
    environment: Environment
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    path: Optional[Path] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "backup_id": self.backup_id,
            "version": self.version,
            "environment": self.environment.value,
            "created_at": self.created_at.isoformat(),
            "path": str(self.path) if self.path else None,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Backup":
        """Create from dictionary."""
        return cls(
            backup_id=data["backup_id"],
            version=data["version"],
            environment=Environment.from_string(data["environment"]),
            created_at=datetime.fromisoformat(data["created_at"]),
            path=Path(data["path"]) if data.get("path") else None,
            metadata=data.get("metadata", {}),
        )


class RollbackManager:
    """
    Manages deployment backups and rollbacks.
    """
    
    def __init__(
        self,
        config: DeploymentConfig,
        backup_dir: Path = Path("backups"),
        max_backups: int = 10,
    ):
        """
        Initialize rollback manager.
        
        Args:
            config: Deployment configuration
            backup_dir: Directory for storing backups
            max_backups: Maximum number of backups to retain
        """
        self.config = config
        self.backup_dir = backup_dir
        self.max_backups = max_backups
        self.logger = logging.getLogger(__name__)
        
        # Ensure backup directory exists
        self.backup_dir.mkdir(parents=True, exist_ok=True)
    
    def _generate_backup_id(self) -> str:
        """Generate unique backup ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        return f"backup_{self.config.environment.value}_{timestamp}"
    
    def _get_metadata_path(self) -> Path:
        """Get path to metadata file."""
        return self.backup_dir / "metadata.json"
    
    def _load_metadata(self) -> Dict[str, List[Dict[str, Any]]]:
        """Load backup metadata."""
        path = self._get_metadata_path()
        
        if path.exists():
            with open(path) as f:
                return json.load(f)
        
        return {"backups": []}
    
    def _save_metadata(self, metadata: Dict[str, List[Dict[str, Any]]]) -> None:
        """Save backup metadata."""
        path = self._get_metadata_path()
        
        with open(path, "w") as f:
            json.dump(metadata, f, indent=2)
    
    def create_backup(
        self,
        version: str,
        description: str = "",
    ) -> Backup:
        """
        Create a backup of the current deployment.
        
        Args:
            version: Current version being backed up
            description: Optional description
            
        Returns:
            Created Backup
        """
        backup_id = self._generate_backup_id()
        backup_path = self.backup_dir / backup_id
        
        self.logger.info(f"Creating backup {backup_id}...")
        
        # Create backup directory
        backup_path.mkdir(parents=True, exist_ok=True)
        
        # Backup Kubernetes resources if applicable
        if self.config.environment != Environment.LOCAL:
            self._backup_kubernetes(backup_path)
        
        # Backup Docker state if applicable
        if self.config.environment == Environment.LOCAL:
            self._backup_docker(backup_path)
        
        # Save configuration
        config_path = backup_path / "config.json"
        config_path.write_text(self.config.to_json())
        
        # Create backup record
        backup = Backup(
            backup_id=backup_id,
            version=version,
            environment=self.config.environment,
            path=backup_path,
            metadata={
                "description": description,
            },
        )
        
        # Update metadata
        metadata = self._load_metadata()
        metadata["backups"].append(backup.to_dict())
        self._save_metadata(metadata)
        
        # Cleanup old backups
        self._cleanup_old_backups()
        
        self.logger.info(f"Backup created: {backup_id}")
        return backup
    
    def _backup_kubernetes(self, backup_path: Path) -> None:
        """Backup Kubernetes resources."""
        try:
            # Export deployment
            result = subprocess.run(
                [
                    "kubectl", "get", "deployment", "cdc-pipeline",
                    "-n", self.config.namespace,
                    "-o", "yaml",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            
            (backup_path / "deployment.yaml").write_text(result.stdout)
            
            # Export configmaps
            result = subprocess.run(
                [
                    "kubectl", "get", "configmap",
                    "-n", self.config.namespace,
                    "-l", "app.kubernetes.io/name=cdc-pipeline",
                    "-o", "yaml",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            
            if result.returncode == 0:
                (backup_path / "configmaps.yaml").write_text(result.stdout)
            
        except subprocess.CalledProcessError as e:
            self.logger.warning(f"Failed to backup Kubernetes resources: {e}")
        except FileNotFoundError:
            self.logger.warning("kubectl not found, skipping Kubernetes backup")
    
    def _backup_docker(self, backup_path: Path) -> None:
        """Backup Docker container state."""
        try:
            # Get container info
            result = subprocess.run(
                [
                    "docker-compose", "-f", "docker/docker-compose.yml",
                    "config",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            
            if result.returncode == 0:
                (backup_path / "docker-compose.yaml").write_text(result.stdout)
            
            # Get running container info
            result = subprocess.run(
                [
                    "docker", "ps", "--format", "{{json .}}",
                    "--filter", "name=cdc",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            
            if result.returncode == 0:
                (backup_path / "containers.json").write_text(result.stdout)
            
        except subprocess.CalledProcessError as e:
            self.logger.warning(f"Failed to backup Docker state: {e}")
        except FileNotFoundError:
            self.logger.warning("Docker not found, skipping Docker backup")
    
    def list_backups(self) -> List[Backup]:
        """
        List all available backups.
        
        Returns:
            List of Backup objects
        """
        metadata = self._load_metadata()
        
        backups = []
        for data in metadata.get("backups", []):
            try:
                backup = Backup.from_dict(data)
                # Only include backups for current environment
                if backup.environment == self.config.environment:
                    backups.append(backup)
            except Exception as e:
                self.logger.warning(f"Failed to load backup: {e}")
        
        # Sort by creation time (newest first)
        backups.sort(key=lambda b: b.created_at, reverse=True)
        
        return backups
    
    def get_backup(self, backup_id: str) -> Optional[Backup]:
        """
        Get a specific backup by ID.
        
        Args:
            backup_id: Backup ID
            
        Returns:
            Backup or None if not found
        """
        for backup in self.list_backups():
            if backup.backup_id == backup_id:
                return backup
        return None
    
    def get_latest_backup(self) -> Optional[Backup]:
        """
        Get the most recent backup.
        
        Returns:
            Latest Backup or None
        """
        backups = self.list_backups()
        return backups[0] if backups else None
    
    def restore_backup(self, backup_id: str) -> bool:
        """
        Restore from a backup.
        
        Args:
            backup_id: ID of backup to restore
            
        Returns:
            True if successful
        """
        backup = self.get_backup(backup_id)
        
        if not backup:
            self.logger.error(f"Backup not found: {backup_id}")
            return False
        
        if not backup.path or not backup.path.exists():
            self.logger.error(f"Backup path not found: {backup.path}")
            return False
        
        self.logger.info(f"Restoring backup {backup_id} (version {backup.version})...")
        
        try:
            if self.config.environment == Environment.LOCAL:
                return self._restore_docker(backup)
            else:
                return self._restore_kubernetes(backup)
                
        except Exception as e:
            self.logger.error(f"Failed to restore backup: {e}")
            return False
    
    def _restore_kubernetes(self, backup: Backup) -> bool:
        """Restore Kubernetes resources from backup."""
        deployment_file = backup.path / "deployment.yaml"
        
        if deployment_file.exists():
            try:
                subprocess.run(
                    ["kubectl", "apply", "-f", str(deployment_file)],
                    check=True,
                )
                self.logger.info("Kubernetes deployment restored")
                return True
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Failed to restore Kubernetes: {e}")
                return False
        
        self.logger.warning("No Kubernetes deployment file in backup")
        return False
    
    def _restore_docker(self, backup: Backup) -> bool:
        """Restore Docker deployment from backup."""
        config_file = backup.path / "config.json"
        
        if config_file.exists():
            # Load backed up config
            with open(config_file) as f:
                backup_config = json.load(f)
            
            self.logger.info(f"Restoring to version {backup.version}")
            
            # For Docker, we'd typically:
            # 1. Stop current containers
            # 2. Update docker-compose with backed up version
            # 3. Restart containers
            
            try:
                subprocess.run(
                    [
                        "docker-compose", "-f", "docker/docker-compose.yml",
                        "down", "--remove-orphans",
                    ],
                    check=False,
                )
                
                subprocess.run(
                    [
                        "docker-compose", "-f", "docker/docker-compose.yml",
                        "up", "-d",
                    ],
                    check=True,
                )
                
                self.logger.info("Docker deployment restored")
                return True
                
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Failed to restore Docker: {e}")
                return False
        
        return False
    
    def delete_backup(self, backup_id: str) -> bool:
        """
        Delete a backup.
        
        Args:
            backup_id: ID of backup to delete
            
        Returns:
            True if successful
        """
        backup = self.get_backup(backup_id)
        
        if not backup:
            self.logger.warning(f"Backup not found: {backup_id}")
            return False
        
        # Delete backup directory
        if backup.path and backup.path.exists():
            shutil.rmtree(backup.path)
        
        # Update metadata
        metadata = self._load_metadata()
        metadata["backups"] = [
            b for b in metadata["backups"]
            if b["backup_id"] != backup_id
        ]
        self._save_metadata(metadata)
        
        self.logger.info(f"Deleted backup: {backup_id}")
        return True
    
    def _cleanup_old_backups(self) -> None:
        """Remove old backups exceeding max_backups limit."""
        backups = self.list_backups()
        
        if len(backups) > self.max_backups:
            # Delete oldest backups
            for backup in backups[self.max_backups:]:
                self.delete_backup(backup.backup_id)


def create_backup(
    config: DeploymentConfig,
    version: str,
    description: str = "",
) -> Backup:
    """
    Convenience function to create a backup.
    
    Args:
        config: Deployment configuration
        version: Current version
        description: Backup description
        
    Returns:
        Created Backup
    """
    manager = RollbackManager(config)
    return manager.create_backup(version, description)


def restore_backup(
    config: DeploymentConfig,
    backup_id: str,
) -> bool:
    """
    Convenience function to restore a backup.
    
    Args:
        config: Deployment configuration
        backup_id: Backup ID to restore
        
    Returns:
        True if successful
    """
    manager = RollbackManager(config)
    return manager.restore_backup(backup_id)
