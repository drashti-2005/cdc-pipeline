"""
Version Management Module

Provides semantic versioning utilities for the CDC pipeline.
"""

import logging
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class VersionBumpType(Enum):
    """Version bump types."""
    MAJOR = "major"
    MINOR = "minor"
    PATCH = "patch"
    PRERELEASE = "prerelease"


@dataclass
class SemanticVersion:
    """
    Semantic version representation.
    
    Follows Semantic Versioning 2.0.0 (https://semver.org/)
    
    Format: MAJOR.MINOR.PATCH[-PRERELEASE][+BUILD]
    
    Examples:
        1.0.0
        1.2.3-alpha.1
        2.0.0-rc.1+build.123
    """
    
    major: int
    minor: int
    patch: int
    prerelease: Optional[str] = None
    build: Optional[str] = None
    
    def __str__(self) -> str:
        """Convert to string representation."""
        version = f"{self.major}.{self.minor}.{self.patch}"
        
        if self.prerelease:
            version += f"-{self.prerelease}"
        
        if self.build:
            version += f"+{self.build}"
        
        return version
    
    def __lt__(self, other: "SemanticVersion") -> bool:
        """Compare versions for sorting."""
        if not isinstance(other, SemanticVersion):
            return NotImplemented
        
        # Compare major.minor.patch
        self_tuple = (self.major, self.minor, self.patch)
        other_tuple = (other.major, other.minor, other.patch)
        
        if self_tuple != other_tuple:
            return self_tuple < other_tuple
        
        # Prerelease versions have lower precedence
        if self.prerelease and not other.prerelease:
            return True
        if not self.prerelease and other.prerelease:
            return False
        
        # Compare prereleases
        if self.prerelease and other.prerelease:
            return self.prerelease < other.prerelease
        
        return False
    
    def __eq__(self, other: object) -> bool:
        """Check equality."""
        if not isinstance(other, SemanticVersion):
            return False
        
        return (
            self.major == other.major
            and self.minor == other.minor
            and self.patch == other.patch
            and self.prerelease == other.prerelease
        )
    
    def __hash__(self) -> int:
        """Hash for use in sets/dicts."""
        return hash((self.major, self.minor, self.patch, self.prerelease))
    
    def bump(self, bump_type: VersionBumpType) -> "SemanticVersion":
        """
        Create a new version with the specified bump.
        
        Args:
            bump_type: Type of version bump
            
        Returns:
            New SemanticVersion instance
        """
        if bump_type == VersionBumpType.MAJOR:
            return SemanticVersion(self.major + 1, 0, 0)
        
        elif bump_type == VersionBumpType.MINOR:
            return SemanticVersion(self.major, self.minor + 1, 0)
        
        elif bump_type == VersionBumpType.PATCH:
            return SemanticVersion(self.major, self.minor, self.patch + 1)
        
        elif bump_type == VersionBumpType.PRERELEASE:
            # Increment prerelease number
            if self.prerelease:
                match = re.match(r"(.+)\.(\d+)$", self.prerelease)
                if match:
                    prefix = match.group(1)
                    num = int(match.group(2)) + 1
                    return SemanticVersion(
                        self.major, self.minor, self.patch,
                        prerelease=f"{prefix}.{num}"
                    )
                else:
                    return SemanticVersion(
                        self.major, self.minor, self.patch,
                        prerelease=f"{self.prerelease}.1"
                    )
            else:
                return SemanticVersion(
                    self.major, self.minor, self.patch,
                    prerelease="alpha.1"
                )
        
        raise ValueError(f"Unknown bump type: {bump_type}")
    
    def is_prerelease(self) -> bool:
        """Check if this is a prerelease version."""
        return self.prerelease is not None
    
    def is_stable(self) -> bool:
        """Check if this is a stable release (>= 1.0.0, no prerelease)."""
        return self.major >= 1 and not self.prerelease
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "major": self.major,
            "minor": self.minor,
            "patch": self.patch,
            "prerelease": self.prerelease,
            "build": self.build,
            "version": str(self),
        }


def parse_version(version_string: str) -> SemanticVersion:
    """
    Parse a version string into a SemanticVersion.
    
    Args:
        version_string: Version string (e.g., "1.2.3", "1.0.0-alpha.1")
        
    Returns:
        SemanticVersion instance
        
    Raises:
        ValueError: If version string is invalid
    """
    # Remove 'v' prefix if present
    version_string = version_string.lstrip("v")
    
    # Regex for semantic version
    pattern = r"""
        ^
        (?P<major>0|[1-9]\d*)
        \.
        (?P<minor>0|[1-9]\d*)
        \.
        (?P<patch>0|[1-9]\d*)
        (?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?
        (?:\+(?P<build>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?
        $
    """
    
    match = re.match(pattern, version_string, re.VERBOSE)
    
    if not match:
        raise ValueError(f"Invalid version string: {version_string}")
    
    return SemanticVersion(
        major=int(match.group("major")),
        minor=int(match.group("minor")),
        patch=int(match.group("patch")),
        prerelease=match.group("prerelease"),
        build=match.group("build"),
    )


def get_version(version_file: Optional[Path] = None) -> SemanticVersion:
    """
    Get the current version from version file or setup.py.
    
    Args:
        version_file: Optional path to version file
        
    Returns:
        Current SemanticVersion
    """
    # Try version file first
    if version_file and version_file.exists():
        version_string = version_file.read_text().strip()
        return parse_version(version_string)
    
    # Try setup.py
    setup_py = Path("setup.py")
    if setup_py.exists():
        content = setup_py.read_text()
        match = re.search(r'version=["\']([^"\']+)["\']', content)
        if match:
            return parse_version(match.group(1))
    
    # Try __version__ in __init__.py
    init_py = Path("src/__init__.py")
    if init_py.exists():
        content = init_py.read_text()
        match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
        if match:
            return parse_version(match.group(1))
    
    # Default version
    logger.warning("Could not find version, using default 0.1.0")
    return SemanticVersion(0, 1, 0)


def bump_version(
    bump_type: VersionBumpType,
    version_file: Optional[Path] = None,
    dry_run: bool = False,
) -> Tuple[SemanticVersion, SemanticVersion]:
    """
    Bump the version in the version file.
    
    Args:
        bump_type: Type of version bump
        version_file: Path to version file
        dry_run: If True, don't write changes
        
    Returns:
        Tuple of (old_version, new_version)
    """
    old_version = get_version(version_file)
    new_version = old_version.bump(bump_type)
    
    logger.info(f"Bumping version: {old_version} -> {new_version}")
    
    if not dry_run:
        # Update version file
        if version_file:
            version_file.write_text(str(new_version))
            logger.info(f"Updated {version_file}")
        
        # Update setup.py
        setup_py = Path("setup.py")
        if setup_py.exists():
            content = setup_py.read_text()
            new_content = re.sub(
                r'(version=["\'])([^"\']+)(["\'])',
                f'\\g<1>{new_version}\\g<3>',
                content
            )
            setup_py.write_text(new_content)
            logger.info("Updated setup.py")
    
    return old_version, new_version


def compare_versions(v1: str, v2: str) -> int:
    """
    Compare two version strings.
    
    Args:
        v1: First version string
        v2: Second version string
        
    Returns:
        -1 if v1 < v2, 0 if equal, 1 if v1 > v2
    """
    version1 = parse_version(v1)
    version2 = parse_version(v2)
    
    if version1 < version2:
        return -1
    elif version1 == version2:
        return 0
    else:
        return 1


def get_version_info() -> dict:
    """
    Get detailed version information.
    
    Returns:
        Dictionary with version details
    """
    import platform
    import sys
    
    version = get_version()
    
    return {
        "version": str(version),
        "version_info": version.to_dict(),
        "python_version": sys.version,
        "platform": platform.platform(),
        "is_stable": version.is_stable(),
        "is_prerelease": version.is_prerelease(),
    }
