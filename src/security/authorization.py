"""
Authorization Module

Provides access control using Role-Based Access Control (RBAC):
- Permissions: What actions can be performed
- Roles: Groups of permissions
- Resources: What is being accessed

SIMPLE EXPLANATION:
Authorization is like checking if you're allowed to do something:
- "Can I read this file?" → Check your permissions
- "Can I delete this record?" → Check your role
- "Access granted" or "Access denied" → Allow or block
"""

import functools
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Dict, List, Optional, Any, Set, Callable, Union

logger = logging.getLogger(__name__)


class AccessDeniedError(Exception):
    """Access denied exception."""
    
    def __init__(
        self,
        message: str,
        permission: Optional[str] = None,
        resource: Optional[str] = None,
    ):
        super().__init__(message)
        self.permission = permission
        self.resource = resource


class Action(Enum):
    """Standard actions that can be performed."""
    
    CREATE = auto()
    READ = auto()
    UPDATE = auto()
    DELETE = auto()
    LIST = auto()
    EXECUTE = auto()
    ADMIN = auto()


@dataclass
class Permission:
    """
    A permission defines what action can be performed on what resource.
    
    Format: action:resource[:scope]
    
    Examples:
        - read:customers
        - write:orders:own
        - delete:*
        - admin:pipeline
    """
    
    action: str
    resource: str
    scope: Optional[str] = None
    
    def __post_init__(self):
        """Normalize action and resource."""
        self.action = self.action.lower()
        self.resource = self.resource.lower()
        if self.scope:
            self.scope = self.scope.lower()
    
    @classmethod
    def from_string(cls, permission_str: str) -> "Permission":
        """
        Parse permission from string.
        
        Format: action:resource[:scope]
        """
        parts = permission_str.lower().split(":")
        
        if len(parts) < 2:
            raise ValueError(f"Invalid permission format: {permission_str}")
        
        return cls(
            action=parts[0],
            resource=parts[1],
            scope=parts[2] if len(parts) > 2 else None,
        )
    
    def __str__(self) -> str:
        """Convert to string format."""
        if self.scope:
            return f"{self.action}:{self.resource}:{self.scope}"
        return f"{self.action}:{self.resource}"
    
    def matches(self, other: "Permission") -> bool:
        """
        Check if this permission matches another.
        
        Supports wildcards:
            - * matches everything
            - read:* matches read:anything
        """
        # Check action
        if self.action != "*" and other.action != "*":
            if self.action != other.action:
                return False
        
        # Check resource
        if self.resource != "*" and other.resource != "*":
            if self.resource != other.resource:
                return False
        
        # Check scope
        if self.scope and other.scope:
            if self.scope != "*" and other.scope != "*":
                if self.scope != other.scope:
                    return False
        
        return True
    
    def __hash__(self) -> int:
        return hash(str(self))
    
    def __eq__(self, other: object) -> bool:
        if isinstance(other, Permission):
            return str(self) == str(other)
        if isinstance(other, str):
            return str(self) == other.lower()
        return False


@dataclass
class Role:
    """
    A role is a named collection of permissions.
    
    Roles can inherit from other roles.
    
    SIMPLE EXPLANATION:
    A role is like a job title:
    - "Admin" can do everything
    - "Reader" can only read
    - "Editor" can read and write
    """
    
    name: str
    permissions: Set[Permission] = field(default_factory=set)
    inherits: List[str] = field(default_factory=list)
    description: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_permission(self, permission: Union[str, Permission]) -> None:
        """Add a permission to this role."""
        if isinstance(permission, str):
            permission = Permission.from_string(permission)
        self.permissions.add(permission)
    
    def remove_permission(self, permission: Union[str, Permission]) -> bool:
        """Remove a permission from this role."""
        if isinstance(permission, str):
            permission = Permission.from_string(permission)
        
        try:
            self.permissions.remove(permission)
            return True
        except KeyError:
            return False
    
    def has_permission(self, permission: Union[str, Permission]) -> bool:
        """Check if role has a permission (direct only, not inherited)."""
        if isinstance(permission, str):
            permission = Permission.from_string(permission)
        
        for p in self.permissions:
            if p.matches(permission):
                return True
        
        return False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "permissions": [str(p) for p in self.permissions],
            "inherits": self.inherits,
            "description": self.description,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Role":
        """Create role from dictionary."""
        role = cls(
            name=data["name"],
            description=data.get("description", ""),
            inherits=data.get("inherits", []),
            metadata=data.get("metadata", {}),
        )
        
        for perm_str in data.get("permissions", []):
            role.add_permission(perm_str)
        
        return role


class RBACManager:
    """
    Role-Based Access Control Manager.
    
    Manages roles, permissions, and user assignments.
    
    SIMPLE EXPLANATION:
    RBAC is like an access control system:
    1. Define roles (admin, reader, editor)
    2. Assign permissions to roles
    3. Assign roles to users
    4. Check if user can do something
    
    USAGE:
        rbac = RBACManager()
        
        # Create roles
        rbac.create_role("admin", ["*:*"])
        rbac.create_role("reader", ["read:*"])
        
        # Assign roles to users
        rbac.assign_role("alice", "admin")
        rbac.assign_role("bob", "reader")
        
        # Check permissions
        if rbac.has_permission("alice", "delete:customers"):
            print("Alice can delete customers")
    """
    
    def __init__(self):
        self._roles: Dict[str, Role] = {}
        self._user_roles: Dict[str, Set[str]] = {}
        self._user_permissions: Dict[str, Set[Permission]] = {}  # Direct permissions
        
        # Initialize default roles
        self._create_default_roles()
    
    def _create_default_roles(self) -> None:
        """Create default system roles."""
        # Admin role - full access
        self.create_role(
            "admin",
            permissions=["*:*"],
            description="Full administrative access",
        )
        
        # Reader role - read-only access
        self.create_role(
            "reader",
            permissions=["read:*", "list:*"],
            description="Read-only access",
        )
        
        # Writer role - read and write access
        self.create_role(
            "writer",
            permissions=["read:*", "list:*", "create:*", "update:*"],
            inherits=["reader"],
            description="Read and write access",
        )
        
        # Operator role - operational access
        self.create_role(
            "operator",
            permissions=["read:*", "list:*", "execute:*"],
            description="Operational access (can execute but not modify)",
        )
    
    def create_role(
        self,
        name: str,
        permissions: Optional[List[str]] = None,
        inherits: Optional[List[str]] = None,
        description: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Role:
        """
        Create a new role.
        
        Args:
            name: Role name
            permissions: List of permission strings
            inherits: List of roles to inherit from
            description: Role description
            metadata: Additional metadata
            
        Returns:
            Created Role object
        """
        role = Role(
            name=name,
            description=description,
            inherits=inherits or [],
            metadata=metadata or {},
        )
        
        for perm_str in permissions or []:
            role.add_permission(perm_str)
        
        self._roles[name] = role
        logger.info(f"Created role: {name}")
        return role
    
    def get_role(self, name: str) -> Optional[Role]:
        """Get a role by name."""
        return self._roles.get(name)
    
    def delete_role(self, name: str) -> bool:
        """Delete a role."""
        if name in self._roles:
            del self._roles[name]
            
            # Remove from all users
            for user in self._user_roles:
                self._user_roles[user].discard(name)
            
            logger.info(f"Deleted role: {name}")
            return True
        return False
    
    def list_roles(self) -> List[str]:
        """List all role names."""
        return list(self._roles.keys())
    
    def assign_role(self, user: str, role_name: str) -> bool:
        """
        Assign a role to a user.
        
        Args:
            user: User identity
            role_name: Role to assign
            
        Returns:
            True if assigned successfully
        """
        if role_name not in self._roles:
            logger.warning(f"Role not found: {role_name}")
            return False
        
        if user not in self._user_roles:
            self._user_roles[user] = set()
        
        self._user_roles[user].add(role_name)
        logger.info(f"Assigned role {role_name} to {user}")
        return True
    
    def revoke_role(self, user: str, role_name: str) -> bool:
        """Revoke a role from a user."""
        if user in self._user_roles:
            self._user_roles[user].discard(role_name)
            logger.info(f"Revoked role {role_name} from {user}")
            return True
        return False
    
    def get_user_roles(self, user: str) -> Set[str]:
        """Get all roles for a user."""
        return self._user_roles.get(user, set()).copy()
    
    def grant_permission(
        self,
        user: str,
        permission: Union[str, Permission],
    ) -> None:
        """Grant a direct permission to a user (not through role)."""
        if isinstance(permission, str):
            permission = Permission.from_string(permission)
        
        if user not in self._user_permissions:
            self._user_permissions[user] = set()
        
        self._user_permissions[user].add(permission)
        logger.info(f"Granted {permission} to {user}")
    
    def revoke_permission(
        self,
        user: str,
        permission: Union[str, Permission],
    ) -> bool:
        """Revoke a direct permission from a user."""
        if isinstance(permission, str):
            permission = Permission.from_string(permission)
        
        if user in self._user_permissions:
            try:
                self._user_permissions[user].remove(permission)
                logger.info(f"Revoked {permission} from {user}")
                return True
            except KeyError:
                pass
        return False
    
    def _get_effective_permissions(self, user: str) -> Set[Permission]:
        """
        Get all effective permissions for a user.
        
        Includes direct permissions and permissions from all roles.
        """
        permissions: Set[Permission] = set()
        
        # Direct permissions
        if user in self._user_permissions:
            permissions.update(self._user_permissions[user])
        
        # Role permissions
        user_roles = self._user_roles.get(user, set())
        visited: Set[str] = set()
        
        def collect_role_permissions(role_name: str) -> None:
            if role_name in visited:
                return
            visited.add(role_name)
            
            role = self._roles.get(role_name)
            if not role:
                return
            
            permissions.update(role.permissions)
            
            # Inherit permissions
            for inherited in role.inherits:
                collect_role_permissions(inherited)
        
        for role_name in user_roles:
            collect_role_permissions(role_name)
        
        return permissions
    
    def has_permission(
        self,
        user: str,
        permission: Union[str, Permission],
        resource_context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Check if user has a permission.
        
        Args:
            user: User identity
            permission: Permission to check
            resource_context: Optional context for scope evaluation
            
        Returns:
            True if user has the permission
        """
        if isinstance(permission, str):
            permission = Permission.from_string(permission)
        
        effective_perms = self._get_effective_permissions(user)
        
        for perm in effective_perms:
            if perm.matches(permission):
                return True
        
        return False
    
    def check_permission(
        self,
        user: str,
        permission: Union[str, Permission],
        resource: Optional[str] = None,
    ) -> None:
        """
        Check permission and raise AccessDeniedError if not allowed.
        
        Args:
            user: User identity
            permission: Permission to check
            resource: Optional resource identifier
            
        Raises:
            AccessDeniedError: If access is denied
        """
        if not self.has_permission(user, permission):
            perm_str = str(permission) if isinstance(permission, Permission) else permission
            raise AccessDeniedError(
                f"Access denied for {user}: {perm_str}",
                permission=perm_str,
                resource=resource,
            )
    
    def get_user_permissions(self, user: str) -> List[str]:
        """Get list of all effective permissions for a user."""
        return [str(p) for p in self._get_effective_permissions(user)]


def check_permission(
    permission: str,
    rbac: Optional[RBACManager] = None,
    user_getter: Optional[Callable[[], str]] = None,
) -> Callable:
    """
    Decorator to check permission before function execution.
    
    USAGE:
        @check_permission("delete:customers")
        def delete_customer(customer_id):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get RBAC manager
            manager = rbac
            if manager is None:
                manager = kwargs.get("_rbac")
                if manager is None:
                    raise ValueError("RBAC manager not provided")
            
            # Get user
            user = None
            if user_getter:
                user = user_getter()
            else:
                user = kwargs.get("_user")
            
            if user is None:
                raise ValueError("User identity not provided")
            
            # Check permission
            manager.check_permission(user, permission)
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


@dataclass
class ResourcePolicy:
    """
    Policy for a specific resource.
    
    Defines what actions are allowed on a resource.
    """
    
    resource: str
    allowed_actions: Set[str] = field(default_factory=set)
    denied_actions: Set[str] = field(default_factory=set)
    conditions: Dict[str, Any] = field(default_factory=dict)
    
    def is_allowed(self, action: str) -> bool:
        """Check if action is allowed by this policy."""
        # Explicit deny takes precedence
        if action in self.denied_actions:
            return False
        
        # Check allowed
        if "*" in self.allowed_actions:
            return True
        
        return action in self.allowed_actions


class PolicyEngine:
    """
    Policy-based authorization engine.
    
    Evaluates policies to determine access.
    
    SIMPLE EXPLANATION:
    Policies are rules like:
    - "Allow read on customers if user.department == customer.department"
    - "Deny delete on * if user.role != admin"
    
    USAGE:
        engine = PolicyEngine()
        
        # Add policies
        engine.add_policy("customers", allowed=["read", "list"])
        engine.add_policy("orders", allowed=["read", "create"])
        
        # Check
        if engine.is_allowed("customers", "read"):
            print("Can read customers")
    """
    
    def __init__(self):
        self._policies: Dict[str, List[ResourcePolicy]] = {}
    
    def add_policy(
        self,
        resource: str,
        allowed: Optional[List[str]] = None,
        denied: Optional[List[str]] = None,
        conditions: Optional[Dict[str, Any]] = None,
    ) -> ResourcePolicy:
        """Add a policy for a resource."""
        policy = ResourcePolicy(
            resource=resource,
            allowed_actions=set(allowed or []),
            denied_actions=set(denied or []),
            conditions=conditions or {},
        )
        
        if resource not in self._policies:
            self._policies[resource] = []
        
        self._policies[resource].append(policy)
        return policy
    
    def is_allowed(
        self,
        resource: str,
        action: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Check if action is allowed on resource."""
        policies = self._policies.get(resource, [])
        
        # Also check wildcard policies
        policies.extend(self._policies.get("*", []))
        
        if not policies:
            return False  # Deny by default
        
        for policy in policies:
            if not policy.is_allowed(action):
                return False
        
        return True
    
    def evaluate(
        self,
        resource: str,
        action: str,
        user: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Evaluate policies for a specific action.
        
        Args:
            resource: Resource being accessed
            action: Action being performed
            user: User performing the action
            context: Additional context for evaluation
            
        Returns:
            True if allowed, False otherwise
        """
        return self.is_allowed(resource, action, context)
