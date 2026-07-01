"""
Security Module

Provides security infrastructure for the CDC pipeline:
- Authentication: Verify user/service identity
- Authorization: Control access with RBAC
- Encryption: Protect data at rest and in transit
- Audit: Log security-relevant events
"""

from .authentication import (
    AuthProvider,
    TokenAuth,
    APIKeyAuth,
    JWTAuth,
    AuthResult,
    AuthError,
    Credentials,
)
from .authorization import (
    Permission,
    Role,
    RBACManager,
    AccessDeniedError,
    check_permission,
)
from .encryption import (
    Encryptor,
    AESEncryptor,
    FieldEncryptor,
    KeyManager,
    hash_password,
    verify_password,
)
from .audit import (
    AuditLogger,
    AuditEvent,
    AuditLevel,
    FileAuditLogger,
    get_audit_logger,
)

__all__ = [
    # Authentication
    "AuthProvider",
    "TokenAuth",
    "APIKeyAuth",
    "JWTAuth",
    "AuthResult",
    "AuthError",
    "Credentials",
    # Authorization
    "Permission",
    "Role",
    "RBACManager",
    "AccessDeniedError",
    "check_permission",
    # Encryption
    "Encryptor",
    "AESEncryptor",
    "FieldEncryptor",
    "KeyManager",
    "hash_password",
    "verify_password",
    # Audit
    "AuditLogger",
    "AuditEvent",
    "AuditLevel",
    "FileAuditLogger",
    "get_audit_logger",
]
