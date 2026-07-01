"""
Security Module Tests

Tests for authentication, authorization, encryption, and audit logging.
"""

import json
import os
import tempfile
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from security.authentication import (
    Credentials,
    AuthResult,
    AuthError,
    InvalidCredentialsError,
    ExpiredTokenError,
    TokenAuth,
    APIKeyAuth,
    JWTAuth,
    MultiProviderAuth,
)
from security.authorization import (
    Permission,
    Role,
    RBACManager,
    AccessDeniedError,
    check_permission,
    ResourcePolicy,
    PolicyEngine,
)
from security.encryption import (
    AESEncryptor,
    FieldEncryptor,
    KeyManager,
    hash_password,
    verify_password,
    generate_secure_token,
    constant_time_compare,
    EncryptionError,
    DecryptionError,
)
from security.audit import (
    AuditLevel,
    EventType,
    AuditEvent,
    FileAuditLogger,
    MemoryAuditLogger,
    configure_audit_logger,
    get_audit_logger,
    audit_log,
)


# =============================================================================
# Authentication Tests
# =============================================================================

class TestCredentials:
    """Test Credentials dataclass."""
    
    def test_empty_credentials(self):
        """Test empty credentials detection."""
        creds = Credentials()
        assert creds.is_empty() is True
    
    def test_credentials_with_username(self):
        """Test credentials with username."""
        creds = Credentials(username="alice", password="secret")
        assert creds.is_empty() is False
    
    def test_credentials_with_api_key(self):
        """Test credentials with API key."""
        creds = Credentials(api_key="my-key")
        assert creds.is_empty() is False
    
    def test_credentials_with_token(self):
        """Test credentials with token."""
        creds = Credentials(token="my-token")
        assert creds.is_empty() is False


class TestAuthResult:
    """Test AuthResult dataclass."""
    
    def test_successful_result(self):
        """Test successful auth result."""
        result = AuthResult(
            success=True,
            identity="alice",
            roles=["admin"],
        )
        assert result.success is True
        assert result.identity == "alice"
    
    def test_failed_result(self):
        """Test failed auth result."""
        result = AuthResult(success=False, error="Invalid credentials")
        assert result.success is False
        assert result.error == "Invalid credentials"
    
    def test_expiration_check(self):
        """Test expiration check."""
        # Not expired
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        result = AuthResult(success=True, expires_at=future)
        assert result.is_expired is False
        
        # Expired
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        result = AuthResult(success=True, expires_at=past)
        assert result.is_expired is True
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        result = AuthResult(
            success=True,
            identity="bob",
            roles=["reader"],
        )
        data = result.to_dict()
        assert data["success"] is True
        assert data["identity"] == "bob"


class TestTokenAuth:
    """Test TokenAuth provider."""
    
    def test_create_token(self):
        """Test token creation."""
        auth = TokenAuth()
        token = auth.create_token("service-a", roles=["reader"])
        
        assert token is not None
        assert len(token) > 20
    
    def test_authenticate_valid_token(self):
        """Test authentication with valid token."""
        auth = TokenAuth()
        token = auth.create_token("service-a", roles=["admin"])
        
        result = auth.authenticate(Credentials(token=token))
        
        assert result.success is True
        assert result.identity == "service-a"
        assert "admin" in result.roles
    
    def test_authenticate_invalid_token(self):
        """Test authentication with invalid token."""
        auth = TokenAuth()
        
        result = auth.authenticate(Credentials(token="invalid-token"))
        
        assert result.success is False
        assert "Invalid token" in result.error
    
    def test_token_expiration(self):
        """Test token expiration."""
        auth = TokenAuth()
        token = auth.create_token(
            "service-a",
            ttl=timedelta(milliseconds=100),
        )
        
        # Token should work immediately
        result = auth.authenticate(Credentials(token=token))
        assert result.success is True
        
        # Wait for expiration
        time.sleep(0.2)
        
        # Token should be expired
        result = auth.authenticate(Credentials(token=token))
        assert result.success is False
        assert "expired" in result.error.lower()
    
    def test_revoke_token(self):
        """Test token revocation."""
        auth = TokenAuth()
        token = auth.create_token("service-a")
        
        # Should work before revocation
        assert auth.validate(token).success is True
        
        # Revoke
        auth.revoke_token(token)
        
        # Should fail after revocation
        assert auth.validate(token).success is False
    
    def test_cleanup_expired(self):
        """Test cleanup of expired tokens."""
        auth = TokenAuth()
        auth.create_token("service-a", ttl=timedelta(milliseconds=50))
        auth.create_token("service-b", ttl=timedelta(hours=1))
        
        time.sleep(0.1)
        
        cleaned = auth.cleanup_expired()
        assert cleaned == 1


class TestAPIKeyAuth:
    """Test APIKeyAuth provider."""
    
    def test_create_key(self):
        """Test API key creation."""
        auth = APIKeyAuth(key_prefix="test_")
        key = auth.create_key("my-service")
        
        assert key.startswith("test_")
        assert len(key) > 10
    
    def test_authenticate_valid_key(self):
        """Test authentication with valid key."""
        auth = APIKeyAuth()
        key = auth.create_key("my-service", roles=["writer"])
        
        result = auth.authenticate(Credentials(api_key=key))
        
        assert result.success is True
        assert result.identity == "my-service"
        assert "writer" in result.roles
    
    def test_authenticate_invalid_key(self):
        """Test authentication with invalid key."""
        auth = APIKeyAuth()
        
        result = auth.authenticate(Credentials(api_key="invalid"))
        
        assert result.success is False
    
    def test_revoke_key(self):
        """Test key revocation."""
        auth = APIKeyAuth()
        key = auth.create_key("my-service")
        
        # Should work before revocation
        assert auth.validate(key).success is True
        
        # Revoke
        auth.revoke_key(key)
        
        # Should fail after revocation
        assert auth.validate(key).success is False


class TestJWTAuth:
    """Test JWTAuth provider."""
    
    def test_create_token(self):
        """Test JWT creation."""
        auth = JWTAuth(secret="test-secret")
        token = auth.create_token("alice", roles=["admin"])
        
        # Should have 3 parts (header.payload.signature)
        parts = token.split(".")
        assert len(parts) == 3
    
    def test_validate_token(self):
        """Test JWT validation."""
        auth = JWTAuth(secret="test-secret")
        token = auth.create_token(
            "alice",
            roles=["admin"],
            permissions=["read:*"],
        )
        
        result = auth.validate(token)
        
        assert result.success is True
        assert result.identity == "alice"
        assert "admin" in result.roles
        assert "read:*" in result.permissions
    
    def test_invalid_signature(self):
        """Test JWT with invalid signature."""
        auth = JWTAuth(secret="secret-1")
        token = auth.create_token("alice")
        
        # Validate with different secret
        auth2 = JWTAuth(secret="secret-2")
        result = auth2.validate(token)
        
        assert result.success is False
        assert "Invalid signature" in result.error
    
    def test_expired_token(self):
        """Test expired JWT."""
        auth = JWTAuth(secret="test-secret")
        token = auth.create_token(
            "alice",
            ttl=timedelta(milliseconds=100),
        )
        
        time.sleep(0.2)
        
        result = auth.validate(token)
        
        assert result.success is False
        assert "expired" in result.error.lower()
    
    def test_revoke_token(self):
        """Test JWT revocation via blacklist."""
        auth = JWTAuth(secret="test-secret")
        token = auth.create_token("alice")
        
        # Should work before revocation
        assert auth.validate(token).success is True
        
        # Revoke
        auth.revoke_token(token)
        
        # Should fail after revocation
        result = auth.validate(token)
        assert result.success is False
        assert "revoked" in result.error.lower()
    
    def test_invalid_format(self):
        """Test JWT with invalid format."""
        auth = JWTAuth(secret="test-secret")
        
        result = auth.validate("not.a.valid.token")
        assert result.success is False


class TestMultiProviderAuth:
    """Test MultiProviderAuth."""
    
    def test_multiple_providers(self):
        """Test authentication with multiple providers."""
        auth = MultiProviderAuth()
        
        token_auth = TokenAuth()
        api_auth = APIKeyAuth()
        
        auth.add_provider("token", token_auth)
        auth.add_provider("api_key", api_auth)
        
        # Create credentials
        token = token_auth.create_token("service-a")
        api_key = api_auth.create_key("service-b")
        
        # Authenticate with token
        result = auth.authenticate(Credentials(token=token))
        assert result.success is True
        assert result.identity == "service-a"
        
        # Authenticate with API key
        result = auth.authenticate(Credentials(api_key=api_key))
        assert result.success is True
        assert result.identity == "service-b"
    
    def test_specific_provider(self):
        """Test authentication with specific provider."""
        auth = MultiProviderAuth()
        auth.add_provider("token", TokenAuth())
        auth.add_provider("api_key", APIKeyAuth())
        
        # Unknown provider
        result = auth.authenticate(
            Credentials(token="test"),
            provider_name="unknown",
        )
        assert result.success is False


# =============================================================================
# Authorization Tests
# =============================================================================

class TestPermission:
    """Test Permission class."""
    
    def test_from_string(self):
        """Test parsing permission from string."""
        perm = Permission.from_string("read:customers")
        
        assert perm.action == "read"
        assert perm.resource == "customers"
        assert perm.scope is None
    
    def test_from_string_with_scope(self):
        """Test parsing permission with scope."""
        perm = Permission.from_string("write:orders:own")
        
        assert perm.action == "write"
        assert perm.resource == "orders"
        assert perm.scope == "own"
    
    def test_to_string(self):
        """Test permission to string."""
        perm = Permission("read", "customers")
        assert str(perm) == "read:customers"
        
        perm = Permission("write", "orders", "own")
        assert str(perm) == "write:orders:own"
    
    def test_matches_exact(self):
        """Test exact permission matching."""
        p1 = Permission.from_string("read:customers")
        p2 = Permission.from_string("read:customers")
        
        assert p1.matches(p2) is True
    
    def test_matches_wildcard_action(self):
        """Test wildcard action matching."""
        p1 = Permission.from_string("*:customers")
        p2 = Permission.from_string("read:customers")
        
        assert p1.matches(p2) is True
    
    def test_matches_wildcard_resource(self):
        """Test wildcard resource matching."""
        p1 = Permission.from_string("read:*")
        p2 = Permission.from_string("read:customers")
        
        assert p1.matches(p2) is True
    
    def test_matches_full_wildcard(self):
        """Test full wildcard matching."""
        p1 = Permission.from_string("*:*")
        p2 = Permission.from_string("delete:customers")
        
        assert p1.matches(p2) is True
    
    def test_no_match(self):
        """Test non-matching permissions."""
        p1 = Permission.from_string("read:customers")
        p2 = Permission.from_string("write:orders")
        
        assert p1.matches(p2) is False


class TestRole:
    """Test Role class."""
    
    def test_add_permission(self):
        """Test adding permissions to role."""
        role = Role(name="reader")
        role.add_permission("read:customers")
        role.add_permission("list:customers")
        
        assert len(role.permissions) == 2
    
    def test_has_permission(self):
        """Test checking role permissions."""
        role = Role(name="reader")
        role.add_permission("read:*")
        
        assert role.has_permission("read:customers") is True
        assert role.has_permission("write:customers") is False
    
    def test_remove_permission(self):
        """Test removing permissions from role."""
        role = Role(name="reader")
        role.add_permission("read:customers")
        
        result = role.remove_permission("read:customers")
        
        assert result is True
        assert len(role.permissions) == 0
    
    def test_to_dict(self):
        """Test role serialization."""
        role = Role(
            name="admin",
            description="Full access",
            inherits=["reader"],
        )
        role.add_permission("*:*")
        
        data = role.to_dict()
        
        assert data["name"] == "admin"
        assert "*:*" in data["permissions"]
        assert "reader" in data["inherits"]


class TestRBACManager:
    """Test RBACManager."""
    
    def test_default_roles(self):
        """Test default roles are created."""
        rbac = RBACManager()
        
        roles = rbac.list_roles()
        
        assert "admin" in roles
        assert "reader" in roles
        assert "writer" in roles
        assert "operator" in roles
    
    def test_create_role(self):
        """Test creating a new role."""
        rbac = RBACManager()
        
        role = rbac.create_role(
            "custom",
            permissions=["read:reports", "create:reports"],
            description="Custom role",
        )
        
        assert role.name == "custom"
        assert len(role.permissions) == 2
    
    def test_assign_role(self):
        """Test assigning role to user."""
        rbac = RBACManager()
        
        result = rbac.assign_role("alice", "admin")
        
        assert result is True
        assert "admin" in rbac.get_user_roles("alice")
    
    def test_has_permission_via_role(self):
        """Test checking permission via role."""
        rbac = RBACManager()
        rbac.assign_role("alice", "admin")
        
        # Admin has *:*
        assert rbac.has_permission("alice", "delete:customers") is True
    
    def test_has_permission_reader(self):
        """Test reader role permissions."""
        rbac = RBACManager()
        rbac.assign_role("bob", "reader")
        
        assert rbac.has_permission("bob", "read:customers") is True
        assert rbac.has_permission("bob", "delete:customers") is False
    
    def test_direct_permission(self):
        """Test granting direct permission."""
        rbac = RBACManager()
        rbac.grant_permission("charlie", "delete:reports")
        
        assert rbac.has_permission("charlie", "delete:reports") is True
        assert rbac.has_permission("charlie", "delete:customers") is False
    
    def test_revoke_role(self):
        """Test revoking role from user."""
        rbac = RBACManager()
        rbac.assign_role("alice", "admin")
        rbac.revoke_role("alice", "admin")
        
        assert "admin" not in rbac.get_user_roles("alice")
    
    def test_check_permission_raises(self):
        """Test check_permission raises AccessDeniedError."""
        rbac = RBACManager()
        rbac.assign_role("bob", "reader")
        
        with pytest.raises(AccessDeniedError) as exc_info:
            rbac.check_permission("bob", "delete:customers")
        
        assert "Access denied" in str(exc_info.value)
    
    def test_role_inheritance(self):
        """Test role inheritance."""
        rbac = RBACManager()
        
        # Writer inherits from reader
        rbac.assign_role("alice", "writer")
        
        # Should have reader permissions
        assert rbac.has_permission("alice", "read:customers") is True
        # And writer permissions
        assert rbac.has_permission("alice", "create:customers") is True
    
    def test_get_user_permissions(self):
        """Test getting all user permissions."""
        rbac = RBACManager()
        rbac.assign_role("alice", "reader")
        
        perms = rbac.get_user_permissions("alice")
        
        assert "read:*" in perms
        assert "list:*" in perms


class TestPolicyEngine:
    """Test PolicyEngine."""
    
    def test_add_policy(self):
        """Test adding a policy."""
        engine = PolicyEngine()
        policy = engine.add_policy(
            "customers",
            allowed=["read", "list"],
        )
        
        assert policy.resource == "customers"
        assert "read" in policy.allowed_actions
    
    def test_is_allowed(self):
        """Test checking if action is allowed."""
        engine = PolicyEngine()
        engine.add_policy("customers", allowed=["read", "list"])
        
        assert engine.is_allowed("customers", "read") is True
        assert engine.is_allowed("customers", "delete") is False
    
    def test_explicit_deny(self):
        """Test explicit deny takes precedence."""
        engine = PolicyEngine()
        engine.add_policy("customers", allowed=["*"], denied=["delete"])
        
        assert engine.is_allowed("customers", "read") is True
        assert engine.is_allowed("customers", "delete") is False


# =============================================================================
# Encryption Tests
# =============================================================================

class TestAESEncryptor:
    """Test AESEncryptor."""
    
    def test_generate_key(self):
        """Test key generation."""
        key = AESEncryptor.generate_key()
        
        assert len(key) == 32
    
    def test_encrypt_decrypt(self):
        """Test encryption and decryption."""
        key = AESEncryptor.generate_key()
        encryptor = AESEncryptor(key)
        
        plaintext = b"Hello, World!"
        
        encrypted = encryptor.encrypt(plaintext)
        decrypted = encryptor.decrypt(encrypted)
        
        assert decrypted == plaintext
    
    def test_encrypt_empty(self):
        """Test encrypting empty data."""
        key = AESEncryptor.generate_key()
        encryptor = AESEncryptor(key)
        
        encrypted = encryptor.encrypt(b"")
        
        assert encrypted == b""
    
    def test_different_ciphertext(self):
        """Test that same plaintext produces different ciphertext (due to random nonce)."""
        key = AESEncryptor.generate_key()
        encryptor = AESEncryptor(key)
        
        plaintext = b"test data"
        
        c1 = encryptor.encrypt(plaintext)
        c2 = encryptor.encrypt(plaintext)
        
        assert c1 != c2  # Different nonces
    
    def test_wrong_key(self):
        """Test decryption with wrong key produces garbage."""
        key1 = AESEncryptor.generate_key()
        key2 = AESEncryptor.generate_key()
        
        encryptor1 = AESEncryptor(key1)
        encryptor2 = AESEncryptor(key2)
        
        plaintext = b"secret data"
        encrypted = encryptor1.encrypt(plaintext)
        
        # Decrypting with wrong key won't raise but produces garbage
        decrypted = encryptor2.decrypt(encrypted)
        assert decrypted != plaintext
    
    def test_invalid_key_length(self):
        """Test that invalid key length raises error."""
        with pytest.raises(ValueError):
            AESEncryptor(b"short")
    
    def test_large_data(self):
        """Test encrypting large data."""
        key = AESEncryptor.generate_key()
        encryptor = AESEncryptor(key)
        
        # 1MB of data
        plaintext = os.urandom(1024 * 1024)
        
        encrypted = encryptor.encrypt(plaintext)
        decrypted = encryptor.decrypt(encrypted)
        
        assert decrypted == plaintext


class TestFieldEncryptor:
    """Test FieldEncryptor."""
    
    def test_encrypt_sensitive_fields(self):
        """Test encrypting sensitive fields."""
        key = AESEncryptor.generate_key()
        encryptor = FieldEncryptor(key)
        
        encryptor.add_sensitive_field("ssn")
        encryptor.add_sensitive_field("credit_card")
        
        data = {
            "name": "John Doe",
            "ssn": "123-45-6789",
            "credit_card": "4111-1111-1111-1111",
        }
        
        encrypted = encryptor.encrypt_fields(data)
        
        assert encrypted["name"] == "John Doe"  # Not encrypted
        assert encrypted["ssn"].startswith("encrypted:")
        assert encrypted["credit_card"].startswith("encrypted:")
    
    def test_decrypt_fields(self):
        """Test decrypting fields."""
        key = AESEncryptor.generate_key()
        encryptor = FieldEncryptor(key)
        
        encryptor.add_sensitive_field("ssn")
        
        data = {"name": "John", "ssn": "123-45-6789"}
        
        encrypted = encryptor.encrypt_fields(data)
        decrypted = encryptor.decrypt_fields(encrypted)
        
        assert decrypted["ssn"] == "123-45-6789"
    
    def test_nested_encryption(self):
        """Test encrypting nested structures."""
        key = AESEncryptor.generate_key()
        encryptor = FieldEncryptor(key)
        
        encryptor.add_sensitive_field("password")
        
        data = {
            "user": {
                "name": "Alice",
                "password": "secret123",
            }
        }
        
        encrypted = encryptor.encrypt_fields(data)
        
        assert encrypted["user"]["password"].startswith("encrypted:")
    
    def test_field_pattern(self):
        """Test field pattern matching."""
        key = AESEncryptor.generate_key()
        encryptor = FieldEncryptor(key)
        
        encryptor.add_field_pattern(r".*password.*")
        
        assert encryptor.is_sensitive("password") is True
        assert encryptor.is_sensitive("user_password") is True
        assert encryptor.is_sensitive("name") is False


class TestKeyManager:
    """Test KeyManager."""
    
    def test_generate_key(self):
        """Test key generation."""
        km = KeyManager()
        key_id = km.generate_key()
        
        assert key_id is not None
        assert key_id.startswith("key-")
    
    def test_get_key(self):
        """Test retrieving key."""
        km = KeyManager()
        key_id = km.generate_key()
        
        key = km.get_key(key_id)
        
        assert len(key) == 32
    
    def test_get_current_key(self):
        """Test getting current key."""
        km = KeyManager()
        key_id = km.generate_key()
        
        current_id, current_key = km.get_current_key()
        
        assert current_id == key_id
        assert len(current_key) == 32
    
    def test_rotate_key(self):
        """Test key rotation."""
        km = KeyManager()
        old_id = km.generate_key()
        
        new_id = km.rotate_key()
        
        assert new_id != old_id
        
        # Old key should be inactive
        with pytest.raises(Exception):
            km.get_key(old_id)
    
    def test_deactivate_key(self):
        """Test key deactivation."""
        km = KeyManager()
        key_id = km.generate_key()
        
        km.deactivate_key(key_id)
        
        with pytest.raises(Exception):
            km.get_key(key_id)
    
    def test_list_keys(self):
        """Test listing keys."""
        km = KeyManager()
        km.generate_key(key_id="key-1")
        km.generate_key(key_id="key-2")
        
        keys = km.list_keys()
        
        assert "key-1" in keys
        assert "key-2" in keys


class TestPasswordHashing:
    """Test password hashing functions."""
    
    def test_hash_password(self):
        """Test password hashing."""
        hashed = hash_password("mypassword")
        
        # Should have format: salt$iterations$hash
        parts = hashed.split("$")
        assert len(parts) == 3
    
    def test_verify_password_correct(self):
        """Test verifying correct password."""
        password = "mypassword123"
        hashed = hash_password(password)
        
        assert verify_password(password, hashed) is True
    
    def test_verify_password_wrong(self):
        """Test verifying wrong password."""
        hashed = hash_password("correct")
        
        assert verify_password("wrong", hashed) is False
    
    def test_different_hashes(self):
        """Test that same password produces different hashes (due to random salt)."""
        password = "test"
        
        h1 = hash_password(password)
        h2 = hash_password(password)
        
        assert h1 != h2  # Different salts
        assert verify_password(password, h1) is True
        assert verify_password(password, h2) is True


class TestSecurityUtilities:
    """Test security utility functions."""
    
    def test_generate_secure_token(self):
        """Test secure token generation."""
        token = generate_secure_token()
        
        assert len(token) > 20
    
    def test_constant_time_compare_equal(self):
        """Test constant time comparison of equal strings."""
        assert constant_time_compare("secret", "secret") is True
    
    def test_constant_time_compare_not_equal(self):
        """Test constant time comparison of different strings."""
        assert constant_time_compare("secret", "other") is False


# =============================================================================
# Audit Logging Tests
# =============================================================================

class TestAuditEvent:
    """Test AuditEvent class."""
    
    def test_create_event(self):
        """Test creating an audit event."""
        event = AuditEvent(
            event_type=EventType.AUTH_SUCCESS,
            timestamp=datetime.now(timezone.utc),
            user="alice",
            action="login",
        )
        
        assert event.event_id is not None
        assert event.user == "alice"
    
    def test_to_dict(self):
        """Test event serialization."""
        event = AuditEvent(
            event_type=EventType.AUTH_SUCCESS,
            timestamp=datetime.now(timezone.utc),
            user="alice",
        )
        
        data = event.to_dict()
        
        assert data["event_type"] == "auth.success"
        assert data["user"] == "alice"
    
    def test_from_dict(self):
        """Test event deserialization."""
        data = {
            "event_type": "auth.success",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user": "bob",
            "level": "INFO",
        }
        
        event = AuditEvent.from_dict(data)
        
        assert event.user == "bob"
        assert event.event_type == EventType.AUTH_SUCCESS
    
    def test_compute_hash(self):
        """Test hash computation."""
        event = AuditEvent(
            event_type=EventType.AUTH_SUCCESS,
            timestamp=datetime.now(timezone.utc),
        )
        
        hash1 = event.compute_hash()
        hash2 = event.compute_hash()
        
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA-256


class TestMemoryAuditLogger:
    """Test MemoryAuditLogger."""
    
    def test_log_event(self):
        """Test logging an event."""
        logger = MemoryAuditLogger()
        
        event = AuditEvent(
            event_type=EventType.AUTH_SUCCESS,
            timestamp=datetime.now(timezone.utc),
            user="alice",
        )
        
        logger.log(event)
        
        events = logger.get_all()
        assert len(events) == 1
        assert events[0].user == "alice"
    
    def test_query_by_user(self):
        """Test querying events by user."""
        logger = MemoryAuditLogger()
        
        logger.log(AuditEvent(
            event_type=EventType.AUTH_SUCCESS,
            timestamp=datetime.now(timezone.utc),
            user="alice",
        ))
        logger.log(AuditEvent(
            event_type=EventType.AUTH_SUCCESS,
            timestamp=datetime.now(timezone.utc),
            user="bob",
        ))
        
        events = logger.query(user="alice")
        
        assert len(events) == 1
        assert events[0].user == "alice"
    
    def test_query_by_event_type(self):
        """Test querying events by type."""
        logger = MemoryAuditLogger()
        
        logger.log(AuditEvent(
            event_type=EventType.AUTH_SUCCESS,
            timestamp=datetime.now(timezone.utc),
        ))
        logger.log(AuditEvent(
            event_type=EventType.AUTH_FAILURE,
            timestamp=datetime.now(timezone.utc),
        ))
        
        events = logger.query(event_type=EventType.AUTH_FAILURE)
        
        assert len(events) == 1
    
    def test_query_limit(self):
        """Test query limit."""
        logger = MemoryAuditLogger()
        
        for i in range(10):
            logger.log(AuditEvent(
                event_type=EventType.AUTH_SUCCESS,
                timestamp=datetime.now(timezone.utc),
            ))
        
        events = logger.query(limit=5)
        
        assert len(events) == 5
    
    def test_max_events(self):
        """Test max events limit."""
        logger = MemoryAuditLogger(max_events=5)
        
        for i in range(10):
            logger.log(AuditEvent(
                event_type=EventType.AUTH_SUCCESS,
                timestamp=datetime.now(timezone.utc),
                details={"index": i},
            ))
        
        events = logger.get_all()
        
        assert len(events) == 5
        # Should have the last 5 events
        assert events[0].details["index"] == 5


class TestFileAuditLogger:
    """Test FileAuditLogger."""
    
    def test_log_event(self):
        """Test logging event to file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = FileAuditLogger(tmpdir)
            
            event = AuditEvent(
                event_type=EventType.AUTH_SUCCESS,
                timestamp=datetime.now(timezone.utc),
                user="alice",
            )
            
            logger.log(event)
            logger.close()
            
            # Check file was created
            log_files = list(Path(tmpdir).glob("audit-*.jsonl"))
            assert len(log_files) == 1
    
    def test_query_events(self):
        """Test querying events from file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = FileAuditLogger(tmpdir)
            
            logger.log(AuditEvent(
                event_type=EventType.AUTH_SUCCESS,
                timestamp=datetime.now(timezone.utc),
                user="alice",
            ))
            logger.log(AuditEvent(
                event_type=EventType.AUTH_FAILURE,
                timestamp=datetime.now(timezone.utc),
                user="bob",
            ))
            
            events = logger.query(user="alice")
            
            assert len(events) == 1
            assert events[0].user == "alice"
            
            logger.close()
    
    def test_chain_integrity(self):
        """Test audit log chain integrity."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = FileAuditLogger(tmpdir)
            
            for i in range(5):
                logger.log(AuditEvent(
                    event_type=EventType.AUTH_SUCCESS,
                    timestamp=datetime.now(timezone.utc),
                ))
            
            # Verify chain
            assert logger.verify_chain() is True
            
            logger.close()


class TestGlobalAuditLogger:
    """Test global audit logger functions."""
    
    def test_configure_and_get(self):
        """Test configuring global logger."""
        logger = MemoryAuditLogger()
        configure_audit_logger(logger)
        
        retrieved = get_audit_logger()
        
        assert retrieved is logger
        
        # Cleanup
        configure_audit_logger(None)
    
    def test_audit_log_function(self):
        """Test audit_log convenience function."""
        logger = MemoryAuditLogger()
        configure_audit_logger(logger)
        
        audit_log(
            EventType.AUTH_SUCCESS,
            user="alice",
            action="login",
        )
        
        events = logger.get_all()
        
        assert len(events) == 1
        assert events[0].user == "alice"
        
        # Cleanup
        configure_audit_logger(None)


# =============================================================================
# Integration Tests
# =============================================================================

class TestSecurityIntegration:
    """Integration tests for security components."""
    
    def test_auth_to_audit_flow(self):
        """Test authentication to audit logging flow."""
        # Setup
        token_auth = TokenAuth()
        rbac = RBACManager()
        audit_logger = MemoryAuditLogger()
        
        configure_audit_logger(audit_logger)
        
        # Create user and assign role
        token = token_auth.create_token("service-a", roles=["reader"])
        rbac.assign_role("service-a", "reader")
        
        # Authenticate
        result = token_auth.authenticate(Credentials(token=token))
        
        if result.success:
            audit_log(
                EventType.AUTH_SUCCESS,
                user=result.identity,
                action="token_auth",
            )
            
            # Check permission
            if rbac.has_permission(result.identity, "read:customers"):
                audit_log(
                    EventType.ACCESS_GRANTED,
                    user=result.identity,
                    resource="customers",
                    action="read",
                )
        
        # Verify audit trail
        events = audit_logger.get_all()
        assert len(events) == 2
        
        # Cleanup
        configure_audit_logger(None)
    
    def test_encrypted_audit_events(self):
        """Test encrypting sensitive data in audit events."""
        key = AESEncryptor.generate_key()
        field_encryptor = FieldEncryptor(key)
        field_encryptor.add_sensitive_field("password")
        field_encryptor.add_sensitive_field("token")
        
        event_data = {
            "user": "alice",
            "action": "login",
            "password": "secret123",
            "token": "abc-xyz",
        }
        
        encrypted = field_encryptor.encrypt_fields(event_data)
        
        assert encrypted["user"] == "alice"  # Not sensitive
        assert encrypted["password"].startswith("encrypted:")
        assert encrypted["token"].startswith("encrypted:")
        
        # Can decrypt
        decrypted = field_encryptor.decrypt_fields(encrypted)
        assert decrypted["password"] == "secret123"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
