"""
Authentication Module

Provides identity verification for users and services:
- Token-based authentication
- API key authentication  
- JWT (JSON Web Token) authentication

SIMPLE EXPLANATION:
Authentication is like checking someone's ID at the door:
- "Who are you?" → Show your credentials
- "Are those valid?" → Verify the credentials
- "Come in" or "Access denied" → Grant or reject
"""

import base64
import hashlib
import hmac
import json
import logging
import secrets
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Callable

logger = logging.getLogger(__name__)


class AuthError(Exception):
    """Authentication error."""
    
    def __init__(self, message: str, code: str = "AUTH_ERROR"):
        super().__init__(message)
        self.code = code


class InvalidCredentialsError(AuthError):
    """Invalid credentials provided."""
    
    def __init__(self, message: str = "Invalid credentials"):
        super().__init__(message, "INVALID_CREDENTIALS")


class ExpiredTokenError(AuthError):
    """Token has expired."""
    
    def __init__(self, message: str = "Token expired"):
        super().__init__(message, "TOKEN_EXPIRED")


class InvalidTokenError(AuthError):
    """Token is invalid."""
    
    def __init__(self, message: str = "Invalid token"):
        super().__init__(message, "INVALID_TOKEN")


@dataclass
class Credentials:
    """
    User or service credentials.
    
    Can hold different types of credentials:
    - username/password
    - API key
    - JWT token
    """
    
    username: Optional[str] = None
    password: Optional[str] = None
    api_key: Optional[str] = None
    token: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def is_empty(self) -> bool:
        """Check if credentials are empty."""
        return not any([self.username, self.api_key, self.token])


@dataclass
class AuthResult:
    """
    Result of authentication attempt.
    
    Contains identity information if successful.
    """
    
    success: bool
    identity: Optional[str] = None  # Username or service ID
    roles: List[str] = field(default_factory=list)
    permissions: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    expires_at: Optional[datetime] = None
    
    @property
    def is_expired(self) -> bool:
        """Check if auth result is expired."""
        if not self.expires_at:
            return False
        return datetime.now(timezone.utc) > self.expires_at
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "identity": self.identity,
            "roles": self.roles,
            "permissions": self.permissions,
            "metadata": self.metadata,
            "error": self.error,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
        }


class AuthProvider(ABC):
    """
    Abstract authentication provider.
    
    Implement this to add new authentication methods.
    """
    
    @abstractmethod
    def authenticate(self, credentials: Credentials) -> AuthResult:
        """
        Authenticate credentials.
        
        Args:
            credentials: Credentials to verify
            
        Returns:
            AuthResult with success/failure and identity
        """
        pass
    
    @abstractmethod
    def validate(self, token: str) -> AuthResult:
        """
        Validate an existing token.
        
        Args:
            token: Token to validate
            
        Returns:
            AuthResult with identity if valid
        """
        pass


class TokenAuth(AuthProvider):
    """
    Simple token-based authentication.
    
    Tokens are opaque strings that map to identities.
    Good for service-to-service authentication.
    
    USAGE:
        auth = TokenAuth()
        
        # Register a token
        token = auth.create_token("service-a", roles=["reader"])
        
        # Authenticate with token
        result = auth.authenticate(Credentials(token=token))
        print(f"Identity: {result.identity}")
    """
    
    def __init__(
        self,
        token_length: int = 32,
        default_ttl_hours: int = 24,
    ):
        """
        Initialize token auth.
        
        Args:
            token_length: Length of generated tokens
            default_ttl_hours: Default token lifetime
        """
        self.token_length = token_length
        self.default_ttl = timedelta(hours=default_ttl_hours)
        
        # Token storage: token → identity info
        self._tokens: Dict[str, Dict[str, Any]] = {}
    
    def create_token(
        self,
        identity: str,
        roles: Optional[List[str]] = None,
        permissions: Optional[List[str]] = None,
        ttl: Optional[timedelta] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Create a new authentication token.
        
        Args:
            identity: User or service identity
            roles: Assigned roles
            permissions: Direct permissions
            ttl: Token lifetime (default: 24h)
            metadata: Additional metadata
            
        Returns:
            Token string
        """
        token = secrets.token_urlsafe(self.token_length)
        ttl = ttl or self.default_ttl
        
        self._tokens[token] = {
            "identity": identity,
            "roles": roles or [],
            "permissions": permissions or [],
            "metadata": metadata or {},
            "created_at": datetime.now(timezone.utc),
            "expires_at": datetime.now(timezone.utc) + ttl,
        }
        
        logger.info(f"Created token for {identity}, expires in {ttl}")
        return token
    
    def revoke_token(self, token: str) -> bool:
        """Revoke a token."""
        if token in self._tokens:
            del self._tokens[token]
            logger.info("Token revoked")
            return True
        return False
    
    def authenticate(self, credentials: Credentials) -> AuthResult:
        """Authenticate using token."""
        if not credentials.token:
            return AuthResult(success=False, error="No token provided")
        
        return self.validate(credentials.token)
    
    def validate(self, token: str) -> AuthResult:
        """Validate a token."""
        if token not in self._tokens:
            return AuthResult(success=False, error="Invalid token")
        
        info = self._tokens[token]
        
        # Check expiration
        if datetime.now(timezone.utc) > info["expires_at"]:
            del self._tokens[token]
            return AuthResult(success=False, error="Token expired")
        
        return AuthResult(
            success=True,
            identity=info["identity"],
            roles=info["roles"],
            permissions=info["permissions"],
            metadata=info["metadata"],
            expires_at=info["expires_at"],
        )
    
    def cleanup_expired(self) -> int:
        """Remove expired tokens."""
        now = datetime.now(timezone.utc)
        expired = [
            token for token, info in self._tokens.items()
            if info["expires_at"] < now
        ]
        
        for token in expired:
            del self._tokens[token]
        
        if expired:
            logger.info(f"Cleaned up {len(expired)} expired tokens")
        
        return len(expired)


class APIKeyAuth(AuthProvider):
    """
    API key authentication.
    
    API keys are long-lived credentials for services.
    
    USAGE:
        auth = APIKeyAuth()
        
        # Register an API key
        key = auth.create_key("my-service", roles=["admin"])
        
        # Authenticate
        result = auth.authenticate(Credentials(api_key=key))
    """
    
    def __init__(self, key_prefix: str = "cdc_"):
        """
        Initialize API key auth.
        
        Args:
            key_prefix: Prefix for generated keys
        """
        self.key_prefix = key_prefix
        self._keys: Dict[str, Dict[str, Any]] = {}
    
    def create_key(
        self,
        identity: str,
        roles: Optional[List[str]] = None,
        permissions: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Create a new API key."""
        key = f"{self.key_prefix}{secrets.token_urlsafe(32)}"
        
        # Store hash, not the actual key
        key_hash = self._hash_key(key)
        
        self._keys[key_hash] = {
            "identity": identity,
            "roles": roles or [],
            "permissions": permissions or [],
            "metadata": metadata or {},
            "created_at": datetime.now(timezone.utc),
            "last_used": None,
        }
        
        logger.info(f"Created API key for {identity}")
        return key
    
    def _hash_key(self, key: str) -> str:
        """Hash an API key for storage."""
        return hashlib.sha256(key.encode()).hexdigest()
    
    def revoke_key(self, key: str) -> bool:
        """Revoke an API key."""
        key_hash = self._hash_key(key)
        if key_hash in self._keys:
            del self._keys[key_hash]
            logger.info("API key revoked")
            return True
        return False
    
    def authenticate(self, credentials: Credentials) -> AuthResult:
        """Authenticate using API key."""
        if not credentials.api_key:
            return AuthResult(success=False, error="No API key provided")
        
        return self.validate(credentials.api_key)
    
    def validate(self, token: str) -> AuthResult:
        """Validate an API key."""
        key_hash = self._hash_key(token)
        
        if key_hash not in self._keys:
            return AuthResult(success=False, error="Invalid API key")
        
        info = self._keys[key_hash]
        
        # Update last used
        info["last_used"] = datetime.now(timezone.utc)
        
        return AuthResult(
            success=True,
            identity=info["identity"],
            roles=info["roles"],
            permissions=info["permissions"],
            metadata=info["metadata"],
        )


class JWTAuth(AuthProvider):
    """
    JWT (JSON Web Token) authentication.
    
    JWTs are self-contained tokens with embedded claims.
    Good for stateless authentication.
    
    SIMPLE EXPLANATION:
    A JWT is like a sealed envelope with your ID inside:
    - Header: Type of envelope
    - Payload: Your ID and permissions
    - Signature: Seal that proves it's authentic
    
    USAGE:
        auth = JWTAuth(secret="your-secret-key")
        
        # Create token
        token = auth.create_token("user@example.com", roles=["admin"])
        
        # Validate token
        result = auth.validate(token)
        print(f"User: {result.identity}")
    """
    
    def __init__(
        self,
        secret: str,
        algorithm: str = "HS256",
        issuer: str = "cdc-pipeline",
        default_ttl_hours: int = 1,
    ):
        """
        Initialize JWT auth.
        
        Args:
            secret: Secret key for signing
            algorithm: Signing algorithm (HS256, HS384, HS512)
            issuer: Token issuer claim
            default_ttl_hours: Default token lifetime
        """
        self.secret = secret.encode()
        self.algorithm = algorithm
        self.issuer = issuer
        self.default_ttl = timedelta(hours=default_ttl_hours)
        
        # Blacklist for revoked tokens
        self._blacklist: set = set()
    
    def _base64url_encode(self, data: bytes) -> str:
        """URL-safe base64 encoding."""
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()
    
    def _base64url_decode(self, data: str) -> bytes:
        """URL-safe base64 decoding."""
        padding = 4 - len(data) % 4
        if padding != 4:
            data += "=" * padding
        return base64.urlsafe_b64decode(data)
    
    def _sign(self, message: str) -> str:
        """Create HMAC signature."""
        if self.algorithm == "HS256":
            digest = hashlib.sha256
        elif self.algorithm == "HS384":
            digest = hashlib.sha384
        elif self.algorithm == "HS512":
            digest = hashlib.sha512
        else:
            raise ValueError(f"Unsupported algorithm: {self.algorithm}")
        
        signature = hmac.new(
            self.secret,
            message.encode(),
            digest
        ).digest()
        
        return self._base64url_encode(signature)
    
    def create_token(
        self,
        identity: str,
        roles: Optional[List[str]] = None,
        permissions: Optional[List[str]] = None,
        ttl: Optional[timedelta] = None,
        claims: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Create a JWT token.
        
        Args:
            identity: User or service identity
            roles: Assigned roles
            permissions: Direct permissions
            ttl: Token lifetime
            claims: Additional JWT claims
            
        Returns:
            JWT token string
        """
        ttl = ttl or self.default_ttl
        now = datetime.now(timezone.utc)
        
        # Header
        header = {
            "alg": self.algorithm,
            "typ": "JWT",
        }
        
        # Payload
        payload = {
            "sub": identity,
            "iss": self.issuer,
            "iat": int(now.timestamp()),
            "exp": int((now + ttl).timestamp()),
            "roles": roles or [],
            "permissions": permissions or [],
        }
        
        if claims:
            payload.update(claims)
        
        # Encode
        header_b64 = self._base64url_encode(json.dumps(header).encode())
        payload_b64 = self._base64url_encode(json.dumps(payload).encode())
        
        # Sign
        message = f"{header_b64}.{payload_b64}"
        signature = self._sign(message)
        
        token = f"{message}.{signature}"
        logger.debug(f"Created JWT for {identity}")
        return token
    
    def revoke_token(self, token: str) -> None:
        """Add token to blacklist."""
        # Extract jti or use hash
        token_hash = hashlib.sha256(token.encode()).hexdigest()[:16]
        self._blacklist.add(token_hash)
        logger.info("JWT added to blacklist")
    
    def authenticate(self, credentials: Credentials) -> AuthResult:
        """Authenticate using JWT."""
        if not credentials.token:
            return AuthResult(success=False, error="No token provided")
        
        return self.validate(credentials.token)
    
    def validate(self, token: str) -> AuthResult:
        """Validate a JWT token."""
        try:
            # Split token
            parts = token.split(".")
            if len(parts) != 3:
                return AuthResult(success=False, error="Invalid token format")
            
            header_b64, payload_b64, signature = parts
            
            # Verify signature
            message = f"{header_b64}.{payload_b64}"
            expected_sig = self._sign(message)
            
            if not hmac.compare_digest(signature, expected_sig):
                return AuthResult(success=False, error="Invalid signature")
            
            # Decode payload
            payload = json.loads(self._base64url_decode(payload_b64))
            
            # Check blacklist
            token_hash = hashlib.sha256(token.encode()).hexdigest()[:16]
            if token_hash in self._blacklist:
                return AuthResult(success=False, error="Token revoked")
            
            # Check expiration
            exp = payload.get("exp", 0)
            if time.time() > exp:
                return AuthResult(success=False, error="Token expired")
            
            # Check issuer
            if payload.get("iss") != self.issuer:
                return AuthResult(success=False, error="Invalid issuer")
            
            return AuthResult(
                success=True,
                identity=payload.get("sub"),
                roles=payload.get("roles", []),
                permissions=payload.get("permissions", []),
                metadata={k: v for k, v in payload.items() 
                         if k not in ("sub", "iss", "iat", "exp", "roles", "permissions")},
                expires_at=datetime.fromtimestamp(exp, tz=timezone.utc),
            )
            
        except Exception as e:
            logger.warning(f"JWT validation failed: {e}")
            return AuthResult(success=False, error=str(e))


class MultiProviderAuth:
    """
    Authentication with multiple providers.
    
    Tries each provider in order until one succeeds.
    
    USAGE:
        auth = MultiProviderAuth()
        auth.add_provider("token", TokenAuth())
        auth.add_provider("api_key", APIKeyAuth())
        auth.add_provider("jwt", JWTAuth(secret="..."))
        
        # Authenticate with any method
        result = auth.authenticate(credentials)
    """
    
    def __init__(self):
        self._providers: Dict[str, AuthProvider] = {}
    
    def add_provider(self, name: str, provider: AuthProvider) -> None:
        """Add an authentication provider."""
        self._providers[name] = provider
        logger.info(f"Added auth provider: {name}")
    
    def authenticate(
        self,
        credentials: Credentials,
        provider_name: Optional[str] = None,
    ) -> AuthResult:
        """
        Authenticate using available providers.
        
        Args:
            credentials: Credentials to verify
            provider_name: Specific provider to use (optional)
            
        Returns:
            AuthResult from first successful provider
        """
        if provider_name:
            if provider_name not in self._providers:
                return AuthResult(success=False, error=f"Unknown provider: {provider_name}")
            return self._providers[provider_name].authenticate(credentials)
        
        # Try each provider
        for name, provider in self._providers.items():
            try:
                result = provider.authenticate(credentials)
                if result.success:
                    result.metadata["auth_provider"] = name
                    return result
            except Exception as e:
                logger.debug(f"Provider {name} failed: {e}")
                continue
        
        return AuthResult(success=False, error="Authentication failed")
    
    def validate(self, token: str) -> AuthResult:
        """Validate token with any provider."""
        for name, provider in self._providers.items():
            try:
                result = provider.validate(token)
                if result.success:
                    result.metadata["auth_provider"] = name
                    return result
            except Exception:
                continue
        
        return AuthResult(success=False, error="Invalid token")
