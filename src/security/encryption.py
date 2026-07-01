"""
Encryption Module

Provides data protection capabilities:
- Symmetric encryption (AES)
- Field-level encryption for sensitive data
- Password hashing and verification
- Key management

SIMPLE EXPLANATION:
Encryption is like putting data in a locked box:
- Encrypt: Lock the data with a key
- Decrypt: Unlock the data with the same key
- Only someone with the key can read the data
"""

import base64
import hashlib
import hmac
import logging
import os
import secrets
import struct
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Union, Tuple

logger = logging.getLogger(__name__)


class EncryptionError(Exception):
    """Encryption-related error."""
    pass


class DecryptionError(Exception):
    """Decryption-related error."""
    pass


class KeyNotFoundError(Exception):
    """Encryption key not found."""
    pass


class Encryptor(ABC):
    """
    Abstract encryptor interface.
    
    Implement this to add new encryption methods.
    """
    
    @abstractmethod
    def encrypt(self, data: bytes) -> bytes:
        """Encrypt data."""
        pass
    
    @abstractmethod
    def decrypt(self, data: bytes) -> bytes:
        """Decrypt data."""
        pass


class AESEncryptor(Encryptor):
    """
    AES encryption using pure Python.
    
    Implements AES-256 in CTR mode for streaming encryption.
    
    SIMPLE EXPLANATION:
    AES is a secure encryption algorithm:
    - Takes a 256-bit key (32 bytes)
    - Encrypts data in 16-byte blocks
    - CTR mode allows streaming encryption
    
    USAGE:
        key = AESEncryptor.generate_key()
        encryptor = AESEncryptor(key)
        
        encrypted = encryptor.encrypt(b"secret data")
        decrypted = encryptor.decrypt(encrypted)
    
    NOTE: For production, use the `cryptography` library.
    This implementation is for demonstration purposes.
    """
    
    # AES S-box (substitution box)
    _SBOX = [
        0x63, 0x7c, 0x77, 0x7b, 0xf2, 0x6b, 0x6f, 0xc5,
        0x30, 0x01, 0x67, 0x2b, 0xfe, 0xd7, 0xab, 0x76,
        0xca, 0x82, 0xc9, 0x7d, 0xfa, 0x59, 0x47, 0xf0,
        0xad, 0xd4, 0xa2, 0xaf, 0x9c, 0xa4, 0x72, 0xc0,
        0xb7, 0xfd, 0x93, 0x26, 0x36, 0x3f, 0xf7, 0xcc,
        0x34, 0xa5, 0xe5, 0xf1, 0x71, 0xd8, 0x31, 0x15,
        0x04, 0xc7, 0x23, 0xc3, 0x18, 0x96, 0x05, 0x9a,
        0x07, 0x12, 0x80, 0xe2, 0xeb, 0x27, 0xb2, 0x75,
        0x09, 0x83, 0x2c, 0x1a, 0x1b, 0x6e, 0x5a, 0xa0,
        0x52, 0x3b, 0xd6, 0xb3, 0x29, 0xe3, 0x2f, 0x84,
        0x53, 0xd1, 0x00, 0xed, 0x20, 0xfc, 0xb1, 0x5b,
        0x6a, 0xcb, 0xbe, 0x39, 0x4a, 0x4c, 0x58, 0xcf,
        0xd0, 0xef, 0xaa, 0xfb, 0x43, 0x4d, 0x33, 0x85,
        0x45, 0xf9, 0x02, 0x7f, 0x50, 0x3c, 0x9f, 0xa8,
        0x51, 0xa3, 0x40, 0x8f, 0x92, 0x9d, 0x38, 0xf5,
        0xbc, 0xb6, 0xda, 0x21, 0x10, 0xff, 0xf3, 0xd2,
        0xcd, 0x0c, 0x13, 0xec, 0x5f, 0x97, 0x44, 0x17,
        0xc4, 0xa7, 0x7e, 0x3d, 0x64, 0x5d, 0x19, 0x73,
        0x60, 0x81, 0x4f, 0xdc, 0x22, 0x2a, 0x90, 0x88,
        0x46, 0xee, 0xb8, 0x14, 0xde, 0x5e, 0x0b, 0xdb,
        0xe0, 0x32, 0x3a, 0x0a, 0x49, 0x06, 0x24, 0x5c,
        0xc2, 0xd3, 0xac, 0x62, 0x91, 0x95, 0xe4, 0x79,
        0xe7, 0xc8, 0x37, 0x6d, 0x8d, 0xd5, 0x4e, 0xa9,
        0x6c, 0x56, 0xf4, 0xea, 0x65, 0x7a, 0xae, 0x08,
        0xba, 0x78, 0x25, 0x2e, 0x1c, 0xa6, 0xb4, 0xc6,
        0xe8, 0xdd, 0x74, 0x1f, 0x4b, 0xbd, 0x8b, 0x8a,
        0x70, 0x3e, 0xb5, 0x66, 0x48, 0x03, 0xf6, 0x0e,
        0x61, 0x35, 0x57, 0xb9, 0x86, 0xc1, 0x1d, 0x9e,
        0xe1, 0xf8, 0x98, 0x11, 0x69, 0xd9, 0x8e, 0x94,
        0x9b, 0x1e, 0x87, 0xe9, 0xce, 0x55, 0x28, 0xdf,
        0x8c, 0xa1, 0x89, 0x0d, 0xbf, 0xe6, 0x42, 0x68,
        0x41, 0x99, 0x2d, 0x0f, 0xb0, 0x54, 0xbb, 0x16,
    ]
    
    # Round constants
    _RCON = [0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x1b, 0x36]
    
    def __init__(self, key: bytes):
        """
        Initialize AES encryptor.
        
        Args:
            key: 32-byte (256-bit) encryption key
        """
        if len(key) != 32:
            raise ValueError("Key must be 32 bytes (256 bits)")
        
        self.key = key
        self._round_keys = self._expand_key(key)
    
    @staticmethod
    def generate_key() -> bytes:
        """Generate a random 256-bit key."""
        return secrets.token_bytes(32)
    
    def _expand_key(self, key: bytes) -> List[List[int]]:
        """Expand key for AES rounds."""
        # Key expansion for AES-256 (14 rounds)
        n_k = 8  # Key length in 32-bit words
        n_r = 14  # Number of rounds
        
        # Convert key to words
        w = []
        for i in range(n_k):
            w.append(list(key[4*i:4*i+4]))
        
        for i in range(n_k, 4 * (n_r + 1)):
            temp = w[i - 1][:]
            
            if i % n_k == 0:
                # RotWord + SubWord + Rcon
                temp = [self._SBOX[b] for b in [temp[1], temp[2], temp[3], temp[0]]]
                temp[0] ^= self._RCON[(i // n_k) - 1]
            elif n_k > 6 and i % n_k == 4:
                temp = [self._SBOX[b] for b in temp]
            
            w.append([w[i - n_k][j] ^ temp[j] for j in range(4)])
        
        # Convert to round keys
        round_keys = []
        for i in range(n_r + 1):
            round_key = []
            for j in range(4):
                round_key.extend(w[4*i + j])
            round_keys.append(round_key)
        
        return round_keys
    
    def _aes_encrypt_block(self, block: bytes) -> bytes:
        """Encrypt a single 16-byte block."""
        state = list(block)
        
        # Initial round
        state = self._add_round_key(state, 0)
        
        # Main rounds
        for r in range(1, 14):
            state = self._sub_bytes(state)
            state = self._shift_rows(state)
            state = self._mix_columns(state)
            state = self._add_round_key(state, r)
        
        # Final round
        state = self._sub_bytes(state)
        state = self._shift_rows(state)
        state = self._add_round_key(state, 14)
        
        return bytes(state)
    
    def _add_round_key(self, state: List[int], round_num: int) -> List[int]:
        """XOR state with round key."""
        rk = self._round_keys[round_num]
        return [state[i] ^ rk[i] for i in range(16)]
    
    def _sub_bytes(self, state: List[int]) -> List[int]:
        """Apply S-box substitution."""
        return [self._SBOX[b] for b in state]
    
    def _shift_rows(self, state: List[int]) -> List[int]:
        """Shift rows transformation."""
        # Convert to matrix (column-major)
        m = [[state[i + 4*j] for j in range(4)] for i in range(4)]
        
        # Shift rows
        for i in range(1, 4):
            m[i] = m[i][i:] + m[i][:i]
        
        # Convert back
        return [m[i % 4][i // 4] for i in range(16)]
    
    def _mix_columns(self, state: List[int]) -> List[int]:
        """Mix columns transformation."""
        def xtime(a: int) -> int:
            return ((a << 1) ^ 0x1b) & 0xff if a & 0x80 else a << 1
        
        def multiply(a: int, b: int) -> int:
            result = 0
            for _ in range(8):
                if b & 1:
                    result ^= a
                a = xtime(a)
                b >>= 1
            return result
        
        result = [0] * 16
        
        for c in range(4):
            i = c * 4
            a = state[i:i+4]
            
            result[i] = multiply(2, a[0]) ^ multiply(3, a[1]) ^ a[2] ^ a[3]
            result[i+1] = a[0] ^ multiply(2, a[1]) ^ multiply(3, a[2]) ^ a[3]
            result[i+2] = a[0] ^ a[1] ^ multiply(2, a[2]) ^ multiply(3, a[3])
            result[i+3] = multiply(3, a[0]) ^ a[1] ^ a[2] ^ multiply(2, a[3])
        
        return result
    
    def encrypt(self, data: bytes) -> bytes:
        """
        Encrypt data using AES-256-CTR.
        
        Format: [16-byte nonce][encrypted data]
        """
        if not data:
            return b""
        
        # Generate random nonce
        nonce = secrets.token_bytes(16)
        
        # CTR mode encryption
        encrypted = bytearray()
        counter = int.from_bytes(nonce, 'big')
        
        for i in range(0, len(data), 16):
            block = data[i:i+16]
            counter_block = counter.to_bytes(16, 'big')
            keystream = self._aes_encrypt_block(counter_block)
            
            encrypted.extend(
                b ^ k for b, k in zip(block, keystream[:len(block)])
            )
            counter += 1
        
        return nonce + bytes(encrypted)
    
    def decrypt(self, data: bytes) -> bytes:
        """
        Decrypt data encrypted with AES-256-CTR.
        
        Expects format: [16-byte nonce][encrypted data]
        """
        if len(data) < 16:
            raise DecryptionError("Data too short")
        
        # Extract nonce
        nonce = data[:16]
        ciphertext = data[16:]
        
        if not ciphertext:
            return b""
        
        # CTR mode decryption (same as encryption)
        decrypted = bytearray()
        counter = int.from_bytes(nonce, 'big')
        
        for i in range(0, len(ciphertext), 16):
            block = ciphertext[i:i+16]
            counter_block = counter.to_bytes(16, 'big')
            keystream = self._aes_encrypt_block(counter_block)
            
            decrypted.extend(
                b ^ k for b, k in zip(block, keystream[:len(block)])
            )
            counter += 1
        
        return bytes(decrypted)


class FieldEncryptor:
    """
    Field-level encryption for sensitive data.
    
    Encrypts specific fields in dictionaries while leaving
    non-sensitive fields in plaintext.
    
    SIMPLE EXPLANATION:
    Instead of encrypting everything, only encrypt sensitive fields:
    - Name: "John" (plaintext)
    - Email: "john@example.com" (plaintext)
    - SSN: "encrypted:abc123..." (encrypted)
    
    USAGE:
        encryptor = FieldEncryptor(key)
        encryptor.add_sensitive_field("ssn")
        encryptor.add_sensitive_field("credit_card")
        
        data = {"name": "John", "ssn": "123-45-6789"}
        encrypted = encryptor.encrypt_fields(data)
        # {"name": "John", "ssn": "encrypted:..."}
        
        decrypted = encryptor.decrypt_fields(encrypted)
        # {"name": "John", "ssn": "123-45-6789"}
    """
    
    ENCRYPTED_PREFIX = "encrypted:"
    
    def __init__(self, key: bytes):
        """
        Initialize field encryptor.
        
        Args:
            key: 32-byte encryption key
        """
        self._encryptor = AESEncryptor(key)
        self._sensitive_fields: set = set()
        self._field_patterns: List[str] = []
    
    def add_sensitive_field(self, field_name: str) -> None:
        """Add a field name to encrypt."""
        self._sensitive_fields.add(field_name.lower())
    
    def add_field_pattern(self, pattern: str) -> None:
        """Add a regex pattern for field names to encrypt."""
        self._field_patterns.append(pattern)
    
    def is_sensitive(self, field_name: str) -> bool:
        """Check if a field should be encrypted."""
        import re
        
        if field_name.lower() in self._sensitive_fields:
            return True
        
        for pattern in self._field_patterns:
            if re.match(pattern, field_name, re.IGNORECASE):
                return True
        
        return False
    
    def encrypt_value(self, value: Any) -> str:
        """Encrypt a single value."""
        if value is None:
            return None
        
        data = str(value).encode('utf-8')
        encrypted = self._encryptor.encrypt(data)
        return self.ENCRYPTED_PREFIX + base64.b64encode(encrypted).decode()
    
    def decrypt_value(self, value: str) -> str:
        """Decrypt a single encrypted value."""
        if not isinstance(value, str):
            return value
        
        if not value.startswith(self.ENCRYPTED_PREFIX):
            return value
        
        try:
            encoded = value[len(self.ENCRYPTED_PREFIX):]
            encrypted = base64.b64decode(encoded)
            decrypted = self._encryptor.decrypt(encrypted)
            return decrypted.decode('utf-8')
        except Exception as e:
            raise DecryptionError(f"Failed to decrypt value: {e}")
    
    def encrypt_fields(
        self,
        data: Dict[str, Any],
        recursive: bool = True,
    ) -> Dict[str, Any]:
        """
        Encrypt sensitive fields in a dictionary.
        
        Args:
            data: Dictionary to encrypt
            recursive: Encrypt nested dictionaries
            
        Returns:
            Dictionary with sensitive fields encrypted
        """
        result = {}
        
        for key, value in data.items():
            if self.is_sensitive(key) and value is not None:
                result[key] = self.encrypt_value(value)
            elif isinstance(value, dict) and recursive:
                result[key] = self.encrypt_fields(value, recursive)
            elif isinstance(value, list) and recursive:
                result[key] = [
                    self.encrypt_fields(item, recursive)
                    if isinstance(item, dict)
                    else item
                    for item in value
                ]
            else:
                result[key] = value
        
        return result
    
    def decrypt_fields(
        self,
        data: Dict[str, Any],
        recursive: bool = True,
    ) -> Dict[str, Any]:
        """
        Decrypt encrypted fields in a dictionary.
        
        Args:
            data: Dictionary to decrypt
            recursive: Decrypt nested dictionaries
            
        Returns:
            Dictionary with fields decrypted
        """
        result = {}
        
        for key, value in data.items():
            if isinstance(value, str) and value.startswith(self.ENCRYPTED_PREFIX):
                result[key] = self.decrypt_value(value)
            elif isinstance(value, dict) and recursive:
                result[key] = self.decrypt_fields(value, recursive)
            elif isinstance(value, list) and recursive:
                result[key] = [
                    self.decrypt_fields(item, recursive)
                    if isinstance(item, dict)
                    else item
                    for item in value
                ]
            else:
                result[key] = value
        
        return result


@dataclass
class EncryptionKey:
    """Metadata for an encryption key."""
    
    key_id: str
    key: bytes
    created_at: datetime
    expires_at: Optional[datetime] = None
    algorithm: str = "AES-256-CTR"
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_expired(self) -> bool:
        """Check if key is expired."""
        if not self.expires_at:
            return False
        return datetime.now(timezone.utc) > self.expires_at


class KeyManager:
    """
    Encryption key management.
    
    Handles key generation, storage, rotation, and retrieval.
    
    SIMPLE EXPLANATION:
    Key management is like managing the keys to your locks:
    - Generate: Create new keys
    - Store: Keep keys safe
    - Rotate: Replace old keys with new ones
    - Retrieve: Get the right key when needed
    
    USAGE:
        km = KeyManager()
        
        # Generate a new key
        key_id = km.generate_key()
        
        # Get the key for encryption
        key = km.get_key(key_id)
        
        # Rotate keys
        new_key_id = km.rotate_key(key_id)
    """
    
    def __init__(self):
        self._keys: Dict[str, EncryptionKey] = {}
        self._current_key_id: Optional[str] = None
    
    def generate_key(
        self,
        key_id: Optional[str] = None,
        ttl: Optional[timedelta] = None,
        metadata: Optional[Dict[str, Any]] = None,
        set_as_current: bool = True,
    ) -> str:
        """
        Generate a new encryption key.
        
        Args:
            key_id: Key identifier (auto-generated if not provided)
            ttl: Key lifetime
            metadata: Additional metadata
            set_as_current: Set as current active key
            
        Returns:
            Key ID
        """
        key_id = key_id or f"key-{secrets.token_hex(8)}"
        key = AESEncryptor.generate_key()
        
        now = datetime.now(timezone.utc)
        expires_at = now + ttl if ttl else None
        
        self._keys[key_id] = EncryptionKey(
            key_id=key_id,
            key=key,
            created_at=now,
            expires_at=expires_at,
            metadata=metadata or {},
        )
        
        if set_as_current:
            self._current_key_id = key_id
        
        logger.info(f"Generated key: {key_id}")
        return key_id
    
    def get_key(self, key_id: str) -> bytes:
        """Get encryption key by ID."""
        if key_id not in self._keys:
            raise KeyNotFoundError(f"Key not found: {key_id}")
        
        enc_key = self._keys[key_id]
        
        if not enc_key.active:
            raise KeyNotFoundError(f"Key inactive: {key_id}")
        
        if enc_key.is_expired:
            raise KeyNotFoundError(f"Key expired: {key_id}")
        
        return enc_key.key
    
    def get_current_key(self) -> Tuple[str, bytes]:
        """Get the current active key."""
        if not self._current_key_id:
            raise KeyNotFoundError("No current key set")
        
        return self._current_key_id, self.get_key(self._current_key_id)
    
    def rotate_key(self, old_key_id: Optional[str] = None) -> str:
        """
        Rotate to a new key.
        
        Args:
            old_key_id: Key to rotate (uses current if not specified)
            
        Returns:
            New key ID
        """
        old_key_id = old_key_id or self._current_key_id
        
        if old_key_id and old_key_id in self._keys:
            # Mark old key as inactive
            self._keys[old_key_id].active = False
        
        # Generate new key
        new_key_id = self.generate_key(set_as_current=True)
        
        logger.info(f"Rotated key from {old_key_id} to {new_key_id}")
        return new_key_id
    
    def deactivate_key(self, key_id: str) -> bool:
        """Deactivate a key."""
        if key_id in self._keys:
            self._keys[key_id].active = False
            logger.info(f"Deactivated key: {key_id}")
            return True
        return False
    
    def list_keys(self, include_inactive: bool = False) -> List[str]:
        """List all key IDs."""
        return [
            key_id for key_id, key in self._keys.items()
            if include_inactive or key.active
        ]
    
    def import_key(
        self,
        key_id: str,
        key: bytes,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Import an external key."""
        self._keys[key_id] = EncryptionKey(
            key_id=key_id,
            key=key,
            created_at=datetime.now(timezone.utc),
            metadata=metadata or {},
        )
        logger.info(f"Imported key: {key_id}")
    
    def export_key(self, key_id: str) -> bytes:
        """Export a key (dangerous - use with caution)."""
        return self.get_key(key_id)


# Password hashing functions

def hash_password(
    password: str,
    salt: Optional[bytes] = None,
    iterations: int = 100000,
) -> str:
    """
    Hash a password using PBKDF2-SHA256.
    
    SIMPLE EXPLANATION:
    Password hashing makes passwords safe to store:
    - Input: "mypassword123"
    - Output: "salt$iterations$hash..."
    - Cannot be reversed to get the password
    
    Args:
        password: Password to hash
        salt: Optional salt (generated if not provided)
        iterations: Number of PBKDF2 iterations
        
    Returns:
        Encoded hash string: salt$iterations$hash
    """
    salt = salt or secrets.token_bytes(16)
    
    hash_bytes = hashlib.pbkdf2_hmac(
        'sha256',
        password.encode('utf-8'),
        salt,
        iterations,
        dklen=32,
    )
    
    # Encode as: salt$iterations$hash
    salt_b64 = base64.b64encode(salt).decode()
    hash_b64 = base64.b64encode(hash_bytes).decode()
    
    return f"{salt_b64}${iterations}${hash_b64}"


def verify_password(password: str, hashed: str) -> bool:
    """
    Verify a password against a hash.
    
    Args:
        password: Password to verify
        hashed: Encoded hash string
        
    Returns:
        True if password matches
    """
    try:
        parts = hashed.split("$")
        if len(parts) != 3:
            return False
        
        salt_b64, iterations_str, hash_b64 = parts
        salt = base64.b64decode(salt_b64)
        iterations = int(iterations_str)
        expected_hash = base64.b64decode(hash_b64)
        
        actual_hash = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt,
            iterations,
            dklen=32,
        )
        
        return hmac.compare_digest(actual_hash, expected_hash)
    
    except Exception:
        return False


def generate_secure_token(length: int = 32) -> str:
    """Generate a cryptographically secure random token."""
    return secrets.token_urlsafe(length)


def constant_time_compare(a: str, b: str) -> bool:
    """Compare two strings in constant time (prevents timing attacks)."""
    return hmac.compare_digest(a.encode(), b.encode())
