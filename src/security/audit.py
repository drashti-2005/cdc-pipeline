"""
Audit Logging Module

Provides security audit logging capabilities:
- Event tracking for authentication/authorization
- Tamper-evident logging
- Log analysis and reporting

SIMPLE EXPLANATION:
Audit logging is like a security camera:
- Records WHO did WHAT and WHEN
- Cannot be deleted or changed
- Used for security investigations
"""

import asyncio
import hashlib
import json
import logging
import os
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable, Union
from queue import Queue

logger = logging.getLogger(__name__)


class AuditLevel(Enum):
    """Audit event severity levels."""
    
    DEBUG = auto()      # Detailed debugging info
    INFO = auto()       # Normal operations
    WARNING = auto()    # Potential issues
    SECURITY = auto()   # Security-relevant events
    CRITICAL = auto()   # Critical security events
    
    @classmethod
    def from_string(cls, level_str: str) -> "AuditLevel":
        """Parse level from string."""
        return cls[level_str.upper()]
    
    def __str__(self) -> str:
        return self.name


class EventType(Enum):
    """Types of audit events."""
    
    # Authentication events
    AUTH_SUCCESS = "auth.success"
    AUTH_FAILURE = "auth.failure"
    AUTH_LOGOUT = "auth.logout"
    TOKEN_CREATED = "auth.token.created"
    TOKEN_REVOKED = "auth.token.revoked"
    TOKEN_EXPIRED = "auth.token.expired"
    
    # Authorization events
    ACCESS_GRANTED = "access.granted"
    ACCESS_DENIED = "access.denied"
    PERMISSION_CHANGED = "access.permission.changed"
    ROLE_ASSIGNED = "access.role.assigned"
    ROLE_REVOKED = "access.role.revoked"
    
    # Data events
    DATA_READ = "data.read"
    DATA_CREATED = "data.created"
    DATA_UPDATED = "data.updated"
    DATA_DELETED = "data.deleted"
    DATA_EXPORTED = "data.exported"
    
    # System events
    CONFIG_CHANGED = "system.config.changed"
    KEY_ROTATED = "system.key.rotated"
    SERVICE_STARTED = "system.service.started"
    SERVICE_STOPPED = "system.service.stopped"
    
    # Pipeline events
    PIPELINE_STARTED = "pipeline.started"
    PIPELINE_STOPPED = "pipeline.stopped"
    PIPELINE_ERROR = "pipeline.error"
    
    # Custom event
    CUSTOM = "custom"


@dataclass
class AuditEvent:
    """
    An audit log event.
    
    Contains all information about a security-relevant event.
    """
    
    event_type: Union[EventType, str]
    timestamp: datetime
    level: AuditLevel = AuditLevel.INFO
    
    # Who performed the action
    user: Optional[str] = None
    service: Optional[str] = None
    
    # What resource was accessed
    resource: Optional[str] = None
    resource_id: Optional[str] = None
    
    # What action was performed
    action: Optional[str] = None
    result: Optional[str] = None  # "success", "failure", etc.
    
    # Additional details
    details: Dict[str, Any] = field(default_factory=dict)
    
    # Request context
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    request_id: Optional[str] = None
    
    # Integrity
    event_id: Optional[str] = None
    previous_hash: Optional[str] = None
    
    def __post_init__(self):
        """Generate event ID if not provided."""
        if not self.event_id:
            self.event_id = self._generate_id()
    
    def _generate_id(self) -> str:
        """Generate unique event ID."""
        import secrets
        return f"evt-{secrets.token_hex(8)}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = {
            "event_id": self.event_id,
            "event_type": str(self.event_type.value if isinstance(self.event_type, EventType) else self.event_type),
            "timestamp": self.timestamp.isoformat(),
            "level": str(self.level),
            "user": self.user,
            "service": self.service,
            "resource": self.resource,
            "resource_id": self.resource_id,
            "action": self.action,
            "result": self.result,
            "details": self.details,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "request_id": self.request_id,
            "previous_hash": self.previous_hash,
        }
        return {k: v for k, v in data.items() if v is not None}
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), default=str)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AuditEvent":
        """Create event from dictionary."""
        # Parse event type
        event_type_str = data.get("event_type", "custom")
        try:
            event_type = EventType(event_type_str)
        except ValueError:
            event_type = event_type_str
        
        # Parse timestamp
        timestamp = data.get("timestamp")
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
        elif not timestamp:
            timestamp = datetime.now(timezone.utc)
        
        # Parse level
        level_str = data.get("level", "INFO")
        if isinstance(level_str, str):
            level = AuditLevel.from_string(level_str)
        else:
            level = level_str
        
        return cls(
            event_id=data.get("event_id"),
            event_type=event_type,
            timestamp=timestamp,
            level=level,
            user=data.get("user"),
            service=data.get("service"),
            resource=data.get("resource"),
            resource_id=data.get("resource_id"),
            action=data.get("action"),
            result=data.get("result"),
            details=data.get("details", {}),
            ip_address=data.get("ip_address"),
            user_agent=data.get("user_agent"),
            request_id=data.get("request_id"),
            previous_hash=data.get("previous_hash"),
        )
    
    def compute_hash(self) -> str:
        """Compute hash of this event for chain integrity."""
        data = self.to_json()
        return hashlib.sha256(data.encode()).hexdigest()


class AuditLogger(ABC):
    """
    Abstract audit logger.
    
    Implement this to create new audit log destinations.
    """
    
    @abstractmethod
    def log(self, event: AuditEvent) -> None:
        """Log an audit event."""
        pass
    
    @abstractmethod
    def query(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        event_type: Optional[EventType] = None,
        user: Optional[str] = None,
        level: Optional[AuditLevel] = None,
        limit: int = 100,
    ) -> List[AuditEvent]:
        """Query audit events."""
        pass


class FileAuditLogger(AuditLogger):
    """
    File-based audit logger.
    
    Writes audit events to JSON lines files with hash chaining
    for tamper detection.
    
    SIMPLE EXPLANATION:
    Each event is:
    1. Written to a file
    2. Linked to the previous event with a hash
    3. If someone changes an event, the chain breaks
    
    USAGE:
        logger = FileAuditLogger("/var/log/audit")
        
        # Log events
        logger.log(AuditEvent(
            event_type=EventType.AUTH_SUCCESS,
            timestamp=datetime.now(timezone.utc),
            user="alice",
        ))
        
        # Query events
        events = logger.query(user="alice", limit=10)
    """
    
    def __init__(
        self,
        log_dir: Union[str, Path],
        rotation_size_mb: int = 100,
        compression: bool = True,
    ):
        """
        Initialize file audit logger.
        
        Args:
            log_dir: Directory for audit logs
            rotation_size_mb: Rotate logs at this size
            compression: Compress rotated logs
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        self.rotation_size = rotation_size_mb * 1024 * 1024
        self.compression = compression
        
        self._current_file: Optional[Path] = None
        self._file_handle = None
        self._last_hash: Optional[str] = None
        self._lock = threading.Lock()
        
        # Initialize current log file
        self._ensure_current_file()
    
    def _ensure_current_file(self) -> None:
        """Ensure we have a current log file open."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        log_file = self.log_dir / f"audit-{today}.jsonl"
        
        if self._current_file != log_file:
            if self._file_handle:
                self._file_handle.close()
            
            self._current_file = log_file
            self._file_handle = open(log_file, "a", encoding="utf-8")
            
            # Load last hash from file
            self._last_hash = self._get_last_hash(log_file)
    
    def _get_last_hash(self, log_file: Path) -> Optional[str]:
        """Get hash of last event in file."""
        if not log_file.exists():
            return None
        
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                last_line = None
                for line in f:
                    if line.strip():
                        last_line = line
                
                if last_line:
                    event_data = json.loads(last_line)
                    event = AuditEvent.from_dict(event_data)
                    return event.compute_hash()
        except Exception:
            pass
        
        return None
    
    def _rotate_if_needed(self) -> None:
        """Rotate log file if it exceeds size limit."""
        if not self._current_file or not self._current_file.exists():
            return
        
        if self._current_file.stat().st_size < self.rotation_size:
            return
        
        # Close current file
        if self._file_handle:
            self._file_handle.close()
            self._file_handle = None
        
        # Rename with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        rotated = self._current_file.with_suffix(f".{timestamp}.jsonl")
        self._current_file.rename(rotated)
        
        # Compress if enabled
        if self.compression:
            import gzip
            with open(rotated, "rb") as f_in:
                with gzip.open(f"{rotated}.gz", "wb") as f_out:
                    f_out.write(f_in.read())
            rotated.unlink()
        
        # Reset
        self._current_file = None
        self._last_hash = None
        self._ensure_current_file()
    
    def log(self, event: AuditEvent) -> None:
        """Log an audit event."""
        with self._lock:
            self._ensure_current_file()
            self._rotate_if_needed()
            
            # Add chain hash
            if self._last_hash:
                event.previous_hash = self._last_hash
            
            # Write event
            line = event.to_json() + "\n"
            self._file_handle.write(line)
            self._file_handle.flush()
            
            # Update last hash
            self._last_hash = event.compute_hash()
    
    def query(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        event_type: Optional[EventType] = None,
        user: Optional[str] = None,
        level: Optional[AuditLevel] = None,
        limit: int = 100,
    ) -> List[AuditEvent]:
        """Query audit events from files."""
        events = []
        
        # Find relevant log files
        log_files = sorted(self.log_dir.glob("audit-*.jsonl"), reverse=True)
        
        for log_file in log_files:
            if len(events) >= limit:
                break
            
            try:
                with open(log_file, "r", encoding="utf-8") as f:
                    for line in f:
                        if not line.strip():
                            continue
                        
                        event = AuditEvent.from_dict(json.loads(line))
                        
                        # Apply filters
                        if start_time and event.timestamp < start_time:
                            continue
                        if end_time and event.timestamp > end_time:
                            continue
                        if event_type and event.event_type != event_type:
                            continue
                        if user and event.user != user:
                            continue
                        if level and event.level != level:
                            continue
                        
                        events.append(event)
                        
                        if len(events) >= limit:
                            break
            except Exception as e:
                logger.warning(f"Error reading audit log {log_file}: {e}")
        
        return events
    
    def verify_chain(self, log_file: Optional[Path] = None) -> bool:
        """
        Verify the integrity of the audit log chain.
        
        Returns True if chain is intact, False if tampered.
        """
        log_file = log_file or self._current_file
        if not log_file or not log_file.exists():
            return True
        
        previous_hash = None
        
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    
                    event = AuditEvent.from_dict(json.loads(line))
                    
                    # Check chain
                    if event.previous_hash != previous_hash:
                        logger.warning(f"Chain broken at event {event.event_id}")
                        return False
                    
                    previous_hash = event.compute_hash()
            
            return True
        
        except Exception as e:
            logger.error(f"Error verifying chain: {e}")
            return False
    
    def close(self) -> None:
        """Close the logger."""
        if self._file_handle:
            self._file_handle.close()
            self._file_handle = None


class MemoryAuditLogger(AuditLogger):
    """
    In-memory audit logger for testing.
    
    Stores events in memory with optional size limit.
    """
    
    def __init__(self, max_events: int = 10000):
        """Initialize memory logger."""
        self._events: List[AuditEvent] = []
        self._max_events = max_events
        self._lock = threading.Lock()
    
    def log(self, event: AuditEvent) -> None:
        """Log an event."""
        with self._lock:
            self._events.append(event)
            
            # Trim if over limit
            if len(self._events) > self._max_events:
                self._events = self._events[-self._max_events:]
    
    def query(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        event_type: Optional[EventType] = None,
        user: Optional[str] = None,
        level: Optional[AuditLevel] = None,
        limit: int = 100,
    ) -> List[AuditEvent]:
        """Query events."""
        results = []
        
        with self._lock:
            for event in reversed(self._events):
                if start_time and event.timestamp < start_time:
                    continue
                if end_time and event.timestamp > end_time:
                    continue
                if event_type and event.event_type != event_type:
                    continue
                if user and event.user != user:
                    continue
                if level and event.level != level:
                    continue
                
                results.append(event)
                
                if len(results) >= limit:
                    break
        
        return results
    
    def clear(self) -> None:
        """Clear all events."""
        with self._lock:
            self._events.clear()
    
    def get_all(self) -> List[AuditEvent]:
        """Get all events."""
        with self._lock:
            return self._events.copy()


class AsyncAuditLogger(AuditLogger):
    """
    Async wrapper for audit loggers.
    
    Buffers events and writes them asynchronously.
    """
    
    def __init__(self, backend: AuditLogger, batch_size: int = 100):
        """Initialize async logger."""
        self._backend = backend
        self._batch_size = batch_size
        self._queue: Queue = Queue()
        self._running = False
        self._thread: Optional[threading.Thread] = None
    
    def start(self) -> None:
        """Start background writer."""
        self._running = True
        self._thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._thread.start()
    
    def stop(self) -> None:
        """Stop background writer."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
    
    def _writer_loop(self) -> None:
        """Background writer loop."""
        while self._running or not self._queue.empty():
            try:
                event = self._queue.get(timeout=1)
                self._backend.log(event)
            except Exception:
                continue
    
    def log(self, event: AuditEvent) -> None:
        """Queue event for async logging."""
        self._queue.put(event)
    
    def query(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        event_type: Optional[EventType] = None,
        user: Optional[str] = None,
        level: Optional[AuditLevel] = None,
        limit: int = 100,
    ) -> List[AuditEvent]:
        """Query from backend."""
        return self._backend.query(
            start_time, end_time, event_type, user, level, limit
        )


# Global audit logger instance
_global_logger: Optional[AuditLogger] = None


def configure_audit_logger(logger: AuditLogger) -> None:
    """Configure the global audit logger."""
    global _global_logger
    _global_logger = logger


def get_audit_logger() -> Optional[AuditLogger]:
    """Get the global audit logger."""
    return _global_logger


def audit_log(
    event_type: Union[EventType, str],
    user: Optional[str] = None,
    resource: Optional[str] = None,
    action: Optional[str] = None,
    result: Optional[str] = None,
    level: AuditLevel = AuditLevel.INFO,
    **kwargs,
) -> None:
    """
    Log an audit event using the global logger.
    
    USAGE:
        audit_log(
            EventType.AUTH_SUCCESS,
            user="alice",
            action="login",
            result="success",
        )
    """
    if not _global_logger:
        return
    
    event = AuditEvent(
        event_type=event_type,
        timestamp=datetime.now(timezone.utc),
        level=level,
        user=user,
        resource=resource,
        action=action,
        result=result,
        **kwargs,
    )
    
    _global_logger.log(event)


def audit_decorator(
    event_type: Union[EventType, str],
    resource: Optional[str] = None,
    action: Optional[str] = None,
    user_param: str = "user",
    include_args: bool = False,
) -> Callable:
    """
    Decorator to automatically log function calls.
    
    USAGE:
        @audit_decorator(EventType.DATA_READ, resource="customers")
        def get_customer(user: str, customer_id: str):
            ...
    """
    def decorator(func: Callable) -> Callable:
        import functools
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            user = kwargs.get(user_param)
            
            details = {}
            if include_args:
                details["args"] = str(args)
                details["kwargs"] = {k: str(v) for k, v in kwargs.items()}
            
            try:
                result = func(*args, **kwargs)
                
                audit_log(
                    event_type=event_type,
                    user=user,
                    resource=resource,
                    action=action or func.__name__,
                    result="success",
                    details=details,
                )
                
                return result
            
            except Exception as e:
                audit_log(
                    event_type=event_type,
                    user=user,
                    resource=resource,
                    action=action or func.__name__,
                    result="failure",
                    level=AuditLevel.WARNING,
                    details={**details, "error": str(e)},
                )
                raise
        
        return wrapper
    return decorator
