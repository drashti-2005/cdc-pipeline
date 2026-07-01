"""
Cross-Region Replication

Manages data replication between regions:
- Async replication: Best effort, eventually consistent
- Sync replication: Strong consistency, higher latency
- Conflict resolution strategies

SIMPLE EXPLANATION:
Replication is like keeping backup copies:
- Write data to primary region
- Copy to other regions automatically
- Handle conflicts when same data is written in multiple places
"""

import asyncio
import logging
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from queue import Queue, Empty
from typing import Dict, List, Optional, Any, Callable, Set

from src.multiregion.config import (
    Region,
    RegionConfig,
    MultiRegionConfig,
    RegionStatus,
)

logger = logging.getLogger(__name__)


class ReplicationMode(Enum):
    """
    Replication mode between regions.
    
    NONE: No replication
    ASYNC: Asynchronous replication (eventually consistent)
    SYNC: Synchronous replication (strongly consistent)
    SEMI_SYNC: At least one replica must acknowledge
    """
    
    NONE = "none"
    ASYNC = "async"
    SYNC = "sync"
    SEMI_SYNC = "semi_sync"


class ReplicationStatus(Enum):
    """Status of a replication operation."""
    
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class ConflictResolution(Enum):
    """
    Strategy for resolving replication conflicts.
    
    LAST_WRITE_WINS: Most recent timestamp wins
    FIRST_WRITE_WINS: Earliest timestamp wins
    SOURCE_WINS: Source region always wins
    MERGE: Attempt to merge changes
    MANUAL: Require manual resolution
    """
    
    LAST_WRITE_WINS = "last_write_wins"
    FIRST_WRITE_WINS = "first_write_wins"
    SOURCE_WINS = "source_wins"
    MERGE = "merge"
    MANUAL = "manual"


@dataclass
class ReplicationLag:
    """
    Replication lag between regions.
    
    Tracks how far behind a replica is from the source.
    """
    
    source_region: str
    target_region: str
    lag_ms: float
    lag_events: int = 0
    last_replicated_offset: int = 0
    last_source_offset: int = 0
    measured_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    @property
    def lag_seconds(self) -> float:
        """Lag in seconds."""
        return self.lag_ms / 1000.0
    
    @property
    def is_caught_up(self) -> bool:
        """Check if replica is caught up (lag < 1 second)."""
        return self.lag_ms < 1000.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "source_region": self.source_region,
            "target_region": self.target_region,
            "lag_ms": self.lag_ms,
            "lag_events": self.lag_events,
            "last_replicated_offset": self.last_replicated_offset,
            "last_source_offset": self.last_source_offset,
            "measured_at": self.measured_at.isoformat(),
        }


@dataclass
class ReplicationEvent:
    """
    An event to be replicated.
    
    Wraps data with metadata for replication tracking.
    """
    
    event_id: str
    source_region: str
    target_regions: List[str]
    data: Dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: ReplicationStatus = ReplicationStatus.PENDING
    attempts: int = 0
    last_error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "event_id": self.event_id,
            "source_region": self.source_region,
            "target_regions": self.target_regions,
            "data": self.data,
            "timestamp": self.timestamp.isoformat(),
            "status": self.status.value,
            "attempts": self.attempts,
            "last_error": self.last_error,
        }


@dataclass
class ReplicationResult:
    """Result of a replication operation."""
    
    event_id: str
    target_region: str
    success: bool
    latency_ms: float
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class ReplicationTransport(ABC):
    """
    Abstract transport for sending data between regions.
    
    Implementations might use Kafka, HTTP, or direct database connections.
    """
    
    @abstractmethod
    def send(
        self,
        target_region: RegionConfig,
        event: ReplicationEvent,
    ) -> ReplicationResult:
        """Send event to target region."""
        pass
    
    @abstractmethod
    def send_batch(
        self,
        target_region: RegionConfig,
        events: List[ReplicationEvent],
    ) -> List[ReplicationResult]:
        """Send batch of events to target region."""
        pass


class MockReplicationTransport(ReplicationTransport):
    """
    Mock transport for testing.
    
    Simulates replication with configurable latency and failure rate.
    """
    
    def __init__(
        self,
        latency_ms: float = 50.0,
        failure_rate: float = 0.0,
    ):
        self.latency_ms = latency_ms
        self.failure_rate = failure_rate
        self._sent_events: List[ReplicationEvent] = []
    
    def send(
        self,
        target_region: RegionConfig,
        event: ReplicationEvent,
    ) -> ReplicationResult:
        """Send event (simulated)."""
        import random
        
        start = time.time()
        
        # Simulate latency
        time.sleep(self.latency_ms / 1000.0)
        
        # Simulate failures
        if random.random() < self.failure_rate:
            return ReplicationResult(
                event_id=event.event_id,
                target_region=target_region.region.name,
                success=False,
                latency_ms=(time.time() - start) * 1000,
                error="Simulated failure",
            )
        
        self._sent_events.append(event)
        
        return ReplicationResult(
            event_id=event.event_id,
            target_region=target_region.region.name,
            success=True,
            latency_ms=(time.time() - start) * 1000,
        )
    
    def send_batch(
        self,
        target_region: RegionConfig,
        events: List[ReplicationEvent],
    ) -> List[ReplicationResult]:
        """Send batch (simulated)."""
        return [self.send(target_region, event) for event in events]
    
    @property
    def sent_events(self) -> List[ReplicationEvent]:
        """Get list of sent events (for testing)."""
        return self._sent_events.copy()


class ReplicationManager:
    """
    Manages cross-region replication.
    
    USAGE:
        config = create_default_config()
        manager = ReplicationManager(config)
        
        # Start replication
        manager.start()
        
        # Replicate an event
        manager.replicate(event_data, source_region="us-east-1")
        
        # Check lag
        lag = manager.get_lag("us-east-1", "eu-west-1")
        print(f"Replication lag: {lag.lag_ms}ms")
        
        # Stop
        manager.stop()
    """
    
    def __init__(
        self,
        config: MultiRegionConfig,
        transport: Optional[ReplicationTransport] = None,
        mode: ReplicationMode = ReplicationMode.ASYNC,
        conflict_resolution: ConflictResolution = ConflictResolution.LAST_WRITE_WINS,
    ):
        """
        Initialize replication manager.
        
        Args:
            config: Multi-region configuration
            transport: Transport for sending data between regions
            mode: Replication mode
            conflict_resolution: Conflict resolution strategy
        """
        self.config = config
        self.transport = transport or MockReplicationTransport()
        self.mode = mode
        self.conflict_resolution = conflict_resolution
        
        # Replication queue (for async mode)
        self._queue: Queue = Queue()
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        
        # Metrics
        self._replicated_count: Dict[str, int] = {}
        self._failed_count: Dict[str, int] = {}
        self._lag: Dict[tuple, ReplicationLag] = {}
        
        # Event tracking
        self._pending_events: Dict[str, ReplicationEvent] = {}
        self._event_id_counter = 0
        self._lock = threading.Lock()
        
        logger.info(f"ReplicationManager initialized: mode={mode.value}")
    
    def _generate_event_id(self) -> str:
        """Generate unique event ID."""
        with self._lock:
            self._event_id_counter += 1
            return f"repl-{self._event_id_counter:08d}"
    
    def start(self) -> None:
        """Start replication workers."""
        if self._running:
            return
        
        self._running = True
        
        if self.mode == ReplicationMode.ASYNC:
            self._worker_thread = threading.Thread(
                target=self._async_worker,
                daemon=True,
            )
            self._worker_thread.start()
            logger.info("Async replication worker started")
    
    def stop(self) -> None:
        """Stop replication workers."""
        self._running = False
        
        if self._worker_thread:
            self._worker_thread.join(timeout=5.0)
            self._worker_thread = None
        
        logger.info("Replication stopped")
    
    def _async_worker(self) -> None:
        """Worker thread for async replication."""
        while self._running:
            try:
                event = self._queue.get(timeout=1.0)
                self._process_event(event)
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Async replication error: {e}")
    
    def _process_event(self, event: ReplicationEvent) -> List[ReplicationResult]:
        """Process a replication event."""
        results = []
        
        event.status = ReplicationStatus.IN_PROGRESS
        
        for target_name in event.target_regions:
            target = self.config.get_region(target_name)
            if not target:
                logger.warning(f"Target region not found: {target_name}")
                continue
            
            if target.region.status != RegionStatus.ACTIVE:
                logger.warning(f"Target region not active: {target_name}")
                continue
            
            try:
                result = self.transport.send(target, event)
                results.append(result)
                
                if result.success:
                    self._replicated_count[target_name] = \
                        self._replicated_count.get(target_name, 0) + 1
                else:
                    self._failed_count[target_name] = \
                        self._failed_count.get(target_name, 0) + 1
                    
            except Exception as e:
                logger.error(f"Replication to {target_name} failed: {e}")
                event.last_error = str(e)
                results.append(ReplicationResult(
                    event_id=event.event_id,
                    target_region=target_name,
                    success=False,
                    latency_ms=0,
                    error=str(e),
                ))
        
        # Update event status
        if all(r.success for r in results):
            event.status = ReplicationStatus.COMPLETED
        elif any(r.success for r in results):
            event.status = ReplicationStatus.COMPLETED  # Partial success
        else:
            event.status = ReplicationStatus.FAILED
        
        return results
    
    def replicate(
        self,
        data: Dict[str, Any],
        source_region: str,
        target_regions: Optional[List[str]] = None,
    ) -> ReplicationEvent:
        """
        Replicate data to target regions.
        
        Args:
            data: Data to replicate
            source_region: Source region name
            target_regions: Target regions (default: configured targets)
            
        Returns:
            ReplicationEvent tracking the replication
        """
        # Get source region config
        source = self.config.get_region(source_region)
        if not source:
            raise ValueError(f"Source region not found: {source_region}")
        
        # Determine targets
        if target_regions is None:
            target_regions = source.replication_targets
        
        if not target_regions:
            logger.debug(f"No replication targets for {source_region}")
            return None
        
        # Create replication event
        event = ReplicationEvent(
            event_id=self._generate_event_id(),
            source_region=source_region,
            target_regions=target_regions,
            data=data,
        )
        
        self._pending_events[event.event_id] = event
        
        # Process based on mode
        if self.mode == ReplicationMode.SYNC:
            # Synchronous: wait for all replicas
            self._process_event(event)
        elif self.mode == ReplicationMode.SEMI_SYNC:
            # Semi-sync: wait for at least one replica
            results = self._process_event(event)
            if not any(r.success for r in results):
                raise RuntimeError("Semi-sync replication failed: no replicas acknowledged")
        else:
            # Async: queue for background processing
            self._queue.put(event)
        
        return event
    
    def get_event_status(self, event_id: str) -> Optional[ReplicationStatus]:
        """Get status of a replication event."""
        event = self._pending_events.get(event_id)
        return event.status if event else None
    
    def get_lag(
        self,
        source_region: str,
        target_region: str,
    ) -> Optional[ReplicationLag]:
        """Get replication lag between regions."""
        return self._lag.get((source_region, target_region))
    
    def update_lag(
        self,
        source_region: str,
        target_region: str,
        lag_ms: float,
        lag_events: int = 0,
    ) -> None:
        """Update replication lag measurement."""
        self._lag[(source_region, target_region)] = ReplicationLag(
            source_region=source_region,
            target_region=target_region,
            lag_ms=lag_ms,
            lag_events=lag_events,
        )
    
    def get_all_lags(self) -> List[ReplicationLag]:
        """Get all replication lag measurements."""
        return list(self._lag.values())
    
    def get_stats(self) -> Dict[str, Any]:
        """Get replication statistics."""
        return {
            "mode": self.mode.value,
            "running": self._running,
            "pending_events": len(self._pending_events),
            "queue_size": self._queue.qsize(),
            "replicated_count": dict(self._replicated_count),
            "failed_count": dict(self._failed_count),
            "lags": [lag.to_dict() for lag in self._lag.values()],
        }
    
    def resolve_conflict(
        self,
        local_data: Dict[str, Any],
        remote_data: Dict[str, Any],
        local_timestamp: datetime,
        remote_timestamp: datetime,
    ) -> Dict[str, Any]:
        """
        Resolve a replication conflict.
        
        Args:
            local_data: Local version of data
            remote_data: Remote version of data
            local_timestamp: Local write timestamp
            remote_timestamp: Remote write timestamp
            
        Returns:
            Resolved data
        """
        if self.conflict_resolution == ConflictResolution.LAST_WRITE_WINS:
            if remote_timestamp > local_timestamp:
                return remote_data
            return local_data
        
        elif self.conflict_resolution == ConflictResolution.FIRST_WRITE_WINS:
            if local_timestamp < remote_timestamp:
                return local_data
            return remote_data
        
        elif self.conflict_resolution == ConflictResolution.SOURCE_WINS:
            return local_data
        
        elif self.conflict_resolution == ConflictResolution.MERGE:
            # Simple merge: combine fields, prefer remote for conflicts
            merged = {**local_data}
            for key, value in remote_data.items():
                if key not in merged or remote_timestamp > local_timestamp:
                    merged[key] = value
            return merged
        
        else:
            raise ValueError(f"Cannot auto-resolve with strategy: {self.conflict_resolution}")


class ReplicationMonitor:
    """
    Monitor replication health across regions.
    
    Tracks lag, throughput, and alerts on issues.
    """
    
    def __init__(
        self,
        manager: ReplicationManager,
        lag_threshold_ms: float = 5000.0,
        check_interval_seconds: float = 10.0,
    ):
        """
        Initialize replication monitor.
        
        Args:
            manager: Replication manager to monitor
            lag_threshold_ms: Threshold for lag alerts (default 5s)
            check_interval_seconds: Check interval
        """
        self.manager = manager
        self.lag_threshold_ms = lag_threshold_ms
        self.check_interval_seconds = check_interval_seconds
        
        self._running = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._alerts: List[Dict[str, Any]] = []
    
    def start(self) -> None:
        """Start monitoring."""
        if self._running:
            return
        
        self._running = True
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
        )
        self._monitor_thread.start()
        logger.info("Replication monitor started")
    
    def stop(self) -> None:
        """Stop monitoring."""
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)
            self._monitor_thread = None
    
    def _monitor_loop(self) -> None:
        """Monitoring loop."""
        while self._running:
            try:
                self._check_health()
            except Exception as e:
                logger.error(f"Monitor error: {e}")
            
            time.sleep(self.check_interval_seconds)
    
    def _check_health(self) -> None:
        """Check replication health."""
        for lag in self.manager.get_all_lags():
            if lag.lag_ms > self.lag_threshold_ms:
                alert = {
                    "type": "high_lag",
                    "source": lag.source_region,
                    "target": lag.target_region,
                    "lag_ms": lag.lag_ms,
                    "threshold_ms": self.lag_threshold_ms,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                self._alerts.append(alert)
                logger.warning(
                    f"High replication lag: {lag.source_region} → {lag.target_region}: "
                    f"{lag.lag_ms:.0f}ms (threshold: {self.lag_threshold_ms:.0f}ms)"
                )
    
    def get_alerts(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent alerts."""
        return self._alerts[-limit:]
    
    def clear_alerts(self) -> None:
        """Clear all alerts."""
        self._alerts.clear()
