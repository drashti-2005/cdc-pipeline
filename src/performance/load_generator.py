"""
Load Generator for CDC Pipeline Performance Testing

Generates realistic CDC events at configurable rates for:
- Throughput testing
- Stress testing
- Latency measurement under load
- Capacity planning
"""

import time
import random
import logging
import threading
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Generator, Callable
from enum import Enum
from datetime import datetime, timezone
import uuid

from schemas.cdc_event import CDCEvent, OperationType, SourceInfo

logger = logging.getLogger(__name__)


class LoadProfile(Enum):
    """Pre-defined load profiles for testing."""
    
    STEADY = "steady"           # Constant rate
    RAMP_UP = "ramp_up"         # Gradually increasing
    RAMP_DOWN = "ramp_down"     # Gradually decreasing
    SPIKE = "spike"             # Sudden bursts
    SINE_WAVE = "sine_wave"     # Oscillating load
    STEP = "step"               # Step function increases


@dataclass
class EventBatch:
    """A batch of generated events."""
    
    events: List[CDCEvent]
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    batch_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    
    @property
    def size(self) -> int:
        """Number of events in batch."""
        return len(self.events)
    
    def by_table(self) -> Dict[str, List[CDCEvent]]:
        """Group events by table name."""
        result: Dict[str, List[CDCEvent]] = {}
        for event in self.events:
            table = event.source.table
            if table not in result:
                result[table] = []
            result[table].append(event)
        return result


def generate_customer_event(
    customer_id: Optional[int] = None,
    operation: OperationType = OperationType.INSERT,
) -> CDCEvent:
    """
    Generate a realistic customer CDC event.
    
    Args:
        customer_id: Specific customer ID, or random if None
        operation: CDC operation type
        
    Returns:
        CDCEvent for a customer record
    """
    cid = customer_id or random.randint(1, 1000000)
    
    first_names = ["John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]
    
    first = random.choice(first_names)
    last = random.choice(last_names)
    
    after = None
    before = None
    
    if operation in (OperationType.INSERT, OperationType.UPDATE):
        after = {
            "id": cid,
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}.{last.lower()}{cid}@example.com",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    
    if operation in (OperationType.UPDATE, OperationType.DELETE):
        before = {
            "id": cid,
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}.{last.lower()}{cid}@old.com",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    
    return CDCEvent(
        operation=operation,
        before=before,
        after=after,
        source=SourceInfo(
            database="cdc_source",
            schema_name="public",
            table="customers",
        ),
        timestamp_ms=int(time.time() * 1000),
    )


def generate_product_event(
    product_id: Optional[int] = None,
    operation: OperationType = OperationType.INSERT,
) -> CDCEvent:
    """
    Generate a realistic product CDC event.
    
    Args:
        product_id: Specific product ID, or random if None
        operation: CDC operation type
        
    Returns:
        CDCEvent for a product record
    """
    pid = product_id or random.randint(1, 100000)
    
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports", "Toys"]
    adjectives = ["Premium", "Basic", "Pro", "Ultra", "Mini", "Max"]
    nouns = ["Widget", "Gadget", "Device", "Tool", "Kit", "Set"]
    
    name = f"{random.choice(adjectives)} {random.choice(nouns)} {pid}"
    price = round(random.uniform(9.99, 999.99), 2)
    
    after = None
    before = None
    
    if operation in (OperationType.INSERT, OperationType.UPDATE):
        after = {
            "id": pid,
            "name": name,
            "category": random.choice(categories),
            "price": price,
            "stock": random.randint(0, 1000),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    
    if operation in (OperationType.UPDATE, OperationType.DELETE):
        before = {
            "id": pid,
            "name": name,
            "category": random.choice(categories),
            "price": price * 0.9,  # Old price
            "stock": random.randint(0, 1000),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    
    return CDCEvent(
        operation=operation,
        before=before,
        after=after,
        source=SourceInfo(
            database="cdc_source",
            schema_name="public",
            table="products",
        ),
        timestamp_ms=int(time.time() * 1000),
    )


def generate_order_event(
    order_id: Optional[int] = None,
    customer_id: Optional[int] = None,
    operation: OperationType = OperationType.INSERT,
) -> CDCEvent:
    """
    Generate a realistic order CDC event.
    
    Args:
        order_id: Specific order ID, or random if None
        customer_id: Specific customer ID, or random if None
        operation: CDC operation type
        
    Returns:
        CDCEvent for an order record
    """
    oid = order_id or random.randint(1, 10000000)
    cid = customer_id or random.randint(1, 1000000)
    
    statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]
    
    total = round(random.uniform(10.00, 5000.00), 2)
    
    after = None
    before = None
    
    if operation in (OperationType.INSERT, OperationType.UPDATE):
        after = {
            "id": oid,
            "customer_id": cid,
            "total_amount": total,
            "status": random.choice(statuses),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    
    if operation in (OperationType.UPDATE, OperationType.DELETE):
        before = {
            "id": oid,
            "customer_id": cid,
            "total_amount": total,
            "status": "pending",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    
    return CDCEvent(
        operation=operation,
        before=before,
        after=after,
        source=SourceInfo(
            database="cdc_source",
            schema_name="public",
            table="orders",
        ),
        timestamp_ms=int(time.time() * 1000),
    )


def generate_order_item_event(
    item_id: Optional[int] = None,
    order_id: Optional[int] = None,
    product_id: Optional[int] = None,
    operation: OperationType = OperationType.INSERT,
) -> CDCEvent:
    """Generate a realistic order_item CDC event."""
    iid = item_id or random.randint(1, 50000000)
    oid = order_id or random.randint(1, 10000000)
    pid = product_id or random.randint(1, 100000)
    
    quantity = random.randint(1, 10)
    unit_price = round(random.uniform(9.99, 999.99), 2)
    
    after = None
    before = None
    
    if operation in (OperationType.INSERT, OperationType.UPDATE):
        after = {
            "id": iid,
            "order_id": oid,
            "product_id": pid,
            "quantity": quantity,
            "unit_price": unit_price,
        }
    
    if operation in (OperationType.UPDATE, OperationType.DELETE):
        before = {
            "id": iid,
            "order_id": oid,
            "product_id": pid,
            "quantity": quantity,
            "unit_price": unit_price,
        }
    
    return CDCEvent(
        operation=operation,
        before=before,
        after=after,
        source=SourceInfo(
            database="cdc_source",
            schema_name="public",
            table="order_items",
        ),
        timestamp_ms=int(time.time() * 1000),
    )


# Event generators by table
EVENT_GENERATORS: Dict[str, Callable[..., CDCEvent]] = {
    "customers": generate_customer_event,
    "products": generate_product_event,
    "orders": generate_order_event,
    "order_items": generate_order_item_event,
}

# Realistic operation distribution
OPERATION_WEIGHTS = {
    OperationType.INSERT: 45,
    OperationType.UPDATE: 45,
    OperationType.DELETE: 10,
}


def random_operation() -> OperationType:
    """Select a random operation based on realistic weights."""
    ops = list(OPERATION_WEIGHTS.keys())
    weights = list(OPERATION_WEIGHTS.values())
    return random.choices(ops, weights=weights)[0]


class LoadGenerator:
    """
    Generates CDC events at configurable rates and patterns.
    
    Usage:
        generator = LoadGenerator(
            events_per_second=1000,
            tables=["customers", "orders"],
        )
        
        # Generate a batch
        batch = generator.generate_batch(100)
        
        # Generate continuously
        for batch in generator.stream(duration_sec=60):
            process(batch)
    """
    
    def __init__(
        self,
        events_per_second: int = 100,
        batch_size: int = 100,
        tables: Optional[List[str]] = None,
        table_weights: Optional[Dict[str, int]] = None,
        operation_weights: Optional[Dict[OperationType, int]] = None,
    ):
        """
        Initialize load generator.
        
        Args:
            events_per_second: Target event generation rate
            batch_size: Events per batch
            tables: Tables to generate events for
            table_weights: Relative frequency of each table
            operation_weights: Relative frequency of each operation
        """
        self.events_per_second = events_per_second
        self.batch_size = batch_size
        self.tables = tables or ["customers", "products", "orders", "order_items"]
        
        # Default weights
        self.table_weights = table_weights or {
            "customers": 20,
            "products": 15,
            "orders": 35,
            "order_items": 30,
        }
        self.operation_weights = operation_weights or OPERATION_WEIGHTS
        
        self._running = False
        self._events_generated = 0
        self._start_time: Optional[float] = None
    
    def _random_table(self) -> str:
        """Select a random table based on weights."""
        tables = [t for t in self.tables if t in self.table_weights]
        weights = [self.table_weights.get(t, 1) for t in tables]
        return random.choices(tables, weights=weights)[0]
    
    def _random_operation(self) -> OperationType:
        """Select a random operation based on weights."""
        ops = list(self.operation_weights.keys())
        weights = list(self.operation_weights.values())
        return random.choices(ops, weights=weights)[0]
    
    def generate_event(self) -> CDCEvent:
        """Generate a single random CDC event."""
        table = self._random_table()
        operation = self._random_operation()
        
        generator = EVENT_GENERATORS.get(table, generate_customer_event)
        return generator(operation=operation)
    
    def generate_batch(self, size: Optional[int] = None) -> EventBatch:
        """
        Generate a batch of events.
        
        Args:
            size: Number of events (defaults to batch_size)
            
        Returns:
            EventBatch containing generated events
        """
        size = size or self.batch_size
        events = [self.generate_event() for _ in range(size)]
        self._events_generated += size
        return EventBatch(events=events)
    
    def generate_events(self, count: int) -> Generator[CDCEvent, None, None]:
        """
        Generate specified number of events.
        
        Args:
            count: Number of events to generate
            
        Yields:
            CDCEvent instances
        """
        for _ in range(count):
            yield self.generate_event()
            self._events_generated += 1
    
    def stream(
        self,
        duration_sec: Optional[float] = None,
        max_events: Optional[int] = None,
        profile: LoadProfile = LoadProfile.STEADY,
    ) -> Generator[EventBatch, None, None]:
        """
        Stream batches of events.
        
        Args:
            duration_sec: How long to generate (None = indefinite)
            max_events: Maximum events to generate
            profile: Load profile to use
            
        Yields:
            EventBatch at configured rate
        """
        self._running = True
        self._start_time = time.time()
        self._events_generated = 0
        
        batch_interval = self.batch_size / self.events_per_second
        
        logger.info(
            f"Starting load generation: {self.events_per_second} events/sec, "
            f"batch_size={self.batch_size}, profile={profile.value}"
        )
        
        try:
            while self._running:
                batch_start = time.time()
                
                # Check termination conditions
                elapsed = batch_start - self._start_time
                if duration_sec and elapsed >= duration_sec:
                    break
                if max_events and self._events_generated >= max_events:
                    break
                
                # Adjust rate based on profile
                rate_multiplier = self._get_rate_multiplier(elapsed, duration_sec, profile)
                adjusted_batch_size = int(self.batch_size * rate_multiplier)
                adjusted_batch_size = max(1, adjusted_batch_size)
                
                # Generate batch
                batch = self.generate_batch(adjusted_batch_size)
                yield batch
                
                # Throttle to maintain rate
                batch_elapsed = time.time() - batch_start
                sleep_time = batch_interval - batch_elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
        finally:
            self._running = False
            total_time = time.time() - self._start_time
            actual_rate = self._events_generated / total_time if total_time > 0 else 0
            logger.info(
                f"Load generation complete: {self._events_generated} events in "
                f"{total_time:.2f}s ({actual_rate:.2f} events/sec)"
            )
    
    def _get_rate_multiplier(
        self,
        elapsed: float,
        duration: Optional[float],
        profile: LoadProfile,
    ) -> float:
        """Calculate rate multiplier based on profile."""
        if profile == LoadProfile.STEADY:
            return 1.0
        
        if duration is None:
            duration = 60.0  # Default duration for profile calculation
        
        progress = min(elapsed / duration, 1.0)
        
        if profile == LoadProfile.RAMP_UP:
            return 0.1 + (0.9 * progress)
        
        elif profile == LoadProfile.RAMP_DOWN:
            return 1.0 - (0.9 * progress)
        
        elif profile == LoadProfile.SPIKE:
            # Spike every 10 seconds
            cycle = elapsed % 10
            if cycle < 2:
                return 3.0  # 3x spike
            return 1.0
        
        elif profile == LoadProfile.SINE_WAVE:
            import math
            return 0.5 + 0.5 * math.sin(elapsed * 0.5)
        
        elif profile == LoadProfile.STEP:
            # Increase by 25% every quarter
            step = int(progress * 4)
            return 1.0 + (step * 0.25)
        
        return 1.0
    
    def stop(self) -> None:
        """Stop streaming generation."""
        self._running = False
    
    @property
    def events_generated(self) -> int:
        """Total events generated."""
        return self._events_generated
    
    @property
    def actual_rate(self) -> float:
        """Actual events per second achieved."""
        if self._start_time is None:
            return 0
        elapsed = time.time() - self._start_time
        return self._events_generated / elapsed if elapsed > 0 else 0
    
    def stats(self) -> Dict[str, Any]:
        """Get generation statistics."""
        return {
            "events_generated": self._events_generated,
            "target_rate": self.events_per_second,
            "actual_rate": self.actual_rate,
            "batch_size": self.batch_size,
            "tables": self.tables,
        }


class ConcurrentLoadGenerator:
    """
    Generate load from multiple threads for higher throughput.
    
    Usage:
        generator = ConcurrentLoadGenerator(
            total_events_per_second=10000,
            num_workers=4,
        )
        
        def process_batch(batch):
            for event in batch.events:
                process(event)
        
        generator.start(process_batch, duration_sec=60)
    """
    
    def __init__(
        self,
        total_events_per_second: int = 1000,
        num_workers: int = 4,
        batch_size: int = 100,
        tables: Optional[List[str]] = None,
    ):
        self.total_events_per_second = total_events_per_second
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.tables = tables or ["customers", "products", "orders", "order_items"]
        
        # Each worker handles portion of load
        self.per_worker_rate = total_events_per_second // num_workers
        
        self._workers: List[threading.Thread] = []
        self._running = False
        self._total_events = 0
        self._lock = threading.Lock()
    
    def start(
        self,
        callback: Callable[[EventBatch], None],
        duration_sec: Optional[float] = None,
        max_events: Optional[int] = None,
    ) -> None:
        """
        Start concurrent load generation.
        
        Args:
            callback: Function to call with each batch
            duration_sec: Duration to run
            max_events: Maximum total events
        """
        self._running = True
        self._total_events = 0
        
        max_per_worker = max_events // self.num_workers if max_events else None
        
        def worker(worker_id: int):
            generator = LoadGenerator(
                events_per_second=self.per_worker_rate,
                batch_size=self.batch_size,
                tables=self.tables,
            )
            
            for batch in generator.stream(
                duration_sec=duration_sec,
                max_events=max_per_worker,
            ):
                if not self._running:
                    break
                callback(batch)
                with self._lock:
                    self._total_events += batch.size
        
        # Start workers
        self._workers = []
        for i in range(self.num_workers):
            thread = threading.Thread(target=worker, args=(i,), daemon=True)
            thread.start()
            self._workers.append(thread)
        
        # Wait for completion
        for thread in self._workers:
            thread.join()
        
        self._running = False
    
    def stop(self) -> None:
        """Stop all workers."""
        self._running = False
    
    @property
    def total_events(self) -> int:
        """Total events generated across all workers."""
        return self._total_events
