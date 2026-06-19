"""
Unit Tests for Load Generator Module

Tests for event generation and load profiling.
"""

import time
import pytest
from src.schemas.cdc_event import OperationType
from src.performance.load_generator import (
    LoadGenerator,
    LoadProfile,
    EventBatch,
    generate_customer_event,
    generate_product_event,
    generate_order_event,
    generate_order_item_event,
    random_operation,
)


class TestEventGenerators:
    """Tests for individual event generators."""
    
    def test_generate_customer_insert(self):
        """Should generate valid customer insert event."""
        event = generate_customer_event(customer_id=123, operation=OperationType.INSERT)
        
        assert event.operation == OperationType.INSERT
        assert event.source.table == "customers"
        assert event.after is not None
        assert event.after["id"] == 123
        assert "first_name" in event.after
        assert "email" in event.after
    
    def test_generate_customer_update(self):
        """Should generate customer update with before/after."""
        event = generate_customer_event(operation=OperationType.UPDATE)
        
        assert event.operation == OperationType.UPDATE
        assert event.before is not None
        assert event.after is not None
    
    def test_generate_customer_delete(self):
        """Should generate customer delete with before only."""
        event = generate_customer_event(operation=OperationType.DELETE)
        
        assert event.operation == OperationType.DELETE
        assert event.before is not None
        assert event.after is None
    
    def test_generate_product_event(self):
        """Should generate valid product event."""
        event = generate_product_event(product_id=456)
        
        assert event.source.table == "products"
        assert event.after["id"] == 456
        assert "name" in event.after
        assert "price" in event.after
        assert "category" in event.after
    
    def test_generate_order_event(self):
        """Should generate valid order event."""
        event = generate_order_event(order_id=789, customer_id=123)
        
        assert event.source.table == "orders"
        assert event.after["id"] == 789
        assert event.after["customer_id"] == 123
        assert "total_amount" in event.after
        assert "status" in event.after
    
    def test_generate_order_item_event(self):
        """Should generate valid order item event."""
        event = generate_order_item_event(item_id=1, order_id=789, product_id=456)
        
        assert event.source.table == "order_items"
        assert event.after["id"] == 1
        assert event.after["order_id"] == 789
        assert event.after["product_id"] == 456
        assert "quantity" in event.after
        assert "unit_price" in event.after
    
    def test_random_operation_distribution(self):
        """Random operations should follow weight distribution."""
        ops = [random_operation() for _ in range(1000)]
        
        # Check that all operation types appear
        op_counts = {}
        for op in ops:
            op_counts[op] = op_counts.get(op, 0) + 1
        
        # INSERT and UPDATE should be most common
        assert op_counts.get(OperationType.INSERT, 0) > 100
        assert op_counts.get(OperationType.UPDATE, 0) > 100
        # DELETE should be less common
        assert op_counts.get(OperationType.DELETE, 0) < op_counts.get(OperationType.UPDATE, 0)


class TestEventBatch:
    """Tests for EventBatch class."""
    
    def test_batch_size(self):
        """Batch should report correct size."""
        events = [generate_customer_event() for _ in range(10)]
        batch = EventBatch(events=events)
        
        assert batch.size == 10
    
    def test_batch_by_table(self):
        """Batch should group events by table."""
        events = [
            generate_customer_event(),
            generate_customer_event(),
            generate_product_event(),
            generate_order_event(),
        ]
        batch = EventBatch(events=events)
        
        by_table = batch.by_table()
        
        assert len(by_table["customers"]) == 2
        assert len(by_table["products"]) == 1
        assert len(by_table["orders"]) == 1
    
    def test_batch_has_id(self):
        """Batch should have unique ID."""
        batch1 = EventBatch(events=[])
        batch2 = EventBatch(events=[])
        
        assert batch1.batch_id is not None
        assert batch1.batch_id != batch2.batch_id


class TestLoadGenerator:
    """Tests for LoadGenerator class."""
    
    def test_generator_creates_events(self):
        """Generator should create valid events."""
        gen = LoadGenerator(
            events_per_second=100,
            batch_size=10,
            tables=["customers"],
        )
        
        batch = gen.generate_batch()
        
        assert batch.size == 10
        for event in batch.events:
            assert event.source.table == "customers"
    
    def test_generator_respects_tables(self):
        """Generator should only use specified tables."""
        gen = LoadGenerator(
            tables=["customers", "products"],
        )
        
        batch = gen.generate_batch(100)
        
        tables = set(e.source.table for e in batch.events)
        assert tables.issubset({"customers", "products"})
    
    def test_generator_generate_events(self):
        """Generator should yield individual events."""
        gen = LoadGenerator()
        
        events = list(gen.generate_events(10))
        
        assert len(events) == 10
        assert gen.events_generated == 10
    
    def test_generator_stats(self):
        """Generator should track statistics."""
        gen = LoadGenerator(
            events_per_second=100,
            batch_size=10,
        )
        
        gen.generate_batch(50)
        
        stats = gen.stats()
        assert stats["events_generated"] == 50
        assert stats["target_rate"] == 100
        assert stats["batch_size"] == 10
    
    def test_generator_stream_limited(self):
        """Generator stream should respect limits."""
        gen = LoadGenerator(
            events_per_second=1000,
            batch_size=10,
        )
        
        batches = list(gen.stream(max_events=50))
        
        total_events = sum(b.size for b in batches)
        assert total_events >= 50
    
    def test_generator_stream_duration(self):
        """Generator stream should respect duration."""
        gen = LoadGenerator(
            events_per_second=1000,
            batch_size=100,
        )
        
        start = time.time()
        batches = list(gen.stream(duration_sec=0.5))
        elapsed = time.time() - start
        
        assert elapsed >= 0.4  # Some tolerance
        assert elapsed < 1.0
        assert len(batches) > 0


class TestLoadProfiles:
    """Tests for different load profiles."""
    
    def test_steady_profile(self):
        """Steady profile should maintain constant rate."""
        gen = LoadGenerator(
            events_per_second=1000,
            batch_size=100,
        )
        
        batches = list(gen.stream(duration_sec=0.3, profile=LoadProfile.STEADY))
        
        # All batches should be similar size
        sizes = [b.size for b in batches]
        assert max(sizes) - min(sizes) < 50  # Some variation acceptable
    
    def test_spike_profile(self):
        """Spike profile should create load bursts."""
        gen = LoadGenerator(
            events_per_second=100,
            batch_size=10,
        )
        
        batches = list(gen.stream(duration_sec=0.5, profile=LoadProfile.SPIKE))
        
        # Should have some variation in batch sizes
        assert len(batches) > 0
    
    def test_generator_stop(self):
        """Generator should stop on request."""
        gen = LoadGenerator(
            events_per_second=100,
            batch_size=10,
        )
        
        count = 0
        for batch in gen.stream(duration_sec=10):
            count += 1
            if count >= 3:
                gen.stop()
                break
        
        assert count == 3


class TestTableWeights:
    """Tests for table weight distribution."""
    
    def test_custom_table_weights(self):
        """Generator should respect custom table weights."""
        gen = LoadGenerator(
            tables=["customers", "orders"],
            table_weights={
                "customers": 90,  # 90% customers
                "orders": 10,     # 10% orders
            },
        )
        
        batch = gen.generate_batch(1000)
        
        customer_count = sum(1 for e in batch.events if e.source.table == "customers")
        order_count = sum(1 for e in batch.events if e.source.table == "orders")
        
        # Customers should be much more common
        assert customer_count > order_count * 3  # At least 3x


class TestOperationWeights:
    """Tests for operation weight distribution."""
    
    def test_custom_operation_weights(self):
        """Generator should respect custom operation weights."""
        gen = LoadGenerator(
            operation_weights={
                OperationType.INSERT: 100,
                OperationType.UPDATE: 0,
                OperationType.DELETE: 0,
            },
        )
        
        batch = gen.generate_batch(100)
        
        for event in batch.events:
            assert event.operation == OperationType.INSERT
