"""
Event Processor - Quality-Aware Event Processing
=================================================
Processes CDC events with data quality validation before routing.

PROCESSING PIPELINE
-------------------
    Kafka Message
         │
         ▼
    ┌─────────────────┐
    │ Deserialize     │──── Error ────▶ DLQ (deser_error)
    └─────────────────┘
         │
         ▼
    ┌─────────────────┐
    │ Quality Check   │──── Fail ─────▶ DLQ (quality_failure)
    └─────────────────┘
         │
         ▼
    ┌─────────────────┐
    │ Event Router    │──── Error ────▶ DLQ (sink_failure)
    └─────────────────┘
         │
         ▼
      Success ✓

WHY THIS DESIGN?
----------------
1. **Fail-fast**: Bad events are rejected early (quality check)
2. **No data loss**: All failures go to DLQ for later review
3. **Metrics**: Every step is measured for observability
4. **Pluggable**: Swap quality rules without changing consumer
"""

import json
import logging
from dataclasses import dataclass
from typing import Optional

from . import config
from consumer.dlq_handler import DLQHandler, FailureReason, get_dlq_handler
from consumer.event_router import EventRouter
from quality import QualityChecker, create_cdc_event_checker
from schemas.cdc_event import CDCEvent
from metrics import (
    DATA_QUALITY_CHECKS_TOTAL,
    DATA_QUALITY_FAILURES_TOTAL,
    DATA_QUALITY_PASS_RATE,
)

logger = logging.getLogger(__name__)


@dataclass
class ProcessingResult:
    """Result of processing a single event."""
    success: bool
    event_id: Optional[str] = None
    stage: str = ""  # "deserialization", "quality", "routing"
    error_message: str = ""


class QualityAwareProcessor:
    """
    Processes CDC events with quality validation.
    
    SIMPLE EXPLANATION:
    Think of this as a quality control inspector on an assembly line:
    1. Check if the product (event) is well-formed
    2. Inspect for defects (quality rules)
    3. Route good products to the next station
    4. Send defective products to the repair pile (DLQ)
    
    TECHNICAL DETAILS:
    - Integrates QualityChecker with EventRouter
    - Handles deserialization errors
    - Updates Prometheus metrics at each stage
    - Configurable quality checkers per table
    """
    
    def __init__(
        self,
        router: Optional[EventRouter] = None,
        dlq_handler: Optional[DLQHandler] = None,
        enable_quality_checks: bool = True,
    ):
        """
        Initialize the processor.
        
        Args:
            router: EventRouter for routing valid events (creates default if None)
            dlq_handler: DLQ handler for failed events (uses singleton if None)
            enable_quality_checks: Whether to run quality checks (can disable for testing)
        """
        self._router = router or EventRouter()
        self._dlq = dlq_handler or get_dlq_handler("event-processor")
        self._enable_quality_checks = enable_quality_checks
        
        # Quality checkers by table (can customize per table)
        self._quality_checkers: dict[str, QualityChecker] = {}
        
        # Default CDC event checker for all tables
        self._default_checker = create_cdc_event_checker()
        
        # Statistics
        self._processed = 0
        self._quality_passed = 0
        self._quality_failed = 0
        self._deser_failed = 0
        self._routing_failed = 0
        
        logger.info(
            f"QualityAwareProcessor initialized | "
            f"quality_checks={enable_quality_checks}"
        )
    
    def register_checker(self, table: str, checker: QualityChecker) -> None:
        """
        Register a custom quality checker for a specific table.
        
        Args:
            table: Table name (e.g., "customers", "orders")
            checker: QualityChecker instance with table-specific rules
        """
        self._quality_checkers[table] = checker
        logger.info(f"Registered quality checker for table: {table}")
    
    def process_message(
        self,
        raw_value: bytes,
        topic: str = "",
        partition: int = 0,
        offset: int = 0,
    ) -> ProcessingResult:
        """
        Process a single Kafka message through the full pipeline.
        
        Args:
            raw_value: Raw message bytes from Kafka
            topic: Source Kafka topic
            partition: Source partition
            offset: Source offset
        
        Returns:
            ProcessingResult with success/failure info
        """
        self._processed += 1
        
        # Stage 1: Deserialize
        try:
            event_dict = json.loads(raw_value.decode("utf-8"))
        except Exception as e:
            self._deser_failed += 1
            self._dlq.send_deserialization_error(
                raw_message=raw_value,
                error=e,
                topic=topic,
                partition=partition,
                offset=offset,
            )
            return ProcessingResult(
                success=False,
                stage="deserialization",
                error_message=str(e),
            )
        
        event_id = event_dict.get("event_id", "unknown")
        
        # Stage 2: Quality Check
        if self._enable_quality_checks:
            quality_result = self._run_quality_check(
                event_dict, topic, partition, offset
            )
            if not quality_result.success:
                return quality_result
        
        # Stage 3: Parse to CDCEvent and Route
        try:
            event = CDCEvent.model_validate(event_dict)
        except Exception as e:
            self._deser_failed += 1
            self._dlq.send_generic_failure(
                event=event_dict,
                reason=FailureReason.SCHEMA_MISMATCH,
                error_message=f"Failed to parse CDCEvent: {e}",
                topic=topic,
                partition=partition,
                offset=offset,
            )
            return ProcessingResult(
                success=False,
                event_id=event_id,
                stage="deserialization",
                error_message=str(e),
            )
        
        # Stage 4: Route to sinks
        try:
            success = self._router.route(event)
            if success:
                return ProcessingResult(
                    success=True,
                    event_id=event_id,
                    stage="complete",
                )
            else:
                # Router already sent to DLQ
                self._routing_failed += 1
                return ProcessingResult(
                    success=False,
                    event_id=event_id,
                    stage="routing",
                    error_message="Sink write failed",
                )
        except Exception as e:
            self._routing_failed += 1
            self._dlq.send_sink_failure(
                event=event_dict,
                sink_name="router",
                error=e,
                topic=topic,
                partition=partition,
                offset=offset,
            )
            return ProcessingResult(
                success=False,
                event_id=event_id,
                stage="routing",
                error_message=str(e),
            )
    
    def _run_quality_check(
        self,
        event_dict: dict,
        topic: str,
        partition: int,
        offset: int,
    ) -> ProcessingResult:
        """
        Run quality checks on an event.
        
        Returns ProcessingResult - success=False means event failed checks
        and was sent to DLQ.
        """
        event_id = event_dict.get("event_id", "unknown")
        
        # Get table from source info
        source = event_dict.get("source", {})
        table = source.get("table", "unknown")
        
        # Get appropriate checker
        checker = self._quality_checkers.get(table, self._default_checker)
        
        # Run quality check
        report = checker.check(event_dict, event_id=event_id)
        
        # Update metrics
        for rule in checker.rules:
            DATA_QUALITY_CHECKS_TOTAL.labels(
                checker=checker.name,
                rule=rule.name,
            ).inc()
        
        if report.passed:
            self._quality_passed += 1
            self._update_pass_rate(checker.name)
            return ProcessingResult(success=True, event_id=event_id)
        else:
            # Quality check failed
            self._quality_failed += 1
            self._update_pass_rate(checker.name)
            
            # Record failure metrics
            for failure in report.failures:
                DATA_QUALITY_FAILURES_TOTAL.labels(
                    checker=checker.name,
                    rule=failure.rule_name,
                    severity=failure.severity.value,
                ).inc()
            
            # Send to DLQ
            self._dlq.send_quality_failure(
                event=event_dict,
                quality_report=report.to_dict(),
                topic=topic,
                partition=partition,
                offset=offset,
            )
            
            return ProcessingResult(
                success=False,
                event_id=event_id,
                stage="quality",
                error_message=f"Quality check failed: {len(report.failures)} violations",
            )
    
    def _update_pass_rate(self, checker_name: str) -> None:
        """Update the pass rate gauge."""
        total = self._quality_passed + self._quality_failed
        if total > 0:
            rate = (self._quality_passed / total) * 100
            DATA_QUALITY_PASS_RATE.labels(checker=checker_name).set(rate)
    
    def flush(self) -> None:
        """Flush all buffers."""
        self._router.flush_all()
        self._dlq.flush()
    
    def close(self) -> None:
        """Shutdown the processor."""
        self.flush()
        self._router.close()
        self._dlq.close()
        logger.info(
            f"QualityAwareProcessor closed | "
            f"processed={self._processed} | "
            f"quality_passed={self._quality_passed} | "
            f"quality_failed={self._quality_failed}"
        )
    
    def get_stats(self) -> dict:
        """Get processor statistics."""
        total = self._quality_passed + self._quality_failed
        pass_rate = (self._quality_passed / total * 100) if total > 0 else 100.0
        
        return {
            "processed": self._processed,
            "quality_passed": self._quality_passed,
            "quality_failed": self._quality_failed,
            "quality_pass_rate": f"{pass_rate:.1f}%",
            "deserialization_failed": self._deser_failed,
            "routing_failed": self._routing_failed,
            "router_stats": self._router.get_stats(),
            "dlq_stats": self._dlq.get_stats(),
        }


# ============================================================
# Convenience Functions
# ============================================================

def create_processor_with_table_checkers() -> QualityAwareProcessor:
    """
    Create a processor with pre-configured table-specific checkers.
    
    Returns:
        QualityAwareProcessor with customer and order checkers registered
    """
    from quality import create_customer_checker, create_order_checker
    
    processor = QualityAwareProcessor()
    
    # Register table-specific checkers
    processor.register_checker("customers", create_customer_checker())
    processor.register_checker("orders", create_order_checker())
    
    return processor
