# CDC Event Schemas
from src.schemas.cdc_event import (
    CDCEvent,
    DLQEvent,
    OperationType,
    SourceInfo,
)

# Local Avro Serialization (no Schema Registry needed)
from src.schemas.avro_serializer import (
    LocalAvroSerializer,
    get_serializer,
    cdc_event_to_avro,
    avro_to_cdc_event,
    create_avro_cdc_event,
)

__all__ = [
    # Pydantic models
    "CDCEvent",
    "DLQEvent",
    "OperationType",
    "SourceInfo",
    # Avro serialization
    "LocalAvroSerializer",
    "get_serializer",
    "cdc_event_to_avro",
    "avro_to_cdc_event",
    "create_avro_cdc_event",
]
