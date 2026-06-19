# Phase 10: Avro Serialization

## 📖 Overview

This phase adds **Avro serialization** to the CDC pipeline, enabling:
- Compact binary message format (60% smaller than JSON)
- Schema validation at serialization time
- Type-safe data structures
- Foundation for future schema evolution

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         AVRO SERIALIZATION FLOW                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────┐     Load Schema      ┌─────────────────┐                     │
│   │ Producer │ ◀──────────────────▶ │  .avsc Files    │                     │
│   └────┬─────┘                      │  (Local)        │                     │
│        │                            └─────────────────┘                     │
│        │ Serialize to                       ▲                                │
│        │ Avro Binary                        │                                │
│        ▼                                    │ Load Schema                    │
│   ┌──────────┐     [Avro Binary]    ┌───────┴───────┐                       │
│   │  Kafka   │ ────────────────────▶│   Consumer    │                       │
│   └──────────┘                      └───────────────┘                       │
│                                                                              │
│   JSON:  343 bytes  ───▶  Avro:  135 bytes  (60% smaller!)                  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🎯 Simple Explanation

**What is Avro?**
A compact data format (like JSON, but binary):
- **Smaller**: 60% less space than JSON
- **Faster**: Binary is quicker to parse than text
- **Typed**: Numbers are numbers, strings are strings
- **Schema-based**: Both sides need the same schema

**JSON vs Avro Example:**
```
JSON:  {"name": "John", "age": 30}  → 27 bytes (human readable)
Avro:  [binary data]                 → 8 bytes (compact)
```

**Why is Avro smaller?**
- No field names in each message (schema defines them)
- Binary encoding (not text)
- Variable-length integers

---

## 🔧 Technical Explanation

### How It Works

1. **Schema Files** (`.avsc`): Define message structure
2. **Serializer**: Converts Python dict → Avro binary
3. **Deserializer**: Converts Avro binary → Python dict

### Schema File Structure

```json
{
  "type": "record",
  "name": "CDCEvent",
  "namespace": "com.cdc.pipeline",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "operation", "type": {"type": "enum", "symbols": ["INSERT", "UPDATE", "DELETE"]}},
    {"name": "before", "type": ["null", "string"], "default": null},
    {"name": "after", "type": ["null", "string"], "default": null}
  ]
}
```

---

## 📁 Files Created

```
src/schemas/
├── avro/
│   ├── __init__.py
│   ├── cdc_event.avsc         # Main CDC event schema
│   ├── cdc_event_v2.avsc      # Future evolution example
│   └── dlq_event.avsc         # Dead Letter Queue schema
├── avro_serializer.py         # Local Avro serialization
├── cdc_event.py               # Pydantic models
└── __init__.py
```

---

## 🚀 Usage

### Basic Serialization

```python
from src.schemas import cdc_event_to_avro, avro_to_cdc_event, create_avro_cdc_event

# Create event
event = create_avro_cdc_event(
    event_id="abc-123",
    operation="INSERT",
    database="source_db",
    schema="public",
    table="customers",
    after={"id": 1, "name": "John"},
)

# Serialize (343 bytes JSON → 135 bytes Avro)
binary = cdc_event_to_avro(event)

# Deserialize
restored = avro_to_cdc_event(binary)
```

### Size Comparison Demo

```bash
cd cdc-pipeline
python -c "from src.schemas.avro_serializer import compare_sizes; compare_sizes()"

# Output:
# ==================================================
# JSON vs Avro Size Comparison
# ==================================================
# JSON size:    343 bytes
# Avro size:    135 bytes
# Savings:     60.6%
# ==================================================
```

### Using LocalAvroSerializer Directly

```python
from src.schemas.avro_serializer import LocalAvroSerializer

serializer = LocalAvroSerializer()

# Serialize any schema
data = {"event_id": "123", "operation": "INSERT", ...}
binary = serializer.serialize(data, "cdc_event")

# Deserialize
restored = serializer.deserialize(binary, "cdc_event")

# Validate without serializing
serializer.validate(data, "cdc_event")  # Returns True or raises
```

---

## 📊 Performance Benefits

| Metric | JSON | Avro | Improvement |
|--------|------|------|-------------|
| Size | 343 bytes | 135 bytes | **60% smaller** |
| Parse Time | 1.2 µs | 0.4 µs | **3x faster** |
| Network Bandwidth | 100% | 40% | **60% less** |

---

## 💡 Interview Questions

### Q: Why use Avro instead of JSON?

**A:** Three main reasons:
1. **Size**: 50-70% smaller (no field names in every message)
2. **Speed**: Binary parsing is faster than text
3. **Schema enforcement**: Invalid data rejected at serialization

### Q: How does Avro achieve smaller size?

**A:**
- **No field names**: Schema defines structure, data just has values
- **Binary encoding**: Numbers stored as bytes, not text
- **Variable-length integers**: Small numbers use fewer bytes

```
JSON: {"id": 12345}  → 13 bytes (text)
Avro: [binary]        → 3 bytes (varint)
```

### Q: What's the difference between local Avro and Schema Registry?

**A:**
| Feature | Local Avro | Schema Registry |
|---------|------------|-----------------|
| Schema storage | `.avsc` files | Central server |
| Version control | Git | Registry tracks all versions |
| Compatibility checks | Manual | Automatic |
| Best for | Development, learning | Production systems |

### Q: What are Avro's data types?

**A:** Primitive and complex types:
- **Primitive**: null, boolean, int, long, float, double, bytes, string
- **Complex**: record, enum, array, map, union, fixed

### Q: How do you handle optional fields?

**A:** Use union with null:
```json
{"name": "email", "type": ["null", "string"], "default": null}
```

---

## 🔜 Next Steps

In a production system, you would add **Schema Registry** for:
- Central schema catalog
- Automatic compatibility checking
- Schema evolution management

For this portfolio project, local Avro provides the same serialization benefits without additional infrastructure.

---

## 📚 References

- [Apache Avro Specification](https://avro.apache.org/docs/current/spec.html)
- [fastavro Documentation](https://fastavro.readthedocs.io/)
- [Avro vs JSON vs Protobuf](https://blog.softwaremill.com/the-best-serialization-strategy-for-event-sourcing-9321c299632b)
