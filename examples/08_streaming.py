"""Streaming to Message Queues — Real-time Data Emission.

Real-world scenario: You need to generate a continuous stream of fake
events for load testing a data pipeline, populating a Kafka topic for
development, or stress-testing an HTTP ingestion endpoint. The streaming
module provides emitters for HTTP, Kafka, and RabbitMQ with built-in
rate limiting.

This example demonstrates:
- HTTP streaming with the zero-dependency HttpEmitter
- Token bucket rate limiting
- Kafka streaming (requires confluent-kafka)
- RabbitMQ streaming (requires pika)
- Custom emitter implementation
- Batch emission for throughput

Note: HTTP, Kafka, and RabbitMQ examples require running services.
      The custom emitter example runs standalone.
"""

import time

from dataforge import DataForge
from dataforge.streaming import (
    HttpEmitter,
    KafkaEmitter,
    RabbitMQEmitter,
    StreamEmitter,
    TokenBucketRateLimiter,
    stream_to_emitter,
)

forge = DataForge(seed=42)

# Create a schema for event data
event_schema = forge.schema(
    {
        "event_id": "misc.uuid4",
        "user_email": "internet.email",
        "action": "lorem.word",
        "timestamp": "dt.datetime",
        "ip_address": "internet.ipv4",
    }
)

# --- Example 1: Custom emitter (prints to console) -----------------------

print("=== Custom Console Emitter ===\n")


class ConsoleEmitter(StreamEmitter):
    """Simple emitter that prints rows to console (for demonstration)."""

    __slots__ = ("_count",)

    def __init__(self):
        self._count = 0

    def open(self):
        print("  [Connection opened]")

    def emit(self, row):
        self._count += 1
        if self._count <= 5:
            eid = row["event_id"][:8]
            print(f"  Event {self._count}: {eid}... user={row['user_email']}")
        elif self._count == 6:
            print("  ...")

    def emit_batch(self, rows):
        for row in rows:
            self.emit(row)

    def close(self):
        print(f"  [Connection closed - {self._count} events emitted]")


# Stream 20 events to console
emitted = stream_to_emitter(
    event_schema,
    ConsoleEmitter(),
    count=20,
    batch_size=10,
)
print(f"\nTotal emitted: {emitted}\n")

# --- Example 2: Rate limiting --------------------------------------------

print("=== Token Bucket Rate Limiter ===\n")

limiter = TokenBucketRateLimiter(rate=50.0, burst=10)

print("Rate limiter: 50 events/sec, burst=10")
print("Emitting 30 events with rate limiting...")

start = time.monotonic()
emitted = stream_to_emitter(
    event_schema,
    ConsoleEmitter(),
    count=30,
    batch_size=5,
    rate_limiter=limiter,
)
elapsed = time.monotonic() - start
print(
    f"Emitted {emitted} events in {elapsed:.2f}s ({emitted / elapsed:.1f} events/sec)\n"
)

# --- Example 3: HTTP emitter (setup) -------------------------------------

print("=== HTTP Emitter (setup example) ===\n")

http_emitter = HttpEmitter(
    url="https://api.example.com/events",
    headers={
        "Authorization": "Bearer your-api-key",
        "X-Source": "dataforge-load-test",
    },
    batch_mode=True,
    timeout=30.0,
)
print(f"Emitter: {http_emitter}")
print("Usage:")
print("  stream_to_emitter(schema, http_emitter, count=10000, batch_size=100)")
print()

# Uncomment to actually stream (requires a running HTTP server):
# stream_to_emitter(event_schema, http_emitter, count=10000, batch_size=100)

# --- Example 4: Kafka emitter (setup) ------------------------------------

print("=== Kafka Emitter (setup example) ===\n")

# Requires: pip install confluent-kafka (or: pip install dataforge-py[kafka])
kafka_emitter = KafkaEmitter(
    bootstrap_servers="localhost:9092",
    topic="user-events",
    config={"acks": "all", "retries": "3"},
)
print(f"Emitter: {kafka_emitter}")
print("Usage:")
print("  stream_to_emitter(schema, kafka_emitter, count=50000, batch_size=500)")
print()

# Uncomment if you have Kafka running:
# stream_to_emitter(event_schema, kafka_emitter, count=50000, batch_size=500)

# --- Example 5: RabbitMQ emitter (setup) ----------------------------------

print("=== RabbitMQ Emitter (setup example) ===\n")

# Requires: pip install pika (or: pip install dataforge-py[rabbitmq])
rabbit_emitter = RabbitMQEmitter(
    host="localhost",
    port=5672,
    queue="user-events",
    exchange="",
    routing_key="user-events",
)
print(f"Emitter: {rabbit_emitter}")
print("Usage:")
print("  stream_to_emitter(schema, rabbit_emitter, count=10000, batch_size=100)")
print()

# Uncomment if you have RabbitMQ running:
# stream_to_emitter(event_schema, rabbit_emitter, count=10000, batch_size=100)

# --- Example 6: Load testing workflow pattern -----------------------------

print("=== Load Testing Workflow ===\n")

print("Complete load testing setup:")
print()
print("  from dataforge import DataForge")
print("  from dataforge.streaming import HttpEmitter, TokenBucketRateLimiter")
print("  from dataforge.streaming import stream_to_emitter")
print()
print("  forge = DataForge(seed=42)")
print("  schema = forge.schema({")
print("      'event_type': 'lorem.word',")
print("      'user_id': 'misc.uuid4',")
print("      'timestamp': 'dt.datetime',")
print("      'ip': 'internet.ipv4',")
print("      'user_agent': 'network.user_agent',")
print("  })")
print()
print("  # Stream 100K events at 1000/sec to your API")
print("  limiter = TokenBucketRateLimiter(rate=1000, burst=50)")
print("  emitter = HttpEmitter('http://localhost:8080/events')")
print("  stream_to_emitter(schema, emitter, count=100_000,")
print("                     batch_size=100, rate_limiter=limiter)")
