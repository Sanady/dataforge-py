"""Streaming to message queues — emit generated data to HTTP, Kafka, RabbitMQ.

Provides abstract and concrete emitters for streaming fake data to
external systems in real time with rate limiting.

Usage::

    from dataforge import DataForge
    from dataforge.streaming import HttpEmitter, TokenBucketRateLimiter

    forge = DataForge(seed=42)
    schema = forge.schema(["first_name", "email", "city"])

    # Stream to HTTP endpoint
    emitter = HttpEmitter("https://api.example.com/ingest")
    schema.stream_to(emitter, count=10000, rate_limit=100)

    # With rate limiting
    limiter = TokenBucketRateLimiter(rate=50, burst=10)
    emitter = HttpEmitter("https://api.example.com/ingest")
    stream_to_emitter(schema, emitter, count=1000, rate_limiter=limiter)
"""

from __future__ import annotations

import json as _json
import time as _time
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from dataforge.schema import Schema


# ------------------------------------------------------------------
# Rate limiter
# ------------------------------------------------------------------


class TokenBucketRateLimiter:
    """Token bucket rate limiter using ``time.monotonic()``.

    Parameters
    ----------
    rate : float
        Tokens per second (sustained rate).
    burst : int
        Maximum burst size (bucket capacity).
    """

    __slots__ = ("_rate", "_burst", "_tokens", "_last_time")

    def __init__(self, rate: float = 100.0, burst: int = 10) -> None:
        self._rate = rate
        self._burst = burst
        self._tokens = float(burst)
        self._last_time = _time.monotonic()

    def acquire(self, n: int = 1) -> None:
        """Block until *n* tokens are available."""
        while True:
            now = _time.monotonic()
            elapsed = now - self._last_time
            self._last_time = now
            self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
            if self._tokens >= n:
                self._tokens -= n
                return
            # Sleep for the time needed to accumulate enough tokens
            deficit = n - self._tokens
            _time.sleep(deficit / self._rate)


# ------------------------------------------------------------------
# Abstract emitter
# ------------------------------------------------------------------


class StreamEmitter:
    """Abstract base class for stream emitters.

    Subclasses must implement :meth:`emit` and optionally
    :meth:`open` and :meth:`close` for resource management.
    """

    __slots__ = ()

    def open(self) -> None:
        """Open the connection / prepare resources."""

    def emit(self, row: dict[str, Any]) -> None:
        """Emit a single row to the target system."""
        raise NotImplementedError

    def emit_batch(self, rows: list[dict[str, Any]]) -> None:
        """Emit a batch of rows. Default: emit one by one."""
        for row in rows:
            self.emit(row)

    def close(self) -> None:
        """Close the connection / release resources."""

    def __enter__(self) -> "StreamEmitter":
        self.open()
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


# ------------------------------------------------------------------
# HTTP emitter (zero-dep, stdlib urllib)
# ------------------------------------------------------------------


class HttpEmitter(StreamEmitter):
    """Stream data to an HTTP endpoint via POST requests.

    Uses stdlib ``urllib`` — zero external dependencies.

    Parameters
    ----------
    url : str
        Target URL for POST requests.
    headers : dict[str, str] | None
        Additional HTTP headers.
    batch_mode : bool
        If True, emit_batch sends the whole batch as a JSON array.
        If False, each row is sent individually.
    timeout : float
        Request timeout in seconds.
    """

    __slots__ = ("_url", "_headers", "_batch_mode", "_timeout")

    def __init__(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        batch_mode: bool = True,
        timeout: float = 30.0,
    ) -> None:
        self._url = url
        self._headers = headers or {}
        self._batch_mode = batch_mode
        self._timeout = timeout

    def emit(self, row: dict[str, Any]) -> None:
        """POST a single row as JSON."""
        import urllib.request

        data = _json.dumps(row, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json", **self._headers}
        req = urllib.request.Request(
            self._url, data=data, headers=headers, method="POST"
        )
        urllib.request.urlopen(req, timeout=self._timeout)

    def emit_batch(self, rows: list[dict[str, Any]]) -> None:
        """POST rows as a JSON array (or one-by-one if batch_mode is False)."""
        if not self._batch_mode:
            for row in rows:
                self.emit(row)
            return

        import urllib.request

        data = _json.dumps(rows, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json", **self._headers}
        req = urllib.request.Request(
            self._url, data=data, headers=headers, method="POST"
        )
        urllib.request.urlopen(req, timeout=self._timeout)

    def __repr__(self) -> str:
        return f"HttpEmitter(url={self._url!r})"


# ------------------------------------------------------------------
# Kafka emitter (optional confluent-kafka)
# ------------------------------------------------------------------


class KafkaEmitter(StreamEmitter):
    """Stream data to Apache Kafka.

    Requires ``confluent-kafka`` to be installed.

    Parameters
    ----------
    bootstrap_servers : str
        Kafka bootstrap servers.
    topic : str
        Kafka topic to produce to.
    config : dict | None
        Additional Kafka producer configuration.
    """

    __slots__ = ("_servers", "_topic", "_config", "_producer")

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "dataforge",
        config: dict[str, Any] | None = None,
    ) -> None:
        self._servers = bootstrap_servers
        self._topic = topic
        self._config = config or {}
        self._producer: Any = None

    def open(self) -> None:
        try:
            from confluent_kafka import Producer
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "confluent-kafka is required for KafkaEmitter. "
                "Install it with: pip install confluent-kafka"
            ) from exc
        conf = {"bootstrap.servers": self._servers, **self._config}
        self._producer = Producer(conf)

    def emit(self, row: dict[str, Any]) -> None:
        if self._producer is None:
            self.open()
        data = _json.dumps(row, ensure_ascii=False).encode("utf-8")
        self._producer.produce(self._topic, data)

    def emit_batch(self, rows: list[dict[str, Any]]) -> None:
        if self._producer is None:
            self.open()
        for row in rows:
            data = _json.dumps(row, ensure_ascii=False).encode("utf-8")
            self._producer.produce(self._topic, data)
        self._producer.flush()

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush()
            self._producer = None

    def __repr__(self) -> str:
        return f"KafkaEmitter(servers={self._servers!r}, topic={self._topic!r})"


# ------------------------------------------------------------------
# RabbitMQ emitter (optional pika)
# ------------------------------------------------------------------


class RabbitMQEmitter(StreamEmitter):
    """Stream data to RabbitMQ.

    Requires ``pika`` to be installed.

    Parameters
    ----------
    host : str
        RabbitMQ host.
    queue : str
        Queue name.
    exchange : str
        Exchange name.
    routing_key : str
        Routing key.
    port : int
        RabbitMQ port.
    """

    __slots__ = (
        "_host",
        "_queue",
        "_exchange",
        "_routing_key",
        "_port",
        "_connection",
        "_channel",
    )

    def __init__(
        self,
        host: str = "localhost",
        queue: str = "dataforge",
        exchange: str = "",
        routing_key: str = "dataforge",
        port: int = 5672,
    ) -> None:
        self._host = host
        self._queue = queue
        self._exchange = exchange
        self._routing_key = routing_key
        self._port = port
        self._connection: Any = None
        self._channel: Any = None

    def open(self) -> None:
        try:
            import pika
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "pika is required for RabbitMQEmitter. "
                "Install it with: pip install pika"
            ) from exc
        params = pika.ConnectionParameters(host=self._host, port=self._port)
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self._queue, durable=True)

    def emit(self, row: dict[str, Any]) -> None:
        if self._channel is None:
            self.open()
        data = _json.dumps(row, ensure_ascii=False).encode("utf-8")
        self._channel.basic_publish(
            exchange=self._exchange,
            routing_key=self._routing_key,
            body=data,
        )

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            self._channel = None

    def __repr__(self) -> str:
        return f"RabbitMQEmitter(host={self._host!r}, queue={self._queue!r})"


# ------------------------------------------------------------------
# Streaming helper
# ------------------------------------------------------------------


def stream_to_emitter(
    schema: "Schema",
    emitter: StreamEmitter,
    count: int = 1000,
    batch_size: int = 100,
    rate_limiter: TokenBucketRateLimiter | None = None,
) -> int:
    """Stream schema-generated data to an emitter.

    Uses batch generation and batch emission for better throughput.

    Parameters
    ----------
    schema : Schema
        The DataForge Schema to generate data from.
    emitter : StreamEmitter
        The target emitter.
    count : int
        Total number of rows to emit.
    batch_size : int
        Rows per batch.
    rate_limiter : TokenBucketRateLimiter | None
        Optional rate limiter.

    Returns
    -------
    int
        Number of rows emitted.
    """
    emitted = 0
    remaining = count

    with emitter:
        while remaining > 0:
            chunk = min(remaining, batch_size)
            rows = schema.generate(count=chunk)
            if rate_limiter is not None:
                rate_limiter.acquire(chunk)
            emitter.emit_batch(rows)
            emitted += chunk
            remaining -= chunk

    return emitted


def stream_batch_to_emitter(
    schema: "Schema",
    emitter: StreamEmitter,
    count: int = 1000,
    batch_size: int = 100,
    rate_limiter: TokenBucketRateLimiter | None = None,
) -> int:
    """Stream schema-generated data in batches to an emitter.

    Parameters
    ----------
    schema : Schema
        The DataForge Schema to generate data from.
    emitter : StreamEmitter
        The target emitter.
    count : int
        Total number of rows to emit.
    batch_size : int
        Rows per batch.
    rate_limiter : TokenBucketRateLimiter | None
        Optional rate limiter.

    Returns
    -------
    int
        Number of rows emitted.
    """
    emitted = 0
    remaining = count

    with emitter:
        while remaining > 0:
            chunk = min(remaining, batch_size)
            rows = schema.generate(count=chunk)
            if rate_limiter is not None:
                rate_limiter.acquire(chunk)
            emitter.emit_batch(rows)
            emitted += chunk
            remaining -= chunk

    return emitted
