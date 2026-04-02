"""Tests for streaming to message queues — emitters and rate limiting."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from dataforge import DataForge
from dataforge.streaming import (
    StreamEmitter,
    HttpEmitter,
    KafkaEmitter,
    RabbitMQEmitter,
    TokenBucketRateLimiter,
    stream_to_emitter,
    stream_batch_to_emitter,
)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture
def forge() -> DataForge:
    return DataForge(locale="en_US", seed=42)


@pytest.fixture
def schema(forge: DataForge):
    return forge.schema(["first_name", "email"])


# ------------------------------------------------------------------
# TokenBucketRateLimiter
# ------------------------------------------------------------------


class TestTokenBucketRateLimiter:
    def test_construction(self) -> None:
        limiter = TokenBucketRateLimiter(rate=100.0, burst=10)
        assert limiter._rate == 100.0
        assert limiter._burst == 10

    def test_acquire_within_burst(self) -> None:
        limiter = TokenBucketRateLimiter(rate=1000.0, burst=10)
        # Should not block for burst tokens
        start = time.monotonic()
        for _ in range(10):
            limiter.acquire(1)
        elapsed = time.monotonic() - start
        assert elapsed < 1.0  # Should be nearly instant

    def test_acquire_rate_limited(self) -> None:
        limiter = TokenBucketRateLimiter(rate=100.0, burst=1)
        # After burst, should be rate-limited
        limiter.acquire(1)  # Uses burst token
        start = time.monotonic()
        limiter.acquire(1)  # Should wait ~0.01s
        elapsed = time.monotonic() - start
        # Should take at least some time (rate is 100/s = 0.01s per token)
        # Be lenient with timing
        assert elapsed >= 0.005

    def test_slots(self) -> None:
        limiter = TokenBucketRateLimiter()
        with pytest.raises(AttributeError):
            limiter.nonexistent = True  # type: ignore[attr-defined]


# ------------------------------------------------------------------
# StreamEmitter (abstract base)
# ------------------------------------------------------------------


class TestStreamEmitter:
    def test_emit_not_implemented(self) -> None:
        emitter = StreamEmitter()
        with pytest.raises(NotImplementedError):
            emitter.emit({"key": "value"})

    def test_emit_batch_delegates_to_emit(self) -> None:
        class MockEmitter(StreamEmitter):
            def __init__(self):
                self.emitted = []

            def emit(self, row):
                self.emitted.append(row)

        emitter = MockEmitter()
        rows = [{"a": 1}, {"b": 2}]
        emitter.emit_batch(rows)
        assert emitter.emitted == rows

    def test_context_manager(self) -> None:
        class TrackingEmitter(StreamEmitter):
            def __init__(self):
                self.opened = False
                self.closed = False

            def open(self):
                self.opened = True

            def emit(self, row):
                pass

            def close(self):
                self.closed = True

        emitter = TrackingEmitter()
        with emitter:
            assert emitter.opened
        assert emitter.closed

    def test_slots(self) -> None:
        emitter = StreamEmitter()
        with pytest.raises(AttributeError):
            emitter.nonexistent = True  # type: ignore[attr-defined]


# ------------------------------------------------------------------
# HttpEmitter
# ------------------------------------------------------------------


class TestHttpEmitter:
    def test_repr(self) -> None:
        emitter = HttpEmitter("https://example.com/api")
        assert "HttpEmitter" in repr(emitter)
        assert "https://example.com/api" in repr(emitter)

    def test_construction(self) -> None:
        emitter = HttpEmitter(
            "https://example.com",
            headers={"Authorization": "Bearer token"},
            batch_mode=False,
            timeout=10.0,
        )
        assert emitter._url == "https://example.com"
        assert emitter._headers["Authorization"] == "Bearer token"
        assert emitter._batch_mode is False
        assert emitter._timeout == 10.0

    def test_slots(self) -> None:
        emitter = HttpEmitter("https://example.com")
        with pytest.raises(AttributeError):
            emitter.nonexistent = True  # type: ignore[attr-defined]

    @patch("urllib.request.urlopen")
    def test_emit_single_row(self, mock_urlopen) -> None:
        mock_urlopen.return_value = MagicMock()
        emitter = HttpEmitter("https://example.com/api")
        emitter.emit({"name": "Alice", "age": 30})
        mock_urlopen.assert_called_once()

    @patch("urllib.request.urlopen")
    def test_emit_batch(self, mock_urlopen) -> None:
        mock_urlopen.return_value = MagicMock()
        emitter = HttpEmitter("https://example.com/api", batch_mode=True)
        rows = [{"name": "Alice"}, {"name": "Bob"}]
        emitter.emit_batch(rows)
        mock_urlopen.assert_called_once()

    @patch("urllib.request.urlopen")
    def test_emit_batch_non_batch_mode(self, mock_urlopen) -> None:
        mock_urlopen.return_value = MagicMock()
        emitter = HttpEmitter("https://example.com/api", batch_mode=False)
        rows = [{"name": "Alice"}, {"name": "Bob"}]
        emitter.emit_batch(rows)
        assert mock_urlopen.call_count == 2


# ------------------------------------------------------------------
# KafkaEmitter
# ------------------------------------------------------------------


class TestKafkaEmitter:
    def test_repr(self) -> None:
        emitter = KafkaEmitter(topic="test-topic")
        assert "KafkaEmitter" in repr(emitter)
        assert "test-topic" in repr(emitter)

    def test_construction(self) -> None:
        emitter = KafkaEmitter(
            bootstrap_servers="kafka:9092",
            topic="my-topic",
            config={"acks": "all"},
        )
        assert emitter._servers == "kafka:9092"
        assert emitter._topic == "my-topic"
        assert emitter._producer is None

    def test_open_without_confluent_kafka(self) -> None:
        emitter = KafkaEmitter()
        with patch.dict("sys.modules", {"confluent_kafka": None}):
            with pytest.raises(ModuleNotFoundError, match="confluent-kafka"):
                emitter.open()

    def test_slots(self) -> None:
        emitter = KafkaEmitter()
        with pytest.raises(AttributeError):
            emitter.nonexistent = True  # type: ignore[attr-defined]


# ------------------------------------------------------------------
# RabbitMQEmitter
# ------------------------------------------------------------------


class TestRabbitMQEmitter:
    def test_repr(self) -> None:
        emitter = RabbitMQEmitter(queue="test-queue")
        assert "RabbitMQEmitter" in repr(emitter)
        assert "test-queue" in repr(emitter)

    def test_construction(self) -> None:
        emitter = RabbitMQEmitter(
            host="rabbit-host",
            queue="my-queue",
            exchange="my-exchange",
            routing_key="my-key",
            port=5673,
        )
        assert emitter._host == "rabbit-host"
        assert emitter._queue == "my-queue"
        assert emitter._exchange == "my-exchange"
        assert emitter._port == 5673
        assert emitter._connection is None

    def test_open_without_pika(self) -> None:
        emitter = RabbitMQEmitter()
        with patch.dict("sys.modules", {"pika": None}):
            with pytest.raises(ModuleNotFoundError, match="pika"):
                emitter.open()

    def test_slots(self) -> None:
        emitter = RabbitMQEmitter()
        with pytest.raises(AttributeError):
            emitter.nonexistent = True  # type: ignore[attr-defined]

    def test_close_noop_when_not_connected(self) -> None:
        emitter = RabbitMQEmitter()
        # Should not raise
        emitter.close()


# ------------------------------------------------------------------
# stream_to_emitter helper
# ------------------------------------------------------------------


class TestStreamToEmitter:
    def test_stream_to_emitter(self, schema) -> None:
        class CollectingEmitter(StreamEmitter):
            def __init__(self):
                self.rows = []

            def emit(self, row):
                self.rows.append(row)

        emitter = CollectingEmitter()
        count = stream_to_emitter(schema, emitter, count=20, batch_size=5)
        assert count == 20
        assert len(emitter.rows) == 20
        # Each row should have the schema fields
        assert "first_name" in emitter.rows[0]
        assert "email" in emitter.rows[0]


class TestStreamBatchToEmitter:
    def test_stream_batch(self, schema) -> None:
        class BatchCollector(StreamEmitter):
            def __init__(self):
                self.batches = []

            def emit(self, row):
                pass

            def emit_batch(self, rows):
                self.batches.append(rows)

        emitter = BatchCollector()
        count = stream_batch_to_emitter(schema, emitter, count=25, batch_size=10)
        assert count == 25
        # Should have 3 batches: 10, 10, 5
        assert len(emitter.batches) == 3
        assert len(emitter.batches[0]) == 10
        assert len(emitter.batches[1]) == 10
        assert len(emitter.batches[2]) == 5

    def test_stream_batch_with_rate_limiter(self, schema) -> None:
        class NoopEmitter(StreamEmitter):
            def emit(self, row):
                pass

            def emit_batch(self, rows):
                pass

        emitter = NoopEmitter()
        limiter = TokenBucketRateLimiter(rate=10000, burst=1000)
        count = stream_batch_to_emitter(
            schema, emitter, count=50, batch_size=10, rate_limiter=limiter
        )
        assert count == 50
