"""Tests for streaming file writes and parquet export."""

import csv
import json
import os
import tempfile

import pytest

from dataforge import DataForge


@pytest.fixture
def forge() -> DataForge:
    return DataForge(seed=42)


@pytest.fixture
def schema(forge: DataForge):
    return forge.schema(["first_name", "email", "city"])


class TestStreamToCsv:
    def test_writes_file(self, schema) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name
        try:
            result = schema.stream_to_csv(path, count=100)
            assert result == 100
            assert os.path.exists(path)
        finally:
            os.unlink(path)

    def test_csv_content(self, schema) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name
        try:
            schema.stream_to_csv(path, count=50)
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader)
                assert header == ["first_name", "email", "city"]
                rows = list(reader)
                assert len(rows) == 50
                for row in rows:
                    assert len(row) == 3
        finally:
            os.unlink(path)

    def test_batch_size(self, schema) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name
        try:
            result = schema.stream_to_csv(path, count=100, batch_size=10)
            assert result == 100
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader)  # skip header
                rows = list(reader)
                assert len(rows) == 100
        finally:
            os.unlink(path)

    def test_zero_count(self, schema) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name
        try:
            result = schema.stream_to_csv(path, count=0)
            assert result == 0
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader)
                assert header == ["first_name", "email", "city"]
                rows = list(reader)
                assert len(rows) == 0
        finally:
            os.unlink(path)

    def test_via_forge(self, forge: DataForge) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name
        try:
            result = forge.stream_to_csv(["first_name", "email"], path, count=20)
            assert result == 20
        finally:
            os.unlink(path)


class TestStreamToJsonl:
    def test_writes_file(self, schema) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name
        try:
            result = schema.stream_to_jsonl(path, count=100)
            assert result == 100
            assert os.path.exists(path)
        finally:
            os.unlink(path)

    def test_jsonl_content(self, schema) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name
        try:
            schema.stream_to_jsonl(path, count=50)
            with open(path, "r", encoding="utf-8") as f:
                lines = f.read().strip().split("\n")
            assert len(lines) == 50
            for line in lines:
                obj = json.loads(line)
                assert "first_name" in obj
                assert "email" in obj
                assert "city" in obj
        finally:
            os.unlink(path)

    def test_batch_size(self, schema) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name
        try:
            result = schema.stream_to_jsonl(path, count=100, batch_size=7)
            assert result == 100
            with open(path, "r", encoding="utf-8") as f:
                lines = f.read().strip().split("\n")
            assert len(lines) == 100
        finally:
            os.unlink(path)

    def test_via_forge(self, forge: DataForge) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name
        try:
            result = forge.stream_to_jsonl(["first_name", "email"], path, count=20)
            assert result == 20
        finally:
            os.unlink(path)


class TestToParquet:
    @pytest.fixture(autouse=True)
    def _skip_no_pyarrow(self) -> None:
        pytest.importorskip("pyarrow")

    def test_writes_file(self, schema) -> None:
        with tempfile.NamedTemporaryFile(
            mode="wb", suffix=".parquet", delete=False
        ) as f:
            path = f.name
        try:
            result = schema.to_parquet(path, count=100)
            assert result == 100
            assert os.path.exists(path)
            assert os.path.getsize(path) > 0
        finally:
            os.unlink(path)

    def test_parquet_content(self, schema) -> None:
        import pyarrow.parquet as pq

        with tempfile.NamedTemporaryFile(
            mode="wb", suffix=".parquet", delete=False
        ) as f:
            path = f.name
        try:
            schema.to_parquet(path, count=50)
            table = pq.read_table(path)
            assert table.num_rows == 50
            assert table.column_names == ["first_name", "email", "city"]
        finally:
            os.unlink(path)

    def test_batch_size(self, schema) -> None:
        import pyarrow.parquet as pq

        with tempfile.NamedTemporaryFile(
            mode="wb", suffix=".parquet", delete=False
        ) as f:
            path = f.name
        try:
            schema.to_parquet(path, count=100, batch_size=25)
            table = pq.read_table(path)
            assert table.num_rows == 100
        finally:
            os.unlink(path)

    def test_via_forge(self, forge: DataForge) -> None:
        with tempfile.NamedTemporaryFile(
            mode="wb", suffix=".parquet", delete=False
        ) as f:
            path = f.name
        try:
            result = forge.to_parquet(["first_name", "email"], path, count=20)
            assert result == 20
        finally:
            os.unlink(path)

    def test_no_pyarrow_error(self, forge: DataForge, monkeypatch) -> None:
        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "pyarrow":
                raise ModuleNotFoundError("mocked")
            return real_import(name, *args, **kwargs)

        schema = forge.schema(["first_name"])
        monkeypatch.setattr(builtins, "__import__", mock_import)
        with pytest.raises(ModuleNotFoundError, match="pyarrow"):
            schema.to_parquet("dummy.parquet", count=1)


class TestAsyncStream:
    @pytest.fixture
    def schema(self, forge: DataForge):
        return forge.schema(["first_name", "email", "city"])

    @pytest.mark.asyncio
    async def test_yields_correct_count(self, schema) -> None:
        rows = [row async for row in schema.async_stream(100)]
        assert len(rows) == 100

    @pytest.mark.asyncio
    async def test_row_structure(self, schema) -> None:
        rows = [row async for row in schema.async_stream(10)]
        for row in rows:
            assert isinstance(row, dict)
            assert set(row.keys()) == {"first_name", "email", "city"}
            assert all(isinstance(v, str) for v in row.values())

    @pytest.mark.asyncio
    async def test_batch_size(self, schema) -> None:
        """Custom batch size should produce identical results."""
        rows = [row async for row in schema.async_stream(100, batch_size=7)]
        assert len(rows) == 100
        for row in rows:
            assert len(row) == 3

    @pytest.mark.asyncio
    async def test_zero_count(self, schema) -> None:
        rows = [row async for row in schema.async_stream(0)]
        assert rows == []

    @pytest.mark.asyncio
    async def test_single_row(self, schema) -> None:
        rows = [row async for row in schema.async_stream(1)]
        assert len(rows) == 1
        assert "first_name" in rows[0]

    @pytest.mark.asyncio
    async def test_deterministic_with_seed(self, forge: DataForge) -> None:
        """Same seed should produce identical results."""
        s1 = DataForge(seed=99).schema(["first_name", "city"])
        s2 = DataForge(seed=99).schema(["first_name", "city"])
        rows1 = [row async for row in s1.async_stream(50)]
        rows2 = [row async for row in s2.async_stream(50)]
        assert rows1 == rows2

    @pytest.mark.asyncio
    async def test_with_lambda_fields(self, forge: DataForge) -> None:
        """Lambda fields should work in async_stream."""
        schema = forge.schema(
            {
                "name": "first_name",
                "upper": lambda row: row["name"].upper(),
            }
        )
        rows = [row async for row in schema.async_stream(20)]
        assert len(rows) == 20
        for row in rows:
            assert row["upper"] == row["name"].upper()

    @pytest.mark.asyncio
    async def test_matches_sync_stream(self, forge: DataForge) -> None:
        """async_stream should produce the same data as sync stream."""
        s1 = DataForge(seed=42).schema(["first_name", "email"])
        s2 = DataForge(seed=42).schema(["first_name", "email"])
        sync_rows = list(s1.stream(50))
        async_rows = [row async for row in s2.async_stream(50)]
        assert sync_rows == async_rows
