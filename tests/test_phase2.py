"""Phase 2 tests — nullable fields, encoding, compression, statistical distributions."""

from __future__ import annotations

import gzip
import json
import os
import tempfile

import pytest

from dataforge import DataForge


# ------------------------------------------------------------------
# Nullable field support
# ------------------------------------------------------------------


class TestNullableFields:
    """Test null_fields parameter on Schema."""

    def setup_method(self) -> None:
        self.forge = DataForge(seed=42)

    def test_null_fields_basic(self) -> None:
        """Fields with null_probability should produce some None values."""
        schema = self.forge.schema(
            ["first_name", "email"],
            null_fields={"email": 0.5},
        )
        rows = schema.generate(count=200)
        none_count = sum(1 for r in rows if r["email"] is None)
        # With p=0.5 and 200 rows, we expect ~100 Nones.
        # Allow wide margin: 30-170
        assert 30 < none_count < 170, f"Expected ~100 Nones, got {none_count}"
        # first_name should never be None (not in null_fields)
        assert all(r["first_name"] is not None for r in rows)

    def test_null_fields_zero_probability(self) -> None:
        """Probability 0.0 should produce no None values."""
        schema = self.forge.schema(
            ["first_name", "email"],
            null_fields={"email": 0.0},
        )
        rows = schema.generate(count=100)
        assert all(r["email"] is not None for r in rows)

    def test_null_fields_full_probability(self) -> None:
        """Probability 1.0 should make all values None."""
        schema = self.forge.schema(
            ["first_name", "email"],
            null_fields={"email": 1.0},
        )
        rows = schema.generate(count=50)
        assert all(r["email"] is None for r in rows)

    def test_null_fields_multiple_columns(self) -> None:
        """Multiple columns can have different null probabilities."""
        schema = self.forge.schema(
            ["first_name", "email", "city"],
            null_fields={"email": 0.5, "city": 0.3},
        )
        rows = schema.generate(count=200)
        email_nones = sum(1 for r in rows if r["email"] is None)
        city_nones = sum(1 for r in rows if r["city"] is None)
        # email should have more nulls than city on average
        assert email_nones > 0
        assert city_nones > 0
        assert all(r["first_name"] is not None for r in rows)

    def test_null_fields_invalid_column_raises(self) -> None:
        """Invalid column name in null_fields should raise ValueError."""
        with pytest.raises(ValueError, match="not a column"):
            self.forge.schema(
                ["first_name", "email"],
                null_fields={"nonexistent": 0.5},
            )

    def test_null_fields_none_arg(self) -> None:
        """null_fields=None should work (no nulls)."""
        schema = self.forge.schema(
            ["first_name", "email"],
            null_fields=None,
        )
        rows = schema.generate(count=50)
        assert all(r["email"] is not None for r in rows)

    def test_null_fields_in_csv(self) -> None:
        """Null values should appear as empty strings in CSV output."""
        schema = self.forge.schema(
            ["first_name", "email"],
            null_fields={"email": 1.0},
        )
        csv_output = schema.to_csv(count=5)
        lines = csv_output.strip().splitlines()
        # Header + 5 data rows
        assert len(lines) == 6
        for line in lines[1:]:
            # email column should be empty
            parts = line.strip().split(",")
            assert parts[1] == "", f"Expected empty email, got {parts[1]!r}"

    def test_null_fields_in_json(self) -> None:
        """Null values should appear as null in JSON output."""
        schema = self.forge.schema(
            ["first_name", "email"],
            null_fields={"email": 1.0},
        )
        json_output = schema.to_json(count=3)
        data = json.loads(json_output)
        for row in data:
            assert row["email"] is None

    def test_null_fields_in_sql(self) -> None:
        """Null values should appear as NULL in SQL output."""
        schema = self.forge.schema(
            ["first_name", "email"],
            null_fields={"email": 1.0},
        )
        sql_output = schema.to_sql(table="users", count=3)
        assert "NULL" in sql_output

    def test_null_fields_with_stream(self) -> None:
        """Null injection should work with stream()."""
        schema = self.forge.schema(
            ["first_name", "email"],
            null_fields={"email": 1.0},
        )
        rows = list(schema.stream(count=10))
        assert len(rows) == 10
        assert all(r["email"] is None for r in rows)

    def test_null_fields_with_lambda(self) -> None:
        """Null fields should work alongside lambda fields."""
        schema = self.forge.schema(
            {
                "name": "first_name",
                "email": "email",
                "upper_name": lambda row: row["name"].upper(),
            },
            null_fields={"email": 1.0},
        )
        rows = schema.generate(count=5)
        for row in rows:
            assert row["email"] is None
            assert row["upper_name"] == row["name"].upper()

    def test_null_fields_via_core_schema(self) -> None:
        """DataForge.schema() should pass null_fields through."""
        schema = self.forge.schema(
            ["first_name", "email"],
            null_fields={"email": 1.0},
        )
        rows = schema.generate(count=5)
        assert all(r["email"] is None for r in rows)


# ------------------------------------------------------------------
# Encoding support
# ------------------------------------------------------------------


class TestEncoding:
    """Test encoding parameter on export methods."""

    def setup_method(self) -> None:
        self.forge = DataForge(seed=42)

    def test_csv_encoding_utf8(self) -> None:
        """Default UTF-8 encoding should work."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            schema = self.forge.schema(["first_name", "email"])
            schema.to_csv(count=5, path=path, encoding="utf-8")
            with open(path, encoding="utf-8") as f:
                content = f.read()
            assert "first_name" in content
        finally:
            os.unlink(path)

    def test_csv_encoding_latin1(self) -> None:
        """Latin-1 encoding should be usable."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            schema = self.forge.schema(["first_name", "email"])
            schema.to_csv(count=5, path=path, encoding="latin-1")
            with open(path, encoding="latin-1") as f:
                content = f.read()
            assert "first_name" in content
        finally:
            os.unlink(path)

    def test_jsonl_encoding(self) -> None:
        """JSONL encoding parameter should work."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            path = f.name
        try:
            schema = self.forge.schema(["first_name"])
            schema.to_jsonl(count=3, path=path, encoding="utf-8")
            with open(path, encoding="utf-8") as f:
                lines = f.readlines()
            assert len(lines) == 3
        finally:
            os.unlink(path)

    def test_json_encoding(self) -> None:
        """JSON encoding parameter should work."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            schema = self.forge.schema(["first_name"])
            schema.to_json(count=3, path=path, encoding="utf-8")
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            assert len(data) == 3
        finally:
            os.unlink(path)

    def test_stream_csv_encoding(self) -> None:
        """stream_to_csv should accept encoding parameter."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            schema = self.forge.schema(["first_name", "email"])
            written = schema.stream_to_csv(path=path, count=10, encoding="utf-8")
            assert written == 10
            with open(path, encoding="utf-8") as f:
                content = f.read()
            assert "first_name" in content
        finally:
            os.unlink(path)

    def test_stream_jsonl_encoding(self) -> None:
        """stream_to_jsonl should accept encoding parameter."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            path = f.name
        try:
            schema = self.forge.schema(["first_name"])
            written = schema.stream_to_jsonl(path=path, count=5, encoding="utf-8")
            assert written == 5
        finally:
            os.unlink(path)

    def test_core_csv_encoding_passthrough(self) -> None:
        """DataForge.to_csv() should pass encoding through."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            self.forge.to_csv(
                ["first_name", "email"],
                count=5,
                path=path,
                encoding="latin-1",
            )
            with open(path, encoding="latin-1") as f:
                content = f.read()
            assert "first_name" in content
        finally:
            os.unlink(path)


# ------------------------------------------------------------------
# Compression support (gzip)
# ------------------------------------------------------------------


class TestCompression:
    """Test gzip compression on export methods."""

    def setup_method(self) -> None:
        self.forge = DataForge(seed=42)

    def test_csv_gzip_auto(self) -> None:
        """CSV with .gz extension should auto-compress."""
        with tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=False) as f:
            path = f.name
        try:
            schema = self.forge.schema(["first_name", "email"])
            schema.to_csv(count=5, path=path)
            # Read as gzip
            with gzip.open(path, "rt", encoding="utf-8") as f:
                content = f.read()
            assert "first_name" in content
            # Verify it's actually gzip (raw read starts with gzip magic bytes)
            with open(path, "rb") as f:
                magic = f.read(2)
            assert magic == b"\x1f\x8b"
        finally:
            os.unlink(path)

    def test_csv_gzip_explicit(self) -> None:
        """CSV with compress=True should gzip even without .gz extension."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            schema = self.forge.schema(["first_name", "email"])
            schema.to_csv(count=5, path=path, compress=True)
            with gzip.open(path, "rt", encoding="utf-8") as f:
                content = f.read()
            assert "first_name" in content
        finally:
            os.unlink(path)

    def test_csv_gzip_suppress(self) -> None:
        """compress=False should disable auto-gzip even with .gz extension."""
        with tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=False) as f:
            path = f.name
        try:
            schema = self.forge.schema(["first_name", "email"])
            schema.to_csv(count=5, path=path, compress=False)
            # Should be plain text, not gzip
            with open(path, encoding="utf-8") as f:
                content = f.read()
            assert "first_name" in content
        finally:
            os.unlink(path)

    def test_jsonl_gzip(self) -> None:
        """JSONL with .gz extension should auto-compress."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl.gz", delete=False) as f:
            path = f.name
        try:
            schema = self.forge.schema(["first_name", "email"])
            schema.to_jsonl(count=5, path=path)
            with gzip.open(path, "rt", encoding="utf-8") as f:
                lines = f.readlines()
            assert len(lines) == 5
            # Verify each line is valid JSON
            for line in lines:
                data = json.loads(line)
                assert "first_name" in data
        finally:
            os.unlink(path)

    def test_json_gzip(self) -> None:
        """JSON with .gz extension should auto-compress."""
        with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as f:
            path = f.name
        try:
            schema = self.forge.schema(["first_name"])
            schema.to_json(count=3, path=path)
            with gzip.open(path, "rt", encoding="utf-8") as f:
                data = json.load(f)
            assert len(data) == 3
        finally:
            os.unlink(path)

    def test_sql_gzip(self) -> None:
        """SQL with .gz extension should auto-compress."""
        with tempfile.NamedTemporaryFile(suffix=".sql.gz", delete=False) as f:
            path = f.name
        try:
            schema = self.forge.schema(["first_name", "email"])
            schema.to_sql(table="users", count=3, path=path)
            with gzip.open(path, "rt", encoding="utf-8") as f:
                content = f.read()
            assert "INSERT INTO" in content
        finally:
            os.unlink(path)

    def test_stream_csv_gzip(self) -> None:
        """stream_to_csv with .gz extension should auto-compress."""
        with tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=False) as f:
            path = f.name
        try:
            schema = self.forge.schema(["first_name", "email"])
            written = schema.stream_to_csv(path=path, count=10)
            assert written == 10
            with gzip.open(path, "rt", encoding="utf-8") as f:
                content = f.read()
            assert "first_name" in content
        finally:
            os.unlink(path)

    def test_stream_jsonl_gzip(self) -> None:
        """stream_to_jsonl with .gz extension should auto-compress."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl.gz", delete=False) as f:
            path = f.name
        try:
            schema = self.forge.schema(["first_name"])
            written = schema.stream_to_jsonl(path=path, count=5)
            assert written == 5
            with gzip.open(path, "rt", encoding="utf-8") as f:
                lines = f.readlines()
            assert len(lines) == 5
        finally:
            os.unlink(path)

    def test_core_csv_gzip_passthrough(self) -> None:
        """DataForge.to_csv() should pass compress through."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            self.forge.to_csv(
                ["first_name", "email"],
                count=5,
                path=path,
                compress=True,
            )
            with gzip.open(path, "rt", encoding="utf-8") as f:
                content = f.read()
            assert "first_name" in content
        finally:
            os.unlink(path)

    def test_core_jsonl_gzip_passthrough(self) -> None:
        """DataForge.to_jsonl() should pass compress through."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl.gz", delete=False) as f:
            path = f.name
        try:
            self.forge.to_jsonl(
                ["first_name"],
                count=3,
                path=path,
            )
            with gzip.open(path, "rt", encoding="utf-8") as f:
                lines = f.readlines()
            assert len(lines) == 3
        finally:
            os.unlink(path)

    def test_core_stream_csv_gzip_passthrough(self) -> None:
        """DataForge.stream_to_csv() should pass compress through."""
        with tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=False) as f:
            path = f.name
        try:
            written = self.forge.stream_to_csv(
                ["first_name", "email"],
                path=path,
                count=10,
            )
            assert written == 10
            with gzip.open(path, "rt", encoding="utf-8") as f:
                content = f.read()
            assert "first_name" in content
        finally:
            os.unlink(path)


# ------------------------------------------------------------------
# CLI — null-fields, encoding, compression
# ------------------------------------------------------------------


class TestCLIPhase2:
    """Test CLI flags added in Phase 2."""

    def test_null_fields_flag(self) -> None:
        """--null-fields should produce some null values."""
        from dataforge.cli import main
        import io
        import sys

        old_stdout = sys.stdout
        sys.stdout = buf = io.StringIO()
        try:
            ret = main(
                [
                    "--seed",
                    "42",
                    "--count",
                    "50",
                    "--format",
                    "json",
                    "--null-fields",
                    "email:1.0",
                    "first_name",
                    "email",
                ]
            )
        finally:
            sys.stdout = old_stdout
        assert ret == 0
        data = json.loads(buf.getvalue())
        assert all(row["email"] is None for row in data)

    def test_null_fields_invalid_format(self) -> None:
        """--null-fields with bad format should error."""
        from dataforge.cli import main
        import io
        import sys

        old_stderr = sys.stderr
        sys.stderr = io.StringIO()
        try:
            ret = main(
                [
                    "--null-fields",
                    "badformat",
                    "first_name",
                ]
            )
        finally:
            sys.stderr = old_stderr
        assert ret == 1

    def test_null_fields_invalid_probability(self) -> None:
        """--null-fields with probability > 1 should error."""
        from dataforge.cli import main
        import io
        import sys

        old_stderr = sys.stderr
        sys.stderr = io.StringIO()
        try:
            ret = main(
                [
                    "--null-fields",
                    "email:2.0",
                    "first_name",
                    "email",
                ]
            )
        finally:
            sys.stderr = old_stderr
        assert ret == 1

    def test_compress_flag_stream(self) -> None:
        """--compress with --stream should produce gzip output."""
        from dataforge.cli import main

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            import io
            import sys

            old_stderr = sys.stderr
            sys.stderr = io.StringIO()
            try:
                ret = main(
                    [
                        "--seed",
                        "42",
                        "--count",
                        "10",
                        "--format",
                        "csv",
                        "--compress",
                        "--stream",
                        "-o",
                        path,
                        "first_name",
                        "email",
                    ]
                )
            finally:
                sys.stderr = old_stderr
            assert ret == 0
            with gzip.open(path, "rt", encoding="utf-8") as f:
                content = f.read()
            assert "first_name" in content
        finally:
            os.unlink(path)

    def test_compress_flag_non_stream(self) -> None:
        """--compress with -o should produce gzip output."""
        from dataforge.cli import main

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            ret = main(
                [
                    "--seed",
                    "42",
                    "--count",
                    "5",
                    "--format",
                    "csv",
                    "--compress",
                    "-o",
                    path,
                    "first_name",
                    "email",
                ]
            )
            assert ret == 0
            with gzip.open(path, "rt", encoding="utf-8") as f:
                content = f.read()
            assert "first_name" in content
        finally:
            os.unlink(path)

    def test_encoding_flag(self) -> None:
        """--encoding should be passed through to file output."""
        from dataforge.cli import main

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            ret = main(
                [
                    "--seed",
                    "42",
                    "--count",
                    "5",
                    "--format",
                    "csv",
                    "--encoding",
                    "latin-1",
                    "-o",
                    path,
                    "first_name",
                    "email",
                ]
            )
            assert ret == 0
            with open(path, encoding="latin-1") as f:
                content = f.read()
            assert "first_name" in content
        finally:
            os.unlink(path)


# ------------------------------------------------------------------
# Statistical distributions (from backend, added in Phase 1 but tested here)
# ------------------------------------------------------------------


class TestStatisticalDistributions:
    """Test statistical distribution methods on RandomEngine."""

    def setup_method(self) -> None:
        from dataforge.backend import RandomEngine

        self.engine = RandomEngine(seed=42)

    def test_gauss_range(self) -> None:
        """Gaussian values should be roughly within expected range."""
        values = [self.engine.gauss(mu=100, sigma=10) for _ in range(1000)]
        mean = sum(values) / len(values)
        assert 90 < mean < 110

    def test_gauss_int_clamped(self) -> None:
        """gauss_int should clamp to [min, max]."""
        values = [self.engine.gauss_int(50, 20, 0, 100) for _ in range(1000)]
        assert all(0 <= v <= 100 for v in values)

    def test_exponential_positive(self) -> None:
        """Exponential values should all be positive."""
        values = [self.engine.exponential(1.0) for _ in range(100)]
        assert all(v > 0 for v in values)

    def test_log_normal_positive(self) -> None:
        """Log-normal values should all be positive."""
        values = [self.engine.log_normal(0, 1) for _ in range(100)]
        assert all(v > 0 for v in values)

    def test_triangular_bounds(self) -> None:
        """Triangular values should be within [low, high]."""
        values = [self.engine.triangular(10.0, 20.0) for _ in range(100)]
        assert all(10.0 <= v <= 20.0 for v in values)

    def test_pareto_positive(self) -> None:
        """Pareto values should be positive."""
        values = [self.engine.pareto(2.0) for _ in range(100)]
        assert all(v > 0 for v in values)

    def test_beta_unit_interval(self) -> None:
        """Beta values should be in (0, 1)."""
        values = [self.engine.beta(2.0, 5.0) for _ in range(100)]
        assert all(0 < v < 1 for v in values)

    def test_gamma_positive(self) -> None:
        """Gamma values should be positive."""
        values = [self.engine.gamma(2.0, 1.0) for _ in range(100)]
        assert all(v > 0 for v in values)

    def test_zipf_bounds(self) -> None:
        """Zipf values should be in [1, n]."""
        values = [self.engine.zipf(1.5, 50) for _ in range(100)]
        assert all(1 <= v <= 50 for v in values)

    def test_vonmises_returns_float(self) -> None:
        """Von Mises should return a float."""
        val = self.engine.vonmises(0.0, 2.0)
        assert isinstance(val, float)
