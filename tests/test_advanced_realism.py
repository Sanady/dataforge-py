"""Tests for Phase 5 — Advanced Realism features.

Covers:
- ``realistic_blood_type()`` weighted distribution
- ``weighted_choice()`` / ``weighted_choices()`` engine methods
- Lambda / callable fields in Schema
"""

import os
import tempfile
from collections import Counter

from dataforge import DataForge


# ------------------------------------------------------------------ #
#  Weighted blood type distribution
# ------------------------------------------------------------------ #


class TestRealisticBloodType:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_scalar_returns_str(self) -> None:
        bt = self.forge.medical.realistic_blood_type()
        assert isinstance(bt, str)

    def test_scalar_valid_type(self) -> None:
        valid = {"A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"}
        for _ in range(200):
            assert self.forge.medical.realistic_blood_type() in valid

    def test_batch_returns_list(self) -> None:
        results = self.forge.medical.realistic_blood_type(count=50)
        assert isinstance(results, list)
        assert len(results) == 50

    def test_count_one_returns_str(self) -> None:
        result = self.forge.medical.realistic_blood_type(count=1)
        assert isinstance(result, str)

    def test_distribution_weighted(self) -> None:
        """O+ and A+ should dominate; AB- should be very rare."""
        results = self.forge.medical.realistic_blood_type(count=10_000)
        counts = Counter(results)
        # O+ (~37.4%) and A+ (~35.7%) together should be >50% of total
        dominant = counts.get("O+", 0) + counts.get("A+", 0)
        assert dominant > 5000, f"O+ + A+ = {dominant}, expected >5000"
        # AB- (~0.6%) should be <3% of total
        rare = counts.get("AB-", 0)
        assert rare < 300, f"AB- = {rare}, expected <300"

    def test_schema_field_resolution(self) -> None:
        """realistic_blood_type should be usable in Schema via field map."""
        rows = self.forge.to_dict(
            fields=["realistic_blood_type"],
            count=10,
        )
        assert len(rows) == 10
        valid = {"A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"}
        for row in rows:
            assert row["realistic_blood_type"] in valid

    def test_reproducible_with_seed(self) -> None:
        f1 = DataForge(seed=99)
        f2 = DataForge(seed=99)
        r1 = f1.medical.realistic_blood_type(count=20)
        r2 = f2.medical.realistic_blood_type(count=20)
        assert r1 == r2


# ------------------------------------------------------------------ #
#  Weighted engine methods
# ------------------------------------------------------------------ #


class TestWeightedEngine:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_weighted_choice_returns_element(self) -> None:
        data = ("a", "b", "c")
        weights = (0.8, 0.1, 0.1)
        result = self.forge._engine.weighted_choice(data, weights)
        assert result in data

    def test_weighted_choices_returns_list(self) -> None:
        data = ("a", "b", "c")
        weights = (0.8, 0.1, 0.1)
        results = self.forge._engine.weighted_choices(data, weights, 50)
        assert isinstance(results, list)
        assert len(results) == 50
        assert all(r in data for r in results)

    def test_weighted_choice_respects_weights(self) -> None:
        data = ("heavy", "light")
        weights = (0.99, 0.01)
        results = [
            self.forge._engine.weighted_choice(data, weights) for _ in range(1000)
        ]
        heavy_count = results.count("heavy")
        assert heavy_count > 900, f"heavy={heavy_count}, expected >900"


# ------------------------------------------------------------------ #
#  Lambda / callable fields in Schema
# ------------------------------------------------------------------ #


class TestSchemaLambdaFields:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_lambda_basic(self) -> None:
        """Lambda can transform a previously generated column."""
        schema = self.forge.schema(
            {
                "name": "first_name",
                "upper_name": lambda row: row["name"].upper(),
            }
        )
        rows = schema.generate(count=5)
        assert len(rows) == 5
        for row in rows:
            assert row["upper_name"] == row["name"].upper()

    def test_lambda_multiple(self) -> None:
        """Multiple lambdas can reference each other in order."""
        schema = self.forge.schema(
            {
                "first": "first_name",
                "last": "last_name",
                "full": lambda row: f"{row['first']} {row['last']}",
                "email_like": lambda row: (
                    row["full"].lower().replace(" ", ".") + "@test.com"
                ),
            }
        )
        rows = schema.generate(count=3)
        for row in rows:
            expected_full = f"{row['first']} {row['last']}"
            assert row["full"] == expected_full
            expected_email = expected_full.lower().replace(" ", ".") + "@test.com"
            assert row["email_like"] == expected_email

    def test_lambda_count_one(self) -> None:
        """count=1 should still work with lambdas."""
        schema = self.forge.schema(
            {
                "city": "city",
                "label": lambda row: f"City: {row['city']}",
            }
        )
        rows = schema.generate(count=1)
        assert len(rows) == 1
        assert rows[0]["label"] == f"City: {rows[0]['city']}"

    def test_lambda_stream(self) -> None:
        """Lambdas should work via stream() as well."""
        schema = self.forge.schema(
            {
                "name": "first_name",
                "upper": lambda row: row["name"].upper(),
            }
        )
        rows = list(schema.stream(count=10, batch_size=3))
        assert len(rows) == 10
        for row in rows:
            assert row["upper"] == row["name"].upper()

    def test_lambda_generate_empty(self) -> None:
        """count=0 should return empty list even with lambdas."""
        schema = self.forge.schema(
            {
                "name": "first_name",
                "upper": lambda row: row["name"].upper(),
            }
        )
        assert schema.generate(count=0) == []

    def test_lambda_no_lambdas_unchanged(self) -> None:
        """Schemas without lambdas should behave identically to before."""
        s1 = self.forge.schema(["first_name", "email"])
        f2 = DataForge(locale="en_US", seed=42)
        s2 = f2.schema({"first_name": "first_name", "email": "email"})
        rows1 = s1.generate(count=5)
        rows2 = s2.generate(count=5)
        assert rows1 == rows2

    def test_lambda_to_csv(self) -> None:
        """Lambdas should work in to_csv()."""
        schema = self.forge.schema(
            {
                "name": "first_name",
                "upper": lambda row: row["name"].upper(),
            }
        )
        csv_str = schema.to_csv(count=3)
        lines = csv_str.strip().splitlines()
        assert len(lines) == 4  # header + 3 rows
        assert lines[0].strip() == "name,upper"

    def test_lambda_to_jsonl(self) -> None:
        """Lambdas should work in to_jsonl()."""
        import json

        schema = self.forge.schema(
            {
                "name": "first_name",
                "upper": lambda row: row["name"].upper(),
            }
        )
        jsonl = schema.to_jsonl(count=3)
        lines = [json.loads(line) for line in jsonl.strip().split("\n")]
        assert len(lines) == 3
        for row in lines:
            assert row["upper"] == row["name"].upper()

    def test_lambda_stream_to_csv(self) -> None:
        """Lambdas should work in stream_to_csv()."""
        schema = self.forge.schema(
            {
                "name": "first_name",
                "upper": lambda row: row["name"].upper(),
            }
        )
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            written = schema.stream_to_csv(path, count=10, batch_size=3)
            assert written == 10
            with open(path, encoding="utf-8") as f:
                lines = f.read().strip().split("\n")
            assert len(lines) == 11  # header + 10 rows
        finally:
            os.unlink(path)

    def test_lambda_stream_to_jsonl(self) -> None:
        """Lambdas should work in stream_to_jsonl()."""
        import json

        schema = self.forge.schema(
            {
                "name": "first_name",
                "upper": lambda row: row["name"].upper(),
            }
        )
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            path = f.name
        try:
            written = schema.stream_to_jsonl(path, count=10, batch_size=3)
            assert written == 10
            with open(path, encoding="utf-8") as f:
                lines = [json.loads(line) for line in f.read().strip().split("\n")]
            for row in lines:
                assert row["upper"] == row["name"].upper()
        finally:
            os.unlink(path)

    def test_lambda_non_string_return(self) -> None:
        """Lambda returning non-string preserves native type."""
        schema = self.forge.schema(
            {
                "name": "first_name",
                "name_len": lambda row: len(row["name"]),
            }
        )
        rows = schema.generate(count=3)
        for row in rows:
            assert row["name_len"] == len(row["name"])
            assert isinstance(row["name_len"], int)

    def test_lambda_repr(self) -> None:
        """Schema repr should list all columns including lambda ones."""
        schema = self.forge.schema(
            {
                "name": "first_name",
                "upper": lambda row: row["name"].upper(),
            }
        )
        r = repr(schema)
        assert "name" in r
        assert "upper" in r
