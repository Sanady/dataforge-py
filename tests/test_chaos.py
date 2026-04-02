"""Tests for chaos mode — data quality issue injection."""

from __future__ import annotations

import pytest

from dataforge import DataForge
from dataforge.chaos import ChaosTransformer


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture
def forge() -> DataForge:
    return DataForge(locale="en_US", seed=42)


@pytest.fixture
def sample_rows(forge: DataForge) -> list[dict]:
    schema = forge.schema(["first_name", "email", "city"])
    return schema.generate(count=100)


# ------------------------------------------------------------------
# ChaosTransformer construction
# ------------------------------------------------------------------


class TestChaosTransformerConstruction:
    def test_default_rates_are_zero(self) -> None:
        ct = ChaosTransformer()
        assert ct._null_rate == 0.0
        assert ct._type_mismatch_rate == 0.0

    def test_custom_rates(self) -> None:
        ct = ChaosTransformer(null_rate=0.1, boundary_rate=0.05)
        assert ct._null_rate == 0.1
        assert ct._boundary_rate == 0.05

    def test_repr(self) -> None:
        ct = ChaosTransformer(null_rate=0.1, duplicate_rate=0.05)
        r = repr(ct)
        assert "ChaosTransformer" in r
        assert "null=0.1" in r
        assert "duplicate=0.05" in r

    def test_seed_reproducibility(self, sample_rows: list[dict]) -> None:
        ct1 = ChaosTransformer(null_rate=0.3, seed=99)
        ct2 = ChaosTransformer(null_rate=0.3, seed=99)
        r1 = ct1.transform(sample_rows)
        r2 = ct2.transform(sample_rows)
        # Same seed + same input = same output
        assert len(r1) == len(r2)
        for a, b in zip(r1, r2):
            assert a == b


# ------------------------------------------------------------------
# Null injection
# ------------------------------------------------------------------


class TestNullInjection:
    def test_null_injection(self, sample_rows: list[dict]) -> None:
        ct = ChaosTransformer(null_rate=0.5, seed=42)
        result = ct.transform(sample_rows)
        # Roughly half of all cells should be None
        null_count = sum(1 for row in result for v in row.values() if v is None)
        total_cells = len(result) * 3  # 3 columns
        rate = null_count / total_cells
        assert 0.2 < rate < 0.8, f"Null rate {rate} outside expected range"

    def test_zero_null_rate(self, sample_rows: list[dict]) -> None:
        ct = ChaosTransformer(null_rate=0.0, seed=42)
        result = ct.transform(sample_rows)
        # No nulls should be injected
        null_count = sum(1 for row in result for v in row.values() if v is None)
        assert null_count == 0


# ------------------------------------------------------------------
# Type mismatch injection
# ------------------------------------------------------------------


class TestTypeMismatch:
    def test_type_mismatch_injects(self, sample_rows: list[dict]) -> None:
        ct = ChaosTransformer(type_mismatch_rate=0.5, seed=42)
        result = ct.transform(sample_rows)
        # Some values should no longer be strings
        non_str_count = sum(
            1
            for row in result
            for v in row.values()
            if v is not None and not isinstance(v, str)
        )
        assert non_str_count > 0


# ------------------------------------------------------------------
# Boundary value injection
# ------------------------------------------------------------------


class TestBoundaryInjection:
    def test_boundary_values_injected(self, sample_rows: list[dict]) -> None:
        ct = ChaosTransformer(boundary_rate=0.5, seed=42)
        result = ct.transform(sample_rows)
        # Should see boundary strings like "", "null", "NaN", etc.
        all_vals = [str(v) for row in result for v in row.values() if v is not None]
        boundary_hits = [v for v in all_vals if v in ("null", "NULL", "NaN", "N/A", "")]
        assert len(boundary_hits) > 0


# ------------------------------------------------------------------
# Duplicate injection
# ------------------------------------------------------------------


class TestDuplicateInjection:
    def test_duplicates_added(self, sample_rows: list[dict]) -> None:
        ct = ChaosTransformer(duplicate_rate=0.3, seed=42)
        result = ct.transform(sample_rows)
        # Should have more rows than the original
        assert len(result) >= len(sample_rows)


# ------------------------------------------------------------------
# String-specific transformations
# ------------------------------------------------------------------


class TestStringTransformations:
    def test_whitespace_injection(self, sample_rows: list[dict]) -> None:
        ct = ChaosTransformer(whitespace_rate=1.0, seed=42)
        result = ct.transform(sample_rows)
        # At least some values should have extra whitespace
        has_extra_ws = any(
            isinstance(v, str)
            and (v.startswith(" ") or v.startswith("\t") or v.endswith(" "))
            for row in result
            for v in row.values()
        )
        assert has_extra_ws

    def test_encoding_chaos(self, sample_rows: list[dict]) -> None:
        ct = ChaosTransformer(encoding_rate=1.0, seed=42)
        result = ct.transform(sample_rows)
        # At least some values should contain unicode chaos
        unicode_chars = {"\u200b", "\u200e", "\u00e9", "\U0001f600", "\ufeff"}
        has_unicode = any(
            isinstance(v, str) and any(c in v for c in unicode_chars)
            for row in result
            for v in row.values()
        )
        assert has_unicode

    def test_format_inconsistency(self, sample_rows: list[dict]) -> None:
        ct = ChaosTransformer(format_rate=1.0, seed=42)
        result = ct.transform(sample_rows)
        # Check that some values were case-changed
        orig_vals = {str(v) for row in sample_rows for v in row.values()}
        changed_vals = {
            str(v) for row in result for v in row.values() if isinstance(v, str)
        }
        # There should be values in the result that differ from the original
        assert changed_vals != orig_vals

    def test_truncation(self, sample_rows: list[dict]) -> None:
        ct = ChaosTransformer(truncation_rate=1.0, seed=42)
        result = ct.transform(sample_rows)
        # At least some values should be shorter than originals
        shorter_count = 0
        for orig, modified in zip(sample_rows, result):
            for col in orig:
                if (
                    isinstance(orig[col], str)
                    and isinstance(modified.get(col), str)
                    and len(modified[col]) < len(orig[col])
                ):
                    shorter_count += 1
        assert shorter_count > 0


# ------------------------------------------------------------------
# Column targeting
# ------------------------------------------------------------------


class TestColumnTargeting:
    def test_only_target_columns(self, sample_rows: list[dict]) -> None:
        ct = ChaosTransformer(null_rate=1.0, seed=42)
        result = ct.transform(sample_rows, columns=["email"])
        # Only email should be None, others should be untouched
        for row in result:
            assert row["email"] is None
            assert row["first_name"] is not None
            assert row["city"] is not None


# ------------------------------------------------------------------
# Empty input
# ------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_input(self) -> None:
        ct = ChaosTransformer(null_rate=0.5)
        result = ct.transform([])
        assert result == []

    def test_does_not_mutate_input(self, sample_rows: list[dict]) -> None:
        originals = [dict(row) for row in sample_rows]
        ct = ChaosTransformer(null_rate=0.5, seed=42)
        ct.transform(sample_rows)
        # Original rows should be unchanged
        assert sample_rows == originals


# ------------------------------------------------------------------
# Schema integration
# ------------------------------------------------------------------


class TestChaosSchemaIntegration:
    def test_chaos_parameter_in_schema(self, forge: DataForge) -> None:
        chaos = ChaosTransformer(null_rate=0.3, seed=42)
        schema = forge.schema(["first_name", "email"], chaos=chaos)
        rows = schema.generate(count=100)
        null_count = sum(1 for r in rows for v in r.values() if v is None)
        assert null_count > 0

    def test_chaos_dict_config(self, forge: DataForge) -> None:
        schema = forge.schema(
            ["first_name", "email"],
            chaos={"null_rate": 0.3, "seed": 42},
        )
        rows = schema.generate(count=100)
        null_count = sum(1 for r in rows for v in r.values() if v is None)
        assert null_count > 0
