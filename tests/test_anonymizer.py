"""Tests for data anonymization — deterministic PII replacement."""

from __future__ import annotations

import csv
import os
import tempfile

import pytest

from dataforge import DataForge
from dataforge.anonymizer import Anonymizer


# Fixtures


@pytest.fixture
def forge() -> DataForge:
    return DataForge(locale="en_US", seed=42)


@pytest.fixture
def anon(forge: DataForge) -> Anonymizer:
    return Anonymizer(forge, secret="test-secret")


@pytest.fixture
def sample_rows() -> list[dict]:
    return [
        {"name": "Alice Smith", "email": "alice@real.com", "age": 30},
        {"name": "Bob Jones", "email": "bob@real.com", "age": 25},
        {"name": "Carol White", "email": "carol@real.com", "age": 35},
    ]


# Construction


class TestAnonymizerConstruction:
    def test_default_secret(self, forge: DataForge) -> None:
        anon = Anonymizer(forge)
        assert anon._secret == b"dataforge-anonymizer"

    def test_custom_secret(self, forge: DataForge) -> None:
        anon = Anonymizer(forge, secret="my-secret")
        assert anon._secret == b"my-secret"

    def test_repr_empty_cache(self, anon: Anonymizer) -> None:
        r = repr(anon)
        assert "Anonymizer" in r
        assert "cached_mappings=0" in r

    def test_slots(self, anon: Anonymizer) -> None:
        with pytest.raises(AttributeError):
            anon.nonexistent = True  # type: ignore[attr-defined]


# Deterministic seed derivation


class TestSeedDerivation:
    def test_same_input_same_seed(self, anon: Anonymizer) -> None:
        s1 = anon._derive_seed("email", "alice@test.com")
        s2 = anon._derive_seed("email", "alice@test.com")
        assert s1 == s2

    def test_different_values_different_seeds(self, anon: Anonymizer) -> None:
        s1 = anon._derive_seed("email", "alice@test.com")
        s2 = anon._derive_seed("email", "bob@test.com")
        assert s1 != s2

    def test_different_fields_different_seeds(self, anon: Anonymizer) -> None:
        s1 = anon._derive_seed("email", "alice@test.com")
        s2 = anon._derive_seed("name", "alice@test.com")
        assert s1 != s2


# Anonymize rows


class TestAnonymizeRows:
    def test_basic_anonymization(
        self, anon: Anonymizer, sample_rows: list[dict]
    ) -> None:
        result = anon.anonymize(
            sample_rows, fields={"name": "full_name", "email": "email"}
        )
        assert len(result) == 3
        # Values should differ from originals
        for orig, anon_row in zip(sample_rows, result):
            assert anon_row["name"] != orig["name"]
            assert anon_row["email"] != orig["email"]

    def test_unmapped_fields_pass_through(
        self, anon: Anonymizer, sample_rows: list[dict]
    ) -> None:
        result = anon.anonymize(sample_rows, fields={"name": "full_name"})
        # 'age' not in fields mapping, should pass through unchanged
        for orig, anon_row in zip(sample_rows, result):
            assert anon_row["age"] == orig["age"]

    def test_does_not_mutate_input(
        self, anon: Anonymizer, sample_rows: list[dict]
    ) -> None:
        originals = [dict(row) for row in sample_rows]
        anon.anonymize(sample_rows, fields={"name": "full_name"})
        assert sample_rows == originals

    def test_none_values_not_anonymized(self, anon: Anonymizer) -> None:
        rows = [{"name": None, "email": "test@test.com"}]
        result = anon.anonymize(rows, fields={"name": "full_name", "email": "email"})
        assert result[0]["name"] is None
        assert result[0]["email"] != "test@test.com"

    def test_deterministic_across_calls(
        self, forge: DataForge, sample_rows: list[dict]
    ) -> None:
        """Same secret + same input = same output across instances."""
        anon1 = Anonymizer(forge, secret="same-key")
        anon2 = Anonymizer(forge, secret="same-key")
        fields = {"name": "full_name", "email": "email"}
        r1 = anon1.anonymize(sample_rows, fields=fields)
        r2 = anon2.anonymize(sample_rows, fields=fields)
        for a, b in zip(r1, r2):
            assert a["name"] == b["name"]
            assert a["email"] == b["email"]

    def test_different_secrets_different_output(
        self, forge: DataForge, sample_rows: list[dict]
    ) -> None:
        anon1 = Anonymizer(forge, secret="secret-a")
        anon2 = Anonymizer(forge, secret="secret-b")
        fields = {"name": "full_name"}
        r1 = anon1.anonymize(sample_rows, fields=fields)
        r2 = anon2.anonymize(sample_rows, fields=fields)
        # At least one row should differ
        assert any(a["name"] != b["name"] for a, b in zip(r1, r2))

    def test_same_value_same_fake(self, anon: Anonymizer) -> None:
        rows = [
            {"name": "Alice Smith"},
            {"name": "Alice Smith"},
            {"name": "Bob Jones"},
        ]
        result = anon.anonymize(rows, fields={"name": "full_name"})
        assert result[0]["name"] == result[1]["name"]
        assert result[0]["name"] != result[2]["name"]


# Cache management


class TestCache:
    def test_cache_populated(self, anon: Anonymizer, sample_rows: list[dict]) -> None:
        anon.anonymize(sample_rows, fields={"name": "full_name"})
        assert len(anon._cache) > 0

    def test_clear_cache(self, anon: Anonymizer, sample_rows: list[dict]) -> None:
        anon.anonymize(sample_rows, fields={"name": "full_name"})
        anon.clear_cache()
        assert len(anon._cache) == 0

    def test_repr_reflects_cache(
        self, anon: Anonymizer, sample_rows: list[dict]
    ) -> None:
        anon.anonymize(sample_rows, fields={"name": "full_name"})
        r = repr(anon)
        assert "cached_mappings=" in r
        # Cache should have entries for distinct names
        assert "cached_mappings=0" not in r


# Format-preserving anonymization


class TestFormatPreserving:
    def test_email_has_at_sign(self, anon: Anonymizer) -> None:
        rows = [{"email": "alice@example.com"}]
        result = anon.anonymize(rows, fields={"email": "email"})
        assert "@" in result[0]["email"]

    def test_phone_format_preservation(self, anon: Anonymizer) -> None:
        # _format_preserve_phone should keep separators
        result = Anonymizer._format_preserve_phone("1234567890", "(123) 456-7890")
        # Should preserve the parentheses, spaces, dash pattern
        assert "(" in result
        assert ")" in result
        assert "-" in result


# CSV anonymization


class TestAnonymizeCSV:
    def test_csv_anonymization(self, anon: Anonymizer) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(f, fieldnames=["name", "email", "city"])
            writer.writeheader()
            writer.writerow({"name": "Alice", "email": "alice@test.com", "city": "NYC"})
            writer.writerow({"name": "Bob", "email": "bob@test.com", "city": "LA"})
            input_path = f.name

        output_path = input_path + ".anon.csv"
        try:
            count = anon.anonymize_csv(
                input_path, output_path, fields={"name": "full_name", "email": "email"}
            )
            assert count == 2

            # Read back and verify
            with open(output_path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            assert len(rows) == 2
            assert rows[0]["name"] != "Alice"
            assert rows[0]["email"] != "alice@test.com"
            # City should pass through
            assert rows[0]["city"] == "NYC"
        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_csv_batch_processing(self, anon: Anonymizer) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(f, fieldnames=["name"])
            writer.writeheader()
            for i in range(15):
                writer.writerow({"name": f"Person {i}"})
            input_path = f.name

        output_path = input_path + ".anon.csv"
        try:
            count = anon.anonymize_csv(
                input_path, output_path, fields={"name": "full_name"}, batch_size=5
            )
            assert count == 15
        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)
