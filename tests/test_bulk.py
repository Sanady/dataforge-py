"""Tests for bulk data generation (to_dict, to_csv, to_dataframe)."""

import csv
import io
import os
import tempfile

import pytest

from dataforge import DataForge


class TestToDict:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_list_of_dicts(self) -> None:
        rows = self.forge.to_dict(["first_name", "email"], count=5)
        assert isinstance(rows, list)
        assert len(rows) == 5
        assert all(isinstance(r, dict) for r in rows)

    def test_field_names_as_keys(self) -> None:
        rows = self.forge.to_dict(["first_name", "email"], count=3)
        for row in rows:
            assert "first_name" in row
            assert "email" in row

    def test_dict_field_mapping(self) -> None:
        rows = self.forge.to_dict({"Name": "full_name", "Email": "email"}, count=3)
        for row in rows:
            assert "Name" in row
            assert "Email" in row

    def test_dotted_notation(self) -> None:
        rows = self.forge.to_dict(["person.first_name", "address.city"], count=3)
        for row in rows:
            assert "person.first_name" in row
            assert "address.city" in row

    def test_count_zero(self) -> None:
        rows = self.forge.to_dict(["first_name"], count=0)
        assert rows == []

    def test_values_are_strings(self) -> None:
        rows = self.forge.to_dict(["first_name", "email", "city"], count=10)
        for row in rows:
            assert all(isinstance(v, str) for v in row.values())

    def test_unknown_field_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown field"):
            self.forge.to_dict(["nonexistent_field"], count=1)

    def test_reproducible_with_seed(self) -> None:
        forge1 = DataForge(seed=99)
        forge2 = DataForge(seed=99)
        rows1 = forge1.to_dict(["first_name", "email"], count=5)
        rows2 = forge2.to_dict(["first_name", "email"], count=5)
        assert rows1 == rows2

    def test_all_provider_fields(self) -> None:
        fields = [
            "first_name",
            "last_name",
            "email",
            "city",
            "company",
            "phone",
            "word",
            "date",
            "card_type",
            "color",
            "file_name",
        ]
        rows = self.forge.to_dict(fields, count=3)
        assert len(rows) == 3
        for row in rows:
            assert len(row) == len(fields)


class TestToCsv:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_string(self) -> None:
        result = self.forge.to_csv(["first_name", "email"], count=3)
        assert isinstance(result, str)

    def test_csv_has_header_and_rows(self) -> None:
        result = self.forge.to_csv(["first_name", "email"], count=5)
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert len(rows) == 5
        assert "first_name" in rows[0]
        assert "email" in rows[0]

    def test_csv_with_dict_fields(self) -> None:
        result = self.forge.to_csv({"Name": "full_name", "Phone": "phone"}, count=3)
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert len(rows) == 3
        assert "Name" in rows[0]
        assert "Phone" in rows[0]

    def test_csv_write_to_file(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            result = self.forge.to_csv(["first_name", "email"], count=5, path=tmp_path)
            # Should also return the string
            assert isinstance(result, str)
            assert len(result) > 0

            # File should exist and have content
            with open(tmp_path, "r", newline="") as f:
                content = f.read()
            assert content == result
        finally:
            os.unlink(tmp_path)

    def test_csv_empty_count(self) -> None:
        result = self.forge.to_csv(["first_name"], count=0)
        assert result == ""


class TestToJsonl:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_string(self) -> None:
        result = self.forge.to_jsonl(["first_name", "email"], count=3)
        assert isinstance(result, str)

    def test_jsonl_has_correct_lines(self) -> None:
        import json

        result = self.forge.to_jsonl(["first_name", "email"], count=5)
        lines = result.strip().split("\n")
        assert len(lines) == 5
        for line in lines:
            row = json.loads(line)
            assert "first_name" in row
            assert "email" in row

    def test_jsonl_empty_count(self) -> None:
        result = self.forge.to_jsonl(["first_name"], count=0)
        assert result == ""

    def test_jsonl_write_to_file(self) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False
        ) as tmp:
            tmp_path = tmp.name
        try:
            result = self.forge.to_jsonl(["first_name"], count=3, path=tmp_path)
            with open(tmp_path, "r") as f:
                content = f.read()
            assert content == result
        finally:
            os.unlink(tmp_path)


class TestToSql:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_string(self) -> None:
        result = self.forge.to_sql(["first_name", "email"], table="users", count=3)
        assert isinstance(result, str)

    def test_sql_insert_count(self) -> None:
        result = self.forge.to_sql(["first_name"], table="users", count=5)
        # Multi-row INSERT: 1 INSERT INTO prefix + 5 value lines = 6 lines
        lines = result.strip().split("\n")
        assert lines[0].startswith("INSERT INTO")
        # Count value tuples (lines containing parenthesized values)
        value_lines = [ln for ln in lines if ln.strip().startswith("(")]
        assert len(value_lines) == 5

    def test_sql_empty_count(self) -> None:
        result = self.forge.to_sql(["first_name"], table="users", count=0)
        assert result == ""

    def test_sql_mysql_dialect(self) -> None:
        result = self.forge.to_sql(
            ["first_name"], table="users", count=1, dialect="mysql"
        )
        assert "`users`" in result
        assert "`first_name`" in result

    def test_sql_postgresql_dialect(self) -> None:
        result = self.forge.to_sql(
            ["first_name"], table="users", count=1, dialect="postgresql"
        )
        assert '"users"' in result
        assert '"first_name"' in result


class TestCopy:
    def test_copy_returns_new_instance(self) -> None:
        forge = DataForge(locale="en_US", seed=42)
        copy = forge.copy()
        assert copy is not forge
        assert copy.locale == forge.locale

    def test_copy_with_seed(self) -> None:
        forge = DataForge(locale="en_US", seed=42)
        copy = forge.copy(seed=99)
        assert copy.locale == "en_US"
        name = copy.person.first_name()
        assert isinstance(name, str)


class TestToDataframe:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_requires_pandas(self) -> None:
        try:
            import pandas  # noqa: F401

            df = self.forge.to_dataframe(["first_name", "email"], count=5)
            assert len(df) == 5
            assert "first_name" in df.columns
            assert "email" in df.columns
        except ModuleNotFoundError:
            with pytest.raises(ModuleNotFoundError, match="pandas"):
                self.forge.to_dataframe(["first_name", "email"], count=5)
