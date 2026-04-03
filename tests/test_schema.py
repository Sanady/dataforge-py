"""Tests for the Schema class."""

import json

from dataforge import DataForge, Schema


class TestSchemaImport:
    def test_schema_importable(self) -> None:
        assert Schema is not None

    def test_schema_created_via_forge(self) -> None:
        forge = DataForge(locale="en_US", seed=42)
        schema = forge.schema(["first_name", "email"])
        assert isinstance(schema, Schema)


class TestSchemaGenerate:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_generate_returns_list_of_dicts(self) -> None:
        schema = self.forge.schema(["first_name", "email"])
        result = schema.generate(count=5)
        assert isinstance(result, list)
        assert len(result) == 5
        for row in result:
            assert isinstance(row, dict)
            assert "first_name" in row
            assert "email" in row

    def test_generate_with_dict_fields(self) -> None:
        schema = self.forge.schema({"Name": "full_name"})
        result = schema.generate(count=5)
        assert isinstance(result, list)
        assert len(result) == 5
        for row in result:
            assert isinstance(row, dict)
            assert "Name" in row
            assert isinstance(row["Name"], str)
            assert len(row["Name"]) > 0

    def test_generate_count_zero(self) -> None:
        schema = self.forge.schema(["first_name", "email"])
        result = schema.generate(count=0)
        assert result == []


class TestSchemaStream:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_stream_returns_correct_count(self) -> None:
        schema = self.forge.schema(["first_name"])
        result = list(schema.stream(count=5))
        assert len(result) == 5
        for row in result:
            assert isinstance(row, dict)
            assert "first_name" in row

    def test_stream_is_lazy(self) -> None:
        schema = self.forge.schema(["first_name"])
        stream = schema.stream(count=10)
        # It should be an iterator, not a list
        assert hasattr(stream, "__next__")


class TestSchemaCsv:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_csv_returns_string(self) -> None:
        schema = self.forge.schema(["first_name", "email"])
        result = schema.to_csv(count=3)
        assert isinstance(result, str)

    def test_csv_has_header(self) -> None:
        schema = self.forge.schema(["first_name", "email"])
        result = schema.to_csv(count=3)
        lines = result.strip().split("\n")
        # Header + 3 data rows
        assert len(lines) == 4
        header = lines[0]
        assert "first_name" in header
        assert "email" in header

    def test_csv_with_dict_fields(self) -> None:
        schema = self.forge.schema({"Name": "full_name", "Email": "email"})
        result = schema.to_csv(count=2)
        lines = result.strip().split("\n")
        assert len(lines) == 3
        header = lines[0]
        assert "Name" in header
        assert "Email" in header


class TestSchemaJsonl:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_jsonl_returns_string(self) -> None:
        schema = self.forge.schema(["first_name", "email"])
        result = schema.to_jsonl(count=3)
        assert isinstance(result, str)

    def test_jsonl_each_line_is_valid_json(self) -> None:
        schema = self.forge.schema(["first_name", "email"])
        result = schema.to_jsonl(count=3)
        lines = result.strip().split("\n")
        assert len(lines) == 3
        for line in lines:
            parsed = json.loads(line)
            assert isinstance(parsed, dict)
            assert "first_name" in parsed
            assert "email" in parsed


class TestSchemaSql:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_sql_returns_insert_statements(self) -> None:
        schema = self.forge.schema(["first_name", "email"])
        result = schema.to_sql("users", count=3)
        assert isinstance(result, str)
        lines = result.strip().split("\n")
        # Multi-row INSERT: 1 prefix line + 3 value lines = 4 lines
        assert lines[0].startswith("INSERT INTO")
        assert "users" in lines[0]
        value_lines = [ln for ln in lines if ln.strip().startswith("(")]
        assert len(value_lines) == 3
        assert lines[-1].endswith(";")

    def test_sql_mysql_uses_backticks(self) -> None:
        schema = self.forge.schema(["first_name"])
        result = schema.to_sql("users", count=1, dialect="mysql")
        assert "`users`" in result
        assert "`first_name`" in result

    def test_sql_postgresql_uses_double_quotes(self) -> None:
        schema = self.forge.schema(["first_name"])
        result = schema.to_sql("users", count=1, dialect="postgresql")
        assert '"users"' in result
        assert '"first_name"' in result


class TestSchemaRepr:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_repr_contains_column_names(self) -> None:
        schema = self.forge.schema(["first_name", "email"])
        r = repr(schema)
        assert "first_name" in r
        assert "email" in r

    def test_repr_with_dict_fields(self) -> None:
        schema = self.forge.schema({"Name": "full_name", "Email": "email"})
        r = repr(schema)
        assert "Name" in r
        assert "Email" in r
