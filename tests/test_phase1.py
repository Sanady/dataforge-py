"""Tests for Phase 1 features: native types, type-aware ORM mapping, CLI enhancements."""

import csv
import io
import json

import pytest

from dataforge import DataForge, __version__


# Native type preservation


class TestNativeTypes:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_port_returns_int(self) -> None:
        schema = self.forge.schema(["port"])
        rows = schema.generate(count=5)
        for row in rows:
            assert isinstance(row["port"], int)

    def test_boolean_returns_bool(self) -> None:
        schema = self.forge.schema(["boolean"])
        rows = schema.generate(count=10)
        for row in rows:
            assert isinstance(row["boolean"], bool)

    def test_first_name_returns_str(self) -> None:
        schema = self.forge.schema(["first_name"])
        rows = schema.generate(count=5)
        for row in rows:
            assert isinstance(row["first_name"], str)

    def test_to_dict_preserves_int(self) -> None:
        rows = self.forge.to_dict(["port", "first_name"], count=3)
        for row in rows:
            assert isinstance(row["port"], int)
            assert isinstance(row["first_name"], str)

    def test_to_dict_preserves_bool(self) -> None:
        rows = self.forge.to_dict(["boolean"], count=3)
        for row in rows:
            assert isinstance(row["boolean"], bool)

    def test_mixed_types_in_same_schema(self) -> None:
        rows = self.forge.to_dict(["first_name", "port", "boolean"], count=5)
        for row in rows:
            assert isinstance(row["first_name"], str)
            assert isinstance(row["port"], int)
            assert isinstance(row["boolean"], bool)

    def test_json_output_preserves_types(self) -> None:
        schema = self.forge.schema(["port", "boolean", "first_name"])
        json_str = schema.to_json(count=3)
        data = json.loads(json_str)
        for row in data:
            assert isinstance(row["port"], int)
            assert isinstance(row["boolean"], bool)
            assert isinstance(row["first_name"], str)

    def test_jsonl_output_preserves_types(self) -> None:
        schema = self.forge.schema(["port", "boolean", "first_name"])
        jsonl_str = schema.to_jsonl(count=3)
        for line in jsonl_str.strip().split("\n"):
            row = json.loads(line)
            assert isinstance(row["port"], int)
            assert isinstance(row["boolean"], bool)
            assert isinstance(row["first_name"], str)

    def test_csv_stringifies(self) -> None:
        schema = self.forge.schema(["port", "boolean"])
        csv_str = schema.to_csv(count=3)
        reader = csv.DictReader(io.StringIO(csv_str))
        for row in reader:
            # CSV values are always strings
            assert isinstance(row["port"], str)
            assert isinstance(row["boolean"], str)

    def test_sql_handles_native_types(self) -> None:
        schema = self.forge.schema(["port", "first_name", "boolean"])
        sql = schema.to_sql("test_table", count=3)
        assert "INSERT INTO" in sql
        assert "test_table" in sql
        # Port values should appear as numbers in SQL quotes
        assert ";" in sql

    def test_generate_count_one_preserves_types(self) -> None:
        schema = self.forge.schema(["port", "boolean"])
        rows = schema.generate(count=1)
        assert len(rows) == 1
        assert isinstance(rows[0]["port"], int)
        assert isinstance(rows[0]["boolean"], bool)


# CSV delimiter support


class TestSchemaToJson:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_json_array(self) -> None:
        schema = self.forge.schema(["first_name", "email"])
        result = schema.to_json(count=3)
        data = json.loads(result)
        assert isinstance(data, list)
        assert len(data) == 3

    def test_json_indentation(self) -> None:
        schema = self.forge.schema(["first_name"])
        result = schema.to_json(count=1, indent=4)
        assert "    " in result

    def test_json_writes_to_file(self, tmp_path) -> None:
        schema = self.forge.schema(["first_name", "email"])
        path = str(tmp_path / "data.json")
        schema.to_json(count=5, path=path)
        with open(path) as f:
            data = json.load(f)
        assert len(data) == 5
        assert "first_name" in data[0]


# CSV delimiter support


class TestCsvDelimiter:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_default_comma_delimiter(self) -> None:
        schema = self.forge.schema(["first_name", "email"])
        result = schema.to_csv(count=3)
        lines = result.strip().split("\n")
        assert "," in lines[0]

    def test_pipe_delimiter(self) -> None:
        schema = self.forge.schema(["first_name", "email"])
        result = schema.to_csv(count=3, delimiter="|")
        lines = result.strip().split("\n")
        assert "|" in lines[0]

    def test_tab_delimiter(self) -> None:
        schema = self.forge.schema(["first_name", "email"])
        result = schema.to_csv(count=3, delimiter="\t")
        lines = result.strip().split("\n")
        assert "\t" in lines[0]

    def test_stream_csv_with_delimiter(self, tmp_path) -> None:
        schema = self.forge.schema(["first_name", "email"])
        path = str(tmp_path / "data.tsv")
        schema.stream_to_csv(path=path, count=5, delimiter="\t")
        with open(path) as f:
            content = f.read()
        assert "\t" in content

    def test_core_to_csv_delimiter(self) -> None:
        result = self.forge.to_csv(["first_name", "email"], count=3, delimiter="|")
        lines = result.strip().split("\n")
        assert "|" in lines[0]

    def test_core_stream_to_csv_delimiter(self, tmp_path) -> None:
        path = str(tmp_path / "data.tsv")
        self.forge.stream_to_csv(
            ["first_name", "email"], path=path, count=5, delimiter="\t"
        )
        with open(path) as f:
            content = f.read()
        assert "\t" in content


# Introspection API: list_providers, list_fields


class TestCoreToJson:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_json_string(self) -> None:
        result = self.forge.to_json(["first_name", "email"], count=3)
        data = json.loads(result)
        assert isinstance(data, list)
        assert len(data) == 3

    def test_writes_to_file(self, tmp_path) -> None:
        path = str(tmp_path / "data.json")
        self.forge.to_json(["first_name", "email"], count=5, path=path)
        with open(path) as f:
            data = json.load(f)
        assert len(data) == 5


# Introspection API: list_providers, list_fields


class TestIntrospectionAPI:
    def test_list_providers_returns_sorted_list(self) -> None:
        providers = DataForge.list_providers()
        assert isinstance(providers, list)
        assert providers == sorted(providers)
        assert "person" in providers
        assert "address" in providers
        assert "internet" in providers
        assert len(providers) >= 27

    def test_list_fields_returns_dict(self) -> None:
        fields = DataForge.list_fields()
        assert isinstance(fields, dict)
        assert "first_name" in fields
        assert "email" in fields
        assert len(fields) > 100

    def test_list_fields_values_are_tuples(self) -> None:
        fields = DataForge.list_fields()
        for key, value in fields.items():
            assert isinstance(value, tuple)
            assert len(value) == 2
            provider_name, method_name = value
            assert isinstance(provider_name, str)
            assert isinstance(method_name, str)

    def test_list_fields_is_sorted(self) -> None:
        fields = DataForge.list_fields()
        keys = list(fields.keys())
        assert keys == sorted(keys)

    def test_list_fields_first_name_maps_to_person(self) -> None:
        fields = DataForge.list_fields()
        assert fields["first_name"] == ("person", "first_name")

    def test_list_fields_email_maps_to_internet(self) -> None:
        fields = DataForge.list_fields()
        assert fields["email"] == ("internet", "email")


# Type-aware Pydantic mapping


class TestPydanticTypeAware:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    @pytest.fixture
    def pydantic_available(self):
        pytest.importorskip("pydantic")

    def test_name_based_mapping(self, pydantic_available) -> None:
        from pydantic import BaseModel

        class User(BaseModel):
            first_name: str
            email: str

        schema = self.forge.schema_from_pydantic(User)
        rows = schema.generate(count=3)
        assert len(rows) == 3
        for row in rows:
            assert "first_name" in row
            assert "email" in row

    def test_alias_mapping(self, pydantic_available) -> None:
        from pydantic import BaseModel

        class User(BaseModel):
            fname: str
            lname: str
            mail: str

        schema = self.forge.schema_from_pydantic(User)
        rows = schema.generate(count=3)
        for row in rows:
            assert "fname" in row
            assert "lname" in row
            assert "mail" in row

    def test_type_based_bool_fallback(self, pydantic_available) -> None:
        from pydantic import BaseModel

        class Settings(BaseModel):
            email: str
            is_active: bool  # should fallback to boolean generator

        schema = self.forge.schema_from_pydantic(Settings)
        rows = schema.generate(count=5)
        for row in rows:
            assert "email" in row
            assert "is_active" in row
            assert isinstance(row["is_active"], bool)

    def test_type_based_uuid_fallback(self, pydantic_available) -> None:
        import uuid

        from pydantic import BaseModel

        class Record(BaseModel):
            email: str
            record_id: uuid.UUID

        schema = self.forge.schema_from_pydantic(Record)
        rows = schema.generate(count=3)
        for row in rows:
            assert "record_id" in row

    def test_type_based_date_fallback(self, pydantic_available) -> None:
        import datetime

        from pydantic import BaseModel

        class Event(BaseModel):
            first_name: str
            event_date: datetime.date

        schema = self.forge.schema_from_pydantic(Event)
        rows = schema.generate(count=3)
        for row in rows:
            assert "event_date" in row

    def test_type_based_datetime_fallback(self, pydantic_available) -> None:
        import datetime

        from pydantic import BaseModel

        class Log(BaseModel):
            email: str
            logged_at: datetime.datetime

        schema = self.forge.schema_from_pydantic(Log)
        rows = schema.generate(count=3)
        for row in rows:
            assert "logged_at" in row

    def test_optional_type_unwrapped(self, pydantic_available) -> None:
        from pydantic import BaseModel

        class Profile(BaseModel):
            email: str
            is_verified: bool | None = None

        schema = self.forge.schema_from_pydantic(Profile)
        rows = schema.generate(count=3)
        for row in rows:
            assert "is_verified" in row
            assert isinstance(row["is_verified"], bool)

    def test_unmappable_fields_skipped_with_warning(self, pydantic_available) -> None:
        import warnings

        from pydantic import BaseModel

        class WeirdModel(BaseModel):
            email: str
            xyzzy_field: int

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = self.forge.schema_from_pydantic(WeirdModel)
            # xyzzy_field should be skipped with a warning
            assert any("xyzzy_field" in str(warning.message) for warning in w)

        rows = schema.generate(count=1)
        assert "email" in rows[0]
        assert "xyzzy_field" not in rows[0]

    def test_all_unmappable_raises(self, pydantic_available) -> None:
        from pydantic import BaseModel

        class BadModel(BaseModel):
            xyzzy: int
            plugh: int

        with pytest.raises(ValueError, match="No fields"):
            self.forge.schema_from_pydantic(BadModel)


# Type-aware SQLAlchemy mapping


class TestSQLAlchemyTypeAware:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    @pytest.fixture
    def sa_available(self):
        pytest.importorskip("sqlalchemy")

    def test_name_based_mapping(self, sa_available) -> None:
        from sqlalchemy import Column, Integer, String
        from sqlalchemy.orm import DeclarativeBase

        class Base(DeclarativeBase):
            pass

        class User(Base):
            __tablename__ = "users_test_name"
            id = Column(Integer, primary_key=True)
            first_name = Column(String)
            email = Column(String)

        schema = self.forge.schema_from_sqlalchemy(User)
        rows = schema.generate(count=3)
        assert len(rows) == 3
        for row in rows:
            assert "first_name" in row
            assert "email" in row

    def test_type_based_boolean_fallback(self, sa_available) -> None:
        from sqlalchemy import Boolean, Column, Integer, String
        from sqlalchemy.orm import DeclarativeBase

        class Base(DeclarativeBase):
            pass

        class Settings(Base):
            __tablename__ = "settings_test_bool"
            id = Column(Integer, primary_key=True)
            email = Column(String)
            is_active = Column(Boolean)

        schema = self.forge.schema_from_sqlalchemy(Settings)
        rows = schema.generate(count=3)
        for row in rows:
            assert "email" in row
            assert "is_active" in row

    def test_type_based_date_fallback(self, sa_available) -> None:
        from sqlalchemy import Column, Date, Integer, String
        from sqlalchemy.orm import DeclarativeBase

        class Base(DeclarativeBase):
            pass

        class Events(Base):
            __tablename__ = "events_test_date"
            id = Column(Integer, primary_key=True)
            first_name = Column(String)
            event_date = Column(Date)

        schema = self.forge.schema_from_sqlalchemy(Events)
        rows = schema.generate(count=3)
        for row in rows:
            assert "event_date" in row

    def test_type_based_datetime_fallback(self, sa_available) -> None:
        from sqlalchemy import Column, DateTime, Integer, String
        from sqlalchemy.orm import DeclarativeBase

        class Base(DeclarativeBase):
            pass

        class Logs(Base):
            __tablename__ = "logs_test_datetime"
            id = Column(Integer, primary_key=True)
            email = Column(String)
            created_at_col = Column(DateTime)

        schema = self.forge.schema_from_sqlalchemy(Logs)
        rows = schema.generate(count=3)
        for row in rows:
            assert "email" in row
            # created_at_col should map via DateTime type fallback
            assert "created_at_col" in row

    def test_type_based_text_fallback(self, sa_available) -> None:
        from sqlalchemy import Column, Integer, String, Text
        from sqlalchemy.orm import DeclarativeBase

        class Base(DeclarativeBase):
            pass

        class Articles(Base):
            __tablename__ = "articles_test_text"
            id = Column(Integer, primary_key=True)
            first_name = Column(String)
            article_body = Column(Text)

        schema = self.forge.schema_from_sqlalchemy(Articles)
        rows = schema.generate(count=3)
        for row in rows:
            assert "article_body" in row

    def test_pk_id_skipped(self, sa_available) -> None:
        from sqlalchemy import Column, Integer, String
        from sqlalchemy.orm import DeclarativeBase

        class Base(DeclarativeBase):
            pass

        class Item(Base):
            __tablename__ = "items_test_pk"
            id = Column(Integer, primary_key=True)
            first_name = Column(String)

        schema = self.forge.schema_from_sqlalchemy(Item)
        rows = schema.generate(count=1)
        assert "id" not in rows[0]
        assert "first_name" in rows[0]


# CLI enhancements


class TestCliVersion:
    def test_version_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        from dataforge.cli import main

        with pytest.raises(SystemExit) as exc_info:
            main(["--version"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert __version__ in captured.out


class TestCliListProviders:
    def test_list_providers_returns_zero(self) -> None:
        from dataforge.cli import main

        result = main(["--list-providers"])
        assert result == 0

    def test_list_providers_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        from dataforge.cli import main

        main(["--list-providers"])
        captured = capsys.readouterr()
        assert "person" in captured.out
        assert "address" in captured.out


class TestCliSqlFormat:
    def test_sql_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        from dataforge.cli import main

        result = main(
            [
                "--count",
                "3",
                "--format",
                "sql",
                "--table",
                "users",
                "--seed",
                "42",
                "first_name",
                "email",
            ]
        )
        assert result == 0
        captured = capsys.readouterr()
        assert "INSERT INTO" in captured.out
        assert "users" in captured.out

    def test_sql_mysql_dialect(self, capsys: pytest.CaptureFixture[str]) -> None:
        from dataforge.cli import main

        main(
            [
                "--count",
                "1",
                "--format",
                "sql",
                "--table",
                "users",
                "--dialect",
                "mysql",
                "--seed",
                "42",
                "first_name",
            ]
        )
        captured = capsys.readouterr()
        assert "`users`" in captured.out

    def test_sql_postgresql_dialect(self, capsys: pytest.CaptureFixture[str]) -> None:
        from dataforge.cli import main

        main(
            [
                "--count",
                "1",
                "--format",
                "sql",
                "--table",
                "users",
                "--dialect",
                "postgresql",
                "--seed",
                "42",
                "first_name",
            ]
        )
        captured = capsys.readouterr()
        assert '"users"' in captured.out


class TestCliTsvFormat:
    def test_tsv_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        from dataforge.cli import main

        result = main(
            ["--count", "3", "--format", "tsv", "--seed", "42", "first_name", "email"]
        )
        assert result == 0
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        # Header + 3 data rows
        assert len(lines) == 4
        # Tab-separated
        assert "\t" in lines[0]


class TestCliDelimiter:
    def test_custom_delimiter(self, capsys: pytest.CaptureFixture[str]) -> None:
        from dataforge.cli import main

        result = main(
            [
                "--count",
                "3",
                "--format",
                "csv",
                "--delimiter",
                "|",
                "--seed",
                "42",
                "first_name",
                "email",
            ]
        )
        assert result == 0
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        assert "|" in lines[0]


class TestCliColumnRenaming:
    def test_column_renaming_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        from dataforge.cli import main

        result = main(
            [
                "--count",
                "3",
                "--format",
                "json",
                "--seed",
                "42",
                "Name=full_name",
                "Email=email",
            ]
        )
        assert result == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert len(data) == 3
        for row in data:
            assert "Name" in row
            assert "Email" in row

    def test_column_renaming_csv(self, capsys: pytest.CaptureFixture[str]) -> None:
        from dataforge.cli import main

        main(
            [
                "--count",
                "2",
                "--format",
                "csv",
                "--seed",
                "42",
                "Name=full_name",
                "Email=email",
            ]
        )
        captured = capsys.readouterr()
        reader = csv.DictReader(io.StringIO(captured.out))
        assert "Name" in reader.fieldnames
        assert "Email" in reader.fieldnames

    def test_column_renaming_text(self, capsys: pytest.CaptureFixture[str]) -> None:
        from dataforge.cli import main

        main(
            [
                "--count",
                "2",
                "--seed",
                "42",
                "Full Name=full_name",
            ]
        )
        captured = capsys.readouterr()
        assert "Full Name" in captured.out


class TestCliUnique:
    def test_unique_values(self, capsys: pytest.CaptureFixture[str]) -> None:
        from dataforge.cli import main

        result = main(
            [
                "--count",
                "20",
                "--format",
                "json",
                "--seed",
                "42",
                "--unique",
                "first_name",
            ]
        )
        assert result == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        names = [row["first_name"] for row in data]
        # All values should be unique
        assert len(names) == len(set(names))


class TestCliStream:
    def test_stream_requires_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        from dataforge.cli import main

        result = main(["--stream", "--count", "10", "first_name"])
        assert result == 1
        captured = capsys.readouterr()
        assert "Error" in captured.err

    def test_stream_csv(self, tmp_path) -> None:
        from dataforge.cli import main

        path = str(tmp_path / "stream.csv")
        result = main(
            [
                "--stream",
                "--count",
                "100",
                "--format",
                "csv",
                "--seed",
                "42",
                "-o",
                path,
                "first_name",
                "email",
            ]
        )
        assert result == 0
        with open(path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 100

    def test_stream_jsonl(self, tmp_path) -> None:
        from dataforge.cli import main

        path = str(tmp_path / "stream.jsonl")
        result = main(
            [
                "--stream",
                "--count",
                "50",
                "--format",
                "jsonl",
                "--seed",
                "42",
                "-o",
                path,
                "first_name",
                "email",
            ]
        )
        assert result == 0
        with open(path) as f:
            lines = [line.strip() for line in f if line.strip()]
        assert len(lines) == 50
        for line in lines:
            row = json.loads(line)
            assert "first_name" in row

    def test_stream_not_supported_with_sql(self) -> None:
        from dataforge.cli import main

        result = main(
            [
                "--stream",
                "--count",
                "10",
                "--format",
                "sql",
                "--table",
                "t",
                "-o",
                "dummy.sql",
                "first_name",
            ]
        )
        assert result == 1


# Backend new methods


class TestBackendNewMethods:
    def setup_method(self) -> None:
        from dataforge.backend import RandomEngine

        self.engine = RandomEngine(seed=42)

    def test_random_float(self) -> None:
        from dataforge.backend import RandomEngine

        engine = RandomEngine(seed=42)
        for _ in range(100):
            val = engine.random_float(1.0, 10.0, precision=2)
            assert 1.0 <= val <= 10.0
            # Check precision: should have at most 2 decimal places
            # After rounding, the value multiplied by 100 should be int-like
            assert abs(val * 100 - round(val * 100)) < 1e-9

    def test_sample_without_replacement(self) -> None:
        from dataforge.backend import RandomEngine

        engine = RandomEngine(seed=42)
        pool = tuple(range(10))
        result = engine.sample(pool, 5)
        assert len(result) == 5
        assert len(set(result)) == 5  # All unique
        for v in result:
            assert v in pool

    def test_letterify(self) -> None:
        from dataforge.backend import RandomEngine

        engine = RandomEngine(seed=42)
        result = engine.letterify("???-???")
        assert len(result) == 7
        assert result[3] == "-"
        assert result[0].isalpha()
        assert result[6].isalpha()

    def test_bothify(self) -> None:
        from dataforge.backend import RandomEngine

        engine = RandomEngine(seed=42)
        result = engine.bothify("??##-??##")
        assert len(result) == 9
        assert result[4] == "-"
        assert result[0].isalpha()
        assert result[2].isdigit()

    def test_gauss(self) -> None:
        from dataforge.backend import RandomEngine

        engine = RandomEngine(seed=42)
        values = [engine.gauss(0.0, 1.0) for _ in range(1000)]
        mean = sum(values) / len(values)
        # Should be roughly centered around 0
        assert -0.3 < mean < 0.3

    def test_gauss_int(self) -> None:
        from dataforge.backend import RandomEngine

        engine = RandomEngine(seed=42)
        values = [engine.gauss_int(100, 10, 50, 150) for _ in range(100)]
        for v in values:
            assert isinstance(v, int)
            assert 50 <= v <= 150
        mean = sum(values) / len(values)
        assert 90 < mean < 110

    def test_triangular(self) -> None:
        from dataforge.backend import RandomEngine

        engine = RandomEngine(seed=42)
        for _ in range(100):
            val = engine.triangular(0.0, 10.0, 5.0)
            assert 0.0 <= val <= 10.0

    def test_exponential(self) -> None:
        from dataforge.backend import RandomEngine

        engine = RandomEngine(seed=42)
        for _ in range(100):
            val = engine.exponential(1.0)
            assert val >= 0.0

    def test_zipf(self) -> None:
        from dataforge.backend import RandomEngine

        engine = RandomEngine(seed=42)
        for _ in range(100):
            val = engine.zipf(2.0, 100)
            assert isinstance(val, int)
            assert 1 <= val <= 100


# SQL null handling


class TestSqlNullHandling:
    def test_none_values_become_null(self) -> None:
        forge = DataForge(seed=42)
        schema = forge.schema(
            {
                "name": "full_name",
                "maybe_null": lambda row: None,
            }
        )
        sql = schema.to_sql("test_table", count=3)
        assert "NULL" in sql


# Type resolution helpers


class TestTypeResolution:
    def test_resolve_plain_type(self) -> None:
        from dataforge.core import _resolve_type_annotation

        assert _resolve_type_annotation(str) is str
        assert _resolve_type_annotation(int) is int
        assert _resolve_type_annotation(bool) is bool

    def test_resolve_optional(self) -> None:
        from dataforge.core import _resolve_type_annotation
        from typing import Optional

        result = _resolve_type_annotation(Optional[str])
        assert result is str

    def test_resolve_union_with_none(self) -> None:
        from dataforge.core import _resolve_type_annotation

        result = _resolve_type_annotation(str | None)
        assert result is str

    def test_resolve_list(self) -> None:
        from dataforge.core import _resolve_type_annotation

        result = _resolve_type_annotation(list[str])
        assert result is str

    def test_type_fallback_bool(self) -> None:
        from dataforge.core import _type_fallback

        assert _type_fallback(bool) == "boolean"

    def test_type_fallback_str_returns_none(self) -> None:
        from dataforge.core import _type_fallback

        assert _type_fallback(str) is None

    def test_type_fallback_int_returns_none(self) -> None:
        from dataforge.core import _type_fallback

        assert _type_fallback(int) is None

    def test_type_fallback_float_returns_none(self) -> None:
        from dataforge.core import _type_fallback

        assert _type_fallback(float) is None

    def test_type_fallback_uuid(self) -> None:
        import uuid

        from dataforge.core import _type_fallback

        assert _type_fallback(uuid.UUID) == "uuid4"

    def test_type_fallback_date(self) -> None:
        import datetime

        from dataforge.core import _type_fallback

        assert _type_fallback(datetime.date) == "date"

    def test_type_fallback_datetime(self) -> None:
        import datetime

        from dataforge.core import _type_fallback

        assert _type_fallback(datetime.datetime) == "datetime"

    def test_type_fallback_time(self) -> None:
        import datetime

        from dataforge.core import _type_fallback

        assert _type_fallback(datetime.time) == "time"

    def test_type_fallback_optional_bool(self) -> None:
        from dataforge.core import _type_fallback

        assert _type_fallback(bool | None) == "boolean"
