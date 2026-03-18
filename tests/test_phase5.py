"""Tests for Phase 5 — schema serialization, locale expansion, @provider decorator.

Covers:
- Schema serialization to/from JSON, YAML, TOML (round-trip)
- schema_to_dict / dict_to_schema_args correctness
- save_schema / load_schema file I/O
- Schema.to_schema_dict() and Schema.save_schema()
- DataForge.schema_from_dict() and DataForge.schema_from_file()
- CLI --schema and --save-schema flags
- 5 new locales: sv_SE, da_DK, nb_NO, fi_FI, tr_TR
- @provider decorator: class transformation, count wrapping, registration
"""

from __future__ import annotations

import json
import os
import tempfile

import pytest

from dataforge import DataForge, save_schema, load_schema
from dataforge.schema_io import (
    schema_to_dict,
    dict_to_schema_args,
    _yaml_parse,
    _yaml_dump,
)
from dataforge.decorators import provider, _wrap_with_count


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture
def forge() -> DataForge:
    return DataForge(seed=42)


@pytest.fixture
def tmp_dir():
    """Temporary directory for file I/O tests."""
    with tempfile.TemporaryDirectory() as d:
        yield d


# ==================================================================
# schema_to_dict / dict_to_schema_args
# ==================================================================


class TestSchemaToDict:
    def test_list_fields(self) -> None:
        d = schema_to_dict(["first_name", "email"], count=50)
        assert d["fields"] == ["first_name", "email"]
        assert d["count"] == 50
        assert "null_fields" not in d
        assert "unique_together" not in d

    def test_dict_fields_compact(self) -> None:
        """When all keys == values, it should compact to list form."""
        d = schema_to_dict({"first_name": "first_name", "email": "email"}, count=10)
        assert d["fields"] == ["first_name", "email"]

    def test_dict_fields_renamed(self) -> None:
        d = schema_to_dict({"Name": "full_name", "Email": "email"}, count=10)
        assert d["fields"] == {"Name": "full_name", "Email": "email"}

    def test_null_fields(self) -> None:
        d = schema_to_dict(
            ["first_name", "email"],
            count=100,
            null_fields={"email": 0.3},
        )
        assert d["null_fields"] == {"email": 0.3}

    def test_unique_together(self) -> None:
        d = schema_to_dict(
            ["first_name", "last_name", "email"],
            count=100,
            unique_together=[("first_name", "last_name")],
        )
        assert d["unique_together"] == [["first_name", "last_name"]]

    def test_empty_null_fields_omitted(self) -> None:
        d = schema_to_dict(["first_name"], count=5, null_fields={})
        assert "null_fields" not in d


class TestDictToSchemaArgs:
    def test_list_fields(self) -> None:
        fields, count, null_fields, unique = dict_to_schema_args(
            {"fields": ["first_name", "email"], "count": 50}
        )
        assert fields == ["first_name", "email"]
        assert count == 50
        assert null_fields is None
        assert unique is None

    def test_dict_fields(self) -> None:
        fields, count, null_fields, unique = dict_to_schema_args(
            {"fields": {"Name": "full_name"}, "count": 20}
        )
        assert fields == {"Name": "full_name"}
        assert count == 20

    def test_default_count(self) -> None:
        fields, count, _, _ = dict_to_schema_args({"fields": ["email"]})
        assert count == 10

    def test_null_fields_parsed(self) -> None:
        _, _, null_fields, _ = dict_to_schema_args(
            {"fields": ["email"], "null_fields": {"email": 0.2}}
        )
        assert null_fields == {"email": 0.2}

    def test_unique_together_parsed(self) -> None:
        _, _, _, unique = dict_to_schema_args(
            {
                "fields": ["a", "b"],
                "unique_together": [["a", "b"]],
            }
        )
        assert unique == [("a", "b")]

    def test_missing_fields_raises(self) -> None:
        with pytest.raises(ValueError, match="must contain a 'fields' key"):
            dict_to_schema_args({"count": 10})

    def test_invalid_fields_type_raises(self) -> None:
        with pytest.raises(ValueError, match="must be a list or dict"):
            dict_to_schema_args({"fields": "not_valid"})


# ==================================================================
# JSON serialization round-trip
# ==================================================================


class TestJsonRoundTrip:
    def test_simple_list(self, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "schema.json")
        original = schema_to_dict(["first_name", "email"], count=100)
        save_schema(original, path)
        loaded = load_schema(path)
        assert loaded == original

    def test_dict_fields(self, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "schema.json")
        original = schema_to_dict({"Name": "full_name", "Email": "email"}, count=50)
        save_schema(original, path)
        loaded = load_schema(path)
        assert loaded == original

    def test_with_null_fields(self, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "schema.json")
        original = schema_to_dict(
            ["first_name", "email"],
            count=100,
            null_fields={"email": 0.3},
        )
        save_schema(original, path)
        loaded = load_schema(path)
        assert loaded == original

    def test_with_unique_together(self, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "schema.json")
        original = schema_to_dict(
            ["first_name", "last_name"],
            count=50,
            unique_together=[("first_name", "last_name")],
        )
        save_schema(original, path)
        loaded = load_schema(path)
        assert loaded == original


# ==================================================================
# YAML serialization round-trip
# ==================================================================


class TestYamlRoundTrip:
    def test_simple_list(self, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "schema.yaml")
        original = schema_to_dict(["first_name", "email"], count=100)
        save_schema(original, path)
        loaded = load_schema(path)
        assert loaded["fields"] == original["fields"]
        assert loaded["count"] == original["count"]

    def test_yml_extension(self, tmp_dir: str) -> None:
        """Test .yml extension is also recognized."""
        path = os.path.join(tmp_dir, "schema.yml")
        original = schema_to_dict(["first_name"], count=5)
        save_schema(original, path)
        loaded = load_schema(path)
        assert loaded["fields"] == original["fields"]

    def test_dict_fields(self, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "schema.yaml")
        original = schema_to_dict({"Name": "full_name", "Email": "email"}, count=50)
        save_schema(original, path)
        loaded = load_schema(path)
        assert loaded["fields"] == original["fields"]

    def test_with_null_fields(self, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "schema.yaml")
        original = schema_to_dict(
            ["first_name", "email"],
            count=100,
            null_fields={"email": 0.3},
        )
        save_schema(original, path)
        loaded = load_schema(path)
        assert loaded["null_fields"]["email"] == pytest.approx(0.3)

    def test_yaml_parse_basic(self) -> None:
        text = "fields:\n  - first_name\n  - email\ncount: 100\n"
        result = _yaml_parse(text)
        assert result["fields"] == ["first_name", "email"]
        assert result["count"] == 100

    def test_yaml_parse_inline_dict(self) -> None:
        text = "fields: {Name: full_name, Email: email}\ncount: 10\n"
        result = _yaml_parse(text)
        assert result["fields"]["Name"] == "full_name"

    def test_yaml_dump_round_trip(self) -> None:
        data = {
            "fields": ["first_name", "email"],
            "count": 100,
        }
        lines = _yaml_dump(data, indent=0)
        text = "\n".join(lines) + "\n"
        parsed = _yaml_parse(text)
        assert parsed["fields"] == data["fields"]
        assert parsed["count"] == data["count"]

    def test_yaml_comments_ignored(self) -> None:
        text = "# Comment line\nfields:\n  - email\ncount: 5\n"
        result = _yaml_parse(text)
        assert result["fields"] == ["email"]


# ==================================================================
# TOML serialization round-trip
# ==================================================================


class TestTomlRoundTrip:
    def test_simple_list(self, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "schema.toml")
        original = schema_to_dict(["first_name", "email"], count=100)
        save_schema(original, path)
        loaded = load_schema(path)
        assert loaded["fields"] == original["fields"]
        assert loaded["count"] == original["count"]

    def test_dict_fields(self, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "schema.toml")
        original = schema_to_dict({"Name": "full_name", "Email": "email"}, count=50)
        save_schema(original, path)
        loaded = load_schema(path)
        # TOML loads tables as nested dicts
        fields = loaded.get("fields", {})
        assert fields["Name"] == "full_name"
        assert fields["Email"] == "email"


# ==================================================================
# Format detection errors
# ==================================================================


class TestFormatDetection:
    def test_unknown_extension_raises(self, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "schema.xyz")
        d = schema_to_dict(["first_name"], count=5)
        with pytest.raises(ValueError, match="Cannot determine schema format"):
            save_schema(d, path)

    def test_unknown_format_explicit_raises(self, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "schema.json")
        d = schema_to_dict(["first_name"], count=5)
        with pytest.raises(ValueError, match="Unsupported schema format"):
            save_schema(d, path, format="xml")

    def test_load_nonexistent_file_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_schema("nonexistent_file.json")


# ==================================================================
# Schema.to_schema_dict() and Schema.save_schema()
# ==================================================================


class TestSchemaSerializationMethods:
    def test_to_schema_dict_list(self, forge: DataForge) -> None:
        schema = forge.schema(["first_name", "email"])
        d = schema.to_schema_dict(count=100)
        assert d["fields"] == ["first_name", "email"]
        assert d["count"] == 100

    def test_to_schema_dict_renamed(self, forge: DataForge) -> None:
        schema = forge.schema({"Name": "full_name", "Email": "email"})
        d = schema.to_schema_dict(count=50)
        assert d["fields"] == {"Name": "full_name", "Email": "email"}

    def test_to_schema_dict_with_null_fields(self, forge: DataForge) -> None:
        schema = forge.schema(
            ["first_name", "email"],
            null_fields={"email": 0.2},
        )
        d = schema.to_schema_dict(count=100)
        assert d["null_fields"]["email"] == pytest.approx(0.2)

    def test_to_schema_dict_with_unique_together(self, forge: DataForge) -> None:
        schema = forge.schema(
            ["first_name", "last_name"],
            unique_together=[("first_name", "last_name")],
        )
        d = schema.to_schema_dict(count=50)
        assert d["unique_together"] == [["first_name", "last_name"]]

    def test_to_schema_dict_lambdas_omitted(self, forge: DataForge) -> None:
        schema = forge.schema(
            {
                "name": "full_name",
                "email": "email",
                "username": lambda row: row["name"].lower().replace(" ", "."),
            }
        )
        d = schema.to_schema_dict(count=10)
        # Lambda column should be omitted
        fields = d["fields"]
        assert isinstance(fields, dict)
        assert "name" in fields
        assert "email" in fields
        assert "username" not in fields

    def test_save_schema_json(self, forge: DataForge, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "test.json")
        schema = forge.schema(["first_name", "email"])
        schema.save_schema(path, count=100)
        loaded = load_schema(path)
        assert loaded["fields"] == ["first_name", "email"]
        assert loaded["count"] == 100

    def test_save_schema_yaml(self, forge: DataForge, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "test.yaml")
        schema = forge.schema(["first_name", "email"])
        schema.save_schema(path, count=50)
        loaded = load_schema(path)
        assert loaded["fields"] == ["first_name", "email"]

    def test_save_schema_toml(self, forge: DataForge, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "test.toml")
        schema = forge.schema(["first_name", "email"])
        schema.save_schema(path, count=25)
        loaded = load_schema(path)
        assert loaded["fields"] == ["first_name", "email"]


# ==================================================================
# DataForge.schema_from_dict() and schema_from_file()
# ==================================================================


class TestSchemaFromDict:
    def test_simple(self, forge: DataForge) -> None:
        schema = forge.schema_from_dict(
            {
                "fields": ["first_name", "email"],
                "count": 100,
            }
        )
        rows = schema.generate(count=5)
        assert len(rows) == 5
        assert "first_name" in rows[0]
        assert "email" in rows[0]

    def test_dict_fields(self, forge: DataForge) -> None:
        schema = forge.schema_from_dict(
            {
                "fields": {"Name": "full_name", "Email": "email"},
                "count": 10,
            }
        )
        rows = schema.generate(count=3)
        assert "Name" in rows[0]
        assert "Email" in rows[0]

    def test_with_null_fields(self, forge: DataForge) -> None:
        schema = forge.schema_from_dict(
            {
                "fields": ["first_name", "email"],
                "null_fields": {"email": 1.0},  # Always null
                "count": 10,
            }
        )
        rows = schema.generate(count=10)
        assert all(r["email"] is None for r in rows)


class TestSchemaFromFile:
    def test_json_file(self, forge: DataForge, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "schema.json")
        d = schema_to_dict(["first_name", "email"], count=10)
        save_schema(d, path)
        schema = forge.schema_from_file(path)
        rows = schema.generate(count=5)
        assert len(rows) == 5
        assert "first_name" in rows[0]

    def test_yaml_file(self, forge: DataForge, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "schema.yaml")
        d = schema_to_dict(["first_name", "city"], count=10)
        save_schema(d, path)
        schema = forge.schema_from_file(path)
        rows = schema.generate(count=5)
        assert len(rows) == 5
        assert "city" in rows[0]

    def test_toml_file(self, forge: DataForge, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "schema.toml")
        d = schema_to_dict(["first_name", "email"], count=10)
        save_schema(d, path)
        schema = forge.schema_from_file(path)
        rows = schema.generate(count=5)
        assert len(rows) == 5

    def test_explicit_format(self, forge: DataForge, tmp_dir: str) -> None:
        # Save as JSON but with a non-standard extension, using explicit format
        path = os.path.join(tmp_dir, "schema.txt")
        d = schema_to_dict(["first_name"], count=5)
        save_schema(d, path, format="json")
        schema = forge.schema_from_file(path, format="json")
        rows = schema.generate(count=3)
        assert len(rows) == 3

    def test_round_trip_generate(self, forge: DataForge, tmp_dir: str) -> None:
        """Full round-trip: create schema -> save -> load -> generate."""
        path = os.path.join(tmp_dir, "round_trip.json")

        # Create and save
        original = forge.schema(["first_name", "email", "city"])
        original.save_schema(path, count=20)

        # Load and generate
        loaded = forge.schema_from_file(path)
        rows = loaded.generate(count=20)
        assert len(rows) == 20
        assert all("first_name" in r for r in rows)
        assert all("email" in r for r in rows)
        assert all("city" in r for r in rows)


# ==================================================================
# CLI --schema and --save-schema flags
# ==================================================================


class TestCliSchemaFlags:
    def test_save_schema_json(self, tmp_dir: str) -> None:
        from dataforge.cli import main

        path = os.path.join(tmp_dir, "cli_schema.json")
        ret = main(["--save-schema", path, "first_name", "email"])
        assert ret == 0
        assert os.path.exists(path)
        loaded = load_schema(path)
        assert loaded["fields"] == ["first_name", "email"]

    def test_save_schema_yaml(self, tmp_dir: str) -> None:
        from dataforge.cli import main

        path = os.path.join(tmp_dir, "cli_schema.yaml")
        ret = main(["--save-schema", path, "first_name", "email"])
        assert ret == 0
        assert os.path.exists(path)

    def test_save_schema_with_count(self, tmp_dir: str) -> None:
        from dataforge.cli import main

        path = os.path.join(tmp_dir, "cli_schema.json")
        ret = main(["--count", "50", "--save-schema", path, "first_name", "email"])
        assert ret == 0
        loaded = load_schema(path)
        assert loaded["count"] == 50

    def test_load_schema_json(self, tmp_dir: str) -> None:
        from dataforge.cli import main

        # First save a schema
        schema_path = os.path.join(tmp_dir, "cli_load.json")
        d = schema_to_dict(["first_name", "email"], count=5)
        save_schema(d, schema_path)

        # Then use --schema to load it
        output_path = os.path.join(tmp_dir, "output.json")
        ret = main(["--schema", schema_path, "--format", "json", "-o", output_path])
        assert ret == 0
        assert os.path.exists(output_path)
        with open(output_path) as f:
            data = json.load(f)
        assert len(data) == 5
        assert "first_name" in data[0]

    def test_load_schema_yaml(self, tmp_dir: str) -> None:
        from dataforge.cli import main

        schema_path = os.path.join(tmp_dir, "cli_load.yaml")
        d = schema_to_dict(["first_name", "city"], count=3)
        save_schema(d, schema_path)

        output_path = os.path.join(tmp_dir, "output.json")
        ret = main(["--schema", schema_path, "--format", "json", "-o", output_path])
        assert ret == 0
        with open(output_path) as f:
            data = json.load(f)
        assert len(data) == 3
        assert "city" in data[0]

    def test_schema_with_cli_count_override(self, tmp_dir: str) -> None:
        from dataforge.cli import main

        schema_path = os.path.join(tmp_dir, "override.json")
        d = schema_to_dict(["first_name"], count=5)
        save_schema(d, schema_path)

        output_path = os.path.join(tmp_dir, "output.json")
        # CLI --count 3 should override schema's count=5
        ret = main(
            [
                "--schema",
                schema_path,
                "--count",
                "3",
                "--format",
                "json",
                "-o",
                output_path,
            ]
        )
        assert ret == 0
        with open(output_path) as f:
            data = json.load(f)
        assert len(data) == 3

    def test_schema_nonexistent_file(self, tmp_dir: str) -> None:
        from dataforge.cli import main

        ret = main(["--schema", os.path.join(tmp_dir, "nope.json")])
        assert ret == 1


# ==================================================================
# New Locales: sv_SE, da_DK, nb_NO, fi_FI, tr_TR
# ==================================================================


NEW_LOCALES = ["sv_SE", "da_DK", "nb_NO", "fi_FI", "tr_TR"]


class TestNewLocales:
    @pytest.mark.parametrize("locale", NEW_LOCALES)
    def test_person_first_name(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        name = forge.person.first_name()
        assert isinstance(name, str)
        assert len(name) > 0

    @pytest.mark.parametrize("locale", NEW_LOCALES)
    def test_person_last_name(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        name = forge.person.last_name()
        assert isinstance(name, str)
        assert len(name) > 0

    @pytest.mark.parametrize("locale", NEW_LOCALES)
    def test_person_full_name(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        name = forge.person.full_name()
        assert isinstance(name, str)
        assert " " in name  # full_name has first + last

    @pytest.mark.parametrize("locale", NEW_LOCALES)
    def test_person_batch(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        names = forge.person.first_name(count=10)
        assert isinstance(names, list)
        assert len(names) == 10

    @pytest.mark.parametrize("locale", NEW_LOCALES)
    def test_address_city(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        city = forge.address.city()
        assert isinstance(city, str)
        assert len(city) > 0

    @pytest.mark.parametrize("locale", NEW_LOCALES)
    def test_address_full_address(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        addr = forge.address.full_address()
        assert isinstance(addr, str)
        assert len(addr) > 5

    @pytest.mark.parametrize("locale", NEW_LOCALES)
    def test_company_name(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        name = forge.company.company_name()
        assert isinstance(name, str)
        assert len(name) > 0

    @pytest.mark.parametrize("locale", NEW_LOCALES)
    def test_phone_number(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        phone = forge.phone.phone_number()
        assert isinstance(phone, str)
        assert len(phone) > 0

    @pytest.mark.parametrize("locale", NEW_LOCALES)
    def test_internet_email(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        email = forge.internet.email()
        assert isinstance(email, str)
        assert "@" in email

    @pytest.mark.parametrize("locale", NEW_LOCALES)
    def test_schema_with_locale(self, locale: str) -> None:
        """Test that new locales work with schema generation."""
        forge = DataForge(locale=locale, seed=42)
        schema = forge.schema(["first_name", "city", "email"])
        rows = schema.generate(count=10)
        assert len(rows) == 10
        assert all("first_name" in r for r in rows)
        assert all("city" in r for r in rows)
        assert all("email" in r for r in rows)


class TestLocaleDataPresence:
    """Verify that locale data modules contain expected attributes."""

    @pytest.mark.parametrize("locale", NEW_LOCALES)
    def test_person_data_has_names(self, locale: str) -> None:
        import importlib

        mod = importlib.import_module(f"dataforge.locales.{locale}.person")
        assert hasattr(mod, "first_names")
        assert hasattr(mod, "last_names")
        assert len(mod.first_names) > 10
        assert len(mod.last_names) > 10

    @pytest.mark.parametrize("locale", NEW_LOCALES)
    def test_address_data_has_cities(self, locale: str) -> None:
        import importlib

        mod = importlib.import_module(f"dataforge.locales.{locale}.address")
        assert hasattr(mod, "cities")
        assert hasattr(mod, "street_names")
        assert len(mod.cities) > 5

    @pytest.mark.parametrize("locale", NEW_LOCALES)
    def test_company_data_exists(self, locale: str) -> None:
        import importlib

        mod = importlib.import_module(f"dataforge.locales.{locale}.company")
        assert hasattr(mod, "company_suffixes")

    @pytest.mark.parametrize("locale", NEW_LOCALES)
    def test_phone_data_exists(self, locale: str) -> None:
        import importlib

        mod = importlib.import_module(f"dataforge.locales.{locale}.phone")
        assert hasattr(mod, "phone_formats")

    @pytest.mark.parametrize("locale", NEW_LOCALES)
    def test_data_is_immutable_tuples(self, locale: str) -> None:
        """All locale data should be immutable tuples, not lists."""
        import importlib

        mod = importlib.import_module(f"dataforge.locales.{locale}.person")
        assert isinstance(mod.first_names, tuple)
        assert isinstance(mod.last_names, tuple)


# ==================================================================
# @provider decorator
# ==================================================================


class TestProviderDecorator:
    def test_basic_transformation(self) -> None:
        """@provider transforms a plain class into a BaseProvider subclass."""
        from dataforge.providers.base import BaseProvider

        @provider("test_greet")
        class GreetProvider:
            def hello(self) -> str:
                return "Hello!"

            def goodbye(self) -> str:
                return "Goodbye!"

        assert issubclass(GreetProvider, BaseProvider)
        assert GreetProvider._provider_name == "test_greet"
        assert "hello" in GreetProvider._field_map
        assert "goodbye" in GreetProvider._field_map

    def test_scalar_return(self) -> None:
        """count=1 returns a scalar."""

        @provider("test_scalar")
        class ScalarProvider:
            def greeting(self) -> str:
                return "Hi"

        forge = DataForge(seed=42)
        forge.register_provider(ScalarProvider)
        result = forge.test_scalar.greeting()
        assert result == "Hi"
        assert isinstance(result, str)

    def test_batch_return(self) -> None:
        """count>1 returns a list."""

        @provider("test_batch")
        class BatchProvider:
            def greeting(self) -> str:
                return "Hi"

        forge = DataForge(seed=42)
        forge.register_provider(BatchProvider)
        result = forge.test_batch.greeting(count=5)
        assert isinstance(result, list)
        assert len(result) == 5
        assert all(x == "Hi" for x in result)

    def test_custom_field_map(self) -> None:
        """Explicit field_map overrides auto-generated one."""

        @provider("test_custom_fm", field_map={"temp": "temperature"})
        class CustomFM:
            def temperature(self) -> str:
                return "22C"

        assert CustomFM._field_map == {"temp": "temperature"}

    def test_private_methods_excluded(self) -> None:
        """Methods starting with _ are not included in field_map."""

        @provider("test_private")
        class PrivateProvider:
            def public_method(self) -> str:
                return "public"

            def _private_method(self) -> str:
                return "private"

        assert "public_method" in PrivateProvider._field_map
        assert "_private_method" not in PrivateProvider._field_map

    def test_schema_integration(self) -> None:
        """Provider created via @provider works with Schema."""

        @provider("test_schema_int")
        class SchemaIntProvider:
            def value(self) -> str:
                return "test_value"

        forge = DataForge(seed=42)
        forge.register_provider(SchemaIntProvider)
        schema = forge.schema(["test_schema_int.value"])
        rows = schema.generate(count=5)
        assert len(rows) == 5
        assert all(r["test_schema_int.value"] == "test_value" for r in rows)

    def test_provider_name_preserved(self) -> None:
        """The decorated class keeps its original name."""

        @provider("test_name_pres")
        class MySpecialProvider:
            def foo(self) -> str:
                return "bar"

        assert MySpecialProvider.__name__ == "MySpecialProvider"

    def test_slots_empty(self) -> None:
        """Decorated providers have __slots__ = ()."""

        @provider("test_slots")
        class SlotProvider:
            def x(self) -> str:
                return "y"

        assert SlotProvider.__slots__ == ()

    def test_locale_modules(self) -> None:
        """locale_modules parameter is stored on the class."""

        @provider("test_locale", locale_modules=("person",))
        class LocaleProvider:
            def name(self) -> str:
                return "test"

        assert LocaleProvider._locale_modules == ("person",)


class TestWrapWithCount:
    def test_scalar(self) -> None:
        class Dummy:
            pass

        def original(self):
            return "value"

        wrapped = _wrap_with_count(original)
        d = Dummy()
        assert wrapped(d) == "value"

    def test_batch(self) -> None:
        class Dummy:
            pass

        def original(self):
            return "value"

        wrapped = _wrap_with_count(original)
        d = Dummy()
        result = wrapped(d, count=3)
        assert result == ["value", "value", "value"]

    def test_count_one_explicit(self) -> None:
        class Dummy:
            pass

        def original(self):
            return "x"

        wrapped = _wrap_with_count(original)
        d = Dummy()
        assert wrapped(d, count=1) == "x"


# ==================================================================
# Edge cases / error handling
# ==================================================================


class TestEdgeCases:
    def test_save_and_load_empty_list_fields(self, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "empty.json")
        d = {"fields": [], "count": 0}
        save_schema(d, path)
        loaded = load_schema(path)
        assert loaded["fields"] == []

    def test_json_top_level_not_dict_raises(self, tmp_dir: str) -> None:
        path = os.path.join(tmp_dir, "invalid.json")
        with open(path, "w") as f:
            json.dump([1, 2, 3], f)
        with pytest.raises(ValueError, match="Expected a JSON object"):
            load_schema(path)

    def test_schema_from_dict_preserves_fields_spec(self, forge: DataForge) -> None:
        """_fields_spec should be set for serialization round-trip."""
        schema = forge.schema(["first_name", "email"])
        assert schema._fields_spec == ["first_name", "email"]

    def test_schema_from_dict_fields_spec_dict(self, forge: DataForge) -> None:
        schema = forge.schema({"Name": "full_name", "Email": "email"})
        assert schema._fields_spec == {"Name": "full_name", "Email": "email"}
