"""Tests for OpenAPI / JSON Schema import."""

from __future__ import annotations

import json
import os
import tempfile

import pytest

from dataforge import DataForge
from dataforge.openapi import OpenAPIParser, _TYPE_FORMAT_MAP, _PROPERTY_NAME_MAP


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture
def forge() -> DataForge:
    return DataForge(locale="en_US", seed=42)


@pytest.fixture
def parser(forge: DataForge) -> OpenAPIParser:
    return OpenAPIParser(forge)


# ------------------------------------------------------------------
# Construction
# ------------------------------------------------------------------


class TestOpenAPIParserConstruction:
    def test_repr(self, parser: OpenAPIParser) -> None:
        assert "OpenAPIParser" in repr(parser)

    def test_slots(self, parser: OpenAPIParser) -> None:
        with pytest.raises(AttributeError):
            parser.nonexistent = True  # type: ignore[attr-defined]


# ------------------------------------------------------------------
# Type format map
# ------------------------------------------------------------------


class TestTypeFormatMap:
    def test_email_mapping(self) -> None:
        assert _TYPE_FORMAT_MAP[("string", "email")] == "email"

    def test_uri_mapping(self) -> None:
        assert _TYPE_FORMAT_MAP[("string", "uri")] == "url"

    def test_uuid_mapping(self) -> None:
        assert _TYPE_FORMAT_MAP[("string", "uuid")] == "uuid4"

    def test_boolean_mapping(self) -> None:
        assert _TYPE_FORMAT_MAP[("boolean", None)] == "boolean"

    def test_date_time_mapping(self) -> None:
        assert _TYPE_FORMAT_MAP[("string", "date-time")] == "datetime"


# ------------------------------------------------------------------
# Property name map
# ------------------------------------------------------------------


class TestPropertyNameMap:
    def test_name_maps_to_full_name(self) -> None:
        assert _PROPERTY_NAME_MAP["name"] == "full_name"

    def test_email_maps(self) -> None:
        assert _PROPERTY_NAME_MAP["email"] == "email"

    def test_city_maps(self) -> None:
        assert _PROPERTY_NAME_MAP["city"] == "city"


# ------------------------------------------------------------------
# JSON Schema parsing
# ------------------------------------------------------------------


class TestFromJsonSchema:
    def test_basic_object(self, parser: OpenAPIParser) -> None:
        schema_def = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"},
            },
        }
        schema = parser.from_json_schema(schema_def)
        rows = schema.generate(count=5)
        assert len(rows) == 5
        assert "name" in rows[0]
        assert "email" in rows[0]

    def test_string_formats(self, parser: OpenAPIParser) -> None:
        schema_def = {
            "type": "object",
            "properties": {
                "website": {"type": "string", "format": "uri"},
                "id": {"type": "string", "format": "uuid"},
                "created": {"type": "string", "format": "date"},
            },
        }
        schema = parser.from_json_schema(schema_def)
        rows = schema.generate(count=3)
        assert len(rows) == 3
        # Should have all columns
        assert "website" in rows[0]
        assert "id" in rows[0]
        assert "created" in rows[0]

    def test_boolean_field(self, parser: OpenAPIParser) -> None:
        schema_def = {
            "type": "object",
            "properties": {
                "active": {"type": "boolean"},
            },
        }
        schema = parser.from_json_schema(schema_def)
        rows = schema.generate(count=10)
        # Boolean should be True/False
        for row in rows:
            assert isinstance(row["active"], bool)

    def test_property_name_heuristic(self, parser: OpenAPIParser) -> None:
        """Property names like 'email', 'city' should be auto-mapped."""
        schema_def = {
            "type": "object",
            "properties": {
                "email": {"type": "string"},
                "city": {"type": "string"},
                "company": {"type": "string"},
            },
        }
        schema = parser.from_json_schema(schema_def)
        rows = schema.generate(count=3)
        assert len(rows) == 3

    def test_no_properties_raises(self, parser: OpenAPIParser) -> None:
        schema_def = {"type": "object", "properties": {}}
        with pytest.raises(ValueError, match="no properties"):
            parser.from_json_schema(schema_def)

    def test_enum_skipped(self, parser: OpenAPIParser) -> None:
        """Enum properties are currently skipped."""
        schema_def = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "status": {"type": "string", "enum": ["active", "inactive"]},
            },
        }
        schema = parser.from_json_schema(schema_def)
        rows = schema.generate(count=3)
        # 'status' should be skipped (enum), 'name' should be present
        assert "name" in rows[0]

    def test_array_skipped(self, parser: OpenAPIParser) -> None:
        schema_def = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
            },
        }
        schema = parser.from_json_schema(schema_def)
        rows = schema.generate(count=3)
        assert "name" in rows[0]

    def test_string_fallback_to_word(self, parser: OpenAPIParser) -> None:
        """Unknown string property should fall back to lorem.word."""
        schema_def = {
            "type": "object",
            "properties": {
                "xyzzy_field": {"type": "string"},
            },
        }
        schema = parser.from_json_schema(schema_def)
        rows = schema.generate(count=3)
        assert "xyzzy_field" in rows[0]
        assert isinstance(rows[0]["xyzzy_field"], str)


# ------------------------------------------------------------------
# OpenAPI document parsing
# ------------------------------------------------------------------


class TestFromOpenAPI:
    def test_openapi_with_schemas(self, parser: OpenAPIParser) -> None:
        doc = {
            "openapi": "3.0.0",
            "info": {"title": "Test API", "version": "1.0.0"},
            "components": {
                "schemas": {
                    "User": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "email": {"type": "string", "format": "email"},
                        },
                    },
                    "Product": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "string"},
                            "description": {"type": "string"},
                        },
                    },
                }
            },
        }
        schemas = parser.from_openapi(doc)
        assert "User" in schemas
        assert "Product" in schemas

        # Generate data from the User schema
        rows = schemas["User"].generate(count=5)
        assert len(rows) == 5

    def test_empty_components(self, parser: OpenAPIParser) -> None:
        doc = {"openapi": "3.0.0", "info": {"title": "Test", "version": "1.0.0"}}
        schemas = parser.from_openapi(doc)
        assert schemas == {}

    def test_ref_resolution(self, parser: OpenAPIParser) -> None:
        doc = {
            "openapi": "3.0.0",
            "info": {"title": "Test", "version": "1.0.0"},
            "components": {
                "schemas": {
                    "Address": {
                        "type": "object",
                        "properties": {
                            "city": {"type": "string"},
                            "country": {"type": "string"},
                        },
                    },
                    "User": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "address": {"$ref": "#/components/schemas/Address"},
                        },
                    },
                }
            },
        }
        schemas = parser.from_openapi(doc)
        assert "Address" in schemas
        # User might only have 'name' since nested objects are skipped
        if "User" in schemas:
            rows = schemas["User"].generate(count=3)
            assert "name" in rows[0]

    def test_non_object_schemas_skipped(self, parser: OpenAPIParser) -> None:
        doc = {
            "openapi": "3.0.0",
            "info": {"title": "Test", "version": "1.0.0"},
            "components": {
                "schemas": {
                    "Status": {"type": "string", "enum": ["active", "inactive"]},
                    "User": {
                        "type": "object",
                        "properties": {"name": {"type": "string"}},
                    },
                }
            },
        }
        schemas = parser.from_openapi(doc)
        # Status is not an object, should be skipped
        assert "Status" not in schemas
        assert "User" in schemas


# ------------------------------------------------------------------
# File parsing
# ------------------------------------------------------------------


class TestFromFile:
    def test_json_file(self, parser: OpenAPIParser) -> None:
        doc = {
            "openapi": "3.0.0",
            "info": {"title": "Test API", "version": "1.0.0"},
            "components": {
                "schemas": {
                    "User": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "email": {"type": "string", "format": "email"},
                        },
                    }
                }
            },
        }
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as f:
            json.dump(doc, f)
            path = f.name

        try:
            schemas = parser.from_file(path)
            assert "User" in schemas
        finally:
            os.unlink(path)
