"""OpenAPI / JSON Schema import — generate fake data from API specs.

Parses OpenAPI 3.x and JSON Schema documents, resolves ``$ref``
references, maps types and formats to DataForge providers, and
creates Schema objects that generate conforming data.

Usage::

    from dataforge import DataForge
    from dataforge.openapi import OpenAPIParser

    forge = DataForge(seed=42)
    parser = OpenAPIParser(forge)

    # From an OpenAPI spec file
    schemas = parser.from_file("openapi.yaml")

    # Generate data for a specific schema
    rows = schemas["User"].generate(count=100)

    # From a JSON Schema
    schema = parser.from_json_schema({
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "email": {"type": "string", "format": "email"},
            "age": {"type": "integer", "minimum": 18, "maximum": 99},
        }
    })
    rows = schema.generate(count=50)
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from dataforge.core import DataForge

# ------------------------------------------------------------------
# Type mapping: (JSON Schema type, format) → DataForge field
# ------------------------------------------------------------------

_TYPE_FORMAT_MAP: dict[tuple[str, str | None], str] = {
    # String formats
    ("string", "email"): "email",
    ("string", "uri"): "url",
    ("string", "url"): "url",
    ("string", "hostname"): "hostname",
    ("string", "ipv4"): "ipv4",
    ("string", "ipv6"): "ipv6",
    ("string", "uuid"): "uuid4",
    ("string", "date"): "date",
    ("string", "date-time"): "datetime",
    ("string", "time"): "time",
    ("string", "phone"): "phone_number",
    ("string", "password"): "crypto.sha256",
    ("string", "byte"): "misc.uuid4",
    ("string", "binary"): "misc.uuid4",
    # String without format → contextual
    ("string", None): None,  # resolved by property name
    # Numbers
    ("integer", None): None,
    ("integer", "int32"): None,
    ("integer", "int64"): None,
    ("number", None): None,
    ("number", "float"): None,
    ("number", "double"): None,
    # Boolean
    ("boolean", None): "boolean",
}

# Property name → DataForge field (for unformatted strings/integers)
_PROPERTY_NAME_MAP: dict[str, str] = {
    "name": "full_name",
    "first_name": "first_name",
    "last_name": "last_name",
    "email": "email",
    "phone": "phone_number",
    "address": "full_address",
    "city": "city",
    "state": "state",
    "country": "country",
    "zipcode": "zipcode",
    "zip_code": "zipcode",
    "url": "url",
    "website": "url",
    "username": "username",
    "password": "crypto.sha256",
    "description": "sentence",
    "title": "sentence",
    "company": "company_name",
    "id": "uuid4",
    "created_at": "datetime",
    "updated_at": "datetime",
    "ip_address": "ipv4",
}


class OpenAPIParser:
    """Parse OpenAPI and JSON Schema documents into DataForge Schemas.

    Parameters
    ----------
    forge : DataForge
        The DataForge instance for creating schemas.
    """

    __slots__ = ("_forge", "_ref_cache")

    def __init__(self, forge: DataForge) -> None:
        self._forge = forge
        self._ref_cache: dict[str, Any] = {}

    def from_file(self, path: str) -> dict[str, Any]:
        """Parse an OpenAPI spec file and return schemas.

        Parameters
        ----------
        path : str
            Path to the OpenAPI spec (JSON or YAML).

        Returns
        -------
        dict[str, Schema]
            Mapping of schema name → Schema object.
        """
        from dataforge.schema_io import _detect_format

        fmt = _detect_format(path)
        if fmt == "json":
            import json

            with open(path, "r", encoding="utf-8") as f:
                doc = json.load(f)
        elif fmt in ("yaml", "yml"):
            from dataforge.schema_io import _load_yaml

            doc = _load_yaml(path)
        else:
            import json

            with open(path, "r", encoding="utf-8") as f:
                doc = json.load(f)

        return self.from_openapi(doc)

    def from_openapi(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Parse an OpenAPI document dict.

        Parameters
        ----------
        doc : dict
            The parsed OpenAPI document.

        Returns
        -------
        dict[str, Schema]
        """
        self._ref_cache = doc  # store full doc for $ref resolution
        schemas: dict[str, Any] = {}

        # OpenAPI 3.x: components.schemas
        components = doc.get("components", {})
        schema_defs = components.get("schemas", {})

        for name, schema_def in schema_defs.items():
            resolved = self._resolve_refs(schema_def)
            if resolved.get("type") == "object":
                try:
                    schema = self._build_schema(resolved, name)
                    schemas[name] = schema
                except (ValueError, KeyError):
                    pass  # Skip schemas we can't map

        return schemas

    def from_json_schema(
        self,
        schema_def: dict[str, Any],
        name: str = "root",
    ) -> Any:
        """Create a Schema from a JSON Schema definition.

        Parameters
        ----------
        schema_def : dict
            JSON Schema object definition.
        name : str
            Schema name for error messages.

        Returns
        -------
        Schema
        """
        resolved = self._resolve_refs(schema_def)
        return self._build_schema(resolved, name)

    def _resolve_refs(self, obj: Any) -> Any:
        """Recursively resolve $ref references."""
        if isinstance(obj, dict):
            if "$ref" in obj:
                return self._resolve_ref(obj["$ref"])
            return {k: self._resolve_refs(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._resolve_refs(item) for item in obj]
        return obj

    def _resolve_ref(self, ref: str) -> Any:
        """Resolve a single $ref path like '#/components/schemas/User'."""
        if not ref.startswith("#/"):
            return {}  # External refs not supported
        parts = ref[2:].split("/")
        obj: Any = self._ref_cache
        for part in parts:
            if isinstance(obj, dict):
                obj = obj.get(part, {})
            else:
                return {}
        return self._resolve_refs(obj)

    def _build_schema(
        self,
        schema_def: dict[str, Any],
        name: str,
    ) -> Any:
        """Build a DataForge Schema from a resolved JSON Schema object."""
        from dataforge.schema import Schema

        properties = schema_def.get("properties", {})
        if not properties:
            raise ValueError(f"Schema '{name}' has no properties.")

        field_map: dict[str, Any] = {}

        for prop_name, prop_def in properties.items():
            field = self._map_property(prop_name, prop_def)
            if field is not None:
                field_map[prop_name] = field

        if not field_map:
            raise ValueError(
                f"No properties in schema '{name}' could be mapped to DataForge fields."
            )

        return Schema(self._forge, field_map)

    def _map_property(
        self,
        prop_name: str,
        prop_def: dict[str, Any],
    ) -> str | None:
        """Map a single property to a DataForge field name."""
        schema_type = prop_def.get("type", "string")
        schema_format = prop_def.get("format")

        # Handle enum
        if "enum" in prop_def:
            # For enums, we'll use a lambda with the enum values
            # For simplicity, return None and handle in the caller
            return None  # TODO: enum support via lambda

        # Handle arrays
        if schema_type == "array":
            return None  # Skip arrays for now

        # Handle nested objects
        if schema_type == "object":
            return None  # Skip nested objects for now

        # Check type+format mapping
        key = (schema_type, schema_format)
        mapped = _TYPE_FORMAT_MAP.get(key)
        if mapped is not None:
            return mapped

        # Check without format
        key_nofmt = (schema_type, None)
        mapped = _TYPE_FORMAT_MAP.get(key_nofmt)
        if mapped is not None:
            return mapped

        # Property name heuristic
        name_lower = prop_name.lower().replace("-", "_")
        name_mapped = _PROPERTY_NAME_MAP.get(name_lower)
        if name_mapped:
            return name_mapped

        # Try to resolve via registry
        try:
            self._forge._resolve_field(prop_name)
            return prop_name
        except ValueError:
            pass

        # Numeric type fallback with range
        if schema_type in ("integer", "number"):
            minimum = prop_def.get("minimum")
            maximum = prop_def.get("maximum")
            if minimum is not None or maximum is not None:
                # Use a lambda for range-constrained numbers
                return None  # TODO: range constraint support
            return None

        # Fallback for strings
        if schema_type == "string":
            # Check minLength/maxLength, pattern
            pattern = prop_def.get("pattern")
            if pattern:
                return None  # TODO: regexify support

            # Generic string fallback
            return "lorem.word"

        return None

    def __repr__(self) -> str:
        return "OpenAPIParser()"
