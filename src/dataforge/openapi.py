"""OpenAPI / JSON Schema import — generate fake data from API specs."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from dataforge.core import DataForge

_TYPE_FORMAT_MAP: dict[tuple[str, str | None], str] = {
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
    ("string", None): None,
    ("integer", None): None,
    ("integer", "int32"): None,
    ("integer", "int64"): None,
    ("number", None): None,
    ("number", "float"): None,
    ("number", "double"): None,
    ("boolean", None): "boolean",
}

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
    """Parse OpenAPI and JSON Schema documents into DataForge Schemas."""

    __slots__ = ("_forge", "_ref_cache")

    def __init__(self, forge: DataForge) -> None:
        self._forge = forge
        self._ref_cache: dict[str, Any] = {}

    def from_file(self, path: str) -> dict[str, Any]:
        """Parse an OpenAPI spec file and return schemas."""
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
        """Parse an OpenAPI document dict."""
        self._ref_cache = doc
        schemas: dict[str, Any] = {}

        components = doc.get("components", {})
        schema_defs = components.get("schemas", {})

        for name, schema_def in schema_defs.items():
            resolved = self._resolve_refs(schema_def)
            if resolved.get("type") == "object":
                try:
                    schema = self._build_schema(resolved, name)
                    schemas[name] = schema
                except (ValueError, KeyError):
                    pass

        return schemas

    def from_json_schema(
        self,
        schema_def: dict[str, Any],
        name: str = "root",
    ) -> Any:
        """Create a Schema from a JSON Schema definition."""
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
            return {}
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

        if "enum" in prop_def:
            return None

        if schema_type == "array":
            return None

        if schema_type == "object":
            return None

        key = (schema_type, schema_format)
        mapped = _TYPE_FORMAT_MAP.get(key)
        if mapped is not None:
            return mapped

        key_nofmt = (schema_type, None)
        mapped = _TYPE_FORMAT_MAP.get(key_nofmt)
        if mapped is not None:
            return mapped

        name_lower = prop_name.lower().replace("-", "_")
        name_mapped = _PROPERTY_NAME_MAP.get(name_lower)
        if name_mapped:
            return name_mapped

        try:
            self._forge._resolve_field(prop_name)
            return prop_name
        except ValueError:
            pass

        if schema_type in ("integer", "number"):
            minimum = prop_def.get("minimum")
            maximum = prop_def.get("maximum")
            if minimum is not None or maximum is not None:
                return None
            return None

        if schema_type == "string":
            pattern = prop_def.get("pattern")
            if pattern:
                return None
            return "lorem.word"

        return None

    def __repr__(self) -> str:
        return "OpenAPIParser()"
