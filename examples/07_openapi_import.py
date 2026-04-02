"""OpenAPI / JSON Schema Import — Generate Data from API Specs.

Real-world scenario: You have an OpenAPI specification for your REST API
and need to generate realistic test payloads. The OpenAPIParser reads your
spec (JSON or YAML), resolves $ref references, maps types and formats to
DataForge providers, and creates Schema objects that generate conforming data.

This example demonstrates:
- Parsing JSON Schema definitions
- Parsing OpenAPI 3.x specifications
- Resolving $ref references
- Generating data that conforms to the schema
- Type and format mapping (email, date-time, uuid, etc.)
"""

import json

from dataforge import DataForge
from dataforge.openapi import OpenAPIParser

forge = DataForge(seed=42)
parser = OpenAPIParser(forge)

# --- Example 1: Simple JSON Schema ---------------------------------------

print("=== Simple JSON Schema ===\n")

user_schema_def = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "email": {"type": "string", "format": "email"},
        "website": {"type": "string", "format": "uri"},
        "created_at": {"type": "string", "format": "date-time"},
        "is_active": {"type": "boolean"},
    },
}

user_schema = parser.from_json_schema(user_schema_def, name="User")
rows = user_schema.generate(count=5)

print("Generated users from JSON Schema:")
for row in rows:
    print(f"  {row}")
print()

# --- Example 2: Full OpenAPI 3.x specification ----------------------------

print("=== OpenAPI 3.x Specification ===\n")

openapi_doc = {
    "openapi": "3.0.0",
    "info": {"title": "E-Commerce API", "version": "1.0.0"},
    "paths": {},
    "components": {
        "schemas": {
            "Customer": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "format": "uuid"},
                    "first_name": {"type": "string"},
                    "last_name": {"type": "string"},
                    "email": {"type": "string", "format": "email"},
                    "phone": {"type": "string", "format": "phone"},
                    "city": {"type": "string"},
                    "created_at": {"type": "string", "format": "date-time"},
                },
            },
            "Product": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "format": "uuid"},
                    "title": {"type": "string"},
                    "description": {"type": "string"},
                    "company": {"type": "string"},
                    "url": {"type": "string", "format": "uri"},
                },
            },
            "Order": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "format": "uuid"},
                    "customer": {"$ref": "#/components/schemas/Customer"},
                    "created_at": {"type": "string", "format": "date-time"},
                    "ip_address": {"type": "string", "format": "ipv4"},
                },
            },
        },
    },
}

schemas = parser.from_openapi(openapi_doc)

print(f"Parsed {len(schemas)} schemas: {list(schemas.keys())}")
print()

# Generate data for each schema
for name, schema in schemas.items():
    print(f"--- {name} ---")
    rows = schema.generate(count=3)
    for row in rows:
        print(f"  {row}")
    print()

# --- Example 3: Schema with $ref references -------------------------------

print("=== $ref Resolution ===\n")

doc_with_refs = {
    "openapi": "3.0.0",
    "info": {"title": "Test API", "version": "1.0.0"},
    "paths": {},
    "components": {
        "schemas": {
            "Address": {
                "type": "object",
                "properties": {
                    "city": {"type": "string"},
                    "state": {"type": "string"},
                    "country": {"type": "string"},
                    "zip_code": {"type": "string"},
                },
            },
            "Person": {
                "type": "object",
                "properties": {
                    "first_name": {"type": "string"},
                    "last_name": {"type": "string"},
                    "email": {"type": "string", "format": "email"},
                    # Nested $ref - the parser flattens this
                    "address": {"$ref": "#/components/schemas/Address"},
                },
            },
        },
    },
}

schemas = parser.from_openapi(doc_with_refs)
print(f"Schemas found: {list(schemas.keys())}")
for name, schema in schemas.items():
    rows = schema.generate(count=2)
    print(f"\n{name}:")
    for row in rows:
        print(f"  {row}")
print()

# --- Example 4: Generate API test payloads --------------------------------

print("=== Generate API Test Payloads ===\n")

api_schema_def = {
    "type": "object",
    "properties": {
        "username": {"type": "string"},
        "email": {"type": "string", "format": "email"},
        "password": {"type": "string", "format": "password"},
        "ip_address": {"type": "string", "format": "ipv4"},
    },
}

payload_schema = parser.from_json_schema(api_schema_def, name="CreateUser")
payloads = payload_schema.generate(count=3)

print("API test payloads (ready for POST requests):")
for i, payload in enumerate(payloads):
    print(f"\n  Request {i + 1}:")
    print(f"  {json.dumps(payload, indent=4)}")
