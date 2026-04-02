"""dataforge — High-performance fake data generator for testing.

Usage::

    from dataforge import DataForge

    forge = DataForge(locale="en_US", seed=42)
    forge.person.first_name()           # "James"
    forge.address.full_address()        # "4821 Oak Ave, Chicago, IL 60614"
    forge.person.full_name(count=1000)  # list of 1000 full names

    # Unique values
    forge.unique.person.first_name()    # guaranteed unique per call

    # Introspection
    DataForge.list_providers()          # ["address", "company", ...]
    DataForge.list_fields()             # {"first_name": ("person", ...), ...}

    # Relational data
    from dataforge import RelationalSchema
    rel = forge.relational({...})       # multi-table with FKs

    # Schema serialization
    from dataforge import save_schema, load_schema
    schema = forge.schema(["first_name", "email"])
    schema.save_schema("my_schema.yaml")
    loaded = forge.schema_from_file("my_schema.yaml")

    # Time-series generation
    ts = forge.timeseries(start="2024-01-01", end="2024-12-31", interval="1h")
    rows = ts.generate()

    # Schema inference
    s = forge.infer_schema([{"name": "Alice", "email": "alice@ex.com"}])

    # Chaos mode (data quality testing)
    from dataforge.chaos import ChaosTransformer
    s = forge.schema(["first_name", "email"], chaos=ChaosTransformer(null_rate=0.1))

    # Data anonymization
    from dataforge.anonymizer import Anonymizer

    # Database seeding
    from dataforge.seeder import DatabaseSeeder

    # OpenAPI schema import
    from dataforge.openapi import OpenAPIParser

    # Streaming to HTTP / Kafka / RabbitMQ
    from dataforge.streaming import HttpEmitter, KafkaEmitter
"""

from dataforge.core import DataForge
from dataforge.schema import Schema
from dataforge.relational import RelationalSchema
from dataforge.schema_io import save_schema, load_schema

__version__ = "0.3.0"
__all__ = [
    "DataForge",
    "Schema",
    "RelationalSchema",
    "save_schema",
    "load_schema",
    "__version__",
]
