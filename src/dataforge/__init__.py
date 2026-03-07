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
"""

from dataforge.core import DataForge
from dataforge.schema import Schema
from dataforge.relational import RelationalSchema

__version__ = "0.3.0"
__all__ = ["DataForge", "Schema", "RelationalSchema", "__version__"]
