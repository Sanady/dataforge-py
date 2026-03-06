"""dataforge — High-performance fake data generator for testing.

Usage::

    from dataforge import DataForge

    forge = DataForge(locale="en_US", seed=42)
    forge.person.first_name()           # "James"
    forge.address.full_address()        # "4821 Oak Ave, Chicago, IL 60614"
    forge.person.full_name(count=1000)  # list of 1000 full names

    # Unique values
    forge.unique.person.first_name()    # guaranteed unique per call
"""

from dataforge.core import DataForge
from dataforge.schema import Schema

__version__ = "0.2.0"
__all__ = ["DataForge", "Schema", "__version__"]
