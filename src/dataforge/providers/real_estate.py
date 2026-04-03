"""RealEstateProvider — generates fake real estate data.

Includes property types, listing prices, square footage, room counts,
neighborhoods, building materials, and listing statuses.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from dataforge.providers.base import BaseProvider

# Data tuples (immutable, module-level for zero per-call overhead)

_PROPERTY_TYPES: tuple[str, ...] = (
    "Single Family Home",
    "Condo",
    "Townhouse",
    "Apartment",
    "Duplex",
    "Triplex",
    "Penthouse",
    "Studio",
    "Loft",
    "Villa",
    "Cottage",
    "Bungalow",
    "Ranch",
    "Colonial",
    "Victorian",
    "Modern",
    "Contemporary",
    "Mediterranean",
    "Tudor",
    "Cape Cod",
)

_NEIGHBORHOODS: tuple[str, ...] = (
    "Downtown",
    "Midtown",
    "Uptown",
    "Westside",
    "Eastside",
    "Northgate",
    "Southpark",
    "Riverside",
    "Lakewood",
    "Hillcrest",
    "Oakwood",
    "Maplewood",
    "Cedar Heights",
    "Pine Valley",
    "Sunset Hills",
    "Greenfield",
    "Brookside",
    "Fairview",
    "Heritage Park",
    "Eagle Ridge",
)

_BUILDING_MATERIALS: tuple[str, ...] = (
    "Brick",
    "Wood Frame",
    "Concrete",
    "Steel Frame",
    "Stone",
    "Stucco",
    "Vinyl Siding",
    "Aluminum Siding",
    "Fiber Cement",
    "Log",
    "Adobe",
    "Glass",
    "Timber Frame",
    "Precast Concrete",
    "Insulated Concrete Form",
)

_LISTING_STATUSES: tuple[str, ...] = (
    "For Sale",
    "For Rent",
    "Pending",
    "Sold",
    "Under Contract",
    "Contingent",
    "Active",
    "Coming Soon",
    "Off Market",
    "Foreclosure",
    "Short Sale",
    "Auction",
)

_AMENITIES: tuple[str, ...] = (
    "Swimming Pool",
    "Garage",
    "Garden",
    "Fireplace",
    "Central AC",
    "Hardwood Floors",
    "Granite Countertops",
    "Stainless Steel Appliances",
    "Walk-in Closet",
    "Balcony",
    "Patio",
    "Deck",
    "Hot Tub",
    "Home Office",
    "Gym",
    "Wine Cellar",
    "Smart Home",
    "Solar Panels",
    "EV Charger",
    "Security System",
)

_HEATING_TYPES: tuple[str, ...] = (
    "Forced Air",
    "Radiant",
    "Baseboard",
    "Heat Pump",
    "Geothermal",
    "Steam",
    "Electric",
    "Gas",
    "Oil",
    "Wood Stove",
)


class RealEstateProvider(BaseProvider):
    """Generates fake real estate data.

    This provider is locale-independent.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "real_estate"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "property_type": "property_type",
        "listing_price": "listing_price",
        "square_footage": "square_footage",
        "sqft": "square_footage",
        "bedrooms": "bedrooms",
        "bathrooms": "bathrooms",
        "neighborhood": "neighborhood",
        "building_material": "building_material",
        "listing_status": "listing_status",
        "amenity": "amenity",
        "heating_type": "heating_type",
        "year_built": "year_built",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "property_type": _PROPERTY_TYPES,
        "neighborhood": _NEIGHBORHOODS,
        "building_material": _BUILDING_MATERIALS,
        "listing_status": _LISTING_STATUSES,
        "amenity": _AMENITIES,
        "heating_type": _HEATING_TYPES,
    }

    # Scalar helpers

    def _one_listing_price(self) -> str:
        """Generate a single listing price string."""
        ri = self._engine.random_int
        price = ri(50_000, 5_000_000)
        # Round to nearest thousand
        price = (price // 1000) * 1000
        return f"${price:,}"

    def _one_sqft(self) -> str:
        """Generate a single square footage string."""
        return f"{self._engine.random_int(400, 10000):,} sqft"

    def _one_bedrooms(self) -> str:
        """Generate a single bedroom count string."""
        return str(self._engine.random_int(1, 7))

    def _one_bathrooms(self) -> str:
        """Generate a single bathroom count string."""
        choices = ("1", "1.5", "2", "2.5", "3", "3.5", "4", "4.5", "5")
        return self._engine._rng.choice(choices)

    def _one_year_built(self) -> str:
        """Generate a single year built string."""
        return str(self._engine.random_int(1900, 2026))

    # Public API — custom methods

    def listing_price(self, count: int = 1) -> str | list[str]:
        """Generate a listing price (e.g. ``"$450,000"``)."""
        if count == 1:
            return self._one_listing_price()
        return [self._one_listing_price() for _ in range(count)]

    def square_footage(self, count: int = 1) -> str | list[str]:
        """Generate square footage (e.g. ``"2,400 sqft"``)."""
        if count == 1:
            return self._one_sqft()
        return [self._one_sqft() for _ in range(count)]

    def bedrooms(self, count: int = 1) -> str | list[str]:
        """Generate a bedroom count (e.g. ``"3"``)."""
        if count == 1:
            return self._one_bedrooms()
        return [self._one_bedrooms() for _ in range(count)]

    def bathrooms(self, count: int = 1) -> str | list[str]:
        """Generate a bathroom count (e.g. ``"2.5"``)."""
        if count == 1:
            return self._one_bathrooms()
        return [self._one_bathrooms() for _ in range(count)]

    def year_built(self, count: int = 1) -> str | list[str]:
        """Generate a year built (e.g. ``"1985"``)."""
        if count == 1:
            return self._one_year_built()
        return [self._one_year_built() for _ in range(count)]
