"""RealEstateProvider — generates fake real estate data.

Includes property types, listing prices, square footage, room counts,
neighborhoods, building materials, and listing statuses.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from typing import Literal, overload

from dataforge.providers.base import BaseProvider

# ------------------------------------------------------------------
# Data tuples (immutable, module-level for zero per-call overhead)
# ------------------------------------------------------------------

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
    "Farmhouse",
    "Cabin",
    "Mansion",
    "Mobile Home",
    "Co-op",
    "Land",
    "Commercial",
    "Industrial",
    "Mixed Use",
    "Warehouse",
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
    "Harbor Point",
    "Bay View",
    "Forest Glen",
    "Meadow Creek",
    "Stonebridge",
    "Willow Springs",
    "Coral Gables",
    "Silver Lake",
    "Mission Hills",
    "Pacific Heights",
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
    "Structural Insulated Panel",
    "Cross-Laminated Timber",
    "Rammed Earth",
    "Cob",
    "Hempcrete",
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
    "Laundry Room",
    "Basement",
    "Attic",
    "Rooftop Terrace",
    "Elevator",
    "Concierge",
    "Doorman",
    "Pet-Friendly",
    "In-Unit Washer/Dryer",
    "Storage Unit",
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

    # ------------------------------------------------------------------
    # Scalar helpers
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @overload
    def property_type(self) -> str: ...
    @overload
    def property_type(self, count: Literal[1]) -> str: ...
    @overload
    def property_type(self, count: int) -> str | list[str]: ...
    def property_type(self, count: int = 1) -> str | list[str]:
        """Generate a property type (e.g. ``"Condo"``).

        Parameters
        ----------
        count : int
            Number of property types to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_PROPERTY_TYPES)
        return self._engine.choices(_PROPERTY_TYPES, count)

    @overload
    def listing_price(self) -> str: ...
    @overload
    def listing_price(self, count: Literal[1]) -> str: ...
    @overload
    def listing_price(self, count: int) -> str | list[str]: ...
    def listing_price(self, count: int = 1) -> str | list[str]:
        """Generate a listing price (e.g. ``"$450,000"``).

        Parameters
        ----------
        count : int
            Number of listing prices to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_listing_price()
        return [self._one_listing_price() for _ in range(count)]

    @overload
    def square_footage(self) -> str: ...
    @overload
    def square_footage(self, count: Literal[1]) -> str: ...
    @overload
    def square_footage(self, count: int) -> str | list[str]: ...
    def square_footage(self, count: int = 1) -> str | list[str]:
        """Generate square footage (e.g. ``"2,400 sqft"``).

        Parameters
        ----------
        count : int
            Number of square footages to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_sqft()
        return [self._one_sqft() for _ in range(count)]

    @overload
    def bedrooms(self) -> str: ...
    @overload
    def bedrooms(self, count: Literal[1]) -> str: ...
    @overload
    def bedrooms(self, count: int) -> str | list[str]: ...
    def bedrooms(self, count: int = 1) -> str | list[str]:
        """Generate a bedroom count (e.g. ``"3"``).

        Parameters
        ----------
        count : int
            Number of bedroom counts to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_bedrooms()
        return [self._one_bedrooms() for _ in range(count)]

    @overload
    def bathrooms(self) -> str: ...
    @overload
    def bathrooms(self, count: Literal[1]) -> str: ...
    @overload
    def bathrooms(self, count: int) -> str | list[str]: ...
    def bathrooms(self, count: int = 1) -> str | list[str]:
        """Generate a bathroom count (e.g. ``"2.5"``).

        Parameters
        ----------
        count : int
            Number of bathroom counts to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_bathrooms()
        return [self._one_bathrooms() for _ in range(count)]

    @overload
    def neighborhood(self) -> str: ...
    @overload
    def neighborhood(self, count: Literal[1]) -> str: ...
    @overload
    def neighborhood(self, count: int) -> str | list[str]: ...
    def neighborhood(self, count: int = 1) -> str | list[str]:
        """Generate a neighborhood name (e.g. ``"Hillcrest"``).

        Parameters
        ----------
        count : int
            Number of neighborhood names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_NEIGHBORHOODS)
        return self._engine.choices(_NEIGHBORHOODS, count)

    @overload
    def building_material(self) -> str: ...
    @overload
    def building_material(self, count: Literal[1]) -> str: ...
    @overload
    def building_material(self, count: int) -> str | list[str]: ...
    def building_material(self, count: int = 1) -> str | list[str]:
        """Generate a building material (e.g. ``"Brick"``).

        Parameters
        ----------
        count : int
            Number of building materials to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_BUILDING_MATERIALS)
        return self._engine.choices(_BUILDING_MATERIALS, count)

    @overload
    def listing_status(self) -> str: ...
    @overload
    def listing_status(self, count: Literal[1]) -> str: ...
    @overload
    def listing_status(self, count: int) -> str | list[str]: ...
    def listing_status(self, count: int = 1) -> str | list[str]:
        """Generate a listing status (e.g. ``"For Sale"``).

        Parameters
        ----------
        count : int
            Number of listing statuses to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_LISTING_STATUSES)
        return self._engine.choices(_LISTING_STATUSES, count)

    @overload
    def amenity(self) -> str: ...
    @overload
    def amenity(self, count: Literal[1]) -> str: ...
    @overload
    def amenity(self, count: int) -> str | list[str]: ...
    def amenity(self, count: int = 1) -> str | list[str]:
        """Generate a property amenity (e.g. ``"Swimming Pool"``).

        Parameters
        ----------
        count : int
            Number of amenities to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_AMENITIES)
        return self._engine.choices(_AMENITIES, count)

    @overload
    def heating_type(self) -> str: ...
    @overload
    def heating_type(self, count: Literal[1]) -> str: ...
    @overload
    def heating_type(self, count: int) -> str | list[str]: ...
    def heating_type(self, count: int = 1) -> str | list[str]:
        """Generate a heating type (e.g. ``"Forced Air"``).

        Parameters
        ----------
        count : int
            Number of heating types to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_HEATING_TYPES)
        return self._engine.choices(_HEATING_TYPES, count)

    @overload
    def year_built(self) -> str: ...
    @overload
    def year_built(self, count: Literal[1]) -> str: ...
    @overload
    def year_built(self, count: int) -> str | list[str]: ...
    def year_built(self, count: int = 1) -> str | list[str]:
        """Generate a year built (e.g. ``"1985"``).

        Parameters
        ----------
        count : int
            Number of year-built values to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_year_built()
        return [self._one_year_built() for _ in range(count)]
