"""LogisticsProvider — generates fake logistics and shipping data.

Includes carriers, shipping methods, container types, warehouse names,
tracking statuses, incoterms, and customs codes.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from typing import Literal, overload

from dataforge.providers.base import BaseProvider

# ------------------------------------------------------------------
# Data tuples (immutable, module-level for zero per-call overhead)
# ------------------------------------------------------------------

_CARRIERS: tuple[str, ...] = (
    "FedEx",
    "UPS",
    "USPS",
    "DHL",
    "Amazon Logistics",
    "Maersk",
    "MSC",
    "CMA CGM",
    "Hapag-Lloyd",
    "Evergreen",
    "COSCO",
    "Yang Ming",
    "ONE",
    "ZIM",
    "HMM",
    "DB Schenker",
    "Kuehne + Nagel",
    "XPO Logistics",
    "C.H. Robinson",
    "J.B. Hunt",
    "Schneider National",
    "Old Dominion",
    "Saia",
    "Estes Express",
    "R+L Carriers",
    "YRC Freight",
    "ABF Freight",
    "Holland",
    "FedEx Freight",
    "UPS Freight",
)

_SHIPPING_METHODS: tuple[str, ...] = (
    "Ground",
    "Express",
    "Overnight",
    "Two-Day",
    "Economy",
    "Standard",
    "Priority",
    "Freight",
    "LTL",
    "FTL",
    "Air Freight",
    "Ocean Freight",
    "Rail Freight",
    "Same Day",
    "White Glove",
    "Parcel Post",
    "Registered Mail",
    "Certified Mail",
    "International Economy",
    "International Priority",
)

_CONTAINER_TYPES: tuple[str, ...] = (
    "20ft Standard",
    "40ft Standard",
    "40ft High Cube",
    "20ft Refrigerated",
    "40ft Refrigerated",
    "20ft Open Top",
    "40ft Open Top",
    "20ft Flat Rack",
    "40ft Flat Rack",
    "20ft Tank",
    "45ft High Cube",
    "20ft Ventilated",
    "40ft Double Door",
    "ISO Tank",
    "Bulk Container",
    "Platform Container",
    "Half Height Container",
    "Pallet Wide Container",
    "Swap Body",
    "Flexi Tank",
)

_TRACKING_STATUSES: tuple[str, ...] = (
    "Order Placed",
    "Processing",
    "Shipped",
    "In Transit",
    "Out for Delivery",
    "Delivered",
    "Attempted Delivery",
    "Returned to Sender",
    "Customs Clearance",
    "Held at Customs",
    "Released from Customs",
    "At Local Facility",
    "At Regional Hub",
    "Departed Origin",
    "Arrived at Destination",
    "Exception",
    "Lost",
    "Damaged",
    "Awaiting Pickup",
    "Picked Up",
)

_INCOTERMS: tuple[str, ...] = (
    "EXW",
    "FCA",
    "CPT",
    "CIP",
    "DAP",
    "DPU",
    "DDP",
    "FAS",
    "FOB",
    "CFR",
    "CIF",
)

_WAREHOUSE_ADJECTIVES: tuple[str, ...] = (
    "Central",
    "Pacific",
    "Atlantic",
    "Northern",
    "Southern",
    "Eastern",
    "Western",
    "Global",
    "National",
    "Regional",
    "Metro",
    "Inland",
    "Coastal",
    "Highland",
    "Lakeside",
    "Riverside",
    "Summit",
    "Valley",
    "Gateway",
    "Crossroads",
)

_WAREHOUSE_TYPES: tuple[str, ...] = (
    "Distribution Center",
    "Fulfillment Center",
    "Warehouse",
    "Logistics Hub",
    "Storage Facility",
    "Cross-Dock",
    "Cold Storage",
    "Bonded Warehouse",
    "Freight Terminal",
    "Sort Center",
)

_PACKAGE_TYPES: tuple[str, ...] = (
    "Box",
    "Envelope",
    "Tube",
    "Pallet",
    "Crate",
    "Barrel",
    "Bag",
    "Flat",
    "Roll",
    "Bundle",
    "Carton",
    "Drum",
    "Sack",
    "Tote",
    "Skid",
    "Padded Mailer",
    "Poly Bag",
    "Corrugated Box",
    "Wooden Crate",
    "Shrink Wrap Pallet",
)

_HS_PREFIXES: tuple[str, ...] = (
    "01",
    "02",
    "03",
    "04",
    "07",
    "08",
    "09",
    "10",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "27",
    "28",
    "29",
    "30",
    "39",
    "40",
    "42",
    "44",
    "48",
    "49",
    "50",
    "52",
    "54",
    "55",
    "61",
    "62",
    "63",
    "64",
    "70",
    "71",
    "72",
    "73",
    "76",
    "84",
    "85",
    "87",
    "90",
    "94",
    "95",
    "96",
)


class LogisticsProvider(BaseProvider):
    """Generates fake logistics and shipping data.

    This provider is locale-independent.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "logistics"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "carrier": "carrier",
        "shipping_carrier": "carrier",
        "shipping_method": "shipping_method",
        "container_type": "container_type",
        "container": "container_type",
        "tracking_status": "tracking_status",
        "shipment_status": "tracking_status",
        "incoterm": "incoterm",
        "warehouse": "warehouse",
        "warehouse_name": "warehouse",
        "package_type": "package_type",
        "hs_code": "hs_code",
        "customs_code": "hs_code",
        "shipping_weight": "shipping_weight",
        "freight_class": "freight_class",
    }

    # ------------------------------------------------------------------
    # Scalar helpers
    # ------------------------------------------------------------------

    def _one_warehouse(self) -> str:
        """Generate a single warehouse name."""
        choice = self._engine._rng.choice
        return f"{choice(_WAREHOUSE_ADJECTIVES)} {choice(_WAREHOUSE_TYPES)}"

    def _one_hs_code(self) -> str:
        """Generate a single HS (Harmonized System) code."""
        choice = self._engine._rng.choice
        ri = self._engine.random_int
        prefix = choice(_HS_PREFIXES)
        suffix = ri(10, 99)
        sub = ri(10, 99)
        return f"{prefix}{suffix}.{sub}"

    def _one_shipping_weight(self) -> str:
        """Generate a single shipping weight string."""
        ri = self._engine.random_int
        lbs = ri(1, 2000)
        oz = ri(0, 15)
        return f"{lbs}.{oz} lbs"

    def _one_freight_class(self) -> str:
        """Generate a single NMFC freight class."""
        classes = (
            "50",
            "55",
            "60",
            "65",
            "70",
            "77.5",
            "85",
            "92.5",
            "100",
            "110",
            "125",
            "150",
            "175",
            "200",
            "250",
            "300",
            "400",
            "500",
        )
        return self._engine._rng.choice(classes)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @overload
    def carrier(self) -> str: ...
    @overload
    def carrier(self, count: Literal[1]) -> str: ...
    @overload
    def carrier(self, count: int) -> str | list[str]: ...
    def carrier(self, count: int = 1) -> str | list[str]:
        """Generate a shipping carrier (e.g. ``"FedEx"``).

        Parameters
        ----------
        count : int
            Number of carrier names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_CARRIERS)
        return self._engine.choices(_CARRIERS, count)

    @overload
    def shipping_method(self) -> str: ...
    @overload
    def shipping_method(self, count: Literal[1]) -> str: ...
    @overload
    def shipping_method(self, count: int) -> str | list[str]: ...
    def shipping_method(self, count: int = 1) -> str | list[str]:
        """Generate a shipping method (e.g. ``"Express"``).

        Parameters
        ----------
        count : int
            Number of shipping methods to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_SHIPPING_METHODS)
        return self._engine.choices(_SHIPPING_METHODS, count)

    @overload
    def container_type(self) -> str: ...
    @overload
    def container_type(self, count: Literal[1]) -> str: ...
    @overload
    def container_type(self, count: int) -> str | list[str]: ...
    def container_type(self, count: int = 1) -> str | list[str]:
        """Generate a container type (e.g. ``"40ft High Cube"``).

        Parameters
        ----------
        count : int
            Number of container types to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_CONTAINER_TYPES)
        return self._engine.choices(_CONTAINER_TYPES, count)

    @overload
    def tracking_status(self) -> str: ...
    @overload
    def tracking_status(self, count: Literal[1]) -> str: ...
    @overload
    def tracking_status(self, count: int) -> str | list[str]: ...
    def tracking_status(self, count: int = 1) -> str | list[str]:
        """Generate a tracking status (e.g. ``"In Transit"``).

        Parameters
        ----------
        count : int
            Number of tracking statuses to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_TRACKING_STATUSES)
        return self._engine.choices(_TRACKING_STATUSES, count)

    @overload
    def incoterm(self) -> str: ...
    @overload
    def incoterm(self, count: Literal[1]) -> str: ...
    @overload
    def incoterm(self, count: int) -> str | list[str]: ...
    def incoterm(self, count: int = 1) -> str | list[str]:
        """Generate an Incoterm (e.g. ``"FOB"``).

        Parameters
        ----------
        count : int
            Number of Incoterms to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_INCOTERMS)
        return self._engine.choices(_INCOTERMS, count)

    @overload
    def warehouse(self) -> str: ...
    @overload
    def warehouse(self, count: Literal[1]) -> str: ...
    @overload
    def warehouse(self, count: int) -> str | list[str]: ...
    def warehouse(self, count: int = 1) -> str | list[str]:
        """Generate a warehouse name (e.g. ``"Pacific Fulfillment Center"``).

        Parameters
        ----------
        count : int
            Number of warehouse names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_warehouse()
        return [self._one_warehouse() for _ in range(count)]

    @overload
    def package_type(self) -> str: ...
    @overload
    def package_type(self, count: Literal[1]) -> str: ...
    @overload
    def package_type(self, count: int) -> str | list[str]: ...
    def package_type(self, count: int = 1) -> str | list[str]:
        """Generate a package type (e.g. ``"Box"``).

        Parameters
        ----------
        count : int
            Number of package types to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_PACKAGE_TYPES)
        return self._engine.choices(_PACKAGE_TYPES, count)

    @overload
    def hs_code(self) -> str: ...
    @overload
    def hs_code(self, count: Literal[1]) -> str: ...
    @overload
    def hs_code(self, count: int) -> str | list[str]: ...
    def hs_code(self, count: int = 1) -> str | list[str]:
        """Generate an HS (Harmonized System) code (e.g. ``"8471.30"``).

        Parameters
        ----------
        count : int
            Number of HS codes to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_hs_code()
        return [self._one_hs_code() for _ in range(count)]

    @overload
    def shipping_weight(self) -> str: ...
    @overload
    def shipping_weight(self, count: Literal[1]) -> str: ...
    @overload
    def shipping_weight(self, count: int) -> str | list[str]: ...
    def shipping_weight(self, count: int = 1) -> str | list[str]:
        """Generate a shipping weight (e.g. ``"45.8 lbs"``).

        Parameters
        ----------
        count : int
            Number of weights to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_shipping_weight()
        return [self._one_shipping_weight() for _ in range(count)]

    @overload
    def freight_class(self) -> str: ...
    @overload
    def freight_class(self, count: Literal[1]) -> str: ...
    @overload
    def freight_class(self, count: int) -> str | list[str]: ...
    def freight_class(self, count: int = 1) -> str | list[str]:
        """Generate an NMFC freight class (e.g. ``"85"``).

        Parameters
        ----------
        count : int
            Number of freight classes to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_freight_class()
        return [self._one_freight_class() for _ in range(count)]
