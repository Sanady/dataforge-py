"""LogisticsProvider — generates fake logistics and shipping data.

Includes carriers, shipping methods, container types, warehouse names,
tracking statuses, incoterms, and customs codes.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from dataforge.providers.base import BaseProvider

# Data tuples (immutable, module-level for zero per-call overhead)

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

    _choice_fields: dict[str, tuple[str, ...]] = {
        "carrier": _CARRIERS,
        "shipping_method": _SHIPPING_METHODS,
        "container_type": _CONTAINER_TYPES,
        "tracking_status": _TRACKING_STATUSES,
        "incoterm": _INCOTERMS,
        "package_type": _PACKAGE_TYPES,
    }

    # Scalar helpers

    def _one_warehouse(self) -> str:
        choice = self._engine._rng.choice
        return f"{choice(_WAREHOUSE_ADJECTIVES)} {choice(_WAREHOUSE_TYPES)}"

    def _one_hs_code(self) -> str:
        choice = self._engine._rng.choice
        ri = self._engine.random_int
        prefix = choice(_HS_PREFIXES)
        suffix = ri(10, 99)
        sub = ri(10, 99)
        return f"{prefix}{suffix}.{sub}"

    def _one_shipping_weight(self) -> str:
        ri = self._engine.random_int
        lbs = ri(1, 2000)
        oz = ri(0, 15)
        return f"{lbs}.{oz} lbs"

    def _one_freight_class(self) -> str:
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

    # Public API — custom methods

    def warehouse(self, count: int = 1) -> str | list[str]:
        """Generate a warehouse name (e.g. ``"Pacific Fulfillment Center"``)."""
        if count == 1:
            return self._one_warehouse()
        return [self._one_warehouse() for _ in range(count)]

    def hs_code(self, count: int = 1) -> str | list[str]:
        """Generate an HS (Harmonized System) code (e.g. ``"8471.30"``)."""
        if count == 1:
            return self._one_hs_code()
        return [self._one_hs_code() for _ in range(count)]

    def shipping_weight(self, count: int = 1) -> str | list[str]:
        """Generate a shipping weight (e.g. ``"45.8 lbs"``)."""
        if count == 1:
            return self._one_shipping_weight()
        return [self._one_shipping_weight() for _ in range(count)]

    def freight_class(self, count: int = 1) -> str | list[str]:
        """Generate an NMFC freight class (e.g. ``"85"``)."""
        if count == 1:
            return self._one_freight_class()
        return [self._one_freight_class() for _ in range(count)]
