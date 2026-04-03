"""AutomotiveProvider — generates fake vehicle-related data.

Includes license plates, VINs, vehicle makes, models, years, and colors.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from dataforge.providers.base import BaseProvider

# Data tuples (immutable, module-level for zero per-call overhead)

_VEHICLE_MAKES: tuple[str, ...] = (
    "Toyota",
    "Honda",
    "Ford",
    "Chevrolet",
    "BMW",
    "Mercedes-Benz",
    "Audi",
    "Volkswagen",
    "Hyundai",
    "Kia",
    "Nissan",
    "Subaru",
    "Mazda",
    "Lexus",
    "Tesla",
    "Jeep",
    "Ram",
    "GMC",
    "Dodge",
    "Buick",
)

_VEHICLE_MODELS: tuple[str, ...] = (
    "Camry",
    "Civic",
    "F-150",
    "Silverado",
    "3 Series",
    "C-Class",
    "A4",
    "Golf",
    "Elantra",
    "Forte",
    "Altima",
    "Outback",
    "CX-5",
    "RX",
    "Model 3",
    "Wrangler",
    "1500",
    "Sierra",
    "Charger",
    "Encore",
    "Escalade",
    "Navigator",
    "MDX",
    "Q50",
    "XC90",
    "911",
    "Range Rover",
    "F-Type",
    "Outlander",
    "Pacifica",
)

_VEHICLE_COLORS: tuple[str, ...] = (
    "Black",
    "White",
    "Silver",
    "Gray",
    "Red",
    "Blue",
    "Green",
    "Brown",
    "Beige",
    "Gold",
    "Orange",
    "Yellow",
    "Purple",
    "Burgundy",
    "Navy",
)

_PLATE_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_PLATE_DIGITS = "0123456789"

# VIN character set (excludes I, O, Q per standard)
_VIN_CHARS = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
_VIN_WEIGHTS = (8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2)
_VIN_TRANSLITERATE = {
    "A": 1,
    "B": 2,
    "C": 3,
    "D": 4,
    "E": 5,
    "F": 6,
    "G": 7,
    "H": 8,
    "J": 1,
    "K": 2,
    "L": 3,
    "M": 4,
    "N": 5,
    "P": 7,
    "R": 9,
    "S": 2,
    "T": 3,
    "U": 4,
    "V": 5,
    "W": 6,
    "X": 7,
    "Y": 8,
    "Z": 9,
    "0": 0,
    "1": 1,
    "2": 2,
    "3": 3,
    "4": 4,
    "5": 5,
    "6": 6,
    "7": 7,
    "8": 8,
    "9": 9,
}


class AutomotiveProvider(BaseProvider):
    """Generates fake automotive / vehicle data.

    This provider is locale-independent.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "automotive"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "license_plate": "license_plate",
        "vin": "vin",
        "vehicle_make": "vehicle_make",
        "vehicle_model": "vehicle_model",
        "vehicle_year": "vehicle_year_str",
        "vehicle_color": "vehicle_color",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "vehicle_make": _VEHICLE_MAKES,
        "vehicle_model": _VEHICLE_MODELS,
        "vehicle_color": _VEHICLE_COLORS,
    }

    # Scalar helpers

    def _one_plate(self) -> str:
        """Generate a single US-style license plate (ABC-1234)."""
        choice = self._engine._rng.choice
        a = choice(_PLATE_LETTERS)
        b = choice(_PLATE_LETTERS)
        c = choice(_PLATE_LETTERS)
        d = choice(_PLATE_DIGITS)
        e = choice(_PLATE_DIGITS)
        f = choice(_PLATE_DIGITS)
        g = choice(_PLATE_DIGITS)
        return f"{a}{b}{c}-{d}{e}{f}{g}"

    def _one_vin(self) -> str:
        """Generate a single 17-char VIN with valid check digit."""
        choices = self._engine._rng.choices
        # Positions: 0-7 (WMI + VDS), 8 (check digit), 9-16 (VIS)
        chars = choices(_VIN_CHARS, k=17)
        # Calculate check digit (position 8)
        total = 0
        for i, ch in enumerate(chars):
            if i == 8:
                continue  # skip check digit position
            total += _VIN_TRANSLITERATE[ch] * _VIN_WEIGHTS[i]
        remainder = total % 11
        chars[8] = "X" if remainder == 10 else str(remainder)
        return "".join(chars)

    # Public API

    def license_plate(self, count: int = 1) -> str | list[str]:
        """Generate a US-style license plate (e.g. ``"ABC-1234"``)."""
        if count == 1:
            return self._one_plate()
        return [self._one_plate() for _ in range(count)]

    def vin(self, count: int = 1) -> str | list[str]:
        """Generate a 17-character Vehicle Identification Number."""
        if count == 1:
            return self._one_vin()
        return [self._one_vin() for _ in range(count)]

    def vehicle_year(self, count: int = 1) -> int | list[int]:
        """Generate a vehicle model year (1990–2026)."""
        ri = self._engine.random_int
        if count == 1:
            return ri(1990, 2026)
        return [ri(1990, 2026) for _ in range(count)]

    def vehicle_year_str(self, count: int = 1) -> str | list[str]:
        """Generate a vehicle model year as a string (``"1990"``–``"2026"``)."""
        ri = self._engine.random_int
        if count == 1:
            return str(ri(1990, 2026))
        return [str(ri(1990, 2026)) for _ in range(count)]
