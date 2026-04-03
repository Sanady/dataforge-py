"""Geo provider — coordinates, countries, continents, places, distances."""

from dataforge.providers.base import BaseProvider

_CONTINENTS: tuple[str, ...] = (
    "Africa",
    "Antarctica",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
)

_OCEANS: tuple[str, ...] = (
    "Pacific Ocean",
    "Atlantic Ocean",
    "Indian Ocean",
    "Southern Ocean",
    "Arctic Ocean",
)

_SEAS: tuple[str, ...] = (
    "Mediterranean Sea",
    "Caribbean Sea",
    "South China Sea",
    "Bering Sea",
    "Gulf of Mexico",
    "Sea of Okhotsk",
    "Sea of Japan",
    "Hudson Bay",
    "East China Sea",
    "Andaman Sea",
    "Black Sea",
    "Red Sea",
    "North Sea",
    "Baltic Sea",
    "Caspian Sea",
)

_MOUNTAIN_RANGES: tuple[str, ...] = (
    "Himalayas",
    "Andes",
    "Alps",
    "Rocky Mountains",
    "Appalachian Mountains",
    "Ural Mountains",
    "Atlas Mountains",
    "Carpathian Mountains",
    "Scandinavian Mountains",
    "Pyrenees",
    "Caucasus Mountains",
    "Sierra Nevada",
    "Karakoram",
    "Hindu Kush",
    "Tian Shan",
)

_RIVERS: tuple[str, ...] = (
    "Amazon",
    "Nile",
    "Yangtze",
    "Mississippi",
    "Yenisei",
    "Yellow River",
    "Ob",
    "Congo",
    "Amur",
    "Lena",
    "Mekong",
    "Mackenzie",
    "Niger",
    "Murray",
    "Tocantins",
)

_COMPASS_DIRECTIONS: tuple[str, ...] = (
    "N",
    "NNE",
    "NE",
    "ENE",
    "E",
    "ESE",
    "SE",
    "SSE",
    "S",
    "SSW",
    "SW",
    "WSW",
    "W",
    "WNW",
    "NW",
    "NNW",
)

_COORDINATE_DMS_DIRS_LAT: tuple[str, ...] = ("N", "S")
_COORDINATE_DMS_DIRS_LON: tuple[str, ...] = ("E", "W")

# Geohash base32 alphabet — module-level constant to avoid per-call
# string creation inside _one_geo_hash().
_BASE32: str = "0123456789bcdefghjkmnpqrstuvwxyz"


class GeoProvider(BaseProvider):
    """Generates fake geographic data — coordinates, places, features."""

    __slots__ = ()

    _provider_name = "geo"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "continent": "continent",
        "ocean": "ocean",
        "sea": "sea",
        "mountain_range": "mountain_range",
        "river": "river",
        "compass_direction": "compass_direction",
        "compass": "compass_direction",
        "geo_coordinate": "geo_coordinate",
        "dms_latitude": "dms_latitude",
        "dms_longitude": "dms_longitude",
        "geo_hash": "geo_hash",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "continent": _CONTINENTS,
        "ocean": _OCEANS,
        "sea": _SEAS,
        "mountain_range": _MOUNTAIN_RANGES,
        "river": _RIVERS,
        "compass_direction": _COMPASS_DIRECTIONS,
    }

    def _one_geo_coordinate(self) -> str:
        lat = self._engine.random_int(-9000, 9000) / 100.0
        lon = self._engine.random_int(-18000, 18000) / 100.0
        return f"{lat:.4f}, {lon:.4f}"

    def _one_dms_lat(self) -> str:
        deg = self._engine.random_int(0, 90)
        mins = self._engine.random_int(0, 59)
        secs = self._engine.random_int(0, 59)
        d = self._engine.choice(_COORDINATE_DMS_DIRS_LAT)
        return f"{deg}°{mins:02d}'{secs:02d}\"{d}"

    def _one_dms_lon(self) -> str:
        deg = self._engine.random_int(0, 180)
        mins = self._engine.random_int(0, 59)
        secs = self._engine.random_int(0, 59)
        d = self._engine.choice(_COORDINATE_DMS_DIRS_LON)
        return f"{deg}°{mins:02d}'{secs:02d}\"{d}"

    def _one_geo_hash(self) -> str:
        # Geohash: base32 string, typically 6-12 chars
        length = self._engine.random_int(6, 12)
        bits = self._engine.getrandbits(length * 5)
        chars: list[str] = []
        b32 = _BASE32
        for _ in range(length):
            chars.append(b32[bits & 0x1F])
            bits >>= 5
        return "".join(chars)

    def geo_coordinate(self, count: int = 1) -> str | list[str]:
        """Generate a geographic coordinate pair (lat, lon)."""
        if count == 1:
            return self._one_geo_coordinate()
        return [self._one_geo_coordinate() for _ in range(count)]

    def dms_latitude(self, count: int = 1) -> str | list[str]:
        """Generate a latitude in degrees-minutes-seconds format."""
        if count == 1:
            return self._one_dms_lat()
        return [self._one_dms_lat() for _ in range(count)]

    def dms_longitude(self, count: int = 1) -> str | list[str]:
        """Generate a longitude in degrees-minutes-seconds format."""
        if count == 1:
            return self._one_dms_lon()
        return [self._one_dms_lon() for _ in range(count)]

    def geo_hash(self, count: int = 1) -> str | list[str]:
        """Generate a geohash string (base32, 6-12 chars)."""
        if count == 1:
            return self._one_geo_hash()
        return [self._one_geo_hash() for _ in range(count)]
