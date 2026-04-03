"""WeatherProvider — generates fake weather data.

Includes conditions, temperatures, humidity, wind speed/direction,
UV index, air quality, and weather alerts.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from dataforge.providers.base import BaseProvider

# Data tuples (immutable, module-level for zero per-call overhead)

_CONDITIONS: tuple[str, ...] = (
    "Sunny",
    "Partly Cloudy",
    "Cloudy",
    "Overcast",
    "Rain",
    "Light Rain",
    "Heavy Rain",
    "Drizzle",
    "Thunderstorm",
    "Snow",
    "Light Snow",
    "Heavy Snow",
    "Sleet",
    "Freezing Rain",
    "Hail",
    "Fog",
    "Mist",
    "Haze",
    "Windy",
    "Clear",
)

_WIND_DIRECTIONS: tuple[str, ...] = (
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

_ALERTS: tuple[str, ...] = (
    "Severe Thunderstorm Warning",
    "Tornado Watch",
    "Tornado Warning",
    "Flash Flood Warning",
    "Flood Watch",
    "Winter Storm Warning",
    "Winter Weather Advisory",
    "Blizzard Warning",
    "Ice Storm Warning",
    "Heat Advisory",
    "Excessive Heat Warning",
    "Wind Advisory",
    "High Wind Warning",
    "Dense Fog Advisory",
    "Frost Advisory",
)

_CLOUD_TYPES: tuple[str, ...] = (
    "Cumulus",
    "Stratus",
    "Cirrus",
    "Cumulonimbus",
    "Nimbostratus",
    "Altocumulus",
    "Altostratus",
    "Cirrocumulus",
    "Cirrostratus",
    "Stratocumulus",
)

_SEASONS: tuple[str, ...] = (
    "Spring",
    "Summer",
    "Autumn",
    "Winter",
)

_AIR_QUALITY: tuple[str, ...] = (
    "Good",
    "Moderate",
    "Unhealthy for Sensitive Groups",
    "Unhealthy",
    "Very Unhealthy",
    "Hazardous",
)


class WeatherProvider(BaseProvider):
    """Generates fake weather data.

    This provider is locale-independent.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "weather"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "weather_condition": "condition",
        "condition": "condition",
        "temperature": "temperature",
        "temp": "temperature",
        "humidity": "humidity",
        "wind_speed": "wind_speed",
        "wind_direction": "wind_direction",
        "uv_index": "uv_index",
        "air_quality": "air_quality",
        "weather_alert": "alert",
        "alert": "alert",
        "cloud_type": "cloud_type",
        "season": "season",
        "pressure": "pressure",
        "visibility": "visibility",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "condition": _CONDITIONS,
        "wind_direction": _WIND_DIRECTIONS,
        "air_quality": _AIR_QUALITY,
        "alert": _ALERTS,
        "cloud_type": _CLOUD_TYPES,
        "season": _SEASONS,
    }

    # Scalar helpers

    def _one_temperature(self) -> str:
        return f"{self._engine.random_int(-20, 120)}°F"

    def _one_humidity(self) -> str:
        return f"{self._engine.random_int(0, 100)}%"

    def _one_wind_speed(self) -> str:
        return f"{self._engine.random_int(0, 150)} mph"

    def _one_uv_index(self) -> str:
        return str(self._engine.random_int(0, 11))

    def _one_pressure(self) -> str:
        ri = self._engine.random_int
        whole = ri(28, 31)
        frac = ri(0, 99)
        return f"{whole}.{frac:02d} inHg"

    def _one_visibility(self) -> str:
        ri = self._engine.random_int
        return f"{ri(0, 30)} mi"

    # Public API — custom methods

    def temperature(self, count: int = 1) -> str | list[str]:
        """Generate a temperature (e.g. ``"72°F"``)."""
        if count == 1:
            return self._one_temperature()
        return [self._one_temperature() for _ in range(count)]

    def humidity(self, count: int = 1) -> str | list[str]:
        """Generate a humidity percentage (e.g. ``"65%"``)."""
        if count == 1:
            return self._one_humidity()
        return [self._one_humidity() for _ in range(count)]

    def wind_speed(self, count: int = 1) -> str | list[str]:
        """Generate a wind speed (e.g. ``"15 mph"``)."""
        if count == 1:
            return self._one_wind_speed()
        return [self._one_wind_speed() for _ in range(count)]

    def uv_index(self, count: int = 1) -> str | list[str]:
        """Generate a UV index (e.g. ``"7"``)."""
        if count == 1:
            return self._one_uv_index()
        return [self._one_uv_index() for _ in range(count)]

    def pressure(self, count: int = 1) -> str | list[str]:
        """Generate barometric pressure (e.g. ``"29.92 inHg"``)."""
        if count == 1:
            return self._one_pressure()
        return [self._one_pressure() for _ in range(count)]

    def visibility(self, count: int = 1) -> str | list[str]:
        """Generate visibility distance (e.g. ``"10 mi"``)."""
        if count == 1:
            return self._one_visibility()
        return [self._one_visibility() for _ in range(count)]
