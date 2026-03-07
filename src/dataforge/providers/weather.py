"""WeatherProvider — generates fake weather data.

Includes conditions, temperatures, humidity, wind speed/direction,
UV index, air quality, and weather alerts.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from typing import Literal, overload

from dataforge.providers.base import BaseProvider

# ------------------------------------------------------------------
# Data tuples (immutable, module-level for zero per-call overhead)
# ------------------------------------------------------------------

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
    "Tornado",
    "Hurricane",
    "Blizzard",
    "Dust Storm",
    "Tropical Storm",
    "Ice Storm",
    "Scattered Showers",
    "Partly Sunny",
    "Mostly Sunny",
    "Mostly Cloudy",
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
    "Freeze Warning",
    "Hurricane Watch",
    "Hurricane Warning",
    "Tropical Storm Warning",
    "Coastal Flood Advisory",
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

    # ------------------------------------------------------------------
    # Scalar helpers
    # ------------------------------------------------------------------

    def _one_temperature(self) -> str:
        """Generate a single temperature string in Fahrenheit."""
        return f"{self._engine.random_int(-20, 120)}°F"

    def _one_humidity(self) -> str:
        """Generate a single humidity percentage string."""
        return f"{self._engine.random_int(0, 100)}%"

    def _one_wind_speed(self) -> str:
        """Generate a single wind speed string."""
        return f"{self._engine.random_int(0, 150)} mph"

    def _one_uv_index(self) -> str:
        """Generate a single UV index string."""
        return str(self._engine.random_int(0, 11))

    def _one_pressure(self) -> str:
        """Generate a single barometric pressure string."""
        ri = self._engine.random_int
        whole = ri(28, 31)
        frac = ri(0, 99)
        return f"{whole}.{frac:02d} inHg"

    def _one_visibility(self) -> str:
        """Generate a single visibility string."""
        ri = self._engine.random_int
        return f"{ri(0, 30)} mi"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @overload
    def condition(self) -> str: ...
    @overload
    def condition(self, count: Literal[1]) -> str: ...
    @overload
    def condition(self, count: int) -> str | list[str]: ...
    def condition(self, count: int = 1) -> str | list[str]:
        """Generate a weather condition (e.g. ``"Sunny"``).

        Parameters
        ----------
        count : int
            Number of conditions to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_CONDITIONS)
        return self._engine.choices(_CONDITIONS, count)

    @overload
    def temperature(self) -> str: ...
    @overload
    def temperature(self, count: Literal[1]) -> str: ...
    @overload
    def temperature(self, count: int) -> str | list[str]: ...
    def temperature(self, count: int = 1) -> str | list[str]:
        """Generate a temperature (e.g. ``"72°F"``).

        Parameters
        ----------
        count : int
            Number of temperatures to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_temperature()
        return [self._one_temperature() for _ in range(count)]

    @overload
    def humidity(self) -> str: ...
    @overload
    def humidity(self, count: Literal[1]) -> str: ...
    @overload
    def humidity(self, count: int) -> str | list[str]: ...
    def humidity(self, count: int = 1) -> str | list[str]:
        """Generate a humidity percentage (e.g. ``"65%"``).

        Parameters
        ----------
        count : int
            Number of humidity values to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_humidity()
        return [self._one_humidity() for _ in range(count)]

    @overload
    def wind_speed(self) -> str: ...
    @overload
    def wind_speed(self, count: Literal[1]) -> str: ...
    @overload
    def wind_speed(self, count: int) -> str | list[str]: ...
    def wind_speed(self, count: int = 1) -> str | list[str]:
        """Generate a wind speed (e.g. ``"15 mph"``).

        Parameters
        ----------
        count : int
            Number of wind speeds to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_wind_speed()
        return [self._one_wind_speed() for _ in range(count)]

    @overload
    def wind_direction(self) -> str: ...
    @overload
    def wind_direction(self, count: Literal[1]) -> str: ...
    @overload
    def wind_direction(self, count: int) -> str | list[str]: ...
    def wind_direction(self, count: int = 1) -> str | list[str]:
        """Generate a wind direction (e.g. ``"NW"``).

        Parameters
        ----------
        count : int
            Number of wind directions to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_WIND_DIRECTIONS)
        return self._engine.choices(_WIND_DIRECTIONS, count)

    @overload
    def uv_index(self) -> str: ...
    @overload
    def uv_index(self, count: Literal[1]) -> str: ...
    @overload
    def uv_index(self, count: int) -> str | list[str]: ...
    def uv_index(self, count: int = 1) -> str | list[str]:
        """Generate a UV index (e.g. ``"7"``).

        Parameters
        ----------
        count : int
            Number of UV index values to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_uv_index()
        return [self._one_uv_index() for _ in range(count)]

    @overload
    def air_quality(self) -> str: ...
    @overload
    def air_quality(self, count: Literal[1]) -> str: ...
    @overload
    def air_quality(self, count: int) -> str | list[str]: ...
    def air_quality(self, count: int = 1) -> str | list[str]:
        """Generate an air quality level (e.g. ``"Good"``).

        Parameters
        ----------
        count : int
            Number of air quality levels to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_AIR_QUALITY)
        return self._engine.choices(_AIR_QUALITY, count)

    @overload
    def alert(self) -> str: ...
    @overload
    def alert(self, count: Literal[1]) -> str: ...
    @overload
    def alert(self, count: int) -> str | list[str]: ...
    def alert(self, count: int = 1) -> str | list[str]:
        """Generate a weather alert (e.g. ``"Tornado Watch"``).

        Parameters
        ----------
        count : int
            Number of alerts to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_ALERTS)
        return self._engine.choices(_ALERTS, count)

    @overload
    def cloud_type(self) -> str: ...
    @overload
    def cloud_type(self, count: Literal[1]) -> str: ...
    @overload
    def cloud_type(self, count: int) -> str | list[str]: ...
    def cloud_type(self, count: int = 1) -> str | list[str]:
        """Generate a cloud type (e.g. ``"Cumulus"``).

        Parameters
        ----------
        count : int
            Number of cloud types to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_CLOUD_TYPES)
        return self._engine.choices(_CLOUD_TYPES, count)

    @overload
    def season(self) -> str: ...
    @overload
    def season(self, count: Literal[1]) -> str: ...
    @overload
    def season(self, count: int) -> str | list[str]: ...
    def season(self, count: int = 1) -> str | list[str]:
        """Generate a season (e.g. ``"Summer"``).

        Parameters
        ----------
        count : int
            Number of seasons to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_SEASONS)
        return self._engine.choices(_SEASONS, count)

    @overload
    def pressure(self) -> str: ...
    @overload
    def pressure(self, count: Literal[1]) -> str: ...
    @overload
    def pressure(self, count: int) -> str | list[str]: ...
    def pressure(self, count: int = 1) -> str | list[str]:
        """Generate barometric pressure (e.g. ``"29.92 inHg"``).

        Parameters
        ----------
        count : int
            Number of pressure values to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_pressure()
        return [self._one_pressure() for _ in range(count)]

    @overload
    def visibility(self) -> str: ...
    @overload
    def visibility(self, count: Literal[1]) -> str: ...
    @overload
    def visibility(self, count: int) -> str | list[str]: ...
    def visibility(self, count: int = 1) -> str | list[str]:
        """Generate visibility distance (e.g. ``"10 mi"``).

        Parameters
        ----------
        count : int
            Number of visibility values to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_visibility()
        return [self._one_visibility() for _ in range(count)]
