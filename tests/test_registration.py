"""Tests for programmatic provider registration."""

from typing import Literal, overload

from dataforge import DataForge
from dataforge.providers.base import BaseProvider


class WeatherProvider(BaseProvider):
    """Test provider for registration tests."""

    __slots__ = ()

    _provider_name = "weather"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "weather_condition": "condition",
        "condition": "condition",
        "temperature": "temperature",
    }

    _CONDITIONS: tuple[str, ...] = (
        "Sunny",
        "Cloudy",
        "Rainy",
        "Snowy",
        "Foggy",
    )

    @overload
    def condition(self) -> str: ...
    @overload
    def condition(self, count: Literal[1]) -> str: ...
    @overload
    def condition(self, count: int) -> str | list[str]: ...
    def condition(self, count: int = 1) -> str | list[str]:
        if count == 1:
            return self._engine.choice(self._CONDITIONS)
        return self._engine.choices(self._CONDITIONS, count)

    @overload
    def temperature(self) -> str: ...
    @overload
    def temperature(self, count: Literal[1]) -> str: ...
    @overload
    def temperature(self, count: int) -> str | list[str]: ...
    def temperature(self, count: int = 1) -> str | list[str]:
        if count == 1:
            return f"{self._engine.random_int(-20, 45)}°C"
        _ri = self._engine.random_int
        return [f"{_ri(-20, 45)}°C" for _ in range(count)]


class TestRegisterProvider:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_register_and_access(self) -> None:
        self.forge.register_provider(WeatherProvider)
        result = self.forge.weather.condition()
        assert isinstance(result, str)
        assert result in WeatherProvider._CONDITIONS

    def test_register_batch(self) -> None:
        self.forge.register_provider(WeatherProvider)
        results = self.forge.weather.condition(count=10)
        assert isinstance(results, list)
        assert len(results) == 10
        for r in results:
            assert r in WeatherProvider._CONDITIONS

    def test_register_temperature(self) -> None:
        self.forge.register_provider(WeatherProvider)
        temp = self.forge.weather.temperature()
        assert isinstance(temp, str)
        assert temp.endswith("°C")

    def test_register_with_custom_name(self) -> None:
        self.forge.register_provider(WeatherProvider, name="wx")
        result = self.forge.wx.condition()
        assert isinstance(result, str)

    def test_registered_provider_in_schema(self) -> None:
        self.forge.register_provider(WeatherProvider)
        schema = self.forge.schema(["condition", "temperature"])
        rows = schema.generate(count=5)
        assert len(rows) == 5
        for row in rows:
            assert "condition" in row
            assert "temperature" in row

    def test_registered_provider_in_to_dict(self) -> None:
        self.forge.register_provider(WeatherProvider)
        rows = self.forge.to_dict(
            fields=["first_name", "weather_condition"],
            count=5,
        )
        assert len(rows) == 5
        for row in rows:
            assert "first_name" in row
            assert "weather_condition" in row

    def test_register_no_name_raises(self) -> None:
        class BadProvider(BaseProvider):
            _provider_name = ""
            _field_map: dict[str, str] = {}

        import pytest

        with pytest.raises(ValueError, match="_provider_name"):
            self.forge.register_provider(BadProvider)

    def test_register_preserves_seed(self) -> None:
        forge1 = DataForge(locale="en_US", seed=42)
        forge2 = DataForge(locale="en_US", seed=42)
        forge1.register_provider(WeatherProvider)
        forge2.register_provider(WeatherProvider)
        assert forge1.weather.condition() == forge2.weather.condition()
