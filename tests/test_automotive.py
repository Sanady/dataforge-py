"""Tests for the AutomotiveProvider."""

import re

from dataforge import DataForge
from dataforge.providers.automotive import (
    _VEHICLE_COLORS,
    _VEHICLE_MAKES,
    _VEHICLE_MODELS,
)


class TestAutomotiveScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_license_plate_returns_str(self) -> None:
        result = self.forge.automotive.license_plate()
        assert isinstance(result, str)

    def test_license_plate_format(self) -> None:
        for _ in range(50):
            result = self.forge.automotive.license_plate()
            assert re.match(r"^[A-Z]{3}-\d{4}$", result), f"Bad plate: {result}"

    def test_vin_returns_str(self) -> None:
        result = self.forge.automotive.vin()
        assert isinstance(result, str)

    def test_vin_length(self) -> None:
        for _ in range(50):
            result = self.forge.automotive.vin()
            assert len(result) == 17, f"VIN wrong length: {result}"

    def test_vin_no_invalid_chars(self) -> None:
        for _ in range(100):
            result = self.forge.automotive.vin()
            assert "I" not in result
            assert "O" not in result
            assert "Q" not in result

    def test_vin_check_digit(self) -> None:
        from dataforge.providers.automotive import _VIN_TRANSLITERATE, _VIN_WEIGHTS

        for _ in range(50):
            vin = self.forge.automotive.vin()
            total = 0
            for i, ch in enumerate(vin):
                if i == 8:
                    continue
                total += _VIN_TRANSLITERATE[ch] * _VIN_WEIGHTS[i]
            remainder = total % 11
            expected = "X" if remainder == 10 else str(remainder)
            assert vin[8] == expected, f"Bad check digit in {vin}"

    def test_vehicle_make_returns_str(self) -> None:
        result = self.forge.automotive.vehicle_make()
        assert isinstance(result, str)
        assert result in _VEHICLE_MAKES

    def test_vehicle_model_returns_str(self) -> None:
        result = self.forge.automotive.vehicle_model()
        assert isinstance(result, str)
        assert result in _VEHICLE_MODELS

    def test_vehicle_year_returns_int(self) -> None:
        result = self.forge.automotive.vehicle_year()
        assert isinstance(result, int)
        assert 1990 <= result <= 2026

    def test_vehicle_year_str_returns_str(self) -> None:
        result = self.forge.automotive.vehicle_year_str()
        assert isinstance(result, str)
        assert 1990 <= int(result) <= 2026

    def test_vehicle_color_returns_str(self) -> None:
        result = self.forge.automotive.vehicle_color()
        assert isinstance(result, str)
        assert result in _VEHICLE_COLORS

    def test_deterministic_with_seed(self) -> None:
        f1 = DataForge(seed=99)
        f2 = DataForge(seed=99)
        assert f1.automotive.license_plate() == f2.automotive.license_plate()
        assert f1.automotive.vin() == f2.automotive.vin()
        assert f1.automotive.vehicle_make() == f2.automotive.vehicle_make()


class TestAutomotiveBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_license_plate_batch(self) -> None:
        result = self.forge.automotive.license_plate(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(re.match(r"^[A-Z]{3}-\d{4}$", p) for p in result)

    def test_vin_batch(self) -> None:
        result = self.forge.automotive.vin(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(len(v) == 17 for v in result)

    def test_vehicle_make_batch(self) -> None:
        result = self.forge.automotive.vehicle_make(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(m in _VEHICLE_MAKES for m in result)

    def test_vehicle_model_batch(self) -> None:
        result = self.forge.automotive.vehicle_model(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(m in _VEHICLE_MODELS for m in result)

    def test_vehicle_year_batch(self) -> None:
        result = self.forge.automotive.vehicle_year(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(1990 <= y <= 2026 for y in result)

    def test_vehicle_year_str_batch(self) -> None:
        result = self.forge.automotive.vehicle_year_str(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(isinstance(y, str) for y in result)

    def test_vehicle_color_batch(self) -> None:
        result = self.forge.automotive.vehicle_color(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(c in _VEHICLE_COLORS for c in result)
