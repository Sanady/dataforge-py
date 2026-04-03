"""Tests for the AddressProvider."""

import re

from dataforge import DataForge
from dataforge.locales.en_US.address import (
    cities,
    states,
    street_names,
    street_suffixes,
)


class TestAddressScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_city_returns_str(self) -> None:
        result = self.forge.address.city()
        assert isinstance(result, str)
        assert result in cities

    def test_state_returns_str(self) -> None:
        result = self.forge.address.state()
        assert isinstance(result, str)
        assert result in states

    def test_zip_code_format(self) -> None:
        for _ in range(50):
            result = self.forge.address.zip_code()
            assert isinstance(result, str)
            assert re.match(r"^\d{5}(-\d{4})?$", result)

    def test_street_name_returns_str(self) -> None:
        result = self.forge.address.street_name()
        assert isinstance(result, str)
        parts = result.rsplit(" ", 1)
        assert parts[0] in street_names
        assert parts[1] in street_suffixes

    def test_street_address_has_number(self) -> None:
        result = self.forge.address.street_address()
        assert isinstance(result, str)
        # Should start with digits
        assert result[0].isdigit()

    def test_building_number_is_digits(self) -> None:
        for _ in range(50):
            result = self.forge.address.building_number()
            assert isinstance(result, str)
            assert result.isdigit()
            assert 3 <= len(result) <= 5

    def test_full_address_format(self) -> None:
        result = self.forge.address.full_address()
        assert isinstance(result, str)
        # Should contain commas separating parts
        assert result.count(",") == 2
        # Pattern: "### Street Suffix, City, ST #####"
        parts = result.split(", ")
        assert len(parts) == 3

    def test_country_returns_str(self) -> None:
        result = self.forge.address.country()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_country_code_returns_str(self) -> None:
        result = self.forge.address.country_code()
        assert isinstance(result, str)
        assert len(result) == 2

    def test_latitude_format(self) -> None:
        for _ in range(50):
            result = self.forge.address.latitude()
            assert isinstance(result, str)
            val = float(result)
            assert -90.0 <= val <= 90.0

    def test_longitude_format(self) -> None:
        for _ in range(50):
            result = self.forge.address.longitude()
            assert isinstance(result, str)
            val = float(result)
            assert -180.0 <= val <= 180.0

    def test_coordinate_returns_tuple(self) -> None:
        result = self.forge.address.coordinate()
        assert isinstance(result, tuple)
        assert len(result) == 2
        lat, lon = result
        assert -90.0 <= float(lat) <= 90.0
        assert -180.0 <= float(lon) <= 180.0


class TestAddressBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_city_batch(self) -> None:
        result = self.forge.address.city(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(c in cities for c in result)

    def test_state_batch(self) -> None:
        result = self.forge.address.state(count=100)
        assert isinstance(result, list)
        assert len(result) == 100

    def test_zip_code_batch(self) -> None:
        result = self.forge.address.zip_code(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(re.match(r"^\d{5}(-\d{4})?$", z) for z in result)

    def test_full_address_batch(self) -> None:
        result = self.forge.address.full_address(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        for addr in result:
            assert addr.count(",") == 2

    def test_country_batch(self) -> None:
        result = self.forge.address.country(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_country_code_batch(self) -> None:
        result = self.forge.address.country_code(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(len(c) == 2 for c in result)

    def test_latitude_batch(self) -> None:
        result = self.forge.address.latitude(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_longitude_batch(self) -> None:
        result = self.forge.address.longitude(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_coordinate_batch(self) -> None:
        result = self.forge.address.coordinate(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(isinstance(c, tuple) and len(c) == 2 for c in result)

    def test_large_batch(self) -> None:
        result = self.forge.address.city(count=10_000)
        assert len(result) == 10_000
