"""Tests for the PersonProvider."""

from dataforge import DataForge
from dataforge.locales.en_US.person import first_names, last_names


class TestPersonScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_first_name_returns_str(self) -> None:
        result = self.forge.person.first_name()
        assert isinstance(result, str)
        assert result in first_names

    def test_last_name_returns_str(self) -> None:
        result = self.forge.person.last_name()
        assert isinstance(result, str)
        assert result in last_names

    def test_full_name_returns_str(self) -> None:
        result = self.forge.person.full_name()
        assert isinstance(result, str)
        parts = result.split(" ")
        assert len(parts) == 2
        assert parts[0] in first_names
        assert parts[1] in last_names

    def test_prefix_returns_str(self) -> None:
        result = self.forge.person.prefix()
        assert isinstance(result, str)
        assert result in ("Mr.", "Mrs.", "Ms.", "Dr.")

    def test_suffix_returns_str(self) -> None:
        result = self.forge.person.suffix()
        assert isinstance(result, str)
        assert result in ("Jr.", "Sr.", "III", "IV", "V")

    def test_male_first_name_returns_str(self) -> None:
        result = self.forge.person.male_first_name()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_female_first_name_returns_str(self) -> None:
        result = self.forge.person.female_first_name()
        assert isinstance(result, str)
        assert len(result) > 0


class TestPersonBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_first_name_batch(self) -> None:
        result = self.forge.person.first_name(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(name in first_names for name in result)

    def test_last_name_batch(self) -> None:
        result = self.forge.person.last_name(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(name in last_names for name in result)

    def test_full_name_batch(self) -> None:
        result = self.forge.person.full_name(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        for name in result:
            parts = name.split(" ")
            assert len(parts) == 2

    def test_male_first_name_batch(self) -> None:
        result = self.forge.person.male_first_name(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_female_first_name_batch(self) -> None:
        result = self.forge.person.female_first_name(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_large_batch(self) -> None:
        result = self.forge.person.first_name(count=10_000)
        assert len(result) == 10_000
