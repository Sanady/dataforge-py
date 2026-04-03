"""Tests for the UniqueProxy — unique value generation."""

import pytest

from dataforge import DataForge


class TestUniqueScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_unique_values_are_different(self) -> None:
        results: set[str] = set()
        for _ in range(20):
            val = self.forge.unique.person.first_name()
            assert val not in results, f"Duplicate: {val}"
            results.add(val)

    def test_unique_returns_string(self) -> None:
        val = self.forge.unique.person.first_name()
        assert isinstance(val, str)
        assert len(val) > 0

    def test_unique_across_providers(self) -> None:
        name = self.forge.unique.person.first_name()
        city = self.forge.unique.address.city()
        assert isinstance(name, str)
        assert isinstance(city, str)

    def test_unique_boolean(self) -> None:
        a = self.forge.unique.misc.boolean()
        b = self.forge.unique.misc.boolean()
        assert a != b
        assert {a, b} == {True, False}

    def test_unique_exhaustion_raises(self) -> None:
        # boolean only has True/False
        self.forge.unique.misc.boolean()
        self.forge.unique.misc.boolean()
        with pytest.raises(RuntimeError, match="unique value"):
            self.forge.unique.misc.boolean()


class TestUniqueBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_batch_all_unique(self) -> None:
        results = self.forge.unique.person.first_name(count=50)
        assert isinstance(results, list)
        assert len(results) == 50
        assert len(set(results)) == 50  # all unique

    def test_batch_extends_uniqueness(self) -> None:
        first = self.forge.unique.person.first_name()
        batch = self.forge.unique.person.first_name(count=10)
        assert first not in batch

    def test_batch_large(self) -> None:
        results = self.forge.unique.address.city(count=40)
        assert len(results) == 40
        assert len(set(results)) == 40


class TestUniqueClear:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_clear_allows_reuse(self) -> None:
        self.forge.unique.misc.boolean()
        self.forge.unique.misc.boolean()
        self.forge.unique.clear()
        # After clear, should be able to generate again
        c = self.forge.unique.misc.boolean()
        assert isinstance(c, bool)

    def test_clear_specific_provider(self) -> None:
        self.forge.unique.misc.boolean()
        self.forge.unique.misc.boolean()
        self.forge.unique.clear("misc")
        c = self.forge.unique.misc.boolean()
        assert isinstance(c, bool)

    def test_clear_one_doesnt_affect_other(self) -> None:
        name = self.forge.unique.person.first_name()
        self.forge.unique.address.city()
        self.forge.unique.clear("address")
        # Person tracking should still be active
        name2 = self.forge.unique.person.first_name()
        assert name != name2


class TestUniqueProxy:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_unique_property_returns_same_proxy(self) -> None:
        proxy1 = self.forge.unique
        proxy2 = self.forge.unique
        assert proxy1 is proxy2

    def test_unique_person_returns_same_provider_proxy(self) -> None:
        p1 = self.forge.unique.person
        p2 = self.forge.unique.person
        assert p1 is p2
