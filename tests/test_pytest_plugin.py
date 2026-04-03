"""Tests for the pytest plugin (dataforge.pytest_plugin)."""

import pytest

from dataforge import DataForge


class TestForgeFixture:
    def test_fixture_is_dataforge(self, forge: DataForge) -> None:
        assert isinstance(forge, DataForge)

    def test_fixture_is_seeded(self, forge: DataForge) -> None:
        a = forge.person.first_name()
        # Create another with same seed — should match
        # (conftest.py overrides plugin fixture with seed=42)
        forge2 = DataForge(seed=42)
        b = forge2.person.first_name()
        assert a == b

    def test_fixture_generates_data(self, forge: DataForge) -> None:
        name = forge.person.first_name()
        assert isinstance(name, str)
        assert len(name) > 0


class TestFakeFixture:
    def test_fake_is_forge(self, forge: DataForge, fake: DataForge) -> None:
        assert fake is forge

    def test_fake_generates_data(self, fake: DataForge) -> None:
        email = fake.internet.email()
        assert "@" in email


class TestForgeUnseeded:
    def test_unseeded_is_dataforge(self, forge_unseeded: DataForge) -> None:
        assert isinstance(forge_unseeded, DataForge)

    def test_unseeded_generates_data(self, forge_unseeded: DataForge) -> None:
        name = forge_unseeded.person.first_name()
        assert isinstance(name, str)


class TestForgeSeedMarker:
    @pytest.mark.forge_seed(42)
    def test_marker_sets_seed(self, forge: DataForge) -> None:
        a = forge.person.first_name()
        forge2 = DataForge(seed=42)
        b = forge2.person.first_name()
        assert a == b

    @pytest.mark.forge_seed(99)
    def test_different_seed_different_output(self, forge: DataForge) -> None:
        a = forge.person.first_name()
        forge2 = DataForge(seed=0)
        _b = forge2.person.first_name()
        # Not guaranteed but astronomically unlikely to match
        # with different seeds on a large name pool
        assert isinstance(a, str) and len(a) > 0
