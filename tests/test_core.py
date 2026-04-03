"""Tests for the DataForge core class."""

from dataforge import DataForge


class TestDataForgeInit:
    def test_default_locale(self) -> None:
        forge = DataForge()
        assert forge.locale == "en_US"

    def test_custom_locale_stored(self) -> None:
        forge = DataForge(locale="en_US")
        assert forge.locale == "en_US"

    def test_repr(self) -> None:
        forge = DataForge(locale="en_US")
        assert repr(forge) == "DataForge(locale='en_US')"

    def test_invalid_locale_falls_back_to_en_US(self) -> None:
        forge = DataForge(locale="xx_XX")
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            name = forge.person.first_name()
            assert isinstance(name, str)
            assert len(name) > 0
            # Should have emitted a fallback warning
            assert any(
                "falling back to 'en_US'" in str(warning.message) for warning in w
            )


class TestDataForgeSeed:
    def test_seeded_person_reproducible(self) -> None:
        forge1 = DataForge(seed=99)
        forge2 = DataForge(seed=99)
        assert forge1.person.first_name() == forge2.person.first_name()

    def test_seeded_address_reproducible(self) -> None:
        forge1 = DataForge(seed=99)
        forge2 = DataForge(seed=99)
        assert forge1.address.full_address() == forge2.address.full_address()

    def test_reseed_produces_same_output(self) -> None:
        forge = DataForge(seed=42)
        name1 = forge.person.first_name()
        forge.seed(42)
        name2 = forge.person.first_name()
        assert name1 == name2


class TestDataForgeLazyLoading:
    def test_person_not_loaded_initially(self) -> None:
        forge = DataForge()
        assert "person" not in forge._providers  # noqa: SLF001

    def test_address_not_loaded_initially(self) -> None:
        forge = DataForge()
        assert "address" not in forge._providers  # noqa: SLF001

    def test_person_loaded_on_access(self) -> None:
        forge = DataForge()
        _ = forge.person.first_name()
        assert "person" in forge._providers  # noqa: SLF001

    def test_address_loaded_on_access(self) -> None:
        forge = DataForge()
        _ = forge.address.city()
        assert "address" in forge._providers  # noqa: SLF001
