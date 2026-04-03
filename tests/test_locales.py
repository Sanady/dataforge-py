"""Tests for locale support across all locale-aware providers."""

import pytest

from dataforge import DataForge

LOCALES = (
    "en_US",
    "de_DE",
    "fr_FR",
    "es_ES",
    "ja_JP",
    "pt_BR",
    "it_IT",
    "ko_KR",
    "zh_CN",
    "nl_NL",
    "pl_PL",
    "hi_IN",
    "ar_SA",
    "ru_RU",
)


class TestLocalePersonProvider:
    @pytest.mark.parametrize("locale", LOCALES)
    def test_first_name(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        result = forge.person.first_name()
        assert isinstance(result, str)
        assert len(result) > 0

    @pytest.mark.parametrize("locale", LOCALES)
    def test_last_name(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        result = forge.person.last_name()
        assert isinstance(result, str)
        assert len(result) > 0

    @pytest.mark.parametrize("locale", LOCALES)
    def test_full_name(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        result = forge.person.full_name()
        assert isinstance(result, str)
        assert " " in result

    @pytest.mark.parametrize("locale", LOCALES)
    def test_full_name_batch(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        result = forge.person.full_name(count=100)
        assert isinstance(result, list)
        assert len(result) == 100


class TestLocaleAddressProvider:
    @pytest.mark.parametrize("locale", LOCALES)
    def test_city(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        result = forge.address.city()
        assert isinstance(result, str)
        assert len(result) > 0

    @pytest.mark.parametrize("locale", LOCALES)
    def test_full_address(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        result = forge.address.full_address()
        assert isinstance(result, str)
        assert "," in result

    @pytest.mark.parametrize("locale", LOCALES)
    def test_zip_code(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        result = forge.address.zip_code()
        assert isinstance(result, str)
        # All zip codes should contain digits
        assert any(ch.isdigit() for ch in result)


class TestLocaleInternetProvider:
    @pytest.mark.parametrize("locale", LOCALES)
    def test_email(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        result = forge.internet.email()
        assert isinstance(result, str)
        assert "@" in result
        assert result.isascii()

    @pytest.mark.parametrize("locale", LOCALES)
    def test_ipv4(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        result = forge.internet.ipv4()
        assert isinstance(result, str)
        assert len(result.split(".")) == 4


class TestLocaleCompanyProvider:
    @pytest.mark.parametrize("locale", LOCALES)
    def test_company_name(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        result = forge.company.company_name()
        assert isinstance(result, str)
        assert len(result) > 0

    @pytest.mark.parametrize("locale", LOCALES)
    def test_job_title(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        result = forge.company.job_title()
        assert isinstance(result, str)


class TestLocalePhoneProvider:
    @pytest.mark.parametrize("locale", LOCALES)
    def test_phone_number(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        result = forge.phone.phone_number()
        assert isinstance(result, str)
        digits = [ch for ch in result if ch.isdigit()]
        assert len(digits) >= 5

    @pytest.mark.parametrize("locale", LOCALES)
    def test_cell_phone(self, locale: str) -> None:
        forge = DataForge(locale=locale, seed=42)
        result = forge.phone.cell_phone()
        assert isinstance(result, str)
