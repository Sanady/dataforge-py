"""Tests for en_GB, en_AU, en_CA locales."""

import pytest

from dataforge import DataForge

_LOCALES = ["en_GB", "en_AU", "en_CA"]


@pytest.fixture(params=_LOCALES)
def locale_forge(request) -> DataForge:
    """Parameterized fixture — one test run per locale."""
    return DataForge(locale=request.param, seed=42)


class TestLocaleSmoke:
    def test_person_first_name(self, locale_forge: DataForge) -> None:
        name = locale_forge.person.first_name()
        assert isinstance(name, str) and len(name) > 0

    def test_person_full_name(self, locale_forge: DataForge) -> None:
        name = locale_forge.person.full_name()
        assert isinstance(name, str) and " " in name

    def test_person_batch(self, locale_forge: DataForge) -> None:
        names = locale_forge.person.first_name(count=100)
        assert isinstance(names, list) and len(names) == 100

    def test_address_city(self, locale_forge: DataForge) -> None:
        city = locale_forge.address.city()
        assert isinstance(city, str) and len(city) > 0

    def test_address_full(self, locale_forge: DataForge) -> None:
        addr = locale_forge.address.full_address()
        assert isinstance(addr, str) and len(addr) > 10

    def test_company_name(self, locale_forge: DataForge) -> None:
        name = locale_forge.company.company_name()
        assert isinstance(name, str) and len(name) > 0

    def test_phone_number(self, locale_forge: DataForge) -> None:
        phone = locale_forge.phone.phone_number()
        assert isinstance(phone, str) and len(phone) > 5

    def test_internet_email(self, locale_forge: DataForge) -> None:
        email = locale_forge.internet.email()
        assert isinstance(email, str) and "@" in email

    def test_internet_safe_email(self, locale_forge: DataForge) -> None:
        email = locale_forge.internet.safe_email()
        assert isinstance(email, str) and "@" in email

    def test_schema_integration(self, locale_forge: DataForge) -> None:
        schema = locale_forge.schema(
            ["first_name", "last_name", "email", "city", "phone_number"]
        )
        rows = schema.generate(50)
        assert len(rows) == 50
        for row in rows:
            assert "first_name" in row
            assert "email" in row
            assert "@" in row["email"]


class TestEnGB:
    @pytest.fixture
    def forge(self) -> DataForge:
        return DataForge(locale="en_GB", seed=42)

    def test_male_first_name(self, forge: DataForge) -> None:
        name = forge.person.male_first_name()
        assert isinstance(name, str) and len(name) > 0

    def test_female_first_name(self, forge: DataForge) -> None:
        name = forge.person.female_first_name()
        assert isinstance(name, str) and len(name) > 0

    def test_uk_email_domain(self, forge: DataForge) -> None:
        emails = forge.internet.email(count=500)
        uk_domains = [e for e in emails if ".co.uk" in e or ".uk" in e]
        assert len(uk_domains) > 0, "Expected some UK email domains"

    def test_deterministic(self, forge: DataForge) -> None:
        name1 = forge.person.first_name()
        forge2 = DataForge(locale="en_GB", seed=42)
        name2 = forge2.person.first_name()
        assert name1 == name2


class TestEnAU:
    @pytest.fixture
    def forge(self) -> DataForge:
        return DataForge(locale="en_AU", seed=42)

    def test_au_states(self, forge: DataForge) -> None:
        from dataforge.locales.en_AU.address import states

        assert "NSW" in states
        assert "VIC" in states
        assert "QLD" in states
        assert len(states) == 8

    def test_au_email_domain(self, forge: DataForge) -> None:
        emails = forge.internet.email(count=500)
        au_domains = [e for e in emails if ".com.au" in e or ".au" in e]
        assert len(au_domains) > 0, "Expected some AU email domains"


class TestEnCA:
    @pytest.fixture
    def forge(self) -> DataForge:
        return DataForge(locale="en_CA", seed=42)

    def test_ca_provinces(self, forge: DataForge) -> None:
        from dataforge.locales.en_CA.address import states

        assert "ON" in states
        assert "QC" in states
        assert "BC" in states
        assert len(states) == 13

    def test_ca_email_domain(self, forge: DataForge) -> None:
        emails = forge.internet.email(count=500)
        ca_domains = [e for e in emails if ".ca" in e]
        assert len(ca_domains) > 0, "Expected some CA email domains"

    def test_french_canadian_names(self, forge: DataForge) -> None:
        from dataforge.locales.en_CA.person import last_names

        french_names = {"Tremblay", "Roy", "Gagnon", "Bouchard", "Gauthier"}
        found = french_names & set(last_names)
        assert len(found) >= 3, f"Expected French-Canadian surnames, found: {found}"
