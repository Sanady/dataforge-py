"""Tests for the PhoneProvider."""

from dataforge import DataForge


class TestPhoneScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_phone_number_returns_str(self) -> None:
        result = self.forge.phone.phone_number()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_cell_phone_returns_str(self) -> None:
        result = self.forge.phone.cell_phone()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_phone_contains_digits(self) -> None:
        for _ in range(50):
            result = self.forge.phone.phone_number()
            digits = [ch for ch in result if ch.isdigit()]
            assert len(digits) >= 7


class TestPhoneBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_phone_batch(self) -> None:
        result = self.forge.phone.phone_number(count=100)
        assert isinstance(result, list)
        assert len(result) == 100

    def test_cell_batch(self) -> None:
        result = self.forge.phone.cell_phone(count=100)
        assert isinstance(result, list)
        assert len(result) == 100


class TestPhoneLocales:
    def test_de_DE_phone(self) -> None:
        forge = DataForge(locale="de_DE", seed=42)
        phone = forge.phone.phone_number()
        assert isinstance(phone, str)

    def test_fr_FR_phone(self) -> None:
        forge = DataForge(locale="fr_FR", seed=42)
        phone = forge.phone.phone_number()
        assert isinstance(phone, str)

    def test_es_ES_phone(self) -> None:
        forge = DataForge(locale="es_ES", seed=42)
        phone = forge.phone.phone_number()
        assert isinstance(phone, str)

    def test_ja_JP_phone(self) -> None:
        forge = DataForge(locale="ja_JP", seed=42)
        phone = forge.phone.phone_number()
        assert isinstance(phone, str)
