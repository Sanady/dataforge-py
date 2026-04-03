"""Tests for the FinanceProvider."""

import re

from dataforge import DataForge


def _luhn_valid(number: str) -> bool:
    """Validate a credit card number using the Luhn algorithm."""
    digits = [int(d) for d in number]
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


class TestFinanceScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_credit_card_number_returns_str(self) -> None:
        result = self.forge.finance.credit_card_number()
        assert isinstance(result, str)

    def test_credit_card_number_is_luhn_valid(self) -> None:
        for _ in range(100):
            number = self.forge.finance.credit_card_number()
            assert _luhn_valid(number), f"Luhn check failed for {number}"

    def test_credit_card_number_starts_with_known_prefix(self) -> None:
        known_prefixes = ("4", "51", "52", "53", "54", "55", "34", "37", "6011", "65")
        for _ in range(50):
            number = self.forge.finance.credit_card_number()
            assert any(number.startswith(p) for p in known_prefixes), (
                f"Unknown prefix for {number}"
            )

    def test_credit_card_number_correct_length(self) -> None:
        for _ in range(50):
            number = self.forge.finance.credit_card_number()
            assert len(number) in (15, 16), f"Bad length {len(number)} for {number}"

    def test_credit_card_returns_dict(self) -> None:
        result = self.forge.finance.credit_card()
        assert isinstance(result, dict)
        assert "type" in result
        assert "number" in result
        assert "exp" in result
        assert "cvv" in result

    def test_credit_card_type_is_valid(self) -> None:
        valid_types = {"Visa", "Mastercard", "American Express", "Discover"}
        for _ in range(50):
            card = self.forge.finance.credit_card()
            assert card["type"] in valid_types

    def test_credit_card_number_in_card_is_luhn_valid(self) -> None:
        for _ in range(50):
            card = self.forge.finance.credit_card()
            assert _luhn_valid(card["number"])

    def test_credit_card_expiry_format(self) -> None:
        for _ in range(50):
            card = self.forge.finance.credit_card()
            assert re.match(r"^\d{2}/\d{2}$", card["exp"])
            month = int(card["exp"].split("/")[0])
            assert 1 <= month <= 12

    def test_credit_card_cvv_length(self) -> None:
        for _ in range(100):
            card = self.forge.finance.credit_card()
            expected_len = 4 if card["type"] == "American Express" else 3
            assert len(card["cvv"]) == expected_len

    def test_card_type_returns_str(self) -> None:
        result = self.forge.finance.card_type()
        assert isinstance(result, str)
        assert result in ("Visa", "Mastercard", "American Express", "Discover")

    def test_iban_returns_str(self) -> None:
        result = self.forge.finance.iban()
        assert isinstance(result, str)

    def test_iban_format(self) -> None:
        known_countries = ("DE", "FR", "GB", "ES", "IT", "NL", "BE", "AT", "CH", "PT")
        for _ in range(50):
            iban = self.forge.finance.iban()
            assert iban[:2] in known_countries
            assert iban[2:4].isdigit()  # check digits
            assert iban[4:].isdigit()  # BBAN

    def test_currency_code_returns_str(self) -> None:
        result = self.forge.finance.currency_code()
        assert isinstance(result, str)
        assert len(result) == 3

    def test_currency_name_returns_str(self) -> None:
        result = self.forge.finance.currency_name()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_currency_symbol_returns_str(self) -> None:
        result = self.forge.finance.currency_symbol()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_price_returns_str(self) -> None:
        result = self.forge.finance.price()
        assert isinstance(result, str)
        assert re.match(r"^\d+\.\d{2}$", result)

    def test_price_within_default_range(self) -> None:
        for _ in range(100):
            price = float(self.forge.finance.price())
            assert 0.99 <= price <= 9999.99

    def test_price_custom_range(self) -> None:
        for _ in range(100):
            price = float(self.forge.finance.price(min_val=10.0, max_val=50.0))
            assert 10.0 <= price <= 50.0

    def test_bic_returns_str(self) -> None:
        result = self.forge.finance.bic()
        assert isinstance(result, str)
        assert len(result) == 11  # 4 bank + 2 country + 2 location + 3 branch (XXX)

    def test_routing_number_returns_str(self) -> None:
        for _ in range(100):
            result = self.forge.finance.routing_number()
            assert isinstance(result, str)
            assert len(result) == 9
            assert result.isdigit()

    def test_routing_number_aba_checksum(self) -> None:
        for _ in range(100):
            rn = self.forge.finance.routing_number()
            digits = [int(d) for d in rn]
            weights = (3, 7, 1, 3, 7, 1, 3, 7, 1)
            total = sum(d * w for d, w in zip(digits, weights))
            assert total % 10 == 0, f"ABA checksum failed for {rn}"

    def test_bitcoin_address_returns_str(self) -> None:
        for _ in range(50):
            result = self.forge.finance.bitcoin_address()
            assert isinstance(result, str)
            assert result.startswith("1")
            assert 26 <= len(result) <= 34


class TestFinanceBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_credit_card_number_batch(self) -> None:
        result = self.forge.finance.credit_card_number(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(_luhn_valid(n) for n in result)

    def test_credit_card_batch(self) -> None:
        result = self.forge.finance.credit_card(count=20)
        assert isinstance(result, list)
        assert len(result) == 20
        assert all(isinstance(c, dict) for c in result)

    def test_card_type_batch(self) -> None:
        result = self.forge.finance.card_type(count=100)
        assert isinstance(result, list)
        assert len(result) == 100

    def test_iban_batch(self) -> None:
        result = self.forge.finance.iban(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_currency_code_batch(self) -> None:
        result = self.forge.finance.currency_code(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(len(c) == 3 for c in result)

    def test_currency_name_batch(self) -> None:
        result = self.forge.finance.currency_name(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_currency_symbol_batch(self) -> None:
        result = self.forge.finance.currency_symbol(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_price_batch(self) -> None:
        result = self.forge.finance.price(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(0.99 <= float(p) <= 9999.99 for p in result)

    def test_bic_batch(self) -> None:
        result = self.forge.finance.bic(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_routing_number_batch(self) -> None:
        result = self.forge.finance.routing_number(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(len(r) == 9 for r in result)

    def test_bitcoin_address_batch(self) -> None:
        result = self.forge.finance.bitcoin_address(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(a.startswith("1") for a in result)
