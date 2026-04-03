"""Tests for the BarcodeProvider."""

from dataforge import DataForge


def _validate_ean_check_digit(code: str) -> bool:
    """Validate EAN/ISBN-13 check digit (mod-10, weight 1/3 alternating)."""
    digits = code[:-1]
    total = 0
    for i, ch in enumerate(digits):
        weight = 1 if i % 2 == 0 else 3
        total += int(ch) * weight
    expected = str((10 - (total % 10)) % 10)
    return code[-1] == expected


def _validate_isbn10_check_digit(code: str) -> bool:
    """Validate ISBN-10 check digit (mod-11, weights 10..2)."""
    digits = code[:9]
    total = sum(int(d) * (10 - i) for i, d in enumerate(digits))
    remainder = (11 - (total % 11)) % 11
    expected = "X" if remainder == 10 else str(remainder)
    return code[-1] == expected


class TestBarcodeScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_ean13_returns_str(self) -> None:
        result = self.forge.barcode.ean13()
        assert isinstance(result, str)

    def test_ean13_length(self) -> None:
        for _ in range(50):
            result = self.forge.barcode.ean13()
            assert len(result) == 13
            assert result.isdigit()

    def test_ean8_returns_str(self) -> None:
        result = self.forge.barcode.ean8()
        assert isinstance(result, str)

    def test_ean8_length(self) -> None:
        for _ in range(50):
            result = self.forge.barcode.ean8()
            assert len(result) == 8
            assert result.isdigit()

    def test_isbn13_returns_str(self) -> None:
        result = self.forge.barcode.isbn13()
        assert isinstance(result, str)

    def test_isbn13_length_and_prefix(self) -> None:
        for _ in range(50):
            result = self.forge.barcode.isbn13()
            assert len(result) == 13
            assert result.isdigit()
            assert result.startswith(("978", "979"))

    def test_isbn10_returns_str(self) -> None:
        result = self.forge.barcode.isbn10()
        assert isinstance(result, str)

    def test_isbn10_length(self) -> None:
        for _ in range(50):
            result = self.forge.barcode.isbn10()
            assert len(result) == 10
            # First 9 characters must be digits
            assert result[:9].isdigit()
            # Last character is a digit or 'X'
            assert result[-1].isdigit() or result[-1] == "X"


class TestBarcodeCheckDigits:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_ean13_check_digit_valid(self) -> None:
        for _ in range(100):
            code = self.forge.barcode.ean13()
            assert _validate_ean_check_digit(code), f"EAN-13 check digit failed: {code}"

    def test_ean8_check_digit_valid(self) -> None:
        for _ in range(100):
            code = self.forge.barcode.ean8()
            assert _validate_ean_check_digit(code), f"EAN-8 check digit failed: {code}"

    def test_isbn13_check_digit_valid(self) -> None:
        for _ in range(100):
            code = self.forge.barcode.isbn13()
            assert _validate_ean_check_digit(code), (
                f"ISBN-13 check digit failed: {code}"
            )

    def test_isbn10_check_digit_valid(self) -> None:
        for _ in range(100):
            code = self.forge.barcode.isbn10()
            assert _validate_isbn10_check_digit(code), (
                f"ISBN-10 check digit failed: {code}"
            )


class TestBarcodeBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_ean13_batch(self) -> None:
        result = self.forge.barcode.ean13(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(len(c) == 13 and c.isdigit() for c in result)

    def test_ean8_batch(self) -> None:
        result = self.forge.barcode.ean8(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(len(c) == 8 and c.isdigit() for c in result)

    def test_isbn13_batch(self) -> None:
        result = self.forge.barcode.isbn13(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(len(c) == 13 and c.startswith(("978", "979")) for c in result)

    def test_isbn10_batch(self) -> None:
        result = self.forge.barcode.isbn10(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(len(c) == 10 for c in result)
