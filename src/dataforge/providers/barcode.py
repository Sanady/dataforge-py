"""Barcode provider — generates fake EAN, ISBN, and UPC barcodes.

All barcodes include valid check digits computed per their respective
standards. This provider is locale-independent.
"""

from dataforge.providers.base import BaseProvider


def _ean_check_digit(digits: str) -> str:
    """Compute EAN/ISBN-13 check digit (mod-10, weight 1/3 alternating).

    Uses alternating-slice sums on ``ord``-based integer tuples —
    eliminates the ``enumerate`` loop and per-iteration ``i % 2``
    branching entirely.
    """
    d = tuple(ord(ch) - 48 for ch in digits)
    total = sum(d[::2]) + 3 * sum(d[1::2])
    return str((10 - (total % 10)) % 10)


def _isbn10_check_digit(digits: str) -> str:
    """Compute ISBN-10 check digit (mod-11, weights 10..2)."""
    total = sum((ord(d) - 48) * (10 - i) for i, d in enumerate(digits))
    remainder = (11 - (total % 11)) % 11
    return "X" if remainder == 10 else str(remainder)


class BarcodeProvider(BaseProvider):
    """Generates fake barcodes with valid check digits.

    This provider is locale-independent.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "barcode"
    _locale_modules = ()
    _field_map = {
        "ean13": "ean13",
        "ean8": "ean8",
        "isbn13": "isbn13",
        "isbn10": "isbn10",
    }

    # Scalar helpers

    def _one_ean13(self) -> str:
        body = self._engine.random_digits_str(12)
        return body + _ean_check_digit(body)

    def _one_ean8(self) -> str:
        body = self._engine.random_digits_str(7)
        return body + _ean_check_digit(body)

    def _one_isbn13(self) -> str:
        # ISBN-13 starts with 978 or 979
        prefix = self._engine.choice(("978", "979"))
        body = prefix + self._engine.random_digits_str(9)
        return body + _ean_check_digit(body)

    def _one_isbn10(self) -> str:
        body = self._engine.random_digits_str(9)
        return body + _isbn10_check_digit(body)

    # Public API

    def ean13(self, count: int = 1) -> str | list[str]:
        """Generate a random EAN-13 barcode (13 digits, valid check digit)."""
        if count == 1:
            return self._one_ean13()
        return [self._one_ean13() for _ in range(count)]

    def ean8(self, count: int = 1) -> str | list[str]:
        """Generate a random EAN-8 barcode (8 digits, valid check digit)."""
        if count == 1:
            return self._one_ean8()
        return [self._one_ean8() for _ in range(count)]

    def isbn13(self, count: int = 1) -> str | list[str]:
        """Generate a random ISBN-13 (starts with 978/979, valid check digit)."""
        if count == 1:
            return self._one_isbn13()
        return [self._one_isbn13() for _ in range(count)]

    def isbn10(self, count: int = 1) -> str | list[str]:
        """Generate a random ISBN-10 (9 digits + check character)."""
        if count == 1:
            return self._one_isbn10()
        return [self._one_isbn10() for _ in range(count)]
