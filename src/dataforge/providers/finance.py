"""Finance provider — generates fake credit card numbers, IBANs, currencies."""

from dataforge.providers.base import BaseProvider

# Credit card prefixes by network (BIN ranges)
_CARD_TYPES: tuple[tuple[str, str, int], ...] = (
    # (name, prefix, total_length)
    ("Visa", "4", 16),
    ("Visa", "4", 16),
    ("Visa", "4", 16),
    ("Mastercard", "51", 16),
    ("Mastercard", "52", 16),
    ("Mastercard", "53", 16),
    ("Mastercard", "54", 16),
    ("Mastercard", "55", 16),
    ("American Express", "34", 15),
    ("American Express", "37", 15),
    ("Discover", "6011", 16),
    ("Discover", "65", 16),
)

_CURRENCIES: tuple[tuple[str, str, str], ...] = (
    # (code, name, symbol)
    ("USD", "US Dollar", "$"),
    ("EUR", "Euro", "\u20ac"),
    ("GBP", "British Pound", "\u00a3"),
    ("JPY", "Japanese Yen", "\u00a5"),
    ("CNY", "Chinese Yuan", "\u00a5"),
    ("KRW", "South Korean Won", "\u20a9"),
    ("BRL", "Brazilian Real", "R$"),
    ("CAD", "Canadian Dollar", "C$"),
    ("AUD", "Australian Dollar", "A$"),
    ("CHF", "Swiss Franc", "CHF"),
    ("INR", "Indian Rupee", "\u20b9"),
    ("MXN", "Mexican Peso", "$"),
    ("SEK", "Swedish Krona", "kr"),
    ("NOK", "Norwegian Krone", "kr"),
    ("DKK", "Danish Krone", "kr"),
    ("PLN", "Polish Zloty", "z\u0142"),
    ("TRY", "Turkish Lira", "\u20ba"),
    ("RUB", "Russian Ruble", "\u20bd"),
    ("ZAR", "South African Rand", "R"),
    ("NZD", "New Zealand Dollar", "NZ$"),
)

# Pre-split parallel tuples for vectorized batch generation —
# avoids per-item tuple indexing overhead in batch paths.
_CURRENCY_CODES: tuple[str, ...] = tuple(c[0] for c in _CURRENCIES)
_CURRENCY_NAMES: tuple[str, ...] = tuple(c[1] for c in _CURRENCIES)
_CURRENCY_SYMBOLS: tuple[str, ...] = tuple(c[2] for c in _CURRENCIES)

# IBAN formats by country: (country_code, total_length)
_IBAN_FORMATS: tuple[tuple[str, int], ...] = (
    ("DE", 22),
    ("FR", 27),
    ("GB", 22),
    ("ES", 24),
    ("IT", 27),
    ("NL", 18),
    ("BE", 16),
    ("AT", 20),
    ("CH", 21),
    ("PT", 25),
)

# BIC/SWIFT code components — pre-computed 4-char bank codes at module level
# to avoid per-call ``[:4].ljust(4, "X")`` overhead.
_BIC_BANK_CODES: tuple[str, ...] = tuple(
    b[:4].ljust(4, "X")
    for b in (
        "DEUTDEFF",
        "BNPAFRPP",
        "BARCGB22",
        "CHASUS33",
        "CITIUS33",
        "COBADEFF",
        "HSBC",
        "INGB",
        "SCBL",
        "UBSW",
        "ABNA",
        "RABO",
        "BOFAUS3N",
        "WFBIUS6S",
        "NWBKGB2L",
        "LOYDGB21",
        "BKENGB2L",
        "SOGEFRPP",
        "CRLYFRPP",
        "AGRIFRPP",
    )
)

_BIC_LOCATIONS: tuple[str, ...] = (
    "FF",
    "PP",
    "22",
    "33",
    "2L",
    "3N",
    "6S",
    "MM",
    "XX",
    "HH",
    "LX",
    "BB",
    "CC",
    "DD",
    "EE",
    "GG",
    "KK",
    "LL",
    "SS",
    "TT",
)

_BIC_COUNTRIES: tuple[str, ...] = (
    "DE",
    "FR",
    "GB",
    "US",
    "NL",
    "CH",
    "IT",
    "ES",
    "AT",
    "BE",
)

# Base58 alphabet for Bitcoin addresses (as a string for O(1) indexing)
_BASE58_STR: str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_LEN: int = 58  # len(_BASE58_STR)

# 7h — Luhn doubling lookup table: eliminates per-digit ``if d > 9: d -= 9``
_LUHN_DOUBLE: tuple[int, ...] = (0, 2, 4, 6, 8, 1, 3, 5, 7, 9)


_CARD_TYPE_NAMES: tuple[str, ...] = (
    "Visa",
    "Mastercard",
    "American Express",
    "Discover",
)


def _luhn_checksum(number: str) -> str:
    """Append a Luhn check digit to *number* and return the full card number.

    Uses reverse index math to avoid creating a ``reversed()`` iterator,
    and ``ord(ch) - 48`` with a pre-computed ``_LUHN_DOUBLE`` lookup
    table to eliminate per-digit branching.
    """
    total = 0
    _double = _LUHN_DOUBLE
    n_len = len(number)
    for i in range(n_len):
        d = ord(number[n_len - 1 - i]) - 48
        if i % 2 == 0:
            d = _double[d]
        total += d
    check = (10 - (total % 10)) % 10
    return number + str(check)


class FinanceProvider(BaseProvider):
    """Generates fake financial data: credit cards, IBANs, currencies.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "finance"
    _locale_modules = ()
    _field_map = {
        "credit_card_number": "credit_card_number",
        "card_number": "credit_card_number",
        "card_type": "card_type",
        "iban": "iban",
        "currency_code": "currency_code",
        "currency": "currency_code",
        "currency_name": "currency_name",
        "currency_symbol": "currency_symbol",
        "price": "price",
        "bic": "bic",
        "routing_number": "routing_number",
        "bitcoin_address": "bitcoin_address",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "currency_code": _CURRENCY_CODES,
        "currency_name": _CURRENCY_NAMES,
        "currency_symbol": _CURRENCY_SYMBOLS,
        "card_type": _CARD_TYPE_NAMES,
    }

    # Scalar helpers

    def _one_credit_card_number(self) -> str:
        _, prefix, length = self._engine.choice(_CARD_TYPES)
        # Generate remaining digits (minus prefix, minus check digit)
        remaining = length - len(prefix) - 1
        body = prefix + self._engine.random_digits_str(remaining)
        return _luhn_checksum(body)

    def _one_credit_card(self) -> dict[str, str]:
        name, prefix, length = self._engine.choice(_CARD_TYPES)
        remaining = length - len(prefix) - 1
        body = prefix + self._engine.random_digits_str(remaining)
        number = _luhn_checksum(body)
        exp_month = str(self._engine.random_int(1, 12)).zfill(2)
        exp_year = str(self._engine.random_int(25, 30))
        cvv_len = 4 if name == "American Express" else 3
        cvv = self._engine.random_digits_str(cvv_len)
        return {
            "type": name,
            "number": number,
            "exp": f"{exp_month}/{exp_year}",
            "cvv": cvv,
        }

    def _one_iban(self) -> str:
        country, length = self._engine.choice(_IBAN_FORMATS)
        bban_len = length - 4  # 2 country + 2 check
        check = str(self._engine.random_int(2, 98)).zfill(2)
        bban = self._engine.random_digits_str(bban_len)
        return f"{country}{check}{bban}"

    def _one_price(self, min_cents: int, max_cents: int) -> str:
        cents = self._engine.random_int(min_cents, max_cents)
        return f"{cents / 100:.2f}"

    def _one_bic(self) -> str:
        bank_code = self._engine.choice(_BIC_BANK_CODES)
        country = self._engine.choice(_BIC_COUNTRIES)
        location = self._engine.choice(_BIC_LOCATIONS)
        return f"{bank_code}{country}{location}XXX"

    def _one_routing_number(self) -> str:
        # First two digits: Federal Reserve district (01-12)
        d1 = self._engine.random_int(0, 1)
        d2 = self._engine.random_int(1, 2) if d1 == 1 else self._engine.random_int(1, 9)
        # Generate 6 random digits in one call
        mid = self._engine.random_digits_str(6)
        d = [
            d1,
            d2,
            ord(mid[0]) - 48,
            ord(mid[1]) - 48,
            ord(mid[2]) - 48,
            ord(mid[3]) - 48,
            ord(mid[4]) - 48,
            ord(mid[5]) - 48,
        ]
        # ABA checksum
        total = (
            3 * d[0]
            + 7 * d[1]
            + d[2]
            + 3 * d[3]
            + 7 * d[4]
            + d[5]
            + 3 * d[6]
            + 7 * d[7]
        )
        check = (10 - (total % 10)) % 10
        return f"{d1}{d2}{mid}{check}"

    def _one_bitcoin_address(self) -> str:
        # P2PKH addresses: "1" + 25-33 Base58 characters
        length = self._engine.random_int(25, 33)
        # Use choices() on the Base58 alphabet — avoids Python-level
        # divmod loop entirely.  The RNG picks k indices in C.
        chars = self._engine._rng.choices(_BASE58_STR, k=length)
        return "1" + "".join(chars)

    # Public API

    def credit_card_number(self, count: int = 1) -> str | list[str]:
        """Generate a random credit card number (Luhn-valid)."""
        if count == 1:
            return self._one_credit_card_number()
        return [self._one_credit_card_number() for _ in range(count)]

    def credit_card(self, count: int = 1) -> dict[str, str] | list[dict[str, str]]:
        """Generate a full credit card (number, type, expiry, CVV)."""
        if count == 1:
            return self._one_credit_card()
        return [self._one_credit_card() for _ in range(count)]

    def cvv(self, count: int = 1) -> str | list[str]:
        """Generate a random CVV (3 digits)."""
        if count == 1:
            return self._engine.random_digits_str(3)
        return [self._engine.random_digits_str(3) for _ in range(count)]

    def expiry_date(self, count: int = 1) -> str | list[str]:
        """Generate a random credit card expiry date (MM/YY)."""
        if count == 1:
            m = str(self._engine.random_int(1, 12)).zfill(2)
            y = str(self._engine.random_int(25, 30))
            return f"{m}/{y}"
        result: list[str] = []
        _ri = self._engine.random_int
        for _ in range(count):
            m = str(_ri(1, 12)).zfill(2)
            y = str(_ri(25, 30))
            result.append(f"{m}/{y}")
        return result

    def iban(self, count: int = 1) -> str | list[str]:
        """Generate a random IBAN."""
        if count == 1:
            return self._one_iban()
        return [self._one_iban() for _ in range(count)]

    def price(
        self, count: int = 1, min_val: float = 0.99, max_val: float = 9999.99
    ) -> str | list[str]:
        """Generate a random price string (e.g. ``"49.99"``)."""
        min_cents = int(min_val * 100)
        max_cents = int(max_val * 100)
        if count == 1:
            return self._one_price(min_cents, max_cents)
        # Inlined batch loop with local-bound random_int
        _ri = self._engine.random_int
        return [f"{_ri(min_cents, max_cents) / 100:.2f}" for _ in range(count)]

    def bic(self, count: int = 1) -> str | list[str]:
        """Generate a random BIC/SWIFT code (e.g. ``"DEUTDEFFXXX"``)."""
        if count == 1:
            return self._one_bic()
        # Inlined batch loop with local-bound choices
        _choice = self._engine.choice
        return [
            f"{_choice(_BIC_BANK_CODES)}{_choice(_BIC_COUNTRIES)}{_choice(_BIC_LOCATIONS)}XXX"
            for _ in range(count)
        ]

    def routing_number(self, count: int = 1) -> str | list[str]:
        """Generate a random US ABA routing number with valid checksum."""
        if count == 1:
            return self._one_routing_number()
        # Inlined batch with local-bound helpers
        _ri = self._engine.random_int
        _rds = self._engine.random_digits_str
        _ord = ord
        result: list[str] = []
        for _ in range(count):
            d1 = _ri(0, 1)
            d2 = _ri(1, 2) if d1 == 1 else _ri(1, 9)
            mid = _rds(6)
            d = [
                d1,
                d2,
                _ord(mid[0]) - 48,
                _ord(mid[1]) - 48,
                _ord(mid[2]) - 48,
                _ord(mid[3]) - 48,
                _ord(mid[4]) - 48,
                _ord(mid[5]) - 48,
            ]
            total = (
                3 * d[0]
                + 7 * d[1]
                + d[2]
                + 3 * d[3]
                + 7 * d[4]
                + d[5]
                + 3 * d[6]
                + 7 * d[7]
            )
            check = (10 - (total % 10)) % 10
            result.append(f"{d1}{d2}{mid}{check}")
        return result

    def bitcoin_address(self, count: int = 1) -> str | list[str]:
        """Generate a random Bitcoin address (P2PKH format, starts with ``1``)."""
        if count == 1:
            return self._one_bitcoin_address()
        # Inlined batch loop — use modular indexing into BASE58 alphabet
        _b58 = _BASE58_STR
        _b58_len = _BASE58_LEN
        _getrandbits = self._engine.getrandbits
        _ri = self._engine.random_int
        result: list[str] = []
        for _i in range(count):
            length = _ri(25, 33)
            # Generate all random bits at once
            bits = _getrandbits(length * 6)
            # Build string using list comprehension with divmod unrolled
            chars: list[str] = ["1"]
            for _j in range(length):
                chars.append(_b58[bits % _b58_len])
                bits //= _b58_len
            result.append("".join(chars))
        return result
