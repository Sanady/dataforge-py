"""Payment provider — credit card types, payment methods, processors, etc."""

from dataforge.providers.base import BaseProvider

_CARD_TYPES: tuple[str, ...] = (
    "Visa",
    "Mastercard",
    "American Express",
    "Discover",
    "Diners Club",
    "JCB",
    "UnionPay",
    "Maestro",
    "Mir",
    "Elo",
)

_PAYMENT_METHODS: tuple[str, ...] = (
    "Credit Card",
    "Debit Card",
    "PayPal",
    "Apple Pay",
    "Google Pay",
    "Samsung Pay",
    "Bank Transfer",
    "Wire Transfer",
    "Cash",
    "Check",
    "Cryptocurrency",
    "Venmo",
    "Zelle",
    "Alipay",
    "WeChat Pay",
)

_PROCESSORS: tuple[str, ...] = (
    "Stripe",
    "Square",
    "Adyen",
    "Braintree",
    "Worldpay",
    "Checkout.com",
    "PayPal Commerce",
    "Authorize.Net",
    "2Checkout",
    "BlueSnap",
    "Payoneer",
    "Razorpay",
    "Mollie",
    "dLocal",
    "Nuvei",
)

_TRANSACTION_STATUSES: tuple[str, ...] = (
    "pending",
    "processing",
    "completed",
    "failed",
    "refunded",
    "partially_refunded",
    "voided",
    "disputed",
    "chargeback",
    "authorized",
)

_CURRENCIES: tuple[str, ...] = (
    "USD",
    "EUR",
    "GBP",
    "JPY",
    "CHF",
    "CAD",
    "AUD",
    "CNY",
    "HKD",
    "NZD",
    "SEK",
    "NOK",
    "DKK",
    "SGD",
    "KRW",
    "INR",
    "BRL",
    "MXN",
    "ZAR",
    "PLN",
    "TRY",
    "RUB",
    "THB",
    "TWD",
    "AED",
)

_CURRENCY_SYMBOLS: tuple[str, ...] = (
    "$",
    "€",
    "£",
    "¥",
    "Fr",
    "C$",
    "A$",
    "¥",
    "HK$",
    "NZ$",
    "kr",
    "kr",
    "kr",
    "S$",
    "₩",
    "₹",
    "R$",
    "MX$",
    "R",
    "zł",
    "₺",
    "₽",
    "฿",
    "NT$",
    "د.إ",
)


class PaymentProvider(BaseProvider):
    """Generates fake payment and transaction data."""

    __slots__ = ()

    _provider_name = "payment"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "card_type": "card_type",
        "payment_method": "payment_method",
        "payment_processor": "payment_processor",
        "processor": "payment_processor",
        "transaction_status": "transaction_status",
        "transaction_id": "transaction_id",
        "txn_id": "transaction_id",
        "currency_code": "currency_code",
        "currency_symbol": "currency_symbol",
        "payment_amount": "payment_amount",
        "cvv": "cvv",
        "expiry_date": "expiry_date",
        "card_expiry": "expiry_date",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "card_type": _CARD_TYPES,
        "payment_method": _PAYMENT_METHODS,
        "payment_processor": _PROCESSORS,
        "transaction_status": _TRANSACTION_STATUSES,
        "currency_code": _CURRENCIES,
        "currency_symbol": _CURRENCY_SYMBOLS,
    }

    def _one_transaction_id(self) -> str:
        return f"TXN-{self._engine.random_digits_str(12)}"

    def _one_payment_amount(self) -> str:
        dollars = self._engine.random_int(1, 9999)
        cents = self._engine.random_int(0, 99)
        return f"{dollars}.{cents:02d}"

    def _one_cvv(self) -> str:
        return self._engine.random_digits_str(
            4 if self._engine.random_int(0, 1) == 0 else 3
        )

    def _one_expiry_date(self) -> str:
        month = self._engine.random_int(1, 12)
        year = self._engine.random_int(25, 32)
        return f"{month:02d}/{year:02d}"

    def transaction_id(self, count: int = 1) -> str | list[str]:
        """Generate a transaction ID (TXN-############)."""
        if count == 1:
            return self._one_transaction_id()
        return [self._one_transaction_id() for _ in range(count)]

    def payment_amount(self, count: int = 1) -> str | list[str]:
        """Generate a payment amount (e.g., 49.99)."""
        if count == 1:
            return self._one_payment_amount()
        return [self._one_payment_amount() for _ in range(count)]

    def cvv(self, count: int = 1) -> str | list[str]:
        """Generate a CVV code (3 or 4 digits)."""
        if count == 1:
            return self._one_cvv()
        return [self._one_cvv() for _ in range(count)]

    def expiry_date(self, count: int = 1) -> str | list[str]:
        """Generate a card expiry date (MM/YY)."""
        if count == 1:
            return self._one_expiry_date()
        return [self._one_expiry_date() for _ in range(count)]
