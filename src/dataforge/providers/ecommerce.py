"""E-commerce provider — products, SKUs, tracking, reviews."""

from dataforge.providers.base import BaseProvider

_PRODUCT_ADJECTIVES: tuple[str, ...] = (
    "Premium",
    "Deluxe",
    "Ultra",
    "Pro",
    "Essential",
    "Classic",
    "Modern",
    "Smart",
    "Eco",
    "Advanced",
    "Elite",
    "Basic",
    "Compact",
    "Portable",
    "Wireless",
)

_PRODUCT_MATERIALS: tuple[str, ...] = (
    "Steel",
    "Aluminum",
    "Bamboo",
    "Cotton",
    "Leather",
    "Silk",
    "Wooden",
    "Ceramic",
    "Glass",
    "Rubber",
    "Plastic",
    "Granite",
    "Marble",
    "Carbon Fiber",
    "Titanium",
)

_PRODUCT_ITEMS: tuple[str, ...] = (
    "Chair",
    "Table",
    "Lamp",
    "Keyboard",
    "Mouse",
    "Monitor",
    "Headphones",
    "Speaker",
    "Camera",
    "Watch",
    "Bag",
    "Wallet",
    "Bottle",
    "Mug",
    "Plate",
    "Bowl",
    "Knife",
    "Pan",
    "Pillow",
    "Blanket",
)

_PRODUCT_CATEGORIES: tuple[str, ...] = (
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Sports & Outdoors",
    "Books",
    "Toys & Games",
    "Automotive",
    "Health & Beauty",
    "Food & Beverages",
    "Office Supplies",
    "Pet Supplies",
    "Jewelry",
    "Music",
    "Tools & Hardware",
    "Baby Products",
    "Arts & Crafts",
    "Industrial",
    "Software",
    "Furniture",
)

_TRACKING_PREFIXES: tuple[str, ...] = (
    "1Z",
    "94",
    "92",
    "TBA",
    "JD",
    "SF",
    "YT",
)

_REVIEW_TITLES: tuple[str, ...] = (
    "Great product!",
    "Highly recommended",
    "Good value for money",
    "Exceeded expectations",
    "Exactly as described",
    "Decent quality",
    "Not bad",
    "Could be better",
    "Disappointed",
    "Amazing!",
    "Perfect fit",
    "Solid build quality",
    "Love it!",
    "Just okay",
    "Works as expected",
)


class EcommerceProvider(BaseProvider):
    """Generates fake e-commerce data."""

    __slots__ = ()

    _provider_name = "ecommerce"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "product_name": "product_name",
        "product": "product_name",
        "product_category": "product_category",
        "category": "product_category",
        "sku": "sku",
        "price_with_currency": "price_with_currency",
        "review_rating": "review_rating",
        "rating": "review_rating",
        "review_title": "review_title",
        "tracking_number": "tracking_number",
        "order_id": "order_id",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "product_category": _PRODUCT_CATEGORIES,
        "review_title": _REVIEW_TITLES,
    }

    _CURRENCIES: tuple[tuple[str, str], ...] = (
        ("$", "USD"),
        ("€", "EUR"),
        ("£", "GBP"),
        ("¥", "JPY"),
        ("$", "CAD"),
        ("$", "AUD"),
    )

    def _one_product_name(self) -> str:
        _c = self._engine.choice
        return (
            f"{_c(_PRODUCT_ADJECTIVES)} {_c(_PRODUCT_MATERIALS)} {_c(_PRODUCT_ITEMS)}"
        )

    def _one_sku(self) -> str:
        letters = "".join(chr(self._engine.random_int(65, 90)) for _ in range(3))
        return f"{letters}-{self._engine.random_digits_str(6)}"

    def _one_tracking(self) -> str:
        prefix = self._engine.choice(_TRACKING_PREFIXES)
        return prefix + self._engine.random_digits_str(18)

    def _one_order_id(self) -> str:
        return f"ORD-{self._engine.random_digits_str(10)}"

    def product_name(self, count: int = 1) -> str | list[str]:
        """Generate a fake product name."""
        if count == 1:
            return self._one_product_name()
        return [self._one_product_name() for _ in range(count)]

    def sku(self, count: int = 1) -> str | list[str]:
        """Generate a product SKU (e.g., ABC-123456)."""
        if count == 1:
            return self._one_sku()
        return [self._one_sku() for _ in range(count)]

    def price_with_currency(self, count: int = 1) -> str | list[str]:
        """Generate a price with currency symbol (e.g., $49.99)."""
        if count == 1:
            sym, _ = self._engine.choice(self._CURRENCIES)
            return f"{sym}{self._engine.random_int(1, 99999) / 100:.2f}"
        _ri = self._engine.random_int
        _c = self._engine.choice
        return [
            f"{_c(self._CURRENCIES)[0]}{_ri(1, 99999) / 100:.2f}" for _ in range(count)
        ]

    def review_rating(self, count: int = 1) -> int | list[int]:
        """Generate a review rating (1-5)."""
        if count == 1:
            return self._engine.random_int(1, 5)
        return [self._engine.random_int(1, 5) for _ in range(count)]

    def tracking_number(self, count: int = 1) -> str | list[str]:
        """Generate a shipping tracking number."""
        if count == 1:
            return self._one_tracking()
        return [self._one_tracking() for _ in range(count)]

    def order_id(self, count: int = 1) -> str | list[str]:
        """Generate an order ID (e.g., ORD-1234567890)."""
        if count == 1:
            return self._one_order_id()
        return [self._one_order_id() for _ in range(count)]
