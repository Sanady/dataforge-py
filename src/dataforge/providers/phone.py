"""Phone provider — generates fake phone and cell numbers."""

from types import ModuleType

from dataforge.backend import RandomEngine
from dataforge.providers.base import BaseProvider


class PhoneProvider(BaseProvider):
    """Generates fake phone and cell phone numbers.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    locale_data : ModuleType
        The imported locale module (e.g. ``dataforge.locales.en_US.phone``).
    """

    __slots__ = ("_phone_formats", "_cell_formats")

    _provider_name = "phone"
    _locale_modules = ("phone",)
    _field_map = {
        "phone_number": "phone_number",
        "phone": "phone_number",
        "cell_phone": "cell_phone",
        "cell": "cell_phone",
    }

    def __init__(self, engine: RandomEngine, locale_data: ModuleType) -> None:
        super().__init__(engine)
        self._phone_formats: tuple[str, ...] = locale_data.phone_formats
        self._cell_formats: tuple[str, ...] = locale_data.cell_formats

    # Scalar helpers

    def _one_phone(self) -> str:
        fmt = self._engine.choice(self._phone_formats)
        return self._engine.numerify(fmt)

    def _one_cell(self) -> str:
        fmt = self._engine.choice(self._cell_formats)
        return self._engine.numerify(fmt)

    # Public API

    def phone_number(self, count: int = 1) -> str | list[str]:
        """Generate a random phone number."""
        if count == 1:
            return self._one_phone()
        return [self._one_phone() for _ in range(count)]

    def cell_phone(self, count: int = 1) -> str | list[str]:
        """Generate a random cell phone number."""
        if count == 1:
            return self._one_cell()
        return [self._one_cell() for _ in range(count)]
