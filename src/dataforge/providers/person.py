"""Person provider — generates fake personal names."""

from types import ModuleType

from dataforge.backend import RandomEngine
from dataforge.providers.base import BaseProvider

# Module-level constants for zero per-call overhead
_PREFIXES: tuple[str, ...] = ("Mr.", "Mrs.", "Ms.", "Dr.")
_SUFFIXES: tuple[str, ...] = ("Jr.", "Sr.", "III", "IV", "V")


class PersonProvider(BaseProvider):
    """Generates fake first names, last names, and full names.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    locale_data : ModuleType
        The imported locale module (e.g. ``dataforge.locales.en_US.person``).
    """

    __slots__ = (
        "_first_names",
        "_last_names",
        "_male_first_names",
        "_female_first_names",
    )

    _provider_name = "person"
    _locale_modules = ("person",)
    _field_map = {
        "first_name": "first_name",
        "last_name": "last_name",
        "full_name": "full_name",
        "name": "full_name",
        "prefix": "prefix",
        "suffix": "suffix",
        "male_first_name": "male_first_name",
        "female_first_name": "female_first_name",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "prefix": _PREFIXES,
        "suffix": _SUFFIXES,
    }

    def __init__(self, engine: RandomEngine, locale_data: ModuleType) -> None:
        super().__init__(engine)
        self._first_names: tuple[str, ...] = locale_data.first_names
        self._last_names: tuple[str, ...] = locale_data.last_names
        # Gendered names — optional in locale data; fall back to full list
        self._male_first_names: tuple[str, ...] = getattr(
            locale_data, "male_first_names", locale_data.first_names
        )
        self._female_first_names: tuple[str, ...] = getattr(
            locale_data, "female_first_names", locale_data.first_names
        )

    # Public API

    def first_name(self, count: int = 1) -> str | list[str]:
        """Generate a random first name."""
        if count == 1:
            return self._engine.choice(self._first_names)
        return self._engine.choices(self._first_names, count)

    def last_name(self, count: int = 1) -> str | list[str]:
        """Generate a random last name."""
        if count == 1:
            return self._engine.choice(self._last_names)
        return self._engine.choices(self._last_names, count)

    def full_name(self, count: int = 1) -> str | list[str]:
        """Generate a random full name (first + last)."""
        if count == 1:
            first = self._engine.choice(self._first_names)
            last = self._engine.choice(self._last_names)
            return f"{first} {last}"

        firsts = self._engine.choices(self._first_names, count)
        lasts = self._engine.choices(self._last_names, count)
        return [f"{f} {ln}" for f, ln in zip(firsts, lasts)]

    def male_first_name(self, count: int = 1) -> str | list[str]:
        """Generate a random male first name."""
        if count == 1:
            return self._engine.choice(self._male_first_names)
        return self._engine.choices(self._male_first_names, count)

    def female_first_name(self, count: int = 1) -> str | list[str]:
        """Generate a random female first name."""
        if count == 1:
            return self._engine.choice(self._female_first_names)
        return self._engine.choices(self._female_first_names, count)
