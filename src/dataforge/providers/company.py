"""Company provider — generates fake company names, catch phrases, job titles."""

from types import ModuleType

from dataforge.backend import RandomEngine
from dataforge.providers.base import BaseProvider


class CompanyProvider(BaseProvider):
    """Generates fake company names, catch phrases, and job titles.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    locale_data : ModuleType
        The imported locale module (e.g. ``dataforge.locales.en_US.company``).
    """

    __slots__ = (
        "_company_names",
        "_company_suffixes",
        "_catch_phrase_adjectives",
        "_catch_phrase_nouns",
        "_job_titles",
    )

    _provider_name = "company"
    _locale_modules = ("company",)
    _field_map = {
        "company_name": "company_name",
        "company": "company_name",
        "company_suffix": "company_suffix",
        "catch_phrase": "catch_phrase",
        "job_title": "job_title",
        "job": "job_title",
    }

    def __init__(self, engine: RandomEngine, locale_data: ModuleType) -> None:
        super().__init__(engine)
        self._company_names: tuple[str, ...] = locale_data.company_names
        self._company_suffixes: tuple[str, ...] = locale_data.company_suffixes
        self._catch_phrase_adjectives: tuple[str, ...] = (
            locale_data.catch_phrase_adjectives
        )
        self._catch_phrase_nouns: tuple[str, ...] = locale_data.catch_phrase_nouns
        self._job_titles: tuple[str, ...] = locale_data.job_titles

    # Scalar helpers

    def _one_company_name(self) -> str:
        name = self._engine.choice(self._company_names)
        suffix = self._engine.choice(self._company_suffixes)
        return f"{name} {suffix}"

    def _one_catch_phrase(self) -> str:
        adj = self._engine.choice(self._catch_phrase_adjectives)
        noun = self._engine.choice(self._catch_phrase_nouns)
        return f"{adj} {noun}"

    # Public API

    def company_name(self, count: int = 1) -> str | list[str]:
        """Generate a random company name."""
        if count == 1:
            return self._one_company_name()
        # Vectorized: 2 bulk choices() + zip (avoids N scalar calls)
        names = self._engine.choices(self._company_names, count)
        suffixes = self._engine.choices(self._company_suffixes, count)
        return [f"{n} {s}" for n, s in zip(names, suffixes)]

    def company_suffix(self, count: int = 1) -> str | list[str]:
        """Generate a random company suffix."""
        if count == 1:
            return self._engine.choice(self._company_suffixes)
        return self._engine.choices(self._company_suffixes, count)

    def catch_phrase(self, count: int = 1) -> str | list[str]:
        """Generate a random catch phrase."""
        if count == 1:
            return self._one_catch_phrase()
        # Vectorized: 2 bulk choices() + zip (avoids N scalar calls)
        adjs = self._engine.choices(self._catch_phrase_adjectives, count)
        nouns = self._engine.choices(self._catch_phrase_nouns, count)
        return [f"{a} {n}" for a, n in zip(adjs, nouns)]

    def job_title(self, count: int = 1) -> str | list[str]:
        """Generate a random job title."""
        if count == 1:
            return self._engine.choice(self._job_titles)
        return self._engine.choices(self._job_titles, count)
