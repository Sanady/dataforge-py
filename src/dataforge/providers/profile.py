"""ProfileProvider — generates coherent fake user profiles.

Each profile composes data from multiple providers (person, internet,
address, phone) to produce a consistent record where names, emails,
and usernames relate to each other.

Individual string fields are exposed in ``_field_map`` for Schema
compatibility.  The compound ``profile()`` method returns a ``dict``
and is available only via direct API use.
"""

from typing import TYPE_CHECKING

from dataforge.backend import RandomEngine
from dataforge.providers.base import BaseProvider

if TYPE_CHECKING:
    from dataforge.core import DataForge


class ProfileProvider(BaseProvider):
    """Generates coherent fake user profiles."""

    __slots__ = ("_forge",)

    _provider_name = "profile"
    _locale_modules: tuple[str, ...] = ()
    _needs_forge: bool = True
    _field_map: dict[str, str] = {
        "profile_first_name": "profile_first_name",
        "profile_last_name": "profile_last_name",
        "profile_email": "profile_email",
        "profile_phone": "profile_phone",
        "profile_city": "profile_city",
        "profile_state": "profile_state",
        "profile_zip_code": "profile_zip_code",
        "profile_job_title": "profile_job_title",
    }

    def __init__(self, engine: RandomEngine, forge: "DataForge") -> None:
        super().__init__(engine)
        self._forge = forge

    # Individual field methods (for _field_map / Schema compatibility)

    def profile_first_name(self, count: int = 1) -> str | list[str]:
        """Generate a first name (delegates to PersonProvider)."""
        return self._forge.person.first_name(count)

    def profile_last_name(self, count: int = 1) -> str | list[str]:
        """Generate a last name (delegates to PersonProvider)."""
        return self._forge.person.last_name(count)

    def profile_email(self, count: int = 1) -> str | list[str]:
        """Generate an email address (delegates to InternetProvider)."""
        return self._forge.internet.email(count)

    def profile_phone(self, count: int = 1) -> str | list[str]:
        """Generate a phone number (delegates to PhoneProvider)."""
        return self._forge.phone.phone_number(count)

    def profile_city(self, count: int = 1) -> str | list[str]:
        """Generate a city name (delegates to AddressProvider)."""
        return self._forge.address.city(count)

    def profile_state(self, count: int = 1) -> str | list[str]:
        """Generate a state name (delegates to AddressProvider)."""
        return self._forge.address.state(count)

    def profile_zip_code(self, count: int = 1) -> str | list[str]:
        """Generate a zip code (delegates to AddressProvider)."""
        return self._forge.address.zip_code(count)

    def profile_job_title(self, count: int = 1) -> str | list[str]:
        """Generate a job title (delegates to CompanyProvider)."""
        return self._forge.company.job_title(count)

    # Compound profile method (direct API only, not in _field_map)

    def profile(self, count: int = 1) -> dict[str, str] | list[dict[str, str]]:
        """Generate a coherent user profile dict."""

        def _one_profile() -> dict[str, str]:
            first = self._forge.person.first_name()
            last = self._forge.person.last_name()
            # Build a coherent email from the person's name
            domain = self._forge.internet.domain()
            email = f"{first.lower()}.{last.lower()}@{domain}"
            return {
                "first_name": first,
                "last_name": last,
                "email": email,
                "phone": self._forge.phone.phone_number(),
                "city": self._forge.address.city(),
                "state": self._forge.address.state(),
                "zip_code": self._forge.address.zip_code(),
                "job_title": self._forge.company.job_title(),
            }

        if count == 1:
            return _one_profile()
        return [_one_profile() for _ in range(count)]
