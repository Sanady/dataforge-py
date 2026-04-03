"""Government provider — SSN, tax IDs, passport numbers, driver's licenses."""

from dataforge.providers.base import BaseProvider


class GovernmentProvider(BaseProvider):
    """Generates fake government identification data."""

    __slots__ = ()

    _provider_name = "government"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "ssn": "ssn",
        "tax_id": "tax_id",
        "passport_number": "passport_number",
        "passport": "passport_number",
        "drivers_license": "drivers_license",
        "national_id": "national_id",
    }

    def _one_ssn(self) -> str:
        """Generate US-format SSN: ###-##-####."""
        _ri = self._engine.random_int
        area = _ri(1, 899)
        if area == 666:
            area = 667
        group = _ri(1, 99)
        serial = _ri(1, 9999)
        return f"{area:03d}-{group:02d}-{serial:04d}"

    def _one_tax_id(self) -> str:
        """Generate US EIN format: ##-#######."""
        return f"{self._engine.random_int(10, 99)}-{self._engine.random_digits_str(7)}"

    def _one_passport(self) -> str:
        """Generate US passport number: letter + 8 digits."""
        letter = chr(self._engine.random_int(65, 90))  # A-Z
        return letter + self._engine.random_digits_str(8)

    def _one_drivers_license(self) -> str:
        """Generate US-style driver's license: letter + 7 digits."""
        letter = chr(self._engine.random_int(65, 90))
        return letter + self._engine.random_digits_str(7)

    def _one_national_id(self) -> str:
        """Generate a national ID number: 10-digit numeric string."""
        return self._engine.random_digits_str(10)

    def ssn(self, count: int = 1) -> str | list[str]:
        """Generate a US Social Security Number (###-##-####)."""
        if count == 1:
            return self._one_ssn()
        return [self._one_ssn() for _ in range(count)]

    def tax_id(self, count: int = 1) -> str | list[str]:
        """Generate a US Employer Identification Number (##-#######)."""
        if count == 1:
            return self._one_tax_id()
        return [self._one_tax_id() for _ in range(count)]

    def passport_number(self, count: int = 1) -> str | list[str]:
        """Generate a US passport number (letter + 8 digits)."""
        if count == 1:
            return self._one_passport()
        return [self._one_passport() for _ in range(count)]

    def drivers_license(self, count: int = 1) -> str | list[str]:
        """Generate a US-style driver's license number."""
        if count == 1:
            return self._one_drivers_license()
        return [self._one_drivers_license() for _ in range(count)]

    def national_id(self, count: int = 1) -> str | list[str]:
        """Generate a 10-digit national ID number."""
        if count == 1:
            return self._one_national_id()
        return [self._one_national_id() for _ in range(count)]
