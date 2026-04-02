"""DataForge — the main entry point for fake data generation.

Usage::

    from dataforge import DataForge

    forge = DataForge(locale="en_US", seed=42)

    forge.person.first_name()           # "James"
    forge.person.full_name(count=1000)  # list of 1000 full names
    forge.address.full_address()        # "4821 Oak Ave, Chicago, IL 60614"
    forge.internet.email()              # "james.smith@gmail.com"
    forge.company.company_name()        # "Acme Inc"
    forge.phone.phone_number()          # "555-123-4567"
    forge.lorem.sentence()              # "Lorem ipsum dolor sit amet."
    forge.dt.date()                     # "2024-03-15"
"""

import importlib
from typing import TYPE_CHECKING, Any
from types import ModuleType

from dataforge.backend import RandomEngine
from dataforge.providers.base import BaseProvider

# ------------------------------------------------------------------
# Heuristic field-name mappings for ORM / model introspection
# ------------------------------------------------------------------

# Maps common model field names to DataForge field shorthand names.
# Used by schema_from_pydantic() and schema_from_sqlalchemy().
_FIELD_ALIASES: dict[str, str] = {
    # Person
    "name": "full_name",
    "full_name": "full_name",
    "fname": "first_name",
    "lname": "last_name",
    "surname": "last_name",
    "last": "last_name",
    "first": "first_name",
    "given_name": "first_name",
    "family_name": "last_name",
    "username": "username",
    "user_name": "username",
    # Contact
    "email_address": "email",
    "mail": "email",
    "phone": "phone_number",
    "phone_num": "phone_number",
    "telephone": "phone_number",
    "cell": "cell_number",
    "mobile": "cell_number",
    "cell_phone": "cell_number",
    "mobile_phone": "cell_number",
    # Address
    "street": "street_address",
    "street_addr": "street_address",
    "addr": "full_address",
    "address": "full_address",
    "zip": "zipcode",
    "zip_code": "zipcode",
    "postal_code": "zipcode",
    "postcode": "zipcode",
    "state_abbr": "state_abbreviation",
    "country_name": "country",
    # Internet
    "url": "url",
    "website": "url",
    "domain": "domain_name",
    "ip": "ipv4",
    "ip_address": "ipv4",
    "ipv4_address": "ipv4",
    "ipv6_address": "ipv6",
    "mac": "mac_address",
    "user_agent_string": "user_agent",
    # Company
    "company": "company_name",
    "company_nm": "company_name",
    "job": "job_title",
    "job_name": "job_title",
    "occupation": "job_title",
    "title": "job_title",
    # Finance
    "credit_card": "credit_card_number",
    "cc_number": "credit_card_number",
    "card_number": "credit_card_number",
    "iban_code": "iban",
    "currency": "currency_code",
    # Datetime
    "date": "date",
    "dob": "date_of_birth",
    "birth_date": "date_of_birth",
    "birthday": "date_of_birth",
    "time": "time",
    "datetime": "datetime",
    "created_at": "datetime",
    "updated_at": "datetime",
    "timestamp": "datetime",
    # Misc
    "uuid": "uuid4",
    "guid": "uuid4",
    "description": "sentence",
    "bio": "paragraph",
    "summary": "sentence",
    "note": "sentence",
    "notes": "paragraph",
    "comment": "sentence",
    "body": "paragraph",
    "text": "paragraph",
    "content": "paragraph",
    # Color
    "color": "color_name",
    "colour": "color_name",
    "hex_color": "hex_color",
    # File
    "filename": "file_name",
    "file": "file_name",
    "extension": "file_extension",
    "mime": "mime_type",
    "mime_type": "mime_type",
    # Network
    "port": "port",
    "hostname": "hostname",
    # Geo
    "latitude": "latitude",
    "lat": "latitude",
    "longitude": "longitude",
    "lng": "longitude",
    "lon": "longitude",
    # Government
    "ssn": "ssn",
    "tax_id": "tax_id",
    "passport": "passport_number",
    "passport_no": "passport_number",
}

# ------------------------------------------------------------------
# Type-based fallback mappings for ORM / model introspection
# ------------------------------------------------------------------
# When a field name cannot be resolved by name alone, these
# fallbacks map Python type annotations to sensible DataForge fields.
# Keys are (module, qualname) tuples for non-builtin types.

_TYPE_FALLBACK_BUILTINS: dict[type, str] = {
    str: "misc.random_element",  # sentinel — too ambiguous
    int: "misc.random_element",  # sentinel — handled specially
    float: "misc.random_element",  # sentinel — handled specially
    bool: "boolean",
}

# String-based type name fallbacks for stdlib / common types.
# Keyed on the type's __qualname__ (works without importing the module).
_TYPE_FALLBACK_NAMES: dict[str, str] = {
    "date": "date",
    "datetime": "datetime",
    "time": "time",
    "Decimal": "misc.random_element",  # sentinel
    "UUID": "uuid4",
}


def _resolve_type_annotation(annotation: Any) -> type | None:
    """Extract the concrete type from a possibly-wrapped annotation.

    Handles ``Optional[X]``, ``X | None``, ``list[X]``, and plain types.
    Returns the core type or ``None`` if it cannot be determined.
    """
    import typing
    import types as _types

    origin = getattr(annotation, "__origin__", None)

    # Handle Optional[X] = Union[X, None]
    if origin is typing.Union or isinstance(annotation, _types.UnionType):
        args = getattr(annotation, "__args__", ())
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            return _resolve_type_annotation(non_none[0])
        return None

    # Handle list[X], List[X] — ignore container, use element type
    if origin is list:
        args = getattr(annotation, "__args__", ())
        if args:
            return _resolve_type_annotation(args[0])
        return None

    # Plain type
    if isinstance(annotation, type):
        return annotation

    return None


def _type_fallback(annotation: Any) -> str | None:
    """Map a Python type annotation to a DataForge field name.

    Returns ``None`` if no sensible fallback exists.  For ``str``,
    ``int``, and ``float``, returns ``None`` because bare types are
    too ambiguous to guess meaningfully.
    """
    concrete = _resolve_type_annotation(annotation)
    if concrete is None:
        return None

    # Check builtin types
    if concrete in _TYPE_FALLBACK_BUILTINS:
        result = _TYPE_FALLBACK_BUILTINS[concrete]
        # Skip ambiguous numeric types — require name-based match
        if result == "misc.random_element":
            return None
        return result

    # Check by qualname (datetime.date, uuid.UUID, etc.)
    qualname = getattr(concrete, "__qualname__", "")
    if qualname in _TYPE_FALLBACK_NAMES:
        result = _TYPE_FALLBACK_NAMES[qualname]
        if result == "misc.random_element":
            return None
        return result

    return None


def _pydantic_heuristic(field_name: str) -> str | None:
    """Map a Pydantic field name to a DataForge field name (or None)."""
    return _FIELD_ALIASES.get(field_name)


def _sqlalchemy_heuristic(col_name: str, column: "Any") -> str | None:
    """Map a SQLAlchemy column name to a DataForge field name (or None).

    Uses the column name first, then falls back to type-based
    heuristics for common SQL column types.
    """
    alias = _FIELD_ALIASES.get(col_name)
    if alias:
        return alias
    # Type-based fallback: if the column is an Integer primary key
    # we already skip it.  Other type-based heuristics could go here.
    return None


_SA_TYPE_MAP: dict[str, str | None] = {
    "String": None,
    "Text": "paragraph",
    "Integer": None,
    "BigInteger": None,
    "SmallInteger": None,
    "Float": None,
    "Numeric": None,
    "Boolean": "boolean",
    "Date": "date",
    "DateTime": "datetime",
    "Time": "time",
    "Uuid": "uuid4",
    "UUID": "uuid4",
}


def _sqlalchemy_type_fallback(column: "Any") -> str | None:
    """Map a SQLAlchemy column type to a DataForge field name.

    Uses the column's type class name to identify common SQL types
    and map them to appropriate DataForge generators.
    """
    col_type = type(column.type)
    type_name = col_type.__name__
    return _SA_TYPE_MAP.get(type_name)


if TYPE_CHECKING:
    from dataforge.providers.address import AddressProvider
    from dataforge.providers.automotive import AutomotiveProvider
    from dataforge.providers.barcode import BarcodeProvider
    from dataforge.providers.color import ColorProvider
    from dataforge.providers.company import CompanyProvider
    from dataforge.providers.crypto import CryptoProvider
    from dataforge.providers.datetime import DateTimeProvider
    from dataforge.providers.ecommerce import EcommerceProvider
    from dataforge.providers.education import EducationProvider
    from dataforge.providers.file import FileProvider
    from dataforge.providers.finance import FinanceProvider
    from dataforge.providers.geo import GeoProvider
    from dataforge.providers.government import GovernmentProvider
    from dataforge.providers.internet import InternetProvider
    from dataforge.providers.lorem import LoremProvider
    from dataforge.providers.medical import MedicalProvider
    from dataforge.providers.misc import MiscProvider
    from dataforge.providers.network import NetworkProvider
    from dataforge.providers.payment import PaymentProvider
    from dataforge.providers.person import PersonProvider
    from dataforge.providers.phone import PhoneProvider
    from dataforge.providers.profile import ProfileProvider
    from dataforge.providers.science import ScienceProvider
    from dataforge.providers.text import TextProvider
    from dataforge.providers.ai_prompt import AiPromptProvider
    from dataforge.providers.llm import LlmProvider
    from dataforge.providers.ai_chat import AiChatProvider
    from dataforge.providers.social_media import SocialMediaProvider
    from dataforge.providers.music import MusicProvider
    from dataforge.providers.sports import SportsProvider
    from dataforge.providers.food import FoodProvider
    from dataforge.providers.legal import LegalProvider
    from dataforge.providers.real_estate import RealEstateProvider
    from dataforge.providers.weather import WeatherProvider
    from dataforge.providers.hardware import HardwareProvider
    from dataforge.providers.logistics import LogisticsProvider


class DataForge:
    """High-performance fake data generator.

    Providers are loaded **lazily** — nothing is imported until a
    provider property is first accessed.  The provider registry
    (:mod:`dataforge.registry`) resolves field names and provider
    classes automatically, so new providers can be added without
    editing this file.

    Parameters
    ----------
    locale : str
        The locale to use for data generation (e.g. ``"en_US"``).
        Locale data is loaded **lazily** — nothing is imported until
        a provider property is first accessed.
    seed : int | None
        Optional seed for reproducible output.  When set, the stdlib
        ``random`` backend is seeded for deterministic generation.

    Examples
    --------
    >>> forge = DataForge(seed=42)
    >>> forge.person.first_name()
    '...'
    >>> forge.address.city()
    '...'
    >>> forge.internet.email()
    '...'
    >>> forge.company.company_name()
    '...'
    >>> forge.phone.phone_number()
    '...'
    >>> forge.lorem.sentence()
    '...'
    >>> forge.dt.date()
    '...'
    >>> forge.finance.credit_card_number()
    '...'
    >>> forge.color.hex_color()
    '...'
    >>> forge.file.file_name()
    '...'
    >>> forge.network.ipv6()
    '...'
    >>> forge.misc.uuid4()
    '...'
    >>> forge.barcode.ean13()
    '...'
    """

    __slots__ = (
        "_engine",
        "_locale",
        "_providers",
        "_locale_cache",
        "_unique_proxy",
    )

    def __init__(self, locale: str = "en_US", seed: int | None = None) -> None:
        self._engine = RandomEngine(seed=seed)
        self._locale = locale
        self._providers: dict[str, BaseProvider] = {}
        self._locale_cache: dict[str, ModuleType] = {}
        self._unique_proxy: Any = None

    # ------------------------------------------------------------------
    # Dynamic provider access via registry
    # ------------------------------------------------------------------

    def _get_provider(self, name: str) -> BaseProvider:
        """Lazily instantiate and cache a provider by registry name.

        Uses the provider registry to resolve the class and its
        locale module requirements.  Providers are instantiated once
        and cached in ``_providers``.
        """
        prov = self._providers.get(name)
        if prov is not None:
            return prov

        from dataforge.registry import get_provider_info

        info = get_provider_info()
        if name not in info:
            raise AttributeError(
                f"DataForge has no provider '{name}'. "
                f"Available: {', '.join(sorted(info))}"
            )

        cls, locale_modules = info[name]
        if getattr(cls, "_needs_forge", False):
            # Compound provider that needs access to the DataForge instance
            prov = cls(self._engine, self)
        elif locale_modules:
            # Provider needs locale data modules
            locale_args = [self._load_locale_module(mod) for mod in locale_modules]
            prov = cls(self._engine, *locale_args)
        else:
            prov = cls(self._engine)

        self._providers[name] = prov
        return prov

    # ------------------------------------------------------------------
    # Explicit provider properties (for IDE autocomplete + type safety)
    # These delegate to _get_provider() which uses the registry.
    # ------------------------------------------------------------------

    @property
    def person(self) -> "PersonProvider":
        """Access the person data provider (names, prefixes, suffixes)."""
        return self._get_provider("person")  # type: ignore[return-value]

    @property
    def address(self) -> "AddressProvider":
        """Access the address data provider (streets, cities, zip codes)."""
        return self._get_provider("address")  # type: ignore[return-value]

    @property
    def internet(self) -> "InternetProvider":
        """Access the internet data provider (emails, usernames, domains, IPs)."""
        return self._get_provider("internet")  # type: ignore[return-value]

    @property
    def company(self) -> "CompanyProvider":
        """Access the company data provider (names, catch phrases, job titles)."""
        return self._get_provider("company")  # type: ignore[return-value]

    @property
    def phone(self) -> "PhoneProvider":
        """Access the phone data provider (phone numbers, cell numbers)."""
        return self._get_provider("phone")  # type: ignore[return-value]

    @property
    def lorem(self) -> "LoremProvider":
        """Access the Lorem Ipsum text provider (words, sentences, paragraphs)."""
        return self._get_provider("lorem")  # type: ignore[return-value]

    @property
    def dt(self) -> "DateTimeProvider":
        """Access the datetime provider (dates, times, datetimes)."""
        return self._get_provider("dt")  # type: ignore[return-value]

    @property
    def finance(self) -> "FinanceProvider":
        """Access the finance provider (credit cards, IBANs, currencies)."""
        return self._get_provider("finance")  # type: ignore[return-value]

    @property
    def color(self) -> "ColorProvider":
        """Access the color provider (hex, RGB, HSL, color names)."""
        return self._get_provider("color")  # type: ignore[return-value]

    @property
    def file(self) -> "FileProvider":
        """Access the file provider (file names, extensions, MIME types, paths)."""
        return self._get_provider("file")  # type: ignore[return-value]

    @property
    def network(self) -> "NetworkProvider":
        """Access the network provider (IPv6, MAC, port, hostname, user agent)."""
        return self._get_provider("network")  # type: ignore[return-value]

    @property
    def misc(self) -> "MiscProvider":
        """Access the misc provider (UUID4, boolean, random_element, null_or)."""
        return self._get_provider("misc")  # type: ignore[return-value]

    @property
    def barcode(self) -> "BarcodeProvider":
        """Access the barcode provider (EAN-13, EAN-8, ISBN-13, ISBN-10)."""
        return self._get_provider("barcode")  # type: ignore[return-value]

    @property
    def crypto(self) -> "CryptoProvider":
        """Access the crypto provider (MD5, SHA-1, SHA-256 hex strings)."""
        return self._get_provider("crypto")  # type: ignore[return-value]

    @property
    def automotive(self) -> "AutomotiveProvider":
        """Access the automotive provider (plates, VINs, makes, models)."""
        return self._get_provider("automotive")  # type: ignore[return-value]

    @property
    def education(self) -> "EducationProvider":
        """Access the education provider (universities, degrees, fields)."""
        return self._get_provider("education")  # type: ignore[return-value]

    @property
    def profile(self) -> "ProfileProvider":
        """Access the profile provider (coherent user profiles)."""
        return self._get_provider("profile")  # type: ignore[return-value]

    @property
    def government(self) -> "GovernmentProvider":
        """Access the government provider (SSN, tax ID, passports)."""
        return self._get_provider("government")  # type: ignore[return-value]

    @property
    def ecommerce(self) -> "EcommerceProvider":
        """Access the e-commerce provider (products, SKUs, orders)."""
        return self._get_provider("ecommerce")  # type: ignore[return-value]

    @property
    def medical(self) -> "MedicalProvider":
        """Access the medical provider (ICD-10, drugs, blood types)."""
        return self._get_provider("medical")  # type: ignore[return-value]

    @property
    def payment(self) -> "PaymentProvider":
        """Access the payment provider (card types, processors, transactions)."""
        return self._get_provider("payment")  # type: ignore[return-value]

    @property
    def text(self) -> "TextProvider":
        """Access the text provider (quotes, headlines, paragraphs)."""
        return self._get_provider("text")  # type: ignore[return-value]

    @property
    def geo(self) -> "GeoProvider":
        """Access the geo provider (continents, oceans, rivers, coordinates)."""
        return self._get_provider("geo")  # type: ignore[return-value]

    @property
    def science(self) -> "ScienceProvider":
        """Access the science provider (elements, planets, units)."""
        return self._get_provider("science")  # type: ignore[return-value]

    @property
    def ai_prompt(self) -> "AiPromptProvider":
        """Access the AI prompt provider (user/system/creative prompts)."""
        return self._get_provider("ai_prompt")  # type: ignore[return-value]

    @property
    def llm(self) -> "LlmProvider":
        """Access the LLM provider (models, agents, RAG, moderation, billing)."""
        return self._get_provider("llm")  # type: ignore[return-value]

    @property
    def ai_chat(self) -> "AiChatProvider":
        """Access the AI chat provider (conversation turns, messages)."""
        return self._get_provider("ai_chat")  # type: ignore[return-value]

    @property
    def social_media(self) -> "SocialMediaProvider":
        """Access the social media provider (platforms, usernames, hashtags)."""
        return self._get_provider("social_media")  # type: ignore[return-value]

    @property
    def music(self) -> "MusicProvider":
        """Access the music provider (genres, artists, albums, songs)."""
        return self._get_provider("music")  # type: ignore[return-value]

    @property
    def sports(self) -> "SportsProvider":
        """Access the sports provider (sports, teams, leagues, venues)."""
        return self._get_provider("sports")  # type: ignore[return-value]

    @property
    def food(self) -> "FoodProvider":
        """Access the food provider (dishes, cuisines, ingredients, restaurants)."""
        return self._get_provider("food")  # type: ignore[return-value]

    @property
    def legal(self) -> "LegalProvider":
        """Access the legal provider (cases, courts, practice areas, firms)."""
        return self._get_provider("legal")  # type: ignore[return-value]

    @property
    def real_estate(self) -> "RealEstateProvider":
        """Access the real estate provider (properties, prices, neighborhoods)."""
        return self._get_provider("real_estate")  # type: ignore[return-value]

    @property
    def weather(self) -> "WeatherProvider":
        """Access the weather provider (conditions, temperature, wind, alerts)."""
        return self._get_provider("weather")  # type: ignore[return-value]

    @property
    def hardware(self) -> "HardwareProvider":
        """Access the hardware provider (CPUs, GPUs, RAM, storage, peripherals)."""
        return self._get_provider("hardware")  # type: ignore[return-value]

    @property
    def logistics(self) -> "LogisticsProvider":
        """Access the logistics provider (carriers, shipping, containers, tracking)."""
        return self._get_provider("logistics")  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Unique value generation
    # ------------------------------------------------------------------

    @property
    def unique(self) -> "Any":
        """Access the unique-value proxy.

        Returns a proxy that ensures every value returned by a
        provider method is unique within the proxy's lifetime.
        Call ``forge.unique.clear()`` to reset tracking.

        Examples
        --------
        >>> forge = DataForge(seed=42)
        >>> a = forge.unique.person.first_name()
        >>> b = forge.unique.person.first_name()
        >>> a != b
        True
        """
        if self._unique_proxy is None:
            from dataforge.unique import UniqueProxy

            self._unique_proxy = UniqueProxy(self)
        return self._unique_proxy

    # ------------------------------------------------------------------
    # Provider registration
    # ------------------------------------------------------------------

    def register_provider(
        self,
        provider_cls: type[BaseProvider],
        name: str | None = None,
    ) -> None:
        """Register a custom provider class at runtime.

        The provider is added to this ``DataForge`` instance's
        internal registry and can be accessed via ``getattr``.

        Parameters
        ----------
        provider_cls : type[BaseProvider]
            The provider class to register.  Must be a
            ``BaseProvider`` subclass with ``_provider_name``.
        name : str | None
            Override the provider name.  Defaults to the class's
            ``_provider_name`` attribute.

        Examples
        --------
        >>> from dataforge.providers.base import BaseProvider
        >>> from dataforge.backend import RandomEngine
        >>> class MyProvider(BaseProvider):
        ...     _provider_name = "my"
        ...     _field_map = {"greeting": "greeting"}
        ...     def greeting(self, count=1):
        ...         return "hello" if count == 1 else ["hello"] * count
        >>> forge = DataForge()
        >>> forge.register_provider(MyProvider)
        >>> forge.my.greeting()
        'hello'
        """
        prov_name = name or getattr(provider_cls, "_provider_name", "")
        if not prov_name:
            raise ValueError(
                f"{provider_cls.__name__} does not define '_provider_name'."
            )
        locale_modules = getattr(provider_cls, "_locale_modules", ())
        if getattr(provider_cls, "_needs_forge", False):
            prov = provider_cls(self._engine, self)  # type: ignore[call-arg]
        elif locale_modules:
            locale_args = [self._load_locale_module(mod) for mod in locale_modules]
            prov = provider_cls(self._engine, *locale_args)  # type: ignore[call-arg]
        else:
            prov = provider_cls(self._engine)
        self._providers[prov_name] = prov

        # Register field mappings so Schema/to_dict can find them
        from dataforge.registry import register_runtime_provider

        register_runtime_provider(prov_name, provider_cls, locale_modules)

    def __getattr__(self, name: str) -> Any:
        """Dynamic attribute access for registered providers.

        Allows ``forge.my_provider`` to work for providers
        registered via :meth:`register_provider` at runtime,
        without requiring a ``@property`` on the class.
        """
        # Check if it's a cached provider
        providers = object.__getattribute__(self, "_providers")
        if name in providers:
            return providers[name]
        # Try registry lookup
        try:
            return self._get_provider(name)
        except AttributeError:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            ) from None

    # ------------------------------------------------------------------
    # Seed control
    # ------------------------------------------------------------------

    def seed(self, value: int) -> None:
        """Re-seed the random engine for reproducible output.

        This resets the internal state of the stdlib ``random`` backend.
        """
        self._engine.seed(value)

    def copy(self, seed: int | None = None) -> "DataForge":
        """Create a new ``DataForge`` instance with the same locale.

        Parameters
        ----------
        seed : int | None
            Optional seed for the new instance.  If ``None``, the new
            instance is unseeded (non-deterministic).

        Returns
        -------
        DataForge
        """
        return DataForge(locale=self._locale, seed=seed)

    # ------------------------------------------------------------------
    # Schema API
    # ------------------------------------------------------------------

    def schema(
        self,
        fields: "list[str] | dict[str, Any]",
        null_fields: "dict[str, float] | None" = None,
        unique_together: "list[tuple[str, ...]] | None" = None,
        chaos: "Any | None" = None,
    ) -> "Any":
        """Create a pre-resolved :class:`Schema` for maximum throughput.

        Parameters
        ----------
        fields : list[str] | dict[str, str | Callable | dict]
            Fields to generate.  String values are resolved to provider
            methods.  Callable values receive the current row dict and
            can reference previously generated columns.  Dict values
            define constraints (``depends_on``, ``temporal``, ``correlate``,
            ``conditional``, ``range``).
        null_fields : dict[str, float] | None
            Optional mapping of column names to null probabilities
            (0.0–1.0).  Example: ``{"email": 0.3}`` makes ~30% of
            email values ``None``.
        unique_together : list[tuple[str, ...]] | None
            Optional list of column-name tuples whose combinations
            must be unique.  Example: ``[("first_name", "last_name")]``
            ensures no two rows share the same name pair.
        chaos : ChaosTransformer | dict | None
            Optional chaos/data-quality transformer.  Pass a
            :class:`~dataforge.chaos.ChaosTransformer` instance or a
            config dict (e.g. ``{"null_rate": 0.1, "type_mismatch_rate": 0.05}``).

        Returns
        -------
        Schema

        Examples
        --------
        >>> forge = DataForge(seed=42)
        >>> s = forge.schema(["first_name", "email"])
        >>> rows = s.generate(count=1000)

        Nullable fields:

        >>> s = forge.schema(["first_name", "email"],
        ...                  null_fields={"email": 0.2})
        >>> rows = s.generate(count=100)
        >>> none_count = sum(1 for r in rows if r["email"] is None)

        Unique combinations:

        >>> s = forge.schema(["first_name", "last_name", "email"],
        ...                  unique_together=[("first_name", "last_name")])
        >>> rows = s.generate(count=50)

        Constrained/correlated fields:

        >>> s = forge.schema({
        ...     "country": "country",
        ...     "state": {"field": "address.state", "depends_on": "country"},
        ... })
        >>> rows = s.generate(count=100)

        Chaos mode:

        >>> from dataforge.chaos import ChaosTransformer
        >>> s = forge.schema(["first_name", "email"],
        ...                  chaos=ChaosTransformer(null_rate=0.1))
        >>> rows = s.generate(count=100)
        """
        from dataforge.schema import Schema

        return Schema(
            self,
            fields,
            null_fields=null_fields,
            unique_together=unique_together,
            chaos=chaos,
        )

    def relational(
        self,
        tables: "dict[str, dict[str, Any]]",
    ) -> "Any":
        """Create a :class:`RelationalSchema` for multi-table generation.

        Generates related tables with referential integrity.  Parent
        tables are generated first; child tables receive foreign keys
        pointing to parent rows.

        Parameters
        ----------
        tables : dict[str, dict]
            Table specifications.  Each spec can include:

            - ``fields`` — list or dict of field specs (same as Schema)
            - ``count`` — number of rows (default: 10)
            - ``parent`` — name of the parent table (creates a FK)
            - ``parent_key`` — FK column name (default: ``{parent}_id``)
            - ``children_per_parent`` — ``(min, max)`` cardinality bounds
            - ``null_fields`` — per-field null probabilities

        Returns
        -------
        RelationalSchema

        Examples
        --------
        >>> forge = DataForge(seed=42)
        >>> rel = forge.relational({
        ...     "users": {
        ...         "fields": ["first_name", "last_name", "email"],
        ...         "count": 10,
        ...     },
        ...     "orders": {
        ...         "fields": ["date", "city"],
        ...         "count": 30,
        ...         "parent": "users",
        ...     },
        ... })
        >>> data = rel.generate()
        >>> len(data["users"])
        10
        """
        from dataforge.relational import RelationalSchema

        return RelationalSchema(self, tables)

    def schema_from_dict(
        self,
        d: dict[str, Any],
    ) -> "Any":
        """Create a :class:`Schema` from a schema definition dict.

        The dict format matches what :meth:`Schema.to_schema_dict`
        produces, and what :func:`dataforge.schema_io.load_schema`
        reads from JSON/YAML/TOML files.

        Parameters
        ----------
        d : dict[str, Any]
            Schema definition with ``fields``, optional ``count``,
            ``null_fields``, and ``unique_together`` keys.

        Returns
        -------
        Schema

        Examples
        --------
        >>> forge = DataForge(seed=42)
        >>> s = forge.schema_from_dict({
        ...     "fields": {"name": "full_name", "email": "email"},
        ...     "count": 100,
        ... })
        >>> rows = s.generate()  # uses count from dict
        """
        from dataforge.schema import Schema
        from dataforge.schema_io import dict_to_schema_args

        fields, _count, null_fields, unique_together = dict_to_schema_args(d)
        return Schema(
            self,
            fields,
            null_fields=null_fields,
            unique_together=unique_together,
        )

    def schema_from_file(
        self,
        path: str,
        format: str | None = None,
    ) -> "Any":
        """Create a :class:`Schema` by loading a schema definition file.

        Supports JSON, YAML, and TOML formats.  The format is
        auto-detected from the file extension when *format* is
        ``None``.

        Parameters
        ----------
        path : str
            Path to the schema definition file.
        format : str | None
            Input format (``"json"``, ``"yaml"``, ``"toml"``).
            Auto-detected from extension when ``None``.

        Returns
        -------
        Schema

        Examples
        --------
        >>> forge = DataForge(seed=42)
        >>> s = forge.schema_from_file("my_schema.yaml")
        >>> rows = s.generate(count=100)
        """
        from dataforge.schema_io import load_schema

        d = load_schema(path, format=format)
        return self.schema_from_dict(d)

    # ------------------------------------------------------------------
    # Locale management
    # ------------------------------------------------------------------

    @property
    def locale(self) -> str:
        """The currently active locale string (e.g. ``"en_US"``)."""
        return self._locale

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_locale_module(self, module_name: str) -> ModuleType:
        """Dynamically import a locale data module.

        Results are cached so that repeated access to the same provider
        does not re-import the module.

        If the requested locale does not provide the specified module,
        falls back to ``en_US`` and emits a warning.

        Parameters
        ----------
        module_name : str
            The name of the submodule inside the locale package
            (e.g. ``"person"``, ``"address"``).
        """
        key = f"{self._locale}.{module_name}"
        if key not in self._locale_cache:
            try:
                mod = importlib.import_module(
                    f"dataforge.locales.{self._locale}.{module_name}"
                )
            except ModuleNotFoundError:
                if self._locale == "en_US":
                    raise ValueError(
                        f"Locale 'en_US' does not have a '{module_name}' data module."
                    )
                import warnings

                warnings.warn(
                    f"Locale '{self._locale}' does not have a '{module_name}' "
                    f"data module — falling back to 'en_US'.",
                    UserWarning,
                    stacklevel=3,
                )
                mod = importlib.import_module(f"dataforge.locales.en_US.{module_name}")
            self._locale_cache[key] = mod
        return self._locale_cache[key]

    def _resolve_field(self, field: str) -> tuple[str, str]:
        """Resolve a field name to (provider_attr, method_name).

        Supports both direct names (e.g. ``"first_name"``) and
        dotted paths (e.g. ``"person.first_name"``).
        """
        # Dotted path: "person.first_name" → ("person", "first_name")
        if "." in field:
            provider_attr, method_name = field.split(".", 1)
            return provider_attr, method_name

        from dataforge.registry import get_field_map

        fm = get_field_map()
        if field in fm:
            return fm[field]
        raise ValueError(
            f"Unknown field '{field}'. Use dotted notation "
            f"(e.g. 'person.first_name') or a known shorthand."
        )

    # ------------------------------------------------------------------
    # Bulk data generation
    # ------------------------------------------------------------------

    def to_dict(
        self,
        fields: list[str] | dict[str, str],
        count: int = 10,
    ) -> list[dict[str, Any]]:
        """Generate *count* rows of fake data as a list of dicts.

        Uses :class:`Schema` internally for zero-duplication.
        Values are preserved in their native Python types.

        Parameters
        ----------
        fields : list[str] | dict[str, str]
            Fields to generate.  Can be a list of field names (e.g.
            ``["first_name", "email"]``) or a dict mapping output column
            names to field names (e.g. ``{"Name": "full_name"}``).
        count : int
            Number of rows to generate.

        Returns
        -------
        list[dict[str, Any]]
            Each dict maps column name → generated value (native type).

        Examples
        --------
        >>> forge = DataForge(seed=42)
        >>> rows = forge.to_dict(["first_name", "email"], count=3)
        >>> len(rows)
        3
        """
        return self.schema(fields).generate(count=count)

    def to_json(
        self,
        fields: list[str] | dict[str, str],
        count: int = 10,
        path: str | None = None,
        indent: int = 2,
        encoding: str = "utf-8",
        compress: bool | None = None,
    ) -> str:
        """Generate fake data and return as a JSON array.

        Delegates to :meth:`Schema.to_json` for zero-duplication.

        Parameters
        ----------
        fields : list[str] | dict[str, str]
            Fields to generate (same format as :meth:`to_dict`).
        count : int
            Number of rows.
        path : str | None
            If provided, write JSON to this file path.
        indent : int
            JSON indentation level (default: 2).
        encoding : str
            Character encoding for file output (default: utf-8).
        compress : bool | None
            If ``True``, gzip the output file.  ``None`` auto-detects
            from a ``.gz`` file extension.

        Returns
        -------
        str
            The JSON content as a string.
        """
        return self.schema(fields).to_json(
            count=count,
            path=path,
            indent=indent,
            encoding=encoding,
            compress=compress,
        )

    def to_csv(
        self,
        fields: list[str] | dict[str, str],
        count: int = 10,
        path: str | None = None,
        delimiter: str = ",",
        encoding: str = "utf-8",
        compress: bool | None = None,
    ) -> str:
        """Generate fake data and return (or write) as CSV.

        Delegates to :meth:`Schema.to_csv` for zero-duplication.

        Parameters
        ----------
        fields : list[str] | dict[str, str]
            Fields to generate (same format as :meth:`to_dict`).
        count : int
            Number of rows.
        path : str | None
            If provided, write CSV to this file path. Otherwise return
            the CSV as a string.
        delimiter : str
            Field delimiter (default: comma).
        encoding : str
            Character encoding for file output (default: utf-8).
        compress : bool | None
            If ``True``, gzip the output file.  ``None`` auto-detects
            from a ``.gz`` file extension.

        Returns
        -------
        str
            The CSV content as a string.
        """
        return self.schema(fields).to_csv(
            count=count,
            path=path,
            delimiter=delimiter,
            encoding=encoding,
            compress=compress,
        )

    def to_jsonl(
        self,
        fields: list[str] | dict[str, str],
        count: int = 10,
        path: str | None = None,
        encoding: str = "utf-8",
        compress: bool | None = None,
    ) -> str:
        """Generate fake data and return (or write) as JSON Lines.

        Delegates to :meth:`Schema.to_jsonl` for zero-duplication.

        Parameters
        ----------
        fields : list[str] | dict[str, str]
            Fields to generate (same format as :meth:`to_dict`).
        count : int
            Number of rows.
        path : str | None
            If provided, write JSONL to this file path.
        encoding : str
            Character encoding for file output (default: utf-8).
        compress : bool | None
            If ``True``, gzip the output file.  ``None`` auto-detects
            from a ``.gz`` file extension.

        Returns
        -------
        str
            The JSONL content as a string.
        """
        return self.schema(fields).to_jsonl(
            count=count,
            path=path,
            encoding=encoding,
            compress=compress,
        )

    def to_sql(
        self,
        fields: list[str] | dict[str, str],
        table: str,
        count: int = 10,
        dialect: str = "sqlite",
        path: str | None = None,
        encoding: str = "utf-8",
        compress: bool | None = None,
    ) -> str:
        """Generate fake data and return as SQL INSERT statements.

        Delegates to :meth:`Schema.to_sql` for zero-duplication.

        Parameters
        ----------
        fields : list[str] | dict[str, str]
            Fields to generate (same format as :meth:`to_dict`).
        table : str
            Target table name.
        count : int
            Number of rows.
        dialect : str
            SQL dialect: ``"sqlite"``, ``"mysql"``, or ``"postgresql"``.
        path : str | None
            If provided, write SQL to this file path.
        encoding : str
            Character encoding for file output (default: utf-8).
        compress : bool | None
            If ``True``, gzip the output file.  ``None`` auto-detects
            from a ``.gz`` file extension.

        Returns
        -------
        str
            SQL INSERT statements as a string.
        """
        return self.schema(fields).to_sql(
            table=table,
            count=count,
            dialect=dialect,
            path=path,
            encoding=encoding,
            compress=compress,
        )

    def to_dataframe(
        self,
        fields: list[str] | dict[str, str],
        count: int = 10,
    ) -> "Any":
        """Generate fake data as a pandas DataFrame.

        Delegates to :meth:`Schema.to_dataframe` for zero-duplication.
        Requires ``pandas`` to be installed.

        Parameters
        ----------
        fields : list[str] | dict[str, str]
            Fields to generate (same format as :meth:`to_dict`).
        count : int
            Number of rows.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with one column per field.
        """
        return self.schema(fields).to_dataframe(count=count)

    def stream_to_csv(
        self,
        fields: list[str] | dict[str, str],
        path: str,
        count: int = 10,
        batch_size: int | None = None,
        delimiter: str = ",",
        encoding: str = "utf-8",
        compress: bool | None = None,
    ) -> int:
        """Stream fake data directly to a CSV file.

        Memory-efficient: writes in batches without materializing
        all rows in memory.

        Parameters
        ----------
        fields : list[str] | dict[str, str]
            Fields to generate.
        path : str
            File path to write.
        count : int
            Number of rows.
        batch_size : int | None
            Rows per batch.  Auto-tuned when ``None``.
        delimiter : str
            Field delimiter (default: comma).
        encoding : str
            Character encoding (default: utf-8).
        compress : bool | None
            If ``True``, gzip the output.  ``None`` auto-detects
            from a ``.gz`` file extension.

        Returns
        -------
        int
            Number of rows written.
        """
        return self.schema(fields).stream_to_csv(
            path=path,
            count=count,
            batch_size=batch_size,
            delimiter=delimiter,
            encoding=encoding,
            compress=compress,
        )

    def stream_to_jsonl(
        self,
        fields: list[str] | dict[str, str],
        path: str,
        count: int = 10,
        batch_size: int | None = None,
        encoding: str = "utf-8",
        compress: bool | None = None,
    ) -> int:
        """Stream fake data directly to a JSON Lines file.

        Memory-efficient: writes in batches without materializing
        all rows in memory.

        Parameters
        ----------
        fields : list[str] | dict[str, str]
            Fields to generate.
        path : str
            File path to write.
        count : int
            Number of rows.
        batch_size : int | None
            Rows per batch.  Auto-tuned when ``None``.
        encoding : str
            Character encoding (default: utf-8).
        compress : bool | None
            If ``True``, gzip the output.  ``None`` auto-detects
            from a ``.gz`` file extension.

        Returns
        -------
        int
            Number of rows written.
        """
        return self.schema(fields).stream_to_jsonl(
            path=path,
            count=count,
            batch_size=batch_size,
            encoding=encoding,
            compress=compress,
        )

    def to_arrow(
        self,
        fields: list[str] | dict[str, str],
        count: int = 10,
        batch_size: int | None = None,
    ) -> "Any":
        """Generate fake data as a PyArrow Table.

        Delegates to :meth:`Schema.to_arrow` for zero-duplication.
        Requires ``pyarrow`` to be installed.

        Parameters
        ----------
        fields : list[str] | dict[str, str]
            Fields to generate (same format as :meth:`to_dict`).
        count : int
            Number of rows.
        batch_size : int | None
            Rows per internal batch.  Auto-tuned when ``None``.

        Returns
        -------
        pyarrow.Table
        """
        return self.schema(fields).to_arrow(count=count, batch_size=batch_size)

    def to_polars(
        self,
        fields: list[str] | dict[str, str],
        count: int = 10,
        batch_size: int | None = None,
    ) -> "Any":
        """Generate fake data as a Polars DataFrame.

        Delegates to :meth:`Schema.to_polars` for zero-duplication.
        Requires ``polars`` to be installed.

        Parameters
        ----------
        fields : list[str] | dict[str, str]
            Fields to generate (same format as :meth:`to_dict`).
        count : int
            Number of rows.
        batch_size : int | None
            Rows per internal batch.  Auto-tuned when ``None``.

        Returns
        -------
        polars.DataFrame
        """
        return self.schema(fields).to_polars(count=count, batch_size=batch_size)

    def to_parquet(
        self,
        fields: list[str] | dict[str, str],
        path: str,
        count: int = 10,
        batch_size: int | None = None,
    ) -> int:
        """Generate fake data and write as a Parquet file.

        Requires ``pyarrow`` to be installed.  Data is written in
        batched row-groups for bounded memory usage.

        Parameters
        ----------
        fields : list[str] | dict[str, str]
            Fields to generate.
        path : str
            File path to write.
        count : int
            Number of rows.
        batch_size : int | None
            Rows per row-group.  Auto-tuned when ``None``.

        Returns
        -------
        int
            Number of rows written.
        """
        return self.schema(fields).to_parquet(
            path=path, count=count, batch_size=batch_size
        )

    def __repr__(self) -> str:
        return f"DataForge(locale={self._locale!r})"

    # ------------------------------------------------------------------
    # Introspection API
    # ------------------------------------------------------------------

    @staticmethod
    def list_providers() -> list[str]:
        """Return a sorted list of all available provider names.

        Returns
        -------
        list[str]
            Provider names (e.g. ``["address", "company", "person", ...]``).
        """
        from dataforge.registry import get_provider_info

        return sorted(get_provider_info())

    @staticmethod
    def list_fields() -> dict[str, tuple[str, str]]:
        """Return all available field names with their provider/method info.

        Returns
        -------
        dict[str, tuple[str, str]]
            Mapping of ``{field_name: (provider_name, method_name)}``,
            sorted by field name.

        Examples
        --------
        >>> fields = DataForge.list_fields()
        >>> fields["first_name"]
        ('person', 'first_name')
        """
        from dataforge.registry import get_field_map

        fm = get_field_map()
        return dict(sorted(fm.items()))

    # ------------------------------------------------------------------
    # Time-series generation
    # ------------------------------------------------------------------

    def timeseries(self, **kwargs: Any) -> "Any":
        """Create a :class:`~dataforge.timeseries.TimeSeriesSchema`.

        Parameters
        ----------
        **kwargs
            All keyword arguments are forwarded to
            :class:`~dataforge.timeseries.TimeSeriesSchema`.
            Common options: ``start``, ``end``, ``interval``,
            ``trend``, ``seasonality_amplitude``, ``noise_std``,
            ``anomaly_rate``, ``spike_amplitude``.

        Returns
        -------
        TimeSeriesSchema

        Examples
        --------
        >>> forge = DataForge(seed=42)
        >>> ts = forge.timeseries(
        ...     start="2024-01-01", end="2024-01-31",
        ...     interval="1h", trend=0.01, noise_std=0.5,
        ... )
        >>> rows = ts.generate()
        """
        from dataforge.timeseries import TimeSeriesSchema

        return TimeSeriesSchema(self, **kwargs)

    # ------------------------------------------------------------------
    # Schema inference
    # ------------------------------------------------------------------

    def infer_schema(
        self,
        data: "list[dict[str, Any]]",
    ) -> "Any":
        """Infer a :class:`Schema` from sample data (list of dicts).

        Analyzes the data to detect types, semantic patterns, and
        distributions, then builds a matching Schema.

        Parameters
        ----------
        data : list[dict]
            Sample rows to analyze.

        Returns
        -------
        Schema

        Examples
        --------
        >>> forge = DataForge(seed=42)
        >>> sample = [{"name": "Alice", "email": "alice@example.com"}]
        >>> s = forge.infer_schema(sample)
        >>> rows = s.generate(count=100)
        """
        from dataforge.inference import SchemaInferrer

        inferrer = SchemaInferrer(self)
        return inferrer.from_records(data)

    def infer_schema_from_csv(
        self,
        path: str,
        max_rows: int = 1000,
    ) -> "Any":
        """Infer a :class:`Schema` from a CSV file.

        Parameters
        ----------
        path : str
            Path to the CSV file to analyze.
        max_rows : int
            Maximum rows to sample for inference.

        Returns
        -------
        Schema

        Examples
        --------
        >>> forge = DataForge(seed=42)
        >>> s = forge.infer_schema_from_csv("users.csv")
        >>> rows = s.generate(count=1000)
        """
        from dataforge.inference import SchemaInferrer

        inferrer = SchemaInferrer(self)
        return inferrer.from_csv(path, max_rows=max_rows)

    # ------------------------------------------------------------------
    # Schema factories from ORM / model introspection
    # ------------------------------------------------------------------

    def schema_from_pydantic(self, model: type) -> "Any":
        """Create a :class:`Schema` by introspecting a Pydantic model.

        Maps model field names to DataForge fields using a three-tier
        strategy:

        1. **Exact name match** — if the field name matches a registered
           DataForge field (e.g. ``first_name``, ``email``), use it.
        2. **Alias match** — common aliases like ``fname`` → ``first_name``.
        3. **Type-based fallback** — if the field type annotation is
           ``bool``, ``datetime.date``, ``uuid.UUID``, etc., map to a
           sensible generator automatically.

        Fields that cannot be mapped are silently skipped (a warning is
        emitted).

        Requires ``pydantic`` to be installed.

        Parameters
        ----------
        model : type
            A Pydantic ``BaseModel`` subclass.

        Returns
        -------
        Schema

        Examples
        --------
        >>> from pydantic import BaseModel
        >>> class User(BaseModel):
        ...     first_name: str
        ...     email: str
        ...     city: str
        >>> forge = DataForge(seed=42)
        >>> s = forge.schema_from_pydantic(User)
        >>> rows = s.generate(count=5)
        """
        from dataforge.schema import Schema

        try:
            from pydantic import BaseModel  # noqa: F811
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "pydantic is required for schema_from_pydantic(). "
                "Install it with: pip install pydantic"
            ) from exc

        if not (isinstance(model, type) and issubclass(model, BaseModel)):
            raise TypeError(f"Expected a Pydantic BaseModel subclass, got {model!r}")

        from dataforge.registry import get_field_map

        field_map = get_field_map()
        mapped: dict[str, str] = {}

        # Pydantic v2 uses model_fields; v1 used __fields__
        model_fields: dict[str, Any] = {}
        if hasattr(model, "model_fields"):
            model_fields = model.model_fields
        elif hasattr(model, "__fields__"):
            model_fields = model.__fields__
        else:
            raise TypeError(
                f"Cannot introspect fields from {model.__name__}. "
                "Ensure it is a valid Pydantic BaseModel."
            )

        import warnings

        for field_name, field_info in model_fields.items():
            # Tier 1: exact name match in registry
            if field_name in field_map:
                mapped[field_name] = field_name
                continue

            # Tier 2: alias / heuristic name match
            alias = _pydantic_heuristic(field_name)
            if alias and alias in field_map:
                mapped[field_name] = alias
                continue

            # Tier 3: type-based fallback
            annotation = getattr(field_info, "annotation", None)
            if annotation is not None:
                type_field = _type_fallback(annotation)
                if type_field and type_field in field_map:
                    mapped[field_name] = type_field
                    continue

            warnings.warn(
                f"Pydantic field '{field_name}' on {model.__name__} "
                f"could not be mapped to a DataForge field — skipping.",
                UserWarning,
                stacklevel=2,
            )

        if not mapped:
            raise ValueError(
                f"No fields on {model.__name__} could be mapped to "
                f"DataForge fields. Ensure the model uses recognisable "
                f"field names (e.g. 'first_name', 'email', 'city')."
            )

        return Schema(self, mapped)

    def schema_from_sqlalchemy(self, model: type) -> "Any":
        """Create a :class:`Schema` by introspecting a SQLAlchemy model.

        Maps column names to DataForge fields using a three-tier
        strategy:

        1. **Exact name match** — if the column name matches a registered
           DataForge field, use it.
        2. **Alias match** — common aliases like ``fname`` → ``first_name``.
        3. **Column type fallback** — if the column type is ``Boolean``,
           ``Date``, ``DateTime``, ``UUID``, ``Text``, etc., map to a
           sensible generator automatically.

        Primary key columns named ``id`` are skipped automatically.
        Columns that cannot be mapped are silently skipped (a warning
        is emitted).

        Requires ``sqlalchemy`` to be installed.

        Parameters
        ----------
        model : type
            A SQLAlchemy declarative model class (must have
            ``__table__`` attribute).

        Returns
        -------
        Schema

        Examples
        --------
        >>> from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
        >>> class Base(DeclarativeBase): pass
        >>> class User(Base):
        ...     __tablename__ = "users"
        ...     id: Mapped[int] = mapped_column(primary_key=True)
        ...     first_name: Mapped[str]
        ...     email: Mapped[str]
        >>> forge = DataForge(seed=42)
        >>> s = forge.schema_from_sqlalchemy(User)
        >>> rows = s.generate(count=5)
        """
        from dataforge.schema import Schema

        try:
            import sqlalchemy  # noqa: F401
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "sqlalchemy is required for schema_from_sqlalchemy(). "
                "Install it with: pip install sqlalchemy"
            ) from exc

        if not hasattr(model, "__table__"):
            raise TypeError(
                f"Expected a SQLAlchemy declarative model with __table__, got {model!r}"
            )

        from dataforge.registry import get_field_map

        field_map = get_field_map()
        mapped: dict[str, str] = {}

        import warnings

        table = model.__table__
        for column in table.columns:
            col_name = column.name
            # Skip primary key 'id' columns — not fake-able
            if col_name == "id" and column.primary_key:
                continue

            # Tier 1: exact name match in registry
            if col_name in field_map:
                mapped[col_name] = col_name
                continue

            # Tier 2: alias / heuristic name match
            alias = _sqlalchemy_heuristic(col_name, column)
            if alias and alias in field_map:
                mapped[col_name] = alias
                continue

            # Tier 3: column type fallback
            type_field = _sqlalchemy_type_fallback(column)
            if type_field and type_field in field_map:
                mapped[col_name] = type_field
                continue

            warnings.warn(
                f"SQLAlchemy column '{col_name}' on "
                f"{model.__name__} could not be mapped to a "
                f"DataForge field — skipping.",
                UserWarning,
                stacklevel=2,
            )

        if not mapped:
            raise ValueError(
                f"No columns on {model.__name__} could be mapped to "
                f"DataForge fields. Ensure the model uses recognisable "
                f"column names (e.g. 'first_name', 'email', 'city')."
            )

        return Schema(self, mapped)
