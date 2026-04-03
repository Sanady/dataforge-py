"""DataForge — the main entry point for fake data generation.

Usage::

    from dataforge import DataForge

    forge = DataForge(locale="en_US", seed=42)

    forge.person.first_name()           # "James"
    forge.person.full_name(count=1000)  # list of 1000 full names
    forge.address.full_address()        # "4821 Oak Ave, Chicago, IL 60614"
    forge.internet.email()              # "james.smith@gmail.com"
"""

import importlib
from typing import Any
from types import ModuleType

from dataforge.backend import RandomEngine
from dataforge.providers.base import BaseProvider

# Heuristic field-name mappings for ORM / model introspection

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

# Type-based fallback mappings for ORM / model introspection

_TYPE_FALLBACK_BUILTINS: dict[type, str] = {
    str: "misc.random_element",  # sentinel — too ambiguous
    int: "misc.random_element",  # sentinel — handled specially
    float: "misc.random_element",  # sentinel — handled specially
    bool: "boolean",
}

_TYPE_FALLBACK_NAMES: dict[str, str] = {
    "date": "date",
    "datetime": "datetime",
    "time": "time",
    "Decimal": "misc.random_element",  # sentinel
    "UUID": "uuid4",
}


def _resolve_type_annotation(annotation: Any) -> type | None:
    """Extract the concrete type from a possibly-wrapped annotation."""
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
    """Map a Python type annotation to a DataForge field name, or None."""
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
    """Map a SQLAlchemy column name to a DataForge field name (or None)."""
    alias = _FIELD_ALIASES.get(col_name)
    if alias:
        return alias
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
    """Map a SQLAlchemy column type to a DataForge field name."""
    col_type = type(column.type)
    type_name = col_type.__name__
    return _SA_TYPE_MAP.get(type_name)


class DataForge:
    """High-performance fake data generator with lazy provider loading."""

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

    # Dynamic provider access via registry

    def _get_provider(self, name: str) -> BaseProvider:
        """Lazily instantiate and cache a provider by registry name."""
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

    # Unique value generation

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

    # Provider registration

    def register_provider(
        self,
        provider_cls: type[BaseProvider],
        name: str | None = None,
    ) -> None:
        """Register a custom provider class at runtime.

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
        """Dynamic attribute access for registered providers."""
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

    # Seed control

    def seed(self, value: int) -> None:
        """Re-seed the random engine for reproducible output."""
        self._engine.seed(value)

    def copy(self, seed: int | None = None) -> "DataForge":
        """Create a new DataForge instance with the same locale."""
        return DataForge(locale=self._locale, seed=seed)

    # Schema API

    def schema(
        self,
        fields: "list[str] | dict[str, Any]",
        null_fields: "dict[str, float] | None" = None,
        unique_together: "list[tuple[str, ...]] | None" = None,
        chaos: "Any | None" = None,
    ) -> "Any":
        """Create a pre-resolved Schema for maximum throughput."""
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
        """Create a RelationalSchema for multi-table generation with referential integrity."""
        from dataforge.relational import RelationalSchema

        return RelationalSchema(self, tables)

    def schema_from_dict(
        self,
        d: dict[str, Any],
    ) -> "Any":
        """Create a Schema from a schema definition dict."""
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
        """Create a Schema by loading a JSON/YAML/TOML definition file."""
        from dataforge.schema_io import load_schema

        d = load_schema(path, format=format)
        return self.schema_from_dict(d)

    # Locale management

    @property
    def locale(self) -> str:
        """The currently active locale string (e.g. ``"en_US"``)."""
        return self._locale

    # Internal helpers

    def _load_locale_module(self, module_name: str) -> ModuleType:
        """Dynamically import and cache a locale data module."""
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
        """Resolve a field name to (provider_attr, method_name)."""
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

    # Bulk data generation

    def to_dict(
        self,
        fields: list[str] | dict[str, str],
        count: int = 10,
    ) -> list[dict[str, Any]]:
        """Generate *count* rows of fake data as a list of dicts."""
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
        """Generate fake data as JSON. Delegates to Schema.to_json."""
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
        """Generate fake data as CSV. Delegates to Schema.to_csv."""
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
        """Generate fake data as JSON Lines. Delegates to Schema.to_jsonl."""
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
        """Generate fake data as SQL INSERT statements. Delegates to Schema.to_sql."""
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
        """Generate fake data as a pandas DataFrame. Requires pandas."""
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
        """Stream fake data to a CSV file in batches. Returns rows written."""
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
        """Stream fake data to a JSON Lines file in batches. Returns rows written."""
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
        """Generate fake data as a PyArrow Table. Requires pyarrow."""
        return self.schema(fields).to_arrow(count=count, batch_size=batch_size)

    def to_polars(
        self,
        fields: list[str] | dict[str, str],
        count: int = 10,
        batch_size: int | None = None,
    ) -> "Any":
        """Generate fake data as a Polars DataFrame. Requires polars."""
        return self.schema(fields).to_polars(count=count, batch_size=batch_size)

    def to_parquet(
        self,
        fields: list[str] | dict[str, str],
        path: str,
        count: int = 10,
        batch_size: int | None = None,
    ) -> int:
        """Generate fake data and write as Parquet. Requires pyarrow. Returns rows written."""
        return self.schema(fields).to_parquet(
            path=path, count=count, batch_size=batch_size
        )

    def __repr__(self) -> str:
        return f"DataForge(locale={self._locale!r})"

    # Introspection API

    @staticmethod
    def list_providers() -> list[str]:
        """Return a sorted list of all available provider names."""
        from dataforge.registry import get_provider_info

        return sorted(get_provider_info())

    @staticmethod
    def list_fields() -> dict[str, tuple[str, str]]:
        """Return all available field names with their provider/method info."""
        from dataforge.registry import get_field_map

        fm = get_field_map()
        return dict(sorted(fm.items()))

    # Time-series generation

    def timeseries(self, **kwargs: Any) -> "Any":
        """Create a TimeSeriesSchema. Kwargs forwarded to TimeSeriesSchema."""
        from dataforge.timeseries import TimeSeriesSchema

        return TimeSeriesSchema(self, **kwargs)

    # Schema inference

    def infer_schema(
        self,
        data: "list[dict[str, Any]]",
    ) -> "Any":
        """Infer a Schema from sample data (list of dicts)."""
        from dataforge.inference import SchemaInferrer

        inferrer = SchemaInferrer(self)
        return inferrer.from_records(data)

    def infer_schema_from_csv(
        self,
        path: str,
        max_rows: int = 1000,
    ) -> "Any":
        """Infer a Schema from a CSV file."""
        from dataforge.inference import SchemaInferrer

        inferrer = SchemaInferrer(self)
        return inferrer.from_csv(path, max_rows=max_rows)

    # Schema factories from ORM / model introspection

    def schema_from_pydantic(self, model: type) -> "Any":
        """Create a Schema by introspecting a Pydantic BaseModel subclass."""
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
        """Create a Schema by introspecting a SQLAlchemy declarative model."""
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
