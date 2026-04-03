"""Base provider — shared foundation for all data providers."""

from dataforge.backend import RandomEngine


def _make_choice_method(data: tuple[str, ...]):
    """Create a choice method that picks from *data*.

    Returns a function with the standard ``(self, count=1)`` signature.
    Uses the same ``engine.choice`` / ``engine.choices`` hot-path as
    hand-written methods so performance is identical.
    """

    def method(self, count: int = 1):
        if count == 1:
            return self._engine.choice(data)
        return self._engine.choices(data, count)

    return method


class BaseProvider:
    """Abstract base for all dataforge providers.

    Subclasses should define class-level metadata for the provider
    registry:

    - ``_provider_name``: short name used as attribute on ``DataForge``
      (e.g. ``"person"``, ``"address"``).
    - ``_locale_modules``: tuple of locale module names needed to
      construct this provider (e.g. ``("person",)``).  Empty tuple
      ``()`` for locale-independent providers.
    - ``_field_map``: dict mapping shorthand field names to method
      names (e.g. ``{"first_name": "first_name", "name": "full_name"}``).
    - ``_choice_fields``: dict mapping method names to data tuples.
      Methods listed here are **auto-generated** at class-creation
      time — no hand-written boilerplate required.
    """

    __slots__ = ("_engine",)

    # Registry metadata — subclasses override these
    _provider_name: str = ""
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {}
    _needs_forge: bool = False

    # Declarative choice fields — auto-generated as methods
    _choice_fields: dict[str, tuple[str, ...]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Auto-generate simple choice methods from _choice_fields
        for name, data in cls._choice_fields.items():
            if name not in cls.__dict__:  # don't override explicit methods
                setattr(cls, name, _make_choice_method(data))

    def __init__(self, engine: RandomEngine) -> None:
        self._engine = engine
