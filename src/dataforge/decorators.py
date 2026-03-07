"""Provider decorators — simplified API for creating custom providers.

The ``@provider`` decorator lets users create lightweight providers
with minimal boilerplate.  Instead of subclassing ``BaseProvider`` and
defining ``_provider_name``, ``_field_map``, ``_locale_modules``,
``__slots__``, and ``@overload`` signatures manually, a single
decorator transforms a plain class into a fully registered provider.

Usage::

    from dataforge.decorators import provider

    @provider("greeting")
    class GreetingProvider:
        def hello(self) -> str:
            return "Hello, world!"

        def goodbye(self) -> str:
            return "Goodbye!"

    # Now usable:
    forge = DataForge()
    forge.register_provider(GreetingProvider)
    forge.greeting.hello()            # "Hello, world!"
    forge.greeting.hello(count=5)     # ["Hello, world!"] * 5

Each public method (no leading ``_``) is auto-wrapped to support
the ``count=1`` → scalar, ``count>1`` → list convention used by
all DataForge providers.  A ``_field_map`` is auto-generated from
all public methods.

Advanced usage::

    @provider(
        "weather_custom",
        field_map={"temp": "temperature", "wind": "wind_speed"},
        locale_modules=("weather",),
    )
    class WeatherCustom:
        def temperature(self) -> str:
            return "22°C"

        def wind_speed(self) -> str:
            return "15 km/h"
"""

from __future__ import annotations

import functools
from typing import Any

from dataforge.backend import RandomEngine
from dataforge.providers.base import BaseProvider


def provider(
    name: str,
    *,
    field_map: dict[str, str] | None = None,
    locale_modules: tuple[str, ...] = (),
    needs_forge: bool = False,
) -> Any:
    """Class decorator that transforms a plain class into a DataForge provider.

    Parameters
    ----------
    name : str
        The provider name (used as ``forge.<name>``).
    field_map : dict[str, str] | None
        Optional explicit field map.  When ``None``, a field map is
        auto-generated from all public methods (method_name → method_name).
    locale_modules : tuple[str, ...]
        Locale data modules required by this provider.
    needs_forge : bool
        If ``True``, the provider receives the ``DataForge`` instance
        as a second constructor argument (for cross-provider access).

    Returns
    -------
    type
        A ``BaseProvider`` subclass ready for registration.

    Examples
    --------
    >>> from dataforge.decorators import provider
    >>> @provider("greet")
    ... class GreetProvider:
    ...     def hello(self):
    ...         return "hi"
    >>> GreetProvider._provider_name
    'greet'
    >>> GreetProvider._field_map
    {'hello': 'hello'}
    """

    def decorator(cls: type) -> type:
        # Discover public methods from the user's class
        user_methods: dict[str, Any] = {}
        for attr_name in list(vars(cls)):
            if attr_name.startswith("_"):
                continue
            obj = vars(cls)[attr_name]
            if callable(obj):
                user_methods[attr_name] = obj

        # Build field_map if not explicitly provided
        fm = field_map if field_map is not None else {m: m for m in user_methods}

        # Build the new class with BaseProvider as base
        # We need to create wrapped methods that support count=1 / count>N
        namespace: dict[str, Any] = {
            "__slots__": (),
            "_provider_name": name,
            "_locale_modules": locale_modules,
            "_field_map": fm,
            "_needs_forge": needs_forge,
        }

        # Create __init__ that handles locale and forge arguments
        if needs_forge:

            def __init__(self: Any, engine: RandomEngine, forge: Any) -> None:
                BaseProvider.__init__(self, engine)
                self._forge = forge

            namespace["__slots__"] = ("_forge",)
        elif locale_modules:
            # Dynamic __init__ that accepts locale module arguments
            def __init__(self: Any, engine: RandomEngine, *locale_args: Any) -> None:  # type: ignore[misc]
                BaseProvider.__init__(self, engine)
                for i, mod_name in enumerate(locale_modules):
                    if i < len(locale_args):
                        object.__setattr__(self, f"_locale_{mod_name}", locale_args[i])

            slot_names = tuple(f"_locale_{m}" for m in locale_modules)
            namespace["__slots__"] = slot_names

        # Wrap each user method to support count parameter
        for method_name, method_func in user_methods.items():
            wrapped = _wrap_with_count(method_func)
            namespace[method_name] = wrapped

        # Create the provider class
        new_cls = type(cls.__name__, (BaseProvider,), namespace)

        # Preserve the original class's module and qualname for debugging
        new_cls.__module__ = cls.__module__
        new_cls.__qualname__ = cls.__qualname__

        return new_cls

    return decorator


def _wrap_with_count(func: Any) -> Any:
    """Wrap a scalar-returning method to support the ``count`` parameter.

    The wrapped method calls the original function once when
    ``count == 1`` (returning a scalar) and ``count`` times when
    ``count > 1`` (returning a list).

    The original function should accept ``self`` as its first argument
    and return a single value.
    """

    @functools.wraps(func)
    def wrapper(self: Any, count: int = 1) -> Any:
        if count == 1:
            return func(self)
        return [func(self) for _ in range(count)]

    return wrapper
