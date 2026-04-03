"""Provider decorators — simplified API for creating custom providers."""

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
    """Class decorator that transforms a plain class into a DataForge provider."""

    def decorator(cls: type) -> type:
        user_methods: dict[str, Any] = {}
        for attr_name in list(vars(cls)):
            if attr_name.startswith("_"):
                continue
            obj = vars(cls)[attr_name]
            if callable(obj):
                user_methods[attr_name] = obj

        fm = field_map if field_map is not None else {m: m for m in user_methods}

        namespace: dict[str, Any] = {
            "__slots__": (),
            "_provider_name": name,
            "_locale_modules": locale_modules,
            "_field_map": fm,
            "_needs_forge": needs_forge,
        }

        if needs_forge:

            def __init__(self: Any, engine: RandomEngine, forge: Any) -> None:
                BaseProvider.__init__(self, engine)
                self._forge = forge

            namespace["__slots__"] = ("_forge",)
        elif locale_modules:

            def __init__(self: Any, engine: RandomEngine, *locale_args: Any) -> None:  # type: ignore[misc]
                BaseProvider.__init__(self, engine)
                for i, mod_name in enumerate(locale_modules):
                    if i < len(locale_args):
                        object.__setattr__(self, f"_locale_{mod_name}", locale_args[i])

            slot_names = tuple(f"_locale_{m}" for m in locale_modules)
            namespace["__slots__"] = slot_names

        for method_name, method_func in user_methods.items():
            wrapped = _wrap_with_count(method_func)
            namespace[method_name] = wrapped

        new_cls = type(cls.__name__, (BaseProvider,), namespace)

        new_cls.__module__ = cls.__module__
        new_cls.__qualname__ = cls.__qualname__

        return new_cls

    return decorator


def _wrap_with_count(func: Any) -> Any:
    """Wrap a scalar-returning method to support the ``count`` parameter."""

    @functools.wraps(func)
    def wrapper(self: Any, count: int = 1) -> Any:
        if count == 1:
            return func(self)
        return [func(self) for _ in range(count)]

    return wrapper
