"""Provider registry — auto-discovery and field resolution for providers."""

import importlib
import importlib.metadata

from dataforge.providers.base import BaseProvider

_provider_info: dict[str, tuple[type, tuple[str, ...]]] | None = None
_field_map: dict[str, tuple[str, str]] | None = None


def _ensure_loaded() -> None:
    """Import all provider modules and build the registry tables."""
    global _provider_info, _field_map
    if _provider_info is not None:
        return

    import dataforge.providers.address  # noqa: F401
    import dataforge.providers.automotive  # noqa: F401
    import dataforge.providers.barcode  # noqa: F401
    import dataforge.providers.color  # noqa: F401
    import dataforge.providers.company  # noqa: F401
    import dataforge.providers.crypto  # noqa: F401
    import dataforge.providers.datetime  # noqa: F401
    import dataforge.providers.ecommerce  # noqa: F401
    import dataforge.providers.education  # noqa: F401
    import dataforge.providers.file  # noqa: F401
    import dataforge.providers.finance  # noqa: F401
    import dataforge.providers.geo  # noqa: F401
    import dataforge.providers.government  # noqa: F401
    import dataforge.providers.internet  # noqa: F401
    import dataforge.providers.lorem  # noqa: F401
    import dataforge.providers.medical  # noqa: F401
    import dataforge.providers.misc  # noqa: F401
    import dataforge.providers.network  # noqa: F401
    import dataforge.providers.payment  # noqa: F401
    import dataforge.providers.person  # noqa: F401
    import dataforge.providers.phone  # noqa: F401
    import dataforge.providers.profile  # noqa: F401
    import dataforge.providers.science  # noqa: F401
    import dataforge.providers.text  # noqa: F401
    import dataforge.providers.ai_prompt  # noqa: F401
    import dataforge.providers.llm  # noqa: F401
    import dataforge.providers.social_media  # noqa: F401
    import dataforge.providers.music  # noqa: F401
    import dataforge.providers.sports  # noqa: F401
    import dataforge.providers.food  # noqa: F401
    import dataforge.providers.legal  # noqa: F401
    import dataforge.providers.real_estate  # noqa: F401
    import dataforge.providers.weather  # noqa: F401
    import dataforge.providers.hardware  # noqa: F401
    import dataforge.providers.logistics  # noqa: F401

    eps = importlib.metadata.entry_points(group="dataforge.providers")
    for ep in eps:
        try:
            importlib.import_module(ep.value)
        except Exception:
            import warnings

            warnings.warn(
                f"Failed to load dataforge provider plugin '{ep.name}': "
                f"could not import '{ep.value}'",
                RuntimeWarning,
                stacklevel=2,
            )

    info: dict[str, tuple[type, tuple[str, ...]]] = {}
    fields: dict[str, tuple[str, str]] = {}

    for cls in BaseProvider.__subclasses__():
        name = getattr(cls, "_provider_name", "")
        if not name:
            continue
        locale_mods = getattr(cls, "_locale_modules", ())
        info[name] = (cls, locale_mods)

        fm = getattr(cls, "_field_map", {})
        for field_name, method_name in fm.items():
            fields[field_name] = (name, method_name)

    _provider_info = info
    _field_map = fields


def get_provider_info() -> dict[str, tuple[type, tuple[str, ...]]]:
    """Return the provider info table."""
    _ensure_loaded()
    if _provider_info is None:
        raise RuntimeError("Provider registry failed to initialize.")
    return _provider_info


def get_field_map() -> dict[str, tuple[str, str]]:
    """Return the field map."""
    _ensure_loaded()
    if _field_map is None:
        raise RuntimeError("Provider registry failed to initialize.")
    return _field_map


def register_runtime_provider(
    name: str,
    cls: type,
    locale_modules: tuple[str, ...] = (),
) -> None:
    """Register a provider at runtime (called by DataForge.register_provider)."""
    _ensure_loaded()
    if _provider_info is None or _field_map is None:
        raise RuntimeError("Provider registry failed to initialize.")

    _provider_info[name] = (cls, locale_modules)

    fm = getattr(cls, "_field_map", {})
    for field_name, method_name in fm.items():
        _field_map[field_name] = (name, method_name)
