"""Provider registry — auto-discovery and field resolution for providers.

The registry scans all ``BaseProvider`` subclasses that define
``_provider_name`` and builds two lookup tables:

- **provider_info**: maps provider name → ``(provider_class, locale_modules)``
- **field_map**: maps shorthand field name → ``(provider_name, method_name)``

These tables are built **lazily** on first access so that import-time
cost is zero.  Once built, lookups are O(1) dict reads.

**Built-in providers** are loaded via direct imports (fastest path).
**External plugins** are discovered through the
``dataforge.providers`` entry-point group, allowing third-party
packages to register providers with zero configuration.

Adding a built-in provider requires only:

1. Create a new ``BaseProvider`` subclass with ``_provider_name``,
   ``_locale_modules``, and ``_field_map`` class attributes.
2. Add a direct import below in :func:`_ensure_loaded`.
3. Add an entry point in ``pyproject.toml`` under
   ``[project.entry-points."dataforge.providers"]``.
"""

import importlib
import importlib.metadata

from dataforge.providers.base import BaseProvider

# Lazy-initialized lookup tables
_provider_info: dict[str, tuple[type, tuple[str, ...]]] | None = None
_field_map: dict[str, tuple[str, str]] | None = None


def _ensure_loaded() -> None:
    """Import all provider modules and build the registry tables.

    Called lazily on first access.  Subsequent calls are no-ops.

    Built-in providers are loaded via direct imports for speed.
    External plugins are discovered through the
    ``dataforge.providers`` entry-point group.
    """
    global _provider_info, _field_map
    if _provider_info is not None:
        return

    # ------------------------------------------------------------------
    # 1. Import all built-in provider modules (fast path).
    # ------------------------------------------------------------------
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
    import dataforge.providers.ai_chat  # noqa: F401
    import dataforge.providers.social_media  # noqa: F401
    import dataforge.providers.music  # noqa: F401
    import dataforge.providers.sports  # noqa: F401
    import dataforge.providers.food  # noqa: F401
    import dataforge.providers.legal  # noqa: F401
    import dataforge.providers.real_estate  # noqa: F401
    import dataforge.providers.weather  # noqa: F401
    import dataforge.providers.hardware  # noqa: F401
    import dataforge.providers.logistics  # noqa: F401

    # ------------------------------------------------------------------
    # 2. Discover external plugins via entry points.
    # ------------------------------------------------------------------
    eps = importlib.metadata.entry_points(group="dataforge.providers")
    for ep in eps:
        # Each entry point value is a module path.
        # Loading it triggers the class registration via __subclasses__.
        try:
            importlib.import_module(ep.value)
        except Exception:
            # Don't let a broken plugin prevent the registry from loading.
            import warnings

            warnings.warn(
                f"Failed to load dataforge provider plugin '{ep.name}': "
                f"could not import '{ep.value}'",
                RuntimeWarning,
                stacklevel=2,
            )

    # ------------------------------------------------------------------
    # 3. Build lookup tables from all registered subclasses.
    # ------------------------------------------------------------------
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
    """Return the provider info table: ``{name: (class, locale_modules)}``."""
    _ensure_loaded()
    if _provider_info is None:
        raise RuntimeError("Provider registry failed to initialize.")
    return _provider_info


def get_field_map() -> dict[str, tuple[str, str]]:
    """Return the field map: ``{field_name: (provider_name, method_name)}``."""
    _ensure_loaded()
    if _field_map is None:
        raise RuntimeError("Provider registry failed to initialize.")
    return _field_map


def register_runtime_provider(
    name: str,
    cls: type,
    locale_modules: tuple[str, ...] = (),
) -> None:
    """Register a provider at runtime (called by DataForge.register_provider).

    Updates the global registry tables so that Schema and field
    resolution can find the new provider's fields.

    Parameters
    ----------
    name : str
        The provider name (e.g. ``"weather"``).
    cls : type
        The provider class.
    locale_modules : tuple[str, ...]
        Locale modules required by the provider.
    """
    _ensure_loaded()
    if _provider_info is None or _field_map is None:
        raise RuntimeError("Provider registry failed to initialize.")

    _provider_info[name] = (cls, locale_modules)

    fm = getattr(cls, "_field_map", {})
    for field_name, method_name in fm.items():
        _field_map[field_name] = (name, method_name)
