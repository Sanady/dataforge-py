"""UniqueProxy — wrapper for unique value generation."""

from __future__ import annotations

from typing import Any

from dataforge.providers.base import BaseProvider


class _UniqueMethodWrapper:
    """Wraps a single provider method to enforce uniqueness."""

    __slots__ = ("_method", "_seen")

    def __init__(self, method: Any) -> None:
        self._method = method
        self._seen: set[Any] = set()

    def __call__(self, count: int = 1, **kwargs: Any) -> Any:
        if count == 1:
            return self._generate_one(**kwargs)
        return self._generate_batch(count, **kwargs)

    def _generate_one(self, _max_retries: int = 10_000, **kwargs: Any) -> Any:
        """Generate a single unique value with retry."""
        seen = self._seen
        method = self._method
        for _ in range(_max_retries):
            val = method(**kwargs)
            if val not in seen:
                seen.add(val)
                return val
        raise RuntimeError(
            f"Could not generate a unique value after {_max_retries} "
            f"retries for {self._method!r}. "
            f"Already generated {len(seen)} unique values."
        )

    def _generate_batch(self, count: int, **kwargs: Any) -> list[Any]:
        """Generate *count* unique values using adaptive over-sampling."""
        seen = self._seen
        method = self._method
        result: list[Any] = []
        remaining = count
        max_total_retries = count * 100

        retries = 0
        oversample_ratio = 0.20
        while remaining > 0:
            if retries > max_total_retries:
                raise RuntimeError(
                    f"Could not generate {count} unique values after "
                    f"{retries} retries for {self._method!r}. "
                    f"Generated {len(result)}/{count}."
                )
            request = remaining + max(int(remaining * oversample_ratio), 10)
            batch = method(count=request, **kwargs)
            batch_collisions = 0
            for val in batch:
                if val not in seen:
                    seen.add(val)
                    result.append(val)
                    remaining -= 1
                    if remaining == 0:
                        break
                else:
                    retries += 1
                    batch_collisions += 1

            if batch_collisions > 0 and len(batch) > 0:
                collision_rate = batch_collisions / len(batch)
                oversample_ratio = min(
                    collision_rate / (1 - collision_rate + 0.01), 10.0
                )

        return result

    def clear(self) -> None:
        """Reset the seen set for this method."""
        self._seen.clear()


class _UniqueProviderProxy:
    """Proxy around a provider that wraps every method for uniqueness."""

    __slots__ = ("_provider", "_wrappers")

    def __init__(self, provider: BaseProvider) -> None:
        self._provider = provider
        self._wrappers: dict[str, _UniqueMethodWrapper] = {}

    def __getattr__(self, name: str) -> Any:
        wrapper = self._wrappers.get(name)
        if wrapper is not None:
            return wrapper
        method = getattr(self._provider, name)
        if not callable(method):
            return method
        wrapper = _UniqueMethodWrapper(method)
        self._wrappers[name] = wrapper
        return wrapper

    def clear(self) -> None:
        """Clear all tracked unique values for this provider."""
        for wrapper in self._wrappers.values():
            wrapper.clear()


class UniqueProxy:
    """Top-level unique proxy — accessed via ``forge.unique``."""

    __slots__ = ("_forge", "_proxies")

    def __init__(self, forge: Any) -> None:
        self._forge = forge
        self._proxies: dict[str, _UniqueProviderProxy] = {}

    def __getattr__(self, name: str) -> Any:
        proxy = self._proxies.get(name)
        if proxy is not None:
            return proxy
        provider = getattr(self._forge, name)
        if isinstance(provider, BaseProvider):
            proxy = _UniqueProviderProxy(provider)
            self._proxies[name] = proxy
            return proxy
        return provider

    def clear(self, provider_name: str | None = None) -> None:
        """Clear tracked unique values."""
        if provider_name is not None:
            proxy = self._proxies.get(provider_name)
            if proxy is not None:
                proxy.clear()
        else:
            for proxy in self._proxies.values():
                proxy.clear()
