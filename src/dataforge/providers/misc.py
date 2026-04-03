"""MiscProvider — utility generators for common testing needs."""

import time as _time
from typing import Any

from dataforge.providers.base import BaseProvider

# Pre-computed bit-mask constants for UUID version/variant setting.
# Combined masks reduce 4 bitmask ops to 2 per UUID generation.
_UUID4_CLEAR = ~(0xF << 76) & ~(0x3 << 62)  # clear version + variant in one AND
_UUID4_SET = (0x4 << 76) | (0x2 << 62)  # set version 4 + variant 1 in one OR


class MiscProvider(BaseProvider):
    """Generates UUIDs, booleans, and utility random selections."""

    __slots__ = ()

    _provider_name = "misc"
    _locale_modules = ()
    _field_map = {
        "uuid4": "uuid4",
        "uuid": "uuid4",
        "uuid7": "uuid7",
        "boolean": "boolean",
    }

    # Scalar helpers

    def _one_uuid4(self) -> str:
        # 128 random bits → set version 4 and variant 1 with 2 ops
        n = (self._engine._rng.getrandbits(128) & _UUID4_CLEAR) | _UUID4_SET
        h = f"{n:032x}"
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"

    def _one_uuid7(self) -> str:
        # 48-bit millisecond Unix timestamp
        ts_ms = int(_time.time() * 1000) & 0xFFFF_FFFF_FFFF
        # 74 random bits (12 + 62) from the seeded RNG
        rand_bits = self._engine._rng.getrandbits(74)
        rand_a = (rand_bits >> 62) & 0xFFF  # 12 bits
        rand_b = rand_bits & 0x3FFF_FFFF_FFFF_FFFF  # 62 bits
        # Assemble 128-bit UUID7 as a single int
        n = (ts_ms << 80) | (0x7 << 76) | (rand_a << 64) | (0x2 << 62) | rand_b
        h = f"{n:032x}"
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"

    # Public API

    def uuid4(self, count: int = 1) -> str | list[str]:
        """Generate a random UUID4 string."""
        if count == 1:
            return self._one_uuid4()
        rng_bits = self._engine._rng.getrandbits
        result: list[str] = []
        _clr = _UUID4_CLEAR
        _set = _UUID4_SET
        for _ in range(count):
            n = (rng_bits(128) & _clr) | _set
            h = f"{n:032x}"
            result.append(f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}")
        return result

    def uuid7(self, count: int = 1) -> str | list[str]:
        """Generate a random UUID7 string (time-ordered, monotonic)."""
        if count == 1:
            return self._one_uuid7()
        rng_bits = self._engine._rng.getrandbits
        _time_time = _time.time
        result: list[str] = []
        for _ in range(count):
            ts_ms = int(_time_time() * 1000) & 0xFFFF_FFFF_FFFF
            rand_bits = rng_bits(74)
            rand_a = (rand_bits >> 62) & 0xFFF
            rand_b = rand_bits & 0x3FFF_FFFF_FFFF_FFFF
            n = (ts_ms << 80) | (0x7 << 76) | (rand_a << 64) | (0x2 << 62) | rand_b
            h = f"{n:032x}"
            result.append(f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}")
        return result

    def boolean(self, count: int = 1, probability: float = 0.5) -> bool | list[bool]:
        """Generate a random boolean."""
        rng = self._engine._rng
        if count == 1:
            return rng.random() < probability
        return [rng.random() < probability for _ in range(count)]

    def random_element(
        self, elements: tuple[Any, ...] | list[Any], count: int = 1
    ) -> Any:
        """Pick random element(s) from a user-provided collection."""
        data = tuple(elements) if isinstance(elements, list) else elements
        if count == 1:
            return self._engine.choice(data)
        return self._engine.choices(data, count)

    def null_or(self, value: Any, probability: float = 0.1) -> Any:
        """Return ``None`` with *probability*, otherwise return *value*."""
        if self._engine._rng.random() < probability:
            return None
        return value
