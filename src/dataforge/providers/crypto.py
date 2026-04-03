"""CryptoProvider — generates fake cryptographic hash strings.

All methods use ``getrandbits()`` + hex formatting for maximum speed.
No ``hashlib`` import is needed — these are random hex strings with
correct lengths, not actual digests.
"""

from dataforge.providers.base import BaseProvider


class CryptoProvider(BaseProvider):
    """Generates random strings matching common hash digest formats."""

    __slots__ = ()

    _provider_name = "crypto"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "md5": "md5",
        "sha1": "sha1",
        "sha256": "sha256",
    }

    # Public API

    def md5(self, count: int = 1) -> str | list[str]:
        """Generate a random MD5-style hex string (32 hex chars)."""
        bits = self._engine._rng.getrandbits
        if count == 1:
            return f"{bits(128):032x}"
        return [f"{bits(128):032x}" for _ in range(count)]

    def sha1(self, count: int = 1) -> str | list[str]:
        """Generate a random SHA-1-style hex string (40 hex chars)."""
        bits = self._engine._rng.getrandbits
        if count == 1:
            return f"{bits(160):040x}"
        return [f"{bits(160):040x}" for _ in range(count)]

    def sha256(self, count: int = 1) -> str | list[str]:
        """Generate a random SHA-256-style hex string (64 hex chars)."""
        bits = self._engine._rng.getrandbits
        if count == 1:
            return f"{bits(256):064x}"
        return [f"{bits(256):064x}" for _ in range(count)]
