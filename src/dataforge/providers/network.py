"""NetworkProvider — generates fake network-related data.

All data is locale-independent and stored as module-level constants
for maximum performance (bytecode constants, zero import overhead).
"""

from dataforge.providers.base import BaseProvider

# User-Agent templates — realistic browser strings
_USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
)

_HTTP_METHODS: tuple[str, ...] = (
    "GET",
    "POST",
    "PUT",
    "DELETE",
    "PATCH",
    "HEAD",
    "OPTIONS",
)

_HTTP_STATUS_CODES: tuple[tuple[int, str], ...] = (
    (200, "OK"),
    (201, "Created"),
    (204, "No Content"),
    (301, "Moved Permanently"),
    (302, "Found"),
    (304, "Not Modified"),
    (400, "Bad Request"),
    (401, "Unauthorized"),
    (403, "Forbidden"),
    (404, "Not Found"),
    (405, "Method Not Allowed"),
    (408, "Request Timeout"),
    (409, "Conflict"),
    (422, "Unprocessable Entity"),
    (429, "Too Many Requests"),
    (500, "Internal Server Error"),
    (502, "Bad Gateway"),
    (503, "Service Unavailable"),
    (504, "Gateway Timeout"),
)

_HOST_PREFIXES: tuple[str, ...] = (
    "srv",
    "web",
    "app",
    "db",
    "api",
    "mail",
    "proxy",
    "node",
    "cache",
    "worker",
    "gateway",
    "lb",
)

_HOST_SUFFIXES: tuple[str, ...] = (
    ".local",
    ".internal",
    ".lan",
    ".corp",
    ".net",
    ".io",
)


class NetworkProvider(BaseProvider):
    """Generates fake network data: IPs, MACs, ports, user agents.

    This provider is locale-independent — all data is hardcoded.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "network"
    _locale_modules = ()
    _field_map = {
        "ipv6": "ipv6",
        "mac_address": "mac_address",
        "port": "port",
        "hostname": "hostname",
        "user_agent": "user_agent",
        "http_method": "http_method",
        "http_status_code": "http_status_code",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "user_agent": _USER_AGENTS,
        "http_method": _HTTP_METHODS,
    }

    # Scalar helpers

    def _one_ipv6(self) -> str:
        # Single getrandbits(128) call instead of 32 choice() calls.
        # Format 128-bit int as 32 hex chars, then insert colons.
        bits = self._engine.getrandbits(128)
        h = f"{bits:032x}"
        return f"{h[0:4]}:{h[4:8]}:{h[8:12]}:{h[12:16]}:{h[16:20]}:{h[20:24]}:{h[24:28]}:{h[28:32]}"

    def _one_mac_address(self) -> str:
        # Single getrandbits(48) call instead of 12 choice() calls.
        # Format 48-bit int as 12 hex chars, then insert colons.
        bits = self._engine.getrandbits(48)
        h = f"{bits:012x}"
        return f"{h[0:2]}:{h[2:4]}:{h[4:6]}:{h[6:8]}:{h[8:10]}:{h[10:12]}"

    def _one_hostname(self) -> str:
        prefix = self._engine.choice(_HOST_PREFIXES)
        num = self._engine.random_int(1, 99999)
        suffix = self._engine.choice(_HOST_SUFFIXES)
        return f"{prefix}-{num}{suffix}"

    # Public API

    def ipv6(self, count: int = 1) -> str | list[str]:
        """Generate a random IPv6 address."""
        if count == 1:
            return self._one_ipv6()
        # Inlined batch with local-bound getrandbits — avoids per-item
        # method call and self._engine attribute lookup overhead.
        _getrandbits = self._engine.getrandbits
        result: list[str] = []
        for _ in range(count):
            bits = _getrandbits(128)
            h = f"{bits:032x}"
            result.append(
                f"{h[0:4]}:{h[4:8]}:{h[8:12]}:{h[12:16]}:{h[16:20]}:{h[20:24]}:{h[24:28]}:{h[28:32]}"
            )
        return result

    def mac_address(self, count: int = 1) -> str | list[str]:
        """Generate a random MAC address (e.g. ``"a1:b2:c3:d4:e5:f6"``)."""
        if count == 1:
            return self._one_mac_address()
        # Inlined batch with local-bound getrandbits
        _getrandbits = self._engine.getrandbits
        result: list[str] = []
        for _ in range(count):
            bits = _getrandbits(48)
            h = f"{bits:012x}"
            result.append(f"{h[0:2]}:{h[2:4]}:{h[4:6]}:{h[6:8]}:{h[8:10]}:{h[10:12]}")
        return result

    def port(self, count: int = 1) -> int | list[int]:
        """Generate a random port number (1–65535)."""
        if count == 1:
            return self._engine.random_int(1, 65535)
        return [self._engine.random_int(1, 65535) for _ in range(count)]

    def hostname(self, count: int = 1) -> str | list[str]:
        """Generate a random hostname (e.g. ``"srv-48201.local"``)."""
        if count == 1:
            return self._one_hostname()
        # Inlined batch loop with local-bound choices
        _choice = self._engine.choice
        _ri = self._engine.random_int
        return [
            f"{_choice(_HOST_PREFIXES)}-{_ri(1, 99999)}{_choice(_HOST_SUFFIXES)}"
            for _ in range(count)
        ]

    def http_status_code(self, count: int = 1) -> str | list[str]:
        """Generate a random HTTP status code with reason (e.g. ``"404 Not Found"``)."""
        if count == 1:
            code, reason = self._engine.choice(_HTTP_STATUS_CODES)
            return f"{code} {reason}"
        results = self._engine.choices(_HTTP_STATUS_CODES, count)
        return [f"{code} {reason}" for code, reason in results]
