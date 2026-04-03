"""Internet provider — generates fake emails, usernames, domains, URLs, IPs."""

import re
import unicodedata
from types import ModuleType

from dataforge.backend import RandomEngine
from dataforge.providers.base import BaseProvider

# Module-level constants for maximum performance
_SLUG_WORDS: tuple[str, ...] = (
    "fast",
    "smart",
    "cool",
    "free",
    "open",
    "next",
    "top",
    "best",
    "big",
    "new",
    "red",
    "blue",
    "green",
    "dark",
    "light",
)

_URL_PROTOCOLS: tuple[str, ...] = ("https", "http")

_SAFE_DOMAINS: tuple[str, ...] = (
    "example.com",
    "example.org",
    "example.net",
)

# Pre-compiled regex for non-alphanumeric stripping
_NON_ALNUM_RE: re.Pattern[str] = re.compile(r"[^a-z0-9]")


def _ascii_safe(text: str) -> str:
    """Transliterate unicode to ASCII-safe lowercase slug.

    Handles accented characters (e.g. 'e' -> 'e') and CJK characters
    (dropped if no transliteration is possible).
    """
    nfkd = unicodedata.normalize("NFKD", text)
    ascii_chars = [
        ch for ch in nfkd if unicodedata.category(ch) != "Mn" and ch.isascii()
    ]
    result = "".join(ascii_chars).lower().strip()
    result = _NON_ALNUM_RE.sub("", result)
    return result or "user"


def _precompute_ascii_names(names: tuple[str, ...]) -> tuple[str, ...]:
    """Pre-compute ASCII-safe versions of all names at init time.

    This eliminates per-call ``unicodedata.normalize`` and ``re.sub``
    overhead during generation — the expensive work is done once.
    """
    seen: dict[str, None] = {}
    for name in names:
        safe = _ascii_safe(name)
        if safe not in seen:
            seen[safe] = None
    return tuple(seen)


class InternetProvider(BaseProvider):
    """Generates fake emails, usernames, domains, URLs, and IP addresses.

    Pre-computes ASCII-safe name lookups at init time so that
    ``email()`` / ``username()`` / ``domain()`` calls avoid all
    ``unicodedata`` and ``re`` overhead during generation.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    locale_data : ModuleType
        The imported locale module (e.g. ``dataforge.locales.en_US.internet``).
    person_data : ModuleType
        The person locale module — needed for generating realistic
        usernames and email addresses from first/last names.
    """

    __slots__ = (
        "_free_email_domains",
        "_domain_suffixes",
        "_user_formats",
        "_ascii_first_names",
        "_ascii_last_names",
    )

    _provider_name = "internet"
    _locale_modules = ("internet", "person")
    _field_map = {
        "email": "email",
        "username": "username",
        "domain": "domain",
        "url": "url",
        "ipv4": "ipv4",
        "slug": "slug",
        "tld": "tld",
        "safe_email": "safe_email",
    }

    def __init__(
        self,
        engine: RandomEngine,
        locale_data: ModuleType,
        person_data: ModuleType,
    ) -> None:
        super().__init__(engine)
        self._free_email_domains: tuple[str, ...] = locale_data.free_email_domains
        self._domain_suffixes: tuple[str, ...] = locale_data.domain_suffixes
        # Pre-process user_formats: convert "##" to "{num}" so that
        # _one_username() can use a single str.format_map() call instead
        # of 3× str.replace() per invocation.
        self._user_formats: tuple[str, ...] = tuple(
            fmt.replace("##", "{num}") for fmt in locale_data.user_formats
        )
        # Pre-compute ASCII-safe names once — all subsequent calls use
        # direct tuple lookups with zero unicode/regex overhead.
        self._ascii_first_names: tuple[str, ...] = _precompute_ascii_names(
            person_data.first_names
        )
        self._ascii_last_names: tuple[str, ...] = _precompute_ascii_names(
            person_data.last_names
        )

    # Scalar helpers

    def _one_username(self) -> str:
        first = self._engine.choice(self._ascii_first_names)
        last = self._engine.choice(self._ascii_last_names)
        fmt = self._engine.choice(self._user_formats)
        return fmt.format(first=first, last=last, num=self._engine.random_int(10, 99))

    def _one_domain(self) -> str:
        word = self._engine.choice(self._ascii_last_names)
        suffix = self._engine.choice(self._domain_suffixes)
        return f"{word}.{suffix}"

    def _one_email_from(self, domains: tuple[str, ...]) -> str:
        username = self._one_username()
        domain = self._engine.choice(domains)
        return f"{username}@{domain}"

    def _batch_emails(self, domains: tuple[str, ...], count: int) -> list[str]:
        _choices = self._engine.choices
        _ri = self._engine.random_int
        firsts = _choices(self._ascii_first_names, count)
        lasts = _choices(self._ascii_last_names, count)
        fmts = _choices(self._user_formats, count)
        doms = _choices(domains, count)
        return [
            f"{fmt.format(first=f, last=ln, num=_ri(10, 99))}@{d}"
            for fmt, f, ln, d in zip(fmts, firsts, lasts, doms)
        ]

    def _one_email(self) -> str:
        return self._one_email_from(self._free_email_domains)

    def _one_url(self) -> str:
        protocol = self._engine.choice(_URL_PROTOCOLS)
        domain = self._one_domain()
        return f"{protocol}://{domain}"

    def _one_ipv4(self) -> str:
        bits = self._engine.getrandbits(32)
        return f"{bits >> 24}.{(bits >> 16) & 0xFF}.{(bits >> 8) & 0xFF}.{bits & 0xFF}"

    def _one_slug(self) -> str:
        n = self._engine.random_int(2, 5)
        words = self._engine.choices(_SLUG_WORDS, n)
        return "-".join(words)

    def _one_safe_email(self) -> str:
        return self._one_email_from(_SAFE_DOMAINS)

    # Public API

    def username(self, count: int = 1) -> str | list[str]:
        """Generate a random username."""
        if count == 1:
            return self._one_username()
        # Vectorized batch: bulk random selections avoid per-item overhead
        _choices = self._engine.choices
        _ri = self._engine.random_int
        firsts = _choices(self._ascii_first_names, count)
        lasts = _choices(self._ascii_last_names, count)
        fmts = _choices(self._user_formats, count)
        return [
            fmt.format(first=f, last=ln, num=_ri(10, 99))
            for fmt, f, ln in zip(fmts, firsts, lasts)
        ]

    def email(self, count: int = 1) -> str | list[str]:
        """Generate a random email address."""
        if count == 1:
            return self._one_email()
        return self._batch_emails(self._free_email_domains, count)

    def domain(self, count: int = 1) -> str | list[str]:
        """Generate a random domain name."""
        if count == 1:
            return self._one_domain()
        # Vectorized batch: bulk random selections
        _choices = self._engine.choices
        words = _choices(self._ascii_last_names, count)
        suffixes = _choices(self._domain_suffixes, count)
        return [f"{w}.{s}" for w, s in zip(words, suffixes)]

    def url(self, count: int = 1) -> str | list[str]:
        """Generate a random URL."""
        if count == 1:
            return self._one_url()
        # Vectorized batch: bulk random selections
        _choices = self._engine.choices
        protocols = _choices(_URL_PROTOCOLS, count)
        words = _choices(self._ascii_last_names, count)
        suffixes = _choices(self._domain_suffixes, count)
        return [f"{p}://{w}.{s}" for p, w, s in zip(protocols, words, suffixes)]

    def ipv4(self, count: int = 1) -> str | list[str]:
        """Generate a random IPv4 address."""
        if count == 1:
            return self._one_ipv4()
        _getrandbits = self._engine.getrandbits
        return [
            f"{(b := _getrandbits(32)) >> 24}.{(b >> 16) & 0xFF}.{(b >> 8) & 0xFF}.{b & 0xFF}"
            for _ in range(count)
        ]

    def slug(self, count: int = 1) -> str | list[str]:
        """Generate a random URL-safe slug."""
        if count == 1:
            return self._one_slug()
        _ri = self._engine.random_int
        _choices = self._engine.choices
        result: list[str] = []
        for _ in range(count):
            n = _ri(2, 5)
            words = _choices(_SLUG_WORDS, n)
            result.append("-".join(words))
        return result

    def tld(self, count: int = 1) -> str | list[str]:
        """Generate a random top-level domain."""
        if count == 1:
            return self._engine.choice(self._domain_suffixes)
        return self._engine.choices(self._domain_suffixes, count)

    def safe_email(self, count: int = 1) -> str | list[str]:
        """Generate a random email using example.com/example.org."""
        if count == 1:
            return self._one_safe_email()
        return self._batch_emails(_SAFE_DOMAINS, count)
