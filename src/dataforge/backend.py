"""RandomEngine — the speed engine behind dataforge.

Provides a unified interface for random selection using stdlib
``random`` — optimised for both scalar picks and batch generation.
"""

import math as _math
import random as _random
from typing import TypeVar

_T = TypeVar("_T")

# Pre-computed power-of-10 table for random_digits_str — eliminates
# per-call ``10**n`` computation for n=1..18.
_POW10: tuple[int, ...] = tuple(10**i for i in range(19))  # _POW10[0]=1 .. _POW10[18]

# Pre-computed letter pool for letterify/bothify
_LETTERS_UPPER: str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_LETTERS_LOWER: str = "abcdefghijklmnopqrstuvwxyz"
_LETTERS_ALL: str = _LETTERS_UPPER + _LETTERS_LOWER

# Module-level cache for cumulative weights — keyed on id(weights).
# Safe because weight tuples are stored as class attributes with stable
# identity and cumulative weights are deterministic from input weights.
_CUM_WEIGHTS_CACHE: dict[int, list[float]] = {}


class RandomEngine:
    """Core randomness engine.

    Parameters
    ----------
    seed : int | None
        Optional seed for reproducibility.
    """

    __slots__ = ("_rng",)

    def __init__(self, seed: int | None = None) -> None:
        self._rng: _random.Random = _random.Random(seed)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def choice(self, data: tuple[_T, ...]) -> _T:
        """Return a single random element from *data*.

        Uses stdlib ``random.Random.choice`` which is the fastest path
        for picking one item.
        """
        return self._rng.choice(data)

    def choices(self, data: tuple[_T, ...], count: int) -> list[_T]:
        """Return *count* random elements from *data*."""
        return self._rng.choices(data, k=count)

    def sample(self, data: tuple[_T, ...], count: int) -> list[_T]:
        """Return *count* unique random elements from *data* (without replacement).

        Parameters
        ----------
        data : tuple
            The population to sample from.
        count : int
            Number of unique items to pick (must be <= len(data)).

        Returns
        -------
        list
        """
        return self._rng.sample(data, count)

    def random_int(self, min_val: int = 0, max_val: int = 9999) -> int:
        """Return a random integer between *min_val* and *max_val* inclusive."""
        return self._rng.randint(min_val, max_val)

    def random_float(
        self,
        min_val: float = 0.0,
        max_val: float = 1.0,
        precision: int = 2,
    ) -> float:
        """Return a random float between *min_val* and *max_val*.

        Parameters
        ----------
        min_val : float
            Lower bound (inclusive).
        max_val : float
            Upper bound (inclusive).
        precision : int
            Number of decimal places to round to.

        Returns
        -------
        float
        """
        val = self._rng.uniform(min_val, max_val)
        return round(val, precision)

    def numerify(self, pattern: str) -> str:
        """Replace every ``#`` in *pattern* with a random digit.

        Example: ``"#####"`` → ``"38201"``

        Optimized: if the pattern is all ``#`` characters, generates
        all digits in a single call via :meth:`random_digits_str`.
        For mixed patterns, pre-counts ``#`` and generates all digits
        in one bulk call, then substitutes via iterator.
        """
        # Fast path: pattern is entirely # characters (very common).
        # Use length check instead of iterating all characters with all().
        hash_count = pattern.count("#")
        if hash_count == len(pattern):
            return self.random_digits_str(hash_count)
        if hash_count == 0:
            return pattern
        # Slow path optimized: generate all digits in one call, then
        # substitute in-place on a list — avoids iterator overhead.
        digits = self.random_digits_str(hash_count)
        chars = list(pattern)
        d = 0
        for i, ch in enumerate(chars):
            if ch == "#":
                chars[i] = digits[d]
                d += 1
        return "".join(chars)

    def letterify(self, pattern: str, upper: bool = False) -> str:
        """Replace every ``?`` in *pattern* with a random letter.

        Example: ``"??-###"`` → ``"Ab-381"``

        Parameters
        ----------
        pattern : str
            Pattern with ``?`` placeholders for letters.
        upper : bool
            If True, use uppercase letters only.
        """
        q_count = pattern.count("?")
        if q_count == 0:
            return pattern
        pool = _LETTERS_UPPER if upper else _LETTERS_ALL
        letters = self._rng.choices(pool, k=q_count)
        it = iter(letters)
        return "".join(next(it) if ch == "?" else ch for ch in pattern)

    def bothify(self, pattern: str) -> str:
        """Replace ``#`` with digits and ``?`` with letters in *pattern*.

        Example: ``"??-####"`` → ``"Kz-3802"``
        """
        hash_count = pattern.count("#")
        q_count = pattern.count("?")
        if hash_count == 0 and q_count == 0:
            return pattern
        digits = self.random_digits_str(hash_count) if hash_count else ""
        letters = self._rng.choices(_LETTERS_ALL, k=q_count) if q_count else ()
        chars = list(pattern)
        d = q = 0
        for i, ch in enumerate(chars):
            if ch == "#":
                chars[i] = digits[d]
                d += 1
            elif ch == "?":
                chars[i] = letters[q]
                q += 1
        return "".join(chars)

    def getrandbits(self, k: int) -> int:
        """Return a random integer with *k* random bits.

        This is the fastest way to generate a large block of randomness
        in a single call — used by providers that need to build strings
        from many random hex/decimal digits (IPv6, MAC, barcodes, etc.).
        """
        return self._rng.getrandbits(k)

    def random_digits_str(self, n: int) -> str:
        """Return a string of *n* random decimal digits.

        Uses a pre-computed ``_POW10`` lookup table to avoid per-call
        ``10**n`` computation.  For small n (≤ 18), a single
        ``randint`` call is the fastest path.
        """
        _pow10 = _POW10
        if n <= 18:
            val = self._rng.randint(0, _pow10[n] - 1)
            return str(val).zfill(n)
        # For larger n, concatenate chunks of 18 digits
        parts: list[str] = []
        remaining = n
        _max18 = _pow10[18] - 1
        _randint = self._rng.randint
        while remaining > 0:
            chunk = min(remaining, 18)
            val = _randint(0, _pow10[chunk] - 1)
            parts.append(str(val).zfill(chunk))
            remaining -= chunk
        return "".join(parts)

    def seed(self, value: int) -> None:
        """Re-seed the engine for reproducibility."""
        self._rng.seed(value)

    def weighted_choices(
        self,
        data: tuple[_T, ...],
        weights: tuple[float, ...] | list[float],
        count: int,
    ) -> list[_T]:
        """Return *count* random elements from *data* with *weights*.

        Each element in *data* is selected with probability proportional
        to its corresponding weight.

        Parameters
        ----------
        data : tuple
            The items to choose from.
        weights : tuple[float, ...] or list[float]
            Non-negative weights (need not sum to 1).
        count : int
            Number of items to pick.

        Returns
        -------
        list
        """
        # Use module-level cumulative weights cache — avoids
        # recomputing inside random.choices() on every call.
        w_id = id(weights)
        cum = _CUM_WEIGHTS_CACHE.get(w_id)
        if cum is None:
            from itertools import accumulate

            cum = list(accumulate(weights))
            _CUM_WEIGHTS_CACHE[w_id] = cum
        return self._rng.choices(data, cum_weights=cum, k=count)

    def weighted_choice(
        self,
        data: tuple[_T, ...],
        weights: tuple[float, ...] | list[float],
    ) -> _T:
        """Return a single random element from *data* with *weights*.

        Scalar version of :meth:`weighted_choices`.
        """
        cache = _CUM_WEIGHTS_CACHE
        w_id = id(weights)
        cum = cache.get(w_id)
        if cum is None:
            from itertools import accumulate

            cum = list(accumulate(weights))
            cache[w_id] = cum
        return self._rng.choices(data, cum_weights=cum, k=1)[0]

    # ------------------------------------------------------------------
    # Statistical distributions
    # ------------------------------------------------------------------

    def gauss(self, mu: float = 0.0, sigma: float = 1.0) -> float:
        """Return a random value from a Gaussian (normal) distribution.

        Uses stdlib ``random.gauss`` which is faster than ``normalvariate``
        and thread-safe in CPython 3.12+.

        Parameters
        ----------
        mu : float
            Mean of the distribution.
        sigma : float
            Standard deviation of the distribution.
        """
        return self._rng.gauss(mu, sigma)

    def gauss_int(self, mu: float, sigma: float, min_val: int, max_val: int) -> int:
        """Return a clamped integer from a Gaussian distribution.

        Generates a float from ``gauss(mu, sigma)`` and clamps to
        ``[min_val, max_val]``.  Useful for realistic age, score,
        salary distributions.

        Parameters
        ----------
        mu : float
            Mean of the distribution.
        sigma : float
            Standard deviation.
        min_val : int
            Lower clamp bound.
        max_val : int
            Upper clamp bound.

        Returns
        -------
        int
        """
        val = self._rng.gauss(mu, sigma)
        return max(min_val, min(max_val, round(val)))

    def exponential(self, lambd: float = 1.0) -> float:
        """Return a random value from an exponential distribution.

        Parameters
        ----------
        lambd : float
            Rate parameter (1/mean). Must be > 0.
        """
        return self._rng.expovariate(lambd)

    def log_normal(self, mu: float = 0.0, sigma: float = 1.0) -> float:
        """Return a random value from a log-normal distribution.

        Parameters
        ----------
        mu : float
            Mean of the underlying normal distribution.
        sigma : float
            Standard deviation of the underlying normal distribution.
        """
        return self._rng.lognormvariate(mu, sigma)

    def triangular(
        self, low: float = 0.0, high: float = 1.0, mode: float | None = None
    ) -> float:
        """Return a random value from a triangular distribution.

        Parameters
        ----------
        low : float
            Lower bound.
        high : float
            Upper bound.
        mode : float | None
            Peak of the distribution. Defaults to midpoint.
        """
        if mode is None:
            mode = (low + high) / 2.0
        return self._rng.triangular(low, high, mode)

    def pareto(self, alpha: float = 1.0) -> float:
        """Return a random value from a Pareto distribution.

        Useful for power-law/Zipf-like distributions (wealth, popularity).

        Parameters
        ----------
        alpha : float
            Shape parameter (must be > 0).
        """
        return self._rng.paretovariate(alpha)

    def vonmises(self, mu: float = 0.0, kappa: float = 0.0) -> float:
        """Return a random value from a von Mises distribution.

        Circular analog of the normal distribution — useful for
        directional data (angles, hours of day).

        Parameters
        ----------
        mu : float
            Mean angle in radians.
        kappa : float
            Concentration parameter (0 = uniform on circle).
        """
        return self._rng.vonmisesvariate(mu, kappa)

    def beta(self, alpha: float = 2.0, beta_param: float = 5.0) -> float:
        """Return a random value from a Beta distribution.

        Useful for probabilities, completion rates, conversion ratios.

        Parameters
        ----------
        alpha : float
            Alpha shape parameter (> 0).
        beta_param : float
            Beta shape parameter (> 0).
        """
        return self._rng.betavariate(alpha, beta_param)

    def gamma(self, alpha: float = 1.0, beta_param: float = 1.0) -> float:
        """Return a random value from a Gamma distribution.

        Parameters
        ----------
        alpha : float
            Shape parameter (> 0).
        beta_param : float
            Scale parameter (> 0).
        """
        return self._rng.gammavariate(alpha, beta_param)

    def zipf(self, alpha: float = 1.5, n: int = 100) -> int:
        """Return a random integer from an approximate Zipf distribution.

        Uses rejection sampling from a Pareto distribution.
        Values range from 1 to *n*.

        Parameters
        ----------
        alpha : float
            Exponent parameter (> 1.0).
        n : int
            Upper bound for the returned integer.

        Returns
        -------
        int
            A random integer in [1, n].
        """
        # Fast approximate Zipf via truncated Pareto
        while True:
            val = int(_math.ceil(self._rng.paretovariate(alpha - 1)))
            if val <= n:
                return val

    # ------------------------------------------------------------------
    # Regex-based string generation
    # ------------------------------------------------------------------

    def regexify(self, pattern: str) -> str:
        """Generate a random string matching a simplified regex *pattern*.

        Supports a practical subset of regex syntax:
        - ``[a-z]``, ``[A-Z]``, ``[0-9]``, ``[abc]`` — character classes
        - ``{n}``, ``{n,m}`` — exact and range repetition
        - ``+`` — 1-3 repetitions
        - ``*`` — 0-3 repetitions
        - ``?`` — 0 or 1
        - ``(a|b|c)`` — alternation groups
        - ``.`` — any printable ASCII character
        - ``\\d``, ``\\w``, ``\\s`` — digit, word char, whitespace
        - Literal characters

        Parameters
        ----------
        pattern : str
            Simplified regex pattern.

        Returns
        -------
        str
        """
        _rng = self._rng
        result: list[str] = []
        i = 0
        n = len(pattern)

        while i < n:
            ch = pattern[i]

            if ch == "\\" and i + 1 < n:
                # Escape sequences
                esc = pattern[i + 1]
                if esc == "d":
                    chars = "0123456789"
                elif esc == "w":
                    chars = _LETTERS_ALL + "0123456789_"
                elif esc == "s":
                    chars = " \t"
                else:
                    # Literal escaped char
                    result.append(esc)
                    i += 2
                    i, _ = self._regexify_quantifier(pattern, i, _rng, result, esc)
                    continue
                base = _rng.choice(chars)
                i += 2
                i, _ = self._regexify_quantifier(pattern, i, _rng, result, base, chars)
                continue

            if ch == "[":
                # Character class
                close = pattern.find("]", i + 1)
                if close == -1:
                    result.append(ch)
                    i += 1
                    continue
                chars = self._parse_char_class(pattern[i + 1 : close])
                base = _rng.choice(chars) if chars else "?"
                i = close + 1
                i, _ = self._regexify_quantifier(pattern, i, _rng, result, base, chars)
                continue

            if ch == "(":
                # Alternation group
                close = pattern.find(")", i + 1)
                if close == -1:
                    result.append(ch)
                    i += 1
                    continue
                options = pattern[i + 1 : close].split("|")
                chosen = _rng.choice(options)
                i = close + 1
                # Check for quantifier on group
                if i < n and pattern[i] in "{+*?":
                    if pattern[i] == "{":
                        end_brace = pattern.find("}", i + 1)
                        if end_brace != -1:
                            rep_spec = pattern[i + 1 : end_brace]
                            i = end_brace + 1
                            if "," in rep_spec:
                                parts = rep_spec.split(",", 1)
                                lo = int(parts[0]) if parts[0] else 0
                                hi = int(parts[1]) if parts[1] else lo
                                reps = _rng.randint(lo, hi)
                            else:
                                reps = int(rep_spec)
                            for _ in range(reps):
                                result.append(_rng.choice(options))
                            continue
                    elif pattern[i] == "+":
                        reps = _rng.randint(1, 3)
                        i += 1
                        for _ in range(reps):
                            result.append(_rng.choice(options))
                        continue
                    elif pattern[i] == "*":
                        reps = _rng.randint(0, 3)
                        i += 1
                        for _ in range(reps):
                            result.append(_rng.choice(options))
                        continue
                    elif pattern[i] == "?":
                        if _rng.random() > 0.5:
                            result.append(chosen)
                        i += 1
                        continue
                result.append(chosen)
                continue

            if ch == ".":
                # Any printable ASCII
                base = chr(_rng.randint(33, 126))
                i += 1
                i, _ = self._regexify_quantifier(pattern, i, _rng, result, base)
                continue

            # Literal character
            i += 1
            i, _ = self._regexify_quantifier(pattern, i, _rng, result, ch)

        return "".join(result)

    @staticmethod
    def _parse_char_class(spec: str) -> str:
        """Parse a character class interior like ``a-zA-Z0-9``."""
        chars: list[str] = []
        i = 0
        n = len(spec)
        while i < n:
            if i + 2 < n and spec[i + 1] == "-":
                lo = ord(spec[i])
                hi = ord(spec[i + 2])
                chars.extend(chr(c) for c in range(lo, hi + 1))
                i += 3
            else:
                chars.append(spec[i])
                i += 1
        return "".join(chars) if chars else "?"

    @staticmethod
    def _regexify_quantifier(
        pattern: str,
        i: int,
        rng: _random.Random,
        result: list[str],
        base: str,
        char_pool: str | None = None,
    ) -> tuple[int, bool]:
        """Handle quantifiers {n}, {n,m}, +, *, ? after a token."""
        n = len(pattern)
        if i >= n:
            result.append(base)
            return i, False

        ch = pattern[i]
        if ch == "{":
            end = pattern.find("}", i + 1)
            if end == -1:
                result.append(base)
                return i, False
            rep_spec = pattern[i + 1 : end]
            i = end + 1
            if "," in rep_spec:
                parts = rep_spec.split(",", 1)
                lo = int(parts[0]) if parts[0] else 0
                hi = int(parts[1]) if parts[1] else lo
                reps = rng.randint(lo, hi)
            else:
                reps = int(rep_spec)
            if char_pool:
                result.extend(rng.choice(char_pool) for _ in range(reps))
            else:
                result.extend(base for _ in range(reps))
            return i, True
        elif ch == "+":
            reps = rng.randint(1, 3)
            if char_pool:
                result.extend(rng.choice(char_pool) for _ in range(reps))
            else:
                result.extend(base for _ in range(reps))
            return i + 1, True
        elif ch == "*":
            reps = rng.randint(0, 3)
            if char_pool:
                result.extend(rng.choice(char_pool) for _ in range(reps))
            else:
                result.extend(base for _ in range(reps))
            return i + 1, True
        elif ch == "?":
            if rng.random() > 0.5:
                result.append(base)
            return i + 1, True
        else:
            result.append(base)
            return i, False
