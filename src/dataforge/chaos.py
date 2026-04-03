"""Chaos mode — inject data quality issues for testing resilience."""

from __future__ import annotations

import random as _random_mod
from typing import Any


_BOUNDARY_STR: tuple[str, ...] = (
    "",
    " ",
    "\t",
    "\n",
    "\r\n",
    "null",
    "NULL",
    "None",
    "undefined",
    "NaN",
    "N/A",
    "n/a",
    "-",
    "true",
    "false",
    "0",
    "<script>alert('xss')</script>",
    "Robert'); DROP TABLE students;--",
    "a" * 1000,
    "\x00",
    "\ufeff",
)

_BOUNDARY_INT: tuple[Any, ...] = (
    0,
    -1,
    1,
    -2147483648,
    2147483647,
    -9223372036854775808,
    9223372036854775807,
    "not_a_number",
    "",
    None,
    float("inf"),
    float("-inf"),
)

_BOUNDARY_FLOAT: tuple[Any, ...] = (
    0.0,
    -0.0,
    float("inf"),
    float("-inf"),
    float("nan"),
    1e-308,
    1e308,
    "not_a_number",
    "",
    None,
)

_BOUNDARY_DATE: tuple[str, ...] = (
    "0000-00-00",
    "9999-12-31",
    "1970-01-01",
    "2038-01-19",
    "not-a-date",
    "",
    "2024-02-30",
    "2024-13-01",
)

_UNICODE_CHAOS: tuple[str, ...] = (
    "\u200b",
    "\u200e",
    "\u200f",
    "\u00e9",
    "\u00f1",
    "\u00fc",
    "\u4e2d",
    "\U0001f600",
    "\u202e",
    "\ufeff",
    "\u0000",
    "\ud83d",
)

_WHITESPACE_CHAOS: tuple[str, ...] = (
    " ",
    "  ",
    "\t",
    " \t",
    "\n",
    "\r",
    "\u00a0",
    "\u2003",
    "\u200b",
)


class ChaosTransformer:
    """Inject data quality issues into generated data."""

    __slots__ = (
        "_null_rate",
        "_type_mismatch_rate",
        "_boundary_rate",
        "_duplicate_rate",
        "_whitespace_rate",
        "_encoding_rate",
        "_format_rate",
        "_truncation_rate",
        "_rng",
    )

    def __init__(
        self,
        null_rate: float = 0.0,
        type_mismatch_rate: float = 0.0,
        boundary_rate: float = 0.0,
        duplicate_rate: float = 0.0,
        whitespace_rate: float = 0.0,
        encoding_rate: float = 0.0,
        format_rate: float = 0.0,
        truncation_rate: float = 0.0,
        seed: int | None = None,
    ) -> None:
        self._null_rate = null_rate
        self._type_mismatch_rate = type_mismatch_rate
        self._boundary_rate = boundary_rate
        self._duplicate_rate = duplicate_rate
        self._whitespace_rate = whitespace_rate
        self._encoding_rate = encoding_rate
        self._format_rate = format_rate
        self._truncation_rate = truncation_rate
        self._rng = _random_mod.Random(seed)

    def transform(
        self,
        rows: list[dict[str, Any]],
        columns: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Apply chaos transformations to rows."""
        if not rows:
            return rows

        rng = self._rng

        null_rate = self._null_rate
        type_mismatch_rate = self._type_mismatch_rate
        boundary_rate = self._boundary_rate
        whitespace_rate = self._whitespace_rate
        encoding_rate = self._encoding_rate
        format_rate = self._format_rate
        truncation_rate = self._truncation_rate

        has_any_cell_transform = (
            null_rate > 0
            or type_mismatch_rate > 0
            or boundary_rate > 0
            or whitespace_rate > 0
            or encoding_rate > 0
            or format_rate > 0
            or truncation_rate > 0
        )

        target_cols = columns or list(rows[0].keys())

        if has_any_cell_transform:
            result: list[dict[str, Any]] = [dict(row) for row in rows]
            _random = rng.random
            for row in result:
                for col in target_cols:
                    if col not in row:
                        continue
                    val = row[col]

                    if null_rate > 0 and _random() < null_rate:
                        row[col] = None
                        continue

                    if type_mismatch_rate > 0 and _random() < type_mismatch_rate:
                        row[col] = self._inject_type_mismatch(val, rng)
                        continue

                    if boundary_rate > 0 and _random() < boundary_rate:
                        row[col] = self._inject_boundary(val, rng)
                        continue

                    if isinstance(val, str):
                        if whitespace_rate > 0 and _random() < whitespace_rate:
                            row[col] = self._inject_whitespace(val, rng)
                            continue

                        if encoding_rate > 0 and _random() < encoding_rate:
                            row[col] = self._inject_encoding(val, rng)
                            continue

                        if format_rate > 0 and _random() < format_rate:
                            row[col] = self._inject_format_issue(val, rng)
                            continue

                        if truncation_rate > 0 and _random() < truncation_rate:
                            row[col] = self._inject_truncation(val, rng)
                            continue
        else:
            result = [dict(row) for row in rows]

        if self._duplicate_rate > 0 and len(result) > 1:
            n_dups = rng.binomialvariate(len(result), self._duplicate_rate)
            for _ in range(n_dups):
                src_idx = rng.randint(0, len(result) - 1)
                insert_idx = rng.randint(0, len(result))
                result.insert(insert_idx, dict(result[src_idx]))

        return result

    @staticmethod
    def _inject_type_mismatch(val: Any, rng: _random_mod.Random) -> Any:
        """Replace value with a type-mismatched one."""
        if isinstance(val, str):
            return rng.choice([42, 3.14, True, False, None, [], {}])
        if isinstance(val, (int, float)):
            return rng.choice(["not_a_number", "", "NaN", True, None])
        if isinstance(val, bool):
            return rng.choice(["yes", "no", 1, 0, "true", "false"])
        return str(val)

    @staticmethod
    def _inject_boundary(val: Any, rng: _random_mod.Random) -> Any:
        """Replace value with a boundary/edge-case value."""
        if isinstance(val, str):
            if len(val) == 10 and val[4:5] == "-" and val[7:8] == "-":
                return rng.choice(_BOUNDARY_DATE)
            return rng.choice(_BOUNDARY_STR)
        if isinstance(val, int):
            return rng.choice(_BOUNDARY_INT)
        if isinstance(val, float):
            return rng.choice(_BOUNDARY_FLOAT)
        return rng.choice(_BOUNDARY_STR)

    @staticmethod
    def _inject_whitespace(val: str, rng: _random_mod.Random) -> str:
        """Add whitespace chaos to a string value."""
        chaos = rng.choice(_WHITESPACE_CHAOS)
        action = rng.randint(0, 2)
        if action == 0:
            return chaos + val
        elif action == 1:
            return val + chaos
        else:
            if len(val) > 1:
                pos = rng.randint(1, len(val) - 1)
                return val[:pos] + chaos + val[pos:]
            return chaos + val

    @staticmethod
    def _inject_encoding(val: str, rng: _random_mod.Random) -> str:
        """Inject unicode edge cases into a string."""
        chaos_char = rng.choice(_UNICODE_CHAOS)
        action = rng.randint(0, 2)
        if action == 0:
            return chaos_char + val
        elif action == 1:
            return val + chaos_char
        else:
            if len(val) > 1:
                pos = rng.randint(1, len(val) - 1)
                return val[:pos] + chaos_char + val[pos:]
            return val + chaos_char

    @staticmethod
    def _inject_format_issue(val: str, rng: _random_mod.Random) -> str:
        """Inject format inconsistency."""
        action = rng.randint(0, 4)
        if action == 0:
            return val.upper()
        elif action == 1:
            return val.lower()
        elif action == 2:
            return val.title()
        elif action == 3:
            return "".join(c.upper() if rng.random() > 0.5 else c.lower() for c in val)
        else:
            for old, new in [("-", "/"), ("/", "-"), (" ", "_"), ("_", " ")]:
                if old in val:
                    return val.replace(old, new)
            return val

    @staticmethod
    def _inject_truncation(val: str, rng: _random_mod.Random) -> str:
        """Truncate a string value."""
        if len(val) <= 1:
            return val
        cut_at = rng.randint(1, max(1, len(val) - 1))
        return val[:cut_at]

    def __repr__(self) -> str:
        rates = []
        for attr in (
            "null",
            "type_mismatch",
            "boundary",
            "duplicate",
            "whitespace",
            "encoding",
            "format",
            "truncation",
        ):
            rate = getattr(self, f"_{attr}_rate")
            if rate > 0:
                rates.append(f"{attr}={rate}")
        return f"ChaosTransformer({', '.join(rates)})"
