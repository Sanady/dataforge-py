"""Chaos mode — inject data quality issues for testing resilience.

A post-processing transformer that injects realistic data quality
problems: nulls, type mismatches, boundary values, duplicates,
whitespace issues, encoding chaos, format inconsistencies, and
truncation.

Usage::

    from dataforge import DataForge
    from dataforge.chaos import ChaosTransformer

    forge = DataForge(seed=42)
    schema = forge.schema(["first_name", "email", "age"])
    rows = schema.generate(count=100)

    chaos = ChaosTransformer(
        null_rate=0.05,
        type_mismatch_rate=0.02,
        boundary_rate=0.01,
        duplicate_rate=0.03,
        whitespace_rate=0.02,
        encoding_rate=0.01,
        format_rate=0.02,
        truncation_rate=0.01,
    )
    dirty_rows = chaos.transform(rows)
"""

from __future__ import annotations

import random as _random_mod
from typing import Any


# ------------------------------------------------------------------
# Boundary value catalogs
# ------------------------------------------------------------------

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
    "\ufeff",  # BOM
)

_BOUNDARY_INT: tuple[Any, ...] = (
    0,
    -1,
    1,
    -2147483648,  # INT32_MIN
    2147483647,  # INT32_MAX
    -9223372036854775808,  # INT64_MIN
    9223372036854775807,  # INT64_MAX
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
    1e-308,  # near MIN_FLOAT
    1e308,  # near MAX_FLOAT
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
    "2024-02-30",  # invalid day
    "2024-13-01",  # invalid month
)

# Unicode edge cases for encoding chaos
_UNICODE_CHAOS: tuple[str, ...] = (
    "\u200b",  # zero-width space
    "\u200e",  # left-to-right mark
    "\u200f",  # right-to-left mark
    "\u00e9",  # é
    "\u00f1",  # ñ
    "\u00fc",  # ü
    "\u4e2d",  # Chinese character
    "\U0001f600",  # emoji
    "\u202e",  # right-to-left override
    "\ufeff",  # BOM
    "\u0000",  # null
    "\ud83d",  # lone surrogate (may cause issues)
)

# Whitespace variants
_WHITESPACE_CHAOS: tuple[str, ...] = (
    " ",  # extra leading space
    "  ",  # double space
    "\t",  # tab
    " \t",  # mixed
    "\n",  # newline
    "\r",  # carriage return
    "\u00a0",  # non-breaking space
    "\u2003",  # em space
    "\u200b",  # zero-width space
)


class ChaosTransformer:
    """Inject data quality issues into generated data.

    All rates are probabilities (0.0–1.0) applied per-cell.

    Parameters
    ----------
    null_rate : float
        Probability of replacing a value with None.
    type_mismatch_rate : float
        Probability of injecting a type-mismatched value.
    boundary_rate : float
        Probability of injecting a boundary/edge-case value.
    duplicate_rate : float
        Probability of duplicating a random existing row.
    whitespace_rate : float
        Probability of adding whitespace chaos to string values.
    encoding_rate : float
        Probability of injecting unicode edge cases into strings.
    format_rate : float
        Probability of format inconsistency (case, separators).
    truncation_rate : float
        Probability of truncating string values.
    seed : int | None
        Optional seed for reproducibility.
    """

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
        """Apply chaos transformations to rows.

        Parameters
        ----------
        rows : list[dict[str, Any]]
            Input rows (will NOT be modified in place — copies are made).
        columns : list[str] | None
            Specific columns to apply chaos to. If None, all columns
            are eligible.

        Returns
        -------
        list[dict[str, Any]]
            Transformed rows with injected data quality issues.
        """
        if not rows:
            return rows

        rng = self._rng

        # Pre-check which cell-level transformations are active to avoid
        # checking rates that are 0 in the inner loop.
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

        # Only copy rows if we have cell-level transforms to apply
        if has_any_cell_transform:
            result: list[dict[str, Any]] = [dict(row) for row in rows]
            _random = rng.random
            for row in result:
                for col in target_cols:
                    if col not in row:
                        continue
                    val = row[col]

                    # Null injection
                    if null_rate > 0 and _random() < null_rate:
                        row[col] = None
                        continue

                    # Type mismatch
                    if type_mismatch_rate > 0 and _random() < type_mismatch_rate:
                        row[col] = self._inject_type_mismatch(val, rng)
                        continue

                    # Boundary values
                    if boundary_rate > 0 and _random() < boundary_rate:
                        row[col] = self._inject_boundary(val, rng)
                        continue

                    # String-specific transformations
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
            # No cell-level transforms — still need copies for duplicate injection
            result = [dict(row) for row in rows]

        # Row-level: duplicate injection
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
            # Detect if it looks like a date
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
            return chaos + val  # prepend
        elif action == 1:
            return val + chaos  # append
        else:
            # Insert in middle
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
            # Random case
            return "".join(c.upper() if rng.random() > 0.5 else c.lower() for c in val)
        else:
            # Replace separators
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
