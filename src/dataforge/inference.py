"""Schema inference — analyze data and auto-create matching Schemas.

Analyzes CSV files, DataFrames, database tables, or lists of dicts
to detect types, semantic patterns, distributions, and null rates,
then builds a matching DataForge Schema.

Usage::

    from dataforge import DataForge
    from dataforge.inference import SchemaInferrer

    forge = DataForge(seed=42)
    inferrer = SchemaInferrer(forge)

    # From CSV
    schema = inferrer.from_csv("data.csv")

    # From list of dicts
    schema = inferrer.from_records([
        {"name": "Alice", "email": "alice@test.com", "age": 30},
        {"name": "Bob", "email": "bob@test.com", "age": 25},
    ])

    # Inspect what was detected
    print(inferrer.describe())
"""

from __future__ import annotations

import re as _re
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from dataforge.core import DataForge

# ------------------------------------------------------------------
# Semantic type detection patterns
# ------------------------------------------------------------------

_SEMANTIC_PATTERNS: list[tuple[str, _re.Pattern[str], str]] = [
    ("email", _re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}$"), "email"),
    ("phone", _re.compile(r"^[\+]?[\d\s\-\(\)]{7,20}$"), "phone_number"),
    (
        "uuid",
        _re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", _re.I
        ),
        "uuid4",
    ),
    ("ipv4", _re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"), "ipv4"),
    ("ipv6", _re.compile(r"^[0-9a-f:]{3,39}$", _re.I), "ipv6"),
    ("url", _re.compile(r"^https?://[^\s]+$"), "url"),
    ("mac", _re.compile(r"^([0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}$"), "mac_address"),
    ("date_iso", _re.compile(r"^\d{4}-\d{2}-\d{2}$"), "date"),
    ("datetime_iso", _re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}"), "datetime"),
    ("time_iso", _re.compile(r"^\d{2}:\d{2}(:\d{2})?$"), "time"),
    ("zipcode_us", _re.compile(r"^\d{5}(-\d{4})?$"), "zipcode"),
    ("ssn", _re.compile(r"^\d{3}-\d{2}-\d{4}$"), "ssn"),
    (
        "credit_card",
        _re.compile(r"^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$"),
        "credit_card_number",
    ),
    ("hex_color", _re.compile(r"^#[0-9a-fA-F]{6}$"), "hex_color"),
    ("ean13", _re.compile(r"^\d{13}$"), "ean13"),
    ("isbn", _re.compile(r"^97[89]-?\d{1,5}-?\d{1,7}-?\d{1,7}-?\d$"), "isbn13"),
]

# ------------------------------------------------------------------
# Column name heuristics (from core.py _FIELD_ALIASES)
# ------------------------------------------------------------------


# Module-level cache for field aliases — populated on first use
_CACHED_ALIASES: dict[str, str] | None = None


def _get_field_aliases() -> dict[str, str]:
    """Import and return field aliases from core (cached after first call)."""
    global _CACHED_ALIASES
    if _CACHED_ALIASES is None:
        from dataforge.core import _FIELD_ALIASES

        _CACHED_ALIASES = _FIELD_ALIASES
    return _CACHED_ALIASES


# ------------------------------------------------------------------
# Type detection
# ------------------------------------------------------------------


# Pre-compiled patterns for fast numeric string detection (avoids try/except overhead)
_INT_PATTERN = _re.compile(r"^-?\d+$")
_FLOAT_PATTERN = _re.compile(r"^-?\d+\.\d*$|^-?\d*\.\d+$|^-?\d+[eE][+-]?\d+$")


def _detect_base_type(values: list[Any]) -> str:
    """Detect the base type of a column's values.

    Returns one of: 'str', 'int', 'float', 'bool', 'date',
    'datetime', 'none', 'mixed'.
    """
    type_counts: dict[str, int] = {}
    _int_match = _INT_PATTERN.match
    _float_match = _FLOAT_PATTERN.match
    for val in values:
        if val is None or (isinstance(val, str) and val.strip() == ""):
            type_counts["none"] = type_counts.get("none", 0) + 1
            continue
        t = type(val).__name__
        if t == "str":
            # Fast regex-based numeric detection (avoids try/except overhead)
            s = val.strip()
            if _int_match(s):
                type_counts["int"] = type_counts.get("int", 0) + 1
                continue
            if _float_match(s):
                type_counts["float"] = type_counts.get("float", 0) + 1
                continue
            if s.lower() in ("true", "false", "yes", "no"):
                type_counts["bool"] = type_counts.get("bool", 0) + 1
                continue
        type_counts[t] = type_counts.get(t, 0) + 1

    # Remove 'none' for type decision
    non_none = {k: v for k, v in type_counts.items() if k != "none"}
    if not non_none:
        return "none"
    dominant = max(non_none, key=lambda k: non_none[k])
    # If >80% of non-none values are the same type, use it
    total_non_none = sum(non_none.values())
    if non_none[dominant] / total_non_none >= 0.8:
        return dominant
    return "mixed"


def _detect_semantic_type(
    col_name: str,
    values: list[Any],
    base_type: str,
) -> str | None:
    """Detect the semantic type of a column.

    Returns a DataForge field name or None.
    """
    # 1. Try column name heuristic
    aliases = _get_field_aliases()
    name_lower = col_name.lower().strip().replace(" ", "_")
    if name_lower in aliases:
        return aliases[name_lower]

    # Also try without common prefixes/suffixes
    for prefix in ("user_", "customer_", "order_", "item_"):
        if name_lower.startswith(prefix):
            stripped = name_lower[len(prefix) :]
            if stripped in aliases:
                return aliases[stripped]

    # 2. Try regex patterns on string values
    if base_type == "str":
        # Sample up to 100 non-null string values for pattern detection
        sample = [str(v) for v in values if v is not None and str(v).strip()][:100]
        if sample:
            for _name, pattern, field in _SEMANTIC_PATTERNS:
                matches = sum(1 for s in sample if pattern.match(s))
                if matches / len(sample) >= 0.7:
                    return field

    # 3. Type-based fallback
    if base_type == "bool":
        return "boolean"
    if base_type == "int":
        # Check if it looks like age, port, year, etc.
        if "age" in name_lower:
            return "misc.random_int"
        if "port" in name_lower:
            return "port"
        if "year" in name_lower:
            return "date"

    return None


def _compute_null_rate(values: list[Any]) -> float:
    """Compute the null/empty rate of a column."""
    if not values:
        return 0.0
    n_null = sum(
        1 for v in values if v is None or (isinstance(v, str) and v.strip() == "")
    )
    return round(n_null / len(values), 3)


def _compute_stats(values: list[Any], base_type: str) -> dict[str, Any]:
    """Compute basic statistics for a column."""
    stats: dict[str, Any] = {"count": len(values)}

    if base_type in ("int", "float"):
        nums = []
        for v in values:
            if v is None:
                continue
            try:
                nums.append(float(v))
            except (ValueError, TypeError):
                pass
        if nums:
            stats["min"] = min(nums)
            stats["max"] = max(nums)
            stats["mean"] = sum(nums) / len(nums)
            stats["unique"] = len(set(nums))

    elif base_type == "str":
        strs = [str(v) for v in values if v is not None]
        if strs:
            lengths = [len(s) for s in strs]
            stats["min_length"] = min(lengths)
            stats["max_length"] = max(lengths)
            stats["avg_length"] = round(sum(lengths) / len(lengths), 1)
            stats["unique"] = len(set(strs))

    return stats


# ------------------------------------------------------------------
# Column analysis result
# ------------------------------------------------------------------


class ColumnAnalysis:
    """Analysis result for a single column."""

    __slots__ = (
        "name",
        "base_type",
        "semantic_type",
        "null_rate",
        "stats",
        "dataforge_field",
    )

    def __init__(
        self,
        name: str,
        base_type: str,
        semantic_type: str | None,
        null_rate: float,
        stats: dict[str, Any],
        dataforge_field: str | None,
    ) -> None:
        self.name = name
        self.base_type = base_type
        self.semantic_type = semantic_type
        self.null_rate = null_rate
        self.stats = stats
        self.dataforge_field = dataforge_field

    def __repr__(self) -> str:
        return (
            f"ColumnAnalysis(name={self.name!r}, type={self.base_type}, "
            f"semantic={self.semantic_type!r}, field={self.dataforge_field!r})"
        )


# ------------------------------------------------------------------
# SchemaInferrer
# ------------------------------------------------------------------


class SchemaInferrer:
    """Analyze data sources and build matching DataForge Schemas.

    Parameters
    ----------
    forge : DataForge
        The DataForge instance to create schemas with.
    sample_size : int
        Maximum number of rows to sample for analysis.
    """

    __slots__ = ("_forge", "_sample_size", "_analyses")

    def __init__(self, forge: DataForge, sample_size: int = 1000) -> None:
        self._forge = forge
        self._sample_size = sample_size
        self._analyses: list[ColumnAnalysis] = []

    def from_records(
        self,
        records: list[dict[str, Any]],
    ) -> Any:
        """Infer a Schema from a list of dicts.

        Parameters
        ----------
        records : list[dict[str, Any]]
            Input data rows.

        Returns
        -------
        Schema
        """
        if not records:
            raise ValueError("Cannot infer schema from empty data.")

        # Sample
        sample = records[: self._sample_size]
        columns = list(sample[0].keys())

        # Analyze each column
        self._analyses = []
        field_map: dict[str, str] = {}
        null_fields: dict[str, float] = {}

        for col_name in columns:
            values = [row.get(col_name) for row in sample]
            analysis = self._analyze_column(col_name, values)
            self._analyses.append(analysis)

            if analysis.dataforge_field:
                field_map[col_name] = analysis.dataforge_field
                if analysis.null_rate > 0.01:
                    null_fields[col_name] = analysis.null_rate

        if not field_map:
            raise ValueError(
                "Could not map any columns to DataForge fields. "
                "Columns found: " + ", ".join(columns)
            )

        from dataforge.schema import Schema

        return Schema(
            self._forge,
            field_map,
            null_fields=null_fields if null_fields else None,
        )

    def from_csv(
        self,
        path: str,
        delimiter: str = ",",
        encoding: str = "utf-8",
    ) -> Any:
        """Infer a Schema from a CSV file.

        Parameters
        ----------
        path : str
            Path to the CSV file.
        delimiter : str
            Field delimiter.
        encoding : str
            File encoding.

        Returns
        -------
        Schema
        """
        import csv

        with open(path, "r", encoding=encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            records: list[dict[str, Any]] = []
            for i, row in enumerate(reader):
                if i >= self._sample_size:
                    break
                records.append(dict(row))

        return self.from_records(records)

    def from_dataframe(self, df: Any) -> Any:
        """Infer a Schema from a pandas DataFrame.

        Parameters
        ----------
        df : pandas.DataFrame
            Input DataFrame.

        Returns
        -------
        Schema
        """
        sample = df.head(self._sample_size)
        records = sample.to_dict("records")
        return self.from_records(records)

    def _analyze_column(
        self,
        col_name: str,
        values: list[Any],
    ) -> ColumnAnalysis:
        """Analyze a single column."""
        base_type = _detect_base_type(values)
        semantic_type = _detect_semantic_type(col_name, values, base_type)
        null_rate = _compute_null_rate(values)
        stats = _compute_stats(values, base_type)

        # Determine DataForge field
        dataforge_field: str | None = None
        if semantic_type:
            # Verify it's a valid field
            try:
                self._forge._resolve_field(semantic_type)
                dataforge_field = semantic_type
            except ValueError:
                dataforge_field = None

        # Fallback: try column name directly
        if dataforge_field is None:
            try:
                self._forge._resolve_field(col_name)
                dataforge_field = col_name
            except ValueError:
                pass

        # Last resort: type-based fallback
        if dataforge_field is None:
            if base_type == "bool":
                dataforge_field = "boolean"
            elif base_type == "int":
                dataforge_field = "misc.random_int"
            elif base_type == "float":
                dataforge_field = "misc.random_int"

        return ColumnAnalysis(
            name=col_name,
            base_type=base_type,
            semantic_type=semantic_type,
            null_rate=null_rate,
            stats=stats,
            dataforge_field=dataforge_field,
        )

    def describe(self) -> str:
        """Return a human-readable description of the inferred schema.

        Returns
        -------
        str
        """
        if not self._analyses:
            return "No schema has been inferred yet."

        lines: list[str] = ["Inferred Schema:", "=" * 60]
        for a in self._analyses:
            status = "mapped" if a.dataforge_field else "UNMAPPED"
            field_str = a.dataforge_field or "???"
            lines.append(
                f"  {a.name:<25} {a.base_type:<8} -> {field_str:<20} "
                f"[{status}] null={a.null_rate:.1%}"
            )
            if a.stats:
                stat_parts = [f"{k}={v}" for k, v in a.stats.items() if k != "count"]
                if stat_parts:
                    lines.append(f"  {'':25} stats: {', '.join(stat_parts)}")
        lines.append("=" * 60)
        mapped_count = sum(1 for a in self._analyses if a.dataforge_field)
        lines.append(
            f"  {mapped_count}/{len(self._analyses)} columns mapped to DataForge fields"
        )
        return "\n".join(lines)

    @property
    def analyses(self) -> list[ColumnAnalysis]:
        """Access the column analyses from the last inference."""
        return list(self._analyses)

    def __repr__(self) -> str:
        if self._analyses:
            return f"SchemaInferrer(columns={len(self._analyses)})"
        return "SchemaInferrer(no analysis performed)"
