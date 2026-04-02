"""Schema — zero-overhead bulk data generation via pre-resolved fields.

A ``Schema`` pre-resolves provider/method lookups once at creation time,
then generates rows with a tight loop over pre-bound callables — no
per-row field resolution, no ``getattr`` calls during generation.

Usage::

    from dataforge import DataForge

    forge = DataForge(seed=42)
    schema = forge.schema(["first_name", "email", "city"])
    rows   = schema.generate(count=1_000_000)

    # Lambda / correlated fields:
    schema = forge.schema({
        "name": "full_name",
        "email": "email",
        "username": lambda row: row["name"].lower().replace(" ", "."),
    })

    # Typed schema — values preserve native Python types:
    schema = forge.schema(["first_name", "port", "boolean"])
    rows = schema.generate(count=10)
    # rows[0]["port"] → 8080 (int, not str)
"""

from __future__ import annotations

import gzip as _gzip
import io as _io
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dataforge.core import DataForge

# Sentinel for columns that depend on the current row
_ROW_LAMBDA = object()


@contextmanager
def _open_file(
    path: str,
    mode: str = "w",
    encoding: str = "utf-8",
    compress: bool | None = None,
    newline: str | None = None,
) -> Iterator[Any]:
    """Context manager that opens a file, auto-detecting gzip from extension.

    Parameters
    ----------
    path : str
        File path.  If it ends with ``.gz``, gzip compression is used
        unless *compress* is explicitly ``False``.
    mode : str
        Open mode (``"w"`` or ``"wb"``).
    encoding : str
        Text encoding (ignored for binary modes).
    compress : bool | None
        Force gzip on/off.  ``None`` = auto-detect from extension.
    newline : str | None
        Newline mode for text files (e.g. ``""`` for CSV).
    """
    use_gzip = compress if compress is not None else path.endswith(".gz")
    if use_gzip:
        # gzip.open returns a binary stream; wrap in TextIOWrapper for text mode
        if "b" not in mode:
            raw = _gzip.open(path, mode + "b")
            f: Any = _io.TextIOWrapper(raw, encoding=encoding, newline=newline)  # type: ignore[arg-type]
            try:
                yield f
            finally:
                f.close()  # closes underlying raw too
        else:
            f = _gzip.open(path, mode)  # type: ignore[assignment]
            try:
                yield f
            finally:
                f.close()
    else:
        f = open(path, mode, encoding=encoding, newline=newline)  # type: ignore[assignment]
        try:
            yield f
        finally:
            f.close()


class Schema:
    """Pre-resolved generation blueprint for maximum throughput.

    All field lookups are performed **once** during ``__init__``.
    Subsequent ``generate()`` calls execute only the bound methods
    with zero overhead from name resolution.

    Values are preserved in their native Python types by default.
    Export methods (``to_csv``, ``to_jsonl``, ``to_sql``) convert
    values to strings as needed by the output format.

    Parameters
    ----------
    forge : DataForge
        The parent generator instance.
    fields : list[str] | dict[str, str | Callable]
        Fields to generate.  String values are resolved to provider
        methods.  Callable values receive the current row dict and
        can reference previously generated columns.
    null_fields : dict[str, float] | None
        Optional mapping of column names to null probabilities
        (0.0–1.0).  After generation, values in the specified columns
        are randomly replaced with ``None`` at the given rate.
        Example: ``{"email": 0.2}`` makes ~20% of email values ``None``.
    unique_together : list[tuple[str, ...]] | None
        Optional list of column-name tuples that must be unique
        **in combination**.  For example,
        ``[("first_name", "last_name")]`` ensures no two rows share
        the same (first_name, last_name) pair.  Rows that violate
        the constraint are re-generated up to a configurable retry
        limit.
    """

    __slots__ = (
        "_columns",
        "_callables",
        "_row_lambdas",
        "_null_fields",
        "_rng",
        "_unique_together",
        "_unique_together_indices",
        "_fields_spec",
        "_chaos",
        "_constraints",
        "_independent_cols",
        "_dependent_order",
        "_forge_ref",
    )

    def __init__(
        self,
        forge: DataForge,
        fields: "list[str] | dict[str, Any]",
        null_fields: "dict[str, float] | None" = None,
        unique_together: "list[tuple[str, ...]] | None" = None,
        chaos: "Any | None" = None,
    ) -> None:
        # Check for dict-based field specs (constraint engine)
        has_dict_specs = False
        if isinstance(fields, dict):
            for v in fields.values():
                if isinstance(v, dict):
                    has_dict_specs = True
                    break

        # Only store forge ref and chaos when actually needed — avoids
        # extra attribute assignments in the common (standard) path.
        self._forge_ref = forge if (has_dict_specs or chaos is not None) else None  # type: ignore[assignment]
        self._chaos = chaos

        if has_dict_specs:
            # Use constraint engine for two-pass generation
            from dataforge.constraints import build_dependency_order

            independent, dependent_order, constraint_map = build_dependency_order(
                fields  # type: ignore[arg-type]
            )

            # Build columns and callables for independent columns only
            columns: list[str] = []
            callables: list[object] = []
            row_lambdas: dict[int, Callable[..., Any]] = {}

            for col_name in independent:
                spec = fields[col_name]  # type: ignore[index]
                if isinstance(spec, dict):
                    field_name = spec.get("field", col_name)
                elif callable(spec):
                    idx = len(columns)
                    columns.append(col_name)
                    callables.append(_ROW_LAMBDA)
                    row_lambdas[idx] = spec
                    continue
                else:
                    field_name = spec
                provider_attr, method_name = forge._resolve_field(field_name)
                provider = getattr(forge, provider_attr)
                method = getattr(provider, method_name)
                columns.append(col_name)
                callables.append(method)

            # Add placeholders for dependent columns (filled per-row)
            for col_name, _constraint in dependent_order:
                columns.append(col_name)
                callables.append(_ROW_LAMBDA)
                # Don't add to row_lambdas — handled by constraint engine

            self._columns = tuple(columns)
            self._callables = tuple(callables)
            self._row_lambdas = row_lambdas
            self._independent_cols: tuple[str, ...] = tuple(independent)
            self._dependent_order = dependent_order
            self._constraints = constraint_map
        else:
            # Standard path — no constraints
            # Normalize to (column_name, field_spec) pairs
            if isinstance(fields, list):
                field_defs: list[tuple[str, str | Callable[..., Any]]] = [
                    (f, f) for f in fields
                ]
            else:
                field_defs = list(fields.items())

            columns = []
            callables = []
            row_lambdas = {}

            for idx, (col_name, field_spec) in enumerate(field_defs):
                columns.append(col_name)
                if callable(field_spec):
                    # Row-dependent lambda — stored separately, executed
                    # per-row after batch columns are generated.
                    callables.append(_ROW_LAMBDA)
                    row_lambdas[idx] = field_spec
                else:
                    # String field name — resolve to provider method
                    provider_attr, method_name = forge._resolve_field(field_spec)
                    provider = getattr(forge, provider_attr)
                    method = getattr(provider, method_name)
                    callables.append(method)

            # Store as tuples for fastest iteration (bytecode LOAD_FAST)
            self._columns = tuple(columns)
            self._callables = tuple(callables)
            self._row_lambdas = row_lambdas
            # Standard path: use None sentinels — avoids creating
            # empty containers and saves 3 allocations per Schema.
            self._independent_cols = None  # type: ignore[assignment]
            self._dependent_order = None  # type: ignore[assignment]
            self._constraints = None  # type: ignore[assignment]

        # Remember the original field spec for schema serialization
        self._fields_spec: list[str] | dict[str, Any] = fields

        # Nullable field support: store (column_index, probability) pairs
        # and the RNG for fast null injection
        self._rng = forge._engine._rng
        if null_fields:
            # Validate all column names exist
            col_set = set(columns)
            for name in null_fields:
                if name not in col_set:
                    raise ValueError(
                        f"null_fields key '{name}' is not a column in this Schema. "
                        f"Available columns: {list(columns)}"
                    )
            self._null_fields: dict[int, float] = {
                columns.index(name): prob
                for name, prob in null_fields.items()
                if 0.0 < prob <= 1.0
            }
        else:
            self._null_fields = {}

        # unique_together: pre-compute column index tuples for fast lookup
        if unique_together:
            col_set = set(columns)
            idx_groups: list[tuple[int, ...]] = []
            for group in unique_together:
                for name in group:
                    if name not in col_set:
                        raise ValueError(
                            f"unique_together column '{name}' is not in this Schema. "
                            f"Available columns: {list(columns)}"
                        )
                idx_groups.append(tuple(columns.index(name) for name in group))
            self._unique_together: list[tuple[int, ...]] = idx_groups
            self._unique_together_indices = idx_groups
        else:
            self._unique_together = []
            self._unique_together_indices = []

    # ------------------------------------------------------------------
    # Core generation
    # ------------------------------------------------------------------

    def _generate_columns(self, count: int) -> list[list[Any]]:
        """Generate column data in bulk (column-first).

        Shared by :meth:`generate`, :meth:`stream`, and export helpers.
        Each field is generated in one batch call via its ``count=N``
        path — no per-row field resolution overhead.

        Values are preserved in their native Python types.  No ``str()``
        coercion is applied — that responsibility belongs to export
        methods that need string output.

        Row-lambda columns are filled with ``None`` here and
        populated later by :meth:`_apply_row_lambdas`.

        Parameters
        ----------
        count : int
            Number of values per column.

        Returns
        -------
        list[list[Any]]
        """
        col_data: list[list[Any]] = []
        _sentinel = _ROW_LAMBDA
        for fn in self._callables:
            if fn is _sentinel:
                # Placeholder — filled by _apply_row_lambdas
                col_data.append([None] * count)
            elif count == 1:
                val = fn()  # type: ignore[operator]
                col_data.append([val])
            else:
                values = fn(count=count)  # type: ignore[operator]
                col_data.append(values if isinstance(values, list) else [values])

        # Apply null injection if any null_fields are configured
        null_fields = self._null_fields
        if null_fields:
            _rng = self._rng
            for col_idx, prob in null_fields.items():
                col = col_data[col_idx]
                # Use bulk index selection via sample() instead of
                # per-element random() — fewer Python-level calls.
                n_nulls = _rng.binomialvariate(count, prob)
                if n_nulls > 0:
                    for i in _rng.sample(range(count), k=min(n_nulls, count)):
                        col[i] = None

        return col_data

    @staticmethod
    def _stringify_columns(col_data: list[list[Any]]) -> list[list[str]]:
        """Convert column data to strings for text-based exports.

        Optimized: skips conversion for columns that are already all
        strings (the common case for most providers).

        Parameters
        ----------
        col_data : list[list[Any]]
            Native-typed column data.

        Returns
        -------
        list[list[str]]
        """
        result: list[list[str]] = []
        _str = str
        _isinstance = isinstance
        for col in col_data:
            if col and _isinstance(col[0], _str):
                result.append(col)  # type: ignore[arg-type]
            elif not col or col[0] is None:
                # First element is None or column is empty — must stringify
                # all elements to be safe.
                result.append([_str(v) if v is not None else "" for v in col])
            else:
                result.append([_str(v) if v is not None else "" for v in col])
        return result

    def _generate_string_columns(self, count: int) -> list[list[str]]:
        """Generate columns and stringify them, handling row lambdas.

        Shared helper used by CSV, Parquet, Arrow, and Polars exports.
        Avoids duplicating the generate→lambda→stringify pattern in
        every export method.

        Returns
        -------
        list[list[str]]
            Stringified column data.
        """
        columns = self._columns
        col_data = self._generate_columns(count)
        _str = str
        if self._row_lambdas:
            batch_rows = [dict(zip(columns, row)) for row in zip(*col_data)]
            self._apply_row_lambdas(batch_rows)
            return [
                [_str(row[c]) if row[c] is not None else "" for row in batch_rows]
                for c in columns
            ]
        return self._stringify_columns(col_data)

    def _apply_row_lambdas(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply row-dependent lambdas to generated rows in-place.

        Each lambda receives the current row dict and its return
        value is stored in the row with its native type.
        Lambdas are applied in column order, so later lambdas
        can reference earlier lambda-generated columns.
        """
        if not self._row_lambdas:
            return rows
        columns = self._columns
        for row in rows:
            for idx, fn in self._row_lambdas.items():
                row[columns[idx]] = fn(row)
        return rows

    def generate(self, count: int = 10) -> list[dict[str, Any]]:
        """Generate *count* rows as a list of dicts.

        Uses **column-first generation**: each field is generated in
        bulk via its ``count=N`` batch path, then columns are zipped
        into row dicts. This replaces ``count × num_fields`` scalar
        calls with ``num_fields`` batch calls — significantly faster
        for large counts.

        Values are preserved in their native Python types (``int``,
        ``float``, ``bool``, ``str``, etc.).

        When ``unique_together`` constraints are active, duplicate
        combinations are detected and replaced with freshly generated
        rows (up to 100 retry rounds).

        Parameters
        ----------
        count : int
            Number of rows to generate.

        Returns
        -------
        list[dict[str, Any]]
        """
        if count == 0:
            return []

        columns = self._columns
        col_data = self._generate_columns(count)

        # Zip columns into row dicts — transposed vectorized assembly
        rows = [dict(zip(columns, row)) for row in zip(*col_data)]
        rows = self._apply_row_lambdas(rows)

        # Apply constraint-based dependent columns (two-pass)
        dep_order = self._dependent_order
        if dep_order:
            engine = self._forge_ref._engine
            forge = self._forge_ref
            for row in rows:
                for col_name, constraint in dep_order:
                    row[col_name] = constraint.generate(row, engine, forge)

        # Enforce unique_together constraints
        if self._unique_together:
            rows = self._enforce_unique_together(rows, count)

        # Apply chaos transformer if configured
        chaos = self._chaos
        if chaos is not None:
            rows = self._apply_chaos(rows)

        return rows

    def _apply_chaos(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply chaos/data-quality transformations to generated rows."""
        chaos = self._chaos
        if chaos is None:
            return rows
        # Accept ChaosTransformer instance or config dict
        if isinstance(chaos, dict):
            from dataforge.chaos import ChaosTransformer

            transformer = ChaosTransformer(**chaos)
        else:
            transformer = chaos
        return transformer.transform(rows)

    def _enforce_unique_together(
        self, rows: list[dict[str, Any]], target: int
    ) -> list[dict[str, Any]]:
        """Re-generate rows until all unique_together constraints are met.

        Optimized: maintains a persistent ``seen`` set across rounds
        so only replacement rows need to be re-checked, avoiding a
        full table rescan every round.
        """
        columns = self._columns
        _MAX_ROUNDS = 100

        # Build persistent seen sets — one per constraint group.
        # These survive across rounds so we only re-check new rows.
        seen_per_group: list[set[tuple[Any, ...]]] = [
            set() for _ in self._unique_together_indices
        ]

        for _round in range(_MAX_ROUNDS):
            all_ok = True
            for g_idx, idx_group in enumerate(self._unique_together_indices):
                seen = seen_per_group[g_idx]
                seen.clear()
                dup_indices: list[int] = []
                for i, row in enumerate(rows):
                    key = tuple(row[columns[j]] for j in idx_group)
                    if key in seen:
                        dup_indices.append(i)
                    else:
                        seen.add(key)

                if dup_indices:
                    all_ok = False
                    # Re-generate only the duplicate rows
                    n_dups = len(dup_indices)
                    new_col_data = self._generate_columns(n_dups)
                    new_rows = [dict(zip(columns, row)) for row in zip(*new_col_data)]
                    new_rows = self._apply_row_lambdas(new_rows)
                    for i, new_row in zip(dup_indices, new_rows):
                        rows[i] = new_row

            if all_ok:
                return rows

        # After max rounds, return what we have (best-effort)
        import warnings

        warnings.warn(
            f"unique_together constraint could not be fully satisfied "
            f"after {_MAX_ROUNDS} rounds. Some duplicate combinations "
            f"may remain.",
            UserWarning,
            stacklevel=3,
        )
        return rows

    def stream(
        self,
        count: int,
        batch_size: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield rows lazily in batches — avoids materializing all rows.

        Internally generates data in column-first batches for
        performance, but yields rows one at a time.

        Parameters
        ----------
        count : int
            Total number of rows to yield.
        batch_size : int | None
            Internal batch size for column-first generation.
            When ``None`` (default), the batch size is auto-tuned
            based on the number of columns and total count to
            balance throughput and memory usage.

        Yields
        ------
        dict[str, Any]
        """
        columns = self._columns
        num_cols = len(columns)

        # Auto-tune batch size when not explicitly set.
        # More columns → smaller batches to bound memory; fewer columns
        # → larger batches to amortize per-batch overhead.  The floor
        # of 1000 keeps overhead low; the ceiling avoids over-allocating
        # when count is small.
        if batch_size is None:
            batch_size = min(count, max(1000, 100_000 // max(num_cols, 1)))

        remaining = count

        row_lambdas = self._row_lambdas
        while remaining > 0:
            chunk = min(remaining, batch_size)
            col_data = self._generate_columns(chunk)
            # Yield row dicts — transposed vectorized assembly
            if row_lambdas:
                batch_rows = [dict(zip(columns, row)) for row in zip(*col_data)]
                self._apply_row_lambdas(batch_rows)
                yield from batch_rows
            else:
                for row in zip(*col_data):
                    yield dict(zip(columns, row))
            remaining -= chunk

    async def async_stream(
        self,
        count: int,
        batch_size: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield rows lazily via ``async for`` — one row at a time.

        Internally uses the same column-first batch generation as
        :meth:`stream` for maximum throughput.  Each batch is generated
        synchronously (CPU-bound work), then rows are yielded with an
        ``await``-compatible suspend point between batches so the event
        loop can service other coroutines.

        Usage::

            async for row in schema.async_stream(100_000):
                await process(row)

        Parameters
        ----------
        count : int
            Total number of rows to yield.
        batch_size : int | None
            Internal batch size.  Auto-tuned when ``None``.

        Yields
        ------
        dict[str, Any]
        """
        import asyncio

        columns = self._columns
        num_cols = len(columns)

        if batch_size is None:
            batch_size = min(count, max(1000, 100_000 // max(num_cols, 1)))

        remaining = count
        row_lambdas = self._row_lambdas
        _sleep = asyncio.sleep

        while remaining > 0:
            chunk = min(remaining, batch_size)
            col_data = self._generate_columns(chunk)
            if row_lambdas:
                batch_rows = [dict(zip(columns, row)) for row in zip(*col_data)]
                self._apply_row_lambdas(batch_rows)
                for row in batch_rows:
                    yield row
            else:
                for row in zip(*col_data):
                    yield dict(zip(columns, row))
            remaining -= chunk
            # Yield control to the event loop between batches
            await _sleep(0)

    # ------------------------------------------------------------------
    # Export helpers
    # ------------------------------------------------------------------

    def to_csv(
        self,
        count: int = 10,
        path: str | None = None,
        delimiter: str = ",",
        encoding: str = "utf-8",
        compress: bool | None = None,
    ) -> str:
        """Generate rows and return as CSV string.

        Parameters
        ----------
        count : int
            Number of rows.
        path : str | None
            Optional file path to write.
        delimiter : str
            Field delimiter (default: comma).
        encoding : str
            Character encoding for file output (default: utf-8).
        compress : bool | None
            If ``True``, gzip the output file.  ``None`` auto-detects
            from a ``.gz`` file extension.

        Returns
        -------
        str
        """
        import csv
        import io

        if count == 0:
            return ""

        columns = self._columns
        col_data = self._generate_columns(count)

        buf = io.StringIO()
        writer = csv.writer(buf, delimiter=delimiter)
        writer.writerow(columns)

        _str = str
        if self._row_lambdas:
            # Row-lambda path: must materialize rows
            rows = [dict(zip(columns, row)) for row in zip(*col_data)]
            self._apply_row_lambdas(rows)
            writer.writerows(
                [_str(row[c]) if row[c] is not None else "" for c in columns]
                for row in rows
            )
        else:
            str_data = self._stringify_columns(col_data)
            writer.writerows(zip(*str_data))

        content = buf.getvalue()

        if path is not None:
            with _open_file(
                path, "w", encoding=encoding, compress=compress, newline=""
            ) as f:
                f.write(content)

        return content

    def stream_to_csv(
        self,
        path: str,
        count: int = 10,
        batch_size: int | None = None,
        delimiter: str = ",",
        encoding: str = "utf-8",
        compress: bool | None = None,
    ) -> int:
        """Stream rows to a CSV file without materializing all data.

        Writes rows in batches to keep memory usage constant
        regardless of *count*.

        Parameters
        ----------
        path : str
            File path to write.
        count : int
            Total number of rows.
        batch_size : int | None
            Internal batch size.  Auto-tuned when ``None``.
        delimiter : str
            Field delimiter (default: comma).
        encoding : str
            Character encoding (default: utf-8).
        compress : bool | None
            If ``True``, gzip the output.  ``None`` auto-detects
            from a ``.gz`` file extension.

        Returns
        -------
        int
            Number of rows written.
        """
        import csv

        columns = self._columns
        num_cols = len(columns)

        if batch_size is None:
            batch_size = min(count, max(1000, 100_000 // max(num_cols, 1)))

        written = 0
        with _open_file(
            path, "w", encoding=encoding, compress=compress, newline=""
        ) as f:
            writer = csv.writer(f, delimiter=delimiter)
            writer.writerow(columns)

            remaining = count
            while remaining > 0:
                chunk = min(remaining, batch_size)
                str_data = self._generate_string_columns(chunk)
                writer.writerows(zip(*str_data))
                written += chunk
                remaining -= chunk

        return written

    def to_jsonl(
        self,
        count: int = 10,
        path: str | None = None,
        encoding: str = "utf-8",
        compress: bool | None = None,
    ) -> str:
        """Generate rows and return as JSON Lines string.

        Values are serialized in their native types — integers stay
        as numbers, booleans as ``true``/``false``, etc.

        Parameters
        ----------
        count : int
            Number of rows.
        path : str | None
            Optional file path to write.
        encoding : str
            Character encoding for file output (default: utf-8).
        compress : bool | None
            If ``True``, gzip the output file.  ``None`` auto-detects
            from a ``.gz`` file extension.

        Returns
        -------
        str
        """
        import json

        rows = self.generate(count)
        _dumps = json.dumps
        # Build final string with trailing newline in one pass —
        # avoids an extra string copy from ``content += "\n"``.
        if not rows:
            return ""
        content = "\n".join(_dumps(row, ensure_ascii=False) for row in rows) + "\n"

        if path is not None:
            with _open_file(path, "w", encoding=encoding, compress=compress) as f:
                f.write(content)

        return content

    def to_json(
        self,
        count: int = 10,
        path: str | None = None,
        indent: int = 2,
        encoding: str = "utf-8",
        compress: bool | None = None,
    ) -> str:
        """Generate rows and return as a JSON array string.

        Parameters
        ----------
        count : int
            Number of rows.
        path : str | None
            Optional file path to write.
        indent : int
            JSON indentation level (default: 2).
        encoding : str
            Character encoding for file output (default: utf-8).
        compress : bool | None
            If ``True``, gzip the output file.  ``None`` auto-detects
            from a ``.gz`` file extension.

        Returns
        -------
        str
        """
        import json

        rows = self.generate(count)
        content = json.dumps(rows, indent=indent, ensure_ascii=False)

        if path is not None:
            with _open_file(path, "w", encoding=encoding, compress=compress) as f:
                f.write(content)

        return content

    def stream_to_jsonl(
        self,
        path: str,
        count: int = 10,
        batch_size: int | None = None,
        encoding: str = "utf-8",
        compress: bool | None = None,
    ) -> int:
        """Stream rows to a JSON Lines file without materializing all data.

        Writes rows in batches to keep memory usage constant
        regardless of *count*.

        Parameters
        ----------
        path : str
            File path to write.
        count : int
            Total number of rows.
        batch_size : int | None
            Internal batch size.  Auto-tuned when ``None``.
        encoding : str
            Character encoding (default: utf-8).
        compress : bool | None
            If ``True``, gzip the output.  ``None`` auto-detects
            from a ``.gz`` file extension.

        Returns
        -------
        int
            Number of rows written.
        """
        import json

        columns = self._columns
        num_cols = len(columns)

        if batch_size is None:
            batch_size = min(count, max(1000, 100_000 // max(num_cols, 1)))

        _dumps = json.dumps
        written = 0
        row_lambdas = self._row_lambdas
        with _open_file(path, "w", encoding=encoding, compress=compress) as f:
            _write = f.write
            remaining = count
            while remaining > 0:
                chunk = min(remaining, batch_size)
                col_data = self._generate_columns(chunk)
                if row_lambdas:
                    batch_rows = [dict(zip(columns, row)) for row in zip(*col_data)]
                    self._apply_row_lambdas(batch_rows)
                    # Buffer entire batch into a single write call
                    _write(
                        "\n".join(_dumps(row, ensure_ascii=False) for row in batch_rows)
                        + "\n"
                    )
                else:
                    # Buffer entire batch — single write per batch instead
                    # of 2× write per row (data + newline).
                    _write(
                        "\n".join(
                            _dumps(dict(zip(columns, row)), ensure_ascii=False)
                            for row in zip(*col_data)
                        )
                        + "\n"
                    )
                written += chunk
                remaining -= chunk

        return written

    def to_sql(
        self,
        table: str,
        count: int = 10,
        dialect: str = "sqlite",
        path: str | None = None,
        encoding: str = "utf-8",
        compress: bool | None = None,
    ) -> str:
        """Generate rows and return as SQL INSERT statements.

        Parameters
        ----------
        table : str
            Target table name.
        count : int
            Number of rows.
        dialect : str
            SQL dialect: ``"sqlite"``, ``"mysql"``, or ``"postgresql"``.
        path : str | None
            If provided, write SQL to this file path.
        encoding : str
            Character encoding for file output (default: utf-8).
        compress : bool | None
            If ``True``, gzip the output file.  ``None`` auto-detects
            from a ``.gz`` file extension.

        Returns
        -------
        str
        """
        rows = self.generate(count)
        if not rows:
            return ""

        columns = self._columns

        # Quote identifiers per dialect
        if dialect == "mysql":
            col_list = ", ".join(f"`{c}`" for c in columns)
            tbl = f"`{table}`"
        else:  # sqlite, postgresql — both use double quotes
            col_list = ", ".join(f'"{c}"' for c in columns)
            tbl = f'"{table}"'

        # Multi-row INSERT: batch 1000 rows per statement for
        # significantly fewer SQL statements and better throughput.
        _BATCH = 1000
        prefix = f"INSERT INTO {tbl} ({col_list}) VALUES"
        parts: list[str] = []
        _str = str
        for batch_start in range(0, len(rows), _BATCH):
            batch = rows[batch_start : batch_start + _BATCH]
            value_rows = []
            for row in batch:
                vals = ", ".join(
                    "NULL"
                    if row[c] is None
                    else "'" + _str(row[c]).replace("'", "''") + "'"
                    for c in columns
                )
                value_rows.append(f"({vals})")
            parts.append(f"{prefix}\n" + ",\n".join(value_rows) + ";")

        content = "\n".join(parts) + "\n"

        if path is not None:
            with _open_file(path, "w", encoding=encoding, compress=compress) as f:
                f.write(content)

        return content

    def to_dataframe(self, count: int = 10) -> "Any":
        """Generate rows as a pandas DataFrame.

        Requires ``pandas`` to be installed.

        Parameters
        ----------
        count : int
            Number of rows.

        Returns
        -------
        pandas.DataFrame
        """
        try:
            import pandas as pd
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "pandas is required for to_dataframe(). "
                "Install it with: pip install pandas"
            ) from exc

        columns = self._columns
        col_data = self._generate_columns(count)

        if self._row_lambdas:
            # Row-lambda path: must materialize rows for inter-column refs
            rows = [dict(zip(columns, row)) for row in zip(*col_data)]
            rows = self._apply_row_lambdas(rows)
            if self._unique_together:
                rows = self._enforce_unique_together(rows, count)
            return pd.DataFrame(rows)

        # Null injection already applied inside _generate_columns.
        # Build DataFrame directly from columnar data — avoids double
        # transposition (col→row dicts→DataFrame re-columnarizes).
        return pd.DataFrame(dict(zip(columns, col_data)))

    def to_parquet(
        self,
        path: str,
        count: int = 10,
        batch_size: int | None = None,
    ) -> int:
        """Generate rows and write as a Parquet file.

        Uses **PyArrow** for zero-copy columnar writes.  Data is
        generated in batches and written as row-groups so memory
        stays bounded even for very large counts.

        Requires ``pyarrow`` to be installed.

        Parameters
        ----------
        path : str
            File path to write.
        count : int
            Number of rows.
        batch_size : int | None
            Rows per row-group.  Auto-tuned when ``None``.

        Returns
        -------
        int
            Number of rows written.
        """
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "pyarrow is required for to_parquet(). "
                "Install it with: pip install pyarrow"
            ) from exc

        columns = self._columns
        num_cols = len(columns)

        if batch_size is None:
            batch_size = min(count, max(1000, 100_000 // max(num_cols, 1)))

        schema = pa.schema([(col, pa.string()) for col in columns])
        written = 0

        with pq.ParquetWriter(path, schema) as writer:
            remaining = count
            while remaining > 0:
                chunk = min(remaining, batch_size)
                str_data = self._generate_string_columns(chunk)
                arrays = [pa.array(col, type=pa.string()) for col in str_data]
                batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
                writer.write_batch(batch)
                written += chunk
                remaining -= chunk

        return written

    # ------------------------------------------------------------------
    # Schema serialization
    # ------------------------------------------------------------------

    def to_schema_dict(self, count: int = 10) -> dict[str, Any]:
        """Export this schema's definition as a serializable dict.

        The returned dict can be saved to JSON/YAML/TOML via
        :func:`dataforge.schema_io.save_schema` and later loaded
        to recreate an equivalent schema.

        Callable (lambda) fields are **not** serializable and are
        silently omitted.

        Parameters
        ----------
        count : int
            Default row count to include in the dict.

        Returns
        -------
        dict[str, Any]
        """
        from dataforge.schema_io import schema_to_dict

        fields = self._fields_spec
        # Filter out lambdas — they can't be serialized
        if isinstance(fields, dict):
            serializable: list[str] | dict[str, str] = {
                k: v for k, v in fields.items() if isinstance(v, str)
            }
        else:
            serializable = list(fields)

        # Reverse-map null_fields from index → column name
        null_fields: dict[str, float] | None = None
        if self._null_fields:
            columns = self._columns
            null_fields = {
                columns[idx]: prob for idx, prob in self._null_fields.items()
            }

        # Reverse-map unique_together from index tuples → name tuples
        unique_together: list[tuple[str, ...]] | None = None
        if self._unique_together:
            columns = self._columns
            unique_together = [
                tuple(columns[i] for i in group) for group in self._unique_together
            ]

        return schema_to_dict(
            fields=serializable,
            count=count,
            null_fields=null_fields,
            unique_together=unique_together,
        )

    def save_schema(
        self,
        path: str,
        count: int = 10,
        format: str | None = None,
    ) -> None:
        """Save this schema's definition to a file.

        Supports JSON, YAML, and TOML formats.  The format is
        auto-detected from the file extension when *format* is
        ``None``.

        Parameters
        ----------
        path : str
            File path to write.
        count : int
            Default row count to include in the definition.
        format : str | None
            Output format (``"json"``, ``"yaml"``, ``"toml"``).
            Auto-detected from extension when ``None``.
        """
        from dataforge.schema_io import save_schema

        d = self.to_schema_dict(count=count)
        save_schema(d, path, format=format)

    def __repr__(self) -> str:
        return f"Schema(columns={list(self._columns)!r})"

    # ------------------------------------------------------------------
    # Arrow / Polars output
    # ------------------------------------------------------------------

    def to_arrow(
        self,
        count: int = 10,
        batch_size: int | None = None,
    ) -> "Any":
        """Generate rows and return as a PyArrow Table.

        Uses **column-first generation** directly into Arrow arrays —
        no intermediate row-dict materialisation.  This is the fastest
        bulk export path because the data never leaves columnar form.

        When *count* exceeds *batch_size*, data is generated in batches
        and concatenated via ``pyarrow.concat_tables`` for bounded
        memory usage during generation.

        Requires ``pyarrow`` to be installed.

        Parameters
        ----------
        count : int
            Number of rows.
        batch_size : int | None
            Rows per internal batch.  Auto-tuned when ``None``.

        Returns
        -------
        pyarrow.Table
        """
        try:
            import pyarrow as pa
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "pyarrow is required for to_arrow(). "
                "Install it with: pip install pyarrow"
            ) from exc

        columns = self._columns
        num_cols = len(columns)

        if batch_size is None:
            batch_size = min(count, max(1000, 100_000 // max(num_cols, 1)))

        schema = pa.schema([(col, pa.string()) for col in columns])

        if count <= batch_size:
            # Single-shot: no concat overhead
            str_data = self._generate_string_columns(count)
            arrays = [pa.array(col, type=pa.string()) for col in str_data]
            return pa.table(dict(zip(columns, arrays)), schema=schema)

        # Multi-batch: generate batches → concat
        batches: list[Any] = []
        remaining = count
        while remaining > 0:
            chunk = min(remaining, batch_size)
            str_data = self._generate_string_columns(chunk)
            arrays = [pa.array(col, type=pa.string()) for col in str_data]
            batches.append(pa.record_batch(arrays, schema=schema))
            remaining -= chunk

        return pa.Table.from_batches(batches, schema=schema)

    def to_polars(
        self,
        count: int = 10,
        batch_size: int | None = None,
    ) -> "Any":
        """Generate rows and return as a Polars DataFrame.

        Uses **column-first generation** directly into Polars Series —
        no intermediate row-dict materialisation.  This is significantly
        faster than converting via pandas because we skip the pandas
        intermediate entirely.

        When *count* exceeds *batch_size*, data is generated in batches
        and concatenated via ``polars.concat`` for bounded memory.

        Requires ``polars`` to be installed.

        Parameters
        ----------
        count : int
            Number of rows.
        batch_size : int | None
            Rows per internal batch.  Auto-tuned when ``None``.

        Returns
        -------
        polars.DataFrame
        """
        try:
            import polars as pl
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "polars is required for to_polars(). "
                "Install it with: pip install polars"
            ) from exc

        columns = self._columns
        num_cols = len(columns)

        if batch_size is None:
            batch_size = min(count, max(1000, 100_000 // max(num_cols, 1)))

        if count <= batch_size:
            str_data = self._generate_string_columns(count)
            return pl.DataFrame(
                {col: data for col, data in zip(columns, str_data)},
                schema={col: pl.Utf8 for col in columns},
            )

        # Multi-batch: generate batches → concat
        frames: list[Any] = []
        remaining = count
        while remaining > 0:
            chunk = min(remaining, batch_size)
            str_data = self._generate_string_columns(chunk)
            frames.append(
                pl.DataFrame(
                    {col: data for col, data in zip(columns, str_data)},
                    schema={col: pl.Utf8 for col in columns},
                )
            )
            remaining -= chunk

        return pl.concat(frames)

    # ------------------------------------------------------------------
    # Streaming to message queues
    # ------------------------------------------------------------------

    def stream_to(
        self,
        emitter: Any,
        count: int = 1000,
        batch_size: int = 100,
        rate_limit: float | None = None,
    ) -> int:
        """Stream generated data to an emitter (HTTP, Kafka, RabbitMQ).

        Parameters
        ----------
        emitter : StreamEmitter
            The target emitter instance.
        count : int
            Total rows to emit.
        batch_size : int
            Rows per batch.
        rate_limit : float | None
            Max rows per second.  ``None`` = unlimited.

        Returns
        -------
        int
            Number of rows emitted.
        """
        from dataforge.streaming import stream_batch_to_emitter, TokenBucketRateLimiter

        limiter = None
        if rate_limit is not None:
            limiter = TokenBucketRateLimiter(rate=rate_limit, burst=max(1, batch_size))
        return stream_batch_to_emitter(
            self, emitter, count=count, batch_size=batch_size, rate_limiter=limiter
        )

    def stream_to_http(
        self,
        url: str,
        count: int = 1000,
        batch_size: int = 100,
        headers: dict[str, str] | None = None,
        rate_limit: float | None = None,
    ) -> int:
        """Stream generated data to an HTTP endpoint via POST.

        Parameters
        ----------
        url : str
            Target URL.
        count : int
            Total rows.
        batch_size : int
            Rows per batch POST.
        headers : dict | None
            Additional HTTP headers.
        rate_limit : float | None
            Max rows per second.

        Returns
        -------
        int
        """
        from dataforge.streaming import HttpEmitter

        emitter = HttpEmitter(url=url, headers=headers, batch_mode=True)
        return self.stream_to(
            emitter, count=count, batch_size=batch_size, rate_limit=rate_limit
        )

    def stream_to_kafka(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "dataforge",
        count: int = 1000,
        batch_size: int = 100,
        rate_limit: float | None = None,
    ) -> int:
        """Stream generated data to a Kafka topic.

        Requires ``confluent-kafka``.

        Parameters
        ----------
        bootstrap_servers : str
            Kafka bootstrap servers.
        topic : str
            Kafka topic.
        count : int
            Total rows.
        batch_size : int
            Rows per batch.
        rate_limit : float | None
            Max rows per second.

        Returns
        -------
        int
        """
        from dataforge.streaming import KafkaEmitter

        emitter = KafkaEmitter(bootstrap_servers=bootstrap_servers, topic=topic)
        return self.stream_to(
            emitter, count=count, batch_size=batch_size, rate_limit=rate_limit
        )
