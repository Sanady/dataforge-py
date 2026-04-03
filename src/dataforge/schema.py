"""Schema — zero-overhead bulk data generation via pre-resolved fields."""

from __future__ import annotations

import gzip as _gzip
import io as _io
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dataforge.core import DataForge

_ROW_LAMBDA = object()


@contextmanager
def _open_file(
    path: str,
    mode: str = "w",
    encoding: str = "utf-8",
    compress: bool | None = None,
    newline: str | None = None,
) -> Iterator[Any]:
    """Context manager that opens a file, auto-detecting gzip from extension."""
    use_gzip = compress if compress is not None else path.endswith(".gz")
    if use_gzip:
        if "b" not in mode:
            raw = _gzip.open(path, mode + "b")
            f: Any = _io.TextIOWrapper(raw, encoding=encoding, newline=newline)  # type: ignore[arg-type]
            try:
                yield f
            finally:
                f.close()
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
    """Pre-resolved generation blueprint for maximum throughput."""

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
        has_dict_specs = False
        if isinstance(fields, dict):
            for v in fields.values():
                if isinstance(v, dict):
                    has_dict_specs = True
                    break

        self._forge_ref = forge if (has_dict_specs or chaos is not None) else None  # type: ignore[assignment]
        self._chaos = chaos

        if has_dict_specs:
            from dataforge.constraints import build_dependency_order

            independent, dependent_order, constraint_map = build_dependency_order(
                fields  # type: ignore[arg-type]
            )

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

            for col_name, _constraint in dependent_order:
                columns.append(col_name)
                callables.append(_ROW_LAMBDA)

            self._columns = tuple(columns)
            self._callables = tuple(callables)
            self._row_lambdas = row_lambdas
            self._independent_cols: tuple[str, ...] = tuple(independent)
            self._dependent_order = dependent_order
            self._constraints = constraint_map
        else:
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
                    callables.append(_ROW_LAMBDA)
                    row_lambdas[idx] = field_spec
                else:
                    provider_attr, method_name = forge._resolve_field(field_spec)
                    provider = getattr(forge, provider_attr)
                    method = getattr(provider, method_name)
                    callables.append(method)

            self._columns = tuple(columns)
            self._callables = tuple(callables)
            self._row_lambdas = row_lambdas
            self._independent_cols = None  # type: ignore[assignment]
            self._dependent_order = None  # type: ignore[assignment]
            self._constraints = None  # type: ignore[assignment]

        self._fields_spec: list[str] | dict[str, Any] = fields

        self._rng = forge._engine._rng
        if null_fields:
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

    def _generate_columns(self, count: int) -> list[list[Any]]:
        """Generate column data in bulk (column-first)."""
        col_data: list[list[Any]] = []
        _sentinel = _ROW_LAMBDA
        for fn in self._callables:
            if fn is _sentinel:
                col_data.append([None] * count)
            elif count == 1:
                val = fn()  # type: ignore[operator]
                col_data.append([val])
            else:
                values = fn(count=count)  # type: ignore[operator]
                col_data.append(values if isinstance(values, list) else [values])

        null_fields = self._null_fields
        if null_fields:
            _rng = self._rng
            for col_idx, prob in null_fields.items():
                col = col_data[col_idx]
                n_nulls = _rng.binomialvariate(count, prob)
                if n_nulls > 0:
                    for i in _rng.sample(range(count), k=min(n_nulls, count)):
                        col[i] = None

        return col_data

    @staticmethod
    def _stringify_columns(col_data: list[list[Any]]) -> list[list[str]]:
        """Convert column data to strings for text-based exports."""
        result: list[list[str]] = []
        _str = str
        _isinstance = isinstance
        for col in col_data:
            if col and _isinstance(col[0], _str):
                result.append(col)  # type: ignore[arg-type]
            elif not col or col[0] is None:
                result.append([_str(v) if v is not None else "" for v in col])
            else:
                result.append([_str(v) if v is not None else "" for v in col])
        return result

    def _generate_string_columns(self, count: int) -> list[list[str]]:
        """Generate columns and stringify them, handling row lambdas."""
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
        """Apply row-dependent lambdas to generated rows in-place."""
        if not self._row_lambdas:
            return rows
        columns = self._columns
        for row in rows:
            for idx, fn in self._row_lambdas.items():
                row[columns[idx]] = fn(row)
        return rows

    def generate(self, count: int = 10) -> list[dict[str, Any]]:
        """Generate *count* rows as a list of dicts with native Python types."""
        if count == 0:
            return []

        columns = self._columns
        col_data = self._generate_columns(count)

        rows = [dict(zip(columns, row)) for row in zip(*col_data)]
        rows = self._apply_row_lambdas(rows)

        dep_order = self._dependent_order
        if dep_order:
            engine = self._forge_ref._engine
            forge = self._forge_ref
            for row in rows:
                for col_name, constraint in dep_order:
                    row[col_name] = constraint.generate(row, engine, forge)

        if self._unique_together:
            rows = self._enforce_unique_together(rows, count)

        chaos = self._chaos
        if chaos is not None:
            rows = self._apply_chaos(rows)

        return rows

    def _apply_chaos(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply chaos/data-quality transformations to generated rows."""
        chaos = self._chaos
        if chaos is None:
            return rows
        if isinstance(chaos, dict):
            from dataforge.chaos import ChaosTransformer

            transformer = ChaosTransformer(**chaos)
        else:
            transformer = chaos
        return transformer.transform(rows)

    def _enforce_unique_together(
        self, rows: list[dict[str, Any]], target: int
    ) -> list[dict[str, Any]]:
        """Re-generate rows until all unique_together constraints are met."""
        columns = self._columns
        _MAX_ROUNDS = 100

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
                    n_dups = len(dup_indices)
                    new_col_data = self._generate_columns(n_dups)
                    new_rows = [dict(zip(columns, row)) for row in zip(*new_col_data)]
                    new_rows = self._apply_row_lambdas(new_rows)
                    for i, new_row in zip(dup_indices, new_rows):
                        rows[i] = new_row

            if all_ok:
                return rows

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
        """Yield rows lazily in batches — avoids materializing all rows."""
        columns = self._columns
        num_cols = len(columns)

        if batch_size is None:
            batch_size = min(count, max(1000, 100_000 // max(num_cols, 1)))

        remaining = count

        row_lambdas = self._row_lambdas
        while remaining > 0:
            chunk = min(remaining, batch_size)
            col_data = self._generate_columns(chunk)
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
        """Yield rows lazily via ``async for`` — one row at a time."""
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
            await _sleep(0)

    def to_csv(
        self,
        count: int = 10,
        path: str | None = None,
        delimiter: str = ",",
        encoding: str = "utf-8",
        compress: bool | None = None,
    ) -> str:
        """Generate rows and return as CSV string."""
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
        """Stream rows to a CSV file without materializing all data."""
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
        """Generate rows and return as JSON Lines string."""
        import json

        rows = self.generate(count)
        _dumps = json.dumps
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
        """Generate rows and return as a JSON array string."""
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
        """Stream rows to a JSON Lines file without materializing all data."""
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
                    _write(
                        "\n".join(_dumps(row, ensure_ascii=False) for row in batch_rows)
                        + "\n"
                    )
                else:
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
        """Generate rows and return as SQL INSERT statements."""
        rows = self.generate(count)
        if not rows:
            return ""

        columns = self._columns

        if dialect == "mysql":
            col_list = ", ".join(f"`{c}`" for c in columns)
            tbl = f"`{table}`"
        else:
            col_list = ", ".join(f'"{c}"' for c in columns)
            tbl = f'"{table}"'

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
        """Generate rows as a pandas DataFrame."""
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
            rows = [dict(zip(columns, row)) for row in zip(*col_data)]
            rows = self._apply_row_lambdas(rows)
            if self._unique_together:
                rows = self._enforce_unique_together(rows, count)
            return pd.DataFrame(rows)

        return pd.DataFrame(dict(zip(columns, col_data)))

    def to_parquet(
        self,
        path: str,
        count: int = 10,
        batch_size: int | None = None,
    ) -> int:
        """Generate rows and write as a Parquet file via PyArrow."""
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

    def to_schema_dict(self, count: int = 10) -> dict[str, Any]:
        """Export this schema's definition as a serializable dict."""
        from dataforge.schema_io import schema_to_dict

        fields = self._fields_spec
        if isinstance(fields, dict):
            serializable: list[str] | dict[str, str] = {
                k: v for k, v in fields.items() if isinstance(v, str)
            }
        else:
            serializable = list(fields)

        null_fields: dict[str, float] | None = None
        if self._null_fields:
            columns = self._columns
            null_fields = {
                columns[idx]: prob for idx, prob in self._null_fields.items()
            }

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
        """Save this schema's definition to a file (JSON, YAML, or TOML)."""
        from dataforge.schema_io import save_schema

        d = self.to_schema_dict(count=count)
        save_schema(d, path, format=format)

    def __repr__(self) -> str:
        return f"Schema(columns={list(self._columns)!r})"

    def to_arrow(
        self,
        count: int = 10,
        batch_size: int | None = None,
    ) -> "Any":
        """Generate rows and return as a PyArrow Table."""
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
            str_data = self._generate_string_columns(count)
            arrays = [pa.array(col, type=pa.string()) for col in str_data]
            return pa.table(dict(zip(columns, arrays)), schema=schema)

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
        """Generate rows and return as a Polars DataFrame."""
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

    def stream_to(
        self,
        emitter: Any,
        count: int = 1000,
        batch_size: int = 100,
        rate_limit: float | None = None,
    ) -> int:
        """Stream generated data to an emitter (HTTP, Kafka, RabbitMQ)."""
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
        """Stream generated data to an HTTP endpoint via POST."""
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
        """Stream generated data to a Kafka topic."""
        from dataforge.streaming import KafkaEmitter

        emitter = KafkaEmitter(bootstrap_servers=bootstrap_servers, topic=topic)
        return self.stream_to(
            emitter, count=count, batch_size=batch_size, rate_limit=rate_limit
        )
