"""Database seeding — populate databases with realistic fake data."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from dataforge.core import DataForge


class DatabaseSeeder:
    """Database seeder with SQLAlchemy table introspection."""

    __slots__ = ("_forge", "_connection_string", "_echo", "_engine", "_metadata")

    def __init__(
        self,
        forge: DataForge,
        connection_string: str,
        echo: bool = False,
    ) -> None:
        self._forge = forge
        self._connection_string = connection_string
        self._echo = echo
        self._engine: Any = None
        self._metadata: Any = None

    def _get_engine(self) -> Any:
        """Lazily create and return SQLAlchemy engine."""
        if self._engine is None:
            try:
                import sqlalchemy as sa
            except ModuleNotFoundError as exc:
                raise ModuleNotFoundError(
                    "sqlalchemy is required for DatabaseSeeder. "
                    "Install it with: pip install sqlalchemy"
                ) from exc
            self._engine = sa.create_engine(self._connection_string, echo=self._echo)
        return self._engine

    def _get_metadata(self) -> Any:
        """Lazily reflect database metadata."""
        if self._metadata is None:
            import sqlalchemy as sa

            engine = self._get_engine()
            self._metadata = sa.MetaData()
            self._metadata.reflect(bind=engine)
        return self._metadata

    def _introspect_table(self, table_name: str) -> dict[str, str]:
        """Introspect a table and map columns to DataForge fields."""
        from dataforge.core import _FIELD_ALIASES, _SA_TYPE_MAP
        from dataforge.registry import get_field_map

        metadata = self._get_metadata()
        if table_name not in metadata.tables:
            raise ValueError(
                f"Table '{table_name}' not found in database. "
                f"Available: {list(metadata.tables.keys())}"
            )

        table = metadata.tables[table_name]
        field_map = get_field_map()
        mapped: dict[str, str] = {}

        for col in table.columns:
            col_name = col.name

            if col.primary_key and col.autoincrement:
                continue

            if col.foreign_keys:
                continue

            if col_name in field_map:
                mapped[col_name] = col_name
                continue

            alias = _FIELD_ALIASES.get(col_name)
            if alias and alias in field_map:
                mapped[col_name] = alias
                continue

            type_name = type(col.type).__name__
            type_field = _SA_TYPE_MAP.get(type_name)
            if type_field and type_field in field_map:
                mapped[col_name] = type_field
                continue

        return mapped

    def seed_table(
        self,
        table_name: str,
        count: int = 100,
        field_overrides: dict[str, str] | None = None,
        batch_size: int = 1000,
    ) -> int:
        """Seed a single table with fake data."""
        engine = self._get_engine()
        metadata = self._get_metadata()
        table = metadata.tables[table_name]
        dialect = engine.dialect.name

        field_map = self._introspect_table(table_name)
        if field_overrides:
            field_map.update(field_overrides)

        if not field_map:
            raise ValueError(
                f"No columns in '{table_name}' could be mapped to DataForge fields. "
                f"Use field_overrides to specify mappings."
            )

        schema = self._forge.schema(field_map)

        with engine.begin() as conn:
            self._apply_dialect_optimizations(conn, dialect, before=True)

            inserted = 0
            remaining = count
            while remaining > 0:
                chunk = min(remaining, batch_size)
                rows = schema.generate(count=chunk)
                conn.execute(table.insert(), rows)
                inserted += chunk
                remaining -= chunk

            self._apply_dialect_optimizations(conn, dialect, before=False)

        return inserted

    def seed_relational(
        self,
        tables: dict[str, dict[str, Any]],
        batch_size: int = 1000,
    ) -> dict[str, int]:
        """Seed multiple related tables with referential integrity."""
        engine = self._get_engine()
        metadata = self._get_metadata()
        dialect = engine.dialect.name

        for name, spec in tables.items():
            if "fields" not in spec:
                field_overrides = spec.get("field_overrides", {})
                detected = self._introspect_table(name)
                detected.update(field_overrides)
                spec["fields"] = detected

        rel_schema = self._forge.relational(tables)
        data = rel_schema.generate()

        result: dict[str, int] = {}
        with engine.begin() as conn:
            self._apply_dialect_optimizations(conn, dialect, before=True)

            for table_name in rel_schema._order:
                if table_name not in metadata.tables:
                    continue
                table = metadata.tables[table_name]
                rows = data[table_name]

                inserted = 0
                for batch_start in range(0, len(rows), batch_size):
                    batch = rows[batch_start : batch_start + batch_size]
                    conn.execute(table.insert(), batch)
                    inserted += len(batch)
                result[table_name] = inserted

            self._apply_dialect_optimizations(conn, dialect, before=False)

        return result

    @staticmethod
    def _apply_dialect_optimizations(
        conn: Any,
        dialect: str,
        before: bool,
    ) -> None:
        """Apply dialect-specific optimizations for bulk inserts."""
        from sqlalchemy import text

        if dialect == "mysql":
            if before:
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
                conn.execute(text("SET UNIQUE_CHECKS = 0"))
            else:
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
                conn.execute(text("SET UNIQUE_CHECKS = 1"))
        elif dialect == "sqlite":
            if before:
                try:
                    conn.execute(text("PRAGMA journal_mode = WAL"))
                    conn.execute(text("PRAGMA synchronous = OFF"))
                    conn.execute(text("PRAGMA cache_size = -64000"))
                except Exception:
                    pass
            else:
                try:
                    conn.execute(text("PRAGMA synchronous = FULL"))
                except Exception:
                    pass

    def list_tables(self) -> list[str]:
        """List all tables in the database."""
        metadata = self._get_metadata()
        return sorted(metadata.tables.keys())

    def __repr__(self) -> str:
        return f"DatabaseSeeder(url={self._connection_string!r})"
