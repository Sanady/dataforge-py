"""Tests for database seeding — populate databases with fake data.

Uses SQLite in-memory databases to test without external dependencies.
"""

from __future__ import annotations

import pytest

from dataforge import DataForge

# Skip all tests if sqlalchemy is not installed
sa = pytest.importorskip("sqlalchemy")


# Fixtures


@pytest.fixture
def forge() -> DataForge:
    return DataForge(locale="en_US", seed=42)


@pytest.fixture
def engine():
    """Create an in-memory SQLite database with test tables."""
    engine = sa.create_engine("sqlite:///:memory:")
    metadata = sa.MetaData()

    sa.Table(
        "users",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("first_name", sa.String(100)),
        sa.Column("last_name", sa.String(100)),
        sa.Column("email", sa.String(255)),
        sa.Column("city", sa.String(100)),
    )

    sa.Table(
        "orders",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id")),
        sa.Column("total", sa.Float),
        sa.Column("created_at", sa.DateTime),
    )

    # A minimal table with few recognizable columns
    sa.Table(
        "items",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("sku", sa.String(50)),
    )

    metadata.create_all(engine)
    return engine


@pytest.fixture
def seeder(forge, engine):
    from dataforge.seeder import DatabaseSeeder

    # We pass the URL string but we also need to inject the pre-created engine
    s = DatabaseSeeder(forge, "sqlite:///:memory:")
    # Override the engine with our already-created one
    s._engine = engine
    s._metadata = None  # Force re-reflect from the injected engine
    return s


# Construction


class TestDatabaseSeederConstruction:
    def test_repr(self, seeder) -> None:
        r = repr(seeder)
        assert "DatabaseSeeder" in r

    def test_slots(self, seeder) -> None:
        with pytest.raises(AttributeError):
            seeder.nonexistent = True

    def test_lazy_engine(self, forge) -> None:
        from dataforge.seeder import DatabaseSeeder

        s = DatabaseSeeder(forge, "sqlite:///:memory:")
        assert s._engine is None


# Table introspection


class TestTableIntrospection:
    def test_list_tables(self, seeder) -> None:
        tables = seeder.list_tables()
        assert "users" in tables
        assert "orders" in tables

    def test_introspect_users(self, seeder) -> None:
        field_map = seeder._introspect_table("users")
        # first_name, last_name, email, city should be mapped
        assert "first_name" in field_map
        assert "last_name" in field_map
        assert "email" in field_map
        assert "city" in field_map
        # 'id' should be skipped (autoincrement PK)
        assert "id" not in field_map

    def test_introspect_orders_skips_fk(self, seeder) -> None:
        field_map = seeder._introspect_table("orders")
        # user_id is FK, should be skipped
        assert "user_id" not in field_map
        # id should be skipped (autoincrement)
        assert "id" not in field_map

    def test_introspect_nonexistent_table(self, seeder) -> None:
        with pytest.raises(ValueError, match="not found"):
            seeder._introspect_table("nonexistent")


# Seed single table


class TestSeedTable:
    def test_seed_users(self, seeder, engine) -> None:
        count = seeder.seed_table("users", count=50)
        assert count == 50

        # Verify rows in database
        with engine.connect() as conn:
            result = conn.execute(sa.text("SELECT COUNT(*) FROM users"))
            assert result.scalar() == 50

    def test_seed_with_overrides(self, seeder, engine) -> None:
        count = seeder.seed_table(
            "users",
            count=10,
            field_overrides={"first_name": "first_name", "email": "email"},
        )
        assert count == 10

    def test_seed_batched(self, seeder, engine) -> None:
        count = seeder.seed_table("users", count=250, batch_size=100)
        assert count == 250

        with engine.connect() as conn:
            result = conn.execute(sa.text("SELECT COUNT(*) FROM users"))
            assert result.scalar() == 250

    def test_seed_empty_mapping_raises(self, seeder, engine) -> None:
        # The 'items' table has 'sku' which may be mapped via type fallback.
        # Verify by checking if introspection returns an empty map
        field_map = seeder._introspect_table("items")
        if not field_map:
            with pytest.raises(ValueError, match="No columns"):
                seeder.seed_table("items", count=10)
        else:
            # If 'sku' was mapped via type fallback, that's OK — just verify seeding works
            count = seeder.seed_table("items", count=10)
            assert count == 10


# Dialect optimizations


class TestDialectOptimizations:
    def test_sqlite_optimizations(self, seeder) -> None:
        from dataforge.seeder import DatabaseSeeder

        # Just verify the static method doesn't raise
        engine = seeder._get_engine()
        with engine.begin() as conn:
            DatabaseSeeder._apply_dialect_optimizations(conn, "sqlite", before=True)
            DatabaseSeeder._apply_dialect_optimizations(conn, "sqlite", before=False)

    def test_unknown_dialect_no_op(self, seeder) -> None:
        from dataforge.seeder import DatabaseSeeder

        engine = seeder._get_engine()
        with engine.begin() as conn:
            # Should not raise for unknown dialect
            DatabaseSeeder._apply_dialect_optimizations(conn, "unknown_db", before=True)
            DatabaseSeeder._apply_dialect_optimizations(
                conn, "unknown_db", before=False
            )
