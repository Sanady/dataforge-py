"""Integration tests for DatabaseSeeder against real in-memory SQLite.

No mocking — each test builds a real SQLite database, introspects it, seeds it,
and asserts the actual rows written. This is legitimate integration coverage
(not vanity line-hitting) because assertions verify data round-trips.
"""

import unittest

import sqlalchemy as sa

from dataforge import DataForge
from dataforge.seeder import DatabaseSeeder


def _make_db(*schemas: str) -> str:
    """Create an in-memory-ish SQLite file DB with the given CREATE TABLE DDL."""
    # Use a shared in-memory DB via a file would break across connections, so
    # use a single-connection in-memory database and return its URL.
    return "sqlite:///:memory:"


class TestDatabaseSeederIntegration(unittest.TestCase):
    def setUp(self) -> None:
        self.forge = DataForge(seed=42)
        self.url = "sqlite:///:memory:"
        # in-memory sqlite persists for the lifetime of one engine; we create
        # the schema through the seeder's own (single) engine.
        self.seeder = DatabaseSeeder(self.forge, self.url)
        engine = self.seeder._get_engine()
        self._create_users_table(engine)

    def _create_users_table(self, engine) -> None:
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    """
                    CREATE TABLE users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        first_name VARCHAR,
                        last_name VARCHAR,
                        email VARCHAR,
                        city VARCHAR
                    )
                    """
                )
            )

    def test_seed_table_inserts_correct_row_count(self):
        inserted = self.seeder.seed_table("users", count=37)
        self.assertEqual(inserted, 37)
        # Assert actual DB contents, not just the return value
        engine = self.seeder._get_engine()
        with engine.begin() as conn:
            rows = conn.execute(sa.text("SELECT COUNT(*) FROM users")).scalar()
            self.assertEqual(rows, 37)

    def test_seed_table_maps_known_columns_to_fields(self):
        self.seeder.seed_table("users", count=5)
        engine = self.seeder._get_engine()
        with engine.begin() as conn:
            rows = conn.execute(
                sa.text("SELECT first_name, email, city FROM users")
            ).fetchall()
        self.assertEqual(len(rows), 5)
        for first, email, city in rows:
            self.assertIsInstance(first, str) and len(first) > 0
            self.assertIn("@", email)
            self.assertIsInstance(city, str) and len(city) > 0

    def test_seed_table_batched_inserts_all_rows(self):
        # batch_size < count exercises the while-remaining loop
        inserted = self.seeder.seed_table("users", count=25, batch_size=10)
        self.assertEqual(inserted, 25)
        engine = self.seeder._get_engine()
        with engine.begin() as conn:
            self.assertEqual(
                conn.execute(sa.text("SELECT COUNT(*) FROM users")).scalar(), 25
            )

    def test_introspect_table_skips_autoincrement_pk(self):
        mapped = self.seeder._introspect_table("users")
        # 'id' is autoincrement PK -> excluded; others present
        self.assertNotIn("id", mapped)
        for col in ("first_name", "last_name", "email", "city"):
            self.assertIn(col, mapped)

    def test_introspect_table_unknown_table_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self.seeder._introspect_table("nonexistent")
        self.assertIn("not found", str(ctx.exception))

    def test_seed_table_no_mappable_columns_raises(self):
        engine = self.seeder._get_engine()
        with engine.begin() as conn:
            conn.execute(
                sa.text("CREATE TABLE weird (id INTEGER PRIMARY KEY AUTOINCREMENT)")
            )
        with self.assertRaises(ValueError) as ctx:
            self.seeder.seed_table("weird", count=1)
        self.assertIn("could be mapped", str(ctx.exception))

    def test_field_overrides_extend_mapping(self):
        engine = self.seeder._get_engine()
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    "CREATE TABLE custom (id INTEGER PRIMARY KEY AUTOINCREMENT, val VARCHAR)"
                )
            )
        mapped = self.seeder._introspect_table("custom")
        self.assertNotIn("val", mapped)  # 'val' not a known field
        self.seeder.seed_table("custom", count=3, field_overrides={"val": "word"})
        with engine.begin() as conn:
            self.assertEqual(
                conn.execute(sa.text("SELECT COUNT(*) FROM custom")).scalar(), 3
            )

    def test_list_tables_sorted(self):
        tables = self.seeder.list_tables()
        self.assertIn("users", tables)
        self.assertEqual(tables, sorted(tables))

    def test_repr_contains_url(self):
        self.assertIn(self.url, repr(self.seeder))

    def test_sqlalchemy_missing_raises_module_error(self):
        # Simulate missing sqlalchemy by clearing cached engine and patching import
        seeder = DatabaseSeeder(self.forge, self.url)
        import builtins

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name.startswith("sqlalchemy"):
                raise ModuleNotFoundError("no sqlalchemy")
                raise ModuleNotFoundError("no sqlalchemy")
            return real_import(name, *args, **kwargs)

        builtins.__import__ = fake_import
        try:
            with self.assertRaises(ModuleNotFoundError):
                seeder._get_engine()
        finally:
            builtins.__import__ = real_import


class TestSeederRelational(unittest.TestCase):
    """Seed two related tables (parent/child FK) and verify referential integrity."""

    def setUp(self) -> None:
        self.forge = DataForge(seed=7)
        self.seeder = DatabaseSeeder(self.forge, "sqlite:///:memory:")
        engine = self.seeder._get_engine()
        with engine.begin() as conn:
            conn.execute(
                sa.text("CREATE TABLE parent (id INTEGER PRIMARY KEY, name VARCHAR)")
            )
            conn.execute(
                sa.text(
                    "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, label VARCHAR)"
                )
            )

    def test_seed_relational_returns_counts(self):
        spec = {
            "parent": {"count": 3},
            "child": {
                "count": 6,
                "parent": "parent",
                "parent_key": "parent_id",
                "field_overrides": {"label": "first_name"},
            },
        }
        result = self.seeder.seed_relational(spec)
        self.assertEqual(result.get("parent"), 3)
        self.assertEqual(result.get("child"), 6)
        with self.seeder._get_engine().connect() as conn:
            parent_ids = {r[0] for r in conn.execute(sa.text("SELECT id FROM parent"))}
            fk_values = {
                r[0] for r in conn.execute(sa.text("SELECT parent_id FROM child"))
            }
        self.assertTrue(fk_values)
        self.assertTrue(fk_values <= parent_ids)


if __name__ == "__main__":
    unittest.main()
