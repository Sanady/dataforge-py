"""Database Seeding — Populate Databases with Realistic Test Data.

Real-world scenario: You need to set up a development database with
realistic test data. The DatabaseSeeder introspects your database
schema via SQLAlchemy, automatically maps columns to appropriate
data generators, and inserts data with dialect-specific optimizations.

This example demonstrates:
- Auto-introspecting database tables
- Seeding a single table
- Overriding column-to-field mappings
- Seeding related tables with referential integrity
- Dialect-specific optimizations (MySQL, SQLite)

Requires: pip install sqlalchemy (or: pip install dataforge-py[db])
"""

# Note: This example requires SQLAlchemy to be installed.
# pip install sqlalchemy

import os

try:
    import sqlalchemy as sa
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
except ImportError:
    print("This example requires SQLAlchemy.")
    print("Install it with: pip install sqlalchemy")
    raise SystemExit(1)

from dataforge import DataForge
from dataforge.seeder import DatabaseSeeder

forge = DataForge(seed=42)

# --- Setup: Create an in-memory SQLite database ---------------------------

print("=== Setup: Creating SQLite Database ===\n")


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    first_name: Mapped[str] = mapped_column(sa.String(100))
    last_name: Mapped[str] = mapped_column(sa.String(100))
    email: Mapped[str] = mapped_column(sa.String(255))
    city: Mapped[str] = mapped_column(sa.String(100))
    created_at: Mapped[str] = mapped_column(sa.String(50))


class Order(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(sa.ForeignKey("users.id"))
    product_name: Mapped[str] = mapped_column(sa.String(200))
    amount: Mapped[str] = mapped_column(sa.String(20))
    order_date: Mapped[str] = mapped_column(sa.String(50))


# Create the database and tables
engine = sa.create_engine("sqlite:///example_seed.db", echo=False)
Base.metadata.create_all(engine)
print("Created tables: users, orders\n")

# --- Example 1: Seed a single table --------------------------------------

print("=== Seed Users Table ===\n")

seeder = DatabaseSeeder(forge, "sqlite:///example_seed.db")

# List available tables
print(f"Available tables: {seeder.list_tables()}")

# Seed the users table (auto-detects column types)
rows_inserted = seeder.seed_table("users", count=50)
print(f"Inserted {rows_inserted} rows into 'users'")

# Verify
with Session(engine) as session:
    users = session.execute(sa.text("SELECT * FROM users LIMIT 5")).fetchall()
    print("\nSample users:")
    for user in users:
        print(f"  id={user[0]}  {user[1]} {user[2]}  {user[3]}  {user[4]}")
print()

# --- Example 2: Seed with field overrides ---------------------------------

print("=== Seed with Custom Field Mappings ===\n")

# Override how certain columns are generated
rows_inserted = seeder.seed_table(
    "orders",
    count=100,
    field_overrides={
        "product_name": "ecommerce.product_name",
        "amount": "finance.price",
        "order_date": "datetime",
        "user_id": "misc.random_int",  # simple approach for demo
    },
)
print(f"Inserted {rows_inserted} rows into 'orders'")

with Session(engine) as session:
    orders = session.execute(sa.text("SELECT * FROM orders LIMIT 5")).fetchall()
    print("\nSample orders:")
    for order in orders:
        print(
            f"  id={order[0]}  user_id={order[1]}  {order[2]}  ${order[3]}  {order[4]}"
        )
print()

# --- Example 3: Seed related tables --------------------------------------

print("=== Seed Related Tables (Referential Integrity) ===\n")

# Clean up for fresh demo
with Session(engine) as session:
    session.execute(sa.text("DELETE FROM orders"))
    session.execute(sa.text("DELETE FROM users"))
    session.commit()

# Use seed_relational for FK-aware seeding
result = seeder.seed_relational(
    {
        "users": {
            "count": 20,
            "fields": {
                "first_name": "first_name",
                "last_name": "last_name",
                "email": "email",
                "city": "city",
                "created_at": "datetime",
            },
        },
        "orders": {
            "count": 60,
            "parent": "users",
            "fields": {
                "product_name": "ecommerce.product_name",
                "amount": "finance.price",
                "order_date": "datetime",
            },
        },
    }
)

print("Rows inserted per table:")
for table_name, count in result.items():
    print(f"  {table_name}: {count}")

# Verify referential integrity
with Session(engine) as session:
    orphans = session.execute(
        sa.text(
            "SELECT COUNT(*) FROM orders WHERE user_id NOT IN (SELECT id FROM users)"
        )
    ).scalar()
    print(f"\nOrphan orders (should be 0): {orphans}")

# Clean up
os.remove("example_seed.db")
print("\nCleaned up example database.")
