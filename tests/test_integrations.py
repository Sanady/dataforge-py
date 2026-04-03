"""Tests for Phase 7 — Advanced Integrations.

Covers:
- Schema.to_arrow()  (requires pyarrow)
- Schema.to_polars() (requires polars)
- DataForge.to_arrow()  delegation
- DataForge.to_polars() delegation
- DataForge.schema_from_pydantic()  (requires pydantic)
- DataForge.schema_from_sqlalchemy() (requires sqlalchemy)
"""

import pytest

from dataforge import DataForge

# Check optional deps at collection time
_has_pyarrow = True
try:
    import pyarrow as pa
except ModuleNotFoundError:
    _has_pyarrow = False

_has_polars = True
try:
    import polars as pl
except ModuleNotFoundError:
    _has_polars = False

_has_pydantic = True
_pydantic_works = True
try:
    import pydantic

    # Check if pydantic actually works on this Python version
    # (pydantic 2.x may not support Python 3.14 beta)
    try:

        class _PydanticCheck(pydantic.BaseModel):
            x: str

        _pydantic_works = True
    except Exception:
        _pydantic_works = False
except ModuleNotFoundError:
    _has_pydantic = False
    _pydantic_works = False

_has_sqlalchemy = True
try:
    import sqlalchemy  # noqa: F401
except ModuleNotFoundError:
    _has_sqlalchemy = False


# Fixtures


@pytest.fixture
def forge() -> DataForge:
    return DataForge(locale="en_US", seed=42)


# to_arrow


@pytest.mark.skipif(not _has_pyarrow, reason="pyarrow not installed")
class TestToArrow:
    def test_basic_arrow_table(self, forge: DataForge) -> None:
        s = forge.schema(["first_name", "email", "city"])
        table = s.to_arrow(count=100)
        assert table.num_rows == 100
        assert table.num_columns == 3
        assert table.column_names == ["first_name", "email", "city"]

    def test_arrow_single_row(self, forge: DataForge) -> None:
        s = forge.schema(["first_name"])
        table = s.to_arrow(count=1)
        assert table.num_rows == 1
        assert len(table.column("first_name")) == 1

    def test_arrow_large_count_batched(self, forge: DataForge) -> None:
        s = forge.schema(["first_name", "email"])
        table = s.to_arrow(count=5000, batch_size=1000)
        assert table.num_rows == 5000

    def test_arrow_all_strings(self, forge: DataForge) -> None:
        s = forge.schema(["first_name", "city"])
        table = s.to_arrow(count=10)
        for col_name in table.column_names:
            assert table.schema.field(col_name).type == pa.string()

    def test_arrow_deterministic(self, forge: DataForge) -> None:
        s = forge.schema(["first_name", "email"])
        t1 = s.to_arrow(count=5)
        forge2 = DataForge(seed=42)
        s2 = forge2.schema(["first_name", "email"])
        t2 = s2.to_arrow(count=5)
        assert t1.equals(t2)

    def test_arrow_with_lambda(self, forge: DataForge) -> None:
        s = forge.schema(
            {
                "name": "first_name",
                "upper": lambda row: row["name"].upper(),
            }
        )
        table = s.to_arrow(count=10)
        assert table.num_rows == 10
        names = table.column("name").to_pylist()
        uppers = table.column("upper").to_pylist()
        for n, u in zip(names, uppers):
            assert u == n.upper()

    def test_arrow_small_count(self, forge: DataForge) -> None:
        s = forge.schema(["first_name"])
        table = s.to_arrow(count=2)
        assert table.num_rows == 2

    def test_delegation_to_arrow(self, forge: DataForge) -> None:
        table = forge.to_arrow(["first_name", "email"], count=20)
        assert table.num_rows == 20
        assert table.column_names == ["first_name", "email"]


# to_polars


@pytest.mark.skipif(not _has_polars, reason="polars not installed")
class TestToPolars:
    def test_basic_polars_df(self, forge: DataForge) -> None:
        s = forge.schema(["first_name", "email", "city"])
        df = s.to_polars(count=100)
        assert df.shape == (100, 3)
        assert df.columns == ["first_name", "email", "city"]

    def test_polars_single_row(self, forge: DataForge) -> None:
        s = forge.schema(["first_name"])
        df = s.to_polars(count=1)
        assert df.shape == (1, 1)

    def test_polars_large_count_batched(self, forge: DataForge) -> None:
        s = forge.schema(["first_name", "email"])
        df = s.to_polars(count=5000, batch_size=1000)
        assert df.shape[0] == 5000

    def test_polars_all_utf8(self, forge: DataForge) -> None:
        s = forge.schema(["first_name", "city"])
        df = s.to_polars(count=10)
        for dtype in df.dtypes:
            assert dtype == pl.Utf8

    def test_polars_deterministic(self, forge: DataForge) -> None:
        s = forge.schema(["first_name", "email"])
        df1 = s.to_polars(count=5)
        forge2 = DataForge(seed=42)
        s2 = forge2.schema(["first_name", "email"])
        df2 = s2.to_polars(count=5)
        assert df1.equals(df2)

    def test_polars_with_lambda(self, forge: DataForge) -> None:
        s = forge.schema(
            {
                "name": "first_name",
                "upper": lambda row: row["name"].upper(),
            }
        )
        df = s.to_polars(count=10)
        assert df.shape == (10, 2)
        for name, upper in zip(df["name"].to_list(), df["upper"].to_list()):
            assert upper == name.upper()

    def test_delegation_to_polars(self, forge: DataForge) -> None:
        df = forge.to_polars(["first_name", "email"], count=20)
        assert df.shape == (20, 2)
        assert df.columns == ["first_name", "email"]


# schema_from_pydantic


@pytest.mark.skipif(
    not _pydantic_works,
    reason="pydantic not installed or incompatible with this Python version",
)
class TestSchemaFromPydantic:
    def test_basic_pydantic_mapping(self, forge: DataForge) -> None:
        from pydantic import BaseModel

        class User(BaseModel):
            first_name: str
            email: str
            city: str

        s = forge.schema_from_pydantic(User)
        rows = s.generate(count=5)
        assert len(rows) == 5
        for row in rows:
            assert "first_name" in row
            assert "email" in row
            assert "city" in row

    def test_alias_mapping(self, forge: DataForge) -> None:
        from pydantic import BaseModel

        class Contact(BaseModel):
            first_name: str
            phone: str

        s = forge.schema_from_pydantic(Contact)
        rows = s.generate(count=3)
        assert len(rows) == 3
        for row in rows:
            assert "first_name" in row
            assert "phone" in row

    def test_unmappable_fields_skipped_with_warning(self, forge: DataForge) -> None:
        from pydantic import BaseModel

        class Weird(BaseModel):
            first_name: str
            xyzzy_field: str

        with pytest.warns(UserWarning, match="xyzzy_field"):
            s = forge.schema_from_pydantic(Weird)
        rows = s.generate(count=3)
        assert len(rows) == 3
        assert "first_name" in rows[0]
        assert "xyzzy_field" not in rows[0]

    def test_all_unmappable_raises(self, forge: DataForge) -> None:
        from pydantic import BaseModel

        class Empty(BaseModel):
            xyzzy: str
            plugh: str

        with pytest.raises(ValueError, match="No fields"):
            with pytest.warns(UserWarning):
                forge.schema_from_pydantic(Empty)

    def test_not_basemodel_raises(self, forge: DataForge) -> None:
        with pytest.raises(TypeError, match="Pydantic BaseModel"):
            forge.schema_from_pydantic(dict)  # type: ignore[arg-type]

    def test_pydantic_deterministic(self, forge: DataForge) -> None:
        from pydantic import BaseModel

        class User(BaseModel):
            first_name: str
            email: str

        s = forge.schema_from_pydantic(User)
        rows1 = s.generate(count=5)

        forge2 = DataForge(seed=42)
        s2 = forge2.schema_from_pydantic(User)
        rows2 = s2.generate(count=5)

        assert rows1 == rows2

    def test_pydantic_many_aliases(self, forge: DataForge) -> None:
        from pydantic import BaseModel

        class Profile(BaseModel):
            name: str  # → full_name
            username: str  # direct match
            address: str  # → full_address
            dob: str  # → date_of_birth
            uuid: str  # → uuid4

        s = forge.schema_from_pydantic(Profile)
        rows = s.generate(count=3)
        assert len(rows) == 3
        for row in rows:
            assert "name" in row
            assert "username" in row
            assert "address" in row
            assert "dob" in row
            assert "uuid" in row


# schema_from_sqlalchemy


@pytest.mark.skipif(not _has_sqlalchemy, reason="sqlalchemy not installed")
class TestSchemaFromSQLAlchemy:
    def _make_base(self):
        from sqlalchemy.orm import DeclarativeBase

        class Base(DeclarativeBase):
            pass

        return Base

    def test_basic_sqlalchemy_mapping(self, forge: DataForge) -> None:
        from sqlalchemy.orm import Mapped, mapped_column

        Base = self._make_base()

        class User(Base):
            __tablename__ = "users"
            id: Mapped[int] = mapped_column(primary_key=True)
            first_name: Mapped[str] = mapped_column()
            email: Mapped[str] = mapped_column()
            city: Mapped[str] = mapped_column()

        s = forge.schema_from_sqlalchemy(User)
        rows = s.generate(count=5)
        assert len(rows) == 5
        for row in rows:
            assert "first_name" in row
            assert "email" in row
            assert "city" in row
            assert "id" not in row  # PK skipped

    def test_alias_mapping(self, forge: DataForge) -> None:
        from sqlalchemy.orm import Mapped, mapped_column

        Base = self._make_base()

        class Contact(Base):
            __tablename__ = "contacts"
            id: Mapped[int] = mapped_column(primary_key=True)
            first_name: Mapped[str] = mapped_column()
            phone: Mapped[str] = mapped_column()

        s = forge.schema_from_sqlalchemy(Contact)
        rows = s.generate(count=3)
        assert len(rows) == 3
        for row in rows:
            assert "first_name" in row
            assert "phone" in row

    def test_unmappable_columns_skipped(self, forge: DataForge) -> None:
        from sqlalchemy.orm import Mapped, mapped_column

        Base = self._make_base()

        class Odd(Base):
            __tablename__ = "odds"
            id: Mapped[int] = mapped_column(primary_key=True)
            first_name: Mapped[str] = mapped_column()
            frobnicator: Mapped[str] = mapped_column()

        with pytest.warns(UserWarning, match="frobnicator"):
            s = forge.schema_from_sqlalchemy(Odd)
        rows = s.generate(count=3)
        assert "first_name" in rows[0]
        assert "frobnicator" not in rows[0]

    def test_all_unmappable_raises(self, forge: DataForge) -> None:
        from sqlalchemy.orm import Mapped, mapped_column

        Base = self._make_base()

        class Nothing(Base):
            __tablename__ = "nothing"
            id: Mapped[int] = mapped_column(primary_key=True)
            xyzzy: Mapped[str] = mapped_column()

        with pytest.raises(ValueError, match="No columns"):
            with pytest.warns(UserWarning):
                forge.schema_from_sqlalchemy(Nothing)

    def test_not_model_raises(self, forge: DataForge) -> None:
        with pytest.raises(TypeError, match="__table__"):
            forge.schema_from_sqlalchemy(dict)  # type: ignore[arg-type]

    def test_sqlalchemy_deterministic(self, forge: DataForge) -> None:
        from sqlalchemy.orm import Mapped, mapped_column

        Base = self._make_base()

        class User2(Base):
            __tablename__ = "users2"
            id: Mapped[int] = mapped_column(primary_key=True)
            first_name: Mapped[str] = mapped_column()
            email: Mapped[str] = mapped_column()

        s = forge.schema_from_sqlalchemy(User2)
        rows1 = s.generate(count=5)

        forge2 = DataForge(seed=42)
        s2 = forge2.schema_from_sqlalchemy(User2)
        rows2 = s2.generate(count=5)

        assert rows1 == rows2

    def test_sqlalchemy_many_aliases(self, forge: DataForge) -> None:
        from sqlalchemy.orm import Mapped, mapped_column

        Base = self._make_base()

        class Profile(Base):
            __tablename__ = "profiles"
            id: Mapped[int] = mapped_column(primary_key=True)
            username: Mapped[str] = mapped_column()  # direct
            address: Mapped[str] = mapped_column()  # → full_address
            phone: Mapped[str] = mapped_column()  # → phone_number
            uuid: Mapped[str] = mapped_column()  # → uuid4

        s = forge.schema_from_sqlalchemy(Profile)
        rows = s.generate(count=3)
        assert len(rows) == 3
        for row in rows:
            assert "username" in row
            assert "address" in row
            assert "phone" in row
            assert "uuid" in row
