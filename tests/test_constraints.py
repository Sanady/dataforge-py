"""Tests for the constraint engine — correlated and conditional field generation."""

from __future__ import annotations

import pytest

from dataforge import DataForge
from dataforge.constraints import (
    DependsOnConstraint,
    TemporalConstraint,
    CorrelateConstraint,
    ConditionalConstraint,
    RangeConstraint,
    parse_field_spec,
    build_dependency_order,
)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture
def forge() -> DataForge:
    return DataForge(locale="en_US", seed=42)


# ------------------------------------------------------------------
# Unit tests for individual constraint classes
# ------------------------------------------------------------------


class TestDependsOnConstraint:
    """Test geographic dependency constraint."""

    def test_state_depends_on_country(self, forge: DataForge) -> None:
        """state depending on country should pick from that country's states."""
        from dataforge.data.correlations.geo import COUNTRY_STATES

        c = DependsOnConstraint("address.state", "state", "country")
        assert c.dep_type == "state"

        row = {"country": "United States"}
        val = c.generate(row, forge._engine, forge)
        assert val in COUNTRY_STATES["United States"]

    def test_city_depends_on_state(self, forge: DataForge) -> None:
        from dataforge.data.correlations.geo import STATE_CITIES

        c = DependsOnConstraint("address.city", "city", "state")
        assert c.dep_type == "city"

        row = {"state": "California"}
        val = c.generate(row, forge._engine, forge)
        assert val in STATE_CITIES["California"]

    def test_currency_depends_on_country(self, forge: DataForge) -> None:
        c = DependsOnConstraint("currency", "currency", "country")
        assert c.dep_type == "currency"

        row = {"country": "Japan"}
        val = c.generate(row, forge._engine, forge)
        assert val == "JPY"

    def test_unknown_country_fallback(self, forge: DataForge) -> None:
        """Unknown country should still produce a value (fallback provinces)."""
        c = DependsOnConstraint("address.state", "state", "country")
        row = {"country": "Atlantis"}
        val = c.generate(row, forge._engine, forge)
        assert isinstance(val, str)


class TestTemporalConstraint:
    """Test temporal ordering constraint."""

    def test_after_reference(self, forge: DataForge) -> None:
        c = TemporalConstraint("date", "end_date", "after", "start_date", (1, 30))
        row = {"start_date": "2024-01-01"}
        val = c.generate(row, forge._engine, forge)
        assert val > "2024-01-01"  # ISO string comparison

    def test_before_reference(self, forge: DataForge) -> None:
        c = TemporalConstraint("date", "start_date", "before", "end_date", (1, 30))
        row = {"end_date": "2024-12-31"}
        val = c.generate(row, forge._engine, forge)
        assert val < "2024-12-31"

    def test_none_reference_fallback(self, forge: DataForge) -> None:
        """If reference is None, should still generate a value."""
        c = TemporalConstraint("date", "end_date", "after", "start_date", (1, 30))
        row = {"start_date": None}
        val = c.generate(row, forge._engine, forge)
        assert val is not None


class TestCorrelateConstraint:
    """Test statistical correlation constraint."""

    def test_basic_correlation(self, forge: DataForge) -> None:
        c = CorrelateConstraint("value", "y", "x", correlation=0.9, mean=0.0, std=1.0)
        row = {"x": 2.0}
        val = c.generate(row, forge._engine, forge)
        assert isinstance(val, float)

    def test_no_reference_fallback(self, forge: DataForge) -> None:
        c = CorrelateConstraint("value", "y", "x", correlation=0.9, mean=50.0, std=10.0)
        row = {}
        val = c.generate(row, forge._engine, forge)
        assert isinstance(val, float)

    def test_correlation_bounded(self) -> None:
        """Correlation should be clamped to [-1, 1]."""
        c = CorrelateConstraint("v", "y", "x", correlation=5.0)
        assert c.correlation == 1.0
        c2 = CorrelateConstraint("v", "y", "x", correlation=-5.0)
        assert c2.correlation == -1.0


class TestConditionalConstraint:
    """Test conditional value pools."""

    def test_conditional_picks_from_pool(self, forge: DataForge) -> None:
        pools = {"M": ("Mr.",), "F": ("Ms.", "Mrs.")}
        c = ConditionalConstraint("title", "title", "gender", pools, ("Mx.",))
        row = {"gender": "M"}
        val = c.generate(row, forge._engine, forge)
        assert val == "Mr."

    def test_conditional_default_pool(self, forge: DataForge) -> None:
        pools = {"M": ("Mr.",), "F": ("Ms.",)}
        c = ConditionalConstraint("title", "title", "gender", pools, ("Mx.",))
        row = {"gender": "X"}
        val = c.generate(row, forge._engine, forge)
        assert val == "Mx."


class TestRangeConstraint:
    """Test numeric range constraint."""

    def test_static_range(self, forge: DataForge) -> None:
        c = RangeConstraint("price", "price", min_val=10.0, max_val=100.0, precision=2)
        row = {}
        val = c.generate(row, forge._engine, forge)
        assert 10.0 <= val <= 100.0

    def test_dynamic_range_from_ref(self, forge: DataForge) -> None:
        c = RangeConstraint(
            "max_price", "max_price", min_ref="min_price", max_val=999.0
        )
        row = {"min_price": 50.0}
        val = c.generate(row, forge._engine, forge)
        assert val >= 50.0

    def test_inverted_bounds_swapped(self, forge: DataForge) -> None:
        """If min > max, they should be swapped."""
        c = RangeConstraint("v", "v", min_val=100.0, max_val=10.0)
        row = {}
        val = c.generate(row, forge._engine, forge)
        assert 10.0 <= val <= 100.0


# ------------------------------------------------------------------
# parse_field_spec tests
# ------------------------------------------------------------------


class TestParseFieldSpec:
    """Test parsing of dict-based field specs."""

    def test_depends_on_spec(self) -> None:
        spec = {"field": "address.city", "depends_on": "country"}
        constraint, deps = parse_field_spec("city", spec)
        assert isinstance(constraint, DependsOnConstraint)
        assert deps == ["country"]

    def test_temporal_spec(self) -> None:
        spec = {"field": "date", "temporal": "after", "reference": "start_date"}
        constraint, deps = parse_field_spec("end_date", spec)
        assert isinstance(constraint, TemporalConstraint)
        assert "start_date" in deps

    def test_correlate_spec(self) -> None:
        spec = {"field": "score", "correlate": "x", "correlation": 0.7}
        constraint, deps = parse_field_spec("y", spec)
        assert isinstance(constraint, CorrelateConstraint)
        assert constraint.correlation == 0.7

    def test_conditional_spec(self) -> None:
        spec = {
            "field": "title",
            "conditional": "gender",
            "value_pools": {"M": ["Mr."], "F": ["Ms."]},
        }
        constraint, deps = parse_field_spec("title", spec)
        assert isinstance(constraint, ConditionalConstraint)
        assert "gender" in deps

    def test_range_spec(self) -> None:
        spec = {"field": "price", "min_val": 0, "max_val": 1000}
        constraint, deps = parse_field_spec("price", spec)
        assert isinstance(constraint, RangeConstraint)

    def test_plain_field_spec(self) -> None:
        """A dict with only 'field' should return no constraint."""
        spec = {"field": "email"}
        constraint, deps = parse_field_spec("email", spec)
        assert constraint is None
        assert deps == []


# ------------------------------------------------------------------
# build_dependency_order tests
# ------------------------------------------------------------------


class TestBuildDependencyOrder:
    """Test DAG building and topological sort."""

    def test_simple_dag(self) -> None:
        specs = {
            "country": "country",
            "state": {"field": "address.state", "depends_on": "country"},
        }
        independent, dependent, constraints = build_dependency_order(specs)
        assert "country" in independent
        assert len(dependent) == 1
        assert dependent[0][0] == "state"

    def test_chain_dependency(self) -> None:
        specs = {
            "country": "country",
            "state": {"field": "address.state", "depends_on": "country"},
            "city": {"field": "address.city", "depends_on": "state"},
        }
        independent, dependent, constraints = build_dependency_order(specs)
        assert independent == ["country"]
        dep_names = [d[0] for d in dependent]
        # state must come before city
        assert dep_names.index("state") < dep_names.index("city")

    def test_circular_dependency_raises(self) -> None:
        specs = {
            "a": {"field": "x", "depends_on": "b"},
            "b": {"field": "y", "depends_on": "a"},
        }
        with pytest.raises(ValueError, match="[Cc]ircular"):
            build_dependency_order(specs)


# ------------------------------------------------------------------
# Full Schema integration tests
# ------------------------------------------------------------------


class TestConstraintSchemaIntegration:
    """Test constraint-based schemas end-to-end via forge.schema()."""

    def test_geographic_chain(self, forge: DataForge) -> None:
        """country → state → city chain should produce consistent data."""
        from dataforge.data.correlations.geo import (
            COUNTRY_STATES,
            STATE_CITIES,
        )

        schema = forge.schema(
            {
                "country": "country",
                "state": {"field": "address.state", "depends_on": "country"},
                "city": {"field": "address.city", "depends_on": "state"},
            }
        )
        rows = schema.generate(count=50)
        assert len(rows) == 50
        for row in rows:
            assert isinstance(row["country"], str)
            assert isinstance(row["state"], str)
            assert isinstance(row["city"], str)
            # Verify geographic consistency
            country = row["country"]
            if country in COUNTRY_STATES:
                assert row["state"] in COUNTRY_STATES[country]
                state = row["state"]
                if state in STATE_CITIES:
                    assert row["city"] in STATE_CITIES[state]

    def test_temporal_ordering(self, forge: DataForge) -> None:
        schema = forge.schema(
            {
                "start_date": "date",
                "end_date": {
                    "field": "date",
                    "temporal": "after",
                    "reference": "start_date",
                },
            }
        )
        rows = schema.generate(count=20)
        for row in rows:
            assert row["end_date"] > row["start_date"]

    def test_mixed_independent_and_dependent(self, forge: DataForge) -> None:
        """Schema with both plain fields and constraints."""
        schema = forge.schema(
            {
                "name": "first_name",
                "email": "email",
                "country": "country",
                "state": {"field": "address.state", "depends_on": "country"},
            }
        )
        rows = schema.generate(count=10)
        assert len(rows) == 10
        for row in rows:
            assert "name" in row
            assert "email" in row
            assert "country" in row
            assert "state" in row

    def test_conditional_schema(self, forge: DataForge) -> None:
        schema = forge.schema(
            {
                "gender": "first_name",  # just use first_name as a stand-in
                "title": {
                    "field": "title",
                    "conditional": "gender",
                    "value_pools": {},
                    "default_pool": ["Dear"],
                },
            }
        )
        rows = schema.generate(count=10)
        for row in rows:
            assert row["title"] == "Dear"

    def test_range_constraint_schema(self, forge: DataForge) -> None:
        schema = forge.schema(
            {
                "min_price": "first_name",  # placeholder
                "price": {
                    "field": "price",
                    "min_val": 10.0,
                    "max_val": 100.0,
                    "precision": 2,
                },
            }
        )
        rows = schema.generate(count=20)
        for row in rows:
            assert 10.0 <= row["price"] <= 100.0
