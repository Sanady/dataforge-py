"""Constraint engine — correlated and conditional field generation."""

from __future__ import annotations

import datetime as _datetime
import math as _math
from collections import deque
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from dataforge.core import DataForge
    from dataforge.backend import RandomEngine


class FieldConstraint:
    """Base class for field constraints."""

    __slots__ = ("field", "column_name")

    def __init__(self, field: str, column_name: str) -> None:
        self.field = field
        self.column_name = column_name

    def generate(
        self, row: dict[str, Any], engine: RandomEngine, forge: "DataForge"
    ) -> Any:
        """Generate a value given the current row context."""
        raise NotImplementedError


class DependsOnConstraint(FieldConstraint):
    """Geographic dependency: city depends on country/state, etc."""

    __slots__ = ("field", "column_name", "depends_on", "dep_type", "_geo_loaded")

    _geo_get_cities: Any = None
    _geo_get_states: Any = None
    _geo_get_zip: Any = None
    _geo_phone_fmt: Any = None
    _geo_currency: Any = None

    def __init__(
        self,
        field: str,
        column_name: str,
        depends_on: str,
    ) -> None:
        self.field = field
        self.column_name = column_name
        self.depends_on = depends_on
        self.dep_type = self._detect_dep_type(field, depends_on)
        self._geo_loaded = False

    @staticmethod
    def _detect_dep_type(field: str, depends_on: str) -> str:
        """Detect the type of geographic dependency."""
        f_lower = field.lower()
        d_lower = depends_on.lower()
        if "city" in f_lower and ("state" in d_lower or "country" in d_lower):
            return "city"
        if "state" in f_lower and "country" in d_lower:
            return "state"
        if ("zip" in f_lower or "postal" in f_lower) and "state" in d_lower:
            return "zipcode"
        if "phone" in f_lower and "country" in d_lower:
            return "phone"
        if "currency" in f_lower and "country" in d_lower:
            return "currency"
        return "generic"

    @classmethod
    def _ensure_geo_loaded(cls) -> None:
        """Load geo module references once (class-level cache)."""
        if cls._geo_get_cities is not None:
            return
        from dataforge.data.correlations.geo import (
            get_cities_for_state,
            get_states_for_country,
            get_zip_prefix_for_state,
            COUNTRY_PHONE_FORMAT,
            COUNTRY_CURRENCY,
        )

        cls._geo_get_cities = staticmethod(get_cities_for_state)  # type: ignore[assignment]
        cls._geo_get_states = staticmethod(get_states_for_country)  # type: ignore[assignment]
        cls._geo_get_zip = staticmethod(get_zip_prefix_for_state)  # type: ignore[assignment]
        cls._geo_phone_fmt = COUNTRY_PHONE_FORMAT
        cls._geo_currency = COUNTRY_CURRENCY

    def generate(
        self, row: dict[str, Any], engine: RandomEngine, forge: "DataForge"
    ) -> Any:
        cls = type(self)
        if cls._geo_get_cities is None:
            cls._ensure_geo_loaded()

        parent_val = row.get(self.depends_on, "")
        parent_str = str(parent_val) if parent_val is not None else ""

        if self.dep_type == "state":
            states = cls._geo_get_states(parent_str)
            return engine.choice(states)

        if self.dep_type == "city":
            cities = cls._geo_get_cities(parent_str)
            return engine.choice(cities)

        if self.dep_type == "zipcode":
            prefix = cls._geo_get_zip(parent_str)
            if prefix:
                return prefix + engine.random_digits_str(2)
            return engine.random_digits_str(5)

        if self.dep_type == "phone":
            fmt = cls._geo_phone_fmt.get(parent_str, "+1-###-###-####")
            return engine.numerify(fmt)

        if self.dep_type == "currency":
            return cls._geo_currency.get(parent_str, "USD")

        provider_attr, method_name = forge._resolve_field(self.field)
        provider = getattr(forge, provider_attr)
        method = getattr(provider, method_name)
        return method()


class TemporalConstraint(FieldConstraint):
    """Temporal ordering: field must be before/after a reference field."""

    __slots__ = ("field", "column_name", "temporal", "reference", "offset_days")

    def __init__(
        self,
        field: str,
        column_name: str,
        temporal: str,
        reference: str,
        offset_days: tuple[int, int] = (1, 365),
    ) -> None:
        self.field = field
        self.column_name = column_name
        self.temporal = temporal
        self.reference = reference
        self.offset_days = offset_days

    def generate(
        self, row: dict[str, Any], engine: RandomEngine, forge: "DataForge"
    ) -> Any:
        ref_val = row.get(self.reference)
        if ref_val is None:
            provider_attr, method_name = forge._resolve_field(self.field)
            provider = getattr(forge, provider_attr)
            return getattr(provider, method_name)()

        if isinstance(ref_val, str):
            ref_date = _datetime.date.fromisoformat(ref_val)
        elif isinstance(ref_val, _datetime.datetime):
            ref_date = ref_val.date()
        elif isinstance(ref_val, _datetime.date):
            ref_date = ref_val
        else:
            ref_date = _datetime.date.fromisoformat(str(ref_val))

        min_off, max_off = self.offset_days
        offset = engine.random_int(min_off, max_off)

        if self.temporal == "after":
            result_date = ref_date + _datetime.timedelta(days=offset)
        else:
            result_date = ref_date - _datetime.timedelta(days=offset)

        return result_date.isoformat()


class CorrelateConstraint(FieldConstraint):
    """Statistical correlation using Cholesky decomposition."""

    __slots__ = ("field", "column_name", "correlate_with", "correlation", "mean", "std")

    def __init__(
        self,
        field: str,
        column_name: str,
        correlate_with: str,
        correlation: float = 0.8,
        mean: float = 0.0,
        std: float = 1.0,
    ) -> None:
        self.field = field
        self.column_name = column_name
        self.correlate_with = correlate_with
        self.correlation = max(-1.0, min(1.0, correlation))
        self.mean = mean
        self.std = std

    def generate(
        self, row: dict[str, Any], engine: RandomEngine, forge: "DataForge"
    ) -> Any:
        ref_val = row.get(self.correlate_with)
        if ref_val is None:
            return engine.gauss(self.mean, self.std)

        try:
            x = float(ref_val)
        except (ValueError, TypeError):
            return engine.gauss(self.mean, self.std)

        rho = self.correlation
        z = engine.gauss(0.0, 1.0)
        y = rho * x + _math.sqrt(max(0.0, 1.0 - rho * rho)) * z
        return round(self.mean + self.std * y, 4)


class ConditionalConstraint(FieldConstraint):
    """Conditional value pools based on another field's value."""

    __slots__ = (
        "field",
        "column_name",
        "conditional_on",
        "value_pools",
        "default_pool",
    )

    def __init__(
        self,
        field: str,
        column_name: str,
        conditional_on: str,
        value_pools: dict[str, tuple[str, ...]],
        default_pool: tuple[str, ...] | None = None,
    ) -> None:
        self.field = field
        self.column_name = column_name
        self.conditional_on = conditional_on
        self.value_pools = value_pools
        self.default_pool = default_pool or ("unknown",)

    def generate(
        self, row: dict[str, Any], engine: RandomEngine, forge: "DataForge"
    ) -> Any:
        condition_val = str(row.get(self.conditional_on, ""))
        pool = self.value_pools.get(condition_val, self.default_pool)
        return engine.choice(pool)


class RangeConstraint(FieldConstraint):
    """Range constraint: numeric value within bounds, optionally dependent."""

    __slots__ = (
        "field",
        "column_name",
        "min_val",
        "max_val",
        "min_ref",
        "max_ref",
        "precision",
    )

    def __init__(
        self,
        field: str,
        column_name: str,
        min_val: float | None = None,
        max_val: float | None = None,
        min_ref: str | None = None,
        max_ref: str | None = None,
        precision: int = 2,
    ) -> None:
        self.field = field
        self.column_name = column_name
        self.min_val = min_val
        self.max_val = max_val
        self.min_ref = min_ref
        self.max_ref = max_ref
        self.precision = precision

    def generate(
        self, row: dict[str, Any], engine: RandomEngine, forge: "DataForge"
    ) -> Any:
        lo = self.min_val if self.min_val is not None else 0.0
        hi = self.max_val if self.max_val is not None else 100.0

        if self.min_ref and self.min_ref in row:
            try:
                lo = float(row[self.min_ref])
            except (ValueError, TypeError):
                pass
        if self.max_ref and self.max_ref in row:
            try:
                hi = float(row[self.max_ref])
            except (ValueError, TypeError):
                pass

        if lo > hi:
            lo, hi = hi, lo

        return engine.random_float(lo, hi, self.precision)


def parse_field_spec(
    column_name: str,
    spec: dict[str, Any],
) -> tuple[FieldConstraint | None, list[str]]:
    """Parse a dict-based field spec into a constraint and its dependencies."""
    field = spec.get("field", column_name)
    deps: list[str] = []

    if "depends_on" in spec:
        dep = spec["depends_on"]
        deps.append(dep)
        return DependsOnConstraint(field, column_name, dep), deps

    if "temporal" in spec:
        ref = spec.get("reference", "")
        if ref:
            deps.append(ref)
        offset = spec.get("offset_days", (1, 365))
        if isinstance(offset, (list, tuple)) and len(offset) == 2:
            offset = (int(offset[0]), int(offset[1]))
        else:
            offset = (1, 365)
        return TemporalConstraint(
            field, column_name, spec["temporal"], ref, offset
        ), deps

    if "correlate" in spec:
        ref = spec["correlate"]
        deps.append(ref)
        return CorrelateConstraint(
            field,
            column_name,
            ref,
            correlation=float(spec.get("correlation", 0.8)),
            mean=float(spec.get("mean", 0.0)),
            std=float(spec.get("std", 1.0)),
        ), deps

    if "conditional" in spec:
        cond_on = spec["conditional"]
        deps.append(cond_on)
        pools = {}
        raw_pools = spec.get("value_pools", {})
        for k, v in raw_pools.items():
            pools[k] = tuple(v) if isinstance(v, (list, tuple)) else (v,)
        default = spec.get("default_pool")
        if default:
            default = (
                tuple(default) if isinstance(default, (list, tuple)) else (default,)
            )
        return ConditionalConstraint(field, column_name, cond_on, pools, default), deps

    if "range" in spec or "min_val" in spec or "max_val" in spec:
        return RangeConstraint(
            field,
            column_name,
            min_val=spec.get("min_val"),
            max_val=spec.get("max_val"),
            min_ref=spec.get("min_ref"),
            max_ref=spec.get("max_ref"),
            precision=int(spec.get("precision", 2)),
        ), [x for x in [spec.get("min_ref"), spec.get("max_ref")] if x]

    return None, []


def build_dependency_order(
    field_specs: dict[str, Any],
) -> tuple[
    list[str],
    list[tuple[str, FieldConstraint]],
    dict[str, FieldConstraint],
]:
    """Build dependency DAG and return generation order."""
    constraints: dict[str, FieldConstraint] = {}
    dep_graph: dict[str, list[str]] = {}
    all_columns = list(field_specs.keys())

    for col_name, spec in field_specs.items():
        if isinstance(spec, dict):
            constraint, deps = parse_field_spec(col_name, spec)
            if constraint is not None:
                constraints[col_name] = constraint
                dep_graph[col_name] = deps
            else:
                dep_graph[col_name] = []
        else:
            dep_graph[col_name] = []

    dependent_set = set(constraints.keys())
    independent = [c for c in all_columns if c not in dependent_set]

    in_degree: dict[str, int] = {c: 0 for c in dependent_set}
    adj: dict[str, list[str]] = {c: [] for c in dependent_set}

    for col, deps in dep_graph.items():
        if col not in dependent_set:
            continue
        for dep in deps:
            if dep in dependent_set:
                adj[dep].append(col)
                in_degree[col] += 1

    queue = deque(c for c in dependent_set if in_degree[c] == 0)
    ordered: list[tuple[str, FieldConstraint]] = []

    while queue:
        node = queue.popleft()
        ordered.append((node, constraints[node]))
        for child in adj.get(node, []):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if len(ordered) != len(dependent_set):
        raise ValueError(
            "Circular dependency detected in field constraints. "
            "Ensure constraint references form a DAG."
        )

    return independent, ordered, constraints
