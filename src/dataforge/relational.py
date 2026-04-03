"""Relational schema — multi-table data generation with foreign keys."""

from __future__ import annotations

from collections import deque
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from dataforge.core import DataForge


class RelationalSchema:
    """Multi-table schema with foreign key relationships."""

    __slots__ = ("_forge", "_table_specs", "_order")

    def __init__(self, forge: DataForge, tables: dict[str, dict[str, Any]]) -> None:
        self._forge = forge
        self._table_specs = tables
        self._order = self._topological_sort(tables)

    @staticmethod
    def _topological_sort(tables: dict[str, dict[str, Any]]) -> list[str]:
        """Sort table names so parents come before children."""
        deps: dict[str, str | None] = {}
        for name, spec in tables.items():
            deps[name] = spec.get("parent")

        for name, parent in deps.items():
            if parent is not None and parent not in tables:
                raise ValueError(
                    f"Table '{name}' references parent '{parent}' "
                    f"which is not defined. Available tables: {list(tables)}"
                )

        in_degree: dict[str, int] = {name: 0 for name in tables}
        children_of: dict[str, list[str]] = {name: [] for name in tables}
        for name, parent in deps.items():
            if parent is not None:
                in_degree[name] += 1
                children_of[parent].append(name)

        queue = deque(name for name, deg in in_degree.items() if deg == 0)
        order: list[str] = []
        while queue:
            node = queue.popleft()
            order.append(node)
            for child in children_of[node]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(order) != len(tables):
            raise ValueError(
                "Circular dependency detected in table relationships. "
                "Ensure parent references form a DAG."
            )

        return order

    def generate(self) -> dict[str, list[dict[str, Any]]]:
        """Generate all tables with referential integrity."""
        forge = self._forge
        rng = forge._engine._rng
        result: dict[str, list[dict[str, Any]]] = {}

        schemas: dict[str, Any] = {}

        for table_name in self._order:
            spec = self._table_specs[table_name]
            fields = spec.get("fields", [])
            count = spec.get("count", 10)
            null_fields = spec.get("null_fields")
            parent_name = spec.get("parent")
            parent_key = spec.get("parent_key")
            children_per_parent = spec.get("children_per_parent")

            if table_name not in schemas:
                schemas[table_name] = forge.schema(fields, null_fields=null_fields)
            schema = schemas[table_name]
            rows = schema.generate(count=count)

            for i, row in enumerate(rows, 1):
                row["id"] = i

            if parent_name is not None:
                if parent_key is None:
                    parent_key = f"{parent_name}_id"

                parent_rows = result[parent_name]
                parent_ids = [r["id"] for r in parent_rows]

                if not parent_ids:
                    for row in rows:
                        row[parent_key] = None
                elif children_per_parent is not None:
                    min_c, max_c = children_per_parent
                    assignments = self._distribute_children(
                        rng, parent_ids, count, min_c, max_c
                    )
                    for row, fk_val in zip(rows, assignments):
                        row[parent_key] = fk_val
                else:
                    assigned = rng.choices(parent_ids, k=count)
                    for row, fk_val in zip(rows, assigned):
                        row[parent_key] = fk_val

            result[table_name] = rows

        return result

    @staticmethod
    def _distribute_children(
        rng: Any,
        parent_ids: list[int],
        total_children: int,
        min_per_parent: int,
        max_per_parent: int,
    ) -> list[int]:
        """Distribute children across parents respecting cardinality bounds."""
        n_parents = len(parent_ids)
        counts: list[int] = []
        remaining = total_children

        for i in range(n_parents):
            if remaining <= 0:
                counts.append(0)
                continue
            parents_left = n_parents - i - 1
            max_here = min(
                max_per_parent,
                remaining - parents_left * min_per_parent,
            )
            min_here = min(min_per_parent, remaining)
            if max_here < min_here:
                max_here = min_here
            c = rng.randint(min_here, max(min_here, max_here))
            counts.append(c)
            remaining -= c

        while remaining > 0:
            for i in range(n_parents):
                if remaining <= 0:
                    break
                if counts[i] < max_per_parent:
                    add = min(remaining, max_per_parent - counts[i])
                    counts[i] += add
                    remaining -= add

        assignments: list[int] = []
        for pid, c in zip(parent_ids, counts):
            assignments.extend([pid] * c)

        rng.shuffle(assignments)

        if len(assignments) > total_children:
            assignments = assignments[:total_children]
        elif len(assignments) < total_children:
            extra = rng.choices(parent_ids, k=total_children - len(assignments))
            assignments.extend(extra)

        return assignments

    def to_sql(
        self,
        dialect: str = "sqlite",
    ) -> str:
        """Generate all tables and return as SQL INSERT statements."""
        data = self.generate()
        parts: list[str] = []
        _str = str

        for table_name in self._order:
            rows = data[table_name]
            if not rows:
                continue

            columns = list(rows[0].keys())

            if dialect == "mysql":
                col_list = ", ".join(f"`{c}`" for c in columns)
                tbl = f"`{table_name}`"
            else:
                col_list = ", ".join(f'"{c}"' for c in columns)
                tbl = f'"{table_name}"'

            _BATCH = 1000
            prefix = f"INSERT INTO {tbl} ({col_list}) VALUES"
            for batch_start in range(0, len(rows), _BATCH):
                batch = rows[batch_start : batch_start + _BATCH]
                value_rows = []
                for row in batch:
                    vals = ", ".join(
                        "NULL"
                        if row[c] is None
                        else (
                            _str(row[c])
                            if isinstance(row[c], (int, float))
                            else "'" + _str(row[c]).replace("'", "''") + "'"
                        )
                        for c in columns
                    )
                    value_rows.append(f"({vals})")
                parts.append(f"{prefix}\n" + ",\n".join(value_rows) + ";")

        return "\n".join(parts) + "\n"

    def __repr__(self) -> str:
        return f"RelationalSchema(tables={list(self._table_specs)})"
