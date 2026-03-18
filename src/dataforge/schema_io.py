"""Schema I/O — serialize and deserialize schema definitions.

Supports JSON, YAML, and TOML formats for defining schemas as config
files that can be loaded by the CLI or Python API.

A schema definition is a dict with the following structure::

    {
        "fields": {"name": "full_name", "email": "email"},
        "null_fields": {"email": 0.2},
        "unique_together": [["name", "email"]],
        "count": 1000
    }

Or with a simple field list::

    {
        "fields": ["first_name", "last_name", "email"],
        "count": 100
    }

Relational schemas use the ``tables`` key::

    {
        "tables": {
            "users": {
                "fields": ["first_name", "email"],
                "count": 100
            },
            "orders": {
                "fields": ["date", "city"],
                "count": 500,
                "parent": "users"
            }
        }
    }
"""

from __future__ import annotations

import json as _json
from typing import Any


def schema_to_dict(
    fields: list[str] | dict[str, str],
    count: int = 10,
    null_fields: dict[str, float] | None = None,
    unique_together: list[tuple[str, ...]] | None = None,
) -> dict[str, Any]:
    """Convert schema parameters to a serializable dict.

    Parameters
    ----------
    fields : list[str] | dict[str, str]
        Field specifications.
    count : int
        Default row count.
    null_fields : dict[str, float] | None
        Null probability mapping.
    unique_together : list[tuple[str, ...]] | None
        Uniqueness constraints.

    Returns
    -------
    dict[str, Any]
    """
    d: dict[str, Any] = {}

    # Normalize fields to the most compact representation
    if isinstance(fields, dict):
        # Check if all keys equal values (simple list form)
        if all(k == v for k, v in fields.items()):
            d["fields"] = list(fields.keys())
        else:
            d["fields"] = dict(fields)
    else:
        d["fields"] = list(fields)

    d["count"] = count

    if null_fields:
        d["null_fields"] = dict(null_fields)

    if unique_together:
        d["unique_together"] = [list(group) for group in unique_together]

    return d


def dict_to_schema_args(
    d: dict[str, Any],
) -> tuple[
    list[str] | dict[str, str],
    int,
    dict[str, float] | None,
    list[tuple[str, ...]] | None,
]:
    """Parse a schema dict back into constructor arguments.

    Parameters
    ----------
    d : dict[str, Any]
        Schema definition dict.

    Returns
    -------
    tuple
        ``(fields, count, null_fields, unique_together)``

    Raises
    ------
    ValueError
        If ``fields`` key is missing.
    """
    if "fields" not in d:
        raise ValueError("Schema definition must contain a 'fields' key.")

    fields_raw = d["fields"]
    if isinstance(fields_raw, list):
        fields: list[str] | dict[str, str] = [str(f) for f in fields_raw]
    elif isinstance(fields_raw, dict):
        fields = {str(k): str(v) for k, v in fields_raw.items()}
    else:
        raise ValueError(
            f"'fields' must be a list or dict, got {type(fields_raw).__name__}"
        )

    count = int(d.get("count", 10))

    null_fields: dict[str, float] | None = None
    if "null_fields" in d and d["null_fields"]:
        null_fields = {str(k): float(v) for k, v in d["null_fields"].items()}

    unique_together: list[tuple[str, ...]] | None = None
    if "unique_together" in d and d["unique_together"]:
        unique_together = [
            tuple(str(c) for c in group) for group in d["unique_together"]
        ]

    return fields, count, null_fields, unique_together


# ------------------------------------------------------------------
# File I/O
# ------------------------------------------------------------------


def save_schema(
    d: dict[str, Any],
    path: str,
    format: str | None = None,
) -> None:
    """Save a schema definition dict to a file.

    Parameters
    ----------
    d : dict[str, Any]
        Schema definition dict (from :func:`schema_to_dict`).
    path : str
        File path to write.
    format : str | None
        Output format: ``"json"``, ``"yaml"``, or ``"toml"``.
        Auto-detected from file extension when ``None``.

    Raises
    ------
    ValueError
        If the format cannot be determined or is unsupported.
    """
    fmt = format or _detect_format(path)

    if fmt == "json":
        _save_json(d, path)
    elif fmt == "yaml":
        _save_yaml(d, path)
    elif fmt == "toml":
        _save_toml(d, path)
    else:
        raise ValueError(
            f"Unsupported schema format '{fmt}'. Use 'json', 'yaml', or 'toml'."
        )


def load_schema(
    path: str,
    format: str | None = None,
) -> dict[str, Any]:
    """Load a schema definition dict from a file.

    Parameters
    ----------
    path : str
        File path to read.
    format : str | None
        Input format: ``"json"``, ``"yaml"``, or ``"toml"``.
        Auto-detected from file extension when ``None``.

    Returns
    -------
    dict[str, Any]
        Parsed schema definition.

    Raises
    ------
    ValueError
        If the format cannot be determined or is unsupported.
    FileNotFoundError
        If the file does not exist.
    """
    fmt = format or _detect_format(path)

    if fmt == "json":
        return _load_json(path)
    elif fmt == "yaml":
        return _load_yaml(path)
    elif fmt == "toml":
        return _load_toml(path)
    else:
        raise ValueError(
            f"Unsupported schema format '{fmt}'. Use 'json', 'yaml', or 'toml'."
        )


def _detect_format(path: str) -> str:
    """Detect schema format from file extension."""
    lower = path.lower()
    if lower.endswith(".json"):
        return "json"
    if lower.endswith((".yaml", ".yml")):
        return "yaml"
    if lower.endswith(".toml"):
        return "toml"
    raise ValueError(
        f"Cannot determine schema format from '{path}'. "
        "Use a .json, .yaml/.yml, or .toml extension, "
        "or specify format explicitly."
    )


# ------------------------------------------------------------------
# JSON (stdlib — zero dependencies)
# ------------------------------------------------------------------


def _save_json(d: dict[str, Any], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        _json.dump(d, f, indent=2, ensure_ascii=False)
        f.write("\n")


def _load_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = _json.load(f)
    if not isinstance(data, dict):
        raise ValueError(
            f"Expected a JSON object at top level, got {type(data).__name__}"
        )
    return data


# ------------------------------------------------------------------
# YAML (pure-Python fallback — zero dependencies)
# ------------------------------------------------------------------
# We implement a minimal YAML parser/emitter that handles the subset
# needed for schema definitions (strings, numbers, lists, dicts,
# booleans, null).  This avoids requiring PyYAML as a dependency.


def _save_yaml(d: dict[str, Any], path: str) -> None:
    lines = _yaml_dump(d, indent=0)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


def _yaml_dump(obj: Any, indent: int) -> list[str]:
    """Minimal YAML emitter for schema dicts."""
    prefix = "  " * indent
    lines: list[str] = []

    if isinstance(obj, dict):
        for key, val in obj.items():
            if isinstance(val, dict):
                lines.append(f"{prefix}{key}:")
                lines.extend(_yaml_dump(val, indent + 1))
            elif isinstance(val, list):
                lines.append(f"{prefix}{key}:")
                for item in val:
                    if isinstance(item, list):
                        # List of lists (unique_together)
                        items_str = ", ".join(_yaml_scalar(v) for v in item)
                        lines.append(f"{prefix}  - [{items_str}]")
                    elif isinstance(item, dict):
                        # First key inline, rest indented
                        first = True
                        for k2, v2 in item.items():
                            if first:
                                lines.append(f"{prefix}  - {k2}: {_yaml_scalar(v2)}")
                                first = False
                            else:
                                lines.append(f"{prefix}    {k2}: {_yaml_scalar(v2)}")
                    else:
                        lines.append(f"{prefix}  - {_yaml_scalar(item)}")
            else:
                lines.append(f"{prefix}{key}: {_yaml_scalar(val)}")
    elif isinstance(obj, list):
        for item in obj:
            lines.append(f"{prefix}- {_yaml_scalar(item)}")
    else:
        lines.append(f"{prefix}{_yaml_scalar(obj)}")

    return lines


def _yaml_scalar(val: Any) -> str:
    """Format a scalar value for YAML output."""
    if val is None:
        return "null"
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        return str(val)
    if isinstance(val, str):
        # Quote if the string could be misinterpreted
        if (
            val == ""
            or val in ("true", "false", "null", "yes", "no", "on", "off")
            or val[0] in ("[", "{", "'", '"', "&", "*", "!", "|", ">", "%", "@", "`")
            or ": " in val
            or val[0] == "#"
            or "," in val
        ):
            # Use double-quoted form with escaping
            escaped = val.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{escaped}"'
        return val
    return str(val)


def _load_yaml(path: str) -> dict[str, Any]:
    """Minimal YAML parser for schema definitions.

    Handles the subset of YAML needed for schema files:
    mappings, sequences, strings, numbers, booleans, null.
    """
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    return _yaml_parse(text)


def _yaml_parse(text: str) -> dict[str, Any]:
    """Parse a YAML string into a dict."""
    lines = text.split("\n")
    # Remove empty lines and comments
    cleaned: list[tuple[int, str]] = []
    for line in lines:
        stripped = line.rstrip()
        if not stripped or stripped.lstrip().startswith("#"):
            continue
        # Calculate indent level
        content = stripped.lstrip()
        indent_chars = len(stripped) - len(content)
        cleaned.append((indent_chars, content))

    result, _ = _yaml_parse_mapping(cleaned, 0, 0)
    return result


def _yaml_parse_mapping(
    lines: list[tuple[int, str]],
    start: int,
    base_indent: int,
) -> tuple[dict[str, Any], int]:
    """Parse a YAML mapping starting at the given line."""
    result: dict[str, Any] = {}
    i = start

    while i < len(lines):
        indent, content = lines[i]
        if indent < base_indent:
            break

        if indent > base_indent and i > start:
            break

        # Parse key: value
        colon_pos = content.find(":")
        if colon_pos == -1:
            i += 1
            continue

        key = content[:colon_pos].strip()
        rest = content[colon_pos + 1 :].strip()

        if rest:
            # Inline value
            result[key] = _yaml_parse_value(rest)
            i += 1
        else:
            # Block value — check next line
            if i + 1 < len(lines):
                next_indent, next_content = lines[i + 1]
                if next_indent > indent:
                    if next_content.startswith("- "):
                        # Sequence
                        val, i = _yaml_parse_sequence(lines, i + 1, next_indent)
                        result[key] = val
                    else:
                        # Nested mapping
                        val, i = _yaml_parse_mapping(lines, i + 1, next_indent)
                        result[key] = val
                else:
                    result[key] = None
                    i += 1
            else:
                result[key] = None
                i += 1

    return result, i


def _yaml_parse_sequence(
    lines: list[tuple[int, str]],
    start: int,
    base_indent: int,
) -> tuple[list[Any], int]:
    """Parse a YAML sequence starting at the given line."""
    result: list[Any] = []
    i = start

    while i < len(lines):
        indent, content = lines[i]
        if indent < base_indent:
            break

        if not content.startswith("- "):
            break

        item_str = content[2:].strip()

        # Check for inline list [a, b, c]
        if item_str.startswith("[") and item_str.endswith("]"):
            inner = item_str[1:-1]
            items = [_yaml_parse_scalar(s.strip()) for s in inner.split(",")]
            result.append(items)
            i += 1
        elif (
            ":" in item_str
            and not item_str.startswith('"')
            and not item_str.startswith("'")
        ):
            # Inline mapping item
            mapping: dict[str, Any] = {}
            colon_pos = item_str.find(":")
            k = item_str[:colon_pos].strip()
            v = item_str[colon_pos + 1 :].strip()
            mapping[k] = _yaml_parse_value(v) if v else None
            # Read continuation lines at deeper indent
            while i + 1 < len(lines):
                next_indent, next_content = lines[i + 1]
                if next_indent <= indent:
                    break
                if ":" in next_content:
                    cp = next_content.find(":")
                    k2 = next_content[:cp].strip()
                    v2 = next_content[cp + 1 :].strip()
                    mapping[k2] = _yaml_parse_value(v2) if v2 else None
                i += 1
            result.append(mapping)
            i += 1
        else:
            result.append(_yaml_parse_value(item_str))
            i += 1

    return result, i


def _yaml_parse_value(s: str) -> Any:
    """Parse an inline YAML value."""
    if not s:
        return None

    # Inline list
    if s.startswith("[") and s.endswith("]"):
        inner = s[1:-1]
        if not inner.strip():
            return []
        return [_yaml_parse_scalar(item.strip()) for item in inner.split(",")]

    # Inline dict
    if s.startswith("{") and s.endswith("}"):
        inner = s[1:-1]
        if not inner.strip():
            return {}
        result: dict[str, Any] = {}
        for pair in inner.split(","):
            if ":" in pair:
                k, v = pair.split(":", 1)
                result[k.strip()] = _yaml_parse_scalar(v.strip())
        return result

    return _yaml_parse_scalar(s)


def _yaml_parse_scalar(s: str) -> Any:
    """Parse a YAML scalar value."""
    if not s:
        return None

    # Remove quotes
    if (s.startswith('"') and s.endswith('"')) or (
        s.startswith("'") and s.endswith("'")
    ):
        return s[1:-1].replace('\\"', '"').replace("\\\\", "\\")

    # Booleans
    if s.lower() in ("true", "yes", "on"):
        return True
    if s.lower() in ("false", "no", "off"):
        return False

    # Null
    if s.lower() in ("null", "~"):
        return None

    # Integer
    try:
        return int(s)
    except ValueError:
        pass

    # Float
    try:
        return float(s)
    except ValueError:
        pass

    return s


# ------------------------------------------------------------------
# TOML (Python 3.11+ stdlib tomllib, or minimal fallback)
# ------------------------------------------------------------------


def _save_toml(d: dict[str, Any], path: str) -> None:
    """Write a schema dict as TOML."""
    lines = _toml_dump(d)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


def _toml_dump(d: dict[str, Any], prefix: str = "") -> list[str]:
    """Minimal TOML emitter for schema dicts."""
    lines: list[str] = []
    # Emit simple key-value pairs first, then tables
    simple_keys: list[str] = []
    table_keys: list[str] = []

    for key in d:
        val = d[key]
        if isinstance(val, dict):
            table_keys.append(key)
        elif isinstance(val, list) and val and isinstance(val[0], (dict, list)):
            table_keys.append(key)
        else:
            simple_keys.append(key)

    for key in simple_keys:
        val = d[key]
        lines.append(f"{key} = {_toml_value(val)}")

    for key in table_keys:
        val = d[key]
        full_key = f"{prefix}{key}" if not prefix else f"{prefix}.{key}"

        if isinstance(val, dict):
            lines.append("")
            lines.append(f"[{full_key}]")
            for k2, v2 in val.items():
                if isinstance(v2, dict):
                    lines.extend(_toml_dump({k2: v2}, prefix=full_key))
                else:
                    lines.append(f"{k2} = {_toml_value(v2)}")
        elif isinstance(val, list):
            # Array of arrays or array of tables
            for item in val:
                if isinstance(item, dict):
                    lines.append("")
                    lines.append(f"[[{full_key}]]")
                    for k2, v2 in item.items():
                        lines.append(f"{k2} = {_toml_value(v2)}")
                elif isinstance(item, (list, tuple)):
                    # unique_together: array of arrays — emit as inline
                    pass
            # For unique_together arrays, emit the whole thing as inline
            if val and isinstance(val[0], (list, tuple)):
                inner = ", ".join(
                    "[" + ", ".join(f'"{c}"' for c in group) + "]" for group in val
                )
                lines.append(f"{key} = [{inner}]")

    return lines


def _toml_value(val: Any) -> str:
    """Format a value for TOML output."""
    if val is None:
        # TOML doesn't have null — use empty string
        return '""'
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        return str(val)
    if isinstance(val, str):
        escaped = val.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    if isinstance(val, (list, tuple)):
        items = ", ".join(_toml_value(v) for v in val)
        return f"[{items}]"
    return f'"{val}"'


def _load_toml(path: str) -> dict[str, Any]:
    """Load a TOML file.

    Uses stdlib ``tomllib`` (Python 3.11+) when available,
    otherwise falls back to a minimal parser.
    """
    try:
        import tomllib  # Python 3.11+
    except ModuleNotFoundError:
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ModuleNotFoundError:
            return _load_toml_fallback(path)

    with open(path, "rb") as f:
        return tomllib.load(f)


def _load_toml_fallback(path: str) -> dict[str, Any]:
    """Minimal TOML parser for schema definitions.

    Handles the subset of TOML needed for schema files:
    bare keys, string/number values, arrays, and tables.
    """
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    result: dict[str, Any] = {}
    current_table: dict[str, Any] = result

    for line in text.split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        # Table header [key] or [[key]]
        if stripped.startswith("[[") and stripped.endswith("]]"):
            table_key = stripped[2:-2].strip()
            parts = [p.strip() for p in table_key.split(".")]
            # Array of tables
            current_table = result
            for part in parts[:-1]:
                current_table = current_table.setdefault(part, {})
            last = parts[-1]
            if last not in current_table:
                current_table[last] = []
            entry: dict[str, Any] = {}
            current_table[last].append(entry)
            current_table = entry
            continue

        if stripped.startswith("[") and stripped.endswith("]"):
            table_key = stripped[1:-1].strip()
            parts = [p.strip() for p in table_key.split(".")]
            current_table = result
            for part in parts:
                current_table = current_table.setdefault(part, {})
            continue

        # Key = value
        if "=" in stripped:
            eq_pos = stripped.index("=")
            key = stripped[:eq_pos].strip().strip('"').strip("'")
            val_str = stripped[eq_pos + 1 :].strip()
            current_table[key] = _toml_parse_value(val_str)

    return result


def _toml_parse_value(s: str) -> Any:
    """Parse a TOML value string."""
    s = s.strip()
    if not s:
        return ""

    # Quoted string
    if s.startswith('"""') and s.endswith('"""'):
        return s[3:-3]
    if s.startswith("'''") and s.endswith("'''"):
        return s[3:-3]
    if s.startswith('"') and s.endswith('"'):
        return s[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    if s.startswith("'") and s.endswith("'"):
        return s[1:-1]

    # Boolean
    if s == "true":
        return True
    if s == "false":
        return False

    # Array
    if s.startswith("[") and s.endswith("]"):
        return _toml_parse_array(s[1:-1].strip())

    # Integer
    try:
        return int(s)
    except ValueError:
        pass

    # Float
    try:
        return float(s)
    except ValueError:
        pass

    return s


def _toml_parse_array(s: str) -> list[Any]:
    """Parse a TOML array interior (without outer brackets)."""
    if not s:
        return []

    items: list[Any] = []
    depth = 0
    current = ""

    for ch in s:
        if ch == "[":
            depth += 1
            current += ch
        elif ch == "]":
            depth -= 1
            current += ch
        elif ch == "," and depth == 0:
            val = current.strip()
            if val:
                items.append(_toml_parse_value(val))
            current = ""
        else:
            current += ch

    val = current.strip()
    if val:
        items.append(_toml_parse_value(val))

    return items
