"""dataforge CLI — generate fake data from the command line.

Usage::

    dataforge --count 100 --format csv name email phone
    dataforge --count 10 --format json first_name last_name city
    dataforge --locale de_DE --count 5 full_name address
    dataforge --list-fields
    dataforge --list-providers
    dataforge --version

    # SQL output
    dataforge --format sql --table users first_name email city

    # TSV output
    dataforge --format tsv first_name email

    # Custom delimiter
    dataforge --format csv --delimiter "|" first_name email

    # Column renaming
    dataforge "Name=full_name" "Email=email" "City=city"

    # Streaming (memory-efficient for large counts)
    dataforge --stream --count 1000000 --format csv -o data.csv first_name email

    # Unique values
    dataforge --unique --count 50 first_name

Supported output formats: text, csv, tsv, json, jsonl, sql
"""

import argparse
import csv
import io
import json
import sys

from dataforge import DataForge, __version__
from dataforge.registry import get_field_map


def _parse_field_spec(spec: str) -> tuple[str, str]:
    """Parse a field spec like ``"Name=full_name"`` or ``"email"``.

    Returns ``(column_name, field_name)``.
    """
    if "=" in spec:
        col_name, field_name = spec.split("=", 1)
        return col_name.strip(), field_name.strip()
    return spec, spec


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dataforge",
        description="Generate fake data for testing from the command line.",
    )
    parser.add_argument(
        "fields",
        nargs="*",
        help="Fields to generate (e.g. first_name email city). "
        'Use "ColName=field_name" for column renaming. '
        "Use --list-fields to see all available fields.",
    )
    parser.add_argument(
        "-n",
        "--count",
        type=int,
        default=10,
        help="Number of rows to generate (default: 10).",
    )
    parser.add_argument(
        "-f",
        "--format",
        choices=("text", "csv", "tsv", "json", "jsonl", "sql"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "-l",
        "--locale",
        default="en_US",
        help="Locale for data generation (default: en_US).",
    )
    parser.add_argument(
        "-s",
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible output.",
    )
    parser.add_argument(
        "--list-fields",
        action="store_true",
        help="List all available field names and exit.",
    )
    parser.add_argument(
        "--list-providers",
        action="store_true",
        help="List all available providers and exit.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        metavar="FILE",
        help="Write output to FILE instead of stdout.",
    )
    parser.add_argument(
        "--no-header",
        action="store_true",
        help="Omit header row in text, csv, and tsv output formats.",
    )
    parser.add_argument(
        "--delimiter",
        default=None,
        help="Field delimiter for CSV output (default: comma).",
    )
    parser.add_argument(
        "--table",
        default="data",
        help='Table name for SQL output (default: "data").',
    )
    parser.add_argument(
        "--dialect",
        choices=("sqlite", "mysql", "postgresql"),
        default="sqlite",
        help="SQL dialect for SQL output (default: sqlite).",
    )
    parser.add_argument(
        "--unique",
        action="store_true",
        help="Ensure all generated values are unique per column.",
    )
    parser.add_argument(
        "--stream",
        action="store_true",
        help="Stream output to file for memory-efficient large generation. "
        "Requires --output.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"dataforge {__version__}",
    )
    return parser


def _format_value(value: object) -> str:
    """Convert a value to its string representation for text output."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def main(argv: list[str] | None = None) -> int:
    """Entry point for the dataforge CLI."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    field_map = get_field_map()

    # --list-providers
    if args.list_providers:
        from dataforge.registry import get_provider_info

        info = get_provider_info()
        for name in sorted(info):
            cls = info[name][0]
            fm = getattr(cls, "_field_map", {})
            print(f"  {name:20s}  ({len(fm)} fields)")
        return 0

    # --list-fields
    if args.list_fields:
        # Group fields by provider
        for name in sorted(field_map.keys()):
            provider, method = field_map[name]
            print(f"  {name:24s}  ({provider}.{method})")
        return 0

    if not args.fields:
        # Default fields
        args.fields = ["first_name", "last_name", "email"]

    # Parse field specs (handle column renaming "Name=full_name")
    field_specs = [_parse_field_spec(f) for f in args.fields]
    headers = [col_name for col_name, _ in field_specs]
    field_names = [field_name for _, field_name in field_specs]

    # Validate fields before generating
    for col_name, field_name in field_specs:
        if field_name not in field_map and "." not in field_name:
            print(
                f"Error: unknown field '{field_name}'. Use --list-fields to see options.",
                file=sys.stderr,
            )
            return 1

    # Validate --stream requires --output
    if args.stream and not args.output:
        print(
            "Error: --stream requires --output to specify a file path.",
            file=sys.stderr,
        )
        return 1

    # Validate --format sql does not combine with --stream (not supported)
    if args.stream and args.format == "sql":
        print(
            "Error: --stream is not supported with --format sql.",
            file=sys.stderr,
        )
        return 1

    forge = DataForge(locale=args.locale, seed=args.seed)

    # Build field dict for Schema (supports column renaming)
    if any(col != field for col, field in field_specs):
        # Column renaming in use — build dict
        fields_arg: list[str] | dict[str, str] = {
            col: field for col, field in field_specs
        }
    else:
        fields_arg = field_names

    # Resolve delimiter
    delimiter = args.delimiter
    if delimiter is None:
        delimiter = "\t" if args.format == "tsv" else ","

    # --stream mode: write directly to file
    if args.stream:
        fmt = args.format
        path = args.output
        if fmt in ("csv", "tsv"):
            written = forge.stream_to_csv(
                fields_arg,
                path=path,
                count=args.count,
                delimiter=delimiter,
            )
            _progress_done(written)
        elif fmt == "jsonl":
            written = forge.stream_to_jsonl(
                fields_arg,
                path=path,
                count=args.count,
            )
            _progress_done(written)
        elif fmt == "json":
            # JSON array can't easily stream, but we can generate
            # and write — still respects --output
            content = forge.to_json(fields_arg, count=args.count)
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            _progress_done(args.count)
        elif fmt == "text":
            # Stream text rows to file
            schema = forge.schema(fields_arg)
            written = 0
            with open(path, "w", encoding="utf-8") as f:
                for row in schema.stream(args.count):
                    vals = [_format_value(row[h]) for h in headers]
                    f.write("\t".join(vals) + "\n")
                    written += 1
            _progress_done(written)
        return 0

    # Non-streaming mode: generate all data in memory
    if args.unique:
        # Generate with unique proxy — row at a time
        schema = forge.schema(fields_arg)
        rows: list[dict[str, object]] = []
        seen: dict[str, set[object]] = {h: set() for h in headers}
        attempts = 0
        max_attempts = args.count * 100
        while len(rows) < args.count and attempts < max_attempts:
            attempts += 1
            row = next(schema.stream(1))
            is_unique = True
            for h in headers:
                if row[h] in seen[h]:
                    is_unique = False
                    break
            if is_unique:
                for h in headers:
                    seen[h].add(row[h])
                rows.append(row)
        if len(rows) < args.count:
            print(
                f"Warning: could only generate {len(rows)} unique rows "
                f"(requested {args.count}).",
                file=sys.stderr,
            )
    else:
        rows = forge.to_dict(fields_arg, count=args.count)

    # Determine output destination
    out_file = None
    if args.output:
        out_file = open(args.output, "w", encoding="utf-8", newline="")
    out = out_file or sys.stdout

    try:
        fmt = args.format

        if fmt == "text":
            # Aligned columns
            str_rows = [{h: _format_value(row[h]) for h in headers} for row in rows]
            col_widths = [len(h) for h in headers]
            for row in str_rows:
                for i, h in enumerate(headers):
                    col_widths[i] = max(col_widths[i], len(row[h]))

            if not args.no_header:
                header_line = "  ".join(
                    h.ljust(col_widths[i]) for i, h in enumerate(headers)
                )
                sep_line = "  ".join("-" * col_widths[i] for i in range(len(headers)))
                print(header_line, file=out)
                print(sep_line, file=out)
            for row in str_rows:
                line = "  ".join(
                    row[h].ljust(col_widths[i]) for i, h in enumerate(headers)
                )
                print(line, file=out)

        elif fmt in ("csv", "tsv"):
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=headers, delimiter=delimiter)
            if not args.no_header:
                writer.writeheader()
            # Convert all values to strings for CSV
            for row in rows:
                writer.writerow({h: _format_value(row[h]) for h in headers})
            print(buf.getvalue(), end="", file=out)

        elif fmt == "json":
            # Serialize with native types (int, bool stay as numbers/bools)
            print(
                json.dumps(
                    [{h: row[h] for h in headers} for row in rows],
                    indent=2,
                    ensure_ascii=False,
                    default=str,
                ),
                file=out,
            )

        elif fmt == "jsonl":
            for row in rows:
                print(
                    json.dumps(
                        {h: row[h] for h in headers},
                        ensure_ascii=False,
                        default=str,
                    ),
                    file=out,
                )

        elif fmt == "sql":
            schema = forge.schema(fields_arg)
            sql = schema.to_sql(
                table=args.table,
                count=args.count,
                dialect=args.dialect,
            )
            print(sql, end="", file=out)

    finally:
        if out_file is not None:
            out_file.close()

    return 0


def _progress_done(count: int) -> None:
    """Print a completion message to stderr for streaming operations."""
    print(f"Done: {count:,} rows written.", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
