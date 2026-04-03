"""dataforge CLI — generate fake data from the command line."""

import argparse
import csv
import io
import json
import sys

from dataforge import DataForge, __version__
from dataforge.registry import get_field_map


def _parse_field_spec(spec: str) -> tuple[str, str]:
    """Parse a field spec like ``"Name=full_name"`` or ``"email"``."""
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
        "--encoding",
        default="utf-8",
        help="Character encoding for file output (default: utf-8).",
    )
    parser.add_argument(
        "--compress",
        action="store_true",
        help="Gzip-compress file output. Auto-detected if --output ends with .gz.",
    )
    parser.add_argument(
        "--null-fields",
        default=None,
        metavar="SPEC",
        help='Make fields nullable. Format: "email:0.2,phone:0.5" '
        "(field_name:probability pairs, comma-separated).",
    )
    parser.add_argument(
        "--schema",
        default=None,
        metavar="FILE",
        help="Load schema definition from a JSON, YAML, or TOML file. "
        "When used, field arguments on the command line are ignored.",
    )
    parser.add_argument(
        "--save-schema",
        default=None,
        metavar="FILE",
        help="Save the current field specification to a schema file "
        "(JSON, YAML, or TOML — detected from extension) and exit.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"dataforge {__version__}",
    )
    parser.add_argument(
        "--tui",
        action="store_true",
        help="Launch the interactive TUI schema builder. Requires textual.",
    )
    parser.add_argument(
        "--infer",
        default=None,
        metavar="CSV_FILE",
        help="Infer a schema from a CSV file and generate data matching it.",
    )
    parser.add_argument(
        "--anonymize",
        default=None,
        metavar="CSV_FILE",
        help="Anonymize a CSV file by replacing PII with fake data.",
    )
    parser.add_argument(
        "--chaos",
        default=None,
        metavar="RATE",
        type=float,
        help="Apply chaos/data-quality transformations at the given rate (0.0-1.0).",
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

    if args.tui:
        try:
            from dataforge.tui import launch

            launch()
        except ModuleNotFoundError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
        return 0

    if args.infer:
        forge = DataForge(locale=args.locale, seed=args.seed)
        try:
            schema = forge.infer_schema_from_csv(args.infer)
        except Exception as exc:
            print(f"Error inferring schema: {exc}", file=sys.stderr)
            return 1
        rows = schema.generate(count=args.count)
        print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
        return 0

    if args.anonymize:
        forge = DataForge(locale=args.locale, seed=args.seed)
        from dataforge.anonymizer import Anonymizer

        anonymizer = Anonymizer(forge)
        output = args.output or args.anonymize.replace(".csv", "_anonymized.csv")
        try:
            anonymizer.anonymize_csv(args.anonymize, output)
        except Exception as exc:
            print(f"Error anonymizing: {exc}", file=sys.stderr)
            return 1
        print(f"Anonymized output written to {output}", file=sys.stderr)
        return 0

    if args.list_providers:
        from dataforge.registry import get_provider_info

        info = get_provider_info()
        for name in sorted(info):
            cls = info[name][0]
            fm = getattr(cls, "_field_map", {})
            print(f"  {name:20s}  ({len(fm)} fields)")
        return 0

    if args.list_fields:
        for name in sorted(field_map.keys()):
            provider, method = field_map[name]
            print(f"  {name:24s}  ({provider}.{method})")
        return 0

    if not args.fields:
        if not args.schema:
            args.fields = ["first_name", "last_name", "email"]
        else:
            args.fields = []

    field_specs = [_parse_field_spec(f) for f in args.fields]
    headers = [col_name for col_name, _ in field_specs]
    field_names = [field_name for _, field_name in field_specs]

    if not args.schema:
        for col_name, field_name in field_specs:
            if field_name not in field_map and "." not in field_name:
                print(
                    f"Error: unknown field '{field_name}'. Use --list-fields to see options.",
                    file=sys.stderr,
                )
                return 1

    if args.stream and not args.output:
        print(
            "Error: --stream requires --output to specify a file path.",
            file=sys.stderr,
        )
        return 1

    if args.stream and args.format == "sql":
        print(
            "Error: --stream is not supported with --format sql.",
            file=sys.stderr,
        )
        return 1

    forge = DataForge(locale=args.locale, seed=args.seed)

    if field_specs and any(col != field for col, field in field_specs):
        fields_arg: list[str] | dict[str, str] = {
            col: field for col, field in field_specs
        }
    else:
        fields_arg = field_names

    null_fields: dict[str, float] | None = None
    if args.null_fields:
        null_fields = {}
        for pair in args.null_fields.split(","):
            pair = pair.strip()
            if ":" not in pair:
                print(
                    f"Error: invalid --null-fields format '{pair}'. "
                    "Expected 'field_name:probability'.",
                    file=sys.stderr,
                )
                return 1
            name, prob_str = pair.split(":", 1)
            try:
                prob = float(prob_str)
            except ValueError:
                print(
                    f"Error: invalid probability '{prob_str}' for field '{name}'.",
                    file=sys.stderr,
                )
                return 1
            if not 0.0 <= prob <= 1.0:
                print(
                    f"Error: probability for '{name}' must be between 0.0 and 1.0, "
                    f"got {prob}.",
                    file=sys.stderr,
                )
                return 1
            null_fields[name.strip()] = prob

    if args.schema:
        from dataforge.schema_io import load_schema, dict_to_schema_args

        try:
            schema_def = load_schema(args.schema)
        except (FileNotFoundError, ValueError) as exc:
            print(f"Error loading schema: {exc}", file=sys.stderr)
            return 1

        schema_fields, schema_count, schema_null, schema_unique = dict_to_schema_args(
            schema_def
        )

        count = args.count if args.count != 10 else schema_count
        args.count = count

        if isinstance(schema_fields, dict):
            fields_arg = schema_fields
            headers = list(schema_fields.keys())
        else:
            fields_arg = schema_fields
            headers = list(schema_fields)

        if null_fields is None and schema_null:
            null_fields = schema_null

    if args.save_schema:
        from dataforge.schema_io import schema_to_dict, save_schema as _save_schema

        try:
            d = schema_to_dict(
                fields=fields_arg,
                count=args.count,
                null_fields=null_fields,
            )
            _save_schema(d, args.save_schema)
        except ValueError as exc:
            print(f"Error saving schema: {exc}", file=sys.stderr)
            return 1
        print(
            f"Schema saved to {args.save_schema}",
            file=sys.stderr,
        )
        return 0

    delimiter = args.delimiter
    if delimiter is None:
        delimiter = "\t" if args.format == "tsv" else ","

    encoding = args.encoding
    compress: bool | None = True if args.compress else None

    chaos_arg = None
    if args.chaos is not None:
        from dataforge.chaos import ChaosTransformer

        chaos_arg = ChaosTransformer(null_rate=args.chaos)

    if args.stream:
        fmt = args.format
        path = args.output
        schema = forge.schema(fields_arg, null_fields=null_fields, chaos=chaos_arg)
        if fmt in ("csv", "tsv"):
            written = schema.stream_to_csv(
                path=path,
                count=args.count,
                delimiter=delimiter,
                encoding=encoding,
                compress=compress,
            )
            _progress_done(written)
        elif fmt == "jsonl":
            written = schema.stream_to_jsonl(
                path=path,
                count=args.count,
                encoding=encoding,
                compress=compress,
            )
            _progress_done(written)
        elif fmt == "json":
            schema_j = forge.schema(
                fields_arg, null_fields=null_fields, chaos=chaos_arg
            )
            schema_j.to_json(
                count=args.count,
                path=path,
                encoding=encoding,
                compress=compress,
            )
            _progress_done(args.count)
        elif fmt == "text":
            from dataforge.schema import _open_file

            written = 0
            with _open_file(path, "w", encoding=encoding, compress=compress) as f:
                for row in schema.stream(args.count):
                    vals = [_format_value(row[h]) for h in headers]
                    f.write("\t".join(vals) + "\n")
                    written += 1
            _progress_done(written)
        return 0

    if args.unique:
        schema = forge.schema(fields_arg, null_fields=null_fields, chaos=chaos_arg)
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
        schema_gen = forge.schema(fields_arg, null_fields=null_fields, chaos=chaos_arg)
        rows = schema_gen.generate(count=args.count)

    out_file = None
    if args.output:
        from dataforge.schema import _open_file

        _ctx = _open_file(
            args.output, "w", encoding=encoding, compress=compress, newline=""
        )
        out_file = _ctx.__enter__()
    else:
        _ctx = None
    out = out_file or sys.stdout

    try:
        fmt = args.format

        if fmt == "text":
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
            for row in rows:
                writer.writerow({h: _format_value(row[h]) for h in headers})
            print(buf.getvalue(), end="", file=out)

        elif fmt == "json":
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
            schema_sql = forge.schema(fields_arg, null_fields=null_fields)
            sql = schema_sql.to_sql(
                table=args.table,
                count=args.count,
                dialect=args.dialect,
            )
            print(sql, end="", file=out)

    finally:
        if _ctx is not None:
            _ctx.__exit__(None, None, None)

    return 0


def _progress_done(count: int) -> None:
    """Print a completion message to stderr for streaming operations."""
    print(f"Done: {count:,} rows written.", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
