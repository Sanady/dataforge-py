"""Chaos Testing — Data Quality Resilience Testing.

Real-world scenario: You are building an ETL pipeline or data ingestion
service and need to verify it handles messy, real-world data gracefully.
The ChaosTransformer injects realistic data quality issues into clean
generated data, simulating problems you'd encounter in production:
nulls, type mismatches, encoding issues, truncation, and more.

This example demonstrates:
- Injecting null values to test NULL handling
- Type mismatch injection for schema validation testing
- Boundary/edge-case values (SQL injection, XSS, overflow)
- Whitespace and encoding chaos
- Format inconsistencies
- Duplicate row injection
- Targeting specific columns
"""

from dataforge import DataForge
from dataforge.chaos import ChaosTransformer

forge = DataForge(seed=42)

# Generate clean baseline data
schema = forge.schema(
    {
        "Name": "person.full_name",
        "Email": "internet.email",
        "City": "address.city",
        "Phone": "phone.phone_number",
    }
)
clean_rows = schema.generate(count=20)

print("=== Clean Baseline Data ===\n")
for row in clean_rows[:5]:
    print(f"  {row}")
print(f"  ... ({len(clean_rows)} total rows)\n")

# --- Example 1: Null injection -------------------------------------------

print("=== Null Injection (10% rate) ===\n")

chaos_nulls = ChaosTransformer(null_rate=0.10, seed=42)
dirty_rows = chaos_nulls.transform(clean_rows)

null_count = sum(1 for row in dirty_rows for val in row.values() if val is None)
total_cells = len(dirty_rows) * len(dirty_rows[0])
print(f"Null cells: {null_count}/{total_cells} ({null_count / total_cells:.1%})")
print("Sample rows with nulls:")
for row in dirty_rows[:5]:
    print(f"  {row}")
print()

# --- Example 2: Type mismatch injection ----------------------------------

print("=== Type Mismatch Injection (5% rate) ===\n")

chaos_types = ChaosTransformer(type_mismatch_rate=0.05, seed=42)
type_dirty = chaos_types.transform(clean_rows)

# Find rows where types changed
for row in type_dirty[:10]:
    for key, val in row.items():
        if not isinstance(val, str) and val is not None:
            print(f"  Type mismatch in '{key}': {val!r} (type: {type(val).__name__})")
print()

# --- Example 3: Boundary value injection ----------------------------------

print("=== Boundary Values (SQL injection, XSS, overflows) ===\n")

chaos_boundary = ChaosTransformer(boundary_rate=0.15, seed=42)
boundary_dirty = chaos_boundary.transform(clean_rows)

for row in boundary_dirty:
    for key, val in row.items():
        if isinstance(val, str) and (
            "DROP TABLE" in val or "<script>" in val or val == "NULL" or len(val) > 100
        ):
            preview = val[:80] + "..." if len(val) > 80 else val
            print(f"  Boundary in '{key}': {preview!r}")
print()

# --- Example 4: Full chaos mode ------------------------------------------

print("=== Full Chaos Mode (all transformations active) ===\n")

full_chaos = ChaosTransformer(
    null_rate=0.03,
    type_mismatch_rate=0.02,
    boundary_rate=0.02,
    duplicate_rate=0.05,
    whitespace_rate=0.03,
    encoding_rate=0.02,
    format_rate=0.03,
    truncation_rate=0.02,
    seed=42,
)
print(f"Transformer: {full_chaos}")
print()

chaotic_rows = full_chaos.transform(clean_rows)
print(f"Original rows: {len(clean_rows)}")
print(f"After chaos:   {len(chaotic_rows)} (duplicates may add rows)")
print()

# Categorize issues found
issues = {"null": 0, "type_mismatch": 0, "modified_string": 0}
for orig, dirty in zip(clean_rows, chaotic_rows[: len(clean_rows)]):
    for key in orig:
        if dirty.get(key) is None and orig[key] is not None:
            issues["null"] += 1
        elif type(dirty.get(key)) is not type(orig[key]):
            issues["type_mismatch"] += 1
        elif dirty.get(key) != orig[key]:
            issues["modified_string"] += 1

print("Issues injected:")
for issue_type, count in issues.items():
    print(f"  {issue_type}: {count}")
print()

# --- Example 5: Target specific columns only ------------------------------

print("=== Target Specific Columns ===\n")

# Only inject chaos into Email and Phone columns
targeted_chaos = ChaosTransformer(
    null_rate=0.20,
    truncation_rate=0.10,
    seed=42,
)
targeted_dirty = targeted_chaos.transform(clean_rows, columns=["Email", "Phone"])

# Verify Name and City are untouched
names_unchanged = all(
    targeted_dirty[i]["Name"] == clean_rows[i]["Name"] for i in range(len(clean_rows))
)
cities_unchanged = all(
    targeted_dirty[i]["City"] == clean_rows[i]["City"] for i in range(len(clean_rows))
)
print(f"Name column unchanged:  {names_unchanged}")
print(f"City column unchanged:  {cities_unchanged}")

email_nulls = sum(1 for r in targeted_dirty if r["Email"] is None)
phone_nulls = sum(1 for r in targeted_dirty if r["Phone"] is None)
print(f"Email nulls injected:   {email_nulls}")
print(f"Phone nulls injected:   {phone_nulls}")
