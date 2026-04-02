"""Schema Inference — Auto-detect and recreate data patterns.

Real-world scenario: You receive a CSV or dataset from a client and need
to generate synthetic data that matches its structure for testing. Instead
of manually mapping each column, the SchemaInferrer analyzes the data and
automatically detects column types (email, phone, UUID, date, etc.).

This example demonstrates:
- Inferring schema from a list of dicts
- Inferring schema from a CSV file
- Inspecting the analysis results
- Generating synthetic data that matches the original structure
"""

import csv
import os

from dataforge import DataForge
from dataforge.inference import SchemaInferrer

forge = DataForge(seed=42)

# --- Example 1: Infer from a list of dicts --------------------------------

print("=== Infer Schema from Records ===\n")

# Simulate a dataset you received from a client
sample_data = [
    {
        "full_name": "Alice Johnson",
        "email": "alice.johnson@example.com",
        "phone": "(555) 123-4567",
        "city": "New York",
        "signup_date": "2024-03-15",
        "is_active": "true",
    },
    {
        "full_name": "Bob Smith",
        "email": "bob.smith@gmail.com",
        "phone": "(555) 987-6543",
        "city": "Los Angeles",
        "signup_date": "2024-01-20",
        "is_active": "false",
    },
    {
        "full_name": "Carol Williams",
        "email": "carol.w@company.org",
        "phone": "(555) 456-7890",
        "city": "Chicago",
        "signup_date": "2024-06-01",
        "is_active": "true",
    },
    {
        "full_name": "David Brown",
        "email": "david.brown@outlook.com",
        "phone": "(555) 321-0987",
        "city": "Houston",
        "signup_date": "2024-02-10",
        "is_active": "true",
    },
    {
        "full_name": "Eve Davis",
        "email": "eve.davis@test.net",
        "phone": "(555) 654-3210",
        "city": "Phoenix",
        "signup_date": "2024-05-22",
        "is_active": "false",
    },
]

inferrer = SchemaInferrer(forge)
schema = inferrer.from_records(sample_data)

# Print what was detected
print(inferrer.describe())
print()

# Generate synthetic data that matches the original structure
print("Generated synthetic data matching the original structure:")
synthetic_rows = schema.generate(count=5)
for row in synthetic_rows:
    print(f"  {row}")
print()

# --- Example 2: Inspect column analyses -----------------------------------

print("=== Column Analysis Details ===\n")

for analysis in inferrer.analyses:
    print(f"Column: {analysis.name}")
    print(f"  Base type:      {analysis.base_type}")
    print(f"  Semantic type:  {analysis.semantic_type}")
    print(f"  DataForge field: {analysis.dataforge_field}")
    print(f"  Null rate:      {analysis.null_rate:.1%}")
    if analysis.stats:
        print(f"  Stats:          {analysis.stats}")
    print()

# --- Example 3: Data with nulls and mixed types --------------------------

print("=== Handling Nulls and Numeric Data ===\n")

messy_data = [
    {"user_id": "550e8400-e29b-41d4-a716-446655440000", "age": "28", "score": "95.5"},
    {"user_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8", "age": "35", "score": ""},
    {"user_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "age": "", "score": "88.0"},
    {"user_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7", "age": "42", "score": "92.3"},
]

inferrer2 = SchemaInferrer(forge)
schema2 = inferrer2.from_records(messy_data)

print(inferrer2.describe())
print()

# Generate matching synthetic data
print("Synthetic data:")
for row in schema2.generate(count=3):
    print(f"  {row}")
print()

# --- Example 4: Infer from CSV file --------------------------------------

print("=== Infer from CSV (demonstration) ===\n")

# First, create a sample CSV file for demonstration
csv_path = "sample_customers.csv"
with open(csv_path, "w", newline="") as f:
    writer = csv.DictWriter(
        f, fieldnames=["first_name", "last_name", "email", "city", "state"]
    )
    writer.writeheader()
    writer.writerows(
        [
            {
                "first_name": "James",
                "last_name": "Smith",
                "email": "james@test.com",
                "city": "Chicago",
                "state": "Illinois",
            },
            {
                "first_name": "Maria",
                "last_name": "Garcia",
                "email": "maria@test.com",
                "city": "Miami",
                "state": "Florida",
            },
            {
                "first_name": "John",
                "last_name": "Lee",
                "email": "john@test.com",
                "city": "Seattle",
                "state": "Washington",
            },
        ]
    )

# Now infer from the CSV
inferrer3 = SchemaInferrer(forge)
schema3 = inferrer3.from_csv(csv_path)

print(inferrer3.describe())
print()

print("Synthetic data from CSV-inferred schema:")
for row in schema3.generate(count=5):
    print(f"  {row}")

# Clean up
os.remove(csv_path)
