"""PII Anonymization — GDPR-compliant Data Masking.

Real-world scenario: You have a production database dump containing
personally identifiable information (PII) and need to create a safe
copy for development/testing. The Anonymizer replaces real PII with
realistic fake data while maintaining referential integrity — the
same real email always maps to the same fake email, so JOINs and
foreign key relationships are preserved across tables.

This example demonstrates:
- Deterministic anonymization (same input -> same output)
- Referential integrity across tables
- Format-preserving email and phone anonymization
- Streaming CSV anonymization for large files
- Using different secrets for different anonymization contexts
"""

import csv
import os

from dataforge import DataForge
from dataforge.anonymizer import Anonymizer

forge = DataForge(seed=42)

# --- Example 1: Basic anonymization --------------------------------------

print("=== Basic PII Anonymization ===\n")

anon = Anonymizer(forge, secret="my-company-secret-2024")

# Simulated production data
production_data = [
    {
        "id": 1,
        "name": "Alice Johnson",
        "email": "alice.johnson@company.com",
        "phone": "(555) 123-4567",
        "ssn": "123-45-6789",
        "department": "Engineering",
    },
    {
        "id": 2,
        "name": "Bob Smith",
        "email": "bob.smith@company.com",
        "phone": "(555) 987-6543",
        "ssn": "987-65-4321",
        "department": "Marketing",
    },
    {
        "id": 3,
        "name": "Carol Williams",
        "email": "carol.williams@company.com",
        "phone": "(555) 456-7890",
        "ssn": "456-78-9012",
        "department": "Sales",
    },
]

# Only anonymize PII fields — 'id' and 'department' pass through
anonymized = anon.anonymize(
    production_data,
    fields={
        "name": "full_name",
        "email": "email",
        "phone": "phone_number",
        "ssn": "ssn",
    },
)

print("Original -> Anonymized:")
for orig, masked in zip(production_data, anonymized):
    print(f"\n  Original:    {orig['name']:<20} {orig['email']:<35} {orig['ssn']}")
    print(f"  Anonymized:  {masked['name']:<20} {masked['email']:<35} {masked['ssn']}")
    print(f"  Preserved:   id={masked['id']}, dept={masked['department']}")
print()

# --- Example 2: Referential integrity ------------------------------------

print("=== Referential Integrity Across Tables ===\n")

# Same person appears in multiple tables
users_table = [
    {"user_id": 1, "email": "alice@real.com", "name": "Alice Real"},
    {"user_id": 2, "email": "bob@real.com", "name": "Bob Real"},
]

orders_table = [
    {"order_id": 101, "user_email": "alice@real.com", "amount": "$49.99"},
    {"order_id": 102, "user_email": "bob@real.com", "amount": "$129.00"},
    {"order_id": 103, "user_email": "alice@real.com", "amount": "$25.50"},
]

# Anonymize both tables with the same Anonymizer instance
anon_users = anon.anonymize(
    users_table,
    fields={
        "email": "email",
        "name": "full_name",
    },
)

anon_orders = anon.anonymize(
    orders_table,
    fields={
        "user_email": "email",  # same field type = same mapping
    },
)

print("Anonymized Users:")
for row in anon_users:
    print(f"  id={row['user_id']}  email={row['email']}")

print("\nAnonymized Orders:")
for row in anon_orders:
    print(
        f"  order={row['order_id']}  email={row['user_email']}  amount={row['amount']}"
    )

# Verify: alice@real.com maps to the same fake email in both tables
alice_user_email = anon_users[0]["email"]
alice_order_emails = [
    r["user_email"] for r in anon_orders if r["order_id"] in (101, 103)
]
print(f"\nAlice's email in users table:  {alice_user_email}")
print(f"Alice's email in orders table: {alice_order_emails}")
print(
    f"Referential integrity maintained: {all(e == alice_user_email for e in alice_order_emails)}"
)
print()

# --- Example 3: Deterministic across runs --------------------------------

print("=== Deterministic Across Runs ===\n")

# Create two separate Anonymizers with the same secret
anon_a = Anonymizer(DataForge(seed=1), secret="same-secret")
anon_b = Anonymizer(DataForge(seed=99), secret="same-secret")

data = [{"email": "test@example.com", "name": "Test User"}]

result_a = anon_a.anonymize(data, fields={"email": "email", "name": "full_name"})
result_b = anon_b.anonymize(data, fields={"email": "email", "name": "full_name"})

print(f"Run A: {result_a[0]}")
print(f"Run B: {result_b[0]}")
print(f"Results match: {result_a[0] == result_b[0]}")
print()

# --- Example 4: Streaming CSV anonymization -------------------------------

print("=== Streaming CSV Anonymization ===\n")

# Create a sample CSV with PII
input_csv = "employees_pii.csv"
with open(input_csv, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["id", "name", "email", "department"])
    writer.writeheader()
    for i in range(100):
        writer.writerow(
            {
                "id": i + 1,
                "name": f"Employee {i + 1}",
                "email": f"emp{i + 1}@company.com",
                "department": ["Engineering", "Sales", "Marketing"][i % 3],
            }
        )

# Anonymize the CSV
output_csv = "employees_safe.csv"
rows_processed = anon.anonymize_csv(
    input_csv,
    output_csv,
    fields={"name": "full_name", "email": "email"},
    batch_size=50,
)

print(f"Processed {rows_processed} rows")
print(f"Input:  {input_csv}")
print(f"Output: {output_csv}")

# Read back a few rows to verify
with open(output_csv, "r") as f:
    reader = csv.DictReader(f)
    print("\nAnonymized sample:")
    for i, row in enumerate(reader):
        if i >= 3:
            break
        print(f"  {row}")

# Clean up
os.remove(input_csv)
os.remove(output_csv)
