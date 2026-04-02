"""Real-World Scenarios — Comprehensive examples combining multiple features.

This example shows complete workflows that combine DataForge's core and
advanced features to solve real-world data generation challenges.
"""

from dataforge import DataForge
from dataforge.anonymizer import Anonymizer
from dataforge.chaos import ChaosTransformer
from dataforge.openapi import OpenAPIParser
from dataforge.timeseries import TimeSeriesSchema

forge = DataForge(seed=42)

# ---------------------------------------------------------------------------
# Scenario 1: E-Commerce Test Environment Setup
# ---------------------------------------------------------------------------
print("=" * 60)
print("SCENARIO 1: E-Commerce Test Environment")
print("=" * 60)

# Generate correlated customer data
customer_schema = forge.schema(
    {
        "customer_id": "misc.uuid4",
        "first_name": "person.first_name",
        "last_name": "person.last_name",
        "email": "internet.email",
        "country": "address.country",
        "state": {"field": "address.state", "depends_on": "country"},
        "city": {"field": "address.city", "depends_on": "state"},
        "phone": "phone.phone_number",
    }
)

customers = customer_schema.generate(count=5)
print("\nCustomers:")
for c in customers:
    print(
        f"  {c['customer_id'][:8]}... {c['first_name']} {c['last_name']} "
        f"— {c['city']}, {c['state']}, {c['country']}"
    )

# Generate orders with temporal constraints
order_schema = forge.schema(
    {
        "order_id": "ecommerce.order_id",
        "product": "ecommerce.product_name",
        "sku": "ecommerce.sku",
        "amount": "finance.price",
        "status": "payment.transaction_status",
        "order_date": "dt.date",
        "ship_date": {
            "field": "dt.date",
            "temporal": "after",
            "reference": "order_date",
            "offset_days": [1, 5],
        },
    }
)

orders = order_schema.generate(count=5)
print("\nOrders:")
for o in orders:
    print(
        f"  {o['order_id']}  {o['product']:20}  ${o['amount']:>7}  "
        f"ordered={o['order_date']}  shipped={o['ship_date']}"
    )

# Export to CSV
csv_output = order_schema.to_csv(count=100)
print(f"\nExported 100 orders to CSV ({len(csv_output)} bytes)")

# ---------------------------------------------------------------------------
# Scenario 2: Healthcare Data for HIPAA-Compliant Testing
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("SCENARIO 2: Healthcare Data (HIPAA-Compliant)")
print("=" * 60)

# Step 1: Generate realistic patient records
patient_schema = forge.schema(
    {
        "mrn": "medical.medical_record_number",
        "first_name": "person.first_name",
        "last_name": "person.last_name",
        "dob": "dt.date_of_birth",
        "blood_type": "medical.realistic_blood_type",
        "diagnosis": "medical.diagnosis",
        "drug": "medical.drug_name",
        "dosage": "medical.dosage",
    }
)

patients = patient_schema.generate(count=5)
print("\nPatient Records (fully synthetic):")
for p in patients:
    print(
        f"  {p['mrn']} {p['first_name']} {p['last_name']} "
        f"DOB={p['dob']} Blood={p['blood_type']} Dx={p['diagnosis']}"
    )

# Step 2: If you had REAL patient data, anonymize it
print("\nAnonymization of real data:")
real_patients = [
    {
        "name": "John Smith",
        "ssn": "123-45-6789",
        "diagnosis": "Hypertension",
        "email": "john.smith@hospital.org",
    },
    {
        "name": "Jane Doe",
        "ssn": "987-65-4321",
        "diagnosis": "Type 2 Diabetes",
        "email": "jane.doe@hospital.org",
    },
]

anon = Anonymizer(forge, secret="hipaa-compliant-key-2024")
safe_patients = anon.anonymize(
    real_patients,
    fields={
        "name": "full_name",
        "ssn": "ssn",
        "email": "email",
        # "diagnosis" passes through (not PII)
    },
)

for orig, safe in zip(real_patients, safe_patients):
    print(
        f"  {orig['name']:20} → {safe['name']:20}  SSN: {orig['ssn']} → {safe['ssn']}"
    )

# ---------------------------------------------------------------------------
# Scenario 3: IoT Monitoring Dashboard Data
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("SCENARIO 3: IoT Dashboard Data")
print("=" * 60)

# Generate sensor data for a factory floor
factory_ts = TimeSeriesSchema(
    forge,
    start="2024-06-01",
    end="2024-06-02",
    interval="5m",
    fields={
        "machine_temp_c": {
            "base": 65.0,
            "seasonality": {"period": 24, "amplitude": 8.0},
            "noise": 2.0,
            "anomaly_rate": 0.03,
            "anomaly_scale": 4.0,
            "min_val": 40.0,
            "max_val": 120.0,
        },
        "vibration_hz": {
            "base": 50.0,
            "noise": 5.0,
            "spike_rate": 0.02,
            "spike_scale": 3.0,
            "min_val": 0.0,
        },
        "power_kw": {
            "base": 100.0,
            "trend": 0.01,
            "noise": 10.0,
            "missing_rate": 0.02,
            "min_val": 0.0,
        },
    },
)

rows = factory_ts.generate()
anomalies = sum(1 for r in rows if r["machine_temp_c"] and r["machine_temp_c"] > 90)
missing = sum(1 for r in rows if r["power_kw"] is None)

print(f"\n  Generated {len(rows)} data points (24h at 5-min intervals)")
print(f"  Temperature anomalies (>90C): {anomalies}")
print(f"  Missing power readings: {missing}")
print(f"  First reading: {rows[0]}")

# Export for dashboard ingestion
csv_data = factory_ts.to_csv()
print(f"  CSV export: {len(csv_data)} bytes")

# ---------------------------------------------------------------------------
# Scenario 4: API Contract Testing with OpenAPI
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("SCENARIO 4: API Contract Testing")
print("=" * 60)

parser = OpenAPIParser(forge)

# Your API's OpenAPI spec (normally loaded from openapi.yaml)
api_spec = {
    "openapi": "3.0.0",
    "info": {"title": "User Service", "version": "2.0.0"},
    "components": {
        "schemas": {
            "UserCreate": {
                "type": "object",
                "properties": {
                    "username": {"type": "string"},
                    "email": {"type": "string", "format": "email"},
                    "password": {"type": "string", "format": "password"},
                    "city": {"type": "string"},
                },
            },
            "UserResponse": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "format": "uuid"},
                    "username": {"type": "string"},
                    "email": {"type": "string", "format": "email"},
                    "created_at": {"type": "string", "format": "date-time"},
                },
            },
        },
    },
}

schemas = parser.from_openapi(api_spec)
print(f"\n  Parsed schemas: {list(schemas.keys())}")

# Generate test payloads
for name, schema in schemas.items():
    payloads = schema.generate(count=3)
    print(f"\n  {name} payloads:")
    for p in payloads:
        print(f"    {p}")

# ---------------------------------------------------------------------------
# Scenario 5: Data Quality Pipeline Testing
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("SCENARIO 5: Data Quality Pipeline Testing")
print("=" * 60)

# Step 1: Generate pristine data
pristine_schema = forge.schema(
    {
        "id": "misc.uuid4",
        "email": "internet.email",
        "name": "person.full_name",
        "date": "dt.date",
        "amount": "finance.price",
    }
)
pristine = pristine_schema.generate(count=100)

# Step 2: Inject real-world data quality issues
chaos = ChaosTransformer(
    null_rate=0.05,
    type_mismatch_rate=0.02,
    boundary_rate=0.01,
    duplicate_rate=0.03,
    whitespace_rate=0.02,
    encoding_rate=0.01,
    format_rate=0.02,
    truncation_rate=0.01,
    seed=42,
)
dirty = chaos.transform(pristine)

# Step 3: Run your validation pipeline
errors_found = 0
for row in dirty:
    if row.get("email") is None or not isinstance(row.get("email"), str):
        errors_found += 1
    elif "@" not in row["email"]:
        errors_found += 1

print(f"\n  Pristine rows:     {len(pristine)}")
print(f"  After chaos:       {len(dirty)} (duplicates added)")
print(f"  Email errors found: {errors_found}")
print(
    f"  Your pipeline {'caught the issues' if errors_found > 0 else 'needs more validation'}!"
)

# ---------------------------------------------------------------------------
# Scenario 6: Multi-Locale International Data
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("SCENARIO 6: Multi-Locale International Data")
print("=" * 60)

locales = ["en_US", "de_DE", "fr_FR", "ja_JP", "pt_BR"]
for locale in locales:
    local_forge = DataForge(locale=locale, seed=42)
    name = local_forge.person.full_name()
    city = local_forge.address.city()
    print(f"  {locale}: {name:25} — {city}")
