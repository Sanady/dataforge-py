"""Constraint Engine — Correlated and Conditional Data Generation.

Real-world scenario: Generate realistic e-commerce or HR data where
fields depend on each other. Cities must belong to their state,
end dates must come after start dates, salaries should correlate with
experience, and product categories determine available product types.

This example demonstrates:
- Geographic dependencies (country -> state -> city)
- Temporal constraints (start_date -> end_date)
- Statistical correlation (experience -> salary)
- Conditional value pools (department -> job title)
- Range constraints with dynamic bounds
"""

from dataforge import DataForge

forge = DataForge(seed=42)

# --- Example 1: Geographic hierarchy (country -> state -> city) -----------

print("=== Geographic Hierarchy ===\n")

geo_schema = forge.schema(
    {
        "country": "country",
        "state": {"field": "address.state", "depends_on": "country"},
        "city": {"field": "address.city", "depends_on": "state"},
    }
)

rows = geo_schema.generate(count=10)
print("Country -> State -> City:")
for row in rows:
    print(f"  {row['country']} -> {row['state']} -> {row['city']}")
print()

# --- Example 2: Temporal constraints (event scheduling) -------------------

print("=== Temporal Constraints (Event Scheduling) ===\n")

event_schema = forge.schema(
    {
        "event_name": "lorem.sentence",
        "start_date": "date",
        "end_date": {
            "field": "date",
            "temporal": "after",
            "reference": "start_date",
            "offset_days": [1, 30],  # end 1-30 days after start
        },
        "registration_deadline": {
            "field": "date",
            "temporal": "before",
            "reference": "start_date",
            "offset_days": [7, 60],  # deadline 7-60 days before start
        },
    }
)

rows = event_schema.generate(count=5)
print("Registration -> Start -> End:")
for row in rows:
    print(
        f"  {row['registration_deadline']} (reg) -> "
        f"{row['start_date']} (start) -> "
        f"{row['end_date']} (end)"
    )
print()

# --- Example 3: Statistical correlation (experience vs salary) ------------

print("=== Statistical Correlation (Experience vs Salary) ===\n")

# salary_score correlates with experience_score by rho=0.85
correlation_schema = forge.schema(
    {
        "employee_name": "person.full_name",
        "experience_score": {"field": "misc.random_int", "min_val": 0, "max_val": 100},
        "salary_score": {
            "field": "misc.random_int",
            "correlate": "experience_score",
            "correlation": 0.85,  # strong positive correlation
            "mean": 50.0,
            "std": 15.0,
        },
    }
)

rows = correlation_schema.generate(count=10)
print("Employee | Experience | Salary Score")
print("-" * 50)
for row in rows:
    exp = row["experience_score"]
    sal = row["salary_score"]
    print(f"  {row['employee_name']:<25} {exp:>5}       {sal:>8}")
print()

# --- Example 4: Conditional value pools (department -> job titles) --------

print("=== Conditional Value Pools ===\n")

dept_schema = forge.schema(
    {
        "employee": "person.full_name",
        "department": "misc.random_element",
        "role": {
            "field": "company.job_title",
            "conditional": "department",
            "value_pools": {
                "Engineering": (
                    "Software Engineer",
                    "Senior Engineer",
                    "Tech Lead",
                    "DevOps Engineer",
                    "QA Engineer",
                ),
                "Marketing": (
                    "Marketing Manager",
                    "Content Strategist",
                    "SEO Specialist",
                    "Brand Manager",
                ),
                "Sales": (
                    "Account Executive",
                    "Sales Manager",
                    "Business Development Rep",
                    "Sales Engineer",
                ),
                "HR": (
                    "HR Manager",
                    "Recruiter",
                    "People Operations",
                    "Compensation Analyst",
                ),
            },
            "default_pool": ("Associate", "Coordinator", "Specialist"),
        },
    }
)

# Note: For the department field, we need to manually set up the random
# elements. Here's a practical approach using a lambda:
dept_schema_v2 = forge.schema(
    {
        "employee": "person.full_name",
        "department": lambda row: forge._engine.choice(
            ("Engineering", "Marketing", "Sales", "HR", "Finance")
        ),
        "role": {
            "field": "company.job_title",
            "conditional": "department",
            "value_pools": {
                "Engineering": (
                    "Software Engineer",
                    "Senior Engineer",
                    "Tech Lead",
                    "DevOps Engineer",
                    "QA Engineer",
                ),
                "Marketing": (
                    "Marketing Manager",
                    "Content Strategist",
                    "SEO Specialist",
                    "Brand Manager",
                ),
                "Sales": (
                    "Account Executive",
                    "Sales Manager",
                    "Business Development Rep",
                    "Sales Engineer",
                ),
                "HR": (
                    "HR Manager",
                    "Recruiter",
                    "People Operations",
                    "Compensation Analyst",
                ),
            },
            "default_pool": ("Associate", "Coordinator", "Specialist"),
        },
    }
)

rows = dept_schema_v2.generate(count=10)
print("Employee | Department | Role")
print("-" * 70)
for row in rows:
    print(f"  {row['employee']:<25} {row['department']:<15} {row['role']}")
print()

# --- Example 5: Range constraints with dynamic bounds --------------------

print("=== Range Constraints ===\n")

price_schema = forge.schema(
    {
        "product": "ecommerce.product_name",
        "min_price": {
            "field": "finance.price",
            "min_val": 5.0,
            "max_val": 50.0,
            "precision": 2,
        },
        "max_price": {
            "field": "finance.price",
            "min_ref": "min_price",  # dynamic lower bound from min_price
            "max_val": 500.0,
            "precision": 2,
        },
        "sale_price": {
            "field": "finance.price",
            "min_ref": "min_price",  # between min_price and max_price
            "max_ref": "max_price",
            "precision": 2,
        },
    }
)

rows = price_schema.generate(count=5)
print("Product | Min | Max | Sale Price")
print("-" * 65)
for row in rows:
    print(
        f"  {row['product']:<30} "
        f"${row['min_price']:>7} - "
        f"${row['max_price']:>7}  "
        f"sale: ${row['sale_price']:>7}"
    )
