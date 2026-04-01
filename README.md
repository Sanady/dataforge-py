# DataForge

**High-performance, zero-dependency fake data generator for Python.**

DataForge generates realistic fake data at millions of items per second. It uses vectorized batch generation, lazy-loaded locale data, and pre-resolved field lookups to deliver throughput that is orders of magnitude faster than existing alternatives — with zero runtime dependencies.

```python
from dataforge import DataForge

forge = DataForge(seed=42)

forge.person.first_name()                # "James"
forge.internet.email()                   # "james.smith@gmail.com"
forge.person.first_name(count=1_000_000) # 1M names in ~55ms
```

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Providers](#providers) (27 providers, 198 methods)
- [Schema API](#schema-api)
- [Bulk Export](#bulk-export)
- [Streaming Export](#streaming-export)
- [Integrations](#integrations) (PyArrow, Polars, Pydantic, SQLAlchemy)
- [Command Line Interface](#command-line-interface)
- [Pytest Plugin](#pytest-plugin)
- [Unique Values](#unique-values)
- [Locales](#locales) (17 locales)
- [Benchmarks](#benchmarks)
- [CI/CD](#cicd)
- [Contributing](#contributing)
- [License](#license)

---

## Features

- **High Performance** — scalar generation at millions of items/s, batch generation at ~18M items/s, CSV export at ~92K rows/s
- **Vectorized Batches** — every method accepts `count=N` and returns a list, using optimized batch paths with vectorized internals for internet, datetime, and finance providers
- **Zero Dependencies** — core library has no external dependencies
- **Type Safe** — fully typed with PEP 484 type hints and `@overload` signatures
- **Reproducible** — global seeding for deterministic output
- **Lazy Loading** — locales and providers are loaded only when first accessed
- **Schema API** — define reusable data blueprints with pre-resolved field lookups and columnar-first generation
- **Rich CLI** — generate CSV, JSON, or JSONL directly from the terminal
- **Bulk Export** — export to dict, CSV, JSONL, SQL, DataFrame, Arrow, Polars, or Parquet with optimized columnar writers
- **Streaming Export** — memory-efficient streaming writes for arbitrarily large datasets
- **Pytest Plugin** — `forge`, `fake`, and `forge_unseeded` fixtures with seed markers
- **Unique Values** — three-layer proxy with set-based dedup and adaptive over-sampling for batches
- **27 Providers** — person, address, internet, company, phone, finance, datetime, color, file, network, lorem, barcode, misc, automotive, crypto, ecommerce, education, geo, government, medical, payment, profile, science, text, ai\_prompt, llm, ai\_chat
- **17 Locales** — en\_US, en\_GB, en\_AU, en\_CA, de\_DE, fr\_FR, es\_ES, it\_IT, pt\_BR, nl\_NL, pl\_PL, ru\_RU, ar\_SA, hi\_IN, ja\_JP, ko\_KR, zh\_CN

## Installation

```bash
# Standard installation (zero dependencies)
pip install dataforge

# With uv
uv add dataforge
```

**Optional integrations** (install separately as needed):

```bash
pip install pyarrow    # to_arrow(), to_parquet()
pip install polars     # to_polars()
pip install pandas     # to_dataframe()
pip install pydantic   # schema_from_pydantic()
pip install sqlalchemy # schema_from_sqlalchemy()
```

**Requires Python >= 3.12.**

## Quick Start

```python
from dataforge import DataForge

# Initialize with optional locale and seed
forge = DataForge(locale="en_US", seed=42)

# Generate single items
forge.person.first_name()         # "James"
forge.internet.email()            # "james.smith@gmail.com"
forge.address.city()              # "Chicago"
forge.finance.price()             # "49.99"
forge.llm.model_name()            # "gpt-4o"

# Generate batches (returns lists)
names  = forge.person.first_name(count=1000)
emails = forge.internet.email(count=1000)
cities = forge.address.city(count=1000)

# Reproducible output
forge_a = DataForge(seed=42)
forge_b = DataForge(seed=42)
assert forge_a.person.first_name() == forge_b.person.first_name()
```

## Providers

DataForge ships with 27 providers organized by domain. Every method accepts `count=N` for batch generation.

### `person` — Names and identity

| Method | Return | Example |
|--------|--------|---------|
| `first_name()` | `str` | `"James"` |
| `last_name()` | `str` | `"Smith"` |
| `full_name()` | `str` | `"James Smith"` |
| `male_first_name()` | `str` | `"Robert"` |
| `female_first_name()` | `str` | `"Jennifer"` |
| `prefix()` | `str` | `"Mr."` |
| `suffix()` | `str` | `"Jr."` |

### `address` — Locations and geography

| Method | Return | Example |
|--------|--------|---------|
| `street_name()` | `str` | `"Elm Street"` |
| `street_address()` | `str` | `"742 Elm Street"` |
| `building_number()` | `str` | `"742"` |
| `city()` | `str` | `"Chicago"` |
| `state()` | `str` | `"California"` |
| `zip_code()` | `str` | `"90210"` |
| `full_address()` | `str` | `"742 Elm St, Chicago, IL 90210"` |
| `country()` | `str` | `"United States"` |
| `country_code()` | `str` | `"US"` |
| `latitude()` | `str` | `"41.8781"` |
| `longitude()` | `str` | `"-87.6298"` |
| `coordinate()` | `tuple[str, str]` | `("41.8781", "-87.6298")` |

### `internet` — Web and network identifiers

| Method | Return | Example |
|--------|--------|---------|
| `email()` | `str` | `"james.smith@gmail.com"` |
| `safe_email()` | `str` | `"james@example.com"` |
| `username()` | `str` | `"jsmith42"` |
| `domain()` | `str` | `"example.com"` |
| `url()` | `str` | `"https://example.com"` |
| `ipv4()` | `str` | `"192.168.1.1"` |
| `slug()` | `str` | `"lorem-ipsum-dolor"` |
| `tld()` | `str` | `"com"` |

### `company` — Business data

| Method | Return | Example |
|--------|--------|---------|
| `company_name()` | `str` | `"Acme Corp"` |
| `company_suffix()` | `str` | `"LLC"` |
| `job_title()` | `str` | `"Software Engineer"` |
| `catch_phrase()` | `str` | `"Innovative solutions"` |

### `phone` — Phone numbers

| Method | Return | Example |
|--------|--------|---------|
| `phone_number()` | `str` | `"(555) 123-4567"` |
| `cell_phone()` | `str` | `"555-987-6543"` |

### `finance` — Financial data

| Method | Return | Example |
|--------|--------|---------|
| `credit_card_number()` | `str` | `"4532015112830366"` |
| `credit_card()` | `dict` | `{"type": "Visa", ...}` |
| `card_type()` | `str` | `"Visa"` |
| `iban()` | `str` | `"DE89370400440532013000"` |
| `bic()` | `str` | `"DEUTDEFFXXX"` |
| `routing_number()` | `str` | `"021000021"` |
| `bitcoin_address()` | `str` | `"1A1zP1eP5QGefi2DMPTfTL..."` |
| `currency_code()` | `str` | `"USD"` |
| `currency_name()` | `str` | `"US Dollar"` |
| `currency_symbol()` | `str` | `"$"` |
| `price(min_val, max_val)` | `str` | `"49.99"` |

### `dt` — Dates and times

| Method | Return | Example |
|--------|--------|---------|
| `date(start, end, fmt)` | `str` | `"2024-03-15"` |
| `time(fmt)` | `str` | `"14:30:00"` |
| `datetime(start, end, fmt)` | `str` | `"2024-03-15 14:30:00"` |
| `date_of_birth(min_age, max_age)` | `str` | `"1990-05-12"` |
| `date_object()` | `date` | `date(2024, 3, 15)` |
| `datetime_object()` | `datetime` | `datetime(2024, 3, 15, ...)` |
| `timezone()` | `str` | `"US/Eastern"` |
| `unix_timestamp(start, end)` | `int` | `1710504600` |

```python
import datetime
forge.dt.date(start=datetime.date(2020, 1, 1), end=datetime.date(2024, 12, 31))
```

### `color` — Color values

| Method | Return | Example |
|--------|--------|---------|
| `color_name()` | `str` | `"Red"` |
| `hex_color()` | `str` | `"#ff5733"` |
| `rgb()` | `tuple[int, int, int]` | `(255, 87, 51)` |
| `rgba()` | `tuple[int, int, int, float]` | `(255, 87, 51, 0.8)` |
| `rgb_string()` | `str` | `"rgb(255, 87, 51)"` |
| `hsl()` | `tuple[int, int, int]` | `(11, 100, 60)` |
| `hsl_string()` | `str` | `"hsl(11, 100%, 60%)"` |

### `file` — File system data

| Method | Return | Example |
|--------|--------|---------|
| `file_name()` | `str` | `"report.pdf"` |
| `file_extension()` | `str` | `"pdf"` |
| `file_path()` | `str` | `"/home/user/report.pdf"` |
| `file_category()` | `str` | `"document"` |
| `mime_type()` | `str` | `"application/pdf"` |

### `network` — Networking and protocols

| Method | Return | Example |
|--------|--------|---------|
| `ipv6()` | `str` | `"2001:0db8:85a3:0000:..."` |
| `mac_address()` | `str` | `"a1:b2:c3:d4:e5:f6"` |
| `port()` | `int` | `8080` |
| `hostname()` | `str` | `"server-01.example.com"` |
| `user_agent()` | `str` | `"Mozilla/5.0 ..."` |
| `http_method()` | `str` | `"GET"` |
| `http_status_code()` | `str` | `"200 OK"` |

### `lorem` — Placeholder text

| Method | Return | Example |
|--------|--------|---------|
| `word()` | `str` | `"lorem"` |
| `sentence(word_count)` | `str` | `"Lorem ipsum dolor sit."` |
| `paragraph(sentence_count)` | `str` | `"Lorem ipsum dolor ..."` |
| `text(max_chars)` | `str` | `"Lorem ipsum ..."` |

### `barcode` — Barcodes and ISBNs

| Method | Return | Example |
|--------|--------|---------|
| `ean13()` | `str` | `"5901234123457"` |
| `ean8()` | `str` | `"96385074"` |
| `isbn13()` | `str` | `"9780306406157"` |
| `isbn10()` | `str` | `"0306406152"` |

All barcodes include valid check digits.

### `misc` — Utilities

| Method | Return | Example |
|--------|--------|---------|
| `uuid4()` | `str` | `"550e8400-e29b-41d4-..."` |
| `uuid7()` | `str` | `"01912b4c-..."` |
| `boolean(probability)` | `bool` | `True` |
| `random_element(elements)` | `Any` | `"a"` |
| `null_or(value, probability)` | `Any` | `None` or value |

### `automotive` — Vehicle data

| Method | Return | Example |
|--------|--------|---------|
| `license_plate()` | `str` | `"ABC-1234"` |
| `vin()` | `str` | `"1HGCM82633A004352"` |
| `vehicle_make()` | `str` | `"Toyota"` |
| `vehicle_model()` | `str` | `"Camry"` |
| `vehicle_year()` | `int` | `2023` |
| `vehicle_year_str()` | `str` | `"2023"` |
| `vehicle_color()` | `str` | `"Silver"` |

### `crypto` — Hash digests

| Method | Return | Example |
|--------|--------|---------|
| `md5()` | `str` | `"d41d8cd98f00b204e98..."` |
| `sha1()` | `str` | `"da39a3ee5e6b4b0d325..."` |
| `sha256()` | `str` | `"e3b0c44298fc1c149af..."` |

### `ecommerce` — E-commerce data

| Method | Return | Example |
|--------|--------|---------|
| `product_name()` | `str` | `"Wireless Mouse"` |
| `product_category()` | `str` | `"Electronics"` |
| `sku()` | `str` | `"SKU-A1B2C3"` |
| `price_with_currency()` | `str` | `"$49.99 USD"` |
| `review_rating()` | `int` | `4` |
| `review_title()` | `str` | `"Great product!"` |
| `tracking_number()` | `str` | `"1Z999AA10123456784"` |
| `order_id()` | `str` | `"ORD-20240315-A1B2"` |

### `education` — Academic data

| Method | Return | Example |
|--------|--------|---------|
| `university()` | `str` | `"MIT"` |
| `degree()` | `str` | `"Bachelor of Science"` |
| `field_of_study()` | `str` | `"Computer Science"` |

### `geo` — Geographic features

| Method | Return | Example |
|--------|--------|---------|
| `continent()` | `str` | `"North America"` |
| `ocean()` | `str` | `"Pacific Ocean"` |
| `sea()` | `str` | `"Mediterranean Sea"` |
| `mountain_range()` | `str` | `"Rocky Mountains"` |
| `river()` | `str` | `"Amazon"` |
| `compass_direction()` | `str` | `"Northeast"` |
| `geo_coordinate()` | `str` | `"41.8781, -87.6298"` |
| `dms_latitude()` | `str` | `"41°52'41.2\"N"` |
| `dms_longitude()` | `str` | `"87°37'47.3\"W"` |
| `geo_hash()` | `str` | `"dp3wjztvh"` |

### `government` — Government IDs

| Method | Return | Example |
|--------|--------|---------|
| `ssn()` | `str` | `"123-45-6789"` |
| `tax_id()` | `str` | `"12-3456789"` |
| `passport_number()` | `str` | `"A12345678"` |
| `drivers_license()` | `str` | `"D123-4567-8901"` |
| `national_id()` | `str` | `"123456789012"` |

### `medical` — Healthcare data

| Method | Return | Example |
|--------|--------|---------|
| `blood_type()` | `str` | `"O+"` |
| `realistic_blood_type()` | `str` | `"O+"` (weighted) |
| `icd10_code()` | `str` | `"J06.9"` |
| `drug_name()` | `str` | `"Amoxicillin"` |
| `drug_form()` | `str` | `"Tablet"` |
| `dosage()` | `str` | `"500mg"` |
| `diagnosis()` | `str` | `"Acute bronchitis"` |
| `procedure()` | `str` | `"Appendectomy"` |
| `medical_record_number()` | `str` | `"MRN-12345678"` |

`realistic_blood_type()` uses American Red Cross population distribution weights.

### `payment` — Payment and transaction data

| Method | Return | Example |
|--------|--------|---------|
| `card_type()` | `str` | `"Visa"` |
| `payment_method()` | `str` | `"Credit Card"` |
| `payment_processor()` | `str` | `"Stripe"` |
| `transaction_status()` | `str` | `"completed"` |
| `transaction_id()` | `str` | `"txn_1a2b3c4d5e"` |
| `currency_code()` | `str` | `"USD"` |
| `currency_symbol()` | `str` | `"$"` |
| `payment_amount()` | `str` | `"149.99"` |
| `cvv()` | `str` | `"123"` |
| `expiry_date()` | `str` | `"12/28"` |

### `profile` — Coherent user profiles

| Method | Return | Example |
|--------|--------|---------|
| `profile()` | `dict` | `{"first_name": "James", ...}` |
| `profile_first_name()` | `str` | `"James"` |
| `profile_last_name()` | `str` | `"Smith"` |
| `profile_email()` | `str` | `"james.smith@gmail.com"` |
| `profile_phone()` | `str` | `"(555) 123-4567"` |
| `profile_city()` | `str` | `"Chicago"` |
| `profile_state()` | `str` | `"Illinois"` |
| `profile_zip_code()` | `str` | `"60601"` |
| `profile_job_title()` | `str` | `"Software Engineer"` |

`profile()` returns a coherent dict combining person, internet, address, phone, and company data.

### `science` — Scientific data

| Method | Return | Example |
|--------|--------|---------|
| `chemical_element()` | `str` | `"Hydrogen"` |
| `element_symbol()` | `str` | `"H"` |
| `si_unit()` | `str` | `"meter"` |
| `planet()` | `str` | `"Mars"` |
| `galaxy()` | `str` | `"Milky Way"` |
| `constellation()` | `str` | `"Orion"` |
| `scientific_discipline()` | `str` | `"Physics"` |
| `metric_prefix()` | `str` | `"kilo"` |

### `text` — Rich text content

| Method | Return | Example |
|--------|--------|---------|
| `quote()` | `str` | `"The only way to do great work..."` |
| `headline()` | `str` | `"Breaking: New Study Reveals..."` |
| `buzzword()` | `str` | `"synergy"` |
| `paragraph()` | `str` | Multi-sentence paragraph |
| `text_block()` | `str` | Multi-paragraph text block |

### `ai_prompt` — AI prompts

| Method | Return | Example |
|--------|--------|---------|
| `user_prompt()` | `str` | `"Explain quantum computing..."` |
| `coding_prompt()` | `str` | `"Write a Python function..."` |
| `creative_prompt()` | `str` | `"Write a short story about..."` |
| `analysis_prompt()` | `str` | `"Analyze the following data..."` |
| `system_prompt()` | `str` | `"You are a helpful assistant..."` |
| `persona_prompt()` | `str` | `"Act as a senior engineer..."` |
| `prompt_template()` | `str` | `"Given {context}, answer {question}"` |
| `few_shot_prompt()` | `str` | Multi-example prompt |

### `llm` — LLM ecosystem data

**Models and metadata:**

| Method | Return | Example |
|--------|--------|---------|
| `model_name()` | `str` | `"gpt-4o"` |
| `provider_name()` | `str` | `"OpenAI"` |
| `api_key()` | `str` | `"sk-proj-a1b2c3d4..."` |
| `finish_reason()` | `str` | `"stop"` |
| `stop_sequence()` | `str` | `"<\|endoftext\|>"` |

**Agents and tool use:**

| Method | Return | Example |
|--------|--------|---------|
| `tool_name()` | `str` | `"web_search"` |
| `tool_call_id()` | `str` | `"call_abc123"` |
| `mcp_server_name()` | `str` | `"filesystem"` |
| `agent_name()` | `str` | `"research-agent"` |
| `capability()` | `str` | `"code_generation"` |

**RAG and embeddings:**

| Method | Return | Example |
|--------|--------|---------|
| `embedding_model()` | `str` | `"text-embedding-3-small"` |
| `vector_db_name()` | `str` | `"Pinecone"` |
| `chunk_id()` | `str` | `"chunk_a1b2c3d4"` |
| `similarity_score()` | `str` | `"0.9234"` |
| `namespace()` | `str` | `"production"` |

**Content moderation:**

| Method | Return | Example |
|--------|--------|---------|
| `moderation_category()` | `str` | `"hate"` |
| `moderation_score()` | `str` | `"0.0012"` |
| `harm_label()` | `str` | `"safe"` |

**Usage and billing:**

| Method | Return | Example |
|--------|--------|---------|
| `token_count()` | `str` | `"1234"` |
| `prompt_tokens()` | `str` | `"256"` |
| `completion_tokens()` | `str` | `"512"` |
| `cost_estimate()` | `str` | `"$0.0042"` |
| `rate_limit_header()` | `str` | `"x-ratelimit-remaining: 42"` |

### `ai_chat` — AI conversation data

| Method | Return | Example |
|--------|--------|---------|
| `chat_message()` | `dict` | `{"role": "user", "content": "...", ...}` |
| `chat_role()` | `str` | `"user"` |
| `chat_model()` | `str` | `"gpt-4o"` |
| `chat_content()` | `str` | `"Explain quantum computing..."` |
| `chat_tokens()` | `str` | `"256"` |
| `chat_finish_reason()` | `str` | `"stop"` |

`chat_message()` returns a coherent dict combining role, model, content, tokens, and finish\_reason.

---

## Schema API

The `Schema` API provides reusable blueprints for structured data generation. Field lookups are pre-resolved at creation time for maximum throughput.

```python
from dataforge import DataForge

forge = DataForge(seed=42)

# List of field names (auto-resolved to provider methods)
schema = forge.schema(["first_name", "last_name", "email", "city"])

# Dict with custom column names
schema = forge.schema({
    "Name": "person.full_name",
    "Email": "internet.email",
    "City": "address.city",
    "Price": "finance.price",
})

# Generate rows
rows = schema.generate(1000)   # list[dict[str, str]]

# Stream rows lazily (memory-efficient)
for row in schema.stream(1_000_000):
    process(row)

# Async streaming
async for row in schema.async_stream(1_000_000):
    await process(row)

# Export directly
csv_str  = schema.to_csv(count=5000)
jsonl_str = schema.to_jsonl(count=5000)
sql_str  = schema.to_sql(count=5000, table="users")
df       = schema.to_dataframe(count=5000)  # requires pandas
```

### Row-dependent fields (correlated data)

Schema supports callable values for fields that depend on other columns:

```python
schema = forge.schema({
    "City": "address.city",
    "Greeting": lambda row: f"Hello from {row['City']}!",
})
```

Callables receive the current row dict and execute after batch columns are generated.

## Bulk Export

Generate datasets directly from the `DataForge` instance:

```python
# List of dictionaries
rows = forge.to_dict(
    fields=["first_name", "last_name", "email", "company.job_title"],
    count=100
)

# CSV (returns string, optionally writes to file)
csv_data = forge.to_csv(
    fields={"Name": "person.full_name", "Email": "internet.email"},
    count=5000,
    path="users.csv"
)

# JSONL (returns string, optionally writes to file)
jsonl_data = forge.to_jsonl(
    fields=["first_name", "email", "city"],
    count=1000,
    path="users.jsonl"
)

# SQL INSERT statements
sql_data = forge.to_sql(
    fields=["first_name", "last_name", "email"],
    count=500,
    table="users",
    dialect="postgresql"  # "sqlite" (default), "mysql", or "postgresql"
)

# Pandas DataFrame (requires pandas)
df = forge.to_dataframe(
    fields=["date", "finance.price", "address.state"],
    count=10_000
)
```

## Streaming Export

For arbitrarily large datasets that don't fit in memory:

```python
# Stream to CSV file in batches
rows_written = forge.stream_to_csv(
    fields=["first_name", "email", "city"],
    path="users.csv",
    count=10_000_000,
    batch_size=100_000
)

# Stream to JSONL file in batches
rows_written = forge.stream_to_jsonl(
    fields=["first_name", "email", "city"],
    path="users.jsonl",
    count=10_000_000,
    batch_size=100_000
)
```

Batch size is auto-tuned when not specified.

## Integrations

### PyArrow

```python
# PyArrow Table
table = forge.to_arrow(
    fields=["first_name", "email", "city"],
    count=1_000_000
)

# Write Parquet file
rows_written = forge.to_parquet(
    fields=["first_name", "email", "city"],
    path="users.parquet",
    count=1_000_000
)
```

All columns are typed as `pa.string()`. Large datasets are generated in batches with bounded memory.

### Polars

```python
# Polars DataFrame
df = forge.to_polars(
    fields=["first_name", "email", "city"],
    count=1_000_000
)
```

All columns are typed as `pl.Utf8`. Large datasets use `pl.concat()` for efficient multi-batch assembly.

### Pydantic

Auto-generate schemas from Pydantic models:

```python
from pydantic import BaseModel

class User(BaseModel):
    first_name: str
    last_name: str
    email: str
    city: str

schema = forge.schema_from_pydantic(User)
rows = schema.generate(1000)  # list[dict] with keys matching model fields
```

Field names are matched to DataForge providers via direct name lookup and heuristic alias mapping (~70 common field names). Supports both Pydantic v1 and v2.

### SQLAlchemy

Auto-generate schemas from SQLAlchemy models:

```python
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    first_name: Mapped[str]
    last_name: Mapped[str]
    email: Mapped[str]

schema = forge.schema_from_sqlalchemy(User)
rows = schema.generate(1000)  # primary key 'id' is auto-skipped
```

## Command Line Interface

DataForge includes a CLI for generating data directly from the terminal.

```bash
# Generate 10 rows of CSV
dataforge --count 10 --format csv first_name last_name email

# Generate JSON output
dataforge -n 5 -f json company_name url city

# Generate JSONL
dataforge -n 100 -f jsonl first_name email city

# Write to file
dataforge -n 1000 -f csv -o users.csv first_name last_name email

# Use a specific locale and seed
dataforge --locale fr_FR --seed 42 -n 5 first_name city

# Omit headers
dataforge -n 10 -f csv --no-header first_name email

# List all available fields
dataforge --list-fields
```

### CLI Options

| Flag | Short | Description |
|------|-------|-------------|
| `--count N` | `-n` | Number of rows (default: 10) |
| `--format FMT` | `-f` | Output format: `text`, `csv`, `json`, `jsonl` |
| `--locale LOC` | `-l` | Locale code (default: `en_US`) |
| `--seed S` | `-s` | Random seed for reproducibility |
| `--output PATH` | `-o` | Write to file instead of stdout |
| `--no-header` | | Omit header row in text/csv output |
| `--list-fields` | | List all available field names |

Default fields (when none specified): `first_name`, `last_name`, `email`.

## Pytest Plugin

DataForge auto-registers as a pytest plugin via the `pytest11` entry point.

### Fixtures

| Fixture | Description |
|---------|-------------|
| `forge` | Seeded `DataForge` instance (deterministic) |
| `fake` | Alias for `forge` |
| `forge_unseeded` | Unseeded `DataForge` instance (non-deterministic) |

### Seed priority

1. `@pytest.mark.forge_seed(N)` marker (per-test)
2. `--forge-seed N` CLI option (session-wide)
3. Default: `0`

### Usage

```python
def test_name(forge):
    name = forge.person.first_name()
    assert isinstance(name, str)

def test_email(fake):
    assert "@" in fake.internet.email()

@pytest.mark.forge_seed(42)
def test_specific(forge):
    assert forge.person.first_name() == "James"

def test_random(forge_unseeded):
    name = forge_unseeded.person.first_name()
    assert len(name) > 0
```

```bash
# Run tests with a specific seed
pytest --forge-seed 42
```

## Unique Values

Generate guaranteed-unique values using the `forge.unique` proxy:

```python
# Single unique values
name1 = forge.unique.person.first_name()
name2 = forge.unique.person.first_name()
assert name1 != name2

# Unique batches
names = forge.unique.person.first_name(count=100)
assert len(names) == len(set(names))

# Clear tracking to reuse values
forge.unique.clear()                     # clear all providers
forge.unique.clear("person")             # clear specific provider
forge.unique.person.first_name.clear()   # clear specific method
```

The unique system uses a three-layer proxy architecture:

1. `forge.unique` — `UniqueProxy` wrapping the forge instance
2. `forge.unique.person` — `_UniqueProviderProxy` wrapping the provider
3. `forge.unique.person.first_name` — `_UniqueMethodWrapper` with set-based dedup

Batch generation uses **adaptive over-sampling** that starts at 20% extra per round and dynamically increases based on the observed collision rate, minimizing retry passes even at high saturation. Raises `RuntimeError` if uniqueness cannot be satisfied after extensive retries.

## Locales

DataForge supports 17 locales with locale-specific person names, addresses, companies, phone numbers, and internet domains:

| Locale | Language | Region |
|--------|----------|--------|
| `en_US` | English | United States |
| `en_GB` | English | United Kingdom |
| `en_AU` | English | Australia |
| `en_CA` | English | Canada |
| `de_DE` | German | Germany |
| `fr_FR` | French | France |
| `es_ES` | Spanish | Spain |
| `it_IT` | Italian | Italy |
| `pt_BR` | Portuguese | Brazil |
| `nl_NL` | Dutch | Netherlands |
| `pl_PL` | Polish | Poland |
| `ru_RU` | Russian (romanized) | Russia |
| `ar_SA` | Arabic (romanized) | Saudi Arabia |
| `hi_IN` | Hindi (romanized) | India |
| `ja_JP` | Japanese | Japan |
| `ko_KR` | Korean | South Korea |
| `zh_CN` | Chinese | China |

```python
forge = DataForge(locale="fr_FR")
forge.address.city()      # "Paris"
forge.person.full_name()  # "Jean Dupont"

forge = DataForge(locale="ja_JP")
forge.person.full_name()  # "田中太郎"
```

## Benchmarks

DataForge is built for speed. Results from a standard developer machine:

### Single Item Generation (10K iterations)

| Operation | Speed |
|-----------|-------|
| `misc.boolean()` | **9.2M items/s** |
| `person.first_name()` | **3.2M items/s** |
| `address.city()` | **3.1M items/s** |
| `dt.timezone()` | **3.2M items/s** |
| `network.port()` | **2.4M items/s** |
| `network.user_agent()` | **3.0M items/s** |
| `file.file_name()` | **1.5M items/s** |
| `dt.unix_timestamp()` | **1.3M items/s** |
| `finance.bic()` | **930K items/s** |

### Batch Generation (1M items)

| Operation | Speed |
|-----------|-------|
| `person.first_name(count=1M)` | **18M items/s** |
| `address.city(count=1M)` | **17M items/s** |
| `dt.timezone(count=1M)` | **18M items/s** |
| `network.user_agent(count=1M)` | **19M items/s** |
| `person.full_name(count=1M)` | **4.7M items/s** |
| `address.country(count=1M)` | **3.6M items/s** |
| `file.file_name(count=1M)` | **1.6M items/s** |
| `finance.bic(count=1M)` | **1.0M items/s** |

### Schema API (5 columns)

| Operation | Speed |
|-----------|-------|
| `generate(100K)` | **108K rows/s** |
| `to_csv(100K)` | **92K rows/s** |
| `stream(100K)` | **110K rows/s** |

Run benchmarks locally:

```bash
uv run python benchmark.py
uv run python benchmark.py --compare  # compare against saved baseline
```

### Performance Architecture

DataForge achieves its throughput through several layered optimizations:

- **Columnar generation** — Schema generates data column-first, then
  transposes to rows, enabling vectorized provider calls
- **`csv.writer` over `csv.DictWriter`** — bulk export skips per-row
  dict overhead, yielding ~36% faster CSV writes
- **Cumulative weight caching** — weighted random choices cache
  cumulative weight arrays at module level, avoiding recomputation
- **Bulk null injection** — uses `binomialvariate()` + `random.sample()`
  instead of per-element coin flips
- **Vectorized batch paths** — internet, datetime, and finance providers
  use dedicated batch code paths that avoid per-item method overhead
- **`deque` for BFS traversal** — relational generation uses O(1)
  `popleft()` instead of O(n) `list.pop(0)`
- **Adaptive unique over-sampling** — starts at 20% extra and scales
  with observed collision rate to minimize retry rounds
- **In-place list mutation** — `numerify()`/`bothify()` build lists
  once and mutate in place instead of appending

## CI/CD

DataForge uses GitHub Actions for continuous integration and delivery:

| Workflow | Trigger | Description |
|----------|---------|-------------|
| **CI** | Push/PR to main | Commitlint + Ruff lint/format + pytest matrix (Python 3.12, 3.13) |
| **Integrations** | Push/PR to main | Tests with optional deps (PyArrow, Polars, Pydantic, SQLAlchemy) |
| **Benchmarks** | Push to main | Runs `benchmark.py --compare`, uploads results as artifact |
| **Release** | Push to main | release-please creates/updates Release PR; on merge, publishes to PyPI |

### Release process

1. All commits to `main` use [Conventional Commits](https://www.conventionalcommits.org/) format
2. `release-please` automatically maintains a living Release PR that bundles changes
3. Merging the Release PR creates a bare numeric version tag (`0.3.0`, etc.) and a GitHub Release
4. The publish job within the same workflow builds and pushes to PyPI via OIDC trusted publishing

### Setup requirements

- **`pypi` environment** — GitHub Environment configured for PyPI OIDC trusted publishing

## Contributing

Contributions are welcome. Please follow these guidelines:

### Development setup

```bash
git clone https://github.com/yourusername/dataforge.git
cd dataforge
uv sync          # install all dependencies
uv run pytest    # run tests (1671 tests)
uv run ruff check src/ tests/        # lint
uv run ruff format --check src/ tests/ # format check
uv run python benchmark.py           # run benchmarks
```

### Commit messages

This project enforces [Conventional Commits](https://www.conventionalcommits.org/). All commit messages must follow this format:

```
<type>: <description>

[optional body]
```

| Type | Use when |
|------|----------|
| `feat` | Adding a new feature |
| `fix` | Fixing a bug |
| `perf` | Performance improvement |
| `refactor` | Code restructuring without behavior change |
| `test` | Adding or updating tests |
| `docs` | Documentation changes |
| `chore` | Maintenance tasks (deps, CI config) |

Examples:

```
feat: add automotive provider with VIN generation
fix: correct Luhn checksum in credit card numbers
perf: use getrandbits() bulk approach for UUID generation
```

### Performance guidelines

Performance is the primary selling point of DataForge. All contributions must:

1. **Never regress benchmarks** — run `uv run python benchmark.py --compare` before submitting
2. **Use `__slots__`** on all classes
3. **Use immutable tuples** for static data (never lists)
4. **Implement batch paths** — every public method must accept `count=N` with an optimized batch code path
5. **Use `@overload` triplets** for type narrowing (no args, `Literal[1]`, `int`)
6. **Inline hot paths** — avoid unnecessary function calls in batch loops

### Pull request process

1. Fork the repository
2. Create a feature branch
3. Make your changes with conventional commit messages
4. Ensure all tests pass and benchmarks don't regress
5. Submit a PR using the provided template

## Copy

Create an independent copy of a `DataForge` instance:

```python
forge2 = forge.copy(seed=99)  # new instance, same locale, different seed
forge3 = forge.copy()          # new instance, same locale, no seed
```

## License

MIT
