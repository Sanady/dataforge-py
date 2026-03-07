"""Benchmark script for dataforge.

Measures generation speed for single items vs batch generation
at various scales (100, 10K, 100K, 1M).

Usage:
    uv run python benchmark.py                 # run and print results
    uv run python benchmark.py --save out.json  # run and save to JSON
    uv run python benchmark.py --compare baseline.json  # compare against baseline
    uv run python benchmark.py --save out.json --compare baseline.json  # both
"""

import time

from dataforge import DataForge

# Global results collector — populated during the benchmark run.
# Structure: {"section.label": rate_items_per_second, ...}
_results: dict[str, float] = {}


def _fmt_time(seconds: float) -> str:
    """Format elapsed time in human-readable form."""
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.1f} µs"
    if seconds < 1.0:
        return f"{seconds * 1_000:.2f} ms"
    return f"{seconds:.3f} s"


def _fmt_rate(count: int, seconds: float) -> str:
    """Format items/second rate."""
    rate = count / seconds if seconds > 0 else float("inf")
    if rate >= 1_000_000:
        return f"{rate / 1_000_000:.2f}M items/s"
    if rate >= 1_000:
        return f"{rate / 1_000:.1f}K items/s"
    return f"{rate:.0f} items/s"


def _bench_scalar(label: str, fn: object, iterations: int) -> None:
    """Benchmark a single scalar call."""
    callable_fn = fn  # type: ignore[arg-type]
    start = time.perf_counter()
    for _ in range(iterations):
        callable_fn()
    elapsed = time.perf_counter() - start
    rate = iterations / elapsed if elapsed > 0 else float("inf")
    _results[f"scalar.{label}"] = rate
    print(
        f"  {label:28s}: {_fmt_time(elapsed):>12}  ({_fmt_rate(iterations, elapsed)})"
    )


def benchmark_single(forge: DataForge, iterations: int = 10_000) -> None:
    """Benchmark single-item generation."""
    print(f"\n{'=' * 60}")
    print(f"  SINGLE ITEM GENERATION ({iterations:,} iterations)")
    print(f"{'=' * 60}")

    calls: list[tuple[str, object]] = [
        # Person
        ("person.first_name()", forge.person.first_name),
        ("person.full_name()", forge.person.full_name),
        ("person.male_first_name()", forge.person.male_first_name),
        ("person.female_first_name()", forge.person.female_first_name),
        # Address
        ("address.city()", forge.address.city),
        ("address.full_address()", forge.address.full_address),
        ("address.country()", forge.address.country),
        ("address.country_code()", forge.address.country_code),
        ("address.latitude()", forge.address.latitude),
        ("address.longitude()", forge.address.longitude),
        ("address.coordinate()", forge.address.coordinate),
        # Internet
        ("internet.email()", forge.internet.email),
        ("internet.ipv4()", forge.internet.ipv4),
        ("internet.slug()", forge.internet.slug),
        ("internet.tld()", forge.internet.tld),
        ("internet.safe_email()", forge.internet.safe_email),
        # Company
        ("company.company_name()", forge.company.company_name),
        # Phone
        ("phone.phone_number()", forge.phone.phone_number),
        # Lorem
        ("lorem.sentence()", forge.lorem.sentence),
        # DateTime
        ("dt.date()", forge.dt.date),
        ("dt.timezone()", forge.dt.timezone),
        ("dt.unix_timestamp()", forge.dt.unix_timestamp),
        # Finance
        ("finance.credit_card_number()", forge.finance.credit_card_number),
        ("finance.iban()", forge.finance.iban),
        ("finance.price()", forge.finance.price),
        ("finance.bic()", forge.finance.bic),
        ("finance.routing_number()", forge.finance.routing_number),
        ("finance.bitcoin_address()", forge.finance.bitcoin_address),
        # Color
        ("color.hex_color()", forge.color.hex_color),
        ("color.rgb()", forge.color.rgb),
        # File
        ("file.file_name()", forge.file.file_name),
        ("file.file_path()", forge.file.file_path),
        # Network (new)
        ("network.ipv6()", forge.network.ipv6),
        ("network.mac_address()", forge.network.mac_address),
        ("network.port()", forge.network.port),
        ("network.hostname()", forge.network.hostname),
        ("network.user_agent()", forge.network.user_agent),
        ("network.http_method()", forge.network.http_method),
        ("network.http_status_code()", forge.network.http_status_code),
        # Misc (new)
        ("misc.uuid4()", forge.misc.uuid4),
        ("misc.uuid7()", forge.misc.uuid7),
        ("misc.boolean()", forge.misc.boolean),
        # Barcode (new)
        ("barcode.ean13()", forge.barcode.ean13),
        ("barcode.ean8()", forge.barcode.ean8),
        ("barcode.isbn13()", forge.barcode.isbn13),
        ("barcode.isbn10()", forge.barcode.isbn10),
        # Government
        ("government.ssn()", forge.government.ssn),
        ("government.tax_id()", forge.government.tax_id),
        ("government.passport_number()", forge.government.passport_number),
        ("government.drivers_license()", forge.government.drivers_license),
        ("government.national_id()", forge.government.national_id),
        # Ecommerce
        ("ecommerce.product_name()", forge.ecommerce.product_name),
        ("ecommerce.product_category()", forge.ecommerce.product_category),
        ("ecommerce.sku()", forge.ecommerce.sku),
        ("ecommerce.price_with_currency()", forge.ecommerce.price_with_currency),
        ("ecommerce.review_rating()", forge.ecommerce.review_rating),
        ("ecommerce.tracking_number()", forge.ecommerce.tracking_number),
        ("ecommerce.order_id()", forge.ecommerce.order_id),
        # Medical
        ("medical.blood_type()", forge.medical.blood_type),
        ("medical.icd10_code()", forge.medical.icd10_code),
        ("medical.drug_name()", forge.medical.drug_name),
        ("medical.dosage()", forge.medical.dosage),
        ("medical.diagnosis()", forge.medical.diagnosis),
        ("medical.procedure()", forge.medical.procedure),
        ("medical.medical_record_number()", forge.medical.medical_record_number),
        # Payment
        ("payment.card_type()", forge.payment.card_type),
        ("payment.payment_method()", forge.payment.payment_method),
        ("payment.transaction_id()", forge.payment.transaction_id),
        ("payment.currency_code()", forge.payment.currency_code),
        ("payment.cvv()", forge.payment.cvv),
        ("payment.expiry_date()", forge.payment.expiry_date),
        ("payment.payment_amount()", forge.payment.payment_amount),
        # Text
        ("text.quote()", forge.text.quote),
        ("text.headline()", forge.text.headline),
        ("text.buzzword()", forge.text.buzzword),
        ("text.paragraph()", forge.text.paragraph),
        ("text.text_block()", forge.text.text_block),
        # Geo
        ("geo.continent()", forge.geo.continent),
        ("geo.ocean()", forge.geo.ocean),
        ("geo.mountain_range()", forge.geo.mountain_range),
        ("geo.river()", forge.geo.river),
        ("geo.compass_direction()", forge.geo.compass_direction),
        ("geo.geo_coordinate()", forge.geo.geo_coordinate),
        ("geo.geo_hash()", forge.geo.geo_hash),
        # Science
        ("science.chemical_element()", forge.science.chemical_element),
        ("science.element_symbol()", forge.science.element_symbol),
        ("science.si_unit()", forge.science.si_unit),
        ("science.planet()", forge.science.planet),
        ("science.constellation()", forge.science.constellation),
        ("science.metric_prefix()", forge.science.metric_prefix),
        # AI Prompt
        ("ai_prompt.user_prompt()", forge.ai_prompt.user_prompt),
        ("ai_prompt.coding_prompt()", forge.ai_prompt.coding_prompt),
        ("ai_prompt.creative_prompt()", forge.ai_prompt.creative_prompt),
        ("ai_prompt.system_prompt()", forge.ai_prompt.system_prompt),
        ("ai_prompt.persona_prompt()", forge.ai_prompt.persona_prompt),
        ("ai_prompt.prompt_template()", forge.ai_prompt.prompt_template),
        ("ai_prompt.few_shot_prompt()", forge.ai_prompt.few_shot_prompt),
        # LLM
        ("llm.model_name()", forge.llm.model_name),
        ("llm.provider_name()", forge.llm.provider_name),
        ("llm.api_key()", forge.llm.api_key),
        ("llm.finish_reason()", forge.llm.finish_reason),
        ("llm.tool_name()", forge.llm.tool_name),
        ("llm.tool_call_id()", forge.llm.tool_call_id),
        ("llm.agent_name()", forge.llm.agent_name),
        ("llm.embedding_model()", forge.llm.embedding_model),
        ("llm.vector_db_name()", forge.llm.vector_db_name),
        ("llm.chunk_id()", forge.llm.chunk_id),
        ("llm.similarity_score()", forge.llm.similarity_score),
        ("llm.moderation_category()", forge.llm.moderation_category),
        ("llm.harm_label()", forge.llm.harm_label),
        ("llm.token_count()", forge.llm.token_count),
        ("llm.cost_estimate()", forge.llm.cost_estimate),
        ("llm.rate_limit_header()", forge.llm.rate_limit_header),
        # AI Chat (compound)
        ("ai_chat.chat_role()", forge.ai_chat.chat_role),
        ("ai_chat.chat_model()", forge.ai_chat.chat_model),
        ("ai_chat.chat_content()", forge.ai_chat.chat_content),
        ("ai_chat.chat_tokens()", forge.ai_chat.chat_tokens),
        # Social Media
        ("social_media.platform()", forge.social_media.platform),
        ("social_media.username()", forge.social_media.username),
        ("social_media.hashtag()", forge.social_media.hashtag),
        ("social_media.post_type()", forge.social_media.post_type),
        ("social_media.reaction()", forge.social_media.reaction),
        ("social_media.follower_count()", forge.social_media.follower_count),
        # Music
        ("music.genre()", forge.music.genre),
        ("music.artist()", forge.music.artist),
        ("music.album()", forge.music.album),
        ("music.song()", forge.music.song),
        ("music.instrument()", forge.music.instrument),
        ("music.record_label()", forge.music.record_label),
        ("music.duration()", forge.music.duration),
        # Sports
        ("sports.sport()", forge.sports.sport),
        ("sports.team()", forge.sports.team),
        ("sports.league()", forge.sports.league),
        ("sports.position()", forge.sports.position),
        ("sports.venue()", forge.sports.venue),
        ("sports.athlete()", forge.sports.athlete),
        ("sports.score()", forge.sports.score),
        # Food
        ("food.dish()", forge.food.dish),
        ("food.cuisine()", forge.food.cuisine),
        ("food.ingredient()", forge.food.ingredient),
        ("food.restaurant()", forge.food.restaurant),
        ("food.dietary_tag()", forge.food.dietary_tag),
        ("food.beverage()", forge.food.beverage),
        ("food.cooking_method()", forge.food.cooking_method),
        # Legal
        ("legal.case_number()", forge.legal.case_number),
        ("legal.court()", forge.legal.court),
        ("legal.practice_area()", forge.legal.practice_area),
        ("legal.legal_term()", forge.legal.legal_term),
        ("legal.law_firm()", forge.legal.law_firm),
        ("legal.judge()", forge.legal.judge),
        ("legal.verdict()", forge.legal.verdict),
        # Real Estate
        ("real_estate.property_type()", forge.real_estate.property_type),
        ("real_estate.listing_price()", forge.real_estate.listing_price),
        ("real_estate.square_footage()", forge.real_estate.square_footage),
        ("real_estate.neighborhood()", forge.real_estate.neighborhood),
        ("real_estate.listing_status()", forge.real_estate.listing_status),
        ("real_estate.amenity()", forge.real_estate.amenity),
        # Weather
        ("weather.condition()", forge.weather.condition),
        ("weather.temperature()", forge.weather.temperature),
        ("weather.humidity()", forge.weather.humidity),
        ("weather.wind_speed()", forge.weather.wind_speed),
        ("weather.wind_direction()", forge.weather.wind_direction),
        ("weather.alert()", forge.weather.alert),
        # Hardware
        ("hardware.cpu()", forge.hardware.cpu),
        ("hardware.gpu()", forge.hardware.gpu),
        ("hardware.ram_size()", forge.hardware.ram_size),
        ("hardware.ram_type()", forge.hardware.ram_type),
        ("hardware.storage()", forge.hardware.storage),
        ("hardware.form_factor()", forge.hardware.form_factor),
        ("hardware.peripheral()", forge.hardware.peripheral),
        ("hardware.manufacturer()", forge.hardware.manufacturer),
        # Logistics
        ("logistics.carrier()", forge.logistics.carrier),
        ("logistics.shipping_method()", forge.logistics.shipping_method),
        ("logistics.container_type()", forge.logistics.container_type),
        ("logistics.tracking_status()", forge.logistics.tracking_status),
        ("logistics.warehouse()", forge.logistics.warehouse),
        ("logistics.package_type()", forge.logistics.package_type),
        ("logistics.hs_code()", forge.logistics.hs_code),
        ("logistics.freight_class()", forge.logistics.freight_class),
    ]
    for label, fn in calls:
        _bench_scalar(label, fn, iterations)


def _bench_batch(label: str, fn: object, batch_sizes: list[int]) -> None:
    """Benchmark a batch call at various sizes."""
    callable_fn = fn  # type: ignore[arg-type]
    print(f"\n{'=' * 60}")
    print(f"  BATCH — {label}")
    print(f"{'=' * 60}")
    for n in batch_sizes:
        start = time.perf_counter()
        callable_fn(count=n)
        elapsed = time.perf_counter() - start
        rate = n / elapsed if elapsed > 0 else float("inf")
        _results[f"batch.{label}.{n}"] = rate
        print(f"  count={n:<12,}: {_fmt_time(elapsed):>12}  ({_fmt_rate(n, elapsed)})")


def benchmark_batch(forge: DataForge) -> None:
    """Benchmark batch generation at various scales."""
    batch_sizes = [100, 10_000, 100_000, 1_000_000]

    calls: list[tuple[str, object]] = [
        ("person.first_name(count=N)", forge.person.first_name),
        ("person.full_name(count=N)", forge.person.full_name),
        ("address.city(count=N)", forge.address.city),
        ("address.full_address(count=N)", forge.address.full_address),
        ("address.country(count=N)", forge.address.country),
        ("address.latitude(count=N)", forge.address.latitude),
        ("internet.email(count=N)", forge.internet.email),
        ("internet.ipv4(count=N)", forge.internet.ipv4),
        ("internet.slug(count=N)", forge.internet.slug),
        ("internet.safe_email(count=N)", forge.internet.safe_email),
        ("dt.timezone(count=N)", forge.dt.timezone),
        ("dt.unix_timestamp(count=N)", forge.dt.unix_timestamp),
        ("finance.credit_card_number(count=N)", forge.finance.credit_card_number),
        ("finance.bic(count=N)", forge.finance.bic),
        ("finance.routing_number(count=N)", forge.finance.routing_number),
        ("finance.bitcoin_address(count=N)", forge.finance.bitcoin_address),
        ("color.hex_color(count=N)", forge.color.hex_color),
        ("file.file_name(count=N)", forge.file.file_name),
        ("network.ipv6(count=N)", forge.network.ipv6),
        ("network.mac_address(count=N)", forge.network.mac_address),
        ("network.hostname(count=N)", forge.network.hostname),
        ("network.user_agent(count=N)", forge.network.user_agent),
        ("misc.uuid4(count=N)", forge.misc.uuid4),
        ("misc.uuid7(count=N)", forge.misc.uuid7),
        ("barcode.ean13(count=N)", forge.barcode.ean13),
        ("barcode.isbn13(count=N)", forge.barcode.isbn13),
        # Government
        ("government.ssn(count=N)", forge.government.ssn),
        ("government.tax_id(count=N)", forge.government.tax_id),
        ("government.passport_number(count=N)", forge.government.passport_number),
        ("government.national_id(count=N)", forge.government.national_id),
        # Ecommerce
        ("ecommerce.product_name(count=N)", forge.ecommerce.product_name),
        ("ecommerce.sku(count=N)", forge.ecommerce.sku),
        ("ecommerce.tracking_number(count=N)", forge.ecommerce.tracking_number),
        ("ecommerce.order_id(count=N)", forge.ecommerce.order_id),
        # Medical
        ("medical.blood_type(count=N)", forge.medical.blood_type),
        ("medical.icd10_code(count=N)", forge.medical.icd10_code),
        ("medical.drug_name(count=N)", forge.medical.drug_name),
        ("medical.diagnosis(count=N)", forge.medical.diagnosis),
        ("medical.medical_record_number(count=N)", forge.medical.medical_record_number),
        # Payment
        ("payment.card_type(count=N)", forge.payment.card_type),
        ("payment.transaction_id(count=N)", forge.payment.transaction_id),
        ("payment.currency_code(count=N)", forge.payment.currency_code),
        ("payment.cvv(count=N)", forge.payment.cvv),
        ("payment.expiry_date(count=N)", forge.payment.expiry_date),
        ("payment.payment_amount(count=N)", forge.payment.payment_amount),
        # Text
        ("text.quote(count=N)", forge.text.quote),
        ("text.headline(count=N)", forge.text.headline),
        ("text.buzzword(count=N)", forge.text.buzzword),
        ("text.paragraph(count=N)", forge.text.paragraph),
        # Geo
        ("geo.continent(count=N)", forge.geo.continent),
        ("geo.ocean(count=N)", forge.geo.ocean),
        ("geo.mountain_range(count=N)", forge.geo.mountain_range),
        ("geo.river(count=N)", forge.geo.river),
        ("geo.geo_coordinate(count=N)", forge.geo.geo_coordinate),
        ("geo.geo_hash(count=N)", forge.geo.geo_hash),
        # Science
        ("science.chemical_element(count=N)", forge.science.chemical_element),
        ("science.element_symbol(count=N)", forge.science.element_symbol),
        ("science.si_unit(count=N)", forge.science.si_unit),
        ("science.planet(count=N)", forge.science.planet),
        ("science.constellation(count=N)", forge.science.constellation),
        # AI Prompt
        ("ai_prompt.user_prompt(count=N)", forge.ai_prompt.user_prompt),
        ("ai_prompt.coding_prompt(count=N)", forge.ai_prompt.coding_prompt),
        ("ai_prompt.system_prompt(count=N)", forge.ai_prompt.system_prompt),
        ("ai_prompt.persona_prompt(count=N)", forge.ai_prompt.persona_prompt),
        ("ai_prompt.prompt_template(count=N)", forge.ai_prompt.prompt_template),
        ("ai_prompt.few_shot_prompt(count=N)", forge.ai_prompt.few_shot_prompt),
        # LLM
        ("llm.model_name(count=N)", forge.llm.model_name),
        ("llm.provider_name(count=N)", forge.llm.provider_name),
        ("llm.api_key(count=N)", forge.llm.api_key),
        ("llm.finish_reason(count=N)", forge.llm.finish_reason),
        ("llm.tool_name(count=N)", forge.llm.tool_name),
        ("llm.tool_call_id(count=N)", forge.llm.tool_call_id),
        ("llm.agent_name(count=N)", forge.llm.agent_name),
        ("llm.chunk_id(count=N)", forge.llm.chunk_id),
        ("llm.similarity_score(count=N)", forge.llm.similarity_score),
        ("llm.moderation_category(count=N)", forge.llm.moderation_category),
        ("llm.harm_label(count=N)", forge.llm.harm_label),
        ("llm.token_count(count=N)", forge.llm.token_count),
        ("llm.cost_estimate(count=N)", forge.llm.cost_estimate),
        ("llm.rate_limit_header(count=N)", forge.llm.rate_limit_header),
        # AI Chat (compound)
        ("ai_chat.chat_role(count=N)", forge.ai_chat.chat_role),
        ("ai_chat.chat_model(count=N)", forge.ai_chat.chat_model),
        ("ai_chat.chat_content(count=N)", forge.ai_chat.chat_content),
        ("ai_chat.chat_tokens(count=N)", forge.ai_chat.chat_tokens),
        # Social Media
        ("social_media.platform(count=N)", forge.social_media.platform),
        ("social_media.username(count=N)", forge.social_media.username),
        ("social_media.hashtag(count=N)", forge.social_media.hashtag),
        ("social_media.post_type(count=N)", forge.social_media.post_type),
        ("social_media.reaction(count=N)", forge.social_media.reaction),
        ("social_media.follower_count(count=N)", forge.social_media.follower_count),
        # Music
        ("music.genre(count=N)", forge.music.genre),
        ("music.artist(count=N)", forge.music.artist),
        ("music.album(count=N)", forge.music.album),
        ("music.song(count=N)", forge.music.song),
        ("music.instrument(count=N)", forge.music.instrument),
        ("music.record_label(count=N)", forge.music.record_label),
        ("music.duration(count=N)", forge.music.duration),
        # Sports
        ("sports.sport(count=N)", forge.sports.sport),
        ("sports.team(count=N)", forge.sports.team),
        ("sports.league(count=N)", forge.sports.league),
        ("sports.position(count=N)", forge.sports.position),
        ("sports.venue(count=N)", forge.sports.venue),
        ("sports.athlete(count=N)", forge.sports.athlete),
        ("sports.score(count=N)", forge.sports.score),
        # Food
        ("food.dish(count=N)", forge.food.dish),
        ("food.cuisine(count=N)", forge.food.cuisine),
        ("food.ingredient(count=N)", forge.food.ingredient),
        ("food.restaurant(count=N)", forge.food.restaurant),
        ("food.dietary_tag(count=N)", forge.food.dietary_tag),
        ("food.beverage(count=N)", forge.food.beverage),
        ("food.cooking_method(count=N)", forge.food.cooking_method),
        # Legal
        ("legal.case_number(count=N)", forge.legal.case_number),
        ("legal.court(count=N)", forge.legal.court),
        ("legal.practice_area(count=N)", forge.legal.practice_area),
        ("legal.legal_term(count=N)", forge.legal.legal_term),
        ("legal.law_firm(count=N)", forge.legal.law_firm),
        ("legal.judge(count=N)", forge.legal.judge),
        ("legal.verdict(count=N)", forge.legal.verdict),
        # Real Estate
        ("real_estate.property_type(count=N)", forge.real_estate.property_type),
        ("real_estate.listing_price(count=N)", forge.real_estate.listing_price),
        ("real_estate.square_footage(count=N)", forge.real_estate.square_footage),
        ("real_estate.neighborhood(count=N)", forge.real_estate.neighborhood),
        ("real_estate.listing_status(count=N)", forge.real_estate.listing_status),
        ("real_estate.amenity(count=N)", forge.real_estate.amenity),
        # Weather
        ("weather.condition(count=N)", forge.weather.condition),
        ("weather.temperature(count=N)", forge.weather.temperature),
        ("weather.humidity(count=N)", forge.weather.humidity),
        ("weather.wind_speed(count=N)", forge.weather.wind_speed),
        ("weather.wind_direction(count=N)", forge.weather.wind_direction),
        ("weather.alert(count=N)", forge.weather.alert),
        # Hardware
        ("hardware.cpu(count=N)", forge.hardware.cpu),
        ("hardware.gpu(count=N)", forge.hardware.gpu),
        ("hardware.ram_size(count=N)", forge.hardware.ram_size),
        ("hardware.ram_type(count=N)", forge.hardware.ram_type),
        ("hardware.storage(count=N)", forge.hardware.storage),
        ("hardware.form_factor(count=N)", forge.hardware.form_factor),
        ("hardware.peripheral(count=N)", forge.hardware.peripheral),
        ("hardware.manufacturer(count=N)", forge.hardware.manufacturer),
        # Logistics
        ("logistics.carrier(count=N)", forge.logistics.carrier),
        ("logistics.shipping_method(count=N)", forge.logistics.shipping_method),
        ("logistics.container_type(count=N)", forge.logistics.container_type),
        ("logistics.tracking_status(count=N)", forge.logistics.tracking_status),
        ("logistics.warehouse(count=N)", forge.logistics.warehouse),
        ("logistics.package_type(count=N)", forge.logistics.package_type),
        ("logistics.hs_code(count=N)", forge.logistics.hs_code),
        ("logistics.freight_class(count=N)", forge.logistics.freight_class),
    ]
    for label, fn in calls:
        _bench_batch(label, fn, batch_sizes)


def benchmark_startup() -> None:
    """Benchmark DataForge initialization time."""
    print(f"\n{'=' * 60}")
    print("  STARTUP TIME")
    print(f"{'=' * 60}")

    # Cold init (no provider access)
    start = time.perf_counter()
    for _ in range(1_000):
        DataForge(seed=42)
    elapsed = time.perf_counter() - start
    _results["startup.init_us"] = (elapsed / 1000) * 1_000_000
    print(
        f"  DataForge() x 1000     : {_fmt_time(elapsed):>12}  ({_fmt_time(elapsed / 1000)} per init)"
    )

    # Init + first provider access
    start = time.perf_counter()
    for _ in range(1_000):
        f = DataForge(seed=42)
        f.person.first_name()
    elapsed = time.perf_counter() - start
    _results["startup.init_plus_first_field_us"] = (elapsed / 1000) * 1_000_000
    print(
        f"  Init + first_name()    : {_fmt_time(elapsed):>12}  ({_fmt_time(elapsed / 1000)} per call)"
    )


def benchmark_schema(forge: DataForge) -> None:
    """Benchmark the Schema API at various scales."""
    print(f"\n{'=' * 60}")
    print("  SCHEMA API BENCHMARK")
    print(f"{'=' * 60}")

    schema = forge.schema(
        ["first_name", "last_name", "email", "city", "date"],
    )

    for n in [100, 10_000, 100_000]:
        start = time.perf_counter()
        schema.generate(n)
        elapsed = time.perf_counter() - start
        rate = n / elapsed if elapsed > 0 else float("inf")
        _results[f"schema.generate.{n}"] = rate
        print(
            f"  generate({n:<8,}) 5 cols : {_fmt_time(elapsed):>12}  ({_fmt_rate(n, elapsed)})"
        )

    # CSV output benchmark
    for n in [10_000, 100_000]:
        start = time.perf_counter()
        schema.to_csv(n)
        elapsed = time.perf_counter() - start
        rate = n / elapsed if elapsed > 0 else float("inf")
        _results[f"schema.to_csv.{n}"] = rate
        print(
            f"  to_csv({n:<8,})  5 cols : {_fmt_time(elapsed):>12}  ({_fmt_rate(n, elapsed)})"
        )

    # Stream benchmark
    start = time.perf_counter()
    count = 0
    for _ in schema.stream(100_000):
        count += 1
    elapsed = time.perf_counter() - start
    rate = count / elapsed if elapsed > 0 else float("inf")
    _results[f"schema.stream.{count}"] = rate
    print(
        f"  stream({count:<8,})  5 cols : {_fmt_time(elapsed):>12}  ({_fmt_rate(count, elapsed)})"
    )

    # Streaming file write benchmarks
    import os
    import tempfile

    for n in [10_000, 100_000]:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            start = time.perf_counter()
            schema.stream_to_csv(path, n)
            elapsed = time.perf_counter() - start
            rate = n / elapsed if elapsed > 0 else float("inf")
            _results[f"schema.stream_csv.{n}"] = rate
            print(
                f"  stream_csv({n:<6,}) 5 cols : {_fmt_time(elapsed):>12}  ({_fmt_rate(n, elapsed)})"
            )
        finally:
            os.unlink(path)

    for n in [10_000, 100_000]:
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            path = f.name
        try:
            start = time.perf_counter()
            schema.stream_to_jsonl(path, n)
            elapsed = time.perf_counter() - start
            rate = n / elapsed if elapsed > 0 else float("inf")
            _results[f"schema.stream_jsonl.{n}"] = rate
            print(
                f"  stream_jsonl({n:<4,}) 5 cols : {_fmt_time(elapsed):>12}  ({_fmt_rate(n, elapsed)})"
            )
        finally:
            os.unlink(path)


def _save_results(path: str) -> None:
    """Save benchmark results to a JSON file."""
    import json

    with open(path, "w", encoding="utf-8") as f:
        json.dump(_results, f, indent=2, sort_keys=True)
    print(f"\n  Results saved to {path}")


def _compare_results(baseline_path: str) -> int:
    """Compare current results against a baseline JSON file.

    Returns the number of regressions detected (0 = clean).
    Returns -1 if the baseline file does not exist yet (first run).
    """
    import json
    import os

    if not os.path.exists(baseline_path):
        print(
            f"\n  NOTE: Baseline file '{baseline_path}' not found — skipping comparison (first run)."
        )
        return 0

    with open(baseline_path, encoding="utf-8") as f:
        baseline: dict[str, float] = json.load(f)

    print(f"\n{'=' * 72}")
    print("  COMPARISON vs BASELINE")
    print(f"{'=' * 72}")
    print(f"  {'Benchmark':<45} {'Change':>10}  {'Status':>8}")
    print(f"  {'-' * 45} {'-' * 10}  {'-' * 8}")

    regressions = 0
    improvements = 0
    # Threshold: 5% regression is flagged
    REGRESSION_THRESHOLD = -0.05

    for key in sorted(_results):
        if key not in baseline:
            continue
        old = baseline[key]
        new = _results[key]
        if old == 0:
            continue
        # For startup metrics (in µs), lower is better → invert
        if key.startswith("startup."):
            pct = (old - new) / old  # positive = improvement (lower time)
        else:
            pct = (new - old) / old  # positive = improvement (higher rate)

        if pct < REGRESSION_THRESHOLD:
            status = "REGR"
            regressions += 1
        elif pct > 0.05:
            status = "BETTER"
            improvements += 1
        else:
            status = "OK"

        sign = "+" if pct >= 0 else ""
        # Truncate long key names
        short_key = key if len(key) <= 45 else key[:42] + "..."
        print(f"  {short_key:<45} {sign}{pct:>8.1%}  {status:>8}")

    print(
        f"\n  Summary: {improvements} improved, {regressions} regressions, "
        f"{len(_results) - improvements - regressions} unchanged"
    )
    if regressions > 0:
        print(f"  WARNING: {regressions} benchmark(s) regressed by >5%!")

    return regressions


def main() -> int:
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="DataForge benchmark suite")
    parser.add_argument(
        "--save",
        metavar="PATH",
        help="Save results as JSON to this file",
    )
    parser.add_argument(
        "--compare",
        metavar="PATH",
        help="Compare results against a baseline JSON file",
    )
    args = parser.parse_args()

    print()
    print("  DATAFORGE BENCHMARK")
    print("  ===================")

    forge = DataForge(locale="en_US", seed=42)

    benchmark_startup()
    benchmark_single(forge)
    benchmark_batch(forge)
    benchmark_schema(forge)

    print(f"\n{'=' * 60}")
    print("  DONE")
    print(f"{'=' * 60}")

    regressions = 0
    if args.save:
        _save_results(args.save)

    if args.compare:
        regressions = _compare_results(args.compare)

    print()

    if regressions > 0:
        sys.exit(1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
