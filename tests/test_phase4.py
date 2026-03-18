"""Tests for Phase 4 — new data providers.

Covers all 9 new providers: social_media, music, sports, food,
legal, real_estate, weather, hardware, logistics.

Each provider is tested for:
- Scalar return (count=1 → str)
- Batch return (count>1 → list[str])
- Schema integration (field_map entries work in Schema)
- Registry discovery (provider appears in registry)
"""

import re

import pytest

from dataforge import DataForge


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture
def forge() -> DataForge:
    return DataForge(seed=42)


# ==================================================================
# SocialMediaProvider
# ==================================================================


class TestSocialMediaProvider:
    def test_platform_scalar(self, forge: DataForge) -> None:
        result = forge.social_media.platform()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_platform_batch(self, forge: DataForge) -> None:
        result = forge.social_media.platform(count=10)
        assert isinstance(result, list)
        assert len(result) == 10
        assert all(isinstance(x, str) for x in result)

    def test_username_scalar(self, forge: DataForge) -> None:
        result = forge.social_media.username()
        assert isinstance(result, str)
        assert "_" in result  # pattern: adj_nounN

    def test_username_batch(self, forge: DataForge) -> None:
        result = forge.social_media.username(count=5)
        assert isinstance(result, list)
        assert len(result) == 5

    def test_hashtag_scalar(self, forge: DataForge) -> None:
        result = forge.social_media.hashtag()
        assert isinstance(result, str)
        assert result.startswith("#")

    def test_hashtag_batch(self, forge: DataForge) -> None:
        result = forge.social_media.hashtag(count=5)
        assert all(x.startswith("#") for x in result)

    def test_post_type_scalar(self, forge: DataForge) -> None:
        result = forge.social_media.post_type()
        assert isinstance(result, str)

    def test_reaction_scalar(self, forge: DataForge) -> None:
        result = forge.social_media.reaction()
        assert isinstance(result, str)

    def test_follower_count_scalar(self, forge: DataForge) -> None:
        result = forge.social_media.follower_count()
        assert isinstance(result, str)

    def test_content_scalar(self, forge: DataForge) -> None:
        result = forge.social_media.content()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_verified_scalar(self, forge: DataForge) -> None:
        result = forge.social_media.verified()
        assert result in ("verified", "unverified")

    def test_verified_batch(self, forge: DataForge) -> None:
        result = forge.social_media.verified(count=20)
        assert all(x in ("verified", "unverified") for x in result)

    def test_schema_integration(self, forge: DataForge) -> None:
        schema = forge.schema(["social_platform", "hashtag", "post_type"])
        rows = schema.generate(5)
        assert len(rows) == 5
        assert all("social_platform" in r for r in rows)
        assert all(r["hashtag"].startswith("#") for r in rows)


# ==================================================================
# MusicProvider
# ==================================================================


class TestMusicProvider:
    def test_genre_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.music.genre(), str)

    def test_genre_batch(self, forge: DataForge) -> None:
        result = forge.music.genre(count=10)
        assert isinstance(result, list) and len(result) == 10

    def test_artist_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.music.artist(), str)

    def test_album_scalar(self, forge: DataForge) -> None:
        result = forge.music.album()
        assert isinstance(result, str)
        assert " " in result  # "Adjective Noun" pattern

    def test_album_batch(self, forge: DataForge) -> None:
        result = forge.music.album(count=5)
        assert len(result) == 5

    def test_song_scalar(self, forge: DataForge) -> None:
        result = forge.music.song()
        assert isinstance(result, str)
        assert " " in result

    def test_song_batch(self, forge: DataForge) -> None:
        result = forge.music.song(count=5)
        assert len(result) == 5

    def test_instrument_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.music.instrument(), str)

    def test_record_label_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.music.record_label(), str)

    def test_streaming_service_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.music.streaming_service(), str)

    def test_duration_scalar(self, forge: DataForge) -> None:
        result = forge.music.duration()
        assert isinstance(result, str)
        assert re.match(r"^\d:\d{2}$", result)

    def test_duration_batch(self, forge: DataForge) -> None:
        result = forge.music.duration(count=5)
        assert all(re.match(r"^\d:\d{2}$", x) for x in result)

    def test_bpm_scalar(self, forge: DataForge) -> None:
        result = forge.music.bpm()
        assert result.isdigit()
        assert 60 <= int(result) <= 200

    def test_schema_integration(self, forge: DataForge) -> None:
        schema = forge.schema(["genre", "artist", "song_title", "instrument"])
        rows = schema.generate(5)
        assert len(rows) == 5
        assert all("genre" in r for r in rows)


# ==================================================================
# SportsProvider
# ==================================================================


class TestSportsProvider:
    def test_sport_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.sports.sport(), str)

    def test_sport_batch(self, forge: DataForge) -> None:
        result = forge.sports.sport(count=10)
        assert isinstance(result, list) and len(result) == 10

    def test_team_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.sports.team(), str)

    def test_league_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.sports.league(), str)

    def test_position_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.sports.position(), str)

    def test_venue_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.sports.venue(), str)

    def test_event_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.sports.event(), str)

    def test_athlete_scalar(self, forge: DataForge) -> None:
        result = forge.sports.athlete()
        assert isinstance(result, str)
        assert " " in result  # "First Last" pattern

    def test_athlete_batch(self, forge: DataForge) -> None:
        result = forge.sports.athlete(count=5)
        assert len(result) == 5
        assert all(" " in x for x in result)

    def test_score_scalar(self, forge: DataForge) -> None:
        result = forge.sports.score()
        assert isinstance(result, str)
        assert "-" in result

    def test_score_batch(self, forge: DataForge) -> None:
        result = forge.sports.score(count=5)
        assert all("-" in x for x in result)

    def test_schema_integration(self, forge: DataForge) -> None:
        schema = forge.schema(["sport", "team", "league", "venue"])
        rows = schema.generate(5)
        assert len(rows) == 5


# ==================================================================
# FoodProvider
# ==================================================================


class TestFoodProvider:
    def test_dish_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.food.dish(), str)

    def test_dish_batch(self, forge: DataForge) -> None:
        result = forge.food.dish(count=10)
        assert isinstance(result, list) and len(result) == 10

    def test_cuisine_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.food.cuisine(), str)

    def test_ingredient_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.food.ingredient(), str)

    def test_restaurant_scalar(self, forge: DataForge) -> None:
        result = forge.food.restaurant()
        assert isinstance(result, str)
        assert result.startswith("The ")

    def test_restaurant_batch(self, forge: DataForge) -> None:
        result = forge.food.restaurant(count=5)
        assert all(x.startswith("The ") for x in result)

    def test_dietary_tag_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.food.dietary_tag(), str)

    def test_beverage_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.food.beverage(), str)

    def test_cooking_method_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.food.cooking_method(), str)

    def test_meal_price_scalar(self, forge: DataForge) -> None:
        result = forge.food.meal_price()
        assert result.startswith("$")
        assert "." in result

    def test_meal_price_batch(self, forge: DataForge) -> None:
        result = forge.food.meal_price(count=5)
        assert all(x.startswith("$") for x in result)

    def test_schema_integration(self, forge: DataForge) -> None:
        schema = forge.schema(["dish", "cuisine", "ingredient", "beverage"])
        rows = schema.generate(5)
        assert len(rows) == 5


# ==================================================================
# LegalProvider
# ==================================================================


class TestLegalProvider:
    def test_case_number_scalar(self, forge: DataForge) -> None:
        result = forge.legal.case_number()
        assert isinstance(result, str)
        assert re.match(r"^[A-Z]+-\d{4}-\d{6}$", result)

    def test_case_number_batch(self, forge: DataForge) -> None:
        result = forge.legal.case_number(count=5)
        assert len(result) == 5
        assert all(re.match(r"^[A-Z]+-\d{4}-\d{6}$", x) for x in result)

    def test_court_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.legal.court(), str)

    def test_practice_area_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.legal.practice_area(), str)

    def test_legal_term_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.legal.legal_term(), str)

    def test_document_type_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.legal.document_type(), str)

    def test_law_firm_scalar(self, forge: DataForge) -> None:
        result = forge.legal.law_firm()
        assert isinstance(result, str)
        assert " " in result

    def test_law_firm_batch(self, forge: DataForge) -> None:
        result = forge.legal.law_firm(count=5)
        assert len(result) == 5

    def test_judge_scalar(self, forge: DataForge) -> None:
        result = forge.legal.judge()
        assert isinstance(result, str)
        assert result.startswith("Hon. ")

    def test_judge_batch(self, forge: DataForge) -> None:
        result = forge.legal.judge(count=5)
        assert all(x.startswith("Hon. ") for x in result)

    def test_verdict_scalar(self, forge: DataForge) -> None:
        result = forge.legal.verdict()
        assert result in ("Guilty", "Not Guilty", "Dismissed", "Settled", "Mistrial")

    def test_schema_integration(self, forge: DataForge) -> None:
        schema = forge.schema(["case_number", "court", "practice_area", "verdict"])
        rows = schema.generate(5)
        assert len(rows) == 5


# ==================================================================
# RealEstateProvider
# ==================================================================


class TestRealEstateProvider:
    def test_property_type_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.real_estate.property_type(), str)

    def test_property_type_batch(self, forge: DataForge) -> None:
        result = forge.real_estate.property_type(count=10)
        assert isinstance(result, list) and len(result) == 10

    def test_listing_price_scalar(self, forge: DataForge) -> None:
        result = forge.real_estate.listing_price()
        assert result.startswith("$")
        assert "," in result

    def test_listing_price_batch(self, forge: DataForge) -> None:
        result = forge.real_estate.listing_price(count=5)
        assert all(x.startswith("$") for x in result)

    def test_square_footage_scalar(self, forge: DataForge) -> None:
        result = forge.real_estate.square_footage()
        assert result.endswith(" sqft")

    def test_bedrooms_scalar(self, forge: DataForge) -> None:
        result = forge.real_estate.bedrooms()
        assert result.isdigit()
        assert 1 <= int(result) <= 7

    def test_bathrooms_scalar(self, forge: DataForge) -> None:
        result = forge.real_estate.bathrooms()
        assert float(result)  # should be parseable as float

    def test_neighborhood_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.real_estate.neighborhood(), str)

    def test_building_material_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.real_estate.building_material(), str)

    def test_listing_status_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.real_estate.listing_status(), str)

    def test_amenity_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.real_estate.amenity(), str)

    def test_heating_type_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.real_estate.heating_type(), str)

    def test_year_built_scalar(self, forge: DataForge) -> None:
        result = forge.real_estate.year_built()
        assert result.isdigit()
        assert 1900 <= int(result) <= 2026

    def test_schema_integration(self, forge: DataForge) -> None:
        schema = forge.schema(["property_type", "listing_price", "neighborhood"])
        rows = schema.generate(5)
        assert len(rows) == 5


# ==================================================================
# WeatherProvider
# ==================================================================


class TestWeatherProvider:
    def test_condition_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.weather.condition(), str)

    def test_condition_batch(self, forge: DataForge) -> None:
        result = forge.weather.condition(count=10)
        assert isinstance(result, list) and len(result) == 10

    def test_temperature_scalar(self, forge: DataForge) -> None:
        result = forge.weather.temperature()
        assert isinstance(result, str)
        assert result.endswith("\u00b0F")

    def test_temperature_batch(self, forge: DataForge) -> None:
        result = forge.weather.temperature(count=5)
        assert all(x.endswith("\u00b0F") for x in result)

    def test_humidity_scalar(self, forge: DataForge) -> None:
        result = forge.weather.humidity()
        assert result.endswith("%")

    def test_wind_speed_scalar(self, forge: DataForge) -> None:
        result = forge.weather.wind_speed()
        assert result.endswith(" mph")

    def test_wind_direction_scalar(self, forge: DataForge) -> None:
        result = forge.weather.wind_direction()
        assert isinstance(result, str)
        assert len(result) <= 3

    def test_uv_index_scalar(self, forge: DataForge) -> None:
        result = forge.weather.uv_index()
        assert result.isdigit()
        assert 0 <= int(result) <= 11

    def test_air_quality_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.weather.air_quality(), str)

    def test_alert_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.weather.alert(), str)

    def test_cloud_type_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.weather.cloud_type(), str)

    def test_season_scalar(self, forge: DataForge) -> None:
        result = forge.weather.season()
        assert result in ("Spring", "Summer", "Autumn", "Winter")

    def test_pressure_scalar(self, forge: DataForge) -> None:
        result = forge.weather.pressure()
        assert result.endswith(" inHg")

    def test_visibility_scalar(self, forge: DataForge) -> None:
        result = forge.weather.visibility()
        assert result.endswith(" mi")

    def test_schema_integration(self, forge: DataForge) -> None:
        schema = forge.schema(["weather_condition", "temperature", "humidity"])
        rows = schema.generate(5)
        assert len(rows) == 5


# ==================================================================
# HardwareProvider
# ==================================================================


class TestHardwareProvider:
    def test_cpu_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.hardware.cpu(), str)

    def test_cpu_batch(self, forge: DataForge) -> None:
        result = forge.hardware.cpu(count=10)
        assert isinstance(result, list) and len(result) == 10

    def test_gpu_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.hardware.gpu(), str)

    def test_gpu_batch(self, forge: DataForge) -> None:
        result = forge.hardware.gpu(count=5)
        assert len(result) == 5

    def test_ram_size_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.hardware.ram_size(), str)

    def test_ram_type_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.hardware.ram_type(), str)

    def test_storage_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.hardware.storage(), str)

    def test_form_factor_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.hardware.form_factor(), str)

    def test_peripheral_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.hardware.peripheral(), str)

    def test_manufacturer_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.hardware.manufacturer(), str)

    def test_port_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.hardware.port(), str)

    def test_port_batch(self, forge: DataForge) -> None:
        result = forge.hardware.port(count=5)
        assert len(result) == 5

    def test_schema_integration(self, forge: DataForge) -> None:
        schema = forge.schema(["cpu", "gpu", "ram_size", "storage"])
        rows = schema.generate(5)
        assert len(rows) == 5


# ==================================================================
# LogisticsProvider
# ==================================================================


class TestLogisticsProvider:
    def test_carrier_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.logistics.carrier(), str)

    def test_carrier_batch(self, forge: DataForge) -> None:
        result = forge.logistics.carrier(count=10)
        assert isinstance(result, list) and len(result) == 10

    def test_shipping_method_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.logistics.shipping_method(), str)

    def test_container_type_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.logistics.container_type(), str)

    def test_tracking_status_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.logistics.tracking_status(), str)

    def test_incoterm_scalar(self, forge: DataForge) -> None:
        result = forge.logistics.incoterm()
        assert isinstance(result, str)
        assert len(result) == 3

    def test_incoterm_batch(self, forge: DataForge) -> None:
        result = forge.logistics.incoterm(count=5)
        assert all(len(x) == 3 for x in result)

    def test_warehouse_scalar(self, forge: DataForge) -> None:
        result = forge.logistics.warehouse()
        assert isinstance(result, str)
        assert " " in result

    def test_warehouse_batch(self, forge: DataForge) -> None:
        result = forge.logistics.warehouse(count=5)
        assert len(result) == 5

    def test_package_type_scalar(self, forge: DataForge) -> None:
        assert isinstance(forge.logistics.package_type(), str)

    def test_hs_code_scalar(self, forge: DataForge) -> None:
        result = forge.logistics.hs_code()
        assert isinstance(result, str)
        assert "." in result

    def test_hs_code_batch(self, forge: DataForge) -> None:
        result = forge.logistics.hs_code(count=5)
        assert all("." in x for x in result)

    def test_shipping_weight_scalar(self, forge: DataForge) -> None:
        result = forge.logistics.shipping_weight()
        assert result.endswith(" lbs")

    def test_freight_class_scalar(self, forge: DataForge) -> None:
        result = forge.logistics.freight_class()
        assert isinstance(result, str)

    def test_schema_integration(self, forge: DataForge) -> None:
        schema = forge.schema(["carrier", "shipping_method", "tracking_status"])
        rows = schema.generate(5)
        assert len(rows) == 5


# ==================================================================
# Registry & Cross-provider tests
# ==================================================================


class TestRegistryDiscovery:
    """Verify all 9 new providers are discovered by the registry."""

    def test_all_providers_in_registry(self) -> None:
        from dataforge.registry import get_provider_info

        info = get_provider_info()
        expected = [
            "social_media",
            "music",
            "sports",
            "food",
            "legal",
            "real_estate",
            "weather",
            "hardware",
            "logistics",
        ]
        for name in expected:
            assert name in info, f"Provider '{name}' not found in registry"

    def test_field_map_entries(self) -> None:
        from dataforge.registry import get_field_map

        fm = get_field_map()
        # Spot-check representative fields from each provider
        assert "social_platform" in fm
        assert "genre" in fm
        assert "sport" in fm
        assert "dish" in fm
        assert "case_number" in fm
        assert "property_type" in fm
        assert "weather_condition" in fm
        assert "cpu" in fm
        assert "carrier" in fm

    def test_list_providers_includes_new(self) -> None:
        providers = DataForge.list_providers()
        for name in [
            "social_media",
            "music",
            "sports",
            "food",
            "legal",
            "real_estate",
            "weather",
            "hardware",
            "logistics",
        ]:
            assert name in providers

    def test_list_fields_includes_new(self) -> None:
        fields = DataForge.list_fields()
        for field in [
            "social_platform",
            "genre",
            "sport",
            "dish",
            "case_number",
            "property_type",
            "weather_condition",
            "cpu",
            "carrier",
        ]:
            assert field in fields


class TestBatchConsistency:
    """Ensure batch generation matches scalar generation semantics."""

    @pytest.mark.parametrize(
        "provider_name,method_name",
        [
            ("social_media", "platform"),
            ("music", "genre"),
            ("sports", "sport"),
            ("food", "dish"),
            ("legal", "court"),
            ("real_estate", "property_type"),
            ("weather", "condition"),
            ("hardware", "cpu"),
            ("logistics", "carrier"),
        ],
    )
    def test_batch_returns_correct_count(
        self, forge: DataForge, provider_name: str, method_name: str
    ) -> None:
        provider = getattr(forge, provider_name)
        method = getattr(provider, method_name)
        result = method(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(isinstance(x, str) for x in result)

    @pytest.mark.parametrize(
        "provider_name,method_name",
        [
            ("social_media", "username"),
            ("music", "album"),
            ("sports", "athlete"),
            ("food", "restaurant"),
            ("legal", "case_number"),
            ("real_estate", "listing_price"),
            ("weather", "temperature"),
            ("logistics", "warehouse"),
        ],
    )
    def test_composed_batch_returns_correct_count(
        self, forge: DataForge, provider_name: str, method_name: str
    ) -> None:
        """Test methods that compose multiple random values."""
        provider = getattr(forge, provider_name)
        method = getattr(provider, method_name)
        result = method(count=50)
        assert isinstance(result, list)
        assert len(result) == 50


class TestReproducibility:
    """Verify seeded providers produce deterministic output."""

    @pytest.mark.parametrize(
        "provider_name,method_name",
        [
            ("social_media", "platform"),
            ("music", "genre"),
            ("sports", "team"),
            ("food", "dish"),
            ("legal", "verdict"),
            ("real_estate", "neighborhood"),
            ("weather", "condition"),
            ("hardware", "gpu"),
            ("logistics", "carrier"),
        ],
    )
    def test_seeded_determinism(self, provider_name: str, method_name: str) -> None:
        f1 = DataForge(seed=99)
        f2 = DataForge(seed=99)
        p1 = getattr(f1, provider_name)
        p2 = getattr(f2, provider_name)
        m1 = getattr(p1, method_name)
        m2 = getattr(p2, method_name)
        assert m1(count=10) == m2(count=10)
