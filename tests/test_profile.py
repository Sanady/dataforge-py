"""Tests for the ProfileProvider."""

from dataforge import DataForge


class TestProfileScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_profile_returns_dict(self) -> None:
        result = self.forge.profile.profile()
        assert isinstance(result, dict)

    def test_profile_has_expected_keys(self) -> None:
        result = self.forge.profile.profile()
        expected_keys = {
            "first_name",
            "last_name",
            "email",
            "phone",
            "city",
            "state",
            "zip_code",
            "job_title",
        }
        assert set(result.keys()) == expected_keys

    def test_profile_values_are_strings(self) -> None:
        result = self.forge.profile.profile()
        for key, value in result.items():
            assert isinstance(value, str), f"{key} is not str: {type(value)}"

    def test_profile_email_coherence(self) -> None:
        result = self.forge.profile.profile()
        email = result["email"]
        first = result["first_name"].lower()
        last = result["last_name"].lower()
        local_part = email.split("@")[0]
        assert first in local_part or last in local_part

    def test_profile_email_has_at_sign(self) -> None:
        for _ in range(20):
            result = self.forge.profile.profile()
            assert "@" in result["email"]

    def test_deterministic_with_seed(self) -> None:
        f1 = DataForge(seed=99)
        f2 = DataForge(seed=99)
        p1 = f1.profile.profile()
        p2 = f2.profile.profile()
        assert p1 == p2


class TestProfileBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_profile_batch_returns_list(self) -> None:
        result = self.forge.profile.profile(count=10)
        assert isinstance(result, list)
        assert len(result) == 10

    def test_profile_batch_all_dicts(self) -> None:
        result = self.forge.profile.profile(count=50)
        assert all(isinstance(p, dict) for p in result)

    def test_profile_batch_all_have_keys(self) -> None:
        expected_keys = {
            "first_name",
            "last_name",
            "email",
            "phone",
            "city",
            "state",
            "zip_code",
            "job_title",
        }
        result = self.forge.profile.profile(count=20)
        for p in result:
            assert set(p.keys()) == expected_keys


class TestProfileFieldMap:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_profile_first_name(self) -> None:
        result = self.forge.profile.profile_first_name()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_profile_last_name(self) -> None:
        result = self.forge.profile.profile_last_name()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_profile_email(self) -> None:
        result = self.forge.profile.profile_email()
        assert isinstance(result, str)
        assert "@" in result

    def test_profile_phone(self) -> None:
        result = self.forge.profile.profile_phone()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_profile_city(self) -> None:
        result = self.forge.profile.profile_city()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_profile_state(self) -> None:
        result = self.forge.profile.profile_state()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_profile_zip_code(self) -> None:
        result = self.forge.profile.profile_zip_code()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_profile_job_title(self) -> None:
        result = self.forge.profile.profile_job_title()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_field_map_batch(self) -> None:
        result = self.forge.profile.profile_first_name(count=10)
        assert isinstance(result, list)
        assert len(result) == 10

    def test_schema_compatibility(self) -> None:
        schema = self.forge.schema(
            {
                "first_name": "profile_first_name",
                "last_name": "profile_last_name",
                "email": "profile_email",
            }
        )
        rows = schema.generate(10)
        assert len(rows) == 10
        for row in rows:
            assert "first_name" in row
            assert "last_name" in row
            assert "email" in row
            assert "@" in row["email"]
