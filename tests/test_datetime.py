"""Tests for the DateTimeProvider."""

import datetime
import re

from dataforge import DataForge


class TestDateTimeScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_date_format(self) -> None:
        result = self.forge.dt.date()
        assert isinstance(result, str)
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", result)

    def test_time_format(self) -> None:
        result = self.forge.dt.time()
        assert isinstance(result, str)
        assert re.match(r"^\d{2}:\d{2}:\d{2}$", result)

    def test_datetime_format(self) -> None:
        result = self.forge.dt.datetime()
        assert isinstance(result, str)
        assert re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", result)

    def test_date_of_birth_age_range(self) -> None:
        today = datetime.date.today()
        for _ in range(100):
            dob_str = self.forge.dt.date_of_birth(min_age=18, max_age=65)
            assert isinstance(dob_str, str)
            dob = datetime.date.fromisoformat(dob_str)
            age = (today - dob).days / 365.25
            # Allow small tolerance for leap years
            assert 17.5 <= age <= 65.5

    def test_custom_format(self) -> None:
        result = self.forge.dt.date(fmt="%d/%m/%Y")
        assert isinstance(result, str)
        assert re.match(r"^\d{2}/\d{2}/\d{4}$", result)

    def test_date_object_returns_date(self) -> None:
        result = self.forge.dt.date_object()
        assert isinstance(result, datetime.date)

    def test_datetime_object_returns_datetime(self) -> None:
        result = self.forge.dt.datetime_object()
        assert isinstance(result, datetime.datetime)

    def test_timezone_returns_str(self) -> None:
        result = self.forge.dt.timezone()
        assert isinstance(result, str)
        assert "/" in result or result == "UTC"

    def test_unix_timestamp_returns_int(self) -> None:
        result = self.forge.dt.unix_timestamp()
        assert isinstance(result, int)
        assert result > 0

    def test_date_with_start_end(self) -> None:
        import datetime as dt

        start = dt.date(2020, 1, 1)
        end = dt.date(2020, 12, 31)
        for _ in range(100):
            result = self.forge.dt.date(start=start, end=end)
            parsed = dt.date.fromisoformat(result)
            assert start <= parsed <= end


class TestDateTimeBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_date_batch(self) -> None:
        result = self.forge.dt.date(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(re.match(r"^\d{4}-\d{2}-\d{2}$", d) for d in result)

    def test_time_batch(self) -> None:
        result = self.forge.dt.time(count=100)
        assert isinstance(result, list)
        assert len(result) == 100

    def test_datetime_batch(self) -> None:
        result = self.forge.dt.datetime(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_dob_batch(self) -> None:
        result = self.forge.dt.date_of_birth(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_date_object_batch(self) -> None:
        result = self.forge.dt.date_object(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(isinstance(d, datetime.date) for d in result)

    def test_datetime_object_batch(self) -> None:
        result = self.forge.dt.datetime_object(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(isinstance(d, datetime.datetime) for d in result)

    def test_timezone_batch(self) -> None:
        result = self.forge.dt.timezone(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_unix_timestamp_batch(self) -> None:
        result = self.forge.dt.unix_timestamp(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(isinstance(t, int) for t in result)


class TestDateTimeLocaleIndependent:
    def test_dt_works_with_any_locale(self) -> None:
        for locale in ("en_US", "de_DE", "fr_FR", "es_ES", "ja_JP"):
            forge = DataForge(locale=locale, seed=42)
            date = forge.dt.date()
            assert re.match(r"^\d{4}-\d{2}-\d{2}$", date)
