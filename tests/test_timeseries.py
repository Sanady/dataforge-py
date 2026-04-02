"""Tests for time-series generation."""

from __future__ import annotations

import json

import pytest

from dataforge import DataForge
from dataforge.timeseries import (
    TimeSeriesSchema,
    _parse_interval,
    _parse_datetime,
    _timestamp_to_iso,
)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture
def forge() -> DataForge:
    return DataForge(locale="en_US", seed=42)


# ------------------------------------------------------------------
# Interval parsing
# ------------------------------------------------------------------


class TestIntervalParsing:
    def test_seconds(self) -> None:
        assert _parse_interval("30s") == 30

    def test_minutes(self) -> None:
        assert _parse_interval("5m") == 300

    def test_hours(self) -> None:
        assert _parse_interval("1h") == 3600

    def test_days(self) -> None:
        assert _parse_interval("1d") == 86400

    def test_weeks(self) -> None:
        assert _parse_interval("2w") == 604800 * 2

    def test_pure_numeric(self) -> None:
        assert _parse_interval("60") == 60

    def test_min_suffix(self) -> None:
        assert _parse_interval("15min") == 900


# ------------------------------------------------------------------
# Datetime parsing
# ------------------------------------------------------------------


class TestDatetimeParsing:
    def test_date_string(self) -> None:
        ts = _parse_datetime("2024-01-01")
        assert isinstance(ts, float)
        assert ts > 0

    def test_datetime_string(self) -> None:
        ts = _parse_datetime("2024-01-01T12:00:00")
        assert isinstance(ts, float)

    def test_iso_roundtrip(self) -> None:
        ts = _parse_datetime("2024-06-15T00:00:00")
        iso = _timestamp_to_iso(ts)
        assert "2024-06-15" in iso


# ------------------------------------------------------------------
# TimeSeriesSchema creation
# ------------------------------------------------------------------


class TestTimeSeriesSchema:
    def test_basic_creation(self, forge: DataForge) -> None:
        ts = TimeSeriesSchema(
            forge,
            start="2024-01-01",
            end="2024-01-02",
            interval="1h",
            fields={"value": {"base": 10.0}},
        )
        assert ts.num_points == 25  # 24 hours + start point

    def test_empty_range(self, forge: DataForge) -> None:
        ts = TimeSeriesSchema(
            forge,
            start="2024-01-02",
            end="2024-01-01",
            interval="1h",
            fields={"value": {"base": 10.0}},
        )
        assert ts.num_points == 0

    def test_repr(self, forge: DataForge) -> None:
        ts = TimeSeriesSchema(
            forge,
            start="2024-01-01",
            end="2024-01-01T23:00:00",
            interval="1h",
            fields={"temp": {}},
        )
        r = repr(ts)
        assert "TimeSeriesSchema" in r
        assert "temp" in r


# ------------------------------------------------------------------
# Data generation
# ------------------------------------------------------------------


class TestTimeSeriesGeneration:
    def test_generate_returns_rows(self, forge: DataForge) -> None:
        ts = TimeSeriesSchema(
            forge,
            start="2024-01-01",
            end="2024-01-01T03:00:00",
            interval="1h",
            fields={"value": {"base": 100.0}},
        )
        rows = ts.generate()
        assert len(rows) == 4
        assert all("timestamp" in r for r in rows)
        assert all("value" in r for r in rows)

    def test_trend(self, forge: DataForge) -> None:
        ts = TimeSeriesSchema(
            forge,
            start="2024-01-01",
            end="2024-01-01T09:00:00",
            interval="1h",
            fields={"value": {"base": 0.0, "trend": 10.0, "noise": 0.0}},
        )
        rows = ts.generate()
        # Values should increase monotonically with no noise
        values = [r["value"] for r in rows]
        for i in range(1, len(values)):
            assert values[i] > values[i - 1]

    def test_seasonality(self, forge: DataForge) -> None:
        ts = TimeSeriesSchema(
            forge,
            start="2024-01-01",
            end="2024-01-02",
            interval="1h",
            fields={
                "value": {
                    "base": 50.0,
                    "trend": 0.0,
                    "seasonality": {"period": 24, "amplitude": 20.0},
                    "noise": 0.0,
                }
            },
        )
        rows = ts.generate()
        values = [r["value"] for r in rows]
        # Values should oscillate — min should be < base and max > base
        assert min(values) < 50.0
        assert max(values) > 50.0

    def test_noise(self, forge: DataForge) -> None:
        ts = TimeSeriesSchema(
            forge,
            start="2024-01-01",
            end="2024-01-01T23:00:00",
            interval="1h",
            fields={"value": {"base": 0.0, "noise": 5.0}},
        )
        rows = ts.generate()
        values = [r["value"] for r in rows]
        # With noise, not all values should be zero
        assert any(v != 0.0 for v in values)

    def test_clamping(self, forge: DataForge) -> None:
        ts = TimeSeriesSchema(
            forge,
            start="2024-01-01",
            end="2024-01-01T23:00:00",
            interval="1h",
            fields={
                "value": {
                    "base": 50.0,
                    "noise": 100.0,
                    "min_val": 0.0,
                    "max_val": 100.0,
                }
            },
        )
        rows = ts.generate()
        for r in rows:
            assert 0.0 <= r["value"] <= 100.0

    def test_missing_data(self, forge: DataForge) -> None:
        ts = TimeSeriesSchema(
            forge,
            start="2024-01-01",
            end="2024-01-10",
            interval="1h",
            fields={"value": {"base": 10.0, "missing_rate": 0.3}},
        )
        rows = ts.generate()
        values = [r["value"] for r in rows]
        null_count = sum(1 for v in values if v is None)
        assert null_count > 0, "Expected some missing data points"

    def test_regime_change(self, forge: DataForge) -> None:
        ts = TimeSeriesSchema(
            forge,
            start="2024-01-01",
            end="2024-01-01T09:00:00",
            interval="1h",
            fields={
                "value": {
                    "base": 10.0,
                    "trend": 0.0,
                    "noise": 0.0,
                    "regime_changes": [{"at_step": 5, "base": 100.0}],
                }
            },
        )
        rows = ts.generate()
        values = [r["value"] for r in rows]
        # After step 5, base changes to 100
        assert values[4] == 10.0  # before regime change
        assert values[5] == 100.0  # after regime change

    def test_anomaly_injection(self, forge: DataForge) -> None:
        ts = TimeSeriesSchema(
            forge,
            start="2024-01-01",
            end="2024-01-30",
            interval="1h",
            fields={
                "value": {
                    "base": 50.0,
                    "noise": 1.0,
                    "anomaly_rate": 0.1,
                    "anomaly_scale": 10.0,
                }
            },
        )
        rows = ts.generate()
        values = [r["value"] for r in rows if r["value"] is not None]
        # Some values should be far from base
        deviations = [abs(v - 50.0) for v in values]
        assert max(deviations) > 5.0, "Expected some anomalous values"

    def test_generate_empty_range(self, forge: DataForge) -> None:
        ts = TimeSeriesSchema(
            forge,
            start="2024-01-02",
            end="2024-01-01",
            interval="1h",
            fields={"value": {"base": 10.0}},
        )
        rows = ts.generate()
        assert rows == []


# ------------------------------------------------------------------
# Export methods
# ------------------------------------------------------------------


class TestTimeSeriesExport:
    def test_to_csv_returns_string(self, forge: DataForge) -> None:
        ts = TimeSeriesSchema(
            forge,
            start="2024-01-01",
            end="2024-01-01T03:00:00",
            interval="1h",
            fields={"value": {"base": 10.0}},
        )
        csv_str = ts.to_csv()
        assert "timestamp" in csv_str
        assert "value" in csv_str
        lines = csv_str.strip().split("\n")
        assert len(lines) == 5  # header + 4 data rows

    def test_to_json_returns_valid_json(self, forge: DataForge) -> None:
        ts = TimeSeriesSchema(
            forge,
            start="2024-01-01",
            end="2024-01-01T03:00:00",
            interval="1h",
            fields={"value": {"base": 10.0}},
        )
        json_str = ts.to_json()
        data = json.loads(json_str)
        assert isinstance(data, list)
        assert len(data) == 4

    def test_stream_yields_all_rows(self, forge: DataForge) -> None:
        ts = TimeSeriesSchema(
            forge,
            start="2024-01-01",
            end="2024-01-01T03:00:00",
            interval="1h",
            fields={"value": {"base": 10.0}},
        )
        rows = list(ts.stream())
        assert len(rows) == 4


# ------------------------------------------------------------------
# Integration via DataForge.timeseries()
# ------------------------------------------------------------------


class TestDataForgeTimeSeriesMethod:
    def test_timeseries_method_exists(self, forge: DataForge) -> None:
        """DataForge should have a timeseries() method."""
        assert hasattr(forge, "timeseries")

    def test_timeseries_via_forge(self, forge: DataForge) -> None:
        ts = forge.timeseries(
            start="2024-01-01",
            end="2024-01-01T05:00:00",
            interval="1h",
            fields={"temp": {"base": 20.0, "noise": 1.0}},
        )
        assert isinstance(ts, TimeSeriesSchema)
        rows = ts.generate()
        assert len(rows) == 6
