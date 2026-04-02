"""Time-series generation — synthetic time-series data with trends and patterns.

Generates realistic time-series data with configurable trend, seasonality,
noise, anomalies, regime changes, missing data gaps, and spiky patterns.

Usage::

    from dataforge import DataForge, TimeSeriesSchema

    forge = DataForge(seed=42)
    ts = TimeSeriesSchema(
        forge,
        start="2024-01-01",
        end="2024-12-31",
        interval="1h",
        fields={
            "temperature": {
                "trend": 0.01,
                "seasonality": {"period": 24, "amplitude": 5.0},
                "noise": 0.5,
                "base": 20.0,
            },
            "humidity": {
                "trend": -0.005,
                "seasonality": {"period": 24, "amplitude": 10.0},
                "noise": 2.0,
                "base": 60.0,
            },
        },
    )
    rows = ts.generate()
"""

from __future__ import annotations

import datetime as _datetime
import math as _math
from collections.abc import Iterator
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from dataforge.core import DataForge

# ------------------------------------------------------------------
# Interval parsing
# ------------------------------------------------------------------

_INTERVAL_UNITS: dict[str, int] = {
    "s": 1,
    "m": 60,
    "min": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
}


def _parse_interval(interval: str) -> int:
    """Parse an interval string like '1h', '30m', '1d' into seconds."""
    interval = interval.strip().lower()
    for suffix, multiplier in sorted(_INTERVAL_UNITS.items(), key=lambda x: -len(x[0])):
        if interval.endswith(suffix):
            num_str = interval[: -len(suffix)].strip()
            num = int(num_str) if num_str else 1
            return num * multiplier
    # Try pure numeric (assume seconds)
    return int(interval)


def _parse_datetime(dt_str: str) -> float:
    """Parse an ISO datetime string to a POSIX timestamp.

    Naive datetimes (without timezone info) are treated as UTC.
    """
    if "T" in dt_str:
        dt = _datetime.datetime.fromisoformat(dt_str)
    else:
        dt = _datetime.datetime.fromisoformat(dt_str + "T00:00:00")
    # Treat naive datetimes as UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_datetime.timezone.utc)
    return dt.timestamp()


_UTC = _datetime.timezone.utc
_fromtimestamp = _datetime.datetime.fromtimestamp


def _timestamp_to_iso(ts: float) -> str:
    """Convert a POSIX timestamp to ISO format string."""
    dt = _fromtimestamp(ts, tz=_UTC)
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")


# ------------------------------------------------------------------
# TimeSeriesSchema
# ------------------------------------------------------------------


class TimeSeriesSchema:
    """Pre-configured time-series generator with trend, seasonality, and noise.

    Parameters
    ----------
    forge : DataForge
        The parent generator instance.
    start : str
        Start datetime (ISO format).
    end : str
        End datetime (ISO format).
    interval : str
        Time step between points (e.g. ``"1h"``, ``"30m"``, ``"1d"``).
    fields : dict[str, dict]
        Field specifications. Each field config can include:

        - ``base`` — base value (default: 0.0)
        - ``trend`` — linear trend per step (default: 0.0)
        - ``seasonality`` — dict with ``period`` (in steps) and
          ``amplitude`` (default: no seasonality)
        - ``noise`` — Gaussian noise std dev (default: 0.0)
        - ``anomaly_rate`` — probability of anomaly per point (default: 0.0)
        - ``anomaly_scale`` — anomaly multiplier (default: 3.0)
        - ``spike_rate`` — probability of spike per point (default: 0.0)
        - ``spike_scale`` — spike multiplier (default: 5.0)
        - ``min_val`` / ``max_val`` — clamp range
        - ``regime_changes`` — list of ``{"at_step": N, "base": X, "trend": Y}``
        - ``missing_rate`` — probability of missing data per point (default: 0.0)
    """

    __slots__ = (
        "_forge",
        "_start",
        "_end",
        "_interval_secs",
        "_fields",
        "_rng",
        "_timestamps",
    )

    def __init__(
        self,
        forge: DataForge,
        start: str,
        end: str,
        interval: str = "1h",
        fields: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self._forge = forge
        self._start = _parse_datetime(start)
        self._end = _parse_datetime(end)
        self._interval_secs = _parse_interval(interval)
        self._fields = fields or {}
        self._rng = forge._engine._rng

        # Pre-compute timestamps
        ts_list: list[float] = []
        t = self._start
        while t <= self._end:
            ts_list.append(t)
            t += self._interval_secs
        self._timestamps: tuple[float, ...] = tuple(ts_list)

    @property
    def num_points(self) -> int:
        """Number of time-series data points."""
        return len(self._timestamps)

    def generate(self) -> list[dict[str, Any]]:
        """Generate the full time-series as a list of row dicts.

        Returns
        -------
        list[dict[str, Any]]
            Each dict has a ``"timestamp"`` key plus one key per field.
        """
        n = self.num_points
        if n == 0:
            return []

        rng = self._rng
        timestamps = self._timestamps

        # Pre-convert all timestamps (avoids per-row function call overhead)
        _to_iso = _timestamp_to_iso
        ts_strings = [_to_iso(ts) for ts in timestamps]

        # Column-first generation: build all field columns, then assemble rows once
        field_columns: list[tuple[str, list[Any]]] = []
        for field_name, config in self._fields.items():
            values = self._generate_field(config, n, rng)
            field_columns.append((field_name, values))

        # Assemble rows in a single pass
        if field_columns:
            rows: list[dict[str, Any]] = [None] * n  # type: ignore[list-item]
            for i in range(n):
                row: dict[str, Any] = {"timestamp": ts_strings[i]}
                for field_name, values in field_columns:
                    row[field_name] = values[i]
                rows[i] = row
        else:
            rows = [{"timestamp": ts} for ts in ts_strings]

        return rows

    def _generate_field(
        self,
        config: dict[str, Any],
        n: int,
        rng: Any,
    ) -> list[Any]:
        """Generate a single field's time-series values."""
        base = float(config.get("base", 0.0))
        trend = float(config.get("trend", 0.0))
        noise_std = float(config.get("noise", 0.0))
        anomaly_rate = float(config.get("anomaly_rate", 0.0))
        anomaly_scale = float(config.get("anomaly_scale", 3.0))
        spike_rate = float(config.get("spike_rate", 0.0))
        spike_scale = float(config.get("spike_scale", 5.0))
        missing_rate = float(config.get("missing_rate", 0.0))
        min_val = config.get("min_val")
        max_val = config.get("max_val")

        # Pre-compute clamping as floats once
        has_min = min_val is not None
        has_max = max_val is not None
        if has_min:
            min_val_f = float(min_val)
        if has_max:
            max_val_f = float(max_val)

        # Seasonality
        season_cfg = config.get("seasonality")
        has_season = season_cfg is not None
        if has_season:
            period = float(season_cfg.get("period", 24))
            amplitude = float(season_cfg.get("amplitude", 1.0))
            phase = float(season_cfg.get("phase", 0.0))
            has_season = period > 0
        else:
            period = amplitude = phase = 0.0

        # Pre-compute feature flags for tight loop
        has_noise = noise_std > 0.0
        has_anomaly = anomaly_rate > 0.0
        has_spike = spike_rate > 0.0
        has_missing = missing_rate > 0.0

        # Regime changes: sorted by step — pre-check emptiness
        regimes = config.get("regime_changes")
        has_regimes = bool(regimes)
        if has_regimes:
            regime_map: dict[int, dict[str, float]] = {}
            for rc in regimes:
                step = int(rc["at_step"])
                regime_map[step] = rc
        else:
            regime_map = None  # type: ignore[assignment]

        # Generate values — tight loop with pre-computed flags
        values: list[Any] = [None] * n
        current_base = base
        current_trend = trend
        _gauss = rng.gauss
        _random = rng.random
        _sin = _math.sin
        _pi2 = 2.0 * _math.pi

        # Pre-compute anomaly noise scale
        anomaly_noise = noise_std * anomaly_scale if has_noise else anomaly_scale

        for i in range(n):
            # Check for regime change (skip dict lookup when no regimes)
            if has_regimes and i in regime_map:
                rc = regime_map[i]
                if "base" in rc:
                    current_base = float(rc["base"])
                if "trend" in rc:
                    current_trend = float(rc["trend"])

            # Missing data
            if has_missing and _random() < missing_rate:
                # values[i] already None
                continue

            # Base + trend
            val = current_base + current_trend * i

            # Seasonality (sinusoidal)
            if has_season:
                val += amplitude * _sin(_pi2 * (i + phase) / period)

            # Noise
            if has_noise:
                val += _gauss(0.0, noise_std)

            # Anomaly injection
            if has_anomaly and _random() < anomaly_rate:
                val += _gauss(0.0, anomaly_noise)

            # Spike injection
            if has_spike and _random() < spike_rate:
                direction = 1.0 if _random() > 0.5 else -1.0
                val += direction * abs(val) * spike_scale if val != 0 else spike_scale

            # Clamping
            if has_min and val < min_val_f:
                val = min_val_f
            if has_max and val > max_val_f:
                val = max_val_f

            values[i] = round(val, 4)

        return values

    def stream(self, batch_size: int = 1000) -> Iterator[dict[str, Any]]:
        """Yield rows lazily in batches.

        Parameters
        ----------
        batch_size : int
            Number of rows per batch.

        Yields
        ------
        dict[str, Any]
        """
        rows = self.generate()
        yield from rows

    def to_csv(
        self,
        path: str | None = None,
        delimiter: str = ",",
    ) -> str:
        """Export time-series as CSV.

        Parameters
        ----------
        path : str | None
            File path to write. Returns string if None.
        delimiter : str
            CSV delimiter.

        Returns
        -------
        str
            CSV content.
        """
        import csv
        import io

        rows = self.generate()
        if not rows:
            return ""

        columns = list(rows[0].keys())
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter=delimiter)
        writer.writerow(columns)

        _str = str
        for row in rows:
            writer.writerow(_str(row[c]) if row[c] is not None else "" for c in columns)

        content = buf.getvalue()
        if path is not None:
            from dataforge.schema import _open_file

            with _open_file(path, "w", newline="") as f:
                f.write(content)

        return content

    def to_json(
        self,
        path: str | None = None,
        indent: int = 2,
    ) -> str:
        """Export time-series as JSON array.

        Parameters
        ----------
        path : str | None
            File path to write.
        indent : int
            JSON indentation.

        Returns
        -------
        str
        """
        import json

        rows = self.generate()
        content = json.dumps(rows, indent=indent, ensure_ascii=False)
        if path is not None:
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
        return content

    def to_dataframe(self) -> Any:
        """Export as pandas DataFrame.

        Returns
        -------
        pandas.DataFrame
        """
        try:
            import pandas as pd
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "pandas is required for to_dataframe(). "
                "Install it with: pip install pandas"
            ) from exc

        rows = self.generate()
        df = pd.DataFrame(rows)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

    def __repr__(self) -> str:
        return (
            f"TimeSeriesSchema(points={self.num_points}, "
            f"fields={list(self._fields.keys())})"
        )
