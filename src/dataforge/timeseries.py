"""Time-series generation — synthetic time-series data with trends and patterns."""

from __future__ import annotations

import datetime as _datetime
import math as _math
from collections.abc import Iterator
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from dataforge.core import DataForge

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
    return int(interval)


def _parse_datetime(dt_str: str) -> float:
    """Parse an ISO datetime string to a POSIX timestamp."""
    if "T" in dt_str:
        dt = _datetime.datetime.fromisoformat(dt_str)
    else:
        dt = _datetime.datetime.fromisoformat(dt_str + "T00:00:00")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_datetime.timezone.utc)
    return dt.timestamp()


_UTC = _datetime.timezone.utc
_fromtimestamp = _datetime.datetime.fromtimestamp


def _timestamp_to_iso(ts: float) -> str:
    """Convert a POSIX timestamp to ISO format string."""
    dt = _fromtimestamp(ts, tz=_UTC)
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")


class TimeSeriesSchema:
    """Pre-configured time-series generator with trend, seasonality, and noise."""

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
        """Generate the full time-series as a list of row dicts."""
        n = self.num_points
        if n == 0:
            return []

        rng = self._rng
        timestamps = self._timestamps

        _to_iso = _timestamp_to_iso
        ts_strings = [_to_iso(ts) for ts in timestamps]

        field_columns: list[tuple[str, list[Any]]] = []
        for field_name, config in self._fields.items():
            values = self._generate_field(config, n, rng)
            field_columns.append((field_name, values))

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

        has_min = min_val is not None
        has_max = max_val is not None
        if has_min:
            min_val_f = float(min_val)
        if has_max:
            max_val_f = float(max_val)

        season_cfg = config.get("seasonality")
        has_season = season_cfg is not None
        if has_season:
            period = float(season_cfg.get("period", 24))
            amplitude = float(season_cfg.get("amplitude", 1.0))
            phase = float(season_cfg.get("phase", 0.0))
            has_season = period > 0
        else:
            period = amplitude = phase = 0.0

        has_noise = noise_std > 0.0
        has_anomaly = anomaly_rate > 0.0
        has_spike = spike_rate > 0.0
        has_missing = missing_rate > 0.0

        regimes = config.get("regime_changes")
        has_regimes = bool(regimes)
        if has_regimes:
            regime_map: dict[int, dict[str, float]] = {}
            for rc in regimes:
                step = int(rc["at_step"])
                regime_map[step] = rc
        else:
            regime_map = None  # type: ignore[assignment]

        values: list[Any] = [None] * n
        current_base = base
        current_trend = trend
        _gauss = rng.gauss
        _random = rng.random
        _sin = _math.sin
        _pi2 = 2.0 * _math.pi

        anomaly_noise = noise_std * anomaly_scale if has_noise else anomaly_scale

        for i in range(n):
            if has_regimes and i in regime_map:
                rc = regime_map[i]
                if "base" in rc:
                    current_base = float(rc["base"])
                if "trend" in rc:
                    current_trend = float(rc["trend"])

            if has_missing and _random() < missing_rate:
                continue

            val = current_base + current_trend * i

            if has_season:
                val += amplitude * _sin(_pi2 * (i + phase) / period)

            if has_noise:
                val += _gauss(0.0, noise_std)

            if has_anomaly and _random() < anomaly_rate:
                val += _gauss(0.0, anomaly_noise)

            if has_spike and _random() < spike_rate:
                direction = 1.0 if _random() > 0.5 else -1.0
                val += direction * abs(val) * spike_scale if val != 0 else spike_scale

            if has_min and val < min_val_f:
                val = min_val_f
            if has_max and val > max_val_f:
                val = max_val_f

            values[i] = round(val, 4)

        return values

    def stream(self, batch_size: int = 1000) -> Iterator[dict[str, Any]]:
        """Yield rows lazily in batches."""
        rows = self.generate()
        yield from rows

    def to_csv(
        self,
        path: str | None = None,
        delimiter: str = ",",
    ) -> str:
        """Export time-series as CSV."""
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
        """Export time-series as JSON array."""
        import json

        rows = self.generate()
        content = json.dumps(rows, indent=indent, ensure_ascii=False)
        if path is not None:
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
        return content

    def to_dataframe(self) -> Any:
        """Export as pandas DataFrame."""
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
