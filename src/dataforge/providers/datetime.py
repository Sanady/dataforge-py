"""DateTime provider — generates fake dates, times, and datetimes.

This provider is locale-independent — it uses Python's datetime module
directly and generates values within configurable ranges.
"""

import datetime as _dt

from dataforge.providers.base import BaseProvider

# Epoch boundaries (as ordinals for fast random int generation)
_MIN_DATE = _dt.date(1970, 1, 1)
_MAX_DATE = _dt.date(2030, 12, 31)
_MIN_ORDINAL = _MIN_DATE.toordinal()
_MAX_ORDINAL = _MAX_DATE.toordinal()
_SECONDS_IN_DAY = 86400
_EPOCH_ORDINAL = _MIN_DATE.toordinal()  # cached for unix_timestamp()

# Pre-computed default timestamp range — avoids .toordinal() on every call
_MIN_TIMESTAMP = 0  # (_MIN_ORDINAL - _EPOCH_ORDINAL) * _SECONDS_IN_DAY
_MAX_TIMESTAMP = (_MAX_ORDINAL - _EPOCH_ORDINAL + 1) * _SECONDS_IN_DAY - 1

# IANA timezone names — common subset for fast random selection
_TIMEZONES: tuple[str, ...] = (
    "UTC",
    "US/Eastern",
    "US/Central",
    "US/Mountain",
    "US/Pacific",
    "US/Alaska",
    "US/Hawaii",
    "Canada/Eastern",
    "Canada/Central",
    "Canada/Pacific",
    "Europe/London",
    "Europe/Paris",
    "Europe/Berlin",
    "Europe/Madrid",
    "Europe/Rome",
    "Europe/Amsterdam",
    "Europe/Brussels",
    "Europe/Vienna",
    "Europe/Warsaw",
    "Europe/Moscow",
    "Europe/Istanbul",
    "Europe/Athens",
    "Europe/Helsinki",
    "Europe/Stockholm",
    "Europe/Oslo",
    "Europe/Zurich",
    "Asia/Tokyo",
    "Asia/Shanghai",
    "Asia/Hong_Kong",
    "Asia/Seoul",
)


class DateTimeProvider(BaseProvider):
    """Generates fake dates, times, datetimes, and dates of birth.

    This provider does **not** require locale data — it uses Python's
    ``datetime`` module directly.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "dt"
    _locale_modules = ()
    _field_map = {
        "date": "date",
        "time": "time",
        "datetime": "datetime",
        "date_of_birth": "date_of_birth",
        "dob": "date_of_birth",
        "timezone": "timezone",
        "unix_timestamp": "unix_timestamp",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "timezone": _TIMEZONES,
    }

    # Scalar helpers

    def _one_date(
        self,
        start: _dt.date = _MIN_DATE,
        end: _dt.date = _MAX_DATE,
    ) -> _dt.date:
        # Use pre-computed ordinals for default range to avoid
        # .toordinal() per call.
        if start is _MIN_DATE and end is _MAX_DATE:
            ordinal = self._engine.random_int(_MIN_ORDINAL, _MAX_ORDINAL)
        else:
            ordinal = self._engine.random_int(start.toordinal(), end.toordinal())
        return _dt.date.fromordinal(ordinal)

    @staticmethod
    def _date_to_iso(d: _dt.date) -> str:
        """Format date as ISO 8601 string without ``strftime`` overhead."""
        return f"{d.year:04d}-{d.month:02d}-{d.day:02d}"

    def _one_time(self) -> _dt.time:
        total_seconds = self._engine.random_int(0, _SECONDS_IN_DAY - 1)
        hour = total_seconds // 3600
        minute = (total_seconds % 3600) // 60
        second = total_seconds % 60
        return _dt.time(hour, minute, second)

    @staticmethod
    def _time_to_hms(t: _dt.time) -> str:
        """Format time as ``HH:MM:SS`` without ``strftime`` overhead."""
        return f"{t.hour:02d}:{t.minute:02d}:{t.second:02d}"

    def _one_time_str(self) -> str:
        """Generate a random time as ``HH:MM:SS`` string — fast path."""
        total = self._engine.random_int(0, _SECONDS_IN_DAY - 1)
        h, rem = divmod(total, 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    def _one_datetime(
        self,
        start: _dt.date = _MIN_DATE,
        end: _dt.date = _MAX_DATE,
    ) -> _dt.datetime:
        d = self._one_date(start, end)
        t = self._one_time()
        return _dt.datetime.combine(d, t)

    def _one_date_of_birth(self, min_age: int = 18, max_age: int = 80) -> _dt.date:
        today = _dt.date.today()
        start = today.replace(year=today.year - max_age)
        end = today.replace(year=today.year - min_age)
        return self._one_date(start, end)

    # Public API

    def date(
        self,
        count: int = 1,
        fmt: str = "%Y-%m-%d",
        start: _dt.date | None = None,
        end: _dt.date | None = None,
    ) -> str | list[str]:
        """Generate a random date string."""
        s = start or _MIN_DATE
        e = end or _MAX_DATE
        # Fast path: default ISO format avoids expensive strftime
        if fmt == "%Y-%m-%d":
            _iso = self._date_to_iso
            # Pre-compute ordinals once for the batch instead of
            # per-item .toordinal() calls inside _one_date().
            if s is _MIN_DATE and e is _MAX_DATE:
                s_ord, e_ord = _MIN_ORDINAL, _MAX_ORDINAL
            else:
                s_ord, e_ord = s.toordinal(), e.toordinal()
            _ri = self._engine.random_int
            _from_ord = _dt.date.fromordinal
            if count == 1:
                return _iso(_from_ord(_ri(s_ord, e_ord)))
            return [_iso(_from_ord(_ri(s_ord, e_ord))) for _ in range(count)]
        if count == 1:
            return self._one_date(s, e).strftime(fmt)
        return [self._one_date(s, e).strftime(fmt) for _ in range(count)]

    def time(self, count: int = 1, fmt: str = "%H:%M:%S") -> str | list[str]:
        """Generate a random time string."""
        # Fast path: default HH:MM:SS format — skip _dt.time object
        if fmt == "%H:%M:%S":
            if count == 1:
                return self._one_time_str()
            # Inlined batch: avoid method call overhead per item
            _ri = self._engine.random_int
            result: list[str] = []
            for _ in range(count):
                total = _ri(0, _SECONDS_IN_DAY - 1)
                h, rem = divmod(total, 3600)
                m, s = divmod(rem, 60)
                result.append(f"{h:02d}:{m:02d}:{s:02d}")
            return result
        if count == 1:
            return self._one_time().strftime(fmt)
        return [self._one_time().strftime(fmt) for _ in range(count)]

    def datetime(
        self,
        count: int = 1,
        fmt: str = "%Y-%m-%d %H:%M:%S",
        start: _dt.date | None = None,
        end: _dt.date | None = None,
    ) -> str | list[str]:
        """Generate a random datetime string."""
        s = start or _MIN_DATE
        e = end or _MAX_DATE
        # Fast path: default ISO-like format — avoid strftime + datetime objects
        if fmt == "%Y-%m-%d %H:%M:%S":
            if s is _MIN_DATE and e is _MAX_DATE:
                s_ord, e_ord = _MIN_ORDINAL, _MAX_ORDINAL
            else:
                s_ord, e_ord = s.toordinal(), e.toordinal()
            _ri = self._engine.random_int
            _from_ord = _dt.date.fromordinal
            _secs = _SECONDS_IN_DAY - 1
            if count == 1:
                d = _from_ord(_ri(s_ord, e_ord))
                total = _ri(0, _secs)
                h, rem = divmod(total, 3600)
                m, sec = divmod(rem, 60)
                return (
                    f"{d.year:04d}-{d.month:02d}-{d.day:02d} {h:02d}:{m:02d}:{sec:02d}"
                )
            result: list[str] = []
            for _ in range(count):
                d = _from_ord(_ri(s_ord, e_ord))
                total = _ri(0, _secs)
                h, rem = divmod(total, 3600)
                m, sec = divmod(rem, 60)
                result.append(
                    f"{d.year:04d}-{d.month:02d}-{d.day:02d} {h:02d}:{m:02d}:{sec:02d}"
                )
            return result
        if count == 1:
            return self._one_datetime(s, e).strftime(fmt)
        return [self._one_datetime(s, e).strftime(fmt) for _ in range(count)]

    def date_of_birth(
        self,
        count: int = 1,
        min_age: int = 18,
        max_age: int = 80,
        fmt: str = "%Y-%m-%d",
    ) -> str | list[str]:
        """Generate a random date of birth."""
        # Compute today() once for the entire batch
        today = _dt.date.today()
        start = today.replace(year=today.year - max_age)
        end = today.replace(year=today.year - min_age)

        if fmt == "%Y-%m-%d":
            _iso = self._date_to_iso
            s_ord, e_ord = start.toordinal(), end.toordinal()
            _ri = self._engine.random_int
            _from_ord = _dt.date.fromordinal
            if count == 1:
                return _iso(_from_ord(_ri(s_ord, e_ord)))
            return [_iso(_from_ord(_ri(s_ord, e_ord))) for _ in range(count)]
        if count == 1:
            return self._one_date(start, end).strftime(fmt)
        return [self._one_date(start, end).strftime(fmt) for _ in range(count)]

    def date_object(self, count: int = 1) -> _dt.date | list[_dt.date]:
        """Generate a random ``datetime.date`` object."""
        if count == 1:
            return self._one_date()
        return [self._one_date() for _ in range(count)]

    def datetime_object(self, count: int = 1) -> _dt.datetime | list[_dt.datetime]:
        """Generate a random ``datetime.datetime`` object."""
        if count == 1:
            return self._one_datetime()
        return [self._one_datetime() for _ in range(count)]

    def unix_timestamp(
        self,
        count: int = 1,
        start: _dt.date | None = None,
        end: _dt.date | None = None,
    ) -> int | list[int]:
        """Generate a random Unix timestamp (seconds since epoch)."""
        # Use pre-computed constants for default range to avoid
        # .toordinal() per call.
        if start is None and end is None:
            min_ts = _MIN_TIMESTAMP
            max_ts = _MAX_TIMESTAMP
        else:
            s = start or _MIN_DATE
            e = end or _MAX_DATE
            min_ts = (s.toordinal() - _EPOCH_ORDINAL) * _SECONDS_IN_DAY
            max_ts = (e.toordinal() - _EPOCH_ORDINAL + 1) * _SECONDS_IN_DAY - 1

        if count == 1:
            return self._engine.random_int(min_ts, max_ts)
        _ri = self._engine.random_int
        return [_ri(min_ts, max_ts) for _ in range(count)]
