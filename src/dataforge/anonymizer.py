"""Data anonymization — deterministic PII replacement with referential integrity."""

from __future__ import annotations

import hashlib as _hashlib
import hmac as _hmac
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from dataforge.core import DataForge


class Anonymizer:
    """Deterministic PII anonymizer with consistent value mappings."""

    __slots__ = ("_forge", "_secret", "_cache", "_field_methods")

    def __init__(self, forge: DataForge, secret: str = "dataforge-anonymizer") -> None:
        self._forge = forge
        self._secret = secret.encode("utf-8")
        self._cache: dict[tuple[str, str], Any] = {}
        self._field_methods: dict[str, Any] = {}

    def _derive_seed(self, field: str, value: str) -> int:
        """Derive a deterministic integer seed from field name and value."""
        msg = f"{field}:{value}".encode("utf-8")
        digest = _hmac.new(self._secret, msg, _hashlib.sha256).digest()
        return int.from_bytes(digest[:8], "big")

    def _get_method(self, field: str) -> Any:
        """Get a resolved provider method for a field (cached)."""
        method = self._field_methods.get(field)
        if method is not None:
            return method
        try:
            provider_attr, method_name = self._forge._resolve_field(field)
            provider = getattr(self._forge, provider_attr)
            method = getattr(provider, method_name)
            self._field_methods[field] = method
            return method
        except (ValueError, AttributeError):
            return None

    def _generate_fake(self, field: str, original_value: Any) -> Any:
        """Generate a deterministic fake value for a given field and original."""
        str_val = str(original_value) if original_value is not None else ""

        cache_key = (field, str_val)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        seed = self._derive_seed(field, str_val)

        method = self._get_method(field)
        if method is not None:
            import random as _random_mod

            temp_rng = _random_mod.Random(seed)
            engine = self._forge._engine
            orig_rng = engine._rng
            engine._rng = temp_rng
            try:
                fake_val = method()
            finally:
                engine._rng = orig_rng
        else:
            fake_val = (
                _hmac.new(
                    self._secret, str_val.encode("utf-8"), _hashlib.sha256
                ).hexdigest()[: len(str_val)]
                if str_val
                else ""
            )

        if field in ("email", "internet.email") and isinstance(original_value, str):
            fake_val = self._format_preserve_email(fake_val, original_value)

        if field in ("phone_number", "phone.phone_number") and isinstance(
            original_value, str
        ):
            fake_val = self._format_preserve_phone(fake_val, original_value)

        self._cache[cache_key] = fake_val
        return fake_val

    @staticmethod
    def _format_preserve_email(fake: Any, original: str) -> str:
        """Ensure fake email preserves the @domain.tld structure."""
        fake_str = str(fake)
        if "@" in fake_str:
            return fake_str
        if "@" in original:
            _, domain = original.rsplit("@", 1)
            return f"{fake_str}@{domain}"
        return fake_str

    @staticmethod
    def _format_preserve_phone(fake: Any, original: str) -> str:
        """Try to preserve phone number format (length and separators)."""
        fake_str = str(fake)
        if len(fake_str) == len(original):
            return fake_str
        result = []
        fake_digits = [c for c in fake_str if c.isdigit()]
        d_idx = 0
        for c in original:
            if c.isdigit():
                if d_idx < len(fake_digits):
                    result.append(fake_digits[d_idx])
                    d_idx += 1
                else:
                    result.append(c)
            else:
                result.append(c)
        return "".join(result)

    def anonymize(
        self,
        rows: list[dict[str, Any]],
        fields: dict[str, str],
    ) -> list[dict[str, Any]]:
        """Anonymize a list of row dicts."""
        result: list[dict[str, Any]] = []
        for row in rows:
            new_row = dict(row)
            for col_name, forge_field in fields.items():
                if col_name in new_row:
                    original = new_row[col_name]
                    if original is not None:
                        new_row[col_name] = self._generate_fake(forge_field, original)
            result.append(new_row)
        return result

    def anonymize_csv(
        self,
        input_path: str,
        output_path: str,
        fields: dict[str, str],
        delimiter: str = ",",
        encoding: str = "utf-8",
        batch_size: int = 1000,
    ) -> int:
        """Anonymize a CSV file in streaming fashion."""
        import csv

        total = 0
        with (
            open(input_path, "r", encoding=encoding, newline="") as fin,
            open(output_path, "w", encoding=encoding, newline="") as fout,
        ):
            reader = csv.DictReader(fin, delimiter=delimiter)
            fieldnames = reader.fieldnames or []
            writer = csv.DictWriter(fout, fieldnames=fieldnames, delimiter=delimiter)
            writer.writeheader()

            batch: list[dict[str, Any]] = []
            for row in reader:
                batch.append(dict(row))
                if len(batch) >= batch_size:
                    anonymized = self.anonymize(batch, fields)
                    writer.writerows(anonymized)
                    total += len(batch)
                    batch = []

            if batch:
                anonymized = self.anonymize(batch, fields)
                writer.writerows(anonymized)
                total += len(batch)

        return total

    def clear_cache(self) -> None:
        """Clear the value mapping cache."""
        self._cache.clear()

    def __repr__(self) -> str:
        return f"Anonymizer(cached_mappings={len(self._cache)})"
