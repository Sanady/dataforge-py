"""Data anonymization — deterministic PII replacement with referential integrity.

Replaces personally identifiable information (PII) with realistic fake
data using deterministic HMAC-SHA256 seeding for consistency: the same
real value always maps to the same fake value across tables and runs.

Usage::

    from dataforge import DataForge
    from dataforge.anonymizer import Anonymizer

    forge = DataForge(seed=42)
    anon = Anonymizer(forge, secret="my-secret-key")

    # Anonymize a list of dicts
    original = [
        {"name": "Alice Smith", "email": "alice@real.com", "ssn": "123-45-6789"},
        {"name": "Bob Jones", "email": "bob@real.com", "ssn": "987-65-4321"},
    ]
    anonymized = anon.anonymize(original, fields={
        "name": "full_name",
        "email": "email",
        "ssn": "ssn",
    })

    # Streaming CSV anonymization
    anon.anonymize_csv("input.csv", "output.csv", fields={...})
"""

from __future__ import annotations

import hashlib as _hashlib
import hmac as _hmac
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from dataforge.core import DataForge


class Anonymizer:
    """Deterministic PII anonymizer with consistent value mappings.

    Uses HMAC-SHA256 to derive deterministic seeds from (secret + original_value),
    ensuring the same input always produces the same fake output. This
    preserves referential integrity across tables automatically.

    Parameters
    ----------
    forge : DataForge
        The DataForge instance for generating fake values.
    secret : str
        Secret key for HMAC derivation. Different secrets produce
        different anonymizations. Keep this secret to prevent
        de-anonymization.
    """

    __slots__ = ("_forge", "_secret", "_cache", "_field_methods")

    def __init__(self, forge: DataForge, secret: str = "dataforge-anonymizer") -> None:
        self._forge = forge
        self._secret = secret.encode("utf-8")
        self._cache: dict[tuple[str, str], Any] = {}  # (field, original) → fake
        # Cache resolved field methods to avoid repeated _resolve_field calls
        self._field_methods: dict[str, Any] = {}

    def _derive_seed(self, field: str, value: str) -> int:
        """Derive a deterministic integer seed from field name and value."""
        msg = f"{field}:{value}".encode("utf-8")
        digest = _hmac.new(self._secret, msg, _hashlib.sha256).digest()
        # Use first 8 bytes as seed (64-bit)
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

        # Instead of creating a full DataForge copy, re-seed the RNG
        # of a lightweight forge copy.  We use copy() only once and
        # rely on the cache to amortize the cost.
        method = self._get_method(field)
        if method is not None:
            # Save and restore the forge's RNG state to get deterministic output
            # without creating a new forge instance.
            import random as _random_mod

            temp_rng = _random_mod.Random(seed)
            # Swap the engine's RNG temporarily for deterministic generation
            engine = self._forge._engine
            orig_rng = engine._rng
            engine._rng = temp_rng
            try:
                fake_val = method()
            finally:
                engine._rng = orig_rng
        else:
            # Fallback: just hash the value
            fake_val = (
                _hmac.new(
                    self._secret, str_val.encode("utf-8"), _hashlib.sha256
                ).hexdigest()[: len(str_val)]
                if str_val
                else ""
            )

        # Format-preserving for emails
        if field in ("email", "internet.email") and isinstance(original_value, str):
            fake_val = self._format_preserve_email(fake_val, original_value)

        # Format-preserving for phone numbers
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
        # If fake doesn't have @, construct one
        if "@" in original:
            _, domain = original.rsplit("@", 1)
            return f"{fake_str}@{domain}"
        return fake_str

    @staticmethod
    def _format_preserve_phone(fake: Any, original: str) -> str:
        """Try to preserve phone number format (length and separators)."""
        fake_str = str(fake)
        # If lengths match, return as-is
        if len(fake_str) == len(original):
            return fake_str
        # Try to match the original format
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
        """Anonymize a list of row dicts.

        Parameters
        ----------
        rows : list[dict[str, Any]]
            Input rows (not modified in place).
        fields : dict[str, str]
            Mapping of column name → DataForge field name.
            Only specified columns are anonymized; others pass through.

        Returns
        -------
        list[dict[str, Any]]
            Anonymized rows.
        """
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
        """Anonymize a CSV file in streaming fashion.

        Parameters
        ----------
        input_path : str
            Path to input CSV.
        output_path : str
            Path to output CSV.
        fields : dict[str, str]
            Column → DataForge field mappings.
        delimiter : str
            CSV delimiter.
        encoding : str
            File encoding.
        batch_size : int
            Rows to process per batch.

        Returns
        -------
        int
            Number of rows processed.
        """
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
