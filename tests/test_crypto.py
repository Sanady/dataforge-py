"""Tests for the CryptoProvider."""

import re

from dataforge import DataForge


class TestCryptoScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_md5_returns_str(self) -> None:
        result = self.forge.crypto.md5()
        assert isinstance(result, str)

    def test_md5_format(self) -> None:
        for _ in range(50):
            result = self.forge.crypto.md5()
            assert re.match(r"^[0-9a-f]{32}$", result), f"Bad MD5: {result}"

    def test_sha1_returns_str(self) -> None:
        result = self.forge.crypto.sha1()
        assert isinstance(result, str)

    def test_sha1_format(self) -> None:
        for _ in range(50):
            result = self.forge.crypto.sha1()
            assert re.match(r"^[0-9a-f]{40}$", result), f"Bad SHA1: {result}"

    def test_sha256_returns_str(self) -> None:
        result = self.forge.crypto.sha256()
        assert isinstance(result, str)

    def test_sha256_format(self) -> None:
        for _ in range(50):
            result = self.forge.crypto.sha256()
            assert re.match(r"^[0-9a-f]{64}$", result), f"Bad SHA256: {result}"

    def test_deterministic_with_seed(self) -> None:
        f1 = DataForge(seed=123)
        f2 = DataForge(seed=123)
        assert f1.crypto.md5() == f2.crypto.md5()
        assert f1.crypto.sha1() == f2.crypto.sha1()
        assert f1.crypto.sha256() == f2.crypto.sha256()


class TestCryptoBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_md5_batch(self) -> None:
        result = self.forge.crypto.md5(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(re.match(r"^[0-9a-f]{32}$", h) for h in result)

    def test_sha1_batch(self) -> None:
        result = self.forge.crypto.sha1(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(re.match(r"^[0-9a-f]{40}$", h) for h in result)

    def test_sha256_batch(self) -> None:
        result = self.forge.crypto.sha256(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(re.match(r"^[0-9a-f]{64}$", h) for h in result)

    def test_batch_uniqueness(self) -> None:
        result = self.forge.crypto.sha256(count=1000)
        unique = set(result)
        # With 256-bit hashes, collisions are astronomically unlikely
        assert len(unique) == 1000
