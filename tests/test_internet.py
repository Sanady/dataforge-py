"""Tests for the InternetProvider."""

from dataforge import DataForge


class TestInternetScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_email_format(self) -> None:
        result = self.forge.internet.email()
        assert isinstance(result, str)
        assert "@" in result
        assert "." in result.split("@")[1]

    def test_username_is_ascii(self) -> None:
        result = self.forge.internet.username()
        assert isinstance(result, str)
        assert result.isascii()
        assert len(result) > 0

    def test_domain_format(self) -> None:
        result = self.forge.internet.domain()
        assert isinstance(result, str)
        assert "." in result

    def test_url_format(self) -> None:
        result = self.forge.internet.url()
        assert isinstance(result, str)
        assert result.startswith(("http://", "https://"))

    def test_ipv4_format(self) -> None:
        for _ in range(50):
            result = self.forge.internet.ipv4()
            assert isinstance(result, str)
            parts = result.split(".")
            assert len(parts) == 4
            for part in parts:
                assert 0 <= int(part) <= 255

    def test_slug_format(self) -> None:
        for _ in range(50):
            result = self.forge.internet.slug()
            assert isinstance(result, str)
            assert "-" in result
            assert all(ch.isalpha() or ch == "-" for ch in result)

    def test_tld_returns_str(self) -> None:
        result = self.forge.internet.tld()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_safe_email_uses_example_domain(self) -> None:
        for _ in range(50):
            result = self.forge.internet.safe_email()
            assert "@" in result
            domain = result.split("@")[1]
            assert domain in ("example.com", "example.org", "example.net")


class TestInternetBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_email_batch(self) -> None:
        result = self.forge.internet.email(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all("@" in e for e in result)

    def test_username_batch(self) -> None:
        result = self.forge.internet.username(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_ipv4_batch(self) -> None:
        result = self.forge.internet.ipv4(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        for ip in result:
            parts = ip.split(".")
            assert len(parts) == 4

    def test_slug_batch(self) -> None:
        result = self.forge.internet.slug(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_tld_batch(self) -> None:
        result = self.forge.internet.tld(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_safe_email_batch(self) -> None:
        result = self.forge.internet.safe_email(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all("@" in e for e in result)


class TestInternetLocales:
    def test_de_DE_email(self) -> None:
        forge = DataForge(locale="de_DE", seed=42)
        email = forge.internet.email()
        assert "@" in email

    def test_fr_FR_email(self) -> None:
        forge = DataForge(locale="fr_FR", seed=42)
        email = forge.internet.email()
        assert "@" in email

    def test_es_ES_email(self) -> None:
        forge = DataForge(locale="es_ES", seed=42)
        email = forge.internet.email()
        assert "@" in email

    def test_ja_JP_email(self) -> None:
        forge = DataForge(locale="ja_JP", seed=42)
        email = forge.internet.email()
        assert "@" in email
        # Should still be ASCII even with Japanese names
        assert email.isascii()
