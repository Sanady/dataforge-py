"""Tests for the NetworkProvider."""

import re

from dataforge import DataForge


class TestNetworkScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_ipv6_returns_str(self) -> None:
        result = self.forge.network.ipv6()
        assert isinstance(result, str)

    def test_ipv6_format(self) -> None:
        for _ in range(50):
            result = self.forge.network.ipv6()
            groups = result.split(":")
            assert len(groups) == 8
            for group in groups:
                assert len(group) == 4
                assert all(c in "0123456789abcdef" for c in group)

    def test_mac_address_returns_str(self) -> None:
        result = self.forge.network.mac_address()
        assert isinstance(result, str)

    def test_mac_address_format(self) -> None:
        for _ in range(50):
            result = self.forge.network.mac_address()
            octets = result.split(":")
            assert len(octets) == 6
            for octet in octets:
                assert len(octet) == 2
                assert all(c in "0123456789abcdef" for c in octet)

    def test_port_returns_int(self) -> None:
        result = self.forge.network.port()
        assert isinstance(result, int)

    def test_port_in_range(self) -> None:
        for _ in range(50):
            result = self.forge.network.port()
            assert 1 <= result <= 65535

    def test_hostname_returns_str(self) -> None:
        result = self.forge.network.hostname()
        assert isinstance(result, str)

    def test_hostname_has_dot(self) -> None:
        for _ in range(50):
            result = self.forge.network.hostname()
            assert "." in result

    def test_user_agent_returns_str(self) -> None:
        result = self.forge.network.user_agent()
        assert isinstance(result, str)

    def test_user_agent_contains_mozilla(self) -> None:
        for _ in range(50):
            result = self.forge.network.user_agent()
            assert "Mozilla" in result

    def test_http_method_returns_str(self) -> None:
        result = self.forge.network.http_method()
        assert isinstance(result, str)

    def test_http_method_is_valid(self) -> None:
        valid_methods = {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}
        for _ in range(50):
            result = self.forge.network.http_method()
            assert result in valid_methods

    def test_http_status_code_returns_str(self) -> None:
        result = self.forge.network.http_status_code()
        assert isinstance(result, str)

    def test_http_status_code_format(self) -> None:
        for _ in range(50):
            result = self.forge.network.http_status_code()
            assert re.match(r"^\d{3} .+$", result), f"Bad status code: {result}"


class TestNetworkBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_ipv6_batch(self) -> None:
        result = self.forge.network.ipv6(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        for addr in result:
            groups = addr.split(":")
            assert len(groups) == 8

    def test_mac_address_batch(self) -> None:
        result = self.forge.network.mac_address(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        for mac in result:
            octets = mac.split(":")
            assert len(octets) == 6

    def test_port_batch(self) -> None:
        result = self.forge.network.port(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(1 <= p <= 65535 for p in result)

    def test_hostname_batch(self) -> None:
        result = self.forge.network.hostname(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all("." in h for h in result)

    def test_user_agent_batch(self) -> None:
        result = self.forge.network.user_agent(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all("Mozilla" in ua for ua in result)

    def test_http_method_batch(self) -> None:
        result = self.forge.network.http_method(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        valid_methods = {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}
        assert all(m in valid_methods for m in result)

    def test_http_status_code_batch(self) -> None:
        result = self.forge.network.http_status_code(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(re.match(r"^\d{3} .+$", s) for s in result)
