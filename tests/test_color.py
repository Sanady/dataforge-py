"""Tests for the ColorProvider."""

import re

from dataforge import DataForge
from dataforge.providers.color import _COLOR_NAMES


class TestColorScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_color_name_returns_str(self) -> None:
        result = self.forge.color.color_name()
        assert isinstance(result, str)
        assert result in _COLOR_NAMES

    def test_hex_color_returns_str(self) -> None:
        result = self.forge.color.hex_color()
        assert isinstance(result, str)

    def test_hex_color_format(self) -> None:
        for _ in range(50):
            result = self.forge.color.hex_color()
            assert re.match(r"^#[0-9a-f]{6}$", result), f"Bad hex color: {result}"

    def test_rgb_returns_tuple(self) -> None:
        result = self.forge.color.rgb()
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_rgb_values_in_range(self) -> None:
        for _ in range(50):
            r, g, b = self.forge.color.rgb()
            assert 0 <= r <= 255
            assert 0 <= g <= 255
            assert 0 <= b <= 255

    def test_rgba_returns_tuple(self) -> None:
        result = self.forge.color.rgba()
        assert isinstance(result, tuple)
        assert len(result) == 4

    def test_rgba_values_in_range(self) -> None:
        for _ in range(50):
            r, g, b, a = self.forge.color.rgba()
            assert 0 <= r <= 255
            assert 0 <= g <= 255
            assert 0 <= b <= 255
            assert 0.0 <= a <= 1.0

    def test_rgb_string_format(self) -> None:
        for _ in range(50):
            result = self.forge.color.rgb_string()
            assert re.match(r"^rgb\(\d{1,3}, \d{1,3}, \d{1,3}\)$", result), (
                f"Bad rgb string: {result}"
            )

    def test_hsl_returns_tuple(self) -> None:
        result = self.forge.color.hsl()
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_hsl_values_in_range(self) -> None:
        for _ in range(50):
            h, s, lt = self.forge.color.hsl()
            assert 0 <= h <= 360
            assert 0 <= s <= 100
            assert 0 <= lt <= 100

    def test_hsl_string_format(self) -> None:
        for _ in range(50):
            result = self.forge.color.hsl_string()
            assert re.match(r"^hsl\(\d{1,3}, \d{1,3}%, \d{1,3}%\)$", result), (
                f"Bad hsl string: {result}"
            )


class TestColorBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_color_name_batch(self) -> None:
        result = self.forge.color.color_name(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(c in _COLOR_NAMES for c in result)

    def test_hex_color_batch(self) -> None:
        result = self.forge.color.hex_color(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(re.match(r"^#[0-9a-f]{6}$", c) for c in result)

    def test_rgb_batch(self) -> None:
        result = self.forge.color.rgb(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(isinstance(c, tuple) and len(c) == 3 for c in result)

    def test_rgba_batch(self) -> None:
        result = self.forge.color.rgba(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(isinstance(c, tuple) and len(c) == 4 for c in result)

    def test_rgb_string_batch(self) -> None:
        result = self.forge.color.rgb_string(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_hsl_batch(self) -> None:
        result = self.forge.color.hsl(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_hsl_string_batch(self) -> None:
        result = self.forge.color.hsl_string(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
