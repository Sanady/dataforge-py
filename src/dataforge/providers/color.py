"""Color provider — generates fake colors in various formats."""

from dataforge.providers.base import BaseProvider

_COLOR_NAMES: tuple[str, ...] = (
    "Red",
    "Green",
    "Blue",
    "Yellow",
    "Orange",
    "Purple",
    "Pink",
    "Brown",
    "Black",
    "White",
    "Gray",
    "Cyan",
    "Magenta",
    "Lime",
    "Maroon",
    "Navy",
    "Olive",
    "Teal",
    "Aqua",
    "Silver",
)


class ColorProvider(BaseProvider):
    """Generates fake color values in various formats.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "color"
    _locale_modules = ()
    _field_map = {
        "color_name": "color_name",
        "color": "color_name",
        "hex_color": "hex_color",
        "rgb_color": "rgb_string",
        "hsl_color": "hsl_string",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "color_name": _COLOR_NAMES,
    }

    # Scalar helpers

    def _one_hex(self) -> str:
        return f"#{self._engine._rng.getrandbits(24):06x}"

    def _one_rgb(self) -> tuple[int, int, int]:
        bits = self._engine._rng.getrandbits(24)
        return (bits >> 16) & 0xFF, (bits >> 8) & 0xFF, bits & 0xFF

    def _one_rgba(self) -> tuple[int, int, int, float]:
        r, g, b = self._one_rgb()
        a = self._engine.random_int(0, 100) / 100.0
        return (r, g, b, a)

    def _one_hsl(self) -> tuple[int, int, int]:
        return (
            self._engine.random_int(0, 360),
            self._engine.random_int(0, 100),
            self._engine.random_int(0, 100),
        )

    # Public API

    def hex_color(self, count: int = 1) -> str | list[str]:
        """Generate a random hex color (e.g. ``"#a3f2c1"``)."""
        if count == 1:
            return self._one_hex()
        return [self._one_hex() for _ in range(count)]

    def rgb(self, count: int = 1) -> tuple[int, int, int] | list[tuple[int, int, int]]:
        """Generate a random RGB tuple (e.g. ``(123, 45, 200)``)."""
        if count == 1:
            return self._one_rgb()
        return [self._one_rgb() for _ in range(count)]

    def rgba(
        self, count: int = 1
    ) -> tuple[int, int, int, float] | list[tuple[int, int, int, float]]:
        """Generate a random RGBA tuple (e.g. ``(123, 45, 200, 0.75)``)."""
        if count == 1:
            return self._one_rgba()
        return [self._one_rgba() for _ in range(count)]

    def rgb_string(self, count: int = 1) -> str | list[str]:
        """Generate a random RGB CSS string (e.g. ``"rgb(123, 45, 200)"``)."""
        if count == 1:
            r, g, b = self._one_rgb()
            return f"rgb({r}, {g}, {b})"
        result: list[str] = []
        for _ in range(count):
            r, g, b = self._one_rgb()
            result.append(f"rgb({r}, {g}, {b})")
        return result

    def hsl(self, count: int = 1) -> tuple[int, int, int] | list[tuple[int, int, int]]:
        """Generate a random HSL tuple (e.g. ``(210, 65, 50)``)."""
        if count == 1:
            return self._one_hsl()
        return [self._one_hsl() for _ in range(count)]

    def hsl_string(self, count: int = 1) -> str | list[str]:
        """Generate a random HSL CSS string (e.g. ``"hsl(210, 65%, 50%)"``)."""
        if count == 1:
            h, s, lt = self._one_hsl()
            return f"hsl({h}, {s}%, {lt}%)"
        result: list[str] = []
        for _ in range(count):
            h, s, lt = self._one_hsl()
            result.append(f"hsl({h}, {s}%, {lt}%)")
        return result
