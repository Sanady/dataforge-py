"""dataforge TUI — interactive schema builder using Textual.

Launch via::

    dataforge --tui

Requires the ``textual`` optional dependency::

    pip install dataforge-py[tui]
"""

from __future__ import annotations


def launch() -> None:
    """Launch the TUI schema builder application."""
    try:
        from textual.app import App  # noqa: F401
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "textual is required for the TUI. "
            "Install it with: pip install dataforge-py[tui]"
        ) from exc

    from dataforge.tui.app import DataForgeTUI

    app = DataForgeTUI()
    app.run()
