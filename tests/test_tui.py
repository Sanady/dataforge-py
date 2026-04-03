"""Tests for TUI — interactive schema builder.

Tests focus on the importable logic and structure since the actual
Textual app requires a terminal. We test what we can without running
the full TUI.
"""

from __future__ import annotations

import pytest


# Helper


def _has_textual() -> bool:
    """Check if textual is available."""
    try:
        import textual  # noqa: F401

        return True
    except ModuleNotFoundError:
        return False


# Import guards


class TestTUIImports:
    @pytest.mark.skipif(
        not _has_textual(),
        reason="textual not installed",
    )
    def test_launch_function_importable(self) -> None:
        from dataforge.tui import launch

        assert callable(launch)

    @pytest.mark.skipif(
        not _has_textual(),
        reason="textual not installed",
    )
    def test_app_class_importable(self) -> None:
        from dataforge.tui.app import DataForgeTUI

        assert DataForgeTUI is not None

    @pytest.mark.skipif(
        not _has_textual(),
        reason="textual not installed",
    )
    def test_export_dialog_importable(self) -> None:
        from dataforge.tui.app import ExportDialog

        assert ExportDialog is not None


# App construction (only if textual is installed)


@pytest.mark.skipif(not _has_textual(), reason="textual not installed")
class TestDataForgeTUIConstruction:
    def test_app_has_title(self) -> None:
        from dataforge.tui.app import DataForgeTUI

        app = DataForgeTUI()
        assert app.TITLE == "DataForge Schema Builder"

    def test_app_has_bindings(self) -> None:
        from dataforge.tui.app import DataForgeTUI

        app = DataForgeTUI()
        # Should have q, p, e, d, c bindings
        binding_keys = [b.key for b in app.BINDINGS]
        assert "q" in binding_keys

    def test_schema_fields_starts_empty(self) -> None:
        from dataforge.tui.app import DataForgeTUI

        app = DataForgeTUI()
        assert app._schema_fields == []

    def test_forge_starts_none(self) -> None:
        from dataforge.tui.app import DataForgeTUI

        app = DataForgeTUI()
        assert app._forge is None


# Export dialog (only if textual is installed)


@pytest.mark.skipif(not _has_textual(), reason="textual not installed")
class TestExportDialog:
    def test_export_dialog_bindings(self) -> None:
        from dataforge.tui.app import ExportDialog

        binding_keys = [b.key for b in ExportDialog.BINDINGS]
        assert "escape" in binding_keys

    def test_export_dialog_has_css(self) -> None:
        from dataforge.tui.app import ExportDialog

        assert ExportDialog.CSS is not None
        assert len(ExportDialog.CSS) > 0
