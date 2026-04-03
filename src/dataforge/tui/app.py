"""TUI application — Textual-based interactive schema builder.

Provides a terminal UI for browsing providers, building schemas,
previewing generated data, and exporting to various formats.
"""

from __future__ import annotations

from typing import Any

try:
    from textual.app import App, ComposeResult
    from textual.containers import Horizontal, Vertical
    from textual.widgets import (
        Header,
        Footer,
        Static,
        DataTable,
        Input,
        Button,
        Select,
        Label,
        Tree,
        ListView,
        ListItem,
    )
    from textual.binding import Binding
    from textual.screen import ModalScreen
    from textual import on
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "textual is required for the TUI. "
        "Install it with: pip install dataforge-py[tui]"
    )


# Export dialog


class ExportDialog(ModalScreen[dict[str, Any] | None]):
    """Modal dialog for configuring data export."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
    ]

    CSS = """
    ExportDialog {
        align: center middle;
    }
    #export-container {
        width: 60;
        height: auto;
        max-height: 24;
        background: $surface;
        border: thick $accent;
        padding: 1 2;
    }
    #export-container Label {
        margin-bottom: 1;
    }
    #export-container Input {
        margin-bottom: 1;
    }
    #export-buttons {
        height: 3;
        margin-top: 1;
        align: right middle;
    }
    """

    def compose(self) -> ComposeResult:
        with Vertical(id="export-container"):
            yield Label("Export Data", id="export-title")
            yield Label("Row count:")
            yield Input(value="100", id="export-count", type="integer")
            yield Label("Format:")
            yield Select(
                [
                    ("CSV", "csv"),
                    ("JSON", "json"),
                    ("JSON Lines", "jsonl"),
                    ("SQL", "sql"),
                ],
                value="csv",
                id="export-format",
            )
            yield Label("File path (leave empty for preview):")
            yield Input(placeholder="output.csv", id="export-path")
            with Horizontal(id="export-buttons"):
                yield Button("Export", variant="primary", id="btn-export")
                yield Button("Cancel", id="btn-cancel-export")

    @on(Button.Pressed, "#btn-export")
    def on_export(self) -> None:
        count_input = self.query_one("#export-count", Input)
        fmt_select = self.query_one("#export-format", Select)
        path_input = self.query_one("#export-path", Input)
        try:
            count = int(count_input.value)
        except ValueError:
            count = 100
        self.dismiss(
            {
                "count": count,
                "format": fmt_select.value,
                "path": path_input.value.strip() or None,
            }
        )

    @on(Button.Pressed, "#btn-cancel-export")
    def on_cancel_export(self) -> None:
        self.dismiss(None)

    def action_cancel(self) -> None:
        self.dismiss(None)


# Main TUI application


class DataForgeTUI(App):
    """Interactive schema builder for DataForge."""

    TITLE = "DataForge Schema Builder"
    CSS = """
    #main-layout {
        layout: grid;
        grid-size: 3 1;
        grid-columns: 1fr 1fr 2fr;
        height: 100%;
    }
    #provider-panel {
        height: 100%;
        border: solid $accent;
    }
    #schema-panel {
        height: 100%;
        border: solid $accent;
    }
    #preview-panel {
        height: 100%;
        border: solid $accent;
    }
    .panel-title {
        text-style: bold;
        background: $accent;
        color: $text;
        padding: 0 1;
        width: 100%;
    }
    #schema-list {
        height: 1fr;
    }
    #preview-table {
        height: 1fr;
    }
    #action-bar {
        height: 3;
        dock: bottom;
        layout: horizontal;
        padding: 0 1;
    }
    #action-bar Button {
        margin-right: 1;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("p", "preview", "Preview"),
        Binding("e", "export", "Export"),
        Binding("d", "delete_field", "Delete Field"),
        Binding("c", "clear_schema", "Clear"),
    ]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._schema_fields: list[tuple[str, str]] = []
        self._forge: Any = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="main-layout"):
            with Vertical(id="provider-panel"):
                yield Static("Providers", classes="panel-title")
                yield Tree("Fields", id="provider-tree")
            with Vertical(id="schema-panel"):
                yield Static("Schema Fields", classes="panel-title")
                yield ListView(id="schema-list")
                with Horizontal(id="action-bar"):
                    yield Button("Preview", variant="primary", id="btn-preview")
                    yield Button("Export", variant="success", id="btn-export-main")
                    yield Button("Clear", variant="warning", id="btn-clear")
            with Vertical(id="preview-panel"):
                yield Static("Preview", classes="panel-title")
                yield DataTable(id="preview-table")
        yield Footer()

    def on_mount(self) -> None:
        """Populate the provider tree on mount."""
        from dataforge import DataForge

        self._forge = DataForge(seed=42)
        tree = self.query_one("#provider-tree", Tree)
        tree.root.expand()

        # Build provider → fields tree
        from dataforge.registry import get_field_map

        field_map = get_field_map()

        # Group fields by provider
        provider_fields: dict[str, list[str]] = {}
        for field_name, (prov_name, method_name) in sorted(field_map.items()):
            provider_fields.setdefault(prov_name, []).append(field_name)

        for prov_name in sorted(provider_fields):
            branch = tree.root.add(prov_name, expand=False)
            for field_name in sorted(provider_fields[prov_name]):
                branch.add_leaf(field_name, data=field_name)

    @on(Tree.NodeSelected, "#provider-tree")
    def on_tree_select(self, event: Tree.NodeSelected) -> None:
        """Add selected field to schema."""
        node = event.node
        if node.data is not None:
            field_name = str(node.data)
            self._add_field(field_name)

    def _add_field(self, field_name: str) -> None:
        """Add a field to the schema."""
        self._schema_fields.append((field_name, field_name))
        schema_list = self.query_one("#schema-list", ListView)
        schema_list.append(
            ListItem(Label(field_name), id=f"field-{len(self._schema_fields) - 1}")
        )

    @on(Button.Pressed, "#btn-preview")
    def on_preview_button(self) -> None:
        self.action_preview()

    @on(Button.Pressed, "#btn-export-main")
    def on_export_button(self) -> None:
        self.action_export()

    @on(Button.Pressed, "#btn-clear")
    def on_clear_button(self) -> None:
        self.action_clear_schema()

    def action_preview(self) -> None:
        """Generate and preview data."""
        if not self._schema_fields or self._forge is None:
            return

        field_names = [f for _, f in self._schema_fields]
        try:
            schema = self._forge.schema(field_names)
            rows = schema.generate(count=10)
        except Exception as e:
            self.notify(f"Error: {e}", severity="error")
            return

        table = self.query_one("#preview-table", DataTable)
        table.clear(columns=True)
        for col in field_names:
            table.add_column(col, key=col)
        for row in rows:
            table.add_row(*[str(row.get(c, "")) for c in field_names])

    def action_export(self) -> None:
        """Open export dialog."""
        if not self._schema_fields:
            self.notify("Add fields to schema first.", severity="warning")
            return
        self.push_screen(ExportDialog(), self._handle_export)

    def _handle_export(self, result: dict[str, Any] | None) -> None:
        """Handle export dialog result."""
        if result is None or self._forge is None:
            return

        field_names = [f for _, f in self._schema_fields]
        count = result["count"]
        fmt = result["format"]
        path = result["path"]

        try:
            schema = self._forge.schema(field_names)
            if fmt == "csv":
                schema.to_csv(count=count, path=path)
            elif fmt == "json":
                schema.to_json(count=count, path=path)
            elif fmt == "jsonl":
                schema.to_jsonl(count=count, path=path)
            elif fmt == "sql":
                schema.to_sql(table="data", count=count, path=path)

            if path:
                self.notify(f"Exported {count} rows to {path}", severity="information")
            else:
                # Show preview in table
                self.action_preview()
                self.notify(f"Generated {count} rows ({fmt})", severity="information")
        except Exception as e:
            self.notify(f"Export error: {e}", severity="error")

    def action_delete_field(self) -> None:
        """Remove the last field from the schema."""
        if self._schema_fields:
            self._schema_fields.pop()
            schema_list = self.query_one("#schema-list", ListView)
            if schema_list.children:
                schema_list.children[-1].remove()

    def action_clear_schema(self) -> None:
        """Clear all fields from the schema."""
        self._schema_fields.clear()
        schema_list = self.query_one("#schema-list", ListView)
        schema_list.clear()
        table = self.query_one("#preview-table", DataTable)
        table.clear(columns=True)
