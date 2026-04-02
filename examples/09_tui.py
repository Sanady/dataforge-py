"""Interactive TUI — Build schemas visually in the terminal.

This example shows how to launch the interactive terminal user interface
for browsing providers, building schemas, previewing data, and exporting
to CSV/JSON/JSONL/SQL — all without writing code.

Requires: pip install dataforge-py[tui]
"""

print("=== DataForge Interactive TUI ===")
print()
print("Launch the TUI with:")
print()
print("    from dataforge.tui.app import DataForgeTUI")
print("    app = DataForgeTUI()")
print("    app.run()")
print()
print("Or from the command line (if you add a CLI entry):")
print("    python -m dataforge.tui.app")
print()
print("Features:")
print("  - 3-panel layout: Provider Tree | Schema Fields | Data Preview")
print("  - Browse all 27 providers and 198+ fields in a tree view")
print("  - Click fields to add them to your schema")
print("  - Press 'p' to preview generated data")
print("  - Press 'e' to export (CSV, JSON, JSONL, SQL)")
print("  - Press 'd' to delete the last field")
print("  - Press 'c' to clear the schema")
print("  - Press 'q' to quit")
print()
print("Keyboard Shortcuts:")
print("  p — Preview generated data in the table")
print("  e — Open export dialog")
print("  d — Delete last field from schema")
print("  c — Clear all schema fields")
print("  q — Quit the application")
print()

# Uncomment to launch:
# from dataforge.tui.app import DataForgeTUI
# app = DataForgeTUI()
# app.run()
