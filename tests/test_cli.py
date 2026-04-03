"""Tests for the dataforge CLI."""

import csv
import io
import json

import pytest

from dataforge.cli import main
from dataforge.registry import get_field_map


class TestCliListFields:
    def test_list_fields_returns_zero(self) -> None:
        result = main(["--list-fields"])
        assert result == 0

    def test_list_fields_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        main(["--list-fields"])
        captured = capsys.readouterr()
        # Should list all fields
        for field_name in get_field_map():
            assert field_name in captured.out


class TestCliErrorHandling:
    def test_unknown_field_returns_one(self) -> None:
        result = main(["--count", "1", "nonexistent_field_xyz"])
        assert result == 1

    def test_unknown_field_stderr(self, capsys: pytest.CaptureFixture[str]) -> None:
        main(["--count", "1", "nonexistent_field_xyz"])
        captured = capsys.readouterr()
        assert "Error" in captured.err
        assert "nonexistent_field_xyz" in captured.err


class TestCliTextFormat:
    def test_default_format(self, capsys: pytest.CaptureFixture[str]) -> None:
        result = main(["--count", "3", "--seed", "42", "first_name", "email"])
        assert result == 0
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        # header + separator + 3 data rows
        assert len(lines) == 5

    def test_default_fields(self, capsys: pytest.CaptureFixture[str]) -> None:
        result = main(["--count", "2", "--seed", "42"])
        assert result == 0
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        # header should contain default fields
        header = lines[0]
        assert "first_name" in header
        assert "last_name" in header
        assert "email" in header


class TestCliCsvFormat:
    def test_csv_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        result = main(
            ["--count", "5", "--format", "csv", "--seed", "42", "first_name", "city"]
        )
        assert result == 0
        captured = capsys.readouterr()
        reader = csv.DictReader(io.StringIO(captured.out))
        rows = list(reader)
        assert len(rows) == 5
        assert "first_name" in rows[0]
        assert "city" in rows[0]

    def test_csv_headers_match_fields(self, capsys: pytest.CaptureFixture[str]) -> None:
        main(
            [
                "--count",
                "1",
                "--format",
                "csv",
                "--seed",
                "42",
                "email",
                "phone",
                "company",
            ]
        )
        captured = capsys.readouterr()
        reader = csv.DictReader(io.StringIO(captured.out))
        assert reader.fieldnames == ["email", "phone", "company"]


class TestCliJsonFormat:
    def test_json_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        result = main(
            ["--count", "3", "--format", "json", "--seed", "42", "first_name", "email"]
        )
        assert result == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert isinstance(data, list)
        assert len(data) == 3
        assert "first_name" in data[0]
        assert "email" in data[0]


class TestCliJsonlFormat:
    def test_jsonl_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        result = main(
            [
                "--count",
                "4",
                "--format",
                "jsonl",
                "--seed",
                "42",
                "first_name",
                "email",
            ]
        )
        assert result == 0
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        assert len(lines) == 4
        for line in lines:
            row = json.loads(line)
            assert "first_name" in row
            assert "email" in row


class TestCliLocale:
    def test_locale_option(self) -> None:
        result = main(
            ["--count", "3", "--locale", "de_DE", "--seed", "42", "first_name"]
        )
        assert result == 0


class TestCliSeed:
    def test_seed_reproducible(self, capsys: pytest.CaptureFixture[str]) -> None:
        main(["--count", "5", "--format", "json", "--seed", "123", "first_name"])
        out1 = capsys.readouterr().out

        main(["--count", "5", "--format", "json", "--seed", "123", "first_name"])
        out2 = capsys.readouterr().out

        assert out1 == out2

    def test_different_seeds_differ(self, capsys: pytest.CaptureFixture[str]) -> None:
        main(["--count", "10", "--format", "json", "--seed", "1", "first_name"])
        out1 = capsys.readouterr().out

        main(["--count", "10", "--format", "json", "--seed", "2", "first_name"])
        out2 = capsys.readouterr().out

        assert out1 != out2


class TestCliAllFields:
    @pytest.mark.parametrize("field", sorted(get_field_map().keys()))
    def test_field_generates(self, field: str) -> None:
        result = main(["--count", "1", "--seed", "42", field])
        assert result == 0


class TestCliNoHeader:
    def test_text_no_header(self, capsys: pytest.CaptureFixture[str]) -> None:
        result = main(["--count", "3", "--no-header", "--seed", "42", "first_name"])
        assert result == 0
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        # 3 data rows only, no header or separator
        assert len(lines) == 3

    def test_csv_no_header(self, capsys: pytest.CaptureFixture[str]) -> None:
        result = main(
            [
                "--count",
                "3",
                "--format",
                "csv",
                "--no-header",
                "--seed",
                "42",
                "first_name",
            ]
        )
        assert result == 0
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        # 3 data rows only
        assert len(lines) == 3
        # First line should NOT be "first_name"
        assert lines[0] != "first_name"


class TestCliOutput:
    def test_output_to_file(self, tmp_path) -> None:
        out_file = str(tmp_path / "out.csv")
        result = main(
            [
                "--count",
                "3",
                "--format",
                "csv",
                "--seed",
                "42",
                "--output",
                out_file,
                "first_name",
            ]
        )
        assert result == 0
        with open(out_file, "r") as f:
            content = f.read()
        assert "first_name" in content
