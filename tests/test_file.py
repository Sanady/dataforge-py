"""Tests for the FileProvider."""

from dataforge import DataForge
from dataforge.providers.file import (
    _DIR_PARTS,
    _FILE_EXTENSIONS,
    _FILE_WORDS,
)

_ALL_EXTS = {ext for ext, _, _ in _FILE_EXTENSIONS}
_ALL_MIMES = {mime for _, mime, _ in _FILE_EXTENSIONS}
_ALL_CATEGORIES = {"text", "document", "image", "audio", "video", "archive"}


class TestFileScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_file_name_returns_str(self) -> None:
        result = self.forge.file.file_name()
        assert isinstance(result, str)

    def test_file_name_has_extension(self) -> None:
        for _ in range(50):
            name = self.forge.file.file_name()
            assert "." in name
            word, ext = name.rsplit(".", 1)
            assert word in _FILE_WORDS
            assert ext in _ALL_EXTS

    def test_file_extension_returns_str(self) -> None:
        result = self.forge.file.file_extension()
        assert isinstance(result, str)
        assert result in _ALL_EXTS

    def test_mime_type_returns_str(self) -> None:
        result = self.forge.file.mime_type()
        assert isinstance(result, str)
        assert result in _ALL_MIMES

    def test_file_path_returns_str(self) -> None:
        result = self.forge.file.file_path()
        assert isinstance(result, str)

    def test_file_path_starts_with_slash(self) -> None:
        for _ in range(50):
            path = self.forge.file.file_path()
            assert path.startswith("/")

    def test_file_path_has_file_name(self) -> None:
        for _ in range(50):
            path = self.forge.file.file_path()
            parts = path.split("/")
            # At least: empty string (before leading /), one dir, filename
            assert len(parts) >= 3
            filename = parts[-1]
            assert "." in filename

    def test_file_path_dir_parts_are_valid(self) -> None:
        for _ in range(50):
            path = self.forge.file.file_path()
            parts = path.split("/")
            # parts[0] is "" (before leading /), parts[-1] is filename
            dir_parts = parts[1:-1]
            assert 1 <= len(dir_parts) <= 4
            assert all(d in _DIR_PARTS for d in dir_parts)

    def test_file_category_returns_str(self) -> None:
        result = self.forge.file.file_category()
        assert isinstance(result, str)
        assert result in _ALL_CATEGORIES


class TestFileBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_file_name_batch(self) -> None:
        result = self.forge.file.file_name(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all("." in name for name in result)

    def test_file_extension_batch(self) -> None:
        result = self.forge.file.file_extension(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(ext in _ALL_EXTS for ext in result)

    def test_mime_type_batch(self) -> None:
        result = self.forge.file.mime_type(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(m in _ALL_MIMES for m in result)

    def test_file_path_batch(self) -> None:
        result = self.forge.file.file_path(count=50)
        assert isinstance(result, list)
        assert len(result) == 50
        assert all(p.startswith("/") for p in result)

    def test_file_category_batch(self) -> None:
        result = self.forge.file.file_category(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(c in _ALL_CATEGORIES for c in result)
