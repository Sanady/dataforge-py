"""Tests for the LoremProvider."""

from dataforge import DataForge


class TestLoremScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_word_returns_str(self) -> None:
        result = self.forge.lorem.word()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_sentence_returns_str(self) -> None:
        result = self.forge.lorem.sentence()
        assert isinstance(result, str)
        assert result.endswith(".")
        # First char should be capitalized
        assert result[0].isupper()

    def test_sentence_word_count(self) -> None:
        result = self.forge.lorem.sentence(word_count=5)
        assert isinstance(result, str)
        # Should have approximately 5 words (minus the period)
        words = result.rstrip(".").split()
        assert len(words) == 5

    def test_paragraph_returns_str(self) -> None:
        result = self.forge.lorem.paragraph()
        assert isinstance(result, str)
        # Should contain multiple sentences
        sentences = [s for s in result.split(". ") if s]
        assert len(sentences) >= 2

    def test_text_max_chars(self) -> None:
        result = self.forge.lorem.text(max_chars=100)
        assert isinstance(result, str)
        assert len(result) <= 110  # Small buffer for edge cases


class TestLoremBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_word_batch(self) -> None:
        result = self.forge.lorem.word(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_sentence_batch(self) -> None:
        result = self.forge.lorem.sentence(count=10)
        assert isinstance(result, list)
        assert len(result) == 10
        assert all(s.endswith(".") for s in result)

    def test_paragraph_batch(self) -> None:
        result = self.forge.lorem.paragraph(count=5)
        assert isinstance(result, list)
        assert len(result) == 5


class TestLoremLocaleIndependent:
    def test_lorem_works_with_any_locale(self) -> None:
        for locale in ("en_US", "de_DE", "fr_FR", "es_ES", "ja_JP"):
            forge = DataForge(locale=locale, seed=42)
            word = forge.lorem.word()
            assert isinstance(word, str)
            sentence = forge.lorem.sentence()
            assert sentence.endswith(".")
