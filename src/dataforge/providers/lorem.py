"""Lorem provider — generates fake words, sentences, paragraphs, and text.

This provider is locale-independent — it uses the standard Lorem Ipsum
word list which is universally used for placeholder text.
"""

from dataforge.providers.base import BaseProvider

# Standard Lorem Ipsum word pool — immutable tuple for speed
_LOREM_WORDS: tuple[str, ...] = (
    "lorem",
    "ipsum",
    "dolor",
    "sit",
    "amet",
    "consectetur",
    "adipiscing",
    "elit",
    "sed",
    "do",
    "eiusmod",
    "tempor",
    "incididunt",
    "ut",
    "labore",
    "et",
    "dolore",
    "magna",
    "aliqua",
    "enim",
)


class LoremProvider(BaseProvider):
    """Generates fake Lorem Ipsum placeholder text.

    This provider does **not** require locale data — the word list is
    universal.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "lorem"
    _locale_modules = ()
    _field_map = {
        "word": "word",
        "sentence": "sentence",
        "paragraph": "paragraph",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "word": _LOREM_WORDS,
    }

    # Scalar helpers

    def _one_sentence(self, word_count: int) -> str:
        words = self._engine.choices(_LOREM_WORDS, word_count)
        words[0] = words[0].capitalize()
        return " ".join(words) + "."

    def _one_paragraph(self, sentence_count: int) -> str:
        sentences = [
            self._one_sentence(self._engine.random_int(5, 15))
            for _ in range(sentence_count)
        ]
        return " ".join(sentences)

    # Public API

    def sentence(self, count: int = 1, word_count: int = 10) -> str | list[str]:
        """Generate random Lorem Ipsum sentence(s)."""
        if count == 1:
            return self._one_sentence(word_count)
        # Batch: generate all words at once, then slice into sentences
        total_words = word_count * count
        _words = self._engine.choices(_LOREM_WORDS, total_words)
        _join = " ".join
        result: list[str] = []
        for i in range(0, total_words, word_count):
            chunk = _words[i : i + word_count]
            chunk[0] = chunk[0].capitalize()
            result.append(_join(chunk) + ".")
        return result

    def paragraph(self, count: int = 1, sentence_count: int = 5) -> str | list[str]:
        """Generate random Lorem Ipsum paragraph(s)."""
        if count == 1:
            return self._one_paragraph(sentence_count)
        return [self._one_paragraph(sentence_count) for _ in range(count)]

    def text(self, max_chars: int = 200) -> str:
        """Generate Lorem Ipsum text up to *max_chars* characters."""
        parts: list[str] = []
        current_len = 0
        while current_len < max_chars:
            sentence = self._one_sentence(self._engine.random_int(5, 15))
            if current_len + len(sentence) + 1 > max_chars:
                break
            parts.append(sentence)
            current_len += len(sentence) + 1  # +1 for the space
        return " ".join(parts) if parts else self._one_sentence(5)
