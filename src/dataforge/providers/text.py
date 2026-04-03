"""Text provider — paragraphs, headlines, slugs, quotes, etc."""

from dataforge.providers.base import BaseProvider

_QUOTE_AUTHORS: tuple[str, ...] = (
    "Albert Einstein",
    "Winston Churchill",
    "Mark Twain",
    "Oscar Wilde",
    "Benjamin Franklin",
    "Mahatma Gandhi",
    "Martin Luther King Jr.",
    "Abraham Lincoln",
    "Confucius",
    "Aristotle",
    "Plato",
    "Socrates",
    "William Shakespeare",
    "Maya Angelou",
    "Nelson Mandela",
)

_QUOTE_TEMPLATES: tuple[str, ...] = (
    "The only way to do great work is to love what you do.",
    "In the middle of difficulty lies opportunity.",
    "Life is what happens when you're busy making other plans.",
    "Be yourself; everyone else is already taken.",
    "The future belongs to those who believe in the beauty of their dreams.",
    "It does not matter how slowly you go as long as you do not stop.",
    "The only impossible journey is the one you never begin.",
    "What you get by achieving your goals is not as important as what you become.",
    "Believe you can and you're halfway there.",
    "Success is not final, failure is not fatal: it is the courage to continue that counts.",
    "The best time to plant a tree was 20 years ago. The second best time is now.",
    "Everything you've ever wanted is on the other side of fear.",
    "Imagination is more important than knowledge.",
    "The mind is everything. What you think you become.",
    "Strive not to be a success, but rather to be of value.",
)

_HEADLINE_STARTERS: tuple[str, ...] = (
    "Breaking:",
    "Exclusive:",
    "Report:",
    "Analysis:",
    "Update:",
    "Opinion:",
    "Study Shows",
    "New Research Reveals",
    "Experts Say",
    "Survey Finds",
    "Officials Announce",
    "Scientists Discover",
)

_HEADLINE_TOPICS: tuple[str, ...] = (
    "Global Economy Expected to Grow",
    "New Technology Breakthrough Announced",
    "Climate Change Impacts Intensify",
    "Healthcare Reform Plan Unveiled",
    "Education Standards Under Review",
    "Housing Market Shows Signs of Recovery",
    "Energy Sector Faces New Challenges",
    "Trade Agreement Reaches Final Stage",
    "Cybersecurity Threats on the Rise",
    "Space Exploration Milestone Achieved",
    "Artificial Intelligence Transforms Industry",
    "Renewable Energy Investment Surges",
    "Transportation Infrastructure Bill Advances",
    "Mental Health Awareness Campaign Launches",
    "Digital Privacy Regulations Proposed",
)

_BUZZWORDS: tuple[str, ...] = (
    "synergy",
    "blockchain",
    "paradigm shift",
    "disruptive innovation",
    "scalability",
    "agile methodology",
    "machine learning",
    "cloud-native",
    "digital transformation",
    "microservices",
    "DevOps",
    "big data",
    "Internet of Things",
    "edge computing",
    "deep learning",
)

_TEXT_WORDS: tuple[str, ...] = (
    "the",
    "be",
    "to",
    "of",
    "and",
    "a",
    "in",
    "that",
    "have",
    "I",
    "it",
    "for",
    "not",
    "on",
    "with",
    "he",
    "as",
    "you",
    "do",
    "at",
    "this",
    "but",
    "his",
    "by",
    "from",
    "they",
    "we",
    "say",
    "her",
    "she",
    "or",
    "an",
    "will",
    "my",
    "one",
    "all",
    "would",
    "there",
    "their",
    "what",
    "so",
    "up",
    "out",
    "if",
    "about",
    "who",
    "get",
    "which",
    "go",
    "me",
)


class TextProvider(BaseProvider):
    """Generates fake text data — paragraphs, headlines, quotes, etc."""

    __slots__ = ()

    _provider_name = "text"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "quote": "quote",
        "headline": "headline",
        "buzzword": "buzzword",
        "paragraph": "paragraph",
        "text_block": "text_block",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "buzzword": _BUZZWORDS,
    }

    def _one_quote(self) -> str:
        quote = self._engine.choice(_QUOTE_TEMPLATES)
        author = self._engine.choice(_QUOTE_AUTHORS)
        return f'"{quote}" — {author}'

    def _one_headline(self) -> str:
        starter = self._engine.choice(_HEADLINE_STARTERS)
        topic = self._engine.choice(_HEADLINE_TOPICS)
        return f"{starter} {topic}"

    def _one_paragraph(self) -> str:
        length = self._engine.random_int(3, 8)
        sentences: list[str] = []
        _choices = self._engine.choices
        _ri = self._engine.random_int
        _words = _TEXT_WORDS
        for _ in range(length):
            word_count = _ri(5, 15)
            words = _choices(_words, word_count)
            # Capitalize first word and join
            sentence = " ".join(words)
            sentence = sentence[0].upper() + sentence[1:] + "."
            sentences.append(sentence)
        return " ".join(sentences)

    def _one_text_block(self) -> str:
        para_count = self._engine.random_int(2, 5)
        return "\n\n".join(self._one_paragraph() for _ in range(para_count))

    def quote(self, count: int = 1) -> str | list[str]:
        """Generate a fake quote with attribution."""
        if count == 1:
            return self._one_quote()
        _quotes = self._engine.choices(_QUOTE_TEMPLATES, count)
        _authors = self._engine.choices(_QUOTE_AUTHORS, count)
        return [f'"{q}" — {a}' for q, a in zip(_quotes, _authors)]

    def headline(self, count: int = 1) -> str | list[str]:
        """Generate a news-style headline."""
        if count == 1:
            return self._one_headline()
        _starters = self._engine.choices(_HEADLINE_STARTERS, count)
        _topics = self._engine.choices(_HEADLINE_TOPICS, count)
        return [f"{s} {t}" for s, t in zip(_starters, _topics)]

    def paragraph(self, count: int = 1) -> str | list[str]:
        """Generate a random paragraph of sentences."""
        if count == 1:
            return self._one_paragraph()
        _choices = self._engine.choices
        _ri = self._engine.random_int
        _words = _TEXT_WORDS
        _join = " ".join
        result: list[str] = []
        for _ in range(count):
            length = _ri(3, 8)
            sentences: list[str] = []
            for _s in range(length):
                word_count = _ri(5, 15)
                ws = _choices(_words, word_count)
                sent = _join(ws)
                sentences.append(sent[0].upper() + sent[1:] + ".")
            result.append(_join(sentences))
        return result

    def text_block(self, count: int = 1) -> str | list[str]:
        """Generate a multi-paragraph text block."""
        if count == 1:
            return self._one_text_block()
        _choices = self._engine.choices
        _ri = self._engine.random_int
        _words = _TEXT_WORDS
        _join_space = " ".join
        _join_para = "\n\n".join
        result: list[str] = []
        for _ in range(count):
            para_count = _ri(2, 5)
            paras: list[str] = []
            for _p in range(para_count):
                length = _ri(3, 8)
                sentences: list[str] = []
                for _s in range(length):
                    word_count = _ri(5, 15)
                    ws = _choices(_words, word_count)
                    sent = _join_space(ws)
                    sentences.append(sent[0].upper() + sent[1:] + ".")
                paras.append(_join_space(sentences))
            result.append(_join_para(paras))
        return result
