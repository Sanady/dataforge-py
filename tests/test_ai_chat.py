"""Tests for AI Chat methods (merged into LlmProvider)."""

from dataforge import DataForge


class TestChatRole:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.chat_role()
        assert isinstance(result, str)
        assert result in {"system", "user", "assistant", "tool"}

    def test_batch(self) -> None:
        results = self.forge.llm.chat_role(count=100)
        assert isinstance(results, list)
        assert len(results) == 100
        valid = {"system", "user", "assistant", "tool"}
        assert all(r in valid for r in results)

    def test_weighted_distribution(self) -> None:
        results = self.forge.llm.chat_role(count=1000)
        counts = {r: results.count(r) for r in {"user", "assistant", "system", "tool"}}
        # user and assistant have weight 40 each, system 15, tool 5
        assert counts["user"] > counts["system"]
        assert counts["assistant"] > counts["tool"]


class TestChatModel:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.chat_model()
        assert isinstance(result, str)

    def test_batch(self) -> None:
        results = self.forge.llm.chat_model(count=50)
        assert len(results) == 50


class TestChatContent:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.chat_content()
        assert isinstance(result, str)
        assert len(result) > 10

    def test_batch(self) -> None:
        results = self.forge.llm.chat_content(count=50)
        assert len(results) == 50


class TestChatTokens:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.chat_tokens()
        assert isinstance(result, str)
        val = int(result)
        assert 1 <= val <= 16384

    def test_batch(self) -> None:
        results = self.forge.llm.chat_tokens(count=50)
        assert len(results) == 50


class TestChatFinishReason:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.chat_finish_reason()
        assert isinstance(result, str)

    def test_batch(self) -> None:
        results = self.forge.llm.chat_finish_reason(count=50)
        assert len(results) == 50


class TestChatMessage:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_dict(self) -> None:
        msg = self.forge.llm.chat_message()
        assert isinstance(msg, dict)
        assert "role" in msg
        assert "model" in msg
        assert "content" in msg
        assert "tokens" in msg
        assert "finish_reason" in msg
        assert msg["role"] in {"system", "user", "assistant", "tool"}
        assert len(msg["content"]) > 0

    def test_batch(self) -> None:
        msgs = self.forge.llm.chat_message(count=20)
        assert isinstance(msgs, list)
        assert len(msgs) == 20
        for msg in msgs:
            assert isinstance(msg, dict)
            assert "role" in msg
            assert "model" in msg
            assert "content" in msg

    def test_deterministic(self) -> None:
        a = DataForge(seed=99).llm.chat_message()
        b = DataForge(seed=99).llm.chat_message()
        assert a == b


class TestAiChatInSchema:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_schema_fields(self) -> None:
        rows = self.forge.to_dict(
            fields=[
                "chat_role",
                "chat_model",
                "chat_content",
                "chat_tokens",
                "chat_finish_reason",
            ],
            count=5,
        )
        assert len(rows) == 5
        for row in rows:
            assert "chat_role" in row
            assert "chat_model" in row
            assert "chat_content" in row
            assert "chat_tokens" in row
            assert "chat_finish_reason" in row
            assert row["chat_role"] in {"system", "user", "assistant", "tool"}

    def test_schema_mixed_providers(self) -> None:
        rows = self.forge.to_dict(
            fields=["chat_role", "chat_model", "first_name", "email"],
            count=5,
        )
        assert len(rows) == 5
        for row in rows:
            assert "chat_role" in row
            assert "first_name" in row
            assert "@" in row["email"]
