"""Tests for the LlmProvider."""

from dataforge import DataForge


class TestModelName:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.model_name()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_batch(self) -> None:
        results = self.forge.llm.model_name(count=50)
        assert isinstance(results, list)
        assert len(results) == 50

    def test_deterministic(self) -> None:
        a = DataForge(seed=99).llm.model_name()
        b = DataForge(seed=99).llm.model_name()
        assert a == b


class TestProviderName:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.provider_name()
        assert isinstance(result, str)

    def test_batch(self) -> None:
        results = self.forge.llm.provider_name(count=50)
        assert len(results) == 50


class TestApiKey:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        key = self.forge.llm.api_key()
        assert isinstance(key, str)
        assert len(key) > 40  # prefix + 40 chars

    def test_batch(self) -> None:
        results = self.forge.llm.api_key(count=50)
        assert len(results) == 50
        assert all(len(k) > 40 for k in results)

    def test_has_prefix(self) -> None:
        prefixes = (
            "sk-",
            "sk-proj-",
            "sk-ant-api03-",
            "AI",
            "gsk_",
            "xai-",
            "pplx-",
            "r8_",
            "hf_",
            "co-",
        )
        for _ in range(50):
            key = self.forge.llm.api_key()
            assert any(key.startswith(p) for p in prefixes), f"Bad prefix: {key}"


class TestFinishReason:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        assert isinstance(self.forge.llm.finish_reason(), str)

    def test_batch(self) -> None:
        results = self.forge.llm.finish_reason(count=50)
        assert len(results) == 50

    def test_valid_values(self) -> None:
        valid = {
            "stop",
            "length",
            "content_filter",
            "tool_calls",
            "end_turn",
            "max_tokens",
            "stop_sequence",
            "function_call",
        }
        for _ in range(50):
            assert self.forge.llm.finish_reason() in valid


class TestStopSequence:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        assert isinstance(self.forge.llm.stop_sequence(), str)

    def test_batch(self) -> None:
        results = self.forge.llm.stop_sequence(count=50)
        assert len(results) == 50


class TestToolName:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        assert isinstance(self.forge.llm.tool_name(), str)

    def test_batch(self) -> None:
        results = self.forge.llm.tool_name(count=50)
        assert len(results) == 50


class TestToolCallId:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.tool_call_id()
        assert isinstance(result, str)
        assert result.startswith("call_")
        assert len(result) == 29  # "call_" + 24 chars

    def test_batch(self) -> None:
        results = self.forge.llm.tool_call_id(count=50)
        assert len(results) == 50
        assert all(r.startswith("call_") for r in results)


class TestMcpServerName:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        assert isinstance(self.forge.llm.mcp_server_name(), str)

    def test_batch(self) -> None:
        results = self.forge.llm.mcp_server_name(count=50)
        assert len(results) == 50


class TestAgentName:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        assert isinstance(self.forge.llm.agent_name(), str)

    def test_batch(self) -> None:
        results = self.forge.llm.agent_name(count=50)
        assert len(results) == 50


class TestCapability:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        assert isinstance(self.forge.llm.capability(), str)

    def test_batch(self) -> None:
        results = self.forge.llm.capability(count=50)
        assert len(results) == 50


class TestEmbeddingModel:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        assert isinstance(self.forge.llm.embedding_model(), str)

    def test_batch(self) -> None:
        results = self.forge.llm.embedding_model(count=50)
        assert len(results) == 50


class TestVectorDbName:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        assert isinstance(self.forge.llm.vector_db_name(), str)

    def test_batch(self) -> None:
        results = self.forge.llm.vector_db_name(count=50)
        assert len(results) == 50


class TestChunkId:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.chunk_id()
        assert isinstance(result, str)
        assert result.startswith("chunk_")

    def test_batch(self) -> None:
        results = self.forge.llm.chunk_id(count=50)
        assert len(results) == 50
        assert all(r.startswith("chunk_") for r in results)


class TestSimilarityScore:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.similarity_score()
        assert isinstance(result, str)
        val = float(result)
        assert 0.0 <= val <= 1.0

    def test_batch(self) -> None:
        results = self.forge.llm.similarity_score(count=50)
        assert len(results) == 50
        for r in results:
            val = float(r)
            assert 0.0 <= val <= 1.0


class TestNamespace:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        assert isinstance(self.forge.llm.namespace(), str)

    def test_batch(self) -> None:
        results = self.forge.llm.namespace(count=50)
        assert len(results) == 50


class TestModerationCategory:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        assert isinstance(self.forge.llm.moderation_category(), str)

    def test_batch(self) -> None:
        results = self.forge.llm.moderation_category(count=50)
        assert len(results) == 50


class TestModerationScore:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.moderation_score()
        assert isinstance(result, str)
        val = float(result)
        assert 0.0 <= val <= 1.0

    def test_batch(self) -> None:
        results = self.forge.llm.moderation_score(count=50)
        assert len(results) == 50


class TestHarmLabel:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        assert isinstance(self.forge.llm.harm_label(), str)

    def test_batch(self) -> None:
        results = self.forge.llm.harm_label(count=50)
        assert len(results) == 50


class TestTokenCount:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.token_count()
        assert isinstance(result, str)
        val = int(result)
        assert 1 <= val <= 16384

    def test_batch(self) -> None:
        results = self.forge.llm.token_count(count=50)
        assert len(results) == 50
        for r in results:
            val = int(r)
            assert 1 <= val <= 16384


class TestPromptTokens:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.prompt_tokens()
        assert isinstance(result, str)
        val = int(result)
        assert 10 <= val <= 8192

    def test_batch(self) -> None:
        results = self.forge.llm.prompt_tokens(count=50)
        assert len(results) == 50


class TestCompletionTokens:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.completion_tokens()
        assert isinstance(result, str)
        val = int(result)
        assert 1 <= val <= 4096

    def test_batch(self) -> None:
        results = self.forge.llm.completion_tokens(count=50)
        assert len(results) == 50


class TestCostEstimate:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.cost_estimate()
        assert isinstance(result, str)
        assert result.startswith("$")
        val = float(result[1:])
        assert val > 0

    def test_batch(self) -> None:
        results = self.forge.llm.cost_estimate(count=50)
        assert len(results) == 50
        assert all(r.startswith("$") for r in results)


class TestRateLimitHeader:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.llm.rate_limit_header()
        assert isinstance(result, str)
        assert ": " in result  # header: value format

    def test_batch(self) -> None:
        results = self.forge.llm.rate_limit_header(count=50)
        assert len(results) == 50
        assert all(": " in r for r in results)


class TestLlmInSchema:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_schema_fields(self) -> None:
        rows = self.forge.to_dict(
            fields=[
                "model_name",
                "agent_name",
                "embedding_model",
                "moderation_category",
                "token_count",
            ],
            count=5,
        )
        assert len(rows) == 5
        for row in rows:
            assert "model_name" in row
            assert "agent_name" in row
            assert "embedding_model" in row
            assert "moderation_category" in row
            assert "token_count" in row

    def test_schema_all_field_map_entries(self) -> None:
        fields = [
            "model_name",
            "provider_name",
            "api_key",
            "finish_reason",
            "stop_sequence",
            "tool_name",
            "tool_call_id",
            "mcp_server_name",
            "agent_name",
            "capability",
            "embedding_model",
            "vector_db_name",
            "chunk_id",
            "similarity_score",
            "namespace",
            "moderation_category",
            "moderation_score",
            "harm_label",
            "token_count",
            "prompt_tokens",
            "completion_tokens",
            "cost_estimate",
            "rate_limit_header",
        ]
        rows = self.forge.to_dict(fields=fields, count=3)
        assert len(rows) == 3
        for row in rows:
            for f in fields:
                assert f in row
                assert isinstance(row[f], str)
                assert len(row[f]) > 0
