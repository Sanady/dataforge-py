"""Tests for the AiPromptProvider."""

from dataforge import DataForge


class TestUserPrompt:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.ai_prompt.user_prompt()
        assert isinstance(result, str)
        assert len(result) > 10

    def test_batch(self) -> None:
        results = self.forge.ai_prompt.user_prompt(count=50)
        assert isinstance(results, list)
        assert len(results) == 50
        assert all(isinstance(r, str) for r in results)

    def test_deterministic(self) -> None:
        a = DataForge(seed=99).ai_prompt.user_prompt()
        b = DataForge(seed=99).ai_prompt.user_prompt()
        assert a == b


class TestCodingPrompt:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.ai_prompt.coding_prompt()
        assert isinstance(result, str)
        assert len(result) > 10

    def test_batch(self) -> None:
        results = self.forge.ai_prompt.coding_prompt(count=50)
        assert isinstance(results, list)
        assert len(results) == 50


class TestCreativePrompt:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.ai_prompt.creative_prompt()
        assert isinstance(result, str)
        assert len(result) > 10

    def test_batch(self) -> None:
        results = self.forge.ai_prompt.creative_prompt(count=50)
        assert isinstance(results, list)
        assert len(results) == 50


class TestAnalysisPrompt:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.ai_prompt.analysis_prompt()
        assert isinstance(result, str)
        assert len(result) > 10

    def test_batch(self) -> None:
        results = self.forge.ai_prompt.analysis_prompt(count=50)
        assert isinstance(results, list)
        assert len(results) == 50


class TestSystemPrompt:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.ai_prompt.system_prompt()
        assert isinstance(result, str)
        assert "You are" in result

    def test_batch(self) -> None:
        results = self.forge.ai_prompt.system_prompt(count=50)
        assert isinstance(results, list)
        assert len(results) == 50
        assert all("You are" in r for r in results)


class TestPersonaPrompt:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.ai_prompt.persona_prompt()
        assert isinstance(result, str)
        assert "You are an" in result

    def test_batch(self) -> None:
        results = self.forge.ai_prompt.persona_prompt(count=50)
        assert isinstance(results, list)
        assert len(results) == 50
        assert all("You are an" in r for r in results)


class TestPromptTemplate:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.ai_prompt.prompt_template()
        assert isinstance(result, str)
        # Should contain placeholders
        assert "{tone}" in result or "{topic}" in result or "{audience}" in result

    def test_batch(self) -> None:
        results = self.forge.ai_prompt.prompt_template(count=50)
        assert isinstance(results, list)
        assert len(results) == 50


class TestFewShotPrompt:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_returns_str(self) -> None:
        result = self.forge.ai_prompt.few_shot_prompt()
        assert isinstance(result, str)
        assert "Example 1:" in result
        assert "Example 2:" in result
        assert "{input}" in result

    def test_batch(self) -> None:
        results = self.forge.ai_prompt.few_shot_prompt(count=20)
        assert isinstance(results, list)
        assert len(results) == 20
        assert all("Example 1:" in r for r in results)


class TestAiPromptInSchema:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_schema_fields(self) -> None:
        rows = self.forge.to_dict(
            fields=["user_prompt", "system_prompt", "coding_prompt"],
            count=5,
        )
        assert len(rows) == 5
        for row in rows:
            assert "user_prompt" in row
            assert "system_prompt" in row
            assert "coding_prompt" in row
            assert len(row["user_prompt"]) > 0
            assert len(row["system_prompt"]) > 0

    def test_schema_all_fields(self) -> None:
        fields = [
            "user_prompt",
            "coding_prompt",
            "creative_prompt",
            "analysis_prompt",
            "system_prompt",
            "persona_prompt",
            "prompt_template",
            "few_shot_prompt",
        ]
        rows = self.forge.to_dict(fields=fields, count=3)
        assert len(rows) == 3
        for row in rows:
            for f in fields:
                assert f in row
                assert isinstance(row[f], str)
                assert len(row[f]) > 0
