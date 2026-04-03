"""LLM provider — model metadata, agents, RAG, moderation, usage/billing, chat."""

from typing import TYPE_CHECKING

from dataforge.backend import RandomEngine
from dataforge.providers.base import BaseProvider

if TYPE_CHECKING:
    from dataforge.core import DataForge

# Module-level immutable tuples — zero per-call allocation


_MODEL_NAMES: tuple[str, ...] = (
    "gpt-4o",
    "gpt-4o-mini",
    "gpt-4-turbo",
    "gpt-4",
    "gpt-3.5-turbo",
    "claude-3.5-sonnet",
    "claude-3.5-haiku",
    "claude-3-opus",
    "claude-3-sonnet",
    "claude-3-haiku",
    "gemini-2.0-flash",
    "gemini-1.5-pro",
    "gemini-1.5-flash",
    "llama-3.1-405b",
    "llama-3.1-70b",
    "llama-3.1-8b",
    "mistral-large",
    "mistral-medium",
    "mistral-small",
    "mixtral-8x22b",
)

_PROVIDER_NAMES: tuple[str, ...] = (
    "OpenAI",
    "Anthropic",
    "Google",
    "Meta",
    "Mistral",
    "Cohere",
    "DeepSeek",
    "Alibaba Cloud",
    "Microsoft",
    "AI21 Labs",
    "Databricks",
    "Amazon Bedrock",
    "Azure OpenAI",
    "Hugging Face",
    "Replicate",
)

_FINISH_REASONS: tuple[str, ...] = (
    "stop",
    "length",
    "content_filter",
    "tool_calls",
    "end_turn",
    "max_tokens",
    "stop_sequence",
    "function_call",
)

_STOP_SEQUENCES: tuple[str, ...] = (
    "<|endoftext|>",
    "<|end|>",
    "</s>",
    "[DONE]",
    "\\n\\n",
    "<stop>",
    "###",
    "---",
    "Human:",
    "User:",
)

# API key prefixes — realistic per-provider patterns
_API_KEY_PREFIXES: tuple[str, ...] = (
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


_TOOL_NAMES: tuple[str, ...] = (
    "web_search",
    "code_interpreter",
    "file_reader",
    "calculator",
    "image_generator",
    "text_to_speech",
    "database_query",
    "api_caller",
    "email_sender",
    "calendar_manager",
    "document_parser",
    "translation_tool",
    "weather_lookup",
    "stock_price",
    "url_fetcher",
)

_AGENT_NAMES: tuple[str, ...] = (
    "ResearchAgent",
    "CodingAssistant",
    "DataAnalyzer",
    "ContentWriter",
    "TaskPlanner",
    "DebugHelper",
    "ReviewBot",
    "SummaryAgent",
    "TranslatorAgent",
    "MonitoringAgent",
    "DeploymentAgent",
    "TestRunner",
    "DocumentAgent",
    "SchedulerBot",
    "SecurityScanner",
)

_MCP_SERVER_NAMES: tuple[str, ...] = (
    "filesystem",
    "github",
    "slack",
    "postgres",
    "sqlite",
    "brave-search",
    "google-maps",
    "puppeteer",
    "memory",
    "sequential-thinking",
    "google-drive",
    "notion",
    "jira",
    "confluence",
    "aws",
)

_CAPABILITIES: tuple[str, ...] = (
    "text-generation",
    "code-completion",
    "image-understanding",
    "tool-use",
    "function-calling",
    "structured-output",
    "streaming",
    "embeddings",
    "fine-tuning",
    "batch-processing",
    "vision",
    "audio-transcription",
    "text-to-speech",
    "retrieval-augmented-generation",
    "multi-turn-conversation",
)


_EMBEDDING_MODELS: tuple[str, ...] = (
    "text-embedding-3-small",
    "text-embedding-3-large",
    "text-embedding-ada-002",
    "voyage-3",
    "voyage-3-lite",
    "voyage-code-3",
    "embed-english-v3.0",
    "embed-multilingual-v3.0",
    "nomic-embed-text-v1.5",
    "bge-large-en-v1.5",
    "bge-m3",
    "gte-large",
    "e5-large-v2",
    "jina-embeddings-v3",
    "mxbai-embed-large-v1",
)

_VECTOR_DB_NAMES: tuple[str, ...] = (
    "Pinecone",
    "Weaviate",
    "Milvus",
    "Qdrant",
    "ChromaDB",
    "pgvector",
    "Elasticsearch",
    "Redis Vector",
    "LanceDB",
    "Vespa",
    "Zilliz Cloud",
    "Supabase Vector",
    "MongoDB Atlas Vector",
    "Azure AI Search",
    "Google Vertex AI",
)

_NAMESPACES: tuple[str, ...] = (
    "default",
    "production",
    "staging",
    "development",
    "documents",
    "knowledge-base",
    "user-content",
    "faq",
    "support-tickets",
    "product-catalog",
    "blog-posts",
    "code-snippets",
    "meeting-notes",
    "research-papers",
    "customer-data",
)


_MODERATION_CATEGORIES: tuple[str, ...] = (
    "hate",
    "hate/threatening",
    "harassment",
    "harassment/threatening",
    "self-harm",
    "self-harm/intent",
    "self-harm/instructions",
    "sexual",
    "sexual/minors",
    "violence",
    "violence/graphic",
    "illicit",
    "illicit/violent",
)

_HARM_LABELS: tuple[str, ...] = (
    "safe",
    "low_risk",
    "medium_risk",
    "high_risk",
    "blocked",
    "flagged",
    "requires_review",
    "auto_approved",
    "escalated",
    "filtered",
)


_RATE_LIMIT_NAMES: tuple[str, ...] = (
    "x-ratelimit-limit-requests",
    "x-ratelimit-limit-tokens",
    "x-ratelimit-remaining-requests",
    "x-ratelimit-remaining-tokens",
    "x-ratelimit-reset-requests",
    "x-ratelimit-reset-tokens",
    "retry-after",
    "x-request-id",
)

# Hex chars for API key / ID generation — avoids repeated string allocation
_HEX_CHARS: str = "0123456789abcdef"
_ALPHANUM: str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

_CHAT_ROLE_VALUES: tuple[str, ...] = ("user", "assistant", "system", "tool")
_CHAT_ROLE_WEIGHTS: tuple[int, ...] = (40, 40, 15, 5)


class LlmProvider(BaseProvider):
    """Generates fake LLM ecosystem data — models, agents, RAG, moderation, billing, chat."""

    __slots__ = ("_forge",)

    _provider_name = "llm"
    _locale_modules: tuple[str, ...] = ()
    _needs_forge: bool = True
    _field_map: dict[str, str] = {
        # LLM metadata
        "model_name": "model_name",
        "llm_provider": "provider_name",
        "provider_name": "provider_name",
        "api_key": "api_key",
        "finish_reason": "finish_reason",
        "stop_sequence": "stop_sequence",
        # Agent / tool use
        "tool_name": "tool_name",
        "tool_call_id": "tool_call_id",
        "mcp_server_name": "mcp_server_name",
        "agent_name": "agent_name",
        "capability": "capability",
        # RAG / embeddings
        "embedding_model": "embedding_model",
        "vector_db_name": "vector_db_name",
        "chunk_id": "chunk_id",
        "similarity_score": "similarity_score",
        "namespace": "namespace",
        # Content moderation
        "moderation_category": "moderation_category",
        "moderation_score": "moderation_score",
        "harm_label": "harm_label",
        # Usage / billing
        "token_count": "token_count",
        "prompt_tokens": "prompt_tokens",
        "completion_tokens": "completion_tokens",
        "cost_estimate": "cost_estimate",
        "rate_limit_header": "rate_limit_header",
        # AI Chat fields
        "chat_role": "chat_role",
        "chat_model": "chat_model",
        "chat_content": "chat_content",
        "chat_tokens": "chat_tokens",
        "chat_finish_reason": "chat_finish_reason",
    }

    def __init__(self, engine: RandomEngine, forge: "DataForge") -> None:
        super().__init__(engine)
        self._forge = forge

    _choice_fields: dict[str, tuple[str, ...]] = {
        "model_name": _MODEL_NAMES,
        "provider_name": _PROVIDER_NAMES,
        "finish_reason": _FINISH_REASONS,
        "stop_sequence": _STOP_SEQUENCES,
        "tool_name": _TOOL_NAMES,
        "mcp_server_name": _MCP_SERVER_NAMES,
        "agent_name": _AGENT_NAMES,
        "capability": _CAPABILITIES,
        "embedding_model": _EMBEDDING_MODELS,
        "vector_db_name": _VECTOR_DB_NAMES,
        "namespace": _NAMESPACES,
        "moderation_category": _MODERATION_CATEGORIES,
        "harm_label": _HARM_LABELS,
    }

    # Scalar helpers

    def _one_api_key(self) -> str:
        prefix = self._engine.choice(_API_KEY_PREFIXES)
        # Generate 40 alphanumeric chars — realistic key length
        _ri = self._engine.random_int
        an = _ALPHANUM
        an_len = len(an)
        return prefix + "".join(an[_ri(0, an_len - 1)] for _ in range(40))

    def _one_tool_call_id(self) -> str:
        # Format: call_XXXX... (24 alphanumeric chars) — matches OpenAI format
        _ri = self._engine.random_int
        an = _ALPHANUM
        an_len = len(an)
        return "call_" + "".join(an[_ri(0, an_len - 1)] for _ in range(24))

    def _one_chunk_id(self) -> str:
        # Format: chunk_XXXXXXXX (8 hex chars)
        bits = self._engine.getrandbits(32)
        return f"chunk_{bits:08x}"

    def _one_similarity_score(self) -> str:
        # Score between 0.0 and 1.0 with 4 decimal places
        return f"{self._engine.random_int(0, 10000) / 10000.0:.4f}"

    def _one_moderation_score(self) -> str:
        # Score between 0.0000 and 1.0000
        return f"{self._engine.random_int(0, 10000) / 10000.0:.4f}"

    def _one_token_count(self) -> str:
        return str(self._engine.random_int(1, 16384))

    def _one_prompt_tokens(self) -> str:
        return str(self._engine.random_int(10, 8192))

    def _one_completion_tokens(self) -> str:
        return str(self._engine.random_int(1, 4096))

    def _one_cost_estimate(self) -> str:
        # Cost in USD: $0.0001 to $9.9999
        cents = self._engine.random_int(1, 99999)
        return f"${cents / 10000.0:.4f}"

    def _one_rate_limit_header(self) -> str:
        name = self._engine.choice(_RATE_LIMIT_NAMES)
        value = str(self._engine.random_int(0, 100000))
        return f"{name}: {value}"

    # Public API — custom methods

    def api_key(self, count: int = 1) -> str | list[str]:
        """Generate a realistic-looking API key."""
        if count == 1:
            return self._one_api_key()
        return [self._one_api_key() for _ in range(count)]

    def tool_call_id(self, count: int = 1) -> str | list[str]:
        """Generate a tool call ID (e.g. call_abc123...)."""
        if count == 1:
            return self._one_tool_call_id()
        return [self._one_tool_call_id() for _ in range(count)]

    def chunk_id(self, count: int = 1) -> str | list[str]:
        """Generate a document chunk ID (e.g. chunk_a1b2c3d4)."""
        if count == 1:
            return self._one_chunk_id()
        return [self._one_chunk_id() for _ in range(count)]

    def similarity_score(self, count: int = 1) -> str | list[str]:
        """Generate a similarity/relevance score (0.0000-1.0000)."""
        if count == 1:
            return self._one_similarity_score()
        return [self._one_similarity_score() for _ in range(count)]

    def moderation_score(self, count: int = 1) -> str | list[str]:
        """Generate a moderation score (0.0000-1.0000)."""
        if count == 1:
            return self._one_moderation_score()
        return [self._one_moderation_score() for _ in range(count)]

    def token_count(self, count: int = 1) -> str | list[str]:
        """Generate a token count (1-16384)."""
        if count == 1:
            return self._one_token_count()
        return [self._one_token_count() for _ in range(count)]

    def prompt_tokens(self, count: int = 1) -> str | list[str]:
        """Generate a prompt token count (10-8192)."""
        if count == 1:
            return self._one_prompt_tokens()
        return [self._one_prompt_tokens() for _ in range(count)]

    def completion_tokens(self, count: int = 1) -> str | list[str]:
        """Generate a completion token count (1-4096)."""
        if count == 1:
            return self._one_completion_tokens()
        return [self._one_completion_tokens() for _ in range(count)]

    def cost_estimate(self, count: int = 1) -> str | list[str]:
        """Generate a cost estimate in USD (e.g. $0.0234)."""
        if count == 1:
            return self._one_cost_estimate()
        return [self._one_cost_estimate() for _ in range(count)]

    def rate_limit_header(self, count: int = 1) -> str | list[str]:
        """Generate a rate limit HTTP header."""
        if count == 1:
            return self._one_rate_limit_header()
        return [self._one_rate_limit_header() for _ in range(count)]

    # AI Chat methods (merged from ai_chat provider)

    def chat_role(self, count: int = 1) -> str | list[str]:
        """Generate a chat message role (user, assistant, system, tool)."""
        if count == 1:
            return self._engine.weighted_choice(_CHAT_ROLE_VALUES, _CHAT_ROLE_WEIGHTS)
        return self._engine.weighted_choices(
            _CHAT_ROLE_VALUES, _CHAT_ROLE_WEIGHTS, count
        )

    def chat_model(self, count: int = 1) -> str | list[str]:
        """Generate a model name for the chat."""
        return self.model_name(count)

    def chat_content(self, count: int = 1) -> str | list[str]:
        """Generate chat message content."""
        return self._forge.ai_prompt.user_prompt(count)

    def chat_tokens(self, count: int = 1) -> str | list[str]:
        """Generate a token count for a chat message."""
        return self.token_count(count)

    def chat_finish_reason(self, count: int = 1) -> str | list[str]:
        """Generate a finish reason for a chat message."""
        return self.finish_reason(count)

    def chat_message(self, count: int = 1) -> dict[str, str] | list[dict[str, str]]:
        """Generate a realistic chat message dict with role, model, content, tokens."""

        def _one() -> dict[str, str]:
            role = self._engine.weighted_choice(_CHAT_ROLE_VALUES, _CHAT_ROLE_WEIGHTS)
            model = self.model_name()
            content = (
                self._forge.ai_prompt.system_prompt()
                if role == "system"
                else self._forge.ai_prompt.user_prompt()
            )
            return {
                "role": role,
                "model": model,
                "content": content,
                "tokens": self.token_count(),
                "finish_reason": self.finish_reason(),
            }

        if count == 1:
            return _one()
        return [_one() for _ in range(count)]
