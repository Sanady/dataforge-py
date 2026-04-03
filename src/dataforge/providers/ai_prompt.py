"""AI Prompt provider — user prompts, system prompts, prompt templates."""

from dataforge.providers.base import BaseProvider

# Module-level immutable tuples — zero per-call allocation

_USER_PROMPTS: tuple[str, ...] = (
    "Summarize this article in 3 bullet points",
    "Explain this concept to a 5 year old",
    "What are the pros and cons of this approach?",
    "Help me brainstorm ideas for a presentation",
    "Translate this text to French",
    "Can you proofread this email for me?",
    "What does this error message mean?",
    "Help me write a cover letter",
    "Create a meal plan for the week",
    "Explain the difference between these two concepts",
    "Suggest improvements for this paragraph",
    "Help me organize my thoughts on this topic",
    "What are some alternatives to this approach?",
    "Can you simplify this explanation?",
    "Give me a step-by-step guide for this process",
    "What questions should I ask in this interview?",
    "Help me draft a professional response",
    "Summarize the key takeaways from this meeting",
    "What are the main arguments for and against this?",
    "Help me create an outline for this essay",
)

_CODING_PROMPTS: tuple[str, ...] = (
    "Write a Python function that parses JSON from a file",
    "How do I fix this TypeError in my code?",
    "Refactor this function to be more efficient",
    "Write unit tests for this class",
    "Explain what this regex pattern does",
    "Help me debug this API endpoint",
    "Convert this SQL query to an ORM query",
    "Write a bash script to automate this deployment",
    "How do I implement pagination in this REST API?",
    "Create a TypeScript interface for this data model",
    "Optimize this database query for performance",
    "Write a GitHub Actions workflow for CI/CD",
    "Help me implement error handling in this function",
    "How do I set up logging in this application?",
    "Write a Docker Compose file for this stack",
    "Implement a retry mechanism with exponential backoff",
    "Create a data validation schema for this input",
    "Write a migration script for this database change",
    "How do I implement caching for this endpoint?",
    "Help me write a recursive algorithm for this problem",
)

_CREATIVE_PROMPTS: tuple[str, ...] = (
    "Write a short story about a robot learning to paint",
    "Create a poem about the changing seasons",
    "Write a dialogue between a cat and a dog",
    "Describe a futuristic city in vivid detail",
    "Write a fairy tale with a modern twist",
    "Create a character profile for a fantasy novel",
    "Write a haiku about morning coffee",
    "Compose a limerick about a programmer",
    "Write a monologue from the perspective of the moon",
    "Create a plot outline for a mystery novel",
    "Write a song lyric about overcoming challenges",
    "Describe an alien world with unique ecosystems",
    "Write a letter from a time traveler to their past self",
    "Create a backstory for a video game character",
    "Write a flash fiction piece about a last sunset",
)

_ANALYSIS_PROMPTS: tuple[str, ...] = (
    "Analyze the sentiment of this customer review",
    "What patterns do you see in this dataset?",
    "Compare the performance metrics across these quarters",
    "Identify potential risks in this project plan",
    "Analyze the market trends for this industry",
    "What insights can you draw from this survey data?",
    "Evaluate the strengths and weaknesses of this proposal",
    "Analyze the root cause of this system failure",
    "What correlations exist between these variables?",
    "Assess the competitive landscape for this product",
    "Analyze the user engagement metrics for this feature",
    "Identify bottlenecks in this supply chain",
    "Evaluate the ROI of this marketing campaign",
    "Analyze the demographic data for this region",
    "What trends emerge from this time series data?",
)

_SYSTEM_PROMPTS: tuple[str, ...] = (
    "You are a helpful assistant that provides clear and concise answers.",
    "You are an expert technical writer. Provide detailed documentation.",
    "You are a patient tutor. Explain concepts step by step.",
    "You are a code reviewer. Focus on correctness, performance, and style.",
    "You are a data analyst. Provide insights backed by evidence.",
    "You are a creative writing coach. Give constructive feedback.",
    "You are a security expert. Identify vulnerabilities and suggest fixes.",
    "You are a product manager. Focus on user needs and business value.",
    "You are a DevOps engineer. Emphasize reliability and automation.",
    "You are a UX researcher. Focus on user experience and accessibility.",
    "You are a financial advisor. Provide balanced and informed guidance.",
    "You are a medical information assistant. Always recommend consulting a doctor.",
    "You are a legal assistant. Provide general information, not legal advice.",
    "You are a language tutor. Help users practice and improve their skills.",
    "You are a project manager. Help organize tasks and track progress.",
)

_PERSONA_ROLES: tuple[str, ...] = (
    "expert Python developer",
    "senior data scientist",
    "experienced DevOps engineer",
    "seasoned frontend developer",
    "professional technical writer",
    "senior security analyst",
    "experienced cloud architect",
    "senior backend engineer",
    "expert database administrator",
    "professional UX designer",
    "senior machine learning engineer",
    "experienced full-stack developer",
    "expert systems architect",
    "professional data engineer",
    "senior site reliability engineer",
)

_PERSONA_TRAITS: tuple[str, ...] = (
    "who focuses on clean, maintainable code",
    "who prioritizes performance and scalability",
    "who emphasizes security best practices",
    "who values thorough testing and documentation",
    "who cares about developer experience",
    "who advocates for simplicity and clarity",
    "who specializes in distributed systems",
    "who focuses on observability and monitoring",
    "who prioritizes accessibility and inclusion",
    "who emphasizes pragmatic solutions",
)

_TEMPLATE_ACTIONS: tuple[str, ...] = (
    "Write",
    "Generate",
    "Create",
    "Draft",
    "Compose",
    "Produce",
    "Build",
    "Design",
    "Develop",
    "Craft",
)

_TEMPLATE_TONES: tuple[str, ...] = (
    "formal",
    "casual",
    "professional",
    "friendly",
    "technical",
    "persuasive",
    "informative",
    "concise",
    "detailed",
    "conversational",
)

_TEMPLATE_FORMATS: tuple[str, ...] = (
    "email",
    "blog post",
    "report",
    "summary",
    "proposal",
    "presentation",
    "documentation",
    "tutorial",
    "review",
    "analysis",
)

_TEMPLATE_TOPICS: tuple[str, ...] = (
    "project updates",
    "quarterly results",
    "product launch",
    "team performance",
    "market research",
    "customer feedback",
    "technical architecture",
    "process improvements",
    "budget planning",
    "risk assessment",
)

_FEW_SHOT_TASKS: tuple[str, ...] = (
    "Classify the sentiment of the following text",
    "Extract the key entities from the following passage",
    "Categorize the following support ticket",
    "Determine the language of the following text",
    "Rate the quality of the following response",
    "Identify the main topic of the following paragraph",
    "Classify the intent of the following user query",
    "Extract the action items from the following text",
    "Determine the urgency level of the following message",
    "Categorize the following product review",
)

_FEW_SHOT_LABELS: tuple[str, ...] = (
    "positive",
    "negative",
    "neutral",
    "urgent",
    "low priority",
    "bug report",
    "feature request",
    "question",
    "feedback",
    "complaint",
)

_FEW_SHOT_EXAMPLES: tuple[str, ...] = (
    "The product works great and I love it!",
    "This is the worst experience I've ever had.",
    "The package arrived on time as expected.",
    "I need help with my account immediately.",
    "It would be nice to have dark mode support.",
    "How do I reset my password?",
    "The new update improved loading times significantly.",
    "I've been waiting for a response for three days.",
    "The interface is intuitive and easy to use.",
    "There seems to be a bug in the checkout process.",
)


class AiPromptProvider(BaseProvider):
    """Generates fake AI prompts — user prompts, system prompts, templates."""

    __slots__ = ()

    _provider_name = "ai_prompt"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "user_prompt": "user_prompt",
        "coding_prompt": "coding_prompt",
        "creative_prompt": "creative_prompt",
        "analysis_prompt": "analysis_prompt",
        "system_prompt": "system_prompt",
        "persona_prompt": "persona_prompt",
        "prompt_template": "prompt_template",
        "few_shot_prompt": "few_shot_prompt",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "user_prompt": _USER_PROMPTS,
        "coding_prompt": _CODING_PROMPTS,
        "creative_prompt": _CREATIVE_PROMPTS,
        "analysis_prompt": _ANALYSIS_PROMPTS,
        "system_prompt": _SYSTEM_PROMPTS,
    }

    def _one_persona_prompt(self) -> str:
        role = self._engine.choice(_PERSONA_ROLES)
        trait = self._engine.choice(_PERSONA_TRAITS)
        return f"You are an {role} {trait}."

    def _one_prompt_template(self) -> str:
        action = self._engine.choice(_TEMPLATE_ACTIONS)
        _tone = self._engine.choice(_TEMPLATE_TONES)
        fmt = self._engine.choice(_TEMPLATE_FORMATS)
        _topic = self._engine.choice(_TEMPLATE_TOPICS)
        return f"{action} a {{tone}} {fmt} about {{topic}} for {{audience}}"

    def _one_few_shot_prompt(self) -> str:
        task = self._engine.choice(_FEW_SHOT_TASKS)
        # Build 2 examples
        ex1 = self._engine.choice(_FEW_SHOT_EXAMPLES)
        lb1 = self._engine.choice(_FEW_SHOT_LABELS)
        ex2 = self._engine.choice(_FEW_SHOT_EXAMPLES)
        lb2 = self._engine.choice(_FEW_SHOT_LABELS)
        return (
            f"{task}.\n\n"
            f'Example 1:\nInput: "{ex1}"\nOutput: {lb1}\n\n'
            f'Example 2:\nInput: "{ex2}"\nOutput: {lb2}\n\n'
            f'Now classify:\nInput: "{{input}}"\nOutput:'
        )

    def persona_prompt(self, count: int = 1) -> str | list[str]:
        """Generate a persona-based system prompt."""
        if count == 1:
            return self._one_persona_prompt()
        return [self._one_persona_prompt() for _ in range(count)]

    def prompt_template(self, count: int = 1) -> str | list[str]:
        """Generate a parameterized prompt template with placeholders."""
        if count == 1:
            return self._one_prompt_template()
        return [self._one_prompt_template() for _ in range(count)]

    def few_shot_prompt(self, count: int = 1) -> str | list[str]:
        """Generate a few-shot prompt with example pairs."""
        if count == 1:
            return self._one_few_shot_prompt()
        return [self._one_few_shot_prompt() for _ in range(count)]
