"""SocialMediaProvider — generates fake social media data.

Includes platforms, usernames, hashtags, post types, reactions,
follower counts, and verification status.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from dataforge.providers.base import BaseProvider

# Data tuples (immutable, module-level for zero per-call overhead)

_PLATFORMS: tuple[str, ...] = (
    "Twitter",
    "Instagram",
    "Facebook",
    "TikTok",
    "YouTube",
    "LinkedIn",
    "Snapchat",
    "Reddit",
    "Pinterest",
    "Threads",
    "Mastodon",
    "Bluesky",
    "Discord",
    "Twitch",
    "Tumblr",
)

_POST_TYPES: tuple[str, ...] = (
    "text",
    "image",
    "video",
    "story",
    "reel",
    "poll",
    "live",
    "carousel",
    "thread",
    "article",
    "short",
    "link",
    "quote",
    "repost",
    "audio",
)

_REACTIONS: tuple[str, ...] = (
    "like",
    "love",
    "haha",
    "wow",
    "sad",
    "angry",
    "care",
    "celebrate",
    "insightful",
    "curious",
    "upvote",
    "downvote",
    "heart",
    "fire",
    "clap",
)

_HASHTAG_WORDS: tuple[str, ...] = (
    "love",
    "instagood",
    "photooftheday",
    "fashion",
    "beautiful",
    "happy",
    "cute",
    "tbt",
    "like4like",
    "followme",
    "picoftheday",
    "selfie",
    "summer",
    "art",
    "instadaily",
    "friends",
    "repost",
    "nature",
    "girl",
    "fun",
)

_USERNAME_ADJECTIVES: tuple[str, ...] = (
    "cool",
    "dark",
    "epic",
    "fast",
    "gold",
    "hot",
    "icy",
    "keen",
    "lit",
    "mega",
    "neo",
    "pro",
    "rad",
    "sky",
    "top",
    "ultra",
    "vibe",
    "wild",
    "zen",
    "blue",
)

_USERNAME_NOUNS: tuple[str, ...] = (
    "wolf",
    "fox",
    "hawk",
    "bear",
    "lion",
    "tiger",
    "dragon",
    "phoenix",
    "ninja",
    "knight",
    "guru",
    "king",
    "queen",
    "ace",
    "star",
    "boss",
    "chief",
    "sage",
    "spark",
    "blaze",
)

_CONTENT_SNIPPETS: tuple[str, ...] = (
    "Just had the best day ever!",
    "Can't believe this happened...",
    "New post who dis",
    "Living my best life",
    "Check out this amazing view!",
    "Monday motivation",
    "Weekend vibes only",
    "Grateful for everything",
    "This is everything",
    "No caption needed",
    "Thoughts?",
    "Big announcement coming soon!",
    "Throwback to this moment",
    "Tag someone who needs to see this",
    "Link in bio!",
)


class SocialMediaProvider(BaseProvider):
    """Generates fake social media data.

    This provider is locale-independent.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "social_media"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "social_platform": "platform",
        "platform": "platform",
        "social_username": "username",
        "hashtag": "hashtag",
        "post_type": "post_type",
        "social_reaction": "reaction",
        "reaction": "reaction",
        "follower_count": "follower_count",
        "social_content": "content",
        "verified": "verified",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "platform": _PLATFORMS,
        "post_type": _POST_TYPES,
        "reaction": _REACTIONS,
        "content": _CONTENT_SNIPPETS,
    }

    # Scalar helpers

    def _one_username(self) -> str:
        choice = self._engine._rng.choice
        adj = choice(_USERNAME_ADJECTIVES)
        noun = choice(_USERNAME_NOUNS)
        num = self._engine.random_int(1, 999)
        return f"{adj}_{noun}{num}"

    def _one_hashtag(self) -> str:
        return "#" + self._engine._rng.choice(_HASHTAG_WORDS)

    def _one_follower_count(self) -> str:
        n = self._engine.random_int(0, 10_000_000)
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f}M"
        if n >= 1_000:
            return f"{n / 1_000:.1f}K"
        return str(n)

    # Public API — custom methods

    def username(self, count: int = 1) -> str | list[str]:
        """Generate a social media username (e.g. ``"cool_wolf42"``)."""
        if count == 1:
            return self._one_username()
        return [self._one_username() for _ in range(count)]

    def hashtag(self, count: int = 1) -> str | list[str]:
        """Generate a hashtag (e.g. ``"#trending"``)."""
        if count == 1:
            return self._one_hashtag()
        return [self._one_hashtag() for _ in range(count)]

    def follower_count(self, count: int = 1) -> str | list[str]:
        """Generate a formatted follower count (e.g. ``"1.2M"``)."""
        if count == 1:
            return self._one_follower_count()
        return [self._one_follower_count() for _ in range(count)]

    def verified(self, count: int = 1) -> str | list[str]:
        """Generate a verification status (``"verified"`` or ``"unverified"``)."""
        _choices = ("verified", "unverified")
        if count == 1:
            return self._engine.choice(_choices)
        return self._engine.choices(_choices, count)
