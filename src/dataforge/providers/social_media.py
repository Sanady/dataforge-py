"""SocialMediaProvider — generates fake social media data.

Includes platforms, usernames, hashtags, post types, reactions,
follower counts, and verification status.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from typing import Literal, overload

from dataforge.providers.base import BaseProvider

# ------------------------------------------------------------------
# Data tuples (immutable, module-level for zero per-call overhead)
# ------------------------------------------------------------------

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
    "WhatsApp",
    "Telegram",
    "Signal",
    "WeChat",
    "Line",
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
    "100",
    "thumbs_up",
    "thumbs_down",
    "laugh",
    "cry",
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
    "style",
    "smile",
    "food",
    "travel",
    "fitness",
    "music",
    "beauty",
    "photo",
    "life",
    "motivation",
    "family",
    "nofilter",
    "makeup",
    "ootd",
    "dog",
    "explore",
    "viral",
    "trending",
    "fyp",
    "goals",
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
    "red",
    "green",
    "black",
    "white",
    "silver",
    "cyber",
    "pixel",
    "quantum",
    "shadow",
    "storm",
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
    "frost",
    "ghost",
    "raven",
    "viper",
    "cobra",
    "eagle",
    "panda",
    "shark",
    "whale",
    "falcon",
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
    "What do you think?",
    "Swipe for more!",
    "Follow for more content like this",
    "Drop a comment below!",
    "Story time...",
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

    # ------------------------------------------------------------------
    # Scalar helpers
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @overload
    def platform(self) -> str: ...
    @overload
    def platform(self, count: Literal[1]) -> str: ...
    @overload
    def platform(self, count: int) -> str | list[str]: ...
    def platform(self, count: int = 1) -> str | list[str]:
        """Generate a social media platform name (e.g. ``"Instagram"``).

        Parameters
        ----------
        count : int
            Number of items to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_PLATFORMS)
        return self._engine.choices(_PLATFORMS, count)

    @overload
    def username(self) -> str: ...
    @overload
    def username(self, count: Literal[1]) -> str: ...
    @overload
    def username(self, count: int) -> str | list[str]: ...
    def username(self, count: int = 1) -> str | list[str]:
        """Generate a social media username (e.g. ``"cool_wolf42"``).

        Parameters
        ----------
        count : int
            Number of items to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_username()
        return [self._one_username() for _ in range(count)]

    @overload
    def hashtag(self) -> str: ...
    @overload
    def hashtag(self, count: Literal[1]) -> str: ...
    @overload
    def hashtag(self, count: int) -> str | list[str]: ...
    def hashtag(self, count: int = 1) -> str | list[str]:
        """Generate a hashtag (e.g. ``"#trending"``).

        Parameters
        ----------
        count : int
            Number of items to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_hashtag()
        return [self._one_hashtag() for _ in range(count)]

    @overload
    def post_type(self) -> str: ...
    @overload
    def post_type(self, count: Literal[1]) -> str: ...
    @overload
    def post_type(self, count: int) -> str | list[str]: ...
    def post_type(self, count: int = 1) -> str | list[str]:
        """Generate a social media post type (e.g. ``"reel"``).

        Parameters
        ----------
        count : int
            Number of items to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_POST_TYPES)
        return self._engine.choices(_POST_TYPES, count)

    @overload
    def reaction(self) -> str: ...
    @overload
    def reaction(self, count: Literal[1]) -> str: ...
    @overload
    def reaction(self, count: int) -> str | list[str]: ...
    def reaction(self, count: int = 1) -> str | list[str]:
        """Generate a social media reaction (e.g. ``"love"``).

        Parameters
        ----------
        count : int
            Number of items to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_REACTIONS)
        return self._engine.choices(_REACTIONS, count)

    @overload
    def follower_count(self) -> str: ...
    @overload
    def follower_count(self, count: Literal[1]) -> str: ...
    @overload
    def follower_count(self, count: int) -> str | list[str]: ...
    def follower_count(self, count: int = 1) -> str | list[str]:
        """Generate a formatted follower count (e.g. ``"1.2M"``).

        Parameters
        ----------
        count : int
            Number of items to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_follower_count()
        return [self._one_follower_count() for _ in range(count)]

    @overload
    def content(self) -> str: ...
    @overload
    def content(self, count: Literal[1]) -> str: ...
    @overload
    def content(self, count: int) -> str | list[str]: ...
    def content(self, count: int = 1) -> str | list[str]:
        """Generate social media post content.

        Parameters
        ----------
        count : int
            Number of items to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_CONTENT_SNIPPETS)
        return self._engine.choices(_CONTENT_SNIPPETS, count)

    @overload
    def verified(self) -> str: ...
    @overload
    def verified(self, count: Literal[1]) -> str: ...
    @overload
    def verified(self, count: int) -> str | list[str]: ...
    def verified(self, count: int = 1) -> str | list[str]:
        """Generate a verification status (``"verified"`` or ``"unverified"``).

        Parameters
        ----------
        count : int
            Number of items to generate.

        Returns
        -------
        str or list[str]
        """
        _choices = ("verified", "unverified")
        if count == 1:
            return self._engine.choice(_choices)
        return self._engine.choices(_choices, count)
