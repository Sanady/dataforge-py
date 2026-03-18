"""MusicProvider — generates fake music-related data.

Includes genres, artists, album names, song titles, instruments,
record labels, and streaming service names.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from typing import Literal, overload

from dataforge.providers.base import BaseProvider

# ------------------------------------------------------------------
# Data tuples (immutable, module-level for zero per-call overhead)
# ------------------------------------------------------------------

_GENRES: tuple[str, ...] = (
    "Rock",
    "Pop",
    "Hip Hop",
    "R&B",
    "Jazz",
    "Classical",
    "Country",
    "Electronic",
    "Blues",
    "Reggae",
    "Metal",
    "Punk",
    "Folk",
    "Soul",
    "Funk",
    "Latin",
    "Indie",
    "Alternative",
    "K-Pop",
    "Afrobeats",
    "Dancehall",
    "Techno",
    "House",
    "Drum and Bass",
    "Ambient",
    "Gospel",
    "Ska",
    "Grunge",
    "Trap",
    "Lo-fi",
)

_ARTISTS: tuple[str, ...] = (
    "The Midnight Echo",
    "Silver Horizon",
    "Luna Vex",
    "Crimson Tide",
    "Electric Phantom",
    "Neon Rivers",
    "Glass Animals",
    "Velvet Storm",
    "Arctic Pulse",
    "Solar Flare",
    "Crystal Method",
    "Iron Butterfly",
    "Shadow Creek",
    "Desert Rose",
    "Ocean Drive",
    "Mountain Echo",
    "Starlight Express",
    "Thunderbolt",
    "Phoenix Rising",
    "Midnight Sun",
    "Blue Velvet",
    "Golden Hour",
    "Platinum Waves",
    "Diamond Cut",
    "Ruby Red",
    "Emerald City",
    "Sapphire Sky",
    "Amber Glow",
    "Ivory Tower",
    "Obsidian Edge",
    "Coral Reef",
    "Jade Garden",
    "Opal Dreams",
    "Topaz Fire",
    "Onyx Knight",
    "Pearl Harbor",
    "Quartz Crystal",
    "Garnet Stone",
    "Turquoise Bay",
    "Cobalt Blue",
)

_ALBUM_ADJECTIVES: tuple[str, ...] = (
    "Eternal",
    "Midnight",
    "Golden",
    "Electric",
    "Silent",
    "Infinite",
    "Broken",
    "Dark",
    "Bright",
    "Lost",
    "Wild",
    "Sacred",
    "Neon",
    "Velvet",
    "Crystal",
    "Frozen",
    "Burning",
    "Rising",
    "Falling",
    "Hidden",
)

_ALBUM_NOUNS: tuple[str, ...] = (
    "Dreams",
    "Echoes",
    "Shadows",
    "Horizons",
    "Reflections",
    "Memories",
    "Whispers",
    "Storms",
    "Visions",
    "Chapters",
    "Roads",
    "Rivers",
    "Mountains",
    "Stars",
    "Flames",
    "Waves",
    "Gardens",
    "Kingdoms",
    "Journeys",
    "Nights",
)

_SONG_STARTERS: tuple[str, ...] = (
    "Dancing in the",
    "Lost in",
    "Running through",
    "Waiting for",
    "Dreaming of",
    "Under the",
    "Beyond the",
    "Into the",
    "Through the",
    "Before the",
    "After the",
    "Across the",
    "Behind the",
    "Above the",
    "Between the",
)

_SONG_ENDINGS: tuple[str, ...] = (
    "Moonlight",
    "Rain",
    "Fire",
    "Dark",
    "Light",
    "Storm",
    "Silence",
    "Night",
    "Dawn",
    "Sunset",
    "Stars",
    "Ocean",
    "Sky",
    "Wind",
    "Thunder",
)

_INSTRUMENTS: tuple[str, ...] = (
    "Guitar",
    "Piano",
    "Drums",
    "Bass",
    "Violin",
    "Cello",
    "Saxophone",
    "Trumpet",
    "Flute",
    "Clarinet",
    "Harp",
    "Trombone",
    "Oboe",
    "Banjo",
    "Ukulele",
    "Mandolin",
    "Harmonica",
    "Accordion",
    "Synthesizer",
    "Organ",
    "Tabla",
    "Sitar",
    "Didgeridoo",
    "Bagpipes",
    "Timpani",
    "Xylophone",
    "Marimba",
    "Vibraphone",
    "French Horn",
    "Tuba",
)

_RECORD_LABELS: tuple[str, ...] = (
    "Universal Music",
    "Sony Music",
    "Warner Music",
    "EMI Records",
    "Columbia Records",
    "Atlantic Records",
    "Interscope Records",
    "Def Jam Recordings",
    "Island Records",
    "Capitol Records",
    "Motown Records",
    "Sub Pop",
    "4AD",
    "Rough Trade",
    "Domino Recording",
    "XL Recordings",
    "Warp Records",
    "Merge Records",
    "Matador Records",
    "Beggars Banquet",
)

_STREAMING_SERVICES: tuple[str, ...] = (
    "Spotify",
    "Apple Music",
    "YouTube Music",
    "Amazon Music",
    "Tidal",
    "Deezer",
    "SoundCloud",
    "Pandora",
    "iHeartRadio",
    "Audiomack",
    "Bandcamp",
    "Napster",
    "Qobuz",
    "Anghami",
    "JioSaavn",
)


class MusicProvider(BaseProvider):
    """Generates fake music-related data.

    This provider is locale-independent.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "music"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "music_genre": "genre",
        "genre": "genre",
        "artist": "artist",
        "artist_name": "artist",
        "album": "album",
        "album_name": "album",
        "song": "song",
        "song_title": "song",
        "instrument": "instrument",
        "record_label": "record_label",
        "streaming_service": "streaming_service",
        "music_duration": "duration",
        "bpm": "bpm",
    }

    # ------------------------------------------------------------------
    # Scalar helpers
    # ------------------------------------------------------------------

    def _one_album(self) -> str:
        """Generate a single album name."""
        choice = self._engine._rng.choice
        return f"{choice(_ALBUM_ADJECTIVES)} {choice(_ALBUM_NOUNS)}"

    def _one_song(self) -> str:
        """Generate a single song title."""
        choice = self._engine._rng.choice
        return f"{choice(_SONG_STARTERS)} {choice(_SONG_ENDINGS)}"

    def _one_duration(self) -> str:
        """Generate a single track duration string (M:SS)."""
        ri = self._engine.random_int
        minutes = ri(1, 8)
        seconds = ri(0, 59)
        return f"{minutes}:{seconds:02d}"

    def _one_bpm(self) -> str:
        """Generate a single BPM value as string."""
        return str(self._engine.random_int(60, 200))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @overload
    def genre(self) -> str: ...
    @overload
    def genre(self, count: Literal[1]) -> str: ...
    @overload
    def genre(self, count: int) -> str | list[str]: ...
    def genre(self, count: int = 1) -> str | list[str]:
        """Generate a music genre (e.g. ``"Jazz"``).

        Parameters
        ----------
        count : int
            Number of genres to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_GENRES)
        return self._engine.choices(_GENRES, count)

    @overload
    def artist(self) -> str: ...
    @overload
    def artist(self, count: Literal[1]) -> str: ...
    @overload
    def artist(self, count: int) -> str | list[str]: ...
    def artist(self, count: int = 1) -> str | list[str]:
        """Generate an artist name (e.g. ``"Silver Horizon"``).

        Parameters
        ----------
        count : int
            Number of artist names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_ARTISTS)
        return self._engine.choices(_ARTISTS, count)

    @overload
    def album(self) -> str: ...
    @overload
    def album(self, count: Literal[1]) -> str: ...
    @overload
    def album(self, count: int) -> str | list[str]: ...
    def album(self, count: int = 1) -> str | list[str]:
        """Generate an album name (e.g. ``"Eternal Dreams"``).

        Parameters
        ----------
        count : int
            Number of album names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_album()
        return [self._one_album() for _ in range(count)]

    @overload
    def song(self) -> str: ...
    @overload
    def song(self, count: Literal[1]) -> str: ...
    @overload
    def song(self, count: int) -> str | list[str]: ...
    def song(self, count: int = 1) -> str | list[str]:
        """Generate a song title (e.g. ``"Dancing in the Moonlight"``).

        Parameters
        ----------
        count : int
            Number of song titles to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_song()
        return [self._one_song() for _ in range(count)]

    @overload
    def instrument(self) -> str: ...
    @overload
    def instrument(self, count: Literal[1]) -> str: ...
    @overload
    def instrument(self, count: int) -> str | list[str]: ...
    def instrument(self, count: int = 1) -> str | list[str]:
        """Generate an instrument name (e.g. ``"Guitar"``).

        Parameters
        ----------
        count : int
            Number of instruments to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_INSTRUMENTS)
        return self._engine.choices(_INSTRUMENTS, count)

    @overload
    def record_label(self) -> str: ...
    @overload
    def record_label(self, count: Literal[1]) -> str: ...
    @overload
    def record_label(self, count: int) -> str | list[str]: ...
    def record_label(self, count: int = 1) -> str | list[str]:
        """Generate a record label name (e.g. ``"Atlantic Records"``).

        Parameters
        ----------
        count : int
            Number of record label names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_RECORD_LABELS)
        return self._engine.choices(_RECORD_LABELS, count)

    @overload
    def streaming_service(self) -> str: ...
    @overload
    def streaming_service(self, count: Literal[1]) -> str: ...
    @overload
    def streaming_service(self, count: int) -> str | list[str]: ...
    def streaming_service(self, count: int = 1) -> str | list[str]:
        """Generate a music streaming service name (e.g. ``"Spotify"``).

        Parameters
        ----------
        count : int
            Number of streaming service names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_STREAMING_SERVICES)
        return self._engine.choices(_STREAMING_SERVICES, count)

    @overload
    def duration(self) -> str: ...
    @overload
    def duration(self, count: Literal[1]) -> str: ...
    @overload
    def duration(self, count: int) -> str | list[str]: ...
    def duration(self, count: int = 1) -> str | list[str]:
        """Generate a track duration (e.g. ``"3:42"``).

        Parameters
        ----------
        count : int
            Number of durations to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_duration()
        return [self._one_duration() for _ in range(count)]

    @overload
    def bpm(self) -> str: ...
    @overload
    def bpm(self, count: Literal[1]) -> str: ...
    @overload
    def bpm(self, count: int) -> str | list[str]: ...
    def bpm(self, count: int = 1) -> str | list[str]:
        """Generate a BPM value as string (e.g. ``"128"``).

        Parameters
        ----------
        count : int
            Number of BPM values to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_bpm()
        return [self._one_bpm() for _ in range(count)]
