"""MusicProvider — generates fake music-related data.

Includes genres, artists, album names, song titles, instruments,
record labels, and streaming service names.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from dataforge.providers.base import BaseProvider

# Data tuples (immutable, module-level for zero per-call overhead)

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

    _choice_fields: dict[str, tuple[str, ...]] = {
        "genre": _GENRES,
        "artist": _ARTISTS,
        "instrument": _INSTRUMENTS,
        "record_label": _RECORD_LABELS,
        "streaming_service": _STREAMING_SERVICES,
    }

    # Scalar helpers

    def _one_album(self) -> str:
        choice = self._engine._rng.choice
        return f"{choice(_ALBUM_ADJECTIVES)} {choice(_ALBUM_NOUNS)}"

    def _one_song(self) -> str:
        choice = self._engine._rng.choice
        return f"{choice(_SONG_STARTERS)} {choice(_SONG_ENDINGS)}"

    def _one_duration(self) -> str:
        ri = self._engine.random_int
        return f"{ri(1, 8)}:{ri(0, 59):02d}"

    def _one_bpm(self) -> str:
        return str(self._engine.random_int(60, 200))

    # Public API — custom methods

    def album(self, count: int = 1) -> str | list[str]:
        """Generate an album name (e.g. ``"Eternal Dreams"``)."""
        if count == 1:
            return self._one_album()
        return [self._one_album() for _ in range(count)]

    def song(self, count: int = 1) -> str | list[str]:
        """Generate a song title (e.g. ``"Dancing in the Moonlight"``)."""
        if count == 1:
            return self._one_song()
        return [self._one_song() for _ in range(count)]

    def duration(self, count: int = 1) -> str | list[str]:
        """Generate a track duration (e.g. ``"3:42"``)."""
        if count == 1:
            return self._one_duration()
        return [self._one_duration() for _ in range(count)]

    def bpm(self, count: int = 1) -> str | list[str]:
        """Generate a BPM value as string (e.g. ``"128"``)."""
        if count == 1:
            return self._one_bpm()
        return [self._one_bpm() for _ in range(count)]
