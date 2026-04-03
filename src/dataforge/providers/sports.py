"""SportsProvider — generates fake sports-related data.

Includes sport names, teams, leagues, positions, venues,
events, and athlete names.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from dataforge.providers.base import BaseProvider

# Data tuples (immutable, module-level for zero per-call overhead)

_SPORTS: tuple[str, ...] = (
    "Football",
    "Basketball",
    "Baseball",
    "Soccer",
    "Tennis",
    "Golf",
    "Hockey",
    "Rugby",
    "Cricket",
    "Swimming",
    "Boxing",
    "MMA",
    "Volleyball",
    "Table Tennis",
    "Badminton",
    "Wrestling",
    "Cycling",
    "Skiing",
    "Snowboarding",
    "Surfing",
)

_TEAMS: tuple[str, ...] = (
    "Eagles",
    "Lions",
    "Tigers",
    "Bears",
    "Wolves",
    "Hawks",
    "Panthers",
    "Sharks",
    "Warriors",
    "Knights",
    "Dragons",
    "Thunder",
    "Lightning",
    "Storm",
    "Blazers",
    "Rockets",
    "Titans",
    "Vikings",
    "Spartans",
    "Rebels",
)

_LEAGUES: tuple[str, ...] = (
    "NFL",
    "NBA",
    "MLB",
    "NHL",
    "MLS",
    "Premier League",
    "La Liga",
    "Bundesliga",
    "Serie A",
    "Ligue 1",
    "Champions League",
    "UEFA Europa League",
    "IPL",
    "ATP Tour",
    "WTA Tour",
    "PGA Tour",
    "Formula 1",
    "NASCAR",
    "UFC",
    "WWE",
)

_POSITIONS: tuple[str, ...] = (
    "Quarterback",
    "Running Back",
    "Wide Receiver",
    "Tight End",
    "Linebacker",
    "Point Guard",
    "Shooting Guard",
    "Small Forward",
    "Power Forward",
    "Center",
    "Pitcher",
    "Catcher",
    "Shortstop",
    "Goalkeeper",
    "Defender",
    "Midfielder",
    "Forward",
    "Striker",
    "Winger",
    "Flanker",
)

_VENUES: tuple[str, ...] = (
    "Madison Square Garden",
    "Wembley Stadium",
    "Camp Nou",
    "Santiago Bernabeu",
    "Old Trafford",
    "Melbourne Cricket Ground",
    "Yankee Stadium",
    "Lambeau Field",
    "Staples Center",
    "AT&T Stadium",
    "Rose Bowl",
    "Maracana",
    "Signal Iduna Park",
    "San Siro",
    "Allianz Arena",
    "Emirates Stadium",
    "Anfield",
    "Fenway Park",
    "Oracle Park",
    "MetLife Stadium",
)

_EVENTS: tuple[str, ...] = (
    "Olympics",
    "World Cup",
    "Super Bowl",
    "World Series",
    "Stanley Cup",
    "NBA Finals",
    "Champions League Final",
    "Wimbledon",
    "US Open",
    "French Open",
    "Australian Open",
    "The Masters",
    "Tour de France",
    "Daytona 500",
    "Kentucky Derby",
    "March Madness",
    "Commonwealth Games",
    "Pan American Games",
    "Asian Games",
    "Rugby World Cup",
)

_ATHLETE_FIRST: tuple[str, ...] = (
    "James",
    "Michael",
    "David",
    "Alex",
    "Chris",
    "Marcus",
    "Tyler",
    "Brandon",
    "Jordan",
    "Andre",
    "Carlos",
    "Daniel",
    "Eric",
    "Kevin",
    "Ryan",
)

_ATHLETE_LAST: tuple[str, ...] = (
    "Johnson",
    "Williams",
    "Smith",
    "Brown",
    "Davis",
    "Garcia",
    "Martinez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
)


class SportsProvider(BaseProvider):
    """Generates fake sports-related data.

    This provider is locale-independent.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "sports"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "sport": "sport",
        "sport_name": "sport",
        "team": "team",
        "team_name": "team",
        "league": "league",
        "league_name": "league",
        "sport_position": "position",
        "venue": "venue",
        "stadium": "venue",
        "sport_event": "event",
        "athlete": "athlete",
        "athlete_name": "athlete",
        "score": "score",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "sport": _SPORTS,
        "team": _TEAMS,
        "league": _LEAGUES,
        "position": _POSITIONS,
        "venue": _VENUES,
        "event": _EVENTS,
    }

    # Scalar helpers

    def _one_athlete(self) -> str:
        choice = self._engine._rng.choice
        return f"{choice(_ATHLETE_FIRST)} {choice(_ATHLETE_LAST)}"

    def _one_score(self) -> str:
        ri = self._engine.random_int
        return f"{ri(0, 120)}-{ri(0, 120)}"

    # Public API — custom methods

    def athlete(self, count: int = 1) -> str | list[str]:
        """Generate an athlete name (e.g. ``"Marcus Johnson"``)."""
        if count == 1:
            return self._one_athlete()
        return [self._one_athlete() for _ in range(count)]

    def score(self, count: int = 1) -> str | list[str]:
        """Generate a game score (e.g. ``"24-17"``)."""
        if count == 1:
            return self._one_score()
        return [self._one_score() for _ in range(count)]
