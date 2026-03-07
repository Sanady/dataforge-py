"""SportsProvider — generates fake sports-related data.

Includes sport names, teams, leagues, positions, venues,
events, and athlete names.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from typing import Literal, overload

from dataforge.providers.base import BaseProvider

# ------------------------------------------------------------------
# Data tuples (immutable, module-level for zero per-call overhead)
# ------------------------------------------------------------------

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
    "Skateboarding",
    "Track and Field",
    "Gymnastics",
    "Fencing",
    "Rowing",
    "Archery",
    "Handball",
    "Water Polo",
    "Lacrosse",
    "Motorsport",
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
    "Royals",
    "Kings",
    "Generals",
    "Mustangs",
    "Cougars",
    "Jaguars",
    "Dolphins",
    "Stallions",
    "Falcons",
    "Cobras",
    "Phoenix",
    "Vipers",
    "Raptors",
    "Hornets",
    "Bulldogs",
    "Grizzlies",
    "Wildcats",
    "Rams",
    "Chargers",
    "Raiders",
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
    "A-League",
    "J-League",
    "K League",
    "Eredivisie",
    "Primeira Liga",
    "Scottish Premiership",
    "Super Rugby",
    "Six Nations",
    "BBL",
    "CPL",
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
    "Fly-half",
    "Hooker",
    "Lock",
    "Prop",
    "Scrum-half",
    "Setter",
    "Libero",
    "Outside Hitter",
    "Goaltender",
    "Defenseman",
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
    "SoFi Stadium",
    "Levi's Stadium",
    "Lincoln Financial Field",
    "Hard Rock Stadium",
    "Allegiant Stadium",
    "State Farm Arena",
    "Barclays Center",
    "Chase Center",
    "United Center",
    "TD Garden",
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
    "Cricket World Cup",
    "FIFA Club World Cup",
    "UEFA Euro",
    "Copa America",
    "African Cup of Nations",
    "Ryder Cup",
    "All-Star Game",
    "Pro Bowl",
    "X Games",
    "Ironman Triathlon",
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
    "Sofia",
    "Emma",
    "Mia",
    "Serena",
    "Venus",
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
    "Thompson",
    "White",
    "Harris",
    "Clark",
    "Robinson",
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

    # ------------------------------------------------------------------
    # Scalar helpers
    # ------------------------------------------------------------------

    def _one_athlete(self) -> str:
        """Generate a single athlete name."""
        choice = self._engine._rng.choice
        return f"{choice(_ATHLETE_FIRST)} {choice(_ATHLETE_LAST)}"

    def _one_score(self) -> str:
        """Generate a single game score."""
        ri = self._engine.random_int
        return f"{ri(0, 120)}-{ri(0, 120)}"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @overload
    def sport(self) -> str: ...
    @overload
    def sport(self, count: Literal[1]) -> str: ...
    @overload
    def sport(self, count: int) -> str | list[str]: ...
    def sport(self, count: int = 1) -> str | list[str]:
        """Generate a sport name (e.g. ``"Basketball"``).

        Parameters
        ----------
        count : int
            Number of sport names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_SPORTS)
        return self._engine.choices(_SPORTS, count)

    @overload
    def team(self) -> str: ...
    @overload
    def team(self, count: Literal[1]) -> str: ...
    @overload
    def team(self, count: int) -> str | list[str]: ...
    def team(self, count: int = 1) -> str | list[str]:
        """Generate a team name (e.g. ``"Eagles"``).

        Parameters
        ----------
        count : int
            Number of team names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_TEAMS)
        return self._engine.choices(_TEAMS, count)

    @overload
    def league(self) -> str: ...
    @overload
    def league(self, count: Literal[1]) -> str: ...
    @overload
    def league(self, count: int) -> str | list[str]: ...
    def league(self, count: int = 1) -> str | list[str]:
        """Generate a league name (e.g. ``"NBA"``).

        Parameters
        ----------
        count : int
            Number of league names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_LEAGUES)
        return self._engine.choices(_LEAGUES, count)

    @overload
    def position(self) -> str: ...
    @overload
    def position(self, count: Literal[1]) -> str: ...
    @overload
    def position(self, count: int) -> str | list[str]: ...
    def position(self, count: int = 1) -> str | list[str]:
        """Generate a sports position (e.g. ``"Quarterback"``).

        Parameters
        ----------
        count : int
            Number of positions to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_POSITIONS)
        return self._engine.choices(_POSITIONS, count)

    @overload
    def venue(self) -> str: ...
    @overload
    def venue(self, count: Literal[1]) -> str: ...
    @overload
    def venue(self, count: int) -> str | list[str]: ...
    def venue(self, count: int = 1) -> str | list[str]:
        """Generate a sports venue name (e.g. ``"Wembley Stadium"``).

        Parameters
        ----------
        count : int
            Number of venue names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_VENUES)
        return self._engine.choices(_VENUES, count)

    @overload
    def event(self) -> str: ...
    @overload
    def event(self, count: Literal[1]) -> str: ...
    @overload
    def event(self, count: int) -> str | list[str]: ...
    def event(self, count: int = 1) -> str | list[str]:
        """Generate a sports event name (e.g. ``"Olympics"``).

        Parameters
        ----------
        count : int
            Number of event names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_EVENTS)
        return self._engine.choices(_EVENTS, count)

    @overload
    def athlete(self) -> str: ...
    @overload
    def athlete(self, count: Literal[1]) -> str: ...
    @overload
    def athlete(self, count: int) -> str | list[str]: ...
    def athlete(self, count: int = 1) -> str | list[str]:
        """Generate an athlete name (e.g. ``"Marcus Johnson"``).

        Parameters
        ----------
        count : int
            Number of athlete names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_athlete()
        return [self._one_athlete() for _ in range(count)]

    @overload
    def score(self) -> str: ...
    @overload
    def score(self, count: Literal[1]) -> str: ...
    @overload
    def score(self, count: int) -> str | list[str]: ...
    def score(self, count: int = 1) -> str | list[str]:
        """Generate a game score (e.g. ``"24-17"``).

        Parameters
        ----------
        count : int
            Number of scores to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_score()
        return [self._one_score() for _ in range(count)]
