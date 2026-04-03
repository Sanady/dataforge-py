"""Geographic correlation data — city/state/country/zip mappings.

Used by the constraint engine to generate geographically consistent
addresses where ``city`` depends on ``country``, ``state`` depends
on ``country``, ``zipcode`` depends on ``state``, etc.

Data is stored as immutable tuples for minimal memory and maximum
``random.choice`` speed.
"""

from __future__ import annotations

# Country → States mapping

COUNTRY_STATES: dict[str, tuple[str, ...]] = {
    "United States": (
        "Alabama",
        "Alaska",
        "Arizona",
        "Arkansas",
        "California",
        "Colorado",
        "Connecticut",
        "Delaware",
        "Florida",
        "Georgia",
        "Hawaii",
        "Idaho",
        "Illinois",
        "Indiana",
        "Iowa",
        "Kansas",
        "Kentucky",
        "Louisiana",
        "Maine",
        "Maryland",
        "Massachusetts",
        "Michigan",
        "Minnesota",
        "Mississippi",
        "Missouri",
        "Montana",
        "Nebraska",
        "Nevada",
        "New Hampshire",
        "New Jersey",
        "New Mexico",
        "New York",
        "North Carolina",
        "North Dakota",
        "Ohio",
        "Oklahoma",
        "Oregon",
        "Pennsylvania",
        "Rhode Island",
        "South Carolina",
        "South Dakota",
        "Tennessee",
        "Texas",
        "Utah",
        "Vermont",
        "Virginia",
        "Washington",
        "West Virginia",
        "Wisconsin",
        "Wyoming",
    ),
    "Canada": (
        "Alberta",
        "British Columbia",
        "Manitoba",
        "New Brunswick",
        "Newfoundland and Labrador",
        "Nova Scotia",
        "Ontario",
        "Prince Edward Island",
        "Quebec",
        "Saskatchewan",
    ),
    "United Kingdom": (
        "England",
        "Scotland",
        "Wales",
        "Northern Ireland",
    ),
    "Germany": (
        "Baden-Württemberg",
        "Bavaria",
        "Berlin",
        "Brandenburg",
        "Bremen",
        "Hamburg",
        "Hesse",
        "Lower Saxony",
        "Mecklenburg-Vorpommern",
        "North Rhine-Westphalia",
        "Rhineland-Palatinate",
        "Saarland",
        "Saxony",
        "Saxony-Anhalt",
        "Schleswig-Holstein",
        "Thuringia",
    ),
    "France": (
        "Île-de-France",
        "Provence-Alpes-Côte d'Azur",
        "Auvergne-Rhône-Alpes",
        "Occitanie",
        "Nouvelle-Aquitaine",
        "Hauts-de-France",
        "Grand Est",
        "Pays de la Loire",
        "Brittany",
        "Normandy",
    ),
    "Australia": (
        "New South Wales",
        "Victoria",
        "Queensland",
        "Western Australia",
        "South Australia",
        "Tasmania",
        "Australian Capital Territory",
        "Northern Territory",
    ),
    "Japan": (
        "Tokyo",
        "Osaka",
        "Kanagawa",
        "Aichi",
        "Hokkaido",
        "Fukuoka",
        "Saitama",
        "Chiba",
        "Hyogo",
        "Kyoto",
    ),
    "Brazil": (
        "São Paulo",
        "Rio de Janeiro",
        "Minas Gerais",
        "Bahia",
        "Paraná",
        "Rio Grande do Sul",
        "Pernambuco",
        "Ceará",
    ),
    "India": (
        "Maharashtra",
        "Karnataka",
        "Tamil Nadu",
        "Uttar Pradesh",
        "Gujarat",
        "Rajasthan",
        "West Bengal",
        "Telangana",
        "Kerala",
        "Delhi",
    ),
    "Mexico": (
        "Mexico City",
        "Jalisco",
        "Nuevo León",
        "Puebla",
        "Guanajuato",
        "Chihuahua",
        "Veracruz",
        "Yucatán",
    ),
}

# State → Cities mapping (representative cities per state)

STATE_CITIES: dict[str, tuple[str, ...]] = {
    # United States
    "California": (
        "Los Angeles",
        "San Francisco",
        "San Diego",
        "San Jose",
        "Sacramento",
    ),
    "New York": ("New York City", "Buffalo", "Rochester", "Albany", "Syracuse"),
    "Texas": ("Houston", "Dallas", "Austin", "San Antonio", "Fort Worth"),
    "Florida": ("Miami", "Orlando", "Tampa", "Jacksonville", "Fort Lauderdale"),
    "Illinois": ("Chicago", "Aurora", "Naperville", "Rockford", "Springfield"),
    "Pennsylvania": ("Philadelphia", "Pittsburgh", "Allentown", "Erie", "Harrisburg"),
    "Ohio": ("Columbus", "Cleveland", "Cincinnati", "Toledo", "Dayton"),
    "Georgia": ("Atlanta", "Savannah", "Augusta", "Macon", "Athens"),
    "Michigan": ("Detroit", "Grand Rapids", "Ann Arbor", "Lansing", "Flint"),
    "Washington": ("Seattle", "Tacoma", "Spokane", "Bellevue", "Olympia"),
    "Massachusetts": ("Boston", "Worcester", "Cambridge", "Springfield", "Lowell"),
    "Colorado": ("Denver", "Colorado Springs", "Aurora", "Boulder", "Fort Collins"),
    "Arizona": ("Phoenix", "Tucson", "Mesa", "Scottsdale", "Tempe"),
    "Oregon": ("Portland", "Salem", "Eugene", "Bend", "Corvallis"),
    "Nevada": ("Las Vegas", "Reno", "Henderson", "Carson City", "Sparks"),
    "Virginia": ("Virginia Beach", "Norfolk", "Richmond", "Arlington", "Alexandria"),
    "North Carolina": ("Charlotte", "Raleigh", "Durham", "Greensboro", "Asheville"),
    "Tennessee": ("Nashville", "Memphis", "Knoxville", "Chattanooga", "Murfreesboro"),
    "Indiana": ("Indianapolis", "Fort Wayne", "Bloomington", "Evansville", "Carmel"),
    "Missouri": (
        "Kansas City",
        "St. Louis",
        "Springfield",
        "Columbia",
        "Jefferson City",
    ),
    # Canada
    "Ontario": ("Toronto", "Ottawa", "Hamilton", "Mississauga", "London"),
    "Quebec": ("Montreal", "Quebec City", "Laval", "Gatineau", "Sherbrooke"),
    "British Columbia": ("Vancouver", "Victoria", "Surrey", "Burnaby", "Kelowna"),
    "Alberta": ("Calgary", "Edmonton", "Red Deer", "Lethbridge", "Medicine Hat"),
    # UK
    "England": ("London", "Manchester", "Birmingham", "Liverpool", "Leeds"),
    "Scotland": ("Edinburgh", "Glasgow", "Aberdeen", "Dundee", "Inverness"),
    "Wales": ("Cardiff", "Swansea", "Newport", "Bangor", "Wrexham"),
    "Northern Ireland": ("Belfast", "Derry", "Lisburn", "Newry", "Bangor"),
    # Germany
    "Bavaria": ("Munich", "Nuremberg", "Augsburg", "Regensburg", "Ingolstadt"),
    "Berlin": ("Berlin",),
    "Hamburg": ("Hamburg",),
    "North Rhine-Westphalia": ("Cologne", "Düsseldorf", "Dortmund", "Essen", "Bonn"),
    "Hesse": ("Frankfurt", "Wiesbaden", "Kassel", "Darmstadt", "Offenbach"),
    "Baden-Württemberg": (
        "Stuttgart",
        "Karlsruhe",
        "Mannheim",
        "Freiburg",
        "Heidelberg",
    ),
    # France
    "Île-de-France": ("Paris", "Versailles", "Boulogne-Billancourt", "Saint-Denis"),
    "Provence-Alpes-Côte d'Azur": ("Marseille", "Nice", "Toulon", "Aix-en-Provence"),
    "Auvergne-Rhône-Alpes": ("Lyon", "Grenoble", "Saint-Étienne", "Clermont-Ferrand"),
    # Australia
    "New South Wales": ("Sydney", "Newcastle", "Wollongong", "Central Coast"),
    "Victoria": ("Melbourne", "Geelong", "Ballarat", "Bendigo"),
    "Queensland": ("Brisbane", "Gold Coast", "Cairns", "Townsville"),
    # Japan
    "Tokyo": ("Shinjuku", "Shibuya", "Minato", "Chiyoda", "Setagaya"),
    "Osaka": ("Osaka", "Sakai", "Higashiosaka", "Suita"),
    # Brazil
    "São Paulo": ("São Paulo", "Campinas", "Santos", "Guarulhos"),
    "Rio de Janeiro": ("Rio de Janeiro", "Niterói", "Duque de Caxias"),
    # India
    "Maharashtra": ("Mumbai", "Pune", "Nagpur", "Thane", "Nashik"),
    "Karnataka": ("Bangalore", "Mysore", "Mangalore", "Hubli"),
    "Tamil Nadu": ("Chennai", "Coimbatore", "Madurai", "Salem"),
    "Delhi": ("New Delhi", "Delhi"),
    # Mexico
    "Mexico City": ("Mexico City",),
    "Jalisco": ("Guadalajara", "Zapopan", "Tlaquepaque"),
}

# State → Zip code ranges (US-style prefix ranges)

STATE_ZIP_PREFIX: dict[str, tuple[str, ...]] = {
    "California": (
        "900",
        "901",
        "902",
        "910",
        "911",
        "920",
        "921",
        "930",
        "935",
        "940",
        "950",
    ),
    "New York": ("100", "101", "102", "103", "110", "112", "114", "120", "130", "140"),
    "Texas": ("750", "751", "760", "770", "773", "780", "782", "786", "790", "797"),
    "Florida": ("320", "321", "326", "327", "328", "330", "331", "333", "340", "346"),
    "Illinois": ("600", "601", "602", "604", "606", "610", "613", "615", "618", "620"),
    "Pennsylvania": (
        "150",
        "151",
        "152",
        "160",
        "170",
        "175",
        "180",
        "190",
        "191",
        "194",
    ),
    "Ohio": ("430", "431", "432", "435", "440", "441", "443", "445", "450", "452"),
    "Georgia": ("300", "301", "302", "303", "305", "306", "310", "312", "316", "318"),
    "Michigan": ("480", "481", "482", "483", "484", "485", "486", "488", "490", "496"),
    "Washington": (
        "980",
        "981",
        "982",
        "983",
        "984",
        "985",
        "986",
        "988",
        "990",
        "992",
    ),
    "Massachusetts": (
        "010",
        "011",
        "012",
        "013",
        "014",
        "015",
        "016",
        "017",
        "018",
        "020",
    ),
    "Colorado": ("800", "801", "802", "803", "804", "805", "806", "808", "809", "810"),
}

# Country → Phone format

COUNTRY_PHONE_FORMAT: dict[str, str] = {
    "United States": "+1-###-###-####",
    "Canada": "+1-###-###-####",
    "United Kingdom": "+44-####-######",
    "Germany": "+49-###-########",
    "France": "+33-#-##-##-##-##",
    "Australia": "+61-#-####-####",
    "Japan": "+81-##-####-####",
    "Brazil": "+55-##-#####-####",
    "India": "+91-#####-#####",
    "Mexico": "+52-##-####-####",
}

# Country → Currency

COUNTRY_CURRENCY: dict[str, str] = {
    "United States": "USD",
    "Canada": "CAD",
    "United Kingdom": "GBP",
    "Germany": "EUR",
    "France": "EUR",
    "Australia": "AUD",
    "Japan": "JPY",
    "Brazil": "BRL",
    "India": "INR",
    "Mexico": "MXN",
}

# All available countries
ALL_COUNTRIES: tuple[str, ...] = tuple(COUNTRY_STATES.keys())

# Default city fallback for unmapped states
_DEFAULT_CITIES: tuple[str, ...] = (
    "Springfield",
    "Riverside",
    "Franklin",
    "Clinton",
    "Georgetown",
    "Salem",
    "Madison",
    "Chester",
)


def get_cities_for_state(state: str) -> tuple[str, ...]:
    """Get cities for a given state, with fallback."""
    return STATE_CITIES.get(state, _DEFAULT_CITIES)


def get_states_for_country(country: str) -> tuple[str, ...]:
    """Get states/provinces for a given country, with fallback."""
    return COUNTRY_STATES.get(country, ("Province 1", "Province 2", "Province 3"))


def get_zip_prefix_for_state(state: str) -> str | None:
    """Get a representative zip prefix for a state, or None."""
    prefixes = STATE_ZIP_PREFIX.get(state)
    if prefixes:
        return prefixes[0]
    return None
