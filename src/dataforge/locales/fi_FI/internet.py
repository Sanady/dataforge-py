"""fi_FI internet data."""

free_email_domains: tuple[str, ...] = (
    "gmail.com",
    "outlook.com",
    "yahoo.com",
    "hotmail.com",
    "luukku.com",
    "suomi24.fi",
    "kolumbus.fi",
    "pp.inet.fi",
)

domain_suffixes: tuple[str, ...] = (
    "fi",
    "com",
    "net",
    "org",
    "eu",
    "info",
)

user_formats: tuple[str, ...] = (
    "{first}.{last}",
    "{first}_{last}",
    "{first}{last}",
    "{first}.{last}##",
    "{first}##",
)
