"""sv_SE internet data."""

free_email_domains: tuple[str, ...] = (
    "gmail.com",
    "hotmail.se",
    "live.se",
    "outlook.com",
    "spray.se",
    "telia.com",
    "yahoo.se",
    "bredband.net",
)

domain_suffixes: tuple[str, ...] = (
    "se",
    "com",
    "net",
    "org",
    "nu",
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
