"""nb_NO internet data — domains, free email providers, TLDs."""

free_email_domains: tuple[str, ...] = (
    "gmail.com",
    "hotmail.com",
    "outlook.com",
    "yahoo.no",
    "online.no",
    "broadpark.no",
    "start.no",
    "frisurf.no",
)

domain_suffixes: tuple[str, ...] = (
    "no",
    "com",
    "net",
    "org",
    "co.no",
)

user_formats: tuple[str, ...] = (
    "{first}.{last}",
    "{first}_{last}",
    "{first}{last}",
    "{first}.{last}##",
    "{first}##",
)
