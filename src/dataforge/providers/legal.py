"""LegalProvider — generates fake legal-related data.

Includes case numbers, court names, legal terms, practice areas,
law firm names, judge names, and legal document types.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from dataforge.providers.base import BaseProvider

# Data tuples (immutable, module-level for zero per-call overhead)

_COURTS: tuple[str, ...] = (
    "Supreme Court",
    "Court of Appeals",
    "District Court",
    "Circuit Court",
    "Family Court",
    "Bankruptcy Court",
    "Tax Court",
    "Probate Court",
    "Municipal Court",
    "Small Claims Court",
    "Juvenile Court",
    "Criminal Court",
    "Civil Court",
    "Federal Court",
    "Superior Court",
)

_PRACTICE_AREAS: tuple[str, ...] = (
    "Criminal Law",
    "Civil Litigation",
    "Corporate Law",
    "Family Law",
    "Immigration Law",
    "Intellectual Property",
    "Real Estate Law",
    "Tax Law",
    "Employment Law",
    "Environmental Law",
    "Bankruptcy Law",
    "Personal Injury",
    "Medical Malpractice",
    "Contract Law",
    "Constitutional Law",
    "International Law",
    "Maritime Law",
    "Antitrust Law",
    "Securities Law",
    "Cybersecurity Law",
)

_LEGAL_TERMS: tuple[str, ...] = (
    "habeas corpus",
    "pro bono",
    "amicus curiae",
    "voir dire",
    "res judicata",
    "stare decisis",
    "prima facie",
    "ex parte",
    "in camera",
    "de facto",
    "de jure",
    "mens rea",
    "actus reus",
    "nolo contendere",
    "subpoena",
    "injunction",
    "deposition",
    "indictment",
    "arraignment",
    "acquittal",
)

_DOCUMENT_TYPES: tuple[str, ...] = (
    "Contract",
    "Affidavit",
    "Brief",
    "Motion",
    "Complaint",
    "Petition",
    "Subpoena",
    "Summons",
    "Warrant",
    "Deposition",
    "Plea Agreement",
    "Settlement Agreement",
    "Power of Attorney",
    "Will",
    "Trust Agreement",
)

_FIRM_PREFIXES: tuple[str, ...] = (
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Davis",
    "Miller",
    "Wilson",
    "Moore",
    "Taylor",
    "Anderson",
    "Thomas",
    "Jackson",
    "White",
    "Harris",
    "Martin",
)

_FIRM_SUFFIXES: tuple[str, ...] = (
    "& Associates",
    "& Partners",
    "Law Group",
    "Law Firm",
    "Legal",
    "LLP",
    "Law Offices",
    "& Co.",
    "Legal Services",
    "Legal Group",
)

_JUDGE_FIRST: tuple[str, ...] = (
    "Robert",
    "William",
    "James",
    "Thomas",
    "Richard",
    "Charles",
    "Joseph",
    "David",
    "Michael",
    "Edward",
    "Ruth",
    "Elena",
    "Sonia",
    "Sandra",
    "Amy",
)

_JUDGE_LAST: tuple[str, ...] = (
    "Roberts",
    "Marshall",
    "Warren",
    "Brennan",
    "Stevens",
    "O'Connor",
    "Kennedy",
    "Scalia",
    "Ginsburg",
    "Breyer",
    "Kagan",
    "Gorsuch",
    "Kavanaugh",
    "Barrett",
    "Sotomayor",
)

_CASE_PREFIXES: tuple[str, ...] = (
    "CV",
    "CR",
    "CIV",
    "CRIM",
    "AP",
    "BK",
    "MC",
    "JV",
    "FA",
    "PR",
)


class LegalProvider(BaseProvider):
    """Generates fake legal-related data.

    This provider is locale-independent.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "legal"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "case_number": "case_number",
        "court": "court",
        "court_name": "court",
        "practice_area": "practice_area",
        "legal_term": "legal_term",
        "legal_document": "document_type",
        "document_type": "document_type",
        "law_firm": "law_firm",
        "law_firm_name": "law_firm",
        "judge": "judge",
        "judge_name": "judge",
        "verdict": "verdict",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "court": _COURTS,
        "practice_area": _PRACTICE_AREAS,
        "legal_term": _LEGAL_TERMS,
        "document_type": _DOCUMENT_TYPES,
    }

    # Scalar helpers

    def _one_case_number(self) -> str:
        """Generate a single case number (e.g. ``"CV-2024-003847"``)."""
        choice = self._engine._rng.choice
        ri = self._engine.random_int
        prefix = choice(_CASE_PREFIXES)
        year = ri(2015, 2026)
        num = ri(1, 999999)
        return f"{prefix}-{year}-{num:06d}"

    def _one_law_firm(self) -> str:
        """Generate a single law firm name."""
        choice = self._engine._rng.choice
        return f"{choice(_FIRM_PREFIXES)} {choice(_FIRM_SUFFIXES)}"

    def _one_judge(self) -> str:
        """Generate a single judge name."""
        choice = self._engine._rng.choice
        return f"Hon. {choice(_JUDGE_FIRST)} {choice(_JUDGE_LAST)}"

    # Public API — custom methods

    def case_number(self, count: int = 1) -> str | list[str]:
        """Generate a case number (e.g. ``"CV-2024-003847"``)."""
        if count == 1:
            return self._one_case_number()
        return [self._one_case_number() for _ in range(count)]

    def law_firm(self, count: int = 1) -> str | list[str]:
        """Generate a law firm name (e.g. ``"Smith & Associates"``)."""
        if count == 1:
            return self._one_law_firm()
        return [self._one_law_firm() for _ in range(count)]

    def judge(self, count: int = 1) -> str | list[str]:
        """Generate a judge name (e.g. ``"Hon. Robert Marshall"``)."""
        if count == 1:
            return self._one_judge()
        return [self._one_judge() for _ in range(count)]

    def verdict(self, count: int = 1) -> str | list[str]:
        """Generate a verdict (e.g. ``"Guilty"``)."""
        verdicts = ("Guilty", "Not Guilty", "Dismissed", "Settled", "Mistrial")
        if count == 1:
            return self._engine.choice(verdicts)
        return self._engine.choices(verdicts, count)
