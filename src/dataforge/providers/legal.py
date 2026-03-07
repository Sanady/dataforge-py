"""LegalProvider — generates fake legal-related data.

Includes case numbers, court names, legal terms, practice areas,
law firm names, judge names, and legal document types.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from typing import Literal, overload

from dataforge.providers.base import BaseProvider

# ------------------------------------------------------------------
# Data tuples (immutable, module-level for zero per-call overhead)
# ------------------------------------------------------------------

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
    "Magistrate Court",
    "Court of Claims",
    "Appellate Court",
    "High Court",
    "Crown Court",
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
    "Entertainment Law",
    "Sports Law",
    "Elder Law",
    "Human Rights Law",
    "Military Law",
    "Patent Law",
    "Trademark Law",
    "Copyright Law",
    "Insurance Law",
    "Health Care Law",
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
    "plea bargain",
    "mistrial",
    "statute of limitations",
    "due process",
    "double jeopardy",
    "tort",
    "liability",
    "negligence",
    "fiduciary duty",
    "arbitration",
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
    "Non-Disclosure Agreement",
    "Lease Agreement",
    "Employment Agreement",
    "Patent Application",
    "Court Order",
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
    "Clark",
    "Lewis",
    "Walker",
    "Hall",
    "Young",
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
    "Katherine",
    "Margaret",
    "Patricia",
    "Linda",
    "Susan",
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
    "Alito",
    "Thomas",
    "Jackson",
    "Holmes",
    "Cardozo",
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

    # ------------------------------------------------------------------
    # Scalar helpers
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @overload
    def case_number(self) -> str: ...
    @overload
    def case_number(self, count: Literal[1]) -> str: ...
    @overload
    def case_number(self, count: int) -> str | list[str]: ...
    def case_number(self, count: int = 1) -> str | list[str]:
        """Generate a case number (e.g. ``"CV-2024-003847"``).

        Parameters
        ----------
        count : int
            Number of case numbers to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_case_number()
        return [self._one_case_number() for _ in range(count)]

    @overload
    def court(self) -> str: ...
    @overload
    def court(self, count: Literal[1]) -> str: ...
    @overload
    def court(self, count: int) -> str | list[str]: ...
    def court(self, count: int = 1) -> str | list[str]:
        """Generate a court name (e.g. ``"Supreme Court"``).

        Parameters
        ----------
        count : int
            Number of court names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_COURTS)
        return self._engine.choices(_COURTS, count)

    @overload
    def practice_area(self) -> str: ...
    @overload
    def practice_area(self, count: Literal[1]) -> str: ...
    @overload
    def practice_area(self, count: int) -> str | list[str]: ...
    def practice_area(self, count: int = 1) -> str | list[str]:
        """Generate a legal practice area (e.g. ``"Corporate Law"``).

        Parameters
        ----------
        count : int
            Number of practice areas to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_PRACTICE_AREAS)
        return self._engine.choices(_PRACTICE_AREAS, count)

    @overload
    def legal_term(self) -> str: ...
    @overload
    def legal_term(self, count: Literal[1]) -> str: ...
    @overload
    def legal_term(self, count: int) -> str | list[str]: ...
    def legal_term(self, count: int = 1) -> str | list[str]:
        """Generate a legal term (e.g. ``"habeas corpus"``).

        Parameters
        ----------
        count : int
            Number of legal terms to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_LEGAL_TERMS)
        return self._engine.choices(_LEGAL_TERMS, count)

    @overload
    def document_type(self) -> str: ...
    @overload
    def document_type(self, count: Literal[1]) -> str: ...
    @overload
    def document_type(self, count: int) -> str | list[str]: ...
    def document_type(self, count: int = 1) -> str | list[str]:
        """Generate a legal document type (e.g. ``"Affidavit"``).

        Parameters
        ----------
        count : int
            Number of document types to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_DOCUMENT_TYPES)
        return self._engine.choices(_DOCUMENT_TYPES, count)

    @overload
    def law_firm(self) -> str: ...
    @overload
    def law_firm(self, count: Literal[1]) -> str: ...
    @overload
    def law_firm(self, count: int) -> str | list[str]: ...
    def law_firm(self, count: int = 1) -> str | list[str]:
        """Generate a law firm name (e.g. ``"Smith & Associates"``).

        Parameters
        ----------
        count : int
            Number of law firm names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_law_firm()
        return [self._one_law_firm() for _ in range(count)]

    @overload
    def judge(self) -> str: ...
    @overload
    def judge(self, count: Literal[1]) -> str: ...
    @overload
    def judge(self, count: int) -> str | list[str]: ...
    def judge(self, count: int = 1) -> str | list[str]:
        """Generate a judge name (e.g. ``"Hon. Robert Marshall"``).

        Parameters
        ----------
        count : int
            Number of judge names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._one_judge()
        return [self._one_judge() for _ in range(count)]

    @overload
    def verdict(self) -> str: ...
    @overload
    def verdict(self, count: Literal[1]) -> str: ...
    @overload
    def verdict(self, count: int) -> str | list[str]: ...
    def verdict(self, count: int = 1) -> str | list[str]:
        """Generate a verdict (e.g. ``"Guilty"``).

        Parameters
        ----------
        count : int
            Number of verdicts to generate.

        Returns
        -------
        str or list[str]
        """
        verdicts = ("Guilty", "Not Guilty", "Dismissed", "Settled", "Mistrial")
        if count == 1:
            return self._engine.choice(verdicts)
        return self._engine.choices(verdicts, count)
