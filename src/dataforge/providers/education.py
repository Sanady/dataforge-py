"""EducationProvider — generates fake education-related data.

Includes university names, degree types, and fields of study.
All data is stored as immutable ``tuple[str, ...]``.
"""

from dataforge.providers.base import BaseProvider

# Data tuples (immutable, module-level)

_UNIVERSITIES: tuple[str, ...] = (
    "Harvard University",
    "Stanford University",
    "MIT",
    "University of Cambridge",
    "University of Oxford",
    "California Institute of Technology",
    "Princeton University",
    "Yale University",
    "Columbia University",
    "University of Chicago",
    "Duke University",
    "University of Pennsylvania",
    "Johns Hopkins University",
    "Northwestern University",
    "Cornell University",
    "University of Michigan",
    "University of California, Berkeley",
    "UCLA",
    "University of Virginia",
    "Georgetown University",
)

_DEGREES: tuple[str, ...] = (
    "Associate of Arts",
    "Associate of Science",
    "Bachelor of Arts",
    "Bachelor of Science",
    "Bachelor of Fine Arts",
    "Bachelor of Engineering",
    "Master of Arts",
    "Master of Science",
    "Master of Business Administration",
    "Master of Fine Arts",
    "Master of Engineering",
    "Master of Education",
    "Master of Public Health",
    "Master of Social Work",
    "Doctor of Philosophy",
)

_FIELDS_OF_STUDY: tuple[str, ...] = (
    "Computer Science",
    "Mathematics",
    "Physics",
    "Chemistry",
    "Biology",
    "Engineering",
    "Electrical Engineering",
    "Mechanical Engineering",
    "Civil Engineering",
    "Chemical Engineering",
    "Biomedical Engineering",
    "Economics",
    "Business Administration",
    "Finance",
    "Accounting",
    "Marketing",
    "Management",
    "Psychology",
    "Sociology",
    "Political Science",
)


class EducationProvider(BaseProvider):
    """Generates fake education-related data.

    This provider is locale-independent.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "education"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "university": "university",
        "degree": "degree",
        "field_of_study": "field_of_study",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "university": _UNIVERSITIES,
        "degree": _DEGREES,
        "field_of_study": _FIELDS_OF_STUDY,
    }
