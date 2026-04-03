"""Tests for the EducationProvider."""

from dataforge import DataForge
from dataforge.providers.education import (
    _DEGREES,
    _FIELDS_OF_STUDY,
    _UNIVERSITIES,
)


class TestEducationScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_university_returns_str(self) -> None:
        result = self.forge.education.university()
        assert isinstance(result, str)
        assert result in _UNIVERSITIES

    def test_degree_returns_str(self) -> None:
        result = self.forge.education.degree()
        assert isinstance(result, str)
        assert result in _DEGREES

    def test_field_of_study_returns_str(self) -> None:
        result = self.forge.education.field_of_study()
        assert isinstance(result, str)
        assert result in _FIELDS_OF_STUDY

    def test_deterministic_with_seed(self) -> None:
        f1 = DataForge(seed=77)
        f2 = DataForge(seed=77)
        assert f1.education.university() == f2.education.university()
        assert f1.education.degree() == f2.education.degree()
        assert f1.education.field_of_study() == f2.education.field_of_study()


class TestEducationBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_university_batch(self) -> None:
        result = self.forge.education.university(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(u in _UNIVERSITIES for u in result)

    def test_degree_batch(self) -> None:
        result = self.forge.education.degree(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(d in _DEGREES for d in result)

    def test_field_of_study_batch(self) -> None:
        result = self.forge.education.field_of_study(count=100)
        assert isinstance(result, list)
        assert len(result) == 100
        assert all(f in _FIELDS_OF_STUDY for f in result)

    def test_variety(self) -> None:
        result = self.forge.education.university(count=500)
        unique = set(result)
        # With 50 universities, 500 draws should hit many of them
        assert len(unique) >= 20
