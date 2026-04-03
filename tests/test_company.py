"""Tests for the CompanyProvider."""

from dataforge import DataForge


class TestCompanyScalar:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_company_name_returns_str(self) -> None:
        result = self.forge.company.company_name()
        assert isinstance(result, str)
        assert len(result) > 0
        # Should have at least a name and suffix
        assert " " in result

    def test_company_suffix_returns_str(self) -> None:
        result = self.forge.company.company_suffix()
        assert isinstance(result, str)

    def test_catch_phrase_returns_str(self) -> None:
        result = self.forge.company.catch_phrase()
        assert isinstance(result, str)
        assert " " in result

    def test_job_title_returns_str(self) -> None:
        result = self.forge.company.job_title()
        assert isinstance(result, str)
        assert len(result) > 0


class TestCompanyBatch:
    def setup_method(self) -> None:
        self.forge = DataForge(locale="en_US", seed=42)

    def test_company_name_batch(self) -> None:
        result = self.forge.company.company_name(count=50)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_job_title_batch(self) -> None:
        result = self.forge.company.job_title(count=100)
        assert isinstance(result, list)
        assert len(result) == 100

    def test_catch_phrase_batch(self) -> None:
        result = self.forge.company.catch_phrase(count=50)
        assert isinstance(result, list)
        assert len(result) == 50


class TestCompanyLocales:
    def test_de_DE_company(self) -> None:
        forge = DataForge(locale="de_DE", seed=42)
        name = forge.company.company_name()
        assert isinstance(name, str)

    def test_fr_FR_company(self) -> None:
        forge = DataForge(locale="fr_FR", seed=42)
        name = forge.company.company_name()
        assert isinstance(name, str)

    def test_es_ES_company(self) -> None:
        forge = DataForge(locale="es_ES", seed=42)
        name = forge.company.company_name()
        assert isinstance(name, str)

    def test_ja_JP_company(self) -> None:
        forge = DataForge(locale="ja_JP", seed=42)
        name = forge.company.company_name()
        assert isinstance(name, str)
