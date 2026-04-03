"""Medical / healthcare provider — ICD-10 codes, drugs, blood types, etc."""

from dataforge.providers.base import BaseProvider

_BLOOD_TYPES: tuple[str, ...] = (
    "A+",
    "A-",
    "B+",
    "B-",
    "AB+",
    "AB-",
    "O+",
    "O-",
)

_ICD10_CATEGORIES: tuple[str, ...] = (
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
)

_DRUG_NAMES: tuple[str, ...] = (
    "Acetaminophen",
    "Ibuprofen",
    "Amoxicillin",
    "Lisinopril",
    "Metformin",
    "Atorvastatin",
    "Amlodipine",
    "Omeprazole",
    "Metoprolol",
    "Losartan",
    "Simvastatin",
    "Albuterol",
    "Gabapentin",
    "Hydrochlorothiazide",
    "Sertraline",
    "Levothyroxine",
    "Azithromycin",
    "Prednisone",
    "Furosemide",
    "Ciprofloxacin",
)

_DRUG_FORMS: tuple[str, ...] = (
    "Tablet",
    "Capsule",
    "Syrup",
    "Injection",
    "Cream",
    "Ointment",
    "Drops",
    "Patch",
    "Inhaler",
    "Suppository",
    "Suspension",
    "Solution",
    "Gel",
    "Spray",
    "Powder",
)

_DIAGNOSES: tuple[str, ...] = (
    "Hypertension",
    "Type 2 Diabetes Mellitus",
    "Hyperlipidemia",
    "Major Depressive Disorder",
    "Generalized Anxiety Disorder",
    "Chronic Obstructive Pulmonary Disease",
    "Asthma",
    "Osteoarthritis",
    "Gastroesophageal Reflux Disease",
    "Urinary Tract Infection",
    "Pneumonia",
    "Bronchitis",
    "Coronary Artery Disease",
    "Heart Failure",
    "Atrial Fibrillation",
    "Chronic Kidney Disease",
    "Hypothyroidism",
    "Anemia",
    "Migraine",
    "Epilepsy",
)

_PROCEDURES: tuple[str, ...] = (
    "Blood Draw",
    "X-Ray",
    "MRI Scan",
    "CT Scan",
    "Ultrasound",
    "ECG/EKG",
    "Echocardiogram",
    "Colonoscopy",
    "Endoscopy",
    "Biopsy",
    "Physical Therapy",
    "Vaccination",
    "Suturing",
    "Cast Application",
    "Joint Injection",
    "Dialysis",
    "Chemotherapy",
    "Radiation Therapy",
    "Cardiac Catheterization",
    "Angioplasty",
)

_DOSAGE_UNITS: tuple[str, ...] = (
    "mg",
    "mcg",
    "g",
    "mL",
    "IU",
    "mEq",
)

# Real-world blood type distribution (US population, approximate)
# Source: American Red Cross
_BLOOD_TYPE_WEIGHTS: tuple[float, ...] = (
    0.357,  # A+
    0.063,  # A-
    0.085,  # B+
    0.015,  # B-
    0.034,  # AB+
    0.006,  # AB-
    0.374,  # O+
    0.066,  # O-
)


class MedicalProvider(BaseProvider):
    """Generates fake medical / healthcare data."""

    __slots__ = ()

    _provider_name = "medical"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "blood_type": "blood_type",
        "realistic_blood_type": "realistic_blood_type",
        "icd10_code": "icd10_code",
        "icd10": "icd10_code",
        "drug_name": "drug_name",
        "drug": "drug_name",
        "drug_form": "drug_form",
        "dosage": "dosage",
        "diagnosis": "diagnosis",
        "procedure": "procedure",
        "medical_record_number": "medical_record_number",
        "mrn": "medical_record_number",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "blood_type": _BLOOD_TYPES,
        "drug_name": _DRUG_NAMES,
        "drug_form": _DRUG_FORMS,
        "diagnosis": _DIAGNOSES,
        "procedure": _PROCEDURES,
    }

    def _one_icd10(self) -> str:
        """Generate ICD-10 code format: A##.# or A##.##"""
        cat = self._engine.choice(_ICD10_CATEGORIES)
        major = self._engine.random_int(0, 99)
        minor = self._engine.random_int(0, 9)
        return f"{cat}{major:02d}.{minor}"

    def _one_dosage(self) -> str:
        amount = self._engine.random_int(1, 1000)
        unit = self._engine.choice(_DOSAGE_UNITS)
        return f"{amount} {unit}"

    def _one_mrn(self) -> str:
        return f"MRN-{self._engine.random_digits_str(8)}"

    def realistic_blood_type(self, count: int = 1) -> str | list[str]:
        """Generate a blood type with real-world frequency distribution."""
        if count == 1:
            return self._engine.weighted_choice(_BLOOD_TYPES, _BLOOD_TYPE_WEIGHTS)
        return self._engine.weighted_choices(_BLOOD_TYPES, _BLOOD_TYPE_WEIGHTS, count)

    def icd10_code(self, count: int = 1) -> str | list[str]:
        """Generate an ICD-10 diagnostic code (e.g., A01.0)."""
        if count == 1:
            return self._one_icd10()
        return [self._one_icd10() for _ in range(count)]

    def dosage(self, count: int = 1) -> str | list[str]:
        """Generate a drug dosage (e.g., 500 mg)."""
        if count == 1:
            return self._one_dosage()
        return [self._one_dosage() for _ in range(count)]

    def medical_record_number(self, count: int = 1) -> str | list[str]:
        """Generate a medical record number (MRN-########)."""
        if count == 1:
            return self._one_mrn()
        return [self._one_mrn() for _ in range(count)]
