from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional


@dataclass
class VaccineEvent:
    canonical_vaccine: str
    vaccine_program: str
    raw_vaccine_name: str
    event_date: Optional[date]
    source_patient_id: Optional[str] = None
    event_done_at_id: Optional[str] = None
    confidence: int = 100


@dataclass
class Patient:
    nhs_number: str
    source_patient_id: Optional[str]
    first_name: str
    last_name: str
    sex: Optional[str]
    date_of_birth: date
    phone: Optional[str]
    email: Optional[str]
    registration_date: Optional[date]
    raw_rows: List[Dict[str, Any]] = field(default_factory=list)
    vaccine_events: List[VaccineEvent] = field(default_factory=list)
    only_unknown_marker: bool = False

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()

    def age_in_days(self, reference_date: date) -> int:
        return (reference_date - self.date_of_birth).days

    def age_in_years(self, reference_date: date) -> int:
        return max(0, self.age_in_days(reference_date) // 365)


@dataclass
class Recommendation:
    patient_nhs_number: str
    patient_name: str
    date_of_birth: date
    phone: Optional[str]
    email: Optional[str]
    recommendation_type: str
    vaccine_group: str
    program_area: str
    due_date: Optional[date]
    status: str
    priority: int
    reason: str
    explanation: Dict[str, Any]


@dataclass
class ProcessedCohort:
    patients: List[Patient]
    recommendations: List[Recommendation]
    warnings: List[str]
    raw_rows: int
    mapped_rows: int
    unvaccinated_patients: int
