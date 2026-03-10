from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional


@dataclass(frozen=True)
class SeriesRule:
    vaccine_group: str
    program_area: str
    due_ages_days: List[int]
    recommendation_type: str = "routine"
    max_age_days: Optional[int] = None
    max_patient_age_days: Optional[int] = None
    cohort_from: Optional[date] = None
    cohort_to: Optional[date] = None


CHILD_SERIES_RULES: List[SeriesRule] = [
    SeriesRule("6-in-1", "routine_child", [56, 84, 112], max_patient_age_days=365 * 3),
    SeriesRule("6-in-1", "routine_child", [548], max_patient_age_days=365 * 5, cohort_from=date(2024, 7, 1)),
    SeriesRule("Rotavirus", "routine_child", [56, 84], max_age_days=168, max_patient_age_days=365 * 1),
    SeriesRule("MenB", "routine_child", [56, 84, 365], max_patient_age_days=365 * 3),
    SeriesRule("PCV", "routine_child", [112, 365], max_patient_age_days=365 * 3),
    SeriesRule("MMR", "routine_child", [365], max_patient_age_days=365 * 18, cohort_to=date(2024, 12, 31)),
    SeriesRule("MMRV", "routine_child", [365, 548], max_patient_age_days=365 * 5, cohort_from=date(2025, 1, 1)),
    SeriesRule(
        "MMRV",
        "routine_child",
        [548],
        max_patient_age_days=365 * 5,
        cohort_from=date(2024, 7, 1),
        cohort_to=date(2024, 12, 31),
    ),
    SeriesRule("dTaP/IPV", "routine_child", [1216], max_patient_age_days=365 * 7),
    SeriesRule("MMR", "routine_child", [1216], max_patient_age_days=365 * 7, cohort_to=date(2022, 8, 31)),
    SeriesRule(
        "MMRV",
        "routine_child",
        [1216],
        max_patient_age_days=365 * 7,
        cohort_from=date(2022, 9, 1),
        cohort_to=date(2024, 12, 31),
    ),
    SeriesRule("HPV", "routine_child", [4380], max_patient_age_days=365 * 19),
    SeriesRule("Td/IPV", "routine_child", [5110], max_patient_age_days=365 * 19),
    SeriesRule("MenACWY", "routine_child", [5110], max_patient_age_days=365 * 19),
]


def applies_to_cohort(rule: SeriesRule, dob: date) -> bool:
    if rule.cohort_from and dob < rule.cohort_from:
        return False
    if rule.cohort_to and dob > rule.cohort_to:
        return False
    return True


def get_child_rules_for_patient(dob: date) -> List[SeriesRule]:
    return [rule for rule in CHILD_SERIES_RULES if applies_to_cohort(rule, dob)]


def current_flu_season_start(reference_date: date) -> date:
    if reference_date.month >= 9:
        return date(reference_date.year, 9, 1)
    if reference_date.month == 1:
        return date(reference_date.year - 1, 9, 1)
    return date(reference_date.year, 9, 1)


def current_covid_season_start(reference_date: date) -> date:
    spring_start = date(reference_date.year, 4, 13)
    spring_end = date(reference_date.year, 6, 30)
    autumn_start = date(reference_date.year, 10, 1)
    autumn_end = date(reference_date.year, 12, 19)

    if reference_date < spring_start:
        return spring_start
    if reference_date <= spring_end:
        return spring_start
    if reference_date < autumn_start:
        return autumn_start
    if reference_date <= autumn_end:
        return autumn_start
    return date(reference_date.year + 1, 4, 13)


def child_seasonal_due_checks(reference_date: date) -> Dict[str, Dict[str, object]]:
    return {
        "Flu": {
            "program_area": "seasonal_child",
            "min_age_years": 2,
            "max_age_years": 15,
            "recommendation_type": "seasonal",
            "season_start": current_flu_season_start(reference_date),
        }
    }


def adult_due_checks(reference_date: date) -> Dict[str, Dict[str, object]]:
    return {
        "Pneumococcal": {
            "program_area": "routine_adult",
            "age_years": 65,
            "recommendation_type": "routine",
        },
        "Shingles": {
            "program_area": "routine_adult",
            "age_years": 65,
            "max_age_years": 79,
            "recommendation_type": "routine",
            "sixty_fifth_birthday_cutoff": date(2023, 9, 1),
            "shingrix_two_dose_start": date(2023, 9, 1),
        },
        "RSV": {
            "program_area": "routine_adult",
            "age_years": 75,
            "max_age_years": 79,
            "recommendation_type": "routine",
        },
        "Flu": {
            "program_area": "seasonal_adult",
            "age_years": 65,
            "recommendation_type": "seasonal",
            "season_start": current_flu_season_start(reference_date),
        },
        "COVID-19": {
            "program_area": "seasonal_adult",
            "age_years": 75,
            "recommendation_type": "seasonal",
            "season_start": current_covid_season_start(reference_date),
        },
    }
