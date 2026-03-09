from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from .catalog import normalize_vaccine_name
from .models import Patient, ProcessedCohort, Recommendation, VaccineEvent
from .parser import clean_nhs_number, normalize_email, normalize_phone, parse_date
from .schedule import adult_due_checks, child_seasonal_due_checks, get_child_rules_for_patient


INPUT_COLUMNS = {
    "source_patient_id": (
        "ImmunizeMe - vaccines: Patient ID",
        "ImmunizeMe - vaccines 0 - 18yrs: Patient ID",
        "ImmunizeMe - vaccines 19 - 64yrs: Patient ID",
        "ImmunizeMe - vaccines 51-120yrs: Patient ID",
        "IMMUNIZEME PT 0 - 18yrs: Patient ID",
        "IMMUNIZEME PT 19 - 64yrs: Patient ID",
        "IMMUNIZEME PT 51-120yrs: Patient ID",
        "BREAKDOWN 19-50yrs: Patient ID",
    ),
    "first_name": "First name",
    "last_name": "Surname",
    "nhs_number": "NHS number",
    "sex": "Sex",
    "date_of_birth": "Date of birth",
    "registration_date": "Registration date",
    "raw_vaccine_name": (
        "ImmunizeMe - vaccines: Vaccination type",
        "ImmunizeMe - vaccines 0 - 18yrs: Vaccination type",
        "ImmunizeMe - vaccines 19 - 64yrs: Vaccination type",
        "ImmunizeMe - vaccines 51-120yrs: Vaccination type",
    ),
    "phone": "Preferred telephone number",
    "email": (
        "Preferred email address",
        "Email address",
        "E-mail address",
        "Email",
        "E-mail",
    ),
    "event_date": (
        "ImmunizeMe - vaccines: Event date",
        "ImmunizeMe - vaccines 0 - 18yrs: Event date",
        "ImmunizeMe - vaccines 19 - 64yrs: Event date",
        "ImmunizeMe - vaccines 51-120yrs: Event date",
    ),
    "event_done_at_id": (
        "ImmunizeMe - vaccines: Event done at ID",
        "ImmunizeMe - vaccines 0 - 18yrs: Event done at ID",
        "ImmunizeMe - vaccines 19 - 64yrs: Event done at ID",
        "ImmunizeMe - vaccines 51-120yrs: Event done at ID",
    ),
}


def _safe_event_sort_date(value: Optional[date]) -> date:
    return value if isinstance(value, date) else date.min


def _clean_patient_identifier(value: object) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    raw = str(value).strip()
    if not raw or raw.lower() in {"nan", "nat"}:
        return None
    digits = clean_nhs_number(value)
    if digits:
        return digits
    return raw


def classify_due_status(due_date: Optional[date], reference_date: date, unvaccinated: bool = False) -> Tuple[str, int]:
    if unvaccinated:
        return "unvaccinated", 5
    if due_date is None:
        return "review", 15
    delta = (due_date - reference_date).days
    if delta < -14:
        return "overdue", 10
    if delta <= 14:
        return "due_now", 20
    return "due_soon", 30


def _iter_rows(df: pd.DataFrame) -> Iterable[Dict[str, object]]:
    for _, row in df.iterrows():
        normalized_row = {
            str(column).replace("\ufeff", "").strip(): row.get(column)
            for column in row.index
        }
        parsed_row: Dict[str, object] = {}
        for column, source in INPUT_COLUMNS.items():
            if isinstance(source, (tuple, list)):
                parsed_row[column] = next(
                    (
                        normalized_row.get(str(item).strip())
                        for item in source
                        if str(item).strip() in normalized_row
                        and pd.notna(normalized_row.get(str(item).strip()))
                    ),
                    None,
                )
            else:
                parsed_row[column] = normalized_row.get(str(source).strip())
        yield parsed_row


def build_patients_from_rows(
    rows: Iterable[Dict[str, object]],
    overrides: Optional[Dict[str, Tuple[str, str]]] = None,
) -> Tuple[List[Patient], List[str], int]:
    patients: Dict[str, Patient] = {}
    warnings: List[str] = []
    mapped_rows = 0

    for raw in rows:
        patient_identifier = _clean_patient_identifier(raw.get("nhs_number")) or _clean_patient_identifier(
            raw.get("source_patient_id")
        )
        if not patient_identifier:
            warnings.append("Skipped row with missing NHS number.")
            continue

        dob = parse_date(raw["date_of_birth"])
        if dob is None:
            warnings.append(f"Skipped patient {patient_identifier} row with invalid DOB.")
            continue

        patient = patients.get(patient_identifier)
        if patient is None:
            patient = Patient(
                nhs_number=patient_identifier,
                source_patient_id=str(raw["source_patient_id"]).strip() if pd.notna(raw["source_patient_id"]) else None,
                first_name=str(raw["first_name"]).strip(),
                last_name=str(raw["last_name"]).strip(),
                sex=str(raw["sex"]).strip() if pd.notna(raw["sex"]) else None,
                date_of_birth=dob,
                phone=normalize_phone(raw["phone"]),
                email=normalize_email(raw.get("email")),
                registration_date=parse_date(raw["registration_date"]),
            )
            patients[patient_identifier] = patient

        patient.raw_rows.append(raw)
        raw_vaccine_name = str(raw["raw_vaccine_name"]).strip() if pd.notna(raw["raw_vaccine_name"]) else ""
        if not raw_vaccine_name:
            continue
        canonical, program, confidence = normalize_vaccine_name(raw_vaccine_name, overrides=overrides)

        if canonical == "Unknown":
            patient.only_unknown_marker = True
            mapped_rows += 1
            continue
        if canonical == "Unmapped":
            warnings.append(
                f"Skipped patient {patient_identifier} row with unmapped vaccine label `{raw_vaccine_name}`."
            )
            continue

        event = VaccineEvent(
            canonical_vaccine=canonical,
            vaccine_program=program,
            raw_vaccine_name=raw_vaccine_name,
            event_date=parse_date(raw["event_date"]),
            source_patient_id=patient.source_patient_id,
            event_done_at_id=str(raw["event_done_at_id"]).strip() if pd.notna(raw["event_done_at_id"]) else None,
            confidence=confidence,
        )
        patient.vaccine_events.append(event)
        mapped_rows += 1

    for patient in patients.values():
        if patient.vaccine_events:
            patient.only_unknown_marker = False
            patient.vaccine_events.sort(
                key=lambda item: (_safe_event_sort_date(item.event_date), item.canonical_vaccine)
            )

    return list(patients.values()), warnings, mapped_rows


def build_patients_from_dataframe(
    df: pd.DataFrame,
    overrides: Optional[Dict[str, Tuple[str, str]]] = None,
) -> Tuple[List[Patient], List[str], int]:
    return build_patients_from_rows(_iter_rows(df), overrides=overrides)


def _events_for(patient: Patient, vaccine_group: str) -> List[VaccineEvent]:
    equivalent_groups = {
        "MMR": {"MMR", "MMRV"},
    }
    valid_groups = equivalent_groups.get(vaccine_group, {vaccine_group})
    return [
        event
        for event in patient.vaccine_events
        if event.canonical_vaccine in valid_groups and event.event_date
    ]


def _add_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        return value.replace(month=2, day=28, year=value.year + years)


def _add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + (month_index // 12)
    month = (month_index % 12) + 1
    day = min(value.day, monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _shingles_first_dose_due_date(patient: Patient, rule: Dict[str, object]) -> date:
    first_dose_due_date = _add_years(patient.date_of_birth, int(rule["age_years"]))
    cutoff = rule.get("sixty_fifth_birthday_cutoff")
    if cutoff and first_dose_due_date < cutoff:
        return _add_years(patient.date_of_birth, 70)
    return first_dose_due_date


def _shingles_due_date(
    patient: Patient,
    observed: List[VaccineEvent],
    reference_date: date,
    rule: Dict[str, object],
) -> Optional[date]:
    if len(observed) >= 2:
        return None

    first_dose_due_date = _shingles_first_dose_due_date(patient, rule)
    eightieth_birthday = _add_years(patient.date_of_birth, 80)
    eighty_first_birthday = _add_years(patient.date_of_birth, 81)

    if observed:
        second_dose_due_date = _add_months(observed[0].event_date, 6)
        if reference_date >= eighty_first_birthday or second_dose_due_date >= eighty_first_birthday:
            return None
        return second_dose_due_date

    if reference_date < first_dose_due_date or reference_date >= eightieth_birthday:
        return None

    return first_dose_due_date


def _build_unvaccinated_recommendation(patient: Patient, reference_date: date) -> Recommendation:
    age_years = patient.age_in_days(reference_date) // 365
    if age_years < 1:
        next_vaccine = "6-in-1"
    elif age_years < 4:
        next_vaccine = "MMRV"
    elif age_years < 15:
        next_vaccine = "Td/IPV"
    elif age_years < 65:
        next_vaccine = "Clinical review"
    elif age_years < 75:
        next_vaccine = "Pneumococcal"
    else:
        next_vaccine = "RSV"

    return Recommendation(
        patient_nhs_number=patient.nhs_number,
        patient_name=patient.full_name,
        date_of_birth=patient.date_of_birth,
        phone=patient.phone,
        email=patient.email,
        recommendation_type="unvaccinated",
        vaccine_group=next_vaccine,
        program_area="unvaccinated_pathway",
        due_date=reference_date,
        status="unvaccinated",
        priority=1,
        reason="Patient has no vaccines on record and should enter the unvaccinated workflow.",
        explanation={
            "age_years": age_years,
            "observed_events": 0,
            "pathway": "unknown_marker_only",
        },
    )


def _build_unvaccinated_recommendations(
    patient: Patient,
    reference_date: date,
    lookahead_days: int,
) -> List[Recommendation]:
    age_days = patient.age_in_days(reference_date)
    age_years = patient.age_in_years(reference_date)
    recommendations: List[Recommendation] = []

    child_rules = get_child_rules_for_patient(patient.date_of_birth)
    for rule in child_rules:
        if rule.max_age_days and age_days > rule.max_age_days:
            continue
        if rule.max_patient_age_days and age_days > rule.max_patient_age_days:
            continue

        expected_count = sum(1 for days in rule.due_ages_days if age_days >= days - lookahead_days)
        if expected_count <= 0:
            continue

        due_days = rule.due_ages_days[0]
        due_date = patient.date_of_birth + timedelta(days=due_days)
        recommendations.append(
            Recommendation(
                patient_nhs_number=patient.nhs_number,
                patient_name=patient.full_name,
                date_of_birth=patient.date_of_birth,
                phone=patient.phone,
                email=patient.email,
                recommendation_type="unvaccinated",
                vaccine_group=rule.vaccine_group,
                program_area="unvaccinated_pathway",
                due_date=due_date,
                status="unvaccinated",
                priority=1,
                reason=f"Patient has no vaccines on record and is eligible for {rule.vaccine_group}.",
                explanation={
                    "age_years": age_years,
                    "observed_events": 0,
                    "pathway": "unknown_marker_only",
                },
            )
        )

    seasonal_child_rules = child_seasonal_due_checks(reference_date)
    for vaccine_group, rule in seasonal_child_rules.items():
        min_age_years = int(rule["min_age_years"])
        max_age_years = int(rule["max_age_years"])
        if age_years < min_age_years or age_years > max_age_years:
            continue

        recommendations.append(
            Recommendation(
                patient_nhs_number=patient.nhs_number,
                patient_name=patient.full_name,
                date_of_birth=patient.date_of_birth,
                phone=patient.phone,
                email=patient.email,
                recommendation_type="unvaccinated",
                vaccine_group=vaccine_group,
                program_area="unvaccinated_pathway",
                due_date=rule["season_start"],
                status="unvaccinated",
                priority=1,
                reason=f"Patient has no vaccines on record and is eligible for seasonal {vaccine_group}.",
                explanation={
                    "age_years": age_years,
                    "observed_events": 0,
                    "pathway": "unknown_marker_only",
                },
            )
        )

    adult_rules = adult_due_checks(reference_date)
    for vaccine_group, rule in adult_rules.items():
        due_date = _add_years(patient.date_of_birth, int(rule["age_years"]))

        if vaccine_group == "Flu":
            if age_years < int(rule["age_years"]):
                continue
            due_date = rule["season_start"]
        elif vaccine_group == "COVID-19":
            if age_years < int(rule["age_years"]):
                continue
            due_date = rule["season_start"]
        elif vaccine_group == "Shingles":
            due_date = _shingles_due_date(patient, observed=[], reference_date=reference_date, rule=rule)
            if due_date is None:
                continue
        else:
            if age_years < int(rule["age_years"]):
                continue
            max_age_years = int(rule["max_age_years"]) if rule.get("max_age_years") is not None else None
            if max_age_years is not None and age_years > max_age_years:
                continue

        recommendations.append(
            Recommendation(
                patient_nhs_number=patient.nhs_number,
                patient_name=patient.full_name,
                date_of_birth=patient.date_of_birth,
                phone=patient.phone,
                email=patient.email,
                recommendation_type="unvaccinated",
                vaccine_group=vaccine_group,
                program_area="unvaccinated_pathway",
                due_date=due_date,
                status="unvaccinated",
                priority=1,
                reason=f"Patient has no vaccines on record and is eligible for {vaccine_group}.",
                explanation={
                    "age_years": age_years,
                    "observed_events": 0,
                    "pathway": "unknown_marker_only",
                },
            )
        )

    if recommendations:
        recommendations.sort(key=lambda item: (item.due_date or date.min, item.vaccine_group))
        return recommendations

    return [_build_unvaccinated_recommendation(patient, reference_date)]


def build_recommendations(
    patients: List[Patient],
    reference_date: date,
    lookahead_days: int = 30,
) -> List[Recommendation]:
    recommendations: List[Recommendation] = []

    for patient in patients:
        if patient.only_unknown_marker:
            recommendations.extend(
                _build_unvaccinated_recommendations(
                    patient,
                    reference_date=reference_date,
                    lookahead_days=lookahead_days,
                )
            )
            continue

        age_days = patient.age_in_days(reference_date)
        child_rules = get_child_rules_for_patient(patient.date_of_birth)
        for rule in child_rules:
            if rule.max_age_days and age_days > rule.max_age_days:
                continue
            if rule.max_patient_age_days and age_days > rule.max_patient_age_days:
                continue

            observed = _events_for(patient, rule.vaccine_group)
            expected_count = sum(1 for days in rule.due_ages_days if age_days >= days - lookahead_days)
            if expected_count <= 0:
                continue

            if len(observed) >= expected_count:
                continue

            next_index = len(observed)
            due_days = rule.due_ages_days[next_index]
            due_date = patient.date_of_birth + timedelta(days=due_days)
            status, priority = classify_due_status(due_date, reference_date)

            recommendations.append(
                Recommendation(
                    patient_nhs_number=patient.nhs_number,
                    patient_name=patient.full_name,
                    date_of_birth=patient.date_of_birth,
                    phone=patient.phone,
                    email=patient.email,
                    recommendation_type=rule.recommendation_type,
                    vaccine_group=rule.vaccine_group,
                    program_area=rule.program_area,
                    due_date=due_date,
                    status=status,
                    priority=priority,
                    reason=f"Missing {rule.vaccine_group} dose {next_index + 1} for the applicable NHS schedule.",
                    explanation={
                        "observed_count": len(observed),
                        "expected_count": expected_count,
                        "due_ages_days": rule.due_ages_days,
                    },
                )
            )

        seasonal_child_rules = child_seasonal_due_checks(reference_date)
        age_years = patient.age_in_years(reference_date)
        for vaccine_group, rule in seasonal_child_rules.items():
            min_age_years = int(rule["min_age_years"])
            max_age_years = int(rule["max_age_years"])
            if age_years < min_age_years or age_years > max_age_years:
                continue

            observed = _events_for(patient, vaccine_group)
            season_start = rule["season_start"]
            had_this_season = any(event.event_date and event.event_date >= season_start for event in observed)
            if had_this_season:
                continue

            status, priority = classify_due_status(season_start, reference_date)
            recommendations.append(
                Recommendation(
                    patient_nhs_number=patient.nhs_number,
                    patient_name=patient.full_name,
                    date_of_birth=patient.date_of_birth,
                    phone=patient.phone,
                    email=patient.email,
                    recommendation_type=str(rule["recommendation_type"]),
                    vaccine_group=vaccine_group,
                    program_area=str(rule["program_area"]),
                    due_date=season_start,
                    status=status,
                    priority=priority + 5,
                    reason=f"Eligible for seasonal {vaccine_group} vaccine based on age.",
                    explanation={
                        "age_years": age_years,
                        "observed_count": len(observed),
                        "season_start": season_start.isoformat(),
                    },
                )
            )

        adult_rules = adult_due_checks(reference_date)
        for vaccine_group, rule in adult_rules.items():
            observed = _events_for(patient, vaccine_group)

            if vaccine_group != "Shingles":
                if age_years < int(rule["age_years"]):
                    continue
                max_age_years = int(rule["max_age_years"]) if rule.get("max_age_years") is not None else None
                if max_age_years is not None and age_years > max_age_years:
                    continue

            due_date = _add_years(patient.date_of_birth, int(rule["age_years"]))

            if vaccine_group == "Flu":
                season_start = rule["season_start"]
                had_this_season = any(event.event_date and event.event_date >= season_start for event in observed)
                if had_this_season:
                    continue
                due_date = season_start
            elif vaccine_group == "COVID-19":
                season_start = rule["season_start"]
                had_this_season = any(event.event_date and event.event_date >= season_start for event in observed)
                if had_this_season:
                    continue
                due_date = season_start
            elif vaccine_group == "Shingles":
                due_date = _shingles_due_date(patient, observed=observed, reference_date=reference_date, rule=rule)
                if due_date is None:
                    continue
            elif observed:
                continue

            status, priority = classify_due_status(due_date, reference_date)
            recommendations.append(
                Recommendation(
                    patient_nhs_number=patient.nhs_number,
                    patient_name=patient.full_name,
                    date_of_birth=patient.date_of_birth,
                    phone=patient.phone,
                    email=patient.email,
                    recommendation_type=str(rule["recommendation_type"]),
                    vaccine_group=vaccine_group,
                    program_area=str(rule["program_area"]),
                    due_date=due_date,
                    status=status,
                    priority=priority + 5,
                    reason=f"Eligible for {vaccine_group} based on age and recorded history.",
                    explanation={
                        "age_years": age_years,
                        "observed_count": len(observed),
                        "dose_interval_months": 6 if vaccine_group == "Shingles" and len(observed) == 1 else None,
                    },
                )
            )

    recommendations.sort(key=lambda item: (item.priority, item.patient_name, item.vaccine_group))
    return recommendations


def process_immunizeme_dataframe(
    df: pd.DataFrame,
    reference_date: Optional[date] = None,
    lookahead_days: int = 30,
    overrides: Optional[Dict[str, Tuple[str, str]]] = None,
) -> ProcessedCohort:
    reference_date = reference_date or date.today()
    patients, warnings, mapped_rows = build_patients_from_dataframe(df, overrides=overrides)
    recommendations = build_recommendations(
        patients,
        reference_date=reference_date,
        lookahead_days=lookahead_days,
    )
    return ProcessedCohort(
        patients=patients,
        recommendations=recommendations,
        warnings=warnings,
        raw_rows=len(df),
        mapped_rows=mapped_rows,
        unvaccinated_patients=sum(1 for patient in patients if patient.only_unknown_marker),
    )


def process_immunizeme_rows(
    rows: List[Dict[str, object]],
    reference_date: Optional[date] = None,
    lookahead_days: int = 30,
    overrides: Optional[Dict[str, Tuple[str, str]]] = None,
) -> ProcessedCohort:
    reference_date = reference_date or date.today()
    patients, warnings, mapped_rows = build_patients_from_rows(rows, overrides=overrides)
    recommendations = build_recommendations(
        patients,
        reference_date=reference_date,
        lookahead_days=lookahead_days,
    )
    return ProcessedCohort(
        patients=patients,
        recommendations=recommendations,
        warnings=warnings,
        raw_rows=len(rows),
        mapped_rows=mapped_rows,
        unvaccinated_patients=sum(1 for patient in patients if patient.only_unknown_marker),
    )
