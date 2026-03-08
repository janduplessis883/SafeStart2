from __future__ import annotations

from datetime import date
from typing import Dict, Iterable, List, Optional

import pandas as pd

from .models import ProcessedCohort


WORKFLOW_STATES = [
    "Ready to text",
    "Prepared today",
    "Recently texted",
    "No phone number",
    "Booked",
    "Unvaccinated pathway",
    "Needs manual review",
]


def classify_recall_workflow(
    recall: dict,
    attempts: List[dict],
    *,
    sent_recently_days: int = 14,
    today_local: Optional[date] = None,
) -> str:
    local_today = today_local or date.today()
    recent_cutoff = pd.Timestamp.now(tz="Europe/London") - pd.Timedelta(days=sent_recently_days)
    phone = str(recall.get("phone") or "").strip()

    if not phone:
        return "No phone number"

    for attempt in attempts:
        method = str(attempt.get("communication_method") or "").strip().lower()
        outcome = str(attempt.get("outcome") or "").strip().lower()
        sent_at = pd.to_datetime(attempt.get("sent_at"), errors="coerce", utc=True)
        sent_at_local = sent_at.tz_convert("Europe/London") if not pd.isna(sent_at) else None

        if outcome == "booked":
            return "Booked"
        if (
            method in {"bulk_sms", "sms"}
            and outcome == "prepared"
            and sent_at_local is not None
            and sent_at_local.date() == local_today
        ):
            return "Prepared today"
        if (
            method in {"bulk_sms", "sms"}
            and outcome in {"sent", "delivered"}
            and sent_at_local is not None
            and sent_at_local >= recent_cutoff
        ):
            return "Recently texted"

    if str(recall.get("status") or "").strip().lower() == "unvaccinated":
        return "Unvaccinated pathway"
    if str(recall.get("status") or "").strip().lower() == "review":
        return "Needs manual review"
    return "Ready to text"


def summarize_patient_recall(
    recall: dict,
    attempts: List[dict],
    patient_timeline: dict,
    *,
    sent_recently_days: int = 14,
    today_local: Optional[date] = None,
) -> Dict[str, object]:
    workflow_state = classify_recall_workflow(
        recall,
        attempts,
        sent_recently_days=sent_recently_days,
        today_local=today_local,
    )
    events = list(patient_timeline.get("events") or [])
    attempts_timeline = list(patient_timeline.get("attempts") or [])

    last_vaccination_date = None
    if events:
        last_vaccination_date = max(
            (
                pd.to_datetime(event.get("event_date"), errors="coerce")
                for event in events
            ),
            default=None,
        )

    last_outreach_at = None
    last_outreach_method = None
    if attempts_timeline:
        sorted_attempts = sorted(
            attempts_timeline,
            key=lambda attempt: pd.to_datetime(attempt.get("sent_at"), errors="coerce", utc=True)
            if attempt.get("sent_at")
            else pd.NaT,
            reverse=True,
        )
        last_attempt = sorted_attempts[0]
        last_outreach_at = pd.to_datetime(last_attempt.get("sent_at"), errors="coerce", utc=True)
        last_outreach_method = last_attempt.get("communication_method")

    workflow_actions = {
        "Ready to text": "Add this patient to the next SMS batch.",
        "Prepared today": "Do not prepare again today. Review the prepared batch or wait for send outcome.",
        "Recently texted": "Do not send another SMS yet. Review response or booking outcome first.",
        "No phone number": "Use manual outreach or update contact details before messaging.",
        "Booked": "Confirm the appointment outcome and complete the recall when appropriate.",
        "Unvaccinated pathway": "Route this patient through the unvaccinated workflow rather than standard recall.",
        "Needs manual review": "Review the patient history manually before further outreach.",
    }

    return {
        "workflow_state": workflow_state,
        "next_action": workflow_actions[workflow_state],
        "due_vaccines": list(recall.get("vaccines") or []),
        "last_vaccination_date": last_vaccination_date,
        "last_outreach_at": last_outreach_at,
        "last_outreach_method": last_outreach_method,
    }


def compare_processed_cohorts(previous: ProcessedCohort, current: ProcessedCohort) -> Dict[str, object]:
    previous_event_keys = {
        (
            patient.nhs_number,
            event.canonical_vaccine,
            event.raw_vaccine_name,
            event.event_date.isoformat() if event.event_date else "",
        )
        for patient in previous.patients
        for event in patient.vaccine_events
    }
    current_event_keys = {
        (
            patient.nhs_number,
            event.canonical_vaccine,
            event.raw_vaccine_name,
            event.event_date.isoformat() if event.event_date else "",
        )
        for patient in current.patients
        for event in patient.vaccine_events
    }

    previous_recommendations = {
        (
            recommendation.patient_nhs_number,
            recommendation.vaccine_group,
            recommendation.recommendation_type,
            recommendation.due_date.isoformat() if recommendation.due_date else "",
        ): recommendation
        for recommendation in previous.recommendations
    }
    current_recommendations = {
        (
            recommendation.patient_nhs_number,
            recommendation.vaccine_group,
            recommendation.recommendation_type,
            recommendation.due_date.isoformat() if recommendation.due_date else "",
        ): recommendation
        for recommendation in current.recommendations
    }

    new_recall_keys = set(current_recommendations) - set(previous_recommendations)
    resolved_recall_keys = set(previous_recommendations) - set(current_recommendations)
    shared_recall_keys = set(previous_recommendations) & set(current_recommendations)

    status_changes: List[dict] = []
    for key in sorted(shared_recall_keys):
        previous_item = previous_recommendations[key]
        current_item = current_recommendations[key]
        if previous_item.status == current_item.status:
            continue
        status_changes.append(
            {
                "nhs_number": current_item.patient_nhs_number,
                "patient_name": current_item.patient_name,
                "vaccine_group": current_item.vaccine_group,
                "recommendation_type": current_item.recommendation_type,
                "due_date": current_item.due_date.isoformat() if current_item.due_date else "",
                "previous_status": previous_item.status,
                "current_status": current_item.status,
            }
        )

    def _count_by_vaccine(recommendations: Iterable[tuple]) -> List[dict]:
        counts: Dict[str, int] = {}
        for key in recommendations:
            vaccine_group = str(key[1] or "Unknown")
            counts[vaccine_group] = counts.get(vaccine_group, 0) + 1
        return [
            {"vaccine_group": vaccine_group, "count": count}
            for vaccine_group, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        ]

    changed_patients = {
        item["nhs_number"]
        for item in status_changes
    }

    return {
        "new_vaccine_events": len(current_event_keys - previous_event_keys),
        "new_recall_count": len(new_recall_keys),
        "resolved_recall_count": len(resolved_recall_keys),
        "status_change_count": len(status_changes),
        "patients_with_status_changes": len(changed_patients),
        "new_recall_vaccine_counts": _count_by_vaccine(new_recall_keys),
        "resolved_recall_vaccine_counts": _count_by_vaccine(resolved_recall_keys),
        "status_changes": status_changes,
    }
