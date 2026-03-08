from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd


def first_name(full_name: Optional[str]) -> str:
    name = str(full_name or "").strip()
    return name.split()[0] if name else "Patient"


def format_recall_date(value: Optional[str]) -> str:
    if not value:
        return "—"
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return str(value)
    return parsed.strftime("%d/%m/%Y")


def _due_phrase(
    recall: dict,
    *,
    today_local: Optional[date] = None,
) -> str:
    message_today = today_local or date.today()
    due_dt = pd.to_datetime(recall.get("due_date"), errors="coerce")
    if pd.isna(due_dt):
        due_local_date = None
    else:
        due_local_date = due_dt.date()
    return "were due" if due_local_date is not None and due_local_date <= message_today else "are due"


def build_outreach_message(
    recall: dict,
    self_book_url: Optional[str] = None,
    *,
    today_local: Optional[date] = None,
) -> str:
    due_phrase = _due_phrase(recall, today_local=today_local)
    patient_first_name = first_name(recall.get("full_name"))
    vaccines = recall.get("vaccines_display") or "your vaccines"
    due_date = format_recall_date(recall.get("due_date"))
    surgery_name = recall.get("surgery_name") or recall.get("surgery_code") or "your surgery"

    if self_book_url:
        return (
            f"Dear {patient_first_name}, you {due_phrase} {vaccines} on {due_date}. "
            f"Book here: {self_book_url} {surgery_name}"
        )
    return (
        f"Dear {patient_first_name}, you {due_phrase} {vaccines} on {due_date}. "
        f"We will send a self-book link to arrange this. {surgery_name}"
    )


def build_email_message(
    recall: dict,
    *,
    today_local: Optional[date] = None,
) -> str:
    due_phrase = _due_phrase(recall, today_local=today_local)
    patient_first_name = first_name(recall.get("full_name"))
    vaccines = recall.get("vaccines_display") or "your vaccines"
    due_date = format_recall_date(recall.get("due_date"))
    surgery_name = recall.get("surgery_name") or recall.get("surgery_code") or "your surgery"
    return (
        f"Dear {patient_first_name}, you {due_phrase} {vaccines} on {due_date}.\n"
        "Read more about vaccinations on the NHS vaccination website at https://www.nhs.uk/vaccinations/\n"
        "We will send a self-book link via SMS to arrange this.\n"
        f"Regards,\n{surgery_name}"
    )
