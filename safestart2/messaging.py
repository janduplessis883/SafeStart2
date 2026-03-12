from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd

from .recalls import is_past_due


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


def _message_due_mode(recall: dict, *, today_local: Optional[date] = None) -> str:
    explicit_mode = str(recall.get("message_due_mode") or "").strip().lower()
    if explicit_mode in {"overdue", "future", "current", "unknown"}:
        return explicit_mode

    message_today = today_local or date.today()
    due_dt = pd.to_datetime(recall.get("due_date"), errors="coerce")
    if pd.isna(due_dt):
        return "unknown"
    due_local_date = due_dt.date()
    if is_past_due(due_local_date, today_local=message_today):
        return "overdue"
    if due_local_date > message_today:
        return "future"
    return "current"


def build_outreach_message(
    recall: dict,
    self_book_url: Optional[str] = None,
    *,
    today_local: Optional[date] = None,
) -> str:
    due_mode = _message_due_mode(recall, today_local=today_local)
    patient_first_name = first_name(recall.get("full_name"))
    vaccines = recall.get("vaccines_display") or "your vaccines"
    due_date = format_recall_date(recall.get("due_date"))
    surgery_name = recall.get("surgery_name") or recall.get("surgery_code") or "your surgery"
    if due_mode == "overdue":
        opening = f"Dear {patient_first_name}, you are eligible for the following vaccines: {vaccines}. "
    elif due_mode == "future":
        opening = f"Dear {patient_first_name}, the following vaccines become due on {due_date}: {vaccines}. "
    elif due_mode == "current":
        opening = f"Dear {patient_first_name}, the following vaccines are due today: {vaccines}. "
    else:
        opening = f"Dear {patient_first_name}, the following vaccines are due: {vaccines}. "

    if self_book_url:
        return f"{opening}Book here: {self_book_url} {surgery_name}"
    return f"{opening}We will send a self-book link to arrange this. {surgery_name}"


def build_email_message(
    recall: dict,
    *,
    today_local: Optional[date] = None,
) -> str:
    due_mode = _message_due_mode(recall, today_local=today_local)
    patient_first_name = first_name(recall.get("full_name"))
    vaccines = recall.get("vaccines_display") or "your vaccines"
    due_date = format_recall_date(recall.get("due_date"))
    surgery_name = recall.get("surgery_name") or recall.get("surgery_code") or "your surgery"
    if due_mode == "overdue":
        opening = f"Dear {patient_first_name}, you are eligible for the following vaccines: {vaccines}.\n"
    elif due_mode == "future":
        opening = f"Dear {patient_first_name}, the following vaccines become due on {due_date}: {vaccines}.\n"
    elif due_mode == "current":
        opening = f"Dear {patient_first_name}, the following vaccines are due today: {vaccines}.\n"
    else:
        opening = f"Dear {patient_first_name}, the following vaccines are due: {vaccines}.\n"
    return (
        opening
        + "Read more about vaccinations on the NHS vaccination website at https://www.nhs.uk/vaccinations/\n"
        + "We will send a self-book link via SMS to arrange this.\n"
        + f"Regards,\n{surgery_name}"
    )
