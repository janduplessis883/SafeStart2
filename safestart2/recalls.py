from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd


def parse_due_date(value: object) -> Optional[date]:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def is_past_due(value: object, *, today_local: Optional[date] = None) -> bool:
    due_date = parse_due_date(value)
    if due_date is None:
        return False
    return due_date < (today_local or date.today())


def group_recalls(recalls: list[dict], *, today_local: Optional[date] = None) -> list[dict]:
    local_today = today_local or date.today()
    grouped: dict[tuple[object, ...], dict] = {}

    for recall in recalls:
        overdue_group = is_past_due(recall.get("due_date"), today_local=local_today)
        if overdue_group:
            key = (
                recall.get("surgery_id"),
                recall.get("nhs_number"),
                "__overdue__",
            )
            group_id = "|".join(
                [
                    str(recall.get("surgery_id") or ""),
                    str(recall.get("nhs_number") or ""),
                    "overdue",
                ]
            )
            group_status = "overdue"
        else:
            key = (
                recall.get("surgery_id"),
                recall.get("nhs_number"),
                recall.get("due_date"),
                recall.get("status"),
            )
            group_id = "|".join(
                [
                    str(recall.get("surgery_id") or ""),
                    str(recall.get("nhs_number") or ""),
                    str(recall.get("due_date") or ""),
                    str(recall.get("status") or ""),
                ]
            )
            group_status = recall.get("status")

        group = grouped.get(key)
        if group is None:
            group = {
                "group_id": group_id,
                "surgery_id": recall.get("surgery_id"),
                "surgery_code": recall.get("surgery_code"),
                "surgery_name": recall.get("surgery_name"),
                "nhs_number": recall.get("nhs_number"),
                "full_name": recall.get("full_name"),
                "date_of_birth": recall.get("date_of_birth"),
                "phone": recall.get("phone"),
                "email": recall.get("email"),
                "surgery_email": recall.get("surgery_email"),
                "due_date": recall.get("due_date"),
                "status": group_status,
                "priority": recall.get("priority"),
                "program_areas": [],
                "reasons": [],
                "recommendations": [],
                "recommendation_ids": [],
                "vaccines": [],
                "due_items": [],
                "attempt_count": 0,
                "last_attempt_at": None,
                "last_attempt_method": None,
                "last_attempt_outcome": None,
                "has_overdue_vaccines": overdue_group,
            }
            grouped[key] = group

        if overdue_group:
            existing_due = parse_due_date(group.get("due_date"))
            current_due = parse_due_date(recall.get("due_date"))
            if existing_due is None or (current_due is not None and current_due < existing_due):
                group["due_date"] = recall.get("due_date")

        group["recommendations"].append(recall)
        recommendation_id = recall.get("id")
        if recommendation_id is not None:
            group["recommendation_ids"].append(recommendation_id)
        group["vaccines"].append(str(recall.get("vaccine_group") or "Unknown"))
        group["program_areas"].append(str(recall.get("program_area") or ""))
        group["reasons"].append(str(recall.get("reason") or ""))
        group["due_items"].append(
            {
                "recommendation_id": recommendation_id,
                "vaccine": str(recall.get("vaccine_group") or "Unknown"),
                "due_date": recall.get("due_date"),
                "status": recall.get("status"),
                "reason": str(recall.get("reason") or ""),
            }
        )

        priority = recall.get("priority")
        if priority is not None:
            current_priority = group.get("priority")
            group["priority"] = priority if current_priority is None else min(current_priority, priority)

        if not group.get("email") and recall.get("email"):
            group["email"] = recall.get("email")
        if not group.get("surgery_email") and recall.get("surgery_email"):
            group["surgery_email"] = recall.get("surgery_email")

        attempt_count = int(recall.get("attempt_count") or 0)
        if attempt_count > int(group.get("attempt_count") or 0):
            group["attempt_count"] = attempt_count
            group["last_attempt_at"] = recall.get("last_attempt_at")
            group["last_attempt_method"] = recall.get("last_attempt_method")
            group["last_attempt_outcome"] = recall.get("last_attempt_outcome")

    grouped_recalls = []
    for group in grouped.values():
        vaccine_list = sorted(set(group["vaccines"]))
        program_list = sorted(set(item for item in group["program_areas"] if item))
        reason_list = sorted(set(item for item in group["reasons"] if item))
        due_items = sorted(
            group["due_items"],
            key=lambda item: (str(item.get("due_date") or "9999-12-31"), str(item.get("vaccine") or "")),
        )
        due_dates = [str(item.get("due_date")) for item in due_items if item.get("due_date")]
        unique_due_dates = list(dict.fromkeys(due_dates))
        grouped_due_date = parse_due_date(group.get("due_date"))
        if group.get("has_overdue_vaccines"):
            message_due_mode = "overdue"
        elif grouped_due_date is not None and grouped_due_date > local_today:
            message_due_mode = "future"
        elif grouped_due_date is not None and grouped_due_date == local_today:
            message_due_mode = "current"
        else:
            message_due_mode = "unknown"

        grouped_recalls.append(
            {
                **group,
                "vaccines": vaccine_list,
                "vaccines_display": ", ".join(vaccine_list),
                "program_areas": program_list,
                "program_area_display": ", ".join(program_list),
                "reasons": reason_list,
                "reason": " | ".join(reason_list),
                "recommendation_count": len(group["recommendation_ids"]),
                "due_items": due_items,
                "original_due_dates": unique_due_dates,
                "message_due_mode": message_due_mode,
                "explanation": {
                    "vaccines": vaccine_list,
                    "program_areas": program_list,
                    "recommendation_ids": group["recommendation_ids"],
                    "due_items": due_items,
                    "original_due_dates": unique_due_dates,
                    "has_overdue_vaccines": bool(group.get("has_overdue_vaccines")),
                },
            }
        )

    grouped_recalls.sort(
        key=lambda recall: (
            int(recall.get("priority") or 999),
            str(recall.get("due_date") or "9999-12-31"),
            str(recall.get("full_name") or ""),
        )
    )
    return grouped_recalls
