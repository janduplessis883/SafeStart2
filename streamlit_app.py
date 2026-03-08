from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone
import json

import altair as alt
import pandas as pd
import streamlit as st

SAFE_START2_ROOT = Path(__file__).parent
sys.path.insert(0, str(SAFE_START2_ROOT))

from safestart2.config import get_resend_settings, get_smsworks_settings
from safestart2.messaging import build_email_message, build_outreach_message, first_name
from safestart2.parser import load_dataframe, sanitize_dataframe_columns
from safestart2.processing import INPUT_COLUMNS, process_immunizeme_dataframe
from safestart2.resend_client import build_resend_requests, send_resend_requests
from safestart2.schedule import get_child_rules_for_patient
from safestart2.smsworks import build_smsworks_dry_run_payload, send_smsworks_requests
from safestart2.supabase_store import (
    AuthenticationError,
    AuthorizationError,
    SupabaseStore,
    UserContext,
)
from safestart2.workflow import (
    WORKFLOW_STATES,
    classify_recall_workflow,
    summarize_patient_recall,
)

RECALL_OUTCOME_OPTIONS = [
    "sent",
    "prepared",
    "delivered",
    "failed",
    "booked",
    "declined",
    "no_response",
]


st.set_page_config(
    page_title="SafeStart-2",
    page_icon="💉",
    layout="wide",
)


def _clear_session() -> None:
    st.session_state.pop("supabase_session", None)
    _invalidate_data_caches()


def _invalidate_data_caches() -> None:
    st.session_state.pop("worklist_data_cache", None)
    st.session_state.pop("vaccination_events_patient_cache", None)
    st.session_state.pop("patient_timeline_cache", None)


def _vaccination_events_cache_key(
    user_context: UserContext,
    surgery_id: Optional[str],
    include_without_events: bool,
) -> tuple[str, str, str, bool]:
    return (
        user_context.email,
        str(user_context.role or ""),
        str(surgery_id or ""),
        include_without_events,
    )


def _get_cached_vaccination_event_patients(
    store: SupabaseStore,
    user_context: UserContext,
    surgery_id: Optional[str],
    include_without_events: bool,
) -> list[dict]:
    cache = st.session_state.setdefault("vaccination_events_patient_cache", {})
    cache_key = _vaccination_events_cache_key(
        user_context,
        surgery_id=surgery_id,
        include_without_events=include_without_events,
    )
    if cache_key not in cache:
        cache[cache_key] = store.list_patients_with_vaccination_events(
            user_context,
            surgery_id=surgery_id,
            include_without_events=include_without_events,
        )
    return list(cache[cache_key])


def _patient_timeline_cache_key(
    user_context: UserContext,
    surgery_id: str,
    nhs_number: str,
) -> tuple[str, str, str, str]:
    return (
        user_context.email,
        str(user_context.role or ""),
        surgery_id,
        nhs_number,
    )


def _get_cached_patient_timeline(
    store: SupabaseStore,
    user_context: UserContext,
    surgery_id: str,
    nhs_number: str,
) -> dict:
    cache = st.session_state.setdefault("patient_timeline_cache", {})
    cache_key = _patient_timeline_cache_key(
        user_context,
        surgery_id=surgery_id,
        nhs_number=nhs_number,
    )
    if cache_key not in cache:
        cache[cache_key] = store.get_patient_timeline(
            user_context=user_context,
            surgery_id=surgery_id,
            nhs_number=nhs_number,
        )
    return dict(cache[cache_key])


def _apply_vaccination_event_exclusions(
    patient: dict,
    *,
    excluded_vaccines: set[str],
) -> dict:
    vaccine_counts = {
        str(vaccine): int(count or 0)
        for vaccine, count in dict(patient.get("event_vaccine_counts") or {}).items()
        if str(vaccine)
    }
    vaccine_last_dates = {
        str(vaccine): value
        for vaccine, value in dict(patient.get("event_vaccine_last_dates") or {}).items()
        if str(vaccine)
    }
    filtered_vaccines = sorted(
        vaccine for vaccine in vaccine_counts
        if vaccine not in excluded_vaccines and int(vaccine_counts.get(vaccine) or 0) > 0
    )
    filtered_last_dates = [
        vaccine_last_dates.get(vaccine)
        for vaccine in filtered_vaccines
        if vaccine_last_dates.get(vaccine)
    ]
    return {
        **patient,
        "event_count": sum(int(vaccine_counts.get(vaccine) or 0) for vaccine in filtered_vaccines),
        "vaccine_count": len(filtered_vaccines),
        "vaccines_display": ", ".join(filtered_vaccines),
        "last_event_date": max((str(value) for value in filtered_last_dates), default=None),
    }


def _worklist_cache_key(
    user_context: UserContext,
    surgery_id: Optional[str],
) -> tuple[str, str, str]:
    return (
        user_context.email,
        str(user_context.role or ""),
        str(surgery_id or ""),
    )


def _get_cached_worklist_data(
    store: SupabaseStore,
    user_context: UserContext,
    surgery_id: Optional[str],
) -> dict:
    cache = st.session_state.setdefault("worklist_data_cache", {})
    cache_key = _worklist_cache_key(user_context, surgery_id=surgery_id)
    if cache_key not in cache:
        recalls = store.list_active_recalls(user_context, surgery_id=surgery_id)
        recommendation_ids = [str(row["id"]) for row in recalls if row.get("id")]
        try:
            attempt_rows = store.list_attempt_rows_for_recommendations(recommendation_ids)
            attempt_error = None
        except Exception as exc:
            attempt_rows = []
            attempt_error = (
                "Workflow filters are unavailable until `sql/003_bulk_sms_batches.sql` is applied: "
                f"{exc}"
            )
        try:
            recall_batches = store.list_recall_batches(user_context, surgery_id=surgery_id)
            recall_batch_error = None
        except Exception as exc:
            recall_batches = []
            recall_batch_error = str(exc)
        cache[cache_key] = {
            "recalls": recalls,
            "attempt_rows": attempt_rows,
            "attempt_error": attempt_error,
            "recall_batches": recall_batches,
            "recall_batch_error": recall_batch_error,
        }
    return dict(cache[cache_key])


def _build_store() -> tuple[SupabaseStore, Optional[str]]:
    try:
        store = SupabaseStore(session_tokens=st.session_state.get("supabase_session"))
    except AuthenticationError as exc:
        _clear_session()
        store = SupabaseStore()
        return store, str(exc)

    session_tokens = store.get_session_tokens()
    if session_tokens:
        st.session_state["supabase_session"] = session_tokens

    return store, None


def _render_sign_in(store: SupabaseStore, session_error: Optional[str]) -> None:
    if not store.enabled:
        st.error(
            "Supabase auth is not configured. Add `[supabase] url` and `anon_key` "
            "(or `key`) to `.streamlit/secrets.toml`."
        )
        st.stop()

    if session_error:
        st.error(session_error)

    left_col, center_col, right_col = st.columns([1, 1.8, 1])
    with center_col:
        st.title("💉 SafeStart-2", text_alignment='center')
        st.caption("Sign in with your Supabase user before uploading or persisting cohorts.", text_alignment='center')
        with st.form("supabase_sign_in", clear_on_submit=False):
            email = st.text_input("Email")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Sign in", type="primary")

        st.info(
            "Each user must exist in Supabase Authentication and have an active row in "
            "`public.surgery_users`."
        )

    if submitted:
        try:
            store.sign_in(email, password)
        except (AuthenticationError, AuthorizationError) as exc:
            st.error(str(exc))
        else:
            st.session_state["supabase_session"] = store.get_session_tokens()
            st.rerun()

    st.stop()


def _render_unauthorized_user(store: SupabaseStore, user_context: UserContext) -> None:
    st.title("💉 SafeStart-2")
    st.error(
        f"Signed in as `{user_context.email}`, but this account is not mapped in "
        "`public.surgery_users`."
    )
    st.code(
        "insert into public.surgery_users (surgery_id, email, full_name, role)\n"
        "values (<surgery_uuid_or_null>, '<email>', '<full name>', 'superuser');",
        language="sql",
    )
    if st.button("Sign out"):
        store.sign_out()
        _clear_session()
        st.rerun()
    st.stop()


store, session_error = _build_store()
if not store.enabled:
    _render_sign_in(store, session_error)

try:
    user_context = store.get_current_user_context()
except AuthorizationError as exc:
    st.title("💉 SafeStart2")
    st.error(str(exc))
    if st.button("Clear saved session"):
        store.sign_out()
        _clear_session()
        st.rerun()
    st.stop()

if not user_context:
    _render_sign_in(store, session_error)
if not user_context.is_authorized:
    _render_unauthorized_user(store, user_context)

assert user_context is not None

def _format_date(value: Optional[str]) -> str:
    if not value:
        return "—"
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return str(value)
    return parsed.strftime("%d/%m/%Y")


def _format_ts(value: Optional[str]) -> str:
    if not value:
        return "—"
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return str(value)
    return parsed.tz_convert("Europe/London").strftime("%d/%m/%Y %H:%M")


def _recall_option_label(recall: dict) -> str:
    due_label = _format_date(recall.get("due_date"))
    return (
        f"{recall.get('full_name', 'Unknown')} | {recall.get('vaccines_display', 'Unknown')} | "
        f"{recall.get('status', 'unknown')} | due {due_label}"
    )


def _build_outreach_message(recall: dict, self_book_url: Optional[str] = None) -> str:
    return build_outreach_message(recall, self_book_url=self_book_url)


def _build_email_subject(recall: dict) -> str:
    surgery_name = recall.get("surgery_name") or recall.get("surgery_code") or "your surgery"
    return f"Vaccines due at {surgery_name}"


def _build_email_message(recall: dict) -> str:
    return build_email_message(recall)


def _build_bulk_sms_rows(recalls: list[dict], self_book_url: Optional[str]) -> list[dict]:
    rows = []
    for recall in recalls:
        phone = str(recall.get("phone") or "").strip()
        rows.append(
            {
                "Group ID": recall["group_id"],
                "Patient": recall.get("full_name") or "—",
                "NHS Number": recall.get("nhs_number") or "—",
                "Firstname": first_name(recall.get("full_name")),
                "DOB": _format_date(recall.get("date_of_birth")),
                "Phone": phone or "—",
                "Vaccines": recall.get("vaccines_display") or "—",
                "Due Date": _format_date(recall.get("due_date")),
                "Status": "ready" if phone else "missing_phone",
                "Exclusion": "",
                "Message": _build_outreach_message(recall, self_book_url=self_book_url),
                "Recommendation IDs": recall.get("recommendation_ids", []),
            }
        )
    return rows


def _build_accurx_sms_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "NHS number": row.get("NHS Number") or "—",
                "Telephone number": row.get("Phone") or "—",
                "DOB": row.get("DOB") or "—",
                "Firstname": row.get("Firstname") or "—",
            }
            for row in rows
        ]
    )


def _build_accurx_email_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "NHS number": row.get("NHS Number") or "—",
                "Phone": row.get("Phone") or "—",
                "DOB": row.get("DOB") or "—",
                "Firstname": row.get("Firstname") or "—",
            }
            for row in rows
        ]
    )


def _build_attempts_by_group(recalls: list[dict], attempt_rows: list[dict]) -> dict[str, list[dict]]:
    recommendation_to_group: dict[str, str] = {}
    for recall in recalls:
        for recommendation_id in recall.get("recommendation_ids", []):
            recommendation_to_group[str(recommendation_id)] = recall["group_id"]

    attempts_by_group: dict[str, list[dict]] = {recall["group_id"]: [] for recall in recalls}
    for attempt in attempt_rows:
        group_id = recommendation_to_group.get(str(attempt.get("recommendation_id") or ""))
        if group_id:
            attempts_by_group.setdefault(group_id, []).append(attempt)
    return attempts_by_group


def _bulk_sms_exclusion_reason(
    recall: dict,
    attempts: list[dict],
    *,
    exclude_missing_phone: bool,
    exclude_prepared_today: bool,
    exclude_sent_recently: bool,
    sent_recently_days: int,
    exclude_booked: bool,
) -> Optional[str]:
    phone = str(recall.get("phone") or "").strip()
    if exclude_missing_phone and not phone:
        return "missing_phone"

    today_local = date.today()
    recent_cutoff = pd.Timestamp.now(tz="Europe/London") - pd.Timedelta(days=sent_recently_days)
    for attempt in attempts:
        method = str(attempt.get("communication_method") or "").strip().lower()
        outcome = str(attempt.get("outcome") or "").strip().lower()
        sent_at = pd.to_datetime(attempt.get("sent_at"), errors="coerce", utc=True)
        sent_at_local = sent_at.tz_convert("Europe/London") if not pd.isna(sent_at) else None

        if exclude_booked and outcome == "booked":
            return "booked"
        if (
            exclude_prepared_today
            and method in {"bulk_sms", "sms"}
            and outcome == "prepared"
            and sent_at_local is not None
            and sent_at_local.date() == today_local
        ):
            return "prepared_today"
        if (
            exclude_sent_recently
            and method in {"bulk_sms", "sms"}
            and outcome in {"sent", "delivered"}
            and sent_at_local is not None
            and sent_at_local >= recent_cutoff
        ):
            return "sent_recently"

    return None


def _build_bulk_sms_candidates(
    recalls: list[dict],
    self_book_url: Optional[str],
    attempts_by_group: dict[str, list[dict]],
    *,
    exclude_missing_phone: bool,
    exclude_prepared_today: bool,
    exclude_sent_recently: bool,
    sent_recently_days: int,
    exclude_booked: bool,
) -> list[dict]:
    rows = _build_bulk_sms_rows(recalls, self_book_url=self_book_url)
    for row in rows:
        recall = next(item for item in recalls if item["group_id"] == row["Group ID"])
        exclusion = _bulk_sms_exclusion_reason(
            recall,
            attempts_by_group.get(recall["group_id"], []),
            exclude_missing_phone=exclude_missing_phone,
            exclude_prepared_today=exclude_prepared_today,
            exclude_sent_recently=exclude_sent_recently,
            sent_recently_days=sent_recently_days,
            exclude_booked=exclude_booked,
        )
        if exclusion:
            row["Status"] = "blocked"
            row["Exclusion"] = exclusion
        elif row["Status"] != "ready":
            row["Exclusion"] = row["Status"]
            row["Status"] = "blocked"
    return rows


def _build_recall_batch_rows(recalls: list[dict], self_book_url: Optional[str]) -> list[dict]:
    rows = []
    for recall in recalls:
        sms_message = _build_outreach_message(recall, self_book_url=self_book_url)
        email_message = _build_email_message(recall)
        rows.append(
            {
                "Group ID": recall["group_id"],
                "Patient": recall.get("full_name") or "—",
                "NHS Number": recall.get("nhs_number") or "—",
                "Firstname": first_name(recall.get("full_name")),
                "DOB": _format_date(recall.get("date_of_birth")),
                "Phone": str(recall.get("phone") or "").strip(),
                "Email": str(recall.get("email") or "").strip(),
                "Reply To": str(recall.get("surgery_email") or "").strip(),
                "Vaccines": recall.get("vaccines_display") or "—",
                "Due Date": _format_date(recall.get("due_date")),
                "SMS Message": sms_message,
                "Email Subject": _build_email_subject(recall),
                "Email Message": email_message,
                "Recommendation IDs": list(recall.get("recommendation_ids") or []),
            }
        )
    return rows


def _classify_batch_rows_for_method(
    batch_rows: list[dict],
    delivery_method: str,
) -> tuple[list[dict], list[dict]]:
    ready_rows: list[dict] = []
    blocked_rows: list[dict] = []
    for row in batch_rows:
        enriched = {**row}
        exclusion = ""
        if delivery_method == "sms":
            if not str(row.get("Phone") or "").strip():
                exclusion = "missing_phone"
        elif delivery_method == "email":
            if not str(row.get("Email") or "").strip():
                exclusion = "missing_email"
            elif not str(row.get("Reply To") or "").strip():
                exclusion = "missing_reply_to"

        if exclusion:
            enriched["Exclusion"] = exclusion
            blocked_rows.append(enriched)
        else:
            ready_rows.append(enriched)
    return ready_rows, blocked_rows


def _format_age_from_dob(value: Optional[str]) -> str:
    if not value:
        return "—"
    dob = pd.to_datetime(value, errors="coerce")
    if pd.isna(dob):
        return "—"

    today = pd.Timestamp(date.today())
    if dob > today:
        return "—"

    years = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    months = (today.year - dob.year) * 12 + (today.month - dob.month) - (today.day < dob.day)
    years = max(int(years), 0)
    months = max(int(months), 0)
    return f"{years}y ({months}m)"


def _age_years_from_dob(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    dob = pd.to_datetime(value, errors="coerce")
    if pd.isna(dob):
        return None

    today = pd.Timestamp(date.today())
    if dob > today:
        return None

    years = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    return max(int(years), 0)


def _filter_dataframe_by_age(
    df: pd.DataFrame,
    reference_date: date,
    min_age_years: int,
    max_age_years: int,
) -> tuple[pd.DataFrame, int]:
    dob_column = INPUT_COLUMNS["date_of_birth"]
    if dob_column not in df.columns:
        return df, 0

    dob_series = pd.to_datetime(df[dob_column], errors="coerce", dayfirst=True)
    age_years = ((pd.Timestamp(reference_date) - dob_series).dt.days // 365).astype("Int64")
    included = age_years.ge(min_age_years) & age_years.le(max_age_years)
    invalid_dob_count = int((dob_series.isna()).sum())
    filtered_df = df[included.fillna(False)].copy()
    return filtered_df, invalid_dob_count


def _parse_batch_notes(notes: Optional[str]) -> dict:
    if not notes:
        return {}
    try:
        parsed = json.loads(notes)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _format_batch_label(batch: dict, timestamp_key: str = "created_at") -> str:
    when = _format_ts(batch.get(timestamp_key))
    surgery = batch.get("surgery_code") or batch.get("surgery_name") or "Unknown surgery"
    method = batch.get("delivery_method") or "unassigned"
    status = batch.get("status") or "unknown"
    selected = batch.get("selected_count") or batch.get("ready_count") or 0
    return f"{when} | {surgery} | {method} | {status} | {selected} selected"


def _mask_secret(value: Optional[str]) -> str:
    secret = str(value or "")
    if len(secret) <= 8:
        return "configured" if secret else "not configured"
    return f"{secret[:4]}...{secret[-4:]}"


def _recall_age_band(value: Optional[str]) -> str:
    age_years = _age_years_from_dob(value)
    if age_years is None:
        return "Unknown"
    if age_years < 1:
        return "<1"
    if age_years <= 4:
        return "1-4"
    if age_years <= 11:
        return "5-11"
    if age_years <= 17:
        return "12-17"
    if age_years <= 64:
        return "18-64"
    if age_years <= 74:
        return "65-74"
    return "75+"


def _sort_recalls(recalls: list[dict], sort_by: str, descending: bool) -> list[dict]:
    def sort_key(recall: dict) -> tuple:
        age_years = _age_years_from_dob(recall.get("date_of_birth"))
        if sort_by == "Due date":
            return (recall.get("due_date") or "9999-12-31", recall.get("full_name") or "")
        if sort_by == "Age":
            return (age_years if age_years is not None else -1, recall.get("full_name") or "")
        if sort_by == "Patient name":
            return (recall.get("full_name") or "", recall.get("due_date") or "9999-12-31")
        if sort_by == "Attempts":
            return (int(recall.get("attempt_count") or 0), recall.get("full_name") or "")
        return (
            int(recall.get("priority") or 999),
            recall.get("due_date") or "9999-12-31",
            recall.get("full_name") or "",
        )

    return sorted(recalls, key=sort_key, reverse=descending)


def _build_recall_overlay_timeline_df(event_rows: list[dict], recall: dict) -> pd.DataFrame:
    rows: list[dict] = []
    dob = pd.to_datetime(recall.get("date_of_birth"), errors="coerce")
    due_date = pd.to_datetime(recall.get("due_date"), errors="coerce")
    schedule_cutoff = max(
        pd.Timestamp(date.today()),
        due_date.normalize() if not pd.isna(due_date) else pd.Timestamp.min,
    )
    if not pd.isna(dob):
        seen_schedule_points: set[tuple[pd.Timestamp, str]] = set()
        for rule in get_child_rules_for_patient(dob.date()):
            for due_days in rule.due_ages_days:
                scheduled_date = dob + pd.Timedelta(days=due_days)
                if scheduled_date > schedule_cutoff:
                    continue
                schedule_key = (scheduled_date.normalize(), rule.vaccine_group)
                if schedule_key in seen_schedule_points:
                    continue
                seen_schedule_points.add(schedule_key)
                rows.append(
                    {
                        "Event Date": scheduled_date.normalize(),
                        "Vaccine": rule.vaccine_group,
                        "Program": rule.program_area or "routine_child",
                        "Marker": "Routine schedule",
                        "Detail": "Routine childhood schedule",
                    }
                )

    rows.extend(
        [
            {
                "Event Date": pd.to_datetime(event.get("event_date"), errors="coerce"),
                "Vaccine": event.get("canonical_vaccine") or "Unknown",
                "Program": event.get("vaccine_program") or "—",
                "Marker": "Recorded",
                "Detail": event.get("raw_vaccine_name") or "—",
            }
            for event in event_rows
        ]
    )
    for vaccine in recall.get("vaccines", []):
        rows.append(
            {
                "Event Date": due_date,
                "Vaccine": vaccine,
                "Program": recall.get("status") or "—",
                "Marker": "Recall due",
                "Detail": recall.get("reason") or "—",
            }
        )
    return pd.DataFrame(rows).dropna(subset=["Event Date"])


def _build_vaccine_grid_chart(vaccine_order: list[str]) -> alt.Chart:
    grid_df = pd.DataFrame({"Vaccine": vaccine_order})
    return (
        alt.Chart(grid_df)
        .mark_rule(color="#d8dbe3", strokeWidth=0.3, opacity=0.5)
        .encode(y=alt.Y("Vaccine:N", sort=vaccine_order))
    )


def _group_recalls(recalls: list[dict]) -> list[dict]:
    grouped: dict[tuple[object, ...], dict] = {}
    for recall in recalls:
        key = (
            recall.get("surgery_id"),
            recall.get("nhs_number"),
            recall.get("due_date"),
            recall.get("status"),
        )
        group = grouped.get(key)
        if group is None:
            group = {
                "group_id": "|".join(
                    [
                        str(recall.get("surgery_id") or ""),
                        str(recall.get("nhs_number") or ""),
                        str(recall.get("due_date") or ""),
                        str(recall.get("status") or ""),
                    ]
                ),
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
                "status": recall.get("status"),
                "priority": recall.get("priority"),
                "program_areas": [],
                "reasons": [],
                "recommendations": [],
                "recommendation_ids": [],
                "vaccines": [],
                "attempt_count": 0,
                "last_attempt_at": None,
                "last_attempt_method": None,
                "last_attempt_outcome": None,
            }
            grouped[key] = group

        group["recommendations"].append(recall)
        group["recommendation_ids"].append(recall["id"])
        group["vaccines"].append(str(recall.get("vaccine_group") or "Unknown"))
        group["program_areas"].append(str(recall.get("program_area") or ""))
        group["reasons"].append(str(recall.get("reason") or ""))

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
                "explanation": {
                    "vaccines": vaccine_list,
                    "program_areas": program_list,
                    "recommendation_ids": group["recommendation_ids"],
                },
            }
        )

    grouped_recalls.sort(
        key=lambda recall: (
            int(recall.get("priority") or 999),
            recall.get("due_date") or "9999-12-31",
            recall.get("full_name") or "",
        )
    )
    return grouped_recalls


def _render_worklist_tab(
    store: SupabaseStore,
    user_context: UserContext,
    self_book_url: Optional[str],
    sms_sender_id: Optional[str],
) -> None:
    smsworks_settings = get_smsworks_settings()
    resend_settings = get_resend_settings()
    st.subheader("Recall Worklist")
    st.caption("Review active recalls, log outreach attempts, and close completed or suppressed items.")

    try:
        surgeries = store.list_accessible_surgeries(user_context)
    except AuthorizationError as exc:
        st.error(str(exc))
        return

    selected_surgery_id: Optional[str] = user_context.surgery_id
    if user_context.is_superuser:
        surgery_options = [None] + [surgery["id"] for surgery in surgeries]
        surgery_labels = {
            None: "All surgeries",
            **{
                surgery["id"]: f"{surgery['surgery_code']} - {surgery['surgery_name']}"
                for surgery in surgeries
            },
        }
        selected_surgery_id = st.selectbox(
            "Surgery filter",
            options=surgery_options,
            format_func=lambda surgery_id: surgery_labels[surgery_id],
            key="worklist_surgery_filter",
        )

    control_col1, control_col2, control_col3, _ = st.columns([0.18, 0.22, 0.26, 0.34])
    if control_col1.button(
        "Refresh data",
        key="worklist_refresh_data",
        icon=":material/refresh:",
    ):
        _invalidate_data_caches()
        st.rerun()
    exclude_flu_recalls = control_col2.toggle(
        "Exclude Flu",
        value=True,
        key="worklist_exclude_flu",
    )
    exclude_covid_recalls = control_col3.toggle(
        "Exclude COVID-19",
        value=True,
        key="worklist_exclude_covid19",
    )

    worklist_data = _get_cached_worklist_data(
        store,
        user_context,
        surgery_id=selected_surgery_id,
    )
    attempt_error = worklist_data.get("attempt_error")
    if attempt_error:
        st.error(str(attempt_error))

    excluded_vaccine_groups = set()
    if exclude_flu_recalls:
        excluded_vaccine_groups.add("Flu")
    if exclude_covid_recalls:
        excluded_vaccine_groups.add("COVID-19")

    recalls = [
        recall
        for recall in worklist_data.get("recalls", [])
        if str(recall.get("vaccine_group") or "") not in excluded_vaccine_groups
    ]
    grouped_recalls = _group_recalls(recalls)
    if not grouped_recalls:
        st.info("No active recalls are currently visible for this account, surgery filter, and vaccine exclusions.")
        return

    selected_recommendation_ids = {
        str(recommendation_id)
        for recall in grouped_recalls
        for recommendation_id in recall.get("recommendation_ids", [])
    }
    all_attempt_rows = [
        row
        for row in worklist_data.get("attempt_rows", [])
        if str(row.get("recommendation_id") or "") in selected_recommendation_ids
    ]
    attempts_by_group = _build_attempts_by_group(grouped_recalls, all_attempt_rows)
    recall_batches = worklist_data.get("recall_batches", [])
    recall_batch_error = worklist_data.get("recall_batch_error")

    metric1, metric2, metric3, metric4, metric5 = st.columns(5)
    metric1.metric("Patient Recalls", f"{len(grouped_recalls):,}")
    metric2.metric("Active Recommendations", f"{len(recalls):,}")
    metric3.metric("Overdue", f"{sum(item['status'] == 'overdue' for item in grouped_recalls):,}")
    metric4.metric("Due Now", f"{sum(item['status'] == 'due_now' for item in grouped_recalls):,}")
    metric5.metric("Unvaccinated", f"{sum(item['status'] == 'unvaccinated' for item in grouped_recalls):,}")
    with st.expander("1. Filter Recall Recommendations", expanded=False, icon=":material/filter_list:"):
        filter_col1, filter_col2, filter_col3, filter_col4, filter_col5, filter_col6 = st.columns([1.1, 1.1, 1, 1.1, 0.9, 0.8])
        all_statuses = sorted({recall["status"] for recall in grouped_recalls})
        status_filter = filter_col1.multiselect(
            "Statuses",
            options=all_statuses,
            default=all_statuses,
            key="worklist_status_filter",
        )
        workflow_options = [
            workflow
            for workflow in WORKFLOW_STATES
            if any(classify_recall_workflow(recall, attempts_by_group.get(recall["group_id"], [])) == workflow for recall in grouped_recalls)
        ]
        workflow_filter = filter_col2.multiselect(
            "Workflow",
            options=workflow_options,
            default=workflow_options,
            key="worklist_workflow_filter",
        )
        vaccine_filter = filter_col3.text_input(
            "Vaccine filter",
            placeholder="e.g. MMR",
            key="worklist_vaccine_filter",
        )
        search_filter = filter_col4.text_input(
            "Patient or NHS search",
            placeholder="Name, NHS, phone, reason",
            key="worklist_search_filter",
        )
        sort_by = filter_col5.selectbox(
            "Sort by",
            options=["Priority", "Due date", "Age", "Patient name", "Attempts"],
            key="worklist_sort_by",
        )
        sort_desc = filter_col6.toggle(
            "Descending",
            value=False,
            key="worklist_sort_desc",
        )
        worklist_min_age, worklist_max_age = st.slider(
            "Recall age range (years)",
            min_value=0,
            max_value=120,
            value=(0, 120),
            step=1,
            key="worklist_age_filter",
        )
        exclusion_col1, exclusion_col2, exclusion_col3, exclusion_col4 = st.columns(4)
        exclude_no_phone = exclusion_col1.toggle(
            "Exclude **no phone**",
            value=False,
            key="worklist_exclude_no_phone",
        )
        exclude_sent_recently = exclusion_col2.toggle(
            "Exclude **sent** in recent activity lookback days",
            value=True,
            key="worklist_exclude_sent_recently",
        )
        exclude_prepared_recently = exclusion_col3.toggle(
            "Exclude **prepared** in recent activity lookback days",
            value=True,
            key="worklist_exclude_prepared_recently",
        )
        lookback_days = exclusion_col4.number_input(
            "Recent activity lookback (days)",
            min_value=1,
            max_value=365,
            value=14,
            step=1,
            key="worklist_recent_activity_days",
            disabled=not (exclude_sent_recently or exclude_prepared_recently),
        )

        recent_batch_group_ids: set[str] = set()
        if exclude_prepared_recently and recall_batches:
            recent_batch_cutoff = pd.Timestamp.now(tz="Europe/London") - pd.Timedelta(days=int(lookback_days))
            for batch in recall_batches:
                created_at = pd.to_datetime(batch.get("created_at"), errors="coerce", utc=True)
                if pd.isna(created_at):
                    continue
                created_local = created_at.tz_convert("Europe/London")
                if created_local < recent_batch_cutoff:
                    continue
                for row in batch.get("export_rows") or []:
                    group_id = str(row.get("Group ID") or "")
                    if group_id:
                        recent_batch_group_ids.add(group_id)

        def _has_recent_send(recall: dict) -> bool:
            if not exclude_sent_recently:
                return False
            recent_cutoff = pd.Timestamp.now(tz="Europe/London") - pd.Timedelta(days=int(lookback_days))
            for attempt in attempts_by_group.get(recall["group_id"], []):
                outcome = str(attempt.get("outcome") or "").strip().lower()
                sent_at = pd.to_datetime(attempt.get("sent_at"), errors="coerce", utc=True)
                if pd.isna(sent_at):
                    continue
                sent_local = sent_at.tz_convert("Europe/London")
                if outcome in {"sent", "delivered"} and sent_local >= recent_cutoff:
                    return True
            return False

        filtered_recalls = [recall for recall in grouped_recalls if recall["status"] in status_filter]
        filtered_recalls = [
            recall
            for recall in filtered_recalls
            if classify_recall_workflow(recall, attempts_by_group.get(recall["group_id"], [])) in workflow_filter
        ]
        filtered_recalls = [
            recall
            for recall in filtered_recalls
            if (
                (age_years := _age_years_from_dob(recall.get("date_of_birth"))) is not None
                and worklist_min_age <= age_years <= worklist_max_age
            )
        ]
        if exclude_no_phone:
            filtered_recalls = [
                recall for recall in filtered_recalls if str(recall.get("phone") or "").strip()
            ]
        if exclude_prepared_recently and recent_batch_group_ids:
            filtered_recalls = [
                recall for recall in filtered_recalls if recall["group_id"] not in recent_batch_group_ids
            ]
        if exclude_sent_recently:
            filtered_recalls = [
                recall for recall in filtered_recalls if not _has_recent_send(recall)
            ]
        if vaccine_filter:
            needle = vaccine_filter.lower()
            filtered_recalls = [
                recall
                for recall in filtered_recalls
                if needle in str(recall.get("vaccines_display", "")).lower()
            ]
        if search_filter:
            needle = search_filter.lower()
            filtered_recalls = [
                recall
                for recall in filtered_recalls
                if needle in " ".join(
                    [
                        str(recall.get("full_name", "")),
                        str(recall.get("nhs_number", "")),
                        str(recall.get("phone", "")),
                        str(recall.get("email", "")),
                        str(recall.get("reason", "")),
                        str(recall.get("vaccines_display", "")),
                    ]
                ).lower()
            ]
        filtered_recalls = _sort_recalls(filtered_recalls, sort_by=sort_by, descending=sort_desc)

        if not filtered_recalls:
            st.warning("No recalls match the current filters.")
            return

        filtered_metric1, filtered_metric2, filtered_metric3, filtered_metric4 = st.columns(4)
        filtered_metric1.metric("Filtered Recalls", f"{len(filtered_recalls):,}")
        filtered_metric2.metric("Filtered Overdue", f"{sum(item['status'] == 'overdue' for item in filtered_recalls):,}")
        filtered_metric3.metric("Filtered Due Now", f"{sum(item['status'] == 'due_now' for item in filtered_recalls):,}")
        filtered_metric4.metric("Filtered Unvaccinated", f"{sum(item['status'] == 'unvaccinated' for item in filtered_recalls):,}")
        if exclude_prepared_recently or exclude_sent_recently or exclude_no_phone:
            st.caption(
                "Active exclusions: "
                f"{'no phone, ' if exclude_no_phone else ''}"
                f"{f'sent in last {int(lookback_days)} days, ' if exclude_sent_recently else ''}"
                f"{f'prepared in last {int(lookback_days)} days, ' if exclude_prepared_recently else ''}"
            .rstrip(", "))

        worklist_df = pd.DataFrame(
            [
                {
                    "Surgery": recall.get("surgery_code") or "—",
                    "Patient": recall.get("full_name") or "—",
                    "NHS Number": recall.get("nhs_number") or "—",
                    "DOB": _format_date(recall.get("date_of_birth")),
                    "Age": _format_age_from_dob(recall.get("date_of_birth")),
                    "Workflow": classify_recall_workflow(recall, attempts_by_group.get(recall["group_id"], [])),
                    "Phone": recall.get("phone") or "—",
                    "Email": recall.get("email") or "—",
                    "Vaccines": recall.get("vaccines_display") or "—",
                    "Due Date": _format_date(recall.get("due_date")),
                    "Status": recall.get("status") or "—",
                    "Priority": recall.get("priority") or "—",
                    "Items": recall.get("recommendation_count", 0),
                    "Attempts": recall.get("attempt_count", 0),
                    "Last Attempt": _format_ts(recall.get("last_attempt_at")),
                    "Reason": recall.get("reason") or "—",
                }
                for recall in filtered_recalls
            ]
        )
        st.dataframe(worklist_df, width="stretch", hide_index=True)

        with st.expander("Filtered cohort analytics", expanded=False):
            analytics_col1, analytics_col2, analytics_col3 = st.columns(3)
            status_summary_df = pd.DataFrame(
                [
                    {"Status": status, "Count": count}
                    for status, count in sorted(
                        (
                            (status, sum(item["status"] == status for item in filtered_recalls))
                            for status in sorted({item["status"] for item in filtered_recalls})
                        ),
                        key=lambda item: (-item[1], item[0]),
                    )
                ]
            )
            vaccine_summary_df = pd.DataFrame(
                [
                    {"Vaccine": vaccine, "Count": count}
                    for vaccine, count in sorted(
                        (
                            (vaccine, sum(vaccine in item.get("vaccines", []) for item in filtered_recalls))
                            for vaccine in sorted({v for item in filtered_recalls for v in item.get("vaccines", [])})
                        ),
                        key=lambda item: (-item[1], item[0]),
                    )[:15]
                ]
            )
            age_band_summary_df = pd.DataFrame(
                [
                    {"Age band": band, "Count": count}
                    for band, count in sorted(
                        (
                            (band, sum(_recall_age_band(item.get("date_of_birth")) == band for item in filtered_recalls))
                            for band in ["<1", "1-4", "5-11", "12-17", "18-64", "65-74", "75+", "Unknown"]
                        ),
                        key=lambda item: (-item[1], item[0]),
                    )
                    if count > 0
                ]
            )
            analytics_col1.dataframe(status_summary_df, width="stretch", hide_index=True)
            analytics_col2.dataframe(vaccine_summary_df, width="stretch", hide_index=True)
            analytics_col3.dataframe(age_band_summary_df, width="stretch", hide_index=True)

    recall_map = {recall["group_id"]: recall for recall in filtered_recalls}
    batch_candidate_rows = _build_recall_batch_rows(filtered_recalls, self_book_url=self_book_url)
    batch_candidate_map = {row["Group ID"]: row for row in batch_candidate_rows}

    with st.expander("2. Open Recall", expanded=False, icon=":material/person_check:"):
        selected_recall_id = st.selectbox(
            "Open recall",
            options=list(recall_map),
            format_func=lambda recall_id: _recall_option_label(recall_map[recall_id]),
            help="Inspect one filtered patient recall in detail before deciding whether and how to include it in a batch.",
            key="worklist_selected_recall",
        )
        selected_recall = recall_map[selected_recall_id]

        attempts = store.list_recall_attempts_for_recommendations(selected_recall["recommendation_ids"])
        try:
            patient_timeline = store.get_patient_timeline(
                user_context=user_context,
                surgery_id=str(selected_recall.get("surgery_id") or ""),
                nhs_number=str(selected_recall.get("nhs_number") or ""),
            )
        except (AuthenticationError, AuthorizationError, RuntimeError, ValueError) as exc:
            st.error(str(exc))
            patient_timeline = {"events": [], "attempts": []}

        patient_summary = summarize_patient_recall(
            selected_recall,
            attempts_by_group.get(selected_recall["group_id"], []),
            patient_timeline,
            sent_recently_days=14,
        )
        summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
        summary_col1.metric("Workflow", str(patient_summary["workflow_state"]))
        summary_col2.metric("Due Vaccines", f"{len(patient_summary['due_vaccines']):,}")
        summary_col3.metric("Last Vaccination", _format_date(patient_summary.get("last_vaccination_date")))
        last_outreach_label = _format_ts(patient_summary.get("last_outreach_at"))
        if patient_summary.get("last_outreach_method"):
            last_outreach_label = f"{last_outreach_label} | {patient_summary['last_outreach_method']}"
        summary_col4.metric("Last Outreach", last_outreach_label)
        st.info(f"Next action: {patient_summary['next_action']}")

        overlay_timeline_df = _build_recall_overlay_timeline_df(
            patient_timeline.get("events", []),
            selected_recall,
        )
        if overlay_timeline_df.empty:
            st.caption("No recorded vaccine events or recall due date are available to plot.")
        else:
            vaccine_order = sorted(overlay_timeline_df["Vaccine"].unique())
            grid_chart = _build_vaccine_grid_chart(vaccine_order)
            base_chart = alt.Chart(overlay_timeline_df).encode(
                x=alt.X("Event Date:T", axis=alt.Axis(grid=False)),
                y=alt.Y("Vaccine:N", sort=vaccine_order, axis=alt.Axis(grid=False)),
                tooltip=["Event Date:T", "Vaccine:N", "Marker:N", "Program:N", "Detail:N"],
            )
            schedule_chart = base_chart.transform_filter(alt.datum.Marker == "Routine schedule").mark_circle(size=130, opacity=0.95).encode(color=alt.value("#e8e8eb"))
            recorded_chart = base_chart.transform_filter(alt.datum.Marker == "Recorded").mark_circle(size=150).encode(color=alt.value("#4294c2"))
            recall_chart = base_chart.transform_filter(alt.datum.Marker == "Recall due").mark_circle(size=170).encode(color=alt.value("#ae4f4d"))
            st.altair_chart(
                alt.layer(grid_chart, schedule_chart, recorded_chart, recall_chart)
                .resolve_scale(color="independent")
                .properties(height=max(220, 34 * overlay_timeline_df["Vaccine"].nunique())),
                width="stretch",
            )

        detail_col1, detail_col2 = st.columns([1.2, 1])
        with detail_col1:
            st.markdown(f"### {selected_recall['full_name']}")
            st.write(f"NHS: `{selected_recall['nhs_number']}`")
            st.write(f"DOB: {_format_date(selected_recall.get('date_of_birth'))}")
            st.write(f"Age: {_format_age_from_dob(selected_recall.get('date_of_birth'))}")
            st.write(f"Phone: {selected_recall.get('phone') or '—'}")
            st.write(f"Email: {selected_recall.get('email') or '—'}")
            if selected_recall.get("surgery_code"):
                st.write(f"Surgery: `{selected_recall['surgery_code']}`")
            st.write(f"Vaccines: `{selected_recall['vaccines_display']}`")
            st.write(f"Status: `{selected_recall['status']}`")
            st.write(f"Due Date: {_format_date(selected_recall.get('due_date'))}")
            st.write(f"Priority: `{selected_recall['priority']}`")
            for reason in selected_recall["reasons"]:
                st.write(f"- {reason}")
        with detail_col2:
            st.markdown("### Metadata")
            st.json(selected_recall.get("explanation") or {})

        if attempts:
            attempts_df = pd.DataFrame(
                [
                    {
                        "Sent At": _format_ts(attempt.get("sent_at")),
                        "Method": attempt.get("communication_method") or "—",
                        "Outcome": attempt.get("outcome") or "—",
                        "Staff": attempt.get("staff_member") or "—",
                        "Recall Items": attempt.get("recommendation_count") or "—",
                        "Message": attempt.get("notes") or "—",
                    }
                    for attempt in attempts
                ]
            )
            st.dataframe(attempts_df, width="stretch", hide_index=True)
        else:
            st.caption("No recall attempts have been logged for this patient recall yet.")

        action_col1, action_col2 = st.columns(2)
        if action_col1.button("Mark complete", key=f"complete_{selected_recall['group_id']}", width="stretch", icon=":material/done_outline:"):
            try:
                store.close_recall_group(user_context, selected_recall["recommendation_ids"], "complete")
            except (AuthenticationError, AuthorizationError, RuntimeError, ValueError) as exc:
                st.error(str(exc))
            else:
                st.success("Recall marked complete.")
                _invalidate_data_caches()
                st.rerun()
        action_col1.caption("Use when this recall has been resolved and no further action is needed.")
        if action_col2.button("Suppress recall", key=f"suppress_{selected_recall['group_id']}", width="stretch", icon=":material/close:"):
            try:
                store.close_recall_group(user_context, selected_recall["recommendation_ids"], "suppressed")
            except (AuthenticationError, AuthorizationError, RuntimeError, ValueError) as exc:
                st.error(str(exc))
            else:
                st.success("Recall suppressed.")
                _invalidate_data_caches()
                st.rerun()
        action_col2.caption("Use when you want to remove it from the worklist without treating it as completed.")

    with st.expander("3. Prepare Batch", expanded=False, icon=":material/batch_prediction:"):
        batch_group_ids = st.multiselect(
            "Select patient recalls for batch",
            options=list(recall_map),
            format_func=lambda recall_id: _recall_option_label(recall_map[recall_id]),
            key="worklist_generic_batch_selection",
            help="Create a generic recall batch from the filtered cohort. You can choose SMS, email, letter, or call later in the delivery step.",
        )
        if batch_group_ids:
            selected_batch_rows = [batch_candidate_map[recall_id] for recall_id in batch_group_ids]
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "Patient": row["Patient"],
                            "Phone": row["Phone"] or "—",
                            "Email": row["Email"] or "—",
                            "Vaccines": row["Vaccines"],
                            "Due Date": row["Due Date"],
                        }
                        for row in selected_batch_rows
                    ]
                ),
                width="stretch",
                hide_index=True,
            )
            with st.form("create_recall_batch"):
                batch_staff = st.text_input("Prepared by", value=user_context.full_name or user_context.email)
                create_batch = st.form_submit_button("Create recall batch", type="primary", icon=":material/batch_prediction:")

            if create_batch:
                try:
                    selected_surgery_ids = {
                        recall_map[group_id].get("surgery_id")
                        for group_id in batch_group_ids
                        if recall_map[group_id].get("surgery_id")
                    }
                    if len(selected_surgery_ids) != 1:
                        raise ValueError("Recall batch preparation must be scoped to a single surgery.")
                    batch_result = store.create_recall_batch(
                        user_context=user_context,
                        surgery_id=str(next(iter(selected_surgery_ids))),
                        prepared_by_name=batch_staff,
                        selected_rows=selected_batch_rows,
                        selection_summary={
                            "statuses": status_filter,
                            "workflow_filter": workflow_filter,
                            "vaccine_filter": vaccine_filter,
                            "search_filter": search_filter,
                            "age_range": [worklist_min_age, worklist_max_age],
                            "selected_group_ids": batch_group_ids,
                        },
                        self_book_url=self_book_url,
                    )
                except (AuthenticationError, AuthorizationError, RuntimeError, ValueError, Exception) as exc:
                    st.error(str(exc))
                else:
                    if batch_result.get("deduplicated"):
                        st.info(
                            f"Reused existing recall batch {batch_result['id']} instead of creating a duplicate."
                        )
                    else:
                        st.success(
                            f"Created recall batch {batch_result['id']} with {batch_result.get('selected_count', len(selected_batch_rows))} selected patient recalls."
                        )
                    _invalidate_data_caches()
                    st.rerun()
        else:
            st.caption("Select one or more filtered recalls to create a batch.")

    with st.expander("4. Deliver Batch", expanded=False, icon=":material/send:"):
        if recall_batch_error:
            st.error(f"Generic recall batches are unavailable until `sql/007_recall_batches.sql` is applied: {recall_batch_error}")
        elif not recall_batches:
            st.info("No recall batches are available yet. Create one in step 3 first.")
        else:
            deliver_batch_id = st.selectbox(
                "Choose batch to deliver",
                options=[batch["id"] for batch in recall_batches],
                format_func=lambda batch_id: _format_batch_label(next(batch for batch in recall_batches if batch["id"] == batch_id)),
                key="deliver_recall_batch_id",
            )
            deliver_batch = next(batch for batch in recall_batches if batch["id"] == deliver_batch_id)
            delivery_method = st.selectbox(
                "Delivery method",
                options=["sms", "email", "letter", "call"],
                index=["sms", "email", "letter", "call"].index(deliver_batch.get("delivery_method") or "sms"),
                key="deliver_recall_batch_method",
            )
            batch_rows = deliver_batch.get("export_rows") or []
            ready_rows, blocked_rows = _classify_batch_rows_for_method(batch_rows, delivery_method)

            delivery_metric1, delivery_metric2, delivery_metric3 = st.columns(3)
            delivery_metric1.metric("Selected", f"{len(batch_rows):,}")
            delivery_metric2.metric("Ready", f"{len(ready_rows):,}")
            delivery_metric3.metric("Blocked", f"{len(blocked_rows):,}")

            preview_columns = ["Patient", "Phone", "Email", "Vaccines", "Due Date"]
            if delivery_method == "sms":
                preview_columns.append("SMS Message")
            elif delivery_method == "email":
                preview_columns.extend(["Email Subject", "Email Message"])
            st.dataframe(
                pd.DataFrame([{column: row.get(column) or "—" for column in preview_columns} for row in batch_rows]),
                width="stretch",
                hide_index=True,
            )

            if blocked_rows:
                st.dataframe(
                    pd.DataFrame(
                        [
                            {
                                "Patient": row.get("Patient") or "—",
                                "Phone": row.get("Phone") or "—",
                                "Email": row.get("Email") or "—",
                                "Exclusion": row.get("Exclusion") or "—",
                            }
                            for row in blocked_rows
                        ]
                    ),
                    width="stretch",
                    hide_index=True,
                )

            if delivery_method == "sms":
                sms_rows = [
                    {
                        "Patient": row["Patient"],
                        "NHS Number": row["NHS Number"],
                        "Firstname": row["Firstname"],
                        "DOB": row["DOB"],
                        "Phone": row["Phone"],
                        "Vaccines": row["Vaccines"],
                        "Due Date": row["Due Date"],
                        "Message": row["SMS Message"],
                        "Recommendation IDs": row["Recommendation IDs"],
                    }
                    for row in ready_rows
                ]
                accurx_df = _build_accurx_sms_df(sms_rows)
                st.download_button(
                    "Download Self-book Accurx CSV",
                    data=accurx_df.to_csv(index=False),
                    file_name=f"safestart2_accurx_batch_{deliver_batch_id}.csv",
                    mime="text/csv",
                    disabled=accurx_df.empty,
                    key=f"deliver_batch_sms_accurx_{deliver_batch_id}",
                )
                smsworks_payload = build_smsworks_dry_run_payload(sms_rows, sender=sms_sender_id or deliver_batch.get("surgery_code"))
                with st.expander("The SMS Works", expanded=False):
                    st.caption("Review provider-ready SMS payloads, then choose whether to send.")
                    st.download_button(
                        "Download The SMS Works JSON",
                        data=json.dumps(smsworks_payload, indent=2),
                        file_name=f"safestart2_smsworks_batch_{deliver_batch_id}.json",
                        mime="application/json",
                    )
                    live_send_enabled = st.checkbox(
                        "Actually send these SMS messages via The SMS Works API",
                        value=False,
                        key=f"send_recall_batch_sms_live_{deliver_batch_id}",
                        disabled=not bool(smsworks_settings and smsworks_settings.jwt),
                    )
                    if st.button(
                        "Send batch via The SMS Works",
                        key=f"send_recall_batch_sms_{deliver_batch_id}",
                        width="stretch",
                        type="primary",
                        icon=":material/sms:",
                        disabled=not bool(smsworks_settings and smsworks_settings.jwt),
                    ):
                        if not live_send_enabled:
                            st.error("Tick the live-send checkbox before sending the batch.")
                        elif not smsworks_payload["requests"]:
                            st.error("There are no valid SMS requests in this batch.")
                        else:
                            send_result = send_smsworks_requests(smsworks_payload["requests"], jwt=smsworks_settings.jwt)
                            sent_rows = []
                            failed_rows = []
                            for result in send_result["results"]:
                                patient_name = str(result["metadata"].get("patient") or "Patient")
                                provider_note = json.dumps(result.get("response") or {}) if result.get("response") else (result.get("error") or "")
                                notes = f"{result['body'].get('content') or ''} | smsworks={provider_note}" if provider_note else str(result["body"].get("content") or "")
                                store.log_recall_attempts(
                                    user_context=user_context,
                                    recommendation_ids=list(result["metadata"].get("recommendation_ids") or []),
                                    communication_method="bulk_sms",
                                    staff_member=user_context.full_name or user_context.email,
                                    outcome="sent" if result["success"] else "failed",
                                    notes=notes,
                                    recall_batch_id=deliver_batch_id,
                                )
                                if result["success"]:
                                    sent_rows.append(result)
                                    st.toast(f"SMS sent to {patient_name}")
                                else:
                                    failed_rows.append(result)
                                    st.toast(f"SMS failed for {patient_name}")
                            if sent_rows:
                                store.set_recall_batch_status(user_context, deliver_batch_id, "sent", delivery_method="sms")
                            elif failed_rows:
                                store.set_recall_batch_status(user_context, deliver_batch_id, "failed", delivery_method="sms")
                            if sent_rows and not failed_rows:
                                st.success(f"Sent {len(sent_rows):,} SMS messages successfully.")
                            elif failed_rows and not sent_rows:
                                st.error(f"All SMS sends failed. Failed messages: {len(failed_rows):,}.")
                            else:
                                st.warning(f"Partial send complete. Sent {len(sent_rows):,}, failed {len(failed_rows):,}.")
                            st.rerun()
            elif delivery_method == "email":
                email_rows = [
                    {
                        "Patient": row["Patient"],
                        "NHS Number": row["NHS Number"],
                        "Firstname": row["Firstname"],
                        "DOB": row["DOB"],
                        "Phone": row["Phone"],
                        "Email": row["Email"],
                        "Reply To": row["Reply To"],
                        "Subject": row["Email Subject"],
                        "Message": row["Email Message"],
                        "Recommendation IDs": row["Recommendation IDs"],
                    }
                    for row in ready_rows
                ]
                resend_payload = build_resend_requests(
                    email_rows,
                    sender_name=deliver_batch.get("sms_sender_id"),
                )
                accurx_email_df = _build_accurx_email_df(email_rows)
                st.download_button(
                    "Download Accurx Self-book Links CSV",
                    data=accurx_email_df.to_csv(index=False),
                    file_name=f"safestart2_accurx_email_batch_{deliver_batch_id}.csv",
                    mime="text/csv",
                    disabled=accurx_email_df.empty,
                    key=f"deliver_batch_email_accurx_{deliver_batch_id}",
                )
                with st.expander("Resend", expanded=False):
                    st.caption(
                        f"Emails are sent from `{resend_payload['summary']['sender']}` and reply to the surgery contact email."
                    )
                    st.download_button(
                        "Download Accurx Self-book Links CSV",
                        data=accurx_email_df.to_csv(index=False),
                        file_name=f"safestart2_accurx_email_batch_{deliver_batch_id}.csv",
                        mime="text/csv",
                        disabled=accurx_email_df.empty,
                        key=f"deliver_batch_email_accurx_resend_{deliver_batch_id}",
                    )
                    st.download_button(
                        "Download Resend JSON",
                        data=json.dumps(resend_payload, indent=2),
                        file_name=f"safestart2_resend_batch_{deliver_batch_id}.json",
                        mime="application/json",
                        key=f"deliver_batch_email_resend_json_{deliver_batch_id}",
                    )
                    live_send_email = st.checkbox(
                        "Actually send these emails via Resend",
                        value=False,
                        key=f"send_recall_batch_email_live_{deliver_batch_id}",
                        disabled=not bool(resend_settings and resend_settings.api_key),
                    )
                    if st.button(
                        "Send batch via Resend",
                        key=f"send_recall_batch_email_{deliver_batch_id}",
                        width="stretch",
                        type="primary",
                        icon=":material/mail:",
                        disabled=not bool(resend_settings and resend_settings.api_key),
                    ):
                        if not live_send_email:
                            st.error("Tick the live-send checkbox before sending the batch.")
                        elif not resend_payload["requests"]:
                            st.error("There are no valid email requests in this batch.")
                        else:
                            send_result = send_resend_requests(resend_payload["requests"], api_key=resend_settings.api_key)
                            sent_rows = []
                            failed_rows = []
                            for result in send_result["results"]:
                                patient_name = str(result["metadata"].get("patient") or "Patient")
                                subject = str(result["metadata"].get("subject") or "")
                                email_message = next((row["Message"] for row in email_rows if row["Patient"] == patient_name and row["Subject"] == subject), "")
                                provider_note = json.dumps(result.get("response") or {}) if result.get("response") else (result.get("error") or "")
                                notes = f"{subject} | {email_message} | resend={provider_note}" if provider_note else f"{subject} | {email_message}"
                                store.log_recall_attempts(
                                    user_context=user_context,
                                    recommendation_ids=list(result["metadata"].get("recommendation_ids") or []),
                                    communication_method="email",
                                    staff_member=user_context.full_name or user_context.email,
                                    outcome="sent" if result["success"] else "failed",
                                    notes=notes,
                                    recall_batch_id=deliver_batch_id,
                                )
                                if result["success"]:
                                    sent_rows.append(result)
                                    st.toast(f"Email sent to {patient_name}")
                                else:
                                    failed_rows.append(result)
                                    st.toast(f"Email failed for {patient_name}")
                            if sent_rows:
                                store.set_recall_batch_status(user_context, deliver_batch_id, "sent", delivery_method="email")
                            elif failed_rows:
                                store.set_recall_batch_status(user_context, deliver_batch_id, "failed", delivery_method="email")
                            if sent_rows and not failed_rows:
                                st.success(f"Sent {len(sent_rows):,} emails successfully.")
                            elif failed_rows and not sent_rows:
                                st.error(f"All email sends failed. Failed messages: {len(failed_rows):,}.")
                            else:
                                st.warning(f"Partial send complete. Sent {len(sent_rows):,}, failed {len(failed_rows):,}.")
                            st.rerun()
            else:
                st.info(
                    f"`{delivery_method}` does not have a live provider integration in the app. "
                    "Use step 6 to record manual batch outcomes after handling it outside the app."
                )

    with st.expander("5. Batch History", expanded=False, icon=":material/history:"):
        if recall_batch_error:
            st.error(f"Recall batch history is unavailable until `sql/007_recall_batches.sql` is applied: {recall_batch_error}")
        elif not recall_batches:
            st.info("No recall batches are visible for this surgery yet.")
        else:
            history_df = pd.DataFrame(
                [
                    {
                        "Prepared At": _format_ts(batch.get("created_at")),
                        "Surgery": batch.get("surgery_code") or "—",
                        "Prepared By": batch.get("prepared_by_name") or batch.get("prepared_by_email") or "—",
                        "Method": batch.get("delivery_method") or "unassigned",
                        "Status": batch.get("status") or "—",
                        "Selected": batch.get("selected_count") or len(batch.get("export_rows") or []),
                        "Ready": batch.get("ready_count") or 0,
                        "Blocked": batch.get("blocked_count") or 0,
                    }
                    for batch in recall_batches
                ]
            )
            st.dataframe(history_df, width="stretch", hide_index=True)
            history_batch_id = st.selectbox(
                "Inspect batch history item",
                options=[batch["id"] for batch in recall_batches],
                format_func=lambda batch_id: _format_batch_label(next(batch for batch in recall_batches if batch["id"] == batch_id)),
                key="history_recall_batch_id",
            )
            history_batch = next(batch for batch in recall_batches if batch["id"] == history_batch_id)
            history_preview_df = pd.DataFrame(history_batch.get("export_rows") or [])
            if not history_preview_df.empty:
                st.dataframe(history_preview_df, width="stretch", hide_index=True)
                export_rows = history_batch.get("export_rows") or []
                history_method = str(history_batch.get("delivery_method") or "").strip().lower()
                history_sms_rows = [
                    {
                        "NHS Number": row.get("NHS Number"),
                        "Phone": row.get("Phone"),
                        "DOB": row.get("DOB"),
                        "Firstname": row.get("Firstname"),
                    }
                    for row in export_rows
                    if str(row.get("Phone") or "").strip()
                ]
                history_email_rows = [
                    {
                        "NHS Number": row.get("NHS Number"),
                        "Phone": row.get("Phone"),
                        "DOB": row.get("DOB"),
                        "Firstname": row.get("Firstname"),
                    }
                    for row in export_rows
                    if str(row.get("Phone") or "").strip()
                ]
                download_col1, download_col2 = st.columns(2)
                if history_method in {"", "sms"} and history_sms_rows:
                    download_col1.download_button(
                        "Download Self-book Accurx CSV",
                        data=_build_accurx_sms_df(history_sms_rows).to_csv(index=False),
                        file_name=f"safestart2_accurx_batch_{history_batch_id}.csv",
                        mime="text/csv",
                        key=f"history_batch_sms_accurx_{history_batch_id}",
                    )
                if history_method in {"", "email"} and history_email_rows:
                    download_col2.download_button(
                        "Download Accurx Self-book Links CSV",
                        data=_build_accurx_email_df(history_email_rows).to_csv(index=False),
                        file_name=f"safestart2_accurx_email_batch_{history_batch_id}.csv",
                        mime="text/csv",
                        key=f"history_batch_email_accurx_{history_batch_id}",
                    )
            with st.expander("Batch filter snapshot", expanded=False):
                st.json(history_batch.get("selection_summary") or {})

    with st.expander("6. Manual Batch Outcome", expanded=False, icon=":material/attach_file:"):
        st.caption("Use this when a batch was handled outside the app, for example by letter or phone call.")
        if recall_batch_error:
            st.error(f"Manual batch outcomes are unavailable until `sql/007_recall_batches.sql` is applied: {recall_batch_error}")
        elif not recall_batches:
            st.info("No recall batches are available to update.")
        else:
            with st.form("manual_recall_batch_outcome"):
                manual_batch_id = st.selectbox(
                    "Batch",
                    options=[batch["id"] for batch in recall_batches],
                    format_func=lambda batch_id: _format_batch_label(next(batch for batch in recall_batches if batch["id"] == batch_id)),
                    key="manual_recall_batch_id",
                )
                manual_col1, manual_col2, manual_col3 = st.columns(3)
                manual_method = manual_col1.selectbox("Method", options=["letter", "call", "sms", "email"])
                manual_outcome = manual_col2.selectbox("Outcome", options=RECALL_OUTCOME_OPTIONS, index=0)
                manual_staff = manual_col3.text_input("Staff member", value=user_context.full_name or user_context.email)
                manual_note = st.text_area("Notes for all items in this batch", height=120)
                apply_manual_batch = st.form_submit_button("Apply manual batch outcome", icon=":material/attach_file:")

            if apply_manual_batch:
                try:
                    manual_batch = next(batch for batch in recall_batches if batch["id"] == manual_batch_id)
                    notes_by_row = {
                        str(row.get("Group ID") or ""): manual_note.strip() or None
                        for row in (manual_batch.get("export_rows") or [])
                    }
                    result = store.log_recall_batch_outcome(
                        user_context=user_context,
                        batch_id=manual_batch_id,
                        communication_method=manual_method,
                        outcome=manual_outcome,
                        staff_member=manual_staff,
                        notes_by_row=notes_by_row,
                    )
                except (AuthenticationError, AuthorizationError, RuntimeError, ValueError, Exception) as exc:
                    st.error(str(exc))
                else:
                    st.success(
                        f"Updated batch {manual_batch_id} to {result['status']} and logged {result['logged_attempts']} recall attempt rows."
                    )
                    _invalidate_data_caches()
                    st.rerun()


def _render_import_tab(
    store: SupabaseStore,
    user_context: UserContext,
    surgery_code: str,
    surgery_name: str,
    uploader_email: str,
    sms_sender_id: str,
    self_book_url: Optional[str],
    reference_date: date,
    lookahead_days: int,
    min_age_years: int,
    max_age_years: int,
) -> None:
    st.subheader("Import History")
    history_surgery_id = user_context.surgery_id
    if user_context.is_superuser:
        selected_surgery = store.find_surgery_by_code(surgery_code) if surgery_code.strip() else None
        history_surgery_id = selected_surgery["id"] if selected_surgery else None
    try:
        batches = store.list_import_batches(user_context, surgery_id=history_surgery_id)
    except AuthorizationError as exc:
        st.error(str(exc))
        batches = []
    if batches:
        history_df = pd.DataFrame(
            [
                {
                    "Imported At": _format_ts(batch.get("imported_at")),
                    "Surgery": batch.get("surgery_code") or "—",
                    "Source": batch.get("source_filename") or "—",
                    "Rows": batch.get("row_count") or 0,
                    "Patients": batch.get("patient_count") or 0,
                    "Recommendations": batch.get("recommendation_count") or 0,
                    "Unvaccinated": batch.get("unvaccinated_count") or 0,
                    "Uploaded By": batch.get("uploaded_by_email") or "—",
                    "Age Range": _parse_batch_notes(batch.get("notes")).get("age_range", "—"),
                }
                for batch in batches
            ]
        )
        st.dataframe(history_df, width="stretch", hide_index=True)

        batch_map = {batch["id"]: batch for batch in batches}
        selected_batch_id = st.selectbox(
            "Inspect import batch",
            options=list(batch_map),
            format_func=lambda batch_id: (
                f"{_format_ts(batch_map[batch_id].get('imported_at'))} | "
                f"{batch_map[batch_id].get('surgery_code') or '—'} | "
                f"{batch_map[batch_id].get('source_filename') or '—'}"
            ),
            key="import_history_selected_batch",
        )
        live_detail_cache_key = f"import_batch_live_detail_{selected_batch_id}"
        load_live_counts = st.button(
            "Load live stored counts",
            key=f"load_live_batch_detail_{selected_batch_id}",
            help=(
                "Run direct Supabase counts for this batch. Use this only if you need to "
                "verify stored table counts beyond the lightweight import summary."
            ),
        )
        try:
            if load_live_counts:
                batch_detail = store.get_import_batch_detail(
                    user_context,
                    selected_batch_id,
                    include_live_counts=True,
                )
                st.session_state[live_detail_cache_key] = batch_detail
            else:
                batch_detail = st.session_state.get(live_detail_cache_key)
                if not batch_detail:
                    batch_detail = store.get_import_batch_detail(user_context, selected_batch_id)
        except Exception as exc:
            st.error(str(exc))
            batch_detail = None

        if batch_detail:
            detail_notes = _parse_batch_notes(batch_detail.get("notes"))
            detail_source = batch_detail.get("detail_source") or "import_metadata"
            if detail_source == "live_counts":
                st.caption("Showing live stored counts from Supabase for this batch.")
            elif detail_source == "stored_summary":
                st.caption("Showing lightweight stored summary captured when this batch was persisted.")
            else:
                st.caption(
                    "Showing import-time summary only. Use `Load live stored counts` if you need "
                    "a direct database check for this older batch."
                )
            detail_col1, detail_col2, detail_col3, detail_col4 = st.columns(4)
            detail_col1.metric("Stored Events", f"{batch_detail.get('event_count', 0):,}")
            detail_col2.metric(
                "Stored Recommendations",
                f"{batch_detail.get('persisted_recommendation_count', 0):,}",
            )
            detail_col3.metric(
                "Active Recommendations",
                f"{batch_detail.get('active_recommendation_count', 0):,}",
            )
            detail_col4.metric("Age Range", detail_notes.get("age_range", "—"))

            summary_col1, summary_col2 = st.columns(2)
            with summary_col1:
                status_counts = batch_detail.get("status_counts") or {}
                if status_counts:
                    status_df = pd.DataFrame(
                        [
                            {"Status": status, "Recommendations": count}
                            for status, count in sorted(
                                status_counts.items(),
                                key=lambda item: (-item[1], item[0]),
                            )
                        ]
                    )
                    st.caption("Recommendation status counts")
                    st.dataframe(status_df, width="stretch", hide_index=True)
                else:
                    st.caption("No stored recommendation statuses for this batch.")
            with summary_col2:
                vaccine_counts = batch_detail.get("vaccine_counts") or {}
                if vaccine_counts:
                    vaccine_df = pd.DataFrame(
                        [
                            {"Vaccine group": vaccine, "Recommendations": count}
                            for vaccine, count in sorted(
                                vaccine_counts.items(),
                                key=lambda item: (-item[1], item[0]),
                            )
                        ]
                    )
                    st.caption("Recommendation vaccine counts")
                    st.dataframe(vaccine_df, width="stretch", hide_index=True)
                else:
                    st.caption("No stored vaccine counts for this batch.")

            st.caption(
                "Persist check: "
                f"{batch_detail.get('persisted_recommendation_count', 0):,} stored recommendations "
                f"against {batch_detail.get('recommendation_count', 0):,} generated at import time."
            )

            st.divider()
            st.caption("Import comparison")
            if st.button("Run comparison against previous batch", key=f"run_batch_comparison_{selected_batch_id}"):
                try:
                    batch_comparison = store.get_import_batch_comparison(user_context, selected_batch_id)
                except Exception as exc:
                    st.error(str(exc))
                    batch_comparison = None

                if batch_comparison:
                    previous_batch = batch_comparison.get("previous_batch")
                    comparison = batch_comparison.get("comparison")
                    if not previous_batch or not comparison:
                        st.info("No earlier import batch is available for comparison.")
                    else:
                        compare_col1, compare_col2, compare_col3, compare_col4, compare_col5 = st.columns(5)
                        compare_col1.metric("Previous Batch", _format_ts(previous_batch.get("imported_at")))
                        compare_col2.metric("New Vaccine Events", f"{comparison.get('new_vaccine_events', 0):,}")
                        compare_col3.metric("New Recalls", f"{comparison.get('new_recall_count', 0):,}")
                        compare_col4.metric("Resolved Recalls", f"{comparison.get('resolved_recall_count', 0):,}")
                        compare_col5.metric(
                            "Patients With Status Changes",
                            f"{comparison.get('patients_with_status_changes', 0):,}",
                        )

                        compare_table_col1, compare_table_col2 = st.columns(2)
                        with compare_table_col1:
                            new_recall_counts = comparison.get("new_recall_vaccine_counts") or []
                            if new_recall_counts:
                                st.caption("New recall vaccines")
                                st.dataframe(
                                    pd.DataFrame(
                                        [
                                            {
                                                "Vaccine group": row["vaccine_group"],
                                                "Count": row["count"],
                                            }
                                            for row in new_recall_counts
                                        ]
                                    ),
                                    width="stretch",
                                    hide_index=True,
                                )
                            else:
                                st.caption("No new recalls versus the previous batch.")
                        with compare_table_col2:
                            resolved_counts = comparison.get("resolved_recall_vaccine_counts") or []
                            if resolved_counts:
                                st.caption("Resolved recall vaccines")
                                st.dataframe(
                                    pd.DataFrame(
                                        [
                                            {
                                                "Vaccine group": row["vaccine_group"],
                                                "Count": row["count"],
                                            }
                                            for row in resolved_counts
                                        ]
                                    ),
                                    width="stretch",
                                    hide_index=True,
                                )
                            else:
                                st.caption("No recalls were resolved versus the previous batch.")

                        status_changes = comparison.get("status_changes") or []
                        if status_changes:
                            st.caption("Status changes")
                            st.dataframe(
                                pd.DataFrame(
                                    [
                                        {
                                            "Patient": row["patient_name"],
                                            "NHS Number": row["nhs_number"],
                                            "Vaccine": row["vaccine_group"],
                                            "Due Date": _format_date(row["due_date"]),
                                            "Previous Status": row["previous_status"],
                                            "Current Status": row["current_status"],
                                        }
                                        for row in status_changes
                                    ]
                                ),
                                width="stretch",
                                hide_index=True,
                            )
                        else:
                            st.caption("No status changes versus the previous batch.")

            try:
                unmapped_event_count = store.count_unmapped_vaccination_events(
                    user_context=user_context,
                    surgery_id=str(batch_detail.get("surgery_id") or ""),
                )
            except (AuthenticationError, AuthorizationError, RuntimeError, ValueError) as exc:
                st.error(str(exc))
                unmapped_event_count = None

            st.divider()
            st.caption("Repair tools")
            repair_col1, repair_col2 = st.columns([1, 2])
            repair_col1.metric(
                "Unmapped Events",
                f"{unmapped_event_count:,}" if unmapped_event_count is not None else "—",
            )
            rebuild_confirm = repair_col2.checkbox(
                "I understand rebuild will clear existing imported data for this surgery and recreate it from this batch.",
                key=f"rebuild_confirm_{selected_batch_id}",
            )

            action_col1, action_col2 = st.columns(2)
            if action_col1.button(
                "Delete unmapped events only",
                key=f"delete_unmapped_events_{selected_batch_id}",
                width="stretch",
            ):
                try:
                    deleted = store.delete_unmapped_vaccination_events(
                        user_context=user_context,
                        surgery_id=str(batch_detail.get("surgery_id") or ""),
                    )
                except (AuthenticationError, AuthorizationError, RuntimeError, ValueError) as exc:
                    st.error(str(exc))
                else:
                    st.success(f"Deleted {deleted:,} unmapped vaccination events.")
                    _invalidate_data_caches()
                    st.rerun()

            if action_col2.button(
                "Rebuild surgery from this batch",
                key=f"rebuild_batch_{selected_batch_id}",
                width="stretch",
            ):
                if not rebuild_confirm:
                    st.error("Tick the confirmation box before rebuilding the surgery from this batch.")
                else:
                    progress_placeholder = st.empty()
                    progress_bar = st.progress(0)

                    def report_rebuild_progress(stage: str, progress: float, message: str) -> None:
                        progress_bar.progress(min(max(progress, 0.0), 1.0))
                        progress_placeholder.info(message)

                    with st.spinner("Rebuilding surgery from stored import rows...", show_time=True):
                        try:
                            result = store.rebuild_surgery_from_batch(
                                user_context=user_context,
                                batch_id=selected_batch_id,
                                progress_callback=report_rebuild_progress,
                            )
                        except (AuthenticationError, AuthorizationError, RuntimeError, ValueError) as exc:
                            progress_bar.empty()
                            progress_placeholder.empty()
                            st.error(str(exc))
                        else:
                            progress_bar.progress(1.0)
                            progress_placeholder.success("Rebuild complete.")
                            st.success(
                                f"Rebuilt surgery from batch {selected_batch_id}. "
                                f"Patients: {result['patients']}, events: {result['events']}, "
                                f"recommendations: {result['recommendations']}."
                            )
                            _invalidate_data_caches()
                            st.rerun()
    else:
        st.caption("No import batches are visible for this surgery yet.")

    st.divider()
    st.subheader("Import File")
    uploaded_file = st.file_uploader(
        "Upload an ImmunizeMe CSV or Excel export",
        type=["csv", "xlsx", "xls"],
    )

    default_dataset = Path("/Users/janduplessis/Library/CloudStorage/OneDrive-NHS/python-data/ImmunizeMe_020625.csv")
    use_default = st.toggle(
        "Use attached ImmunizeMe_020625.csv",
        value=False,
    )

    df = None
    source_name = None
    if uploaded_file is not None:
        df = load_dataframe(uploaded_file)
        source_name = uploaded_file.name
    elif use_default and default_dataset.exists():
        df = sanitize_dataframe_columns(pd.read_csv(default_dataset))
        source_name = default_dataset.name

    if df is None:
        st.info("Upload a file or enable the attached dataset toggle to begin processing.")
        return

    original_row_count = len(df)
    df, invalid_dob_count = _filter_dataframe_by_age(
        df,
        reference_date=reference_date,
        min_age_years=min_age_years,
        max_age_years=max_age_years,
    )
    filtered_out_count = original_row_count - len(df)

    if min_age_years > 0 or max_age_years < 120:
        st.caption(
            f"Age filter applied: {min_age_years} to {max_age_years} years. "
            f"Included {len(df):,} of {original_row_count:,} source rows."
        )
    if invalid_dob_count:
        st.caption(f"Rows with unreadable DOB values before filtering: {invalid_dob_count:,}.")
    if df.empty:
        st.warning("No source rows matched the selected age range.")
        return

    override_surgery_id = user_context.surgery_id
    global_only_overrides = False
    if user_context.is_superuser:
        selected_surgery = store.find_surgery_by_code(surgery_code)
        if selected_surgery:
            override_surgery_id = selected_surgery["id"]
        else:
            override_surgery_id = None
            global_only_overrides = True

    overrides = store.get_alias_overrides(
        surgery_id=override_surgery_id,
        global_only=global_only_overrides,
    )
    processed = process_immunizeme_dataframe(
        df,
        reference_date=reference_date,
        lookahead_days=lookahead_days,
        overrides=overrides,
    )

    st.success(
        f"Processed {processed.raw_rows:,} rows into {len(processed.patients):,} patients and "
        f"{len(processed.recommendations):,} recommendations."
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Patients", f"{len(processed.patients):,}")
    col2.metric("Recommendations", f"{len(processed.recommendations):,}")
    col3.metric("Unvaccinated", f"{processed.unvaccinated_patients:,}")
    col4.metric("Filtered Rows", f"{filtered_out_count:,}")

    recommendation_columns = [
        "NHS Number",
        "Patient",
        "DOB",
        "Phone",
        "Email",
        "Type",
        "Vaccine",
        "Program",
        "Due Date",
        "Status",
        "Priority",
        "Reason",
    ]
    recommendation_rows = [
        {
            "NHS Number": item.patient_nhs_number,
            "Patient": item.patient_name,
            "DOB": item.date_of_birth.strftime("%d/%m/%Y"),
            "Phone": item.phone or "—",
            "Email": item.email or "—",
            "Type": item.recommendation_type,
            "Vaccine": item.vaccine_group,
            "Program": item.program_area,
            "Due Date": item.due_date.strftime("%d/%m/%Y") if item.due_date else "—",
            "Status": item.status,
            "Priority": item.priority,
            "Reason": item.reason,
        }
        for item in processed.recommendations
    ]
    recommendation_df = pd.DataFrame(recommendation_rows, columns=recommendation_columns)

    patient_columns = [
        "NHS Number",
        "Patient",
        "DOB",
        "Sex",
        "Phone",
        "Email",
        "Registration Date",
        "Vaccines On Record",
        "Unvaccinated",
    ]
    patient_rows = [
        {
            "NHS Number": patient.nhs_number,
            "Patient": patient.full_name,
            "DOB": patient.date_of_birth.strftime("%d/%m/%Y"),
            "Sex": patient.sex or "—",
            "Phone": patient.phone or "—",
            "Email": patient.email or "—",
            "Registration Date": patient.registration_date.strftime("%d/%m/%Y")
            if patient.registration_date
            else "—",
            "Vaccines On Record": len(patient.vaccine_events),
            "Unvaccinated": patient.only_unknown_marker,
        }
        for patient in processed.patients
    ]
    patient_df = pd.DataFrame(patient_rows, columns=patient_columns)

    recommendations_tab, unvaccinated_tab, patients_tab, diagnostics_tab = st.tabs(
        ["Recommendations", "Unvaccinated", "Patients", "Diagnostics"]
    )

    with recommendations_tab:
        if recommendation_df.empty:
            st.info("No recommendations generated for this dataset and reference date.")
        else:
            status_filter = st.multiselect(
                "Filter recommendation statuses",
                options=sorted(recommendation_df["Status"].unique()),
                default=sorted(recommendation_df["Status"].unique()),
            )
            vaccine_filter = st.text_input("Filter vaccine group", placeholder="e.g. MMR")
            view = recommendation_df[recommendation_df["Status"].isin(status_filter)]
            if vaccine_filter:
                view = view[view["Vaccine"].str.contains(vaccine_filter, case=False, na=False)]
            st.dataframe(view, width="stretch", hide_index=True)
            st.download_button(
                "Download recommendations CSV",
                data=view.to_csv(index=False),
                file_name="safestart2_recommendations.csv",
                mime="text/csv",
            )

    with unvaccinated_tab:
        unvacc_df = recommendation_df[recommendation_df["Status"] == "unvaccinated"]
        if unvacc_df.empty:
            st.success("No unvaccinated patients detected.")
        else:
            st.warning(
                "These patients only have `unknown` as their vaccine marker and should "
                "follow the unvaccinated workflow."
            )
            st.dataframe(unvacc_df, width="stretch", hide_index=True)

    with patients_tab:
        st.dataframe(patient_df, width="stretch", hide_index=True)
        if patient_df.empty:
            st.info("No patient records were produced from this file.")
        else:
            selected_nhs = st.selectbox("Inspect patient", options=patient_df["NHS Number"].tolist())
            selected = next(patient for patient in processed.patients if patient.nhs_number == selected_nhs)
            st.markdown(f"### {selected.full_name}")
            st.write(f"NHS: `{selected.nhs_number}`")
            st.write(f"DOB: {selected.date_of_birth.strftime('%d/%m/%Y')}")
            st.write(f"Vaccines on record: {len(selected.vaccine_events)}")
            if selected.vaccine_events:
                timeline = pd.DataFrame(
                    [
                        {
                            "Event Date": event.event_date.strftime("%d/%m/%Y") if event.event_date else "—",
                            "Canonical": event.canonical_vaccine,
                            "Program": event.vaccine_program,
                            "Raw Label": event.raw_vaccine_name,
                            "Confidence": event.confidence,
                        }
                        for event in selected.vaccine_events
                    ]
                )
                st.dataframe(timeline, width="stretch", hide_index=True)
            else:
                st.info("No vaccine events on record for this patient.")

    with diagnostics_tab:
        st.subheader("Warnings")
        if processed.warnings:
            for warning in processed.warnings[:200]:
                st.warning(warning)
        else:
            st.success("No parser warnings.")
        st.subheader("Source Columns")
        st.write(list(df.columns))
        st.subheader("Sample Rows")
        st.dataframe(df.head(20), width="stretch", hide_index=True)

    st.divider()
    if st.button("Persist Cohort to Supabase", type="primary", icon=":material/database_upload:"):
        progress_placeholder = st.empty()
        progress_bar = st.progress(0)

        def report_progress(stage: str, progress: float, message: str) -> None:
            progress_bar.progress(min(max(progress, 0.0), 1.0))
            progress_placeholder.info(message)

        with st.spinner("Persisting cohort to Supabase...", show_time=True):
            try:
                result = store.persist_processed_cohort(
                    cohort=processed,
                    user_context=user_context,
                    surgery_code=surgery_code,
                    surgery_name=surgery_name,
                    source_filename=source_name or "uploaded_file",
                    uploaded_by_email=uploader_email,
                    sms_sender_id=sms_sender_id or None,
                    import_metadata={
                        "age_range": f"{min_age_years}-{max_age_years}",
                        "reference_date": reference_date.isoformat(),
                        "lookahead_days": lookahead_days,
                        "self_book_url": self_book_url or "",
                    },
                    progress_callback=report_progress,
                )
            except (AuthenticationError, AuthorizationError) as exc:
                progress_bar.empty()
                progress_placeholder.empty()
                st.error(str(exc))
            except Exception as exc:
                progress_bar.empty()
                progress_placeholder.empty()
                st.error(f"Persist failed: {exc}")
            else:
                progress_bar.progress(1.0)
                progress_placeholder.success("Persist complete.")
                st.success(
                    f"Saved batch {result['batch_id']} for surgery {result['surgery_id']}. "
                    f"Patients: {result['patients']}, events: {result['events']}, "
                    f"recommendations: {result['recommendations']}."
                )
                _invalidate_data_caches()


def _render_settings_tab(
    store: SupabaseStore,
    user_context: UserContext,
    self_book_url: Optional[str],
    sms_sender_id: Optional[str],
) -> None:
    smsworks_settings = get_smsworks_settings()
    resend_settings = get_resend_settings()
    st.subheader("Surgery Settings")
    st.caption("Review surgery metadata and operational settings.")

    try:
        surgeries = store.list_accessible_surgeries(user_context)
    except AuthorizationError as exc:
        st.error(str(exc))
        return

    if not surgeries:
        st.info("No surgeries are visible for this account.")
        return

    selected_surgery_id = user_context.surgery_id
    if user_context.is_superuser:
        selected_surgery_id = st.selectbox(
            "Surgery",
            options=[surgery["id"] for surgery in surgeries],
            format_func=lambda surgery_id: next(
                (
                    f"{surgery['surgery_code']} - {surgery['surgery_name']}"
                    for surgery in surgeries
                    if surgery["id"] == surgery_id
                ),
                "Unknown surgery",
            ),
            key="settings_selected_surgery",
        )

    selected_surgery = next(
        (surgery for surgery in surgeries if surgery["id"] == selected_surgery_id),
        surgeries[0],
    )
    can_edit = user_context.is_superuser

    settings_col1, settings_col2, settings_col3, settings_col4 = st.columns(4)
    settings_col1.metric("Surgery Code", selected_surgery.get("surgery_code") or "—")
    settings_col2.metric("Sender ID", selected_surgery.get("sms_sender_id") or "—")
    settings_col3.metric("Contact Email", selected_surgery.get("email") or "—")
    settings_col4.metric("Contact Phone", selected_surgery.get("phone") or "—")

    with st.form("surgery_settings_form"):
        surgery_name = st.text_input(
            "Surgery Name",
            value=selected_surgery.get("surgery_name") or "",
            disabled=not can_edit,
        )
        sms_sender_id = st.text_input(
            "SMS Sender ID",
            value=selected_surgery.get("sms_sender_id") or "",
            max_chars=11,
            disabled=not can_edit,
        )
        surgery_email = st.text_input(
            "Contact Email",
            value=selected_surgery.get("email") or "",
            disabled=not can_edit,
        )
        surgery_phone = st.text_input(
            "Contact Phone",
            value=selected_surgery.get("phone") or "",
            disabled=not can_edit,
        )
        save_settings = st.form_submit_button("Save surgery settings", disabled=not can_edit)

    st.caption(
        "Self-book link remains a session-only value for now and is not persisted in Supabase."
    )
    if self_book_url:
        st.code(self_book_url)

    st.divider()
    st.subheader("SMS Provider")
    provider_col1, provider_col2, provider_col3 = st.columns(3)
    provider_col1.metric("Provider", "The SMS Works")
    provider_col2.metric("JWT", _mask_secret(smsworks_settings.jwt) if smsworks_settings else "not configured")
    provider_col3.metric(
        "App Sender",
        sms_sender_id or "unset",
    )
    st.caption(
        "Configure Streamlit secrets as `[smsworks] jwt = \"...\"`. "
        "The SMS Works sender comes from the app `SMS Sender ID` field. "
        "The app can send live SMS when explicitly enabled in the workflow."
    )

    st.divider()
    st.subheader("Email Provider")
    email_provider_col1, email_provider_col2, email_provider_col3 = st.columns(3)
    email_provider_col1.metric("Provider", "Resend")
    email_provider_col2.metric("API Key", _mask_secret(resend_settings.api_key) if resend_settings else "not configured")
    email_provider_col3.metric("Sender", f"{sms_sender_id or 'sender_id'} <hello@attribut.me>")
    st.caption(
        "Configure Streamlit secrets as `[resend] RESEND_API_KEY = \"...\"`. "
        "Emails use the surgery `SMS Sender ID` as the display name and reply to the surgery `Contact Email` field."
    )

    if save_settings:
        try:
            store.update_surgery_settings(
                user_context=user_context,
                surgery_id=str(selected_surgery["id"]),
                surgery_name=surgery_name,
                sms_sender_id=sms_sender_id or None,
                email=surgery_email or None,
                phone=surgery_phone or None,
            )
        except (AuthenticationError, AuthorizationError, RuntimeError, ValueError) as exc:
            st.error(str(exc))
        else:
            st.success("Surgery settings saved.")
            st.rerun()


def _render_vaccination_events_tab(
    store: SupabaseStore,
    user_context: UserContext,
) -> None:
    st.subheader("Vaccination Events")
    st.caption("Browse vaccination events stored in Supabase and review each patient timeline.")

    try:
        surgeries = store.list_accessible_surgeries(user_context)
    except AuthorizationError as exc:
        st.error(str(exc))
        return

    selected_surgery_id: Optional[str] = user_context.surgery_id
    if user_context.is_superuser:
        surgery_options = [None] + [surgery["id"] for surgery in surgeries]
        surgery_labels = {
            None: "All surgeries",
            **{
                surgery["id"]: f"{surgery['surgery_code']} - {surgery['surgery_name']}"
                for surgery in surgeries
            },
        }
        selected_surgery_id = st.selectbox(
            "Surgery filter",
            options=surgery_options,
            format_func=lambda surgery_id: surgery_labels[surgery_id],
            key="events_surgery_filter",
        )

    refresh_col, toggle_col1, toggle_col2, _ = st.columns([0.18, 0.2, 0.24, 0.38])
    if refresh_col.button(
        "Refresh data",
        key="events_refresh_data",
        icon=":material/refresh:",
    ):
        _invalidate_data_caches()
        st.rerun()
    exclude_flu_events = toggle_col1.toggle(
        "Exclude Flu",
        value=True,
        key="events_exclude_flu",
    )
    exclude_covid_events = toggle_col2.toggle(
        "Exclude COVID-19",
        value=True,
        key="events_exclude_covid19",
    )

    filter_col1, filter_col2 = st.columns([1.2, 1])
    patient_search = filter_col1.text_input(
        "Patient search",
        placeholder="Name, NHS, phone, vaccine",
        key="events_patient_search",
    )
    min_age_years, max_age_years = filter_col2.slider(
        "Patient age range (years)",
        min_value=0,
        max_value=120,
        value=(0, 120),
        step=1,
        key="events_age_filter",
    )
    include_without_events = st.toggle(
        "Include patients with no stored events",
        value=False,
        key="events_include_without_events",
    )

    try:
        patients = _get_cached_vaccination_event_patients(
            store,
            user_context,
            surgery_id=selected_surgery_id,
            include_without_events=include_without_events,
        )
    except (AuthenticationError, AuthorizationError, RuntimeError, ValueError) as exc:
        st.error(str(exc))
        return

    if not patients:
        st.info("No patient vaccination records are visible for this account and surgery filter.")
        return

    excluded_vaccines = set()
    if exclude_flu_events:
        excluded_vaccines.add("Flu")
    if exclude_covid_events:
        excluded_vaccines.add("COVID-19")
    patients = [
        _apply_vaccination_event_exclusions(patient, excluded_vaccines=excluded_vaccines)
        for patient in patients
    ]
    if not include_without_events:
        patients = [
            patient for patient in patients
            if int(patient.get("event_count") or 0) > 0
        ]

    filtered_patients = [
        patient
        for patient in patients
        if (
            (age_years := _age_years_from_dob(patient.get("date_of_birth"))) is not None
            and min_age_years <= age_years <= max_age_years
        )
    ]
    if patient_search:
        needle = patient_search.lower()
        filtered_patients = [
            patient
            for patient in filtered_patients
            if needle in " ".join(
                [
                    str(patient.get("full_name") or ""),
                    str(patient.get("nhs_number") or ""),
                    str(patient.get("phone") or ""),
                    str(patient.get("vaccines_display") or ""),
                ]
            ).lower()
        ]

    if not filtered_patients:
        st.warning("No patient vaccination records match the current filters.")
        return

    metric_col1, metric_col2, metric_col3 = st.columns(3)
    metric_col1.metric("Patients Shown", f"{len(filtered_patients):,}")
    metric_col2.metric(
        "Stored Events",
        f"{sum(int(patient.get('event_count') or 0) for patient in filtered_patients):,}",
    )
    metric_col3.metric(
        "Distinct Vaccine Groups",
        f"{len({v for patient in filtered_patients for v in str(patient.get('vaccines_display') or '').split(', ') if v}):,}",
    )

    patient_df = pd.DataFrame(
        [
            {
                "Surgery": patient.get("surgery_code") or "—",
                "Patient": patient.get("full_name") or "—",
                "NHS Number": patient.get("nhs_number") or "—",
                "DOB": _format_date(patient.get("date_of_birth")),
                "Age": _format_age_from_dob(patient.get("date_of_birth")),
                "Phone": patient.get("phone") or "—",
                "Events": patient.get("event_count") or 0,
                "Vaccines": patient.get("vaccines_display") or "No stored events",
                "Last Event": _format_date(patient.get("last_event_date")),
            }
            for patient in filtered_patients
        ]
    )
    st.dataframe(patient_df, width="stretch", hide_index=True)

    patient_map = {patient["id"]: patient for patient in filtered_patients}
    selected_patient_id = st.selectbox(
        "Select patient",
        options=list(patient_map),
        format_func=lambda patient_id: (
            f"{patient_map[patient_id].get('full_name') or 'Unknown'} | "
            f"{patient_map[patient_id].get('nhs_number') or '—'} | "
            f"{patient_map[patient_id].get('event_count') or 0} events"
        ),
        key="events_selected_patient",
    )
    selected_patient = patient_map[selected_patient_id]

    timeline = _get_cached_patient_timeline(
        store,
        user_context=user_context,
        surgery_id=str(selected_patient.get("surgery_id") or ""),
        nhs_number=str(selected_patient.get("nhs_number") or ""),
    )
    event_rows = [
        event
        for event in (timeline.get("events", []) or [])
        if str(event.get("canonical_vaccine") or "") not in excluded_vaccines
    ]
    if not event_rows:
        st.info("No vaccination events are stored for this patient after the current vaccine exclusions.")
        return

    st.divider()
    detail_col1, detail_col2, detail_col3, detail_col4 = st.columns(4)
    detail_col1.metric("Patient", selected_patient.get("full_name") or "—")
    detail_col2.metric("Age", _format_age_from_dob(selected_patient.get("date_of_birth")))
    detail_col3.metric("Total Events", f"{len(event_rows):,}")
    detail_col4.metric("Last Event", _format_date(selected_patient.get("last_event_date")))

    vaccine_counts_df = pd.DataFrame(
        sorted(
            (
                {"Vaccine": vaccine, "Count": count}
                for vaccine, count in pd.Series(
                    [event.get("canonical_vaccine") or "Unknown" for event in event_rows]
                ).value_counts().items()
            ),
            key=lambda row: (-row["Count"], row["Vaccine"]),
        )
    )
    timeline_chart_df = pd.DataFrame(
        [
            {
                "Event Date": pd.to_datetime(event.get("event_date"), errors="coerce"),
                "Vaccine": event.get("canonical_vaccine") or "Unknown",
                "Program": event.get("vaccine_program") or "—",
                "Marker": "Recorded",
                "Raw Label": event.get("raw_vaccine_name") or "—",
            }
            for event in event_rows
        ]
    ).dropna(subset=["Event Date"])

    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        st.caption("Vaccine counts")
        counts_chart = (
            alt.Chart(vaccine_counts_df)
            .mark_bar()
            .encode(
                x=alt.X("Count:Q"),
                y=alt.Y("Vaccine:N", sort="-x"),
                tooltip=["Vaccine:N", "Count:Q"],
            )
            .properties(height=max(180, 28 * len(vaccine_counts_df)))
        )
        st.altair_chart(counts_chart, width="stretch")
    with chart_col2:
        st.caption("Vaccine Timeline")
        vaccine_order = sorted(timeline_chart_df["Vaccine"].unique())
        grid_chart = _build_vaccine_grid_chart(vaccine_order)
        timeline_chart = (
            alt.layer(
                grid_chart,
                alt.Chart(timeline_chart_df)
                .mark_circle(size=120)
                .encode(
                    x=alt.X("Event Date:T", axis=alt.Axis(grid=False)),
                    y=alt.Y(
                        "Vaccine:N",
                        sort=vaccine_order,
                        axis=alt.Axis(grid=False),
                    ),
                    color=alt.Color(
                        "Marker:N",
                        scale=alt.Scale(
                            domain=["Recorded", "Recall due"],
                            range=["#4294c2", "#ae4f4d"],
                        ),
                        legend=None,
                    ),
                    tooltip=["Event Date:T", "Vaccine:N", "Program:N", "Raw Label:N"],
                ),
            )
            .properties(height=max(220, 32 * timeline_chart_df["Vaccine"].nunique()))
        )
        st.altair_chart(timeline_chart, width="stretch")

    st.divider()
    event_df = pd.DataFrame(
        [
            {
                "Event Date": _format_date(event.get("event_date")),
                "Vaccine": event.get("canonical_vaccine") or "—",
                "Program": event.get("vaccine_program") or "—",
                "Raw Label": event.get("raw_vaccine_name") or "—",
                "Done At ID": event.get("event_done_at_id") or "—",
            }
            for event in event_rows
        ]
    )
    st.dataframe(event_df, width="stretch", hide_index=True)


st.title("💉 SafeStart2")
st.caption("Fresh Streamlit + Supabase vaccination recall system for ImmunizeMe exports")

with st.sidebar:
    st.header("Session")
    st.caption(f"Signed in as `{user_context.email}`")
    st.caption(f"Role: `{user_context.role}`")
    if user_context.surgery_code:
        st.caption(f"Surgery: `{user_context.surgery_code}`")
    if st.button("Sign out"):
        store.sign_out()
        _clear_session()
        st.rerun()

    st.divider()
    st.header("Import Settings")
    is_superuser = user_context.is_superuser
    if not is_superuser:
        st.caption("Surgery settings are controlled by your `public.surgery_users` mapping.")

    surgery_code = st.text_input(
        "Surgery Code",
        value=user_context.surgery_code or "E87750",
        disabled=not is_superuser,
    )
    surgery_name = st.text_input(
        "Surgery Name",
        value=user_context.surgery_name or "SafeStart2 Demo Surgery",
        disabled=not is_superuser,
    )
    uploader_email = st.text_input(
        "Uploader Email",
        value=user_context.email,
        disabled=True,
    )
    sms_sender_id = st.text_input(
        "SMS Sender ID",
        value=user_context.sms_sender_id or "SafeStart2",
        max_chars=11,
        disabled=not is_superuser,
    )
    self_book_url = st.text_input(
        "Self-book link",
        placeholder="https://...",
        help="Optional link included in SMS and bulk CSV exports.",
    )
    reference_date = st.date_input("Reference Date", value=date.today())
    lookahead_days = st.slider("Lookahead Days", min_value=7, max_value=90, value=30, step=1)
    min_age_years, max_age_years = st.slider(
        "Import age range (years)",
        min_value=0,
        max_value=120,
        value=(0, 120),
        step=1,
    )

    target_surgery = None
    if user_context.is_superuser:
        target_surgery = store.find_surgery_by_code(surgery_code) if surgery_code.strip() else None
    elif user_context.surgery_id:
        target_surgery = {
            "id": user_context.surgery_id,
            "surgery_code": user_context.surgery_code,
            "surgery_name": user_context.surgery_name,
        }

    st.divider()
    st.header("Danger Zone")
    if target_surgery and target_surgery.get("id"):
        st.caption(
            f"Clear imported data for `{target_surgery.get('surgery_code')}` only. "
            "This keeps surgeries and user access records."
        )
        clear_confirm = st.text_input(
            "Type surgery code to confirm",
            key="clear_import_data_confirm",
            placeholder=target_surgery.get("surgery_code") or "",
        )
        if st.button(
            "Clear imported test data",
            key="clear_import_data_button",
            width="stretch",
        ):
            if clear_confirm.strip().upper() != str(target_surgery.get("surgery_code") or "").upper():
                st.error("Confirmation code does not match the selected surgery code.")
            else:
                with st.spinner("Clearing imported test data from Supabase...", show_time=True):
                    try:
                        counts = store.clear_import_data(
                            user_context=user_context,
                            surgery_id=str(target_surgery["id"]),
                        )
                    except (AuthenticationError, AuthorizationError, RuntimeError, ValueError) as exc:
                        st.error(str(exc))
                    else:
                        st.success(
                            "Cleared imported data: "
                            f"patients {counts['patients']}, import rows {counts['import_rows']}, "
                            f"events {counts['vaccination_events']}, recalls {counts['recall_recommendations']}, "
                            f"attempts {counts['recall_attempts']}, recall batches {counts['recall_batches']}, "
                            f"legacy sms batches {counts['bulk_sms_batches']}, "
                            f"import batches {counts['import_batches']}."
                        )
                        _invalidate_data_caches()
                        st.rerun()
    else:
        st.caption("Select an existing surgery before clearing imported test data.")

main_view = st.radio(
    "Section",
    options=[
        ":material/clinical_notes: Recall Worklist",
        ":material/syringe: Vaccination Events",
        ":material/sim_card_download: Import & Process",
        ":material/medical_services: Surgery Settings",
    ],
    horizontal=True,
    label_visibility="collapsed",
    key="main_view",
)

if main_view == ":material/clinical_notes: Recall Worklist":
    _render_worklist_tab(
        store,
        user_context,
        self_book_url=self_book_url or None,
        sms_sender_id=sms_sender_id or None,
    )
elif main_view == ":material/syringe: Vaccination Events":
    _render_vaccination_events_tab(
        store=store,
        user_context=user_context,
    )
elif main_view == ":material/sim_card_download: Import & Process":
    _render_import_tab(
        store=store,
        user_context=user_context,
        surgery_code=surgery_code,
        surgery_name=surgery_name,
        uploader_email=uploader_email,
        sms_sender_id=sms_sender_id,
        self_book_url=self_book_url or None,
        reference_date=reference_date,
        lookahead_days=lookahead_days,
        min_age_years=min_age_years,
        max_age_years=max_age_years,
    )
else:
    _render_settings_tab(
        store=store,
        user_context=user_context,
        self_book_url=self_book_url or None,
        sms_sender_id=sms_sender_id or None,
    )
