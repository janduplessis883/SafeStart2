from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha1
import json
import math
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from supabase import Client, create_client
from supabase_auth.errors import AuthApiError

from .config import get_supabase_settings
from .models import ProcessedCohort
from .processing import classify_due_status, process_immunizeme_dataframe, process_immunizeme_rows
from .schedule import current_covid_season_start, current_flu_season_start
from .workflow import compare_processed_cohorts


class AuthenticationError(RuntimeError):
    pass


class AuthorizationError(RuntimeError):
    pass


@dataclass
class UserContext:
    email: str
    full_name: Optional[str]
    role: Optional[str]
    surgery_id: Optional[str]
    surgery_code: Optional[str] = None
    surgery_name: Optional[str] = None
    sms_sender_id: Optional[str] = None

    @property
    def is_superuser(self) -> bool:
        return self.role == "superuser"

    @property
    def is_authorized(self) -> bool:
        return bool(self.role)


class SupabaseStore:
    IMPORT_ROWS_CHUNK_SIZE = 500
    EVENT_ROWS_CHUNK_SIZE = 500
    RECOMMENDATION_ROWS_CHUNK_SIZE = 250
    UPDATE_IDS_CHUNK_SIZE = 500
    BULK_SMS_BATCH_CHUNK_SIZE = 100
    ACTIVE_RECALLS_PAGE_SIZE = 1000
    PATIENTS_PAGE_SIZE = 1000
    VACCINATION_EVENTS_PAGE_SIZE = 1000
    IMPORT_BATCHES_PAGE_SIZE = 100

    def __init__(self, session_tokens: Optional[Dict[str, str]] = None) -> None:
        settings = get_supabase_settings()
        self.enabled = settings is not None
        self.client: Optional[Client] = None
        self._session_tokens: Optional[Dict[str, str]] = None

        if not settings:
            return

        self.client = create_client(settings.url, settings.anon_key)
        if session_tokens:
            access_token = session_tokens.get("access_token")
            refresh_token = session_tokens.get("refresh_token")
            if access_token and refresh_token:
                try:
                    self.client.auth.set_session(access_token, refresh_token)
                    self._session_tokens = {
                        "access_token": access_token,
                        "refresh_token": refresh_token,
                    }
                except Exception as exc:
                    raise AuthenticationError(
                        "Your saved session is no longer valid. Please sign in again."
                    ) from exc

    def get_session_tokens(self) -> Optional[Dict[str, str]]:
        if not self.client:
            return None
        if self._session_tokens:
            return dict(self._session_tokens)
        try:
            session = self.client.auth.get_session()
        except AuthApiError as exc:
            message = str(exc).lower()
            if "rate limit" in message:
                return None
            raise
        if not session:
            return None
        self._session_tokens = {
            "access_token": session.access_token,
            "refresh_token": session.refresh_token,
        }
        return dict(self._session_tokens)

    def sign_in(self, email: str, password: str) -> UserContext:
        if not self.client:
            raise AuthenticationError("Supabase auth is not configured.")

        try:
            response = self.client.auth.sign_in_with_password(
                {"email": email.strip(), "password": password}
            )
        except Exception as exc:
            raise AuthenticationError("Sign-in failed. Check your email and password.") from exc

        if not response.session:
            raise AuthenticationError("Supabase did not return a session for this sign-in.")
        self._session_tokens = {
            "access_token": response.session.access_token,
            "refresh_token": response.session.refresh_token,
        }

        context = self.get_current_user_context()
        if not context or not context.is_authorized:
            self.sign_out()
            raise AuthorizationError(
                "Sign-in worked, but this email is not mapped in public.surgery_users."
            )

        return context

    def sign_out(self) -> None:
        if not self.client:
            return
        self._session_tokens = None
        try:
            self.client.auth.sign_out()
        except Exception:
            pass

    def get_current_user_context(self) -> Optional[UserContext]:
        if not self.client:
            return None

        user_response = self.client.auth.get_user()
        user = getattr(user_response, "user", None) if user_response else None
        email = str(getattr(user, "email", "") or "").strip().lower()
        if not email:
            return None

        try:
            profile_rows = (
                self.client.table("surgery_users")
                .select("email,full_name,role,surgery_id")
                .eq("email", email)
                .eq("is_active", True)
                .limit(1)
                .execute()
                .data
                or []
            )
        except Exception as exc:
            raise AuthorizationError(
                "Unable to load your surgery access. Rerun `sql/002_rls.sql` with the updated helper functions."
            ) from exc

        if not profile_rows:
            return UserContext(
                email=email,
                full_name=getattr(user, "user_metadata", {}).get("full_name")
                if getattr(user, "user_metadata", None)
                else None,
                role=None,
                surgery_id=None,
            )

        profile = profile_rows[0]
        surgery_id = profile.get("surgery_id")
        surgery_code: Optional[str] = None
        surgery_name: Optional[str] = None
        sms_sender_id: Optional[str] = None

        if surgery_id:
            surgery = self._get_surgery_by_id(str(surgery_id))
            if surgery:
                surgery_code = surgery.get("surgery_code")
                surgery_name = surgery.get("surgery_name")
                sms_sender_id = surgery.get("sms_sender_id")

        return UserContext(
            email=email,
            full_name=profile.get("full_name"),
            role=profile.get("role"),
            surgery_id=str(surgery_id) if surgery_id else None,
            surgery_code=surgery_code,
            surgery_name=surgery_name,
            sms_sender_id=sms_sender_id,
        )

    def get_alias_overrides(
        self,
        surgery_id: Optional[str] = None,
        global_only: bool = False,
    ) -> Dict[str, Tuple[str, str]]:
        if not self.client:
            return {}

        query = (
            self.client.table("vaccine_alias_overrides")
            .select("raw_label,canonical_vaccine,vaccine_program")
            .eq("is_active", True)
        )
        if global_only:
            query = query.is_("surgery_id", "null")
        elif surgery_id:
            query = query.or_(f"surgery_id.is.null,surgery_id.eq.{surgery_id}")

        rows = query.execute().data or []
        return {
            row["raw_label"].strip().lower(): (
                row["canonical_vaccine"],
                row["vaccine_program"],
            )
            for row in rows
        }

    def find_surgery_by_code(self, surgery_code: str) -> Optional[dict]:
        if not self.client or not surgery_code.strip():
            return None

        rows = (
            self.client.table("surgeries")
            .select("id,surgery_code,surgery_name,sms_sender_id")
            .eq("surgery_code", surgery_code.strip().upper())
            .limit(1)
            .execute()
            .data
            or []
        )
        return rows[0] if rows else None

    def resolve_surgery(
        self,
        *,
        user_context: UserContext,
        surgery_code: str,
        surgery_name: str,
        sms_sender_id: Optional[str],
    ) -> dict:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")

        normalized_code = surgery_code.strip().upper()
        normalized_name = surgery_name.strip()
        normalized_sender = sms_sender_id.strip() if sms_sender_id else None

        if user_context.is_superuser:
            if not normalized_code:
                raise AuthorizationError("Superusers must choose a surgery code before persisting.")
            if not normalized_name:
                raise AuthorizationError("Superusers must choose a surgery name before persisting.")

            existing = self.find_surgery_by_code(normalized_code)
            if existing:
                updates = {}
                if normalized_name != existing.get("surgery_name"):
                    updates["surgery_name"] = normalized_name
                if normalized_sender != existing.get("sms_sender_id"):
                    updates["sms_sender_id"] = normalized_sender
                if updates:
                    self.client.table("surgeries").update(updates).eq("id", existing["id"]).execute()
                    existing = {**existing, **updates}
                return existing

            created = (
                self.client.table("surgeries")
                .insert(
                    {
                        "surgery_code": normalized_code,
                        "surgery_name": normalized_name,
                        "sms_sender_id": normalized_sender,
                    }
                )
                .execute()
            )
            return created.data[0]

        if not user_context.surgery_id:
            raise AuthorizationError(
                "Your account is not linked to a surgery. Add a surgery_id in public.surgery_users."
            )

        if normalized_code and user_context.surgery_code and normalized_code != user_context.surgery_code:
            raise AuthorizationError(
                "Your account is limited to its assigned surgery and cannot persist to a different code."
            )

        return {
            "id": user_context.surgery_id,
            "surgery_code": user_context.surgery_code or normalized_code,
            "surgery_name": user_context.surgery_name or normalized_name,
            "sms_sender_id": user_context.sms_sender_id or normalized_sender,
        }

    def persist_processed_cohort(
        self,
        cohort: ProcessedCohort,
        user_context: UserContext,
        surgery_code: str,
        surgery_name: str,
        source_filename: str,
        uploaded_by_email: Optional[str],
        sms_sender_id: Optional[str] = None,
        import_metadata: Optional[dict] = None,
        progress_callback: Optional[Callable[[str, float, str], None]] = None,
    ) -> Dict[str, int]:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")

        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in with an authorized user before persisting.")

        self._notify_progress(progress_callback, "resolve_surgery", 0.02, "Resolving surgery context...")
        surgery = self.resolve_surgery(
            user_context=user_context,
            surgery_code=surgery_code,
            surgery_name=surgery_name,
            sms_sender_id=sms_sender_id,
        )
        surgery_id = surgery["id"]

        self._notify_progress(progress_callback, "create_batch", 0.08, "Creating import batch...")
        batch = (
            self.client.table("import_batches")
            .insert(
                {
                    "surgery_id": surgery_id,
                    "uploaded_by_email": uploaded_by_email or user_context.email,
                    "source_filename": source_filename,
                    "row_count": cohort.raw_rows,
                    "patient_count": len(cohort.patients),
                    "recommendation_count": len(cohort.recommendations),
                    "unvaccinated_count": cohort.unvaccinated_patients,
                    "notes": json.dumps(import_metadata) if import_metadata else None,
                }
            )
            .execute()
        )
        batch_id = batch.data[0]["id"]
        batch_notes = dict(import_metadata or {})

        total_patients = max(len(cohort.patients), 1)
        patient_id_map: Dict[str, str] = {}
        for index, patient in enumerate(cohort.patients, start=1):
            payload = {
                "surgery_id": surgery_id,
                "nhs_number": patient.nhs_number,
                "source_patient_id": patient.source_patient_id,
                "first_name": patient.first_name,
                "last_name": patient.last_name,
                "full_name": patient.full_name,
                "sex": patient.sex,
                "date_of_birth": patient.date_of_birth.isoformat(),
                "phone": patient.phone,
                "email": patient.email,
                "registration_date": patient.registration_date.isoformat()
                if patient.registration_date
                else None,
                "is_unvaccinated": patient.only_unknown_marker,
            }
            result = (
                self.client.table("patients")
                .upsert(payload, on_conflict="surgery_id,nhs_number")
                .execute()
            )
            patient_id_map[patient.nhs_number] = result.data[0]["id"]
            if index == 1 or index == total_patients or index % 250 == 0:
                progress = 0.08 + (0.18 * (index / total_patients))
                self._notify_progress(
                    progress_callback,
                    "patients",
                    progress,
                    f"Upserting patients {index:,}/{total_patients:,}...",
                )

        import_rows: List[dict] = []
        event_rows: List[dict] = []
        for patient in cohort.patients:
            for raw in patient.raw_rows:
                import_rows.append(
                    {
                        "batch_id": batch_id,
                        "surgery_id": surgery_id,
                        "nhs_number": patient.nhs_number,
                        "source_patient_id": patient.source_patient_id,
                        "raw_vaccine_name": str(raw.get("raw_vaccine_name", "") or ""),
                        "raw_event_date": str(raw.get("event_date", "") or ""),
                        "raw_payload": self._json_safe(raw),
                    }
                )
            for event in patient.vaccine_events:
                source_hash = sha1(
                    "|".join(
                        [
                            surgery_id,
                            patient.nhs_number,
                            event.canonical_vaccine,
                            event.raw_vaccine_name,
                            event.event_date.isoformat() if event.event_date else "",
                        ]
                    ).encode("utf-8")
                ).hexdigest()
                event_rows.append(
                    {
                        "surgery_id": surgery_id,
                        "patient_id": patient_id_map[patient.nhs_number],
                        "batch_id": batch_id,
                        "canonical_vaccine": event.canonical_vaccine,
                        "vaccine_program": event.vaccine_program,
                        "raw_vaccine_name": event.raw_vaccine_name,
                        "event_date": event.event_date.isoformat() if event.event_date else None,
                        "event_done_at_id": event.event_done_at_id,
                        "source_hash": source_hash,
                    }
                )

        if import_rows:
            self._notify_progress(
                progress_callback,
                "import_rows",
                0.30,
                f"Writing import rows ({len(import_rows):,})...",
            )
            self._bulk_insert(
                "import_rows",
                import_rows,
                chunk_size=self.IMPORT_ROWS_CHUNK_SIZE,
                progress_callback=progress_callback,
                progress_stage="import_rows",
                progress_start=0.30,
                progress_end=0.50,
                progress_label="Writing import rows",
            )
        if event_rows:
            event_rows = self._dedupe_dict_rows(event_rows, key_fields=["surgery_id", "source_hash"])
            self._notify_progress(
                progress_callback,
                "events",
                0.52,
                f"Writing vaccination events ({len(event_rows):,})...",
            )
            self._bulk_upsert(
                "vaccination_events",
                event_rows,
                on_conflict="surgery_id,source_hash",
                chunk_size=self.EVENT_ROWS_CHUNK_SIZE,
                progress_callback=progress_callback,
                progress_stage="events",
                progress_start=0.52,
                progress_end=0.70,
                progress_label="Writing vaccination events",
            )

        affected_patient_ids = [patient_id_map[patient.nhs_number] for patient in cohort.patients]
        existing_active_recalls: List[dict] = []
        if affected_patient_ids:
            for chunk in self._iter_chunks(affected_patient_ids, self.UPDATE_IDS_CHUNK_SIZE):
                existing_active_recalls.extend(
                    (
                        self.client.table("recall_recommendations")
                        .select("id,patient_id,recommendation_type,vaccine_group,due_date")
                        .eq("surgery_id", surgery_id)
                        .eq("is_active", True)
                        .in_("patient_id", chunk)
                        .limit(5000)
                        .execute()
                        .data
                        or []
                    )
                )
        if affected_patient_ids:
            total_chunks = max((len(affected_patient_ids) + self.UPDATE_IDS_CHUNK_SIZE - 1) // self.UPDATE_IDS_CHUNK_SIZE, 1)
            for chunk_index, chunk in enumerate(self._iter_chunks(affected_patient_ids, self.UPDATE_IDS_CHUNK_SIZE), start=1):
                self.client.table("recall_recommendations").update(
                    {"is_active": False},
                    returning="minimal",
                ).eq(
                    "surgery_id",
                    surgery_id,
                ).in_("patient_id", chunk).eq("is_active", True).execute()
                progress = 0.70 + (0.06 * (chunk_index / total_chunks))
                self._notify_progress(
                    progress_callback,
                    "deactivate_recalls",
                    progress,
                    f"Deactivating prior recalls {chunk_index:,}/{total_chunks:,}...",
                )

        recommendation_rows = []
        for recommendation in cohort.recommendations:
            patient_id = patient_id_map[recommendation.patient_nhs_number]
            recommendation_rows.append(
                {
                    "surgery_id": surgery_id,
                    "patient_id": patient_id,
                    "batch_id": batch_id,
                    "recommendation_type": recommendation.recommendation_type,
                    "vaccine_group": recommendation.vaccine_group,
                    "program_area": recommendation.program_area,
                    "due_date": recommendation.due_date.isoformat() if recommendation.due_date else None,
                    "status": recommendation.status,
                    "priority": recommendation.priority,
                    "reason": recommendation.reason,
                    "explanation": recommendation.explanation,
                    "is_active": True,
                }
            )

        if recommendation_rows:
            recommendation_rows = self._dedupe_dict_rows(
                recommendation_rows,
                key_fields=[
                    "surgery_id",
                    "patient_id",
                    "vaccine_group",
                    "recommendation_type",
                    "due_date",
                ],
            )

        new_recommendation_keys = {
            (
                str(row.get("patient_id") or ""),
                str(row.get("recommendation_type") or ""),
                str(row.get("vaccine_group") or ""),
                str(row.get("due_date") or ""),
            )
            for row in recommendation_rows
        }
        resolved_recall_ids = [
            str(row["id"])
            for row in existing_active_recalls
            if (
                str(row.get("patient_id") or ""),
                str(row.get("recommendation_type") or ""),
                str(row.get("vaccine_group") or ""),
                str(row.get("due_date") or ""),
            )
            not in new_recommendation_keys
        ]
        if resolved_recall_ids:
            total_resolved_chunks = max(
                (len(resolved_recall_ids) + self.UPDATE_IDS_CHUNK_SIZE - 1) // self.UPDATE_IDS_CHUNK_SIZE,
                1,
            )
            for chunk_index, chunk in enumerate(
                self._iter_chunks(resolved_recall_ids, self.UPDATE_IDS_CHUNK_SIZE),
                start=1,
            ):
                self.client.table("recall_recommendations").update(
                    {"status": "resolved_by_vaccination", "is_active": False},
                    returning="minimal",
                ).in_("id", chunk).execute()
                progress = 0.76 + (0.02 * (chunk_index / total_resolved_chunks))
                self._notify_progress(
                    progress_callback,
                    "resolve_vaccinated",
                    progress,
                    f"Marking resolved recalls {chunk_index:,}/{total_resolved_chunks:,}...",
                )

        if recommendation_rows:
            self._notify_progress(
                progress_callback,
                "recommendations",
                0.78,
                f"Writing recall recommendations ({len(recommendation_rows):,})...",
            )
            self._bulk_upsert(
                "recall_recommendations",
                recommendation_rows,
                on_conflict="surgery_id,patient_id,vaccine_group,recommendation_type,due_date",
                chunk_size=self.RECOMMENDATION_ROWS_CHUNK_SIZE,
                progress_callback=progress_callback,
                progress_stage="recommendations",
                progress_start=0.78,
                progress_end=0.96,
                progress_label="Writing recall recommendations",
            )
            persisted_rows = (
                self.client.table("recall_recommendations")
                .select("id")
                .eq("batch_id", batch_id)
                .limit(1)
                .execute()
                .data
                or []
            )
            if not persisted_rows:
                raise RuntimeError(
                    "Recall recommendations were generated but none were persisted. "
                    "Check the `recall_recommendations` table constraints and RLS."
                )

        batch_notes.update(
            {
                "persisted_event_count": len(event_rows),
                "persisted_recommendation_count": len(recommendation_rows),
                "active_recommendation_count": len(recommendation_rows),
                "recommendation_status_counts": self._count_rows_by_field(
                    recommendation_rows,
                    field_name="status",
                    default_value="unknown",
                ),
                "recommendation_vaccine_counts": self._count_rows_by_field(
                    recommendation_rows,
                    field_name="vaccine_group",
                    default_value="Unknown",
                ),
            }
        )
        self.client.table("import_batches").update(
            {"notes": json.dumps(batch_notes)},
            returning="minimal",
        ).eq("id", batch_id).execute()

        self._notify_progress(progress_callback, "complete", 1.0, "Import complete.")
        return {
            "patients": len(patient_id_map),
            "events": len(event_rows),
            "recommendations": len(recommendation_rows),
            "batch_id": batch_id,
            "surgery_id": surgery_id,
        }

    def list_import_batches(
        self,
        user_context: UserContext,
        surgery_id: Optional[str] = None,
    ) -> List[dict]:
        if not self.client:
            return []
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before viewing import batches.")

        rows: List[dict] = []
        offset = 0
        while True:
            query = (
                self.client.table("import_batches")
                .select("id,surgery_id,uploaded_by_email,source_filename,row_count,patient_count,recommendation_count,unvaccinated_count,imported_at,notes")
                .order("imported_at", desc=True)
                .range(offset, offset + self.IMPORT_BATCHES_PAGE_SIZE - 1)
            )
            if surgery_id:
                query = query.eq("surgery_id", surgery_id)
            page = query.execute().data or []
            if not page:
                break
            rows.extend(page)
            if len(page) < self.IMPORT_BATCHES_PAGE_SIZE:
                break
            offset += self.IMPORT_BATCHES_PAGE_SIZE

        surgery_map = {
            surgery["id"]: surgery
            for surgery in self.list_accessible_surgeries(user_context)
        }
        for row in rows:
            surgery = surgery_map.get(row["surgery_id"], {})
            row["surgery_code"] = surgery.get("surgery_code")
            row["surgery_name"] = surgery.get("surgery_name")
        return rows

    def get_import_batch_detail(
        self,
        user_context: UserContext,
        batch_id: str,
        include_live_counts: bool = False,
    ) -> Optional[dict]:
        if not self.client:
            return None
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before viewing import batch detail.")

        rows = (
            self.client.table("import_batches")
            .select(
                "id,surgery_id,uploaded_by_email,source_filename,row_count,patient_count,"
                "recommendation_count,unvaccinated_count,imported_at,notes"
            )
            .eq("id", batch_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        if not rows:
            return None

        batch = rows[0]
        surgery = self._get_surgery_by_id(batch["surgery_id"]) or {}
        notes = self._parse_notes_json(batch.get("notes"))

        event_count = notes.get("persisted_event_count")
        persisted_recommendation_count = notes.get("persisted_recommendation_count")
        active_recommendation_count = notes.get("active_recommendation_count")
        status_counts = notes.get("recommendation_status_counts") or {}
        vaccine_counts = notes.get("recommendation_vaccine_counts") or {}
        detail_source = "stored_summary" if (
            event_count is not None or persisted_recommendation_count is not None
        ) else "import_metadata"

        if include_live_counts:
            try:
                event_count_result = (
                    self.client.table("vaccination_events")
                    .select("id", count="exact")
                    .eq("batch_id", batch_id)
                    .limit(1)
                    .execute()
                )
                recommendation_rows = self._load_recommendation_rows_for_batch(batch_id)
                event_count = int(getattr(event_count_result, "count", 0) or 0)
                persisted_recommendation_count = len(recommendation_rows)
                active_recommendation_count = sum(
                    1 for row in recommendation_rows if bool(row.get("is_active"))
                )
                status_counts = self._count_rows_by_field(
                    recommendation_rows,
                    field_name="status",
                    default_value="unknown",
                )
                vaccine_counts = self._count_rows_by_field(
                    recommendation_rows,
                    field_name="vaccine_group",
                    default_value="Unknown",
                )
                detail_source = "live_counts"
            except Exception:
                pass

        return {
            **batch,
            "surgery_code": surgery.get("surgery_code"),
            "surgery_name": surgery.get("surgery_name"),
            "event_count": int(event_count or 0),
            "persisted_recommendation_count": int(
                persisted_recommendation_count or batch.get("recommendation_count") or 0
            ),
            "active_recommendation_count": int(
                active_recommendation_count
                if active_recommendation_count is not None
                else batch.get("recommendation_count") or 0
            ),
            "status_counts": status_counts,
            "vaccine_counts": vaccine_counts,
            "detail_source": detail_source,
        }

    def get_import_batch_comparison(
        self,
        user_context: UserContext,
        batch_id: str,
    ) -> Optional[dict]:
        if not self.client:
            return None
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before viewing import batch comparison.")

        current_batch = self.get_import_batch_detail(user_context, batch_id)
        if not current_batch:
            return None

        previous_rows = (
            self.client.table("import_batches")
            .select(
                "id,surgery_id,uploaded_by_email,source_filename,row_count,patient_count,"
                "recommendation_count,unvaccinated_count,imported_at,notes"
            )
            .eq("surgery_id", current_batch["surgery_id"])
            .lt("imported_at", current_batch["imported_at"])
            .order("imported_at", desc=True)
            .limit(1)
            .execute()
            .data
            or []
        )
        if not previous_rows:
            return {
                "current_batch": current_batch,
                "previous_batch": None,
                "comparison": None,
            }

        previous_batch = previous_rows[0]
        surgery = self._get_surgery_by_id(previous_batch["surgery_id"]) or {}
        previous_batch["surgery_code"] = surgery.get("surgery_code")
        previous_batch["surgery_name"] = surgery.get("surgery_name")

        current_notes = self._parse_notes_json(current_batch.get("notes"))
        previous_notes = self._parse_notes_json(previous_batch.get("notes"))
        surgery_id = str(current_batch.get("surgery_id") or "")
        overrides = self.get_alias_overrides(surgery_id=surgery_id) if surgery_id else {}

        current_reference_date = self._parse_iso_date(current_notes.get("reference_date")) or date.today()
        previous_reference_date = self._parse_iso_date(previous_notes.get("reference_date")) or current_reference_date
        current_lookahead_days = int(current_notes.get("lookahead_days") or 30)
        previous_lookahead_days = int(previous_notes.get("lookahead_days") or current_lookahead_days)

        current_rows = self._load_import_row_payloads(batch_id)
        previous_rows_payload = self._load_import_row_payloads(str(previous_batch["id"]))

        current_cohort = process_immunizeme_rows(
            current_rows,
            reference_date=current_reference_date,
            lookahead_days=current_lookahead_days,
            overrides=overrides,
        )
        previous_cohort = process_immunizeme_rows(
            previous_rows_payload,
            reference_date=previous_reference_date,
            lookahead_days=previous_lookahead_days,
            overrides=overrides,
        )

        return {
            "current_batch": current_batch,
            "previous_batch": previous_batch,
            "comparison": compare_processed_cohorts(previous_cohort, current_cohort),
        }

    def list_accessible_surgeries(self, user_context: UserContext) -> List[dict]:
        if not self.client:
            return []
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before viewing surgeries.")

        rows = (
            self.client.table("surgeries")
            .select("id,surgery_code,surgery_name,sms_sender_id,email,phone")
            .eq("is_active", True)
            .order("surgery_code")
            .limit(5000)
            .execute()
            .data
            or []
        )
        return rows

    def update_surgery_settings(
        self,
        user_context: UserContext,
        surgery_id: str,
        surgery_name: str,
        sms_sender_id: Optional[str],
        email: Optional[str],
        phone: Optional[str],
    ) -> dict:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if not user_context.is_superuser:
            raise AuthorizationError("Only superusers can update surgery settings.")
        if not surgery_id:
            raise ValueError("A surgery must be selected before updating settings.")

        updated = (
            self.client.table("surgeries")
            .update(
                {
                    "surgery_name": surgery_name.strip(),
                    "sms_sender_id": sms_sender_id.strip() if sms_sender_id else None,
                    "email": email.strip() if email else None,
                    "phone": phone.strip() if phone else None,
                }
            )
            .eq("id", surgery_id)
            .execute()
        )
        if not updated.data:
            raise AuthorizationError("That surgery is not visible to your account.")
        return updated.data[0]

    def list_active_recalls(
        self,
        user_context: UserContext,
        surgery_id: Optional[str] = None,
    ) -> List[dict]:
        if not self.client:
            return []
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before viewing recalls.")

        rows: List[dict] = []
        offset = 0
        while True:
            query = (
                self.client.table("v_active_recalls")
                .select(
                    "id,surgery_id,nhs_number,full_name,date_of_birth,phone,email,"
                    "recommendation_type,vaccine_group,program_area,due_date,status,"
                    "priority,reason,explanation,updated_at"
                )
                .range(offset, offset + self.ACTIVE_RECALLS_PAGE_SIZE - 1)
            )
            if surgery_id:
                query = query.eq("surgery_id", surgery_id)
            page = query.execute().data or []
            if not page:
                break
            rows.extend(page)
            if len(page) < self.ACTIVE_RECALLS_PAGE_SIZE:
                break
            offset += self.ACTIVE_RECALLS_PAGE_SIZE

        rows.sort(
            key=lambda row: (
                int(row.get("priority") or 999),
                row.get("due_date") or "9999-12-31",
                row.get("full_name") or "",
            )
        )

        if not rows:
            return rows

        surgery_map = {
            surgery["id"]: surgery
            for surgery in self.list_accessible_surgeries(user_context)
        }
        recommendation_ids = [row["id"] for row in rows if row.get("id")]
        attempt_map = self._attempt_summary_map(recommendation_ids)

        enriched_rows = []
        for row in rows:
            attempt_summary = attempt_map.get(row["id"], {})
            surgery = surgery_map.get(row["surgery_id"], {})
            enriched_rows.append(
                {
                    **row,
                    "surgery_code": surgery.get("surgery_code"),
                    "surgery_name": surgery.get("surgery_name"),
                    "surgery_email": surgery.get("email"),
                    "attempt_count": attempt_summary.get("attempt_count", 0),
                    "last_attempt_at": attempt_summary.get("last_attempt_at"),
                    "last_attempt_method": attempt_summary.get("last_attempt_method"),
                    "last_attempt_outcome": attempt_summary.get("last_attempt_outcome"),
                }
            )
        return enriched_rows

    def list_patients_with_vaccination_events(
        self,
        user_context: UserContext,
        surgery_id: Optional[str] = None,
        include_without_events: bool = False,
    ) -> List[dict]:
        if not self.client:
            return []
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before viewing vaccination events.")

        patient_rows: List[dict] = []
        offset = 0
        while True:
            query = (
                self.client.table("patients")
                .select("id,surgery_id,nhs_number,full_name,date_of_birth,phone,email,registration_date")
                .order("full_name")
                .range(offset, offset + self.PATIENTS_PAGE_SIZE - 1)
            )
            if surgery_id:
                query = query.eq("surgery_id", surgery_id)
            page = query.execute().data or []
            if not page:
                break
            patient_rows.extend(page)
            if len(page) < self.PATIENTS_PAGE_SIZE:
                break
            offset += self.PATIENTS_PAGE_SIZE
        if not patient_rows:
            return []

        surgery_map = {
            surgery["id"]: surgery
            for surgery in self.list_accessible_surgeries(user_context)
        }
        patient_ids = [row["id"] for row in patient_rows if row.get("id")]
        event_rows: List[dict] = []
        for chunk in self._iter_chunks(patient_ids, self.UPDATE_IDS_CHUNK_SIZE):
            chunk_offset = 0
            while True:
                page = (
                    self.client.table("vaccination_events")
                    .select("patient_id,canonical_vaccine,event_date")
                    .neq("canonical_vaccine", "Unmapped")
                    .in_("patient_id", chunk)
                    .order("event_date", desc=True)
                    .range(chunk_offset, chunk_offset + self.VACCINATION_EVENTS_PAGE_SIZE - 1)
                    .execute()
                    .data
                    or []
                )
                if not page:
                    break
                event_rows.extend(page)
                if len(page) < self.VACCINATION_EVENTS_PAGE_SIZE:
                    break
                chunk_offset += self.VACCINATION_EVENTS_PAGE_SIZE

        event_map: Dict[str, dict] = {}
        for event in event_rows:
            patient_id = str(event.get("patient_id") or "")
            if not patient_id:
                continue
            entry = event_map.setdefault(
                patient_id,
                {
                    "event_count": 0,
                    "vaccines": set(),
                    "last_event_date": None,
                    "event_vaccine_counts": {},
                    "event_vaccine_last_dates": {},
                },
            )
            entry["event_count"] += 1
            vaccine = str(event.get("canonical_vaccine") or "").strip()
            if vaccine:
                entry["vaccines"].add(vaccine)
                entry["event_vaccine_counts"][vaccine] = int(entry["event_vaccine_counts"].get(vaccine) or 0) + 1
            event_date = event.get("event_date")
            if event_date and (entry["last_event_date"] is None or str(event_date) > str(entry["last_event_date"])):
                entry["last_event_date"] = event_date
            if vaccine and event_date:
                current_last_for_vaccine = entry["event_vaccine_last_dates"].get(vaccine)
                if current_last_for_vaccine is None or str(event_date) > str(current_last_for_vaccine):
                    entry["event_vaccine_last_dates"][vaccine] = event_date

        enriched_rows = []
        for row in patient_rows:
            summary = event_map.get(row["id"])
            if not summary and not include_without_events:
                continue
            surgery = surgery_map.get(row["surgery_id"], {})
            enriched_rows.append(
                {
                    **row,
                    "surgery_code": surgery.get("surgery_code"),
                    "surgery_name": surgery.get("surgery_name"),
                    "event_count": summary["event_count"] if summary else 0,
                    "vaccine_count": len(summary["vaccines"]) if summary else 0,
                    "vaccines_display": ", ".join(sorted(summary["vaccines"])) if summary else "",
                    "last_event_date": summary["last_event_date"] if summary else None,
                    "event_vaccine_counts": dict(summary["event_vaccine_counts"]) if summary else {},
                    "event_vaccine_last_dates": dict(summary["event_vaccine_last_dates"]) if summary else {},
                }
            )
        return enriched_rows

    def list_bulk_sms_batches(
        self,
        user_context: UserContext,
        surgery_id: Optional[str] = None,
    ) -> List[dict]:
        if not self.client:
            return []
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before viewing bulk SMS batches.")

        query = (
            self.client.table("bulk_sms_batches")
            .select(
                "id,surgery_id,prepared_by_email,prepared_by_name,status,ready_count,"
                "blocked_count,selection_summary,export_rows,blocked_rows,self_book_url,"
                "created_at,updated_at"
            )
            .order("created_at", desc=True)
            .limit(100)
        )
        if surgery_id:
            query = query.eq("surgery_id", surgery_id)
        rows = query.execute().data or []
        surgery_map = {
            surgery["id"]: surgery
            for surgery in self.list_accessible_surgeries(user_context)
        }
        for row in rows:
            surgery = surgery_map.get(row["surgery_id"], {})
            row["surgery_code"] = surgery.get("surgery_code")
            row["surgery_name"] = surgery.get("surgery_name")
        return rows

    def list_recall_attempts(self, recommendation_id: str) -> List[dict]:
        if not self.client:
            return []
        rows = (
            self.client.table("recall_attempts")
            .select("id,communication_method,staff_member,sent_at,outcome,notes")
            .eq("recommendation_id", recommendation_id)
            .order("sent_at", desc=True)
            .limit(500)
            .execute()
            .data
            or []
        )
        return rows

    def log_recall_attempt(
        self,
        user_context: UserContext,
        recommendation_id: str,
        communication_method: str,
        staff_member: Optional[str],
        outcome: str,
        notes: Optional[str],
    ) -> dict:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before logging recall attempts.")

        recommendation = self._get_recommendation(recommendation_id)
        if not recommendation:
            raise AuthorizationError("That recall could not be found or is not visible to your account.")

        created = (
            self.client.table("recall_attempts")
            .insert(
                {
                    "surgery_id": recommendation["surgery_id"],
                    "recommendation_id": recommendation_id,
                    "communication_method": communication_method,
                    "staff_member": staff_member or user_context.full_name or user_context.email,
                    "outcome": outcome.strip() or "sent",
                    "notes": notes.strip() if notes else None,
                }
            )
            .execute()
        )
        return created.data[0]

    def list_recall_attempts_for_recommendations(self, recommendation_ids: List[str]) -> List[dict]:
        if not self.client or not recommendation_ids:
            return []

        attempts = []
        for chunk in self._iter_chunks(recommendation_ids, self.UPDATE_IDS_CHUNK_SIZE):
            rows = (
                self.client.table("recall_attempts")
                .select("id,recommendation_id,communication_method,staff_member,sent_at,outcome,notes")
                .in_("recommendation_id", chunk)
                .order("sent_at", desc=True)
                .limit(1000)
                .execute()
                .data
                or []
            )
            attempts.extend(rows)

        deduped: Dict[Tuple[object, ...], dict] = {}
        for attempt in attempts:
            key = (
                attempt.get("sent_at"),
                attempt.get("communication_method"),
                attempt.get("staff_member"),
                attempt.get("outcome"),
                attempt.get("notes"),
            )
            existing = deduped.get(key)
            if existing is None:
                deduped[key] = {**attempt, "recommendation_count": 1}
            else:
                existing["recommendation_count"] += 1

        return sorted(
            deduped.values(),
            key=lambda attempt: attempt.get("sent_at") or "",
            reverse=True,
        )

    def list_attempt_rows_for_recommendations(self, recommendation_ids: List[str]) -> List[dict]:
        if not self.client or not recommendation_ids:
            return []

        attempts: List[dict] = []
        for chunk in self._iter_chunks(recommendation_ids, self.UPDATE_IDS_CHUNK_SIZE):
            rows = (
                self.client.table("recall_attempts")
                .select(
                    "id,recommendation_id,bulk_sms_batch_id,recall_batch_id,communication_method,staff_member,"
                    "sent_at,outcome,notes"
                )
                .in_("recommendation_id", chunk)
                .order("sent_at", desc=True)
                .limit(5000)
                .execute()
                .data
                or []
            )
            attempts.extend(rows)
        return attempts

    def log_recall_attempts(
        self,
        user_context: UserContext,
        recommendation_ids: List[str],
        communication_method: str,
        staff_member: Optional[str],
        outcome: str,
        notes: Optional[str],
        sent_at: Optional[str] = None,
        bulk_sms_batch_id: Optional[str] = None,
        recall_batch_id: Optional[str] = None,
    ) -> int:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before logging recall attempts.")
        if not recommendation_ids:
            raise ValueError("No recall recommendations were supplied.")

        recommendations = self._get_recommendations(recommendation_ids)
        if not recommendations:
            raise AuthorizationError("Those recalls could not be found or are not visible to your account.")

        logged_at = sent_at or datetime.now(timezone.utc).isoformat()
        inserted = 0
        for chunk in self._iter_chunks(recommendations, self.UPDATE_IDS_CHUNK_SIZE):
            payload = [
                {
                    "surgery_id": recommendation["surgery_id"],
                    "recommendation_id": recommendation["id"],
                    "bulk_sms_batch_id": bulk_sms_batch_id,
                    "recall_batch_id": recall_batch_id,
                    "communication_method": communication_method,
                    "staff_member": staff_member or user_context.full_name or user_context.email,
                    "sent_at": logged_at,
                    "outcome": outcome.strip() or "sent",
                    "notes": notes.strip() if notes else None,
                }
                for recommendation in chunk
            ]
            self.client.table("recall_attempts").insert(
                payload,
                default_to_null=False,
                returning="minimal",
            ).execute()
            inserted += len(payload)

        return inserted

    def create_bulk_sms_batch(
        self,
        user_context: UserContext,
        surgery_id: str,
        prepared_by_name: Optional[str],
        status: str,
        ready_rows: List[dict],
        blocked_rows: List[dict],
        selection_summary: dict,
        self_book_url: Optional[str] = None,
        sent_at: Optional[str] = None,
    ) -> dict:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before preparing bulk SMS batches.")
        if not surgery_id:
            raise ValueError("A surgery must be selected before preparing a bulk SMS batch.")
        if not ready_rows:
            raise ValueError("At least one SMS-ready patient recall is required.")

        logged_at = sent_at or datetime.now(timezone.utc).isoformat()
        batch_result = (
            self.client.table("bulk_sms_batches")
            .insert(
                {
                    "surgery_id": surgery_id,
                    "prepared_by_email": user_context.email,
                    "prepared_by_name": prepared_by_name or user_context.full_name or user_context.email,
                    "status": status,
                    "ready_count": len(ready_rows),
                    "blocked_count": len(blocked_rows),
                    "selection_summary": self._json_safe(selection_summary),
                    "export_rows": self._json_safe(ready_rows),
                    "blocked_rows": self._json_safe(blocked_rows),
                    "self_book_url": self_book_url,
                }
            )
            .execute()
        )
        batch = batch_result.data[0]

        total_logged = 0
        for row in ready_rows:
            total_logged += self.log_recall_attempts(
                user_context=user_context,
                recommendation_ids=list(row.get("Recommendation IDs") or []),
                communication_method="bulk_sms",
                staff_member=prepared_by_name,
                outcome=status,
                notes=str(row.get("Message") or "").strip() or None,
                sent_at=logged_at,
                bulk_sms_batch_id=batch["id"],
            )

        return {
            **batch,
            "logged_attempts": total_logged,
        }

    def update_bulk_sms_batch_outcome(
        self,
        user_context: UserContext,
        batch_id: str,
        outcome: str,
        staff_member: Optional[str],
        sent_at: Optional[str] = None,
    ) -> dict:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before updating bulk SMS batches.")

        batch = self._get_bulk_sms_batch(batch_id)
        if not batch:
            raise AuthorizationError("That bulk SMS batch could not be found or is not visible.")

        ready_rows = batch.get("export_rows") or []
        if not isinstance(ready_rows, list) or not ready_rows:
            raise ValueError("This bulk SMS batch does not contain any SMS-ready patient recalls.")

        logged_at = sent_at or datetime.now(timezone.utc).isoformat()
        total_logged = 0
        for row in ready_rows:
            total_logged += self.log_recall_attempts(
                user_context=user_context,
                recommendation_ids=list(row.get("Recommendation IDs") or []),
                communication_method="bulk_sms",
                staff_member=staff_member,
                outcome=outcome,
                notes=str(row.get("Message") or "").strip() or None,
                sent_at=logged_at,
                bulk_sms_batch_id=batch_id,
            )

        updated = (
            self.client.table("bulk_sms_batches")
            .update(
                {
                    "status": outcome,
                    "prepared_by_name": staff_member or batch.get("prepared_by_name"),
                }
            )
            .eq("id", batch_id)
            .execute()
        )
        return {
            **(updated.data[0] if updated.data else batch),
            "logged_attempts": total_logged,
        }

    def set_bulk_sms_batch_status(
        self,
        user_context: UserContext,
        batch_id: str,
        status: str,
    ) -> dict:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before updating bulk SMS batches.")

        updated = (
            self.client.table("bulk_sms_batches")
            .update({"status": status})
            .eq("id", batch_id)
            .execute()
        )
        if not updated.data:
            raise AuthorizationError("That bulk SMS batch could not be found or is not visible.")
        return updated.data[0]

    def list_recall_batches(
        self,
        user_context: UserContext,
        surgery_id: Optional[str] = None,
    ) -> List[dict]:
        if not self.client:
            return []
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before viewing recall batches.")

        query = (
            self.client.table("recall_batches")
            .select(
                "id,surgery_id,prepared_by_email,prepared_by_name,delivery_method,status,"
                "selected_count,ready_count,blocked_count,selection_summary,export_rows,"
                "blocked_rows,self_book_url,created_at,updated_at"
            )
            .order("created_at", desc=True)
            .limit(100)
        )
        if surgery_id:
            query = query.eq("surgery_id", surgery_id)
        rows = query.execute().data or []
        surgery_map = {
            surgery["id"]: surgery
            for surgery in self.list_accessible_surgeries(user_context)
        }
        for row in rows:
            surgery = surgery_map.get(row["surgery_id"], {})
            row["surgery_code"] = surgery.get("surgery_code")
            row["surgery_name"] = surgery.get("surgery_name")
            row["surgery_email"] = surgery.get("email")
            row["sms_sender_id"] = surgery.get("sms_sender_id")
        return rows

    def create_recall_batch(
        self,
        user_context: UserContext,
        surgery_id: str,
        prepared_by_name: Optional[str],
        selected_rows: List[dict],
        selection_summary: dict,
        self_book_url: Optional[str] = None,
        delivery_method: Optional[str] = None,
        status: str = "prepared",
    ) -> dict:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before preparing recall batches.")
        if not surgery_id:
            raise ValueError("A surgery must be selected before preparing a recall batch.")
        if not selected_rows:
            raise ValueError("At least one patient recall must be selected.")
        if delivery_method and delivery_method not in {"sms", "email", "letter", "call"}:
            raise ValueError("Recall batch delivery method must be sms, email, letter, or call.")

        normalized_rows = self._json_safe(selected_rows)
        normalized_summary = self._json_safe(selection_summary)
        recent_rows = (
            self.client.table("recall_batches")
            .select(
                "id,surgery_id,prepared_by_email,prepared_by_name,delivery_method,status,"
                "selected_count,ready_count,blocked_count,selection_summary,export_rows,"
                "blocked_rows,self_book_url,created_at,updated_at"
            )
            .eq("surgery_id", surgery_id)
            .eq("prepared_by_email", user_context.email)
            .order("created_at", desc=True)
            .limit(10)
            .execute()
            .data
            or []
        )
        duplicate_cutoff = datetime.now(timezone.utc) - pd.Timedelta(minutes=5)
        for row in recent_rows:
            created_at = pd.to_datetime(row.get("created_at"), errors="coerce", utc=True)
            if pd.isna(created_at) or created_at.to_pydatetime() < duplicate_cutoff:
                continue
            if (
                row.get("delivery_method") == delivery_method
                and row.get("self_book_url") == self_book_url
                and row.get("export_rows") == normalized_rows
                and row.get("selection_summary") == normalized_summary
            ):
                return {**row, "deduplicated": True}

        created = (
            self.client.table("recall_batches")
            .insert(
                {
                    "surgery_id": surgery_id,
                    "prepared_by_email": user_context.email,
                    "prepared_by_name": prepared_by_name or user_context.full_name or user_context.email,
                    "delivery_method": delivery_method,
                    "status": status,
                    "selected_count": len(selected_rows),
                    "ready_count": len(selected_rows),
                    "blocked_count": 0,
                    "selection_summary": normalized_summary,
                    "export_rows": normalized_rows,
                    "blocked_rows": [],
                    "self_book_url": self_book_url,
                }
            )
            .execute()
        )
        return {**created.data[0], "deduplicated": False}

    def set_recall_batch_status(
        self,
        user_context: UserContext,
        batch_id: str,
        status: str,
        delivery_method: Optional[str] = None,
    ) -> dict:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before updating recall batches.")

        updates = {"status": status}
        if delivery_method is not None:
            updates["delivery_method"] = delivery_method
        updated = (
            self.client.table("recall_batches")
            .update(updates, returning="representation")
            .eq("id", batch_id)
            .execute()
        )
        if not updated.data:
            raise AuthorizationError("That recall batch could not be found or is not visible.")
        return updated.data[0]

    def delete_recall_batch(
        self,
        user_context: UserContext,
        batch_id: str,
    ) -> dict:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before deleting recall batches.")

        deleted = (
            self.client.table("recall_batches")
            .delete(returning="representation")
            .eq("id", batch_id)
            .execute()
        )
        if not deleted.data:
            raise AuthorizationError("That recall batch could not be found or is not visible.")
        return deleted.data[0]

    def suppress_recall_batch(
        self,
        user_context: UserContext,
        batch_id: str,
    ) -> dict:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before suppressing recall batches.")

        batch = self._get_recall_batch(batch_id)
        if not batch:
            raise AuthorizationError("That recall batch could not be found or is not visible.")

        export_rows = batch.get("export_rows") or []
        recommendation_ids: List[str] = []
        for row in export_rows:
            for recommendation_id in list(row.get("Recommendation IDs") or []):
                normalized_id = str(recommendation_id or "").strip()
                if normalized_id:
                    recommendation_ids.append(normalized_id)
        unique_recommendation_ids = list(dict.fromkeys(recommendation_ids))
        if not unique_recommendation_ids:
            raise ValueError("This recall batch does not contain any recall recommendations to suppress.")

        suppressed_count = self.close_recall_group(
            user_context=user_context,
            recommendation_ids=unique_recommendation_ids,
            status="suppressed",
        )
        updated_batch = self.set_recall_batch_status(
            user_context=user_context,
            batch_id=batch_id,
            status="suppressed",
        )
        return {
            **updated_batch,
            "suppressed_count": suppressed_count,
        }

    def log_recall_batch_outcome(
        self,
        user_context: UserContext,
        batch_id: str,
        *,
        communication_method: str,
        outcome: str,
        staff_member: Optional[str],
        notes_by_row: Optional[Dict[str, str]] = None,
        sent_at: Optional[str] = None,
    ) -> dict:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before updating recall batches.")
        if communication_method not in {"sms", "email", "letter", "call", "bulk_sms"}:
            raise ValueError("Unsupported communication method for recall batch logging.")

        batch = self._get_recall_batch(batch_id)
        if not batch:
            raise AuthorizationError("That recall batch could not be found or is not visible.")

        export_rows = batch.get("export_rows") or []
        if not isinstance(export_rows, list) or not export_rows:
            raise ValueError("This recall batch does not contain any patient recalls.")

        logged_at = sent_at or datetime.now(timezone.utc).isoformat()
        total_logged = 0
        for row in export_rows:
            recommendation_ids = list(row.get("Recommendation IDs") or [])
            if not recommendation_ids:
                continue
            group_id = str(row.get("Group ID") or "")
            notes = (notes_by_row or {}).get(group_id)
            total_logged += self.log_recall_attempts(
                user_context=user_context,
                recommendation_ids=recommendation_ids,
                communication_method=communication_method,
                staff_member=staff_member,
                outcome=outcome,
                notes=notes,
                sent_at=logged_at,
                recall_batch_id=batch_id,
            )

        updated = self.set_recall_batch_status(
            user_context=user_context,
            batch_id=batch_id,
            status=outcome,
            delivery_method=communication_method if communication_method != "bulk_sms" else "sms",
        )
        return {
            **updated,
            "logged_attempts": total_logged,
        }

    def close_recall(
        self,
        user_context: UserContext,
        recommendation_id: str,
        status: str,
    ) -> dict:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if status not in {"complete", "suppressed"}:
            raise ValueError("Recall status must be `complete` or `suppressed`.")
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before updating recall status.")

        updated = (
            self.client.table("recall_recommendations")
            .update({"status": status, "is_active": False})
            .eq("id", recommendation_id)
            .eq("is_active", True)
            .execute()
        )
        if not updated.data:
            raise AuthorizationError("That recall is no longer active or is not visible to your account.")
        return updated.data[0]

    def close_recall_group(
        self,
        user_context: UserContext,
        recommendation_ids: List[str],
        status: str,
    ) -> int:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if status not in {"complete", "suppressed"}:
            raise ValueError("Recall status must be `complete` or `suppressed`.")
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before updating recall status.")
        if not recommendation_ids:
            raise ValueError("No recall recommendations were supplied.")

        updated_count = 0
        for chunk in self._iter_chunks(recommendation_ids, self.UPDATE_IDS_CHUNK_SIZE):
            updated = (
                self.client.table("recall_recommendations")
                .update({"status": status, "is_active": False}, returning="representation")
                .in_("id", chunk)
                .eq("is_active", True)
                .execute()
            )
            updated_count += len(updated.data or [])

        if updated_count <= 0:
            raise AuthorizationError("Those recalls are no longer active or are not visible to your account.")
        return updated_count

    def clear_import_data(
        self,
        user_context: UserContext,
        surgery_id: str,
    ) -> Dict[str, int]:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before clearing test data.")
        if not surgery_id:
            raise ValueError("A surgery must be selected before clearing test data.")

        counts = {
            "recall_attempts": 0,
            "bulk_sms_batches": 0,
            "recall_batches": 0,
            "recall_recommendations": 0,
            "vaccination_events": 0,
            "import_rows": 0,
            "import_batches": 0,
            "patients": 0,
        }

        delete_order = [
            "recall_attempts",
            "bulk_sms_batches",
            "recall_batches",
            "recall_recommendations",
            "vaccination_events",
            "import_rows",
            "import_batches",
            "patients",
        ]
        for table_name in delete_order:
            counts[table_name] = self._delete_for_surgery(table_name, surgery_id)

        return counts

    def _run_season_rollover(
        self,
        *,
        user_context: UserContext,
        surgery_id: str,
        reference_date: date,
        vaccine_group: str,
        program_areas: List[str],
        target_due_date: date,
        action_label: str,
    ) -> Dict[str, object]:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if not user_context.is_authorized:
            raise AuthorizationError(f"You must sign in before running a {action_label}.")
        if not surgery_id:
            raise ValueError(f"A surgery must be selected before running a {action_label}.")

        target_due_date_str = target_due_date.isoformat()
        target_status, _ = classify_due_status(target_due_date, reference_date)

        rows: List[dict] = []
        offset = 0
        while True:
            page = (
                self.client.table("recall_recommendations")
                .select("id,patient_id,due_date,status,updated_at")
                .eq("surgery_id", surgery_id)
                .eq("is_active", True)
                .eq("vaccine_group", vaccine_group)
                .eq("recommendation_type", "seasonal")
                .in_("program_area", program_areas)
                .range(offset, offset + self.UPDATE_IDS_CHUNK_SIZE - 1)
                .execute()
                .data
                or []
            )
            if not page:
                break
            rows.extend(page)
            if len(page) < self.UPDATE_IDS_CHUNK_SIZE:
                break
            offset += self.UPDATE_IDS_CHUNK_SIZE

        if not rows:
            return {
                "target_due_date": target_due_date_str,
                "target_status": target_status,
                "examined_count": 0,
                "updated_count": 0,
                "deactivated_count": 0,
            }

        grouped_rows: Dict[str, List[dict]] = {}
        for row in rows:
            patient_id = str(row.get("patient_id") or "")
            if not patient_id:
                continue
            grouped_rows.setdefault(patient_id, []).append(row)

        update_ids: List[str] = []
        deactivate_ids: List[str] = []

        for patient_rows in grouped_rows.values():
            ordered_rows = sorted(
                patient_rows,
                key=lambda row: (
                    str(row.get("due_date") or ""),
                    str(row.get("updated_at") or ""),
                    str(row.get("id") or ""),
                ),
                reverse=True,
            )
            target_rows = [row for row in ordered_rows if str(row.get("due_date") or "") == target_due_date_str]

            if target_rows:
                keep_row = target_rows[0]
                if str(keep_row.get("status") or "") != target_status:
                    keep_id = str(keep_row.get("id") or "").strip()
                    if keep_id:
                        update_ids.append(keep_id)
                rows_to_deactivate = [row for row in ordered_rows if row is not keep_row]
            else:
                keep_row = ordered_rows[0]
                keep_id = str(keep_row.get("id") or "").strip()
                if keep_id and (
                    str(keep_row.get("due_date") or "") != target_due_date_str
                    or str(keep_row.get("status") or "") != target_status
                ):
                    update_ids.append(keep_id)
                rows_to_deactivate = ordered_rows[1:]

            for row in rows_to_deactivate:
                row_id = str(row.get("id") or "").strip()
                if row_id:
                    deactivate_ids.append(row_id)

        unique_update_ids = list(dict.fromkeys(update_ids))
        unique_deactivate_ids = list(dict.fromkeys(deactivate_ids))

        for chunk in self._iter_chunks(unique_update_ids, self.UPDATE_IDS_CHUNK_SIZE):
            self.client.table("recall_recommendations").update(
                {"due_date": target_due_date_str, "status": target_status},
                returning="minimal",
            ).in_("id", chunk).execute()

        for chunk in self._iter_chunks(unique_deactivate_ids, self.UPDATE_IDS_CHUNK_SIZE):
            self.client.table("recall_recommendations").update(
                {"is_active": False},
                returning="minimal",
            ).in_("id", chunk).execute()

        return {
            "target_due_date": target_due_date_str,
            "target_status": target_status,
            "examined_count": len(rows),
            "updated_count": len(unique_update_ids),
            "deactivated_count": len(unique_deactivate_ids),
        }

    def run_flu_season_rollover(
        self,
        user_context: UserContext,
        surgery_id: str,
        reference_date: Optional[date] = None,
    ) -> Dict[str, object]:
        reference_date = reference_date or date.today()
        return self._run_season_rollover(
            user_context=user_context,
            surgery_id=surgery_id,
            reference_date=reference_date,
            vaccine_group="Flu",
            program_areas=["seasonal_adult", "seasonal_child"],
            target_due_date=current_flu_season_start(reference_date),
            action_label="flu season rollover",
        )

    def run_covid_season_rollover(
        self,
        user_context: UserContext,
        surgery_id: str,
        reference_date: Optional[date] = None,
    ) -> Dict[str, object]:
        reference_date = reference_date or date.today()
        return self._run_season_rollover(
            user_context=user_context,
            surgery_id=surgery_id,
            reference_date=reference_date,
            vaccine_group="COVID-19",
            program_areas=["seasonal_adult"],
            target_due_date=current_covid_season_start(reference_date),
            action_label="COVID-19 season rollover",
        )

    def count_unmapped_vaccination_events(
        self,
        user_context: UserContext,
        surgery_id: str,
    ) -> int:
        if not self.client:
            return 0
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before viewing unmapped events.")
        if not surgery_id:
            raise ValueError("A surgery must be selected before viewing unmapped events.")

        result = (
            self.client.table("vaccination_events")
            .select("id", count="exact")
            .eq("surgery_id", surgery_id)
            .eq("canonical_vaccine", "Unmapped")
            .limit(1)
            .execute()
        )
        return int(getattr(result, "count", 0) or 0)

    def delete_unmapped_vaccination_events(
        self,
        user_context: UserContext,
        surgery_id: str,
    ) -> int:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before deleting unmapped events.")
        if not surgery_id:
            raise ValueError("A surgery must be selected before deleting unmapped events.")

        total = 0
        while True:
            rows = (
                self.client.table("vaccination_events")
                .select("id")
                .eq("surgery_id", surgery_id)
                .eq("canonical_vaccine", "Unmapped")
                .limit(self.UPDATE_IDS_CHUNK_SIZE)
                .execute()
                .data
                or []
            )
            if not rows:
                break
            ids = [row["id"] for row in rows if row.get("id")]
            if not ids:
                break
            self.client.table("vaccination_events").delete(returning="minimal").in_("id", ids).execute()
            total += len(ids)
        return total

    def rebuild_surgery_from_batch(
        self,
        user_context: UserContext,
        batch_id: str,
        progress_callback: Optional[Callable[[str, float, str], None]] = None,
    ) -> Dict[str, int]:
        if not self.client:
            raise RuntimeError("Supabase is not configured.")
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before rebuilding a batch.")

        batch_rows = (
            self.client.table("import_batches")
            .select(
                "id,surgery_id,uploaded_by_email,source_filename,row_count,patient_count,"
                "recommendation_count,unvaccinated_count,imported_at,notes"
            )
            .eq("id", batch_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        if not batch_rows:
            raise ValueError("The selected import batch could not be found.")

        batch = batch_rows[0]
        surgery = self._get_surgery_by_id(batch["surgery_id"])
        if not surgery:
            raise ValueError("The surgery for this import batch could not be found.")

        self._notify_progress(progress_callback, "rebuild_load", 0.05, "Loading stored import rows...")
        raw_payloads = self._load_import_row_payloads(batch_id)
        if not raw_payloads:
            raise ValueError("No import rows were found for the selected batch.")

        df = pd.DataFrame(raw_payloads)
        notes = self._parse_notes_json(batch.get("notes"))
        reference_date_value = notes.get("reference_date")
        reference_date = (
            date.fromisoformat(reference_date_value)
            if isinstance(reference_date_value, str) and reference_date_value
            else date.today()
        )
        lookahead_days_value = notes.get("lookahead_days", 30)
        lookahead_days = int(lookahead_days_value) if str(lookahead_days_value).strip() else 30

        overrides = self.get_alias_overrides(surgery_id=batch["surgery_id"])
        self._notify_progress(progress_callback, "rebuild_process", 0.15, "Reprocessing stored import rows...")
        cohort = process_immunizeme_dataframe(
            df,
            reference_date=reference_date,
            lookahead_days=lookahead_days,
            overrides=overrides,
        )

        self._notify_progress(progress_callback, "rebuild_clear", 0.25, "Clearing existing surgery import data...")
        self.clear_import_data(user_context=user_context, surgery_id=batch["surgery_id"])

        import_metadata = {
            **notes,
            "rebuild_from_batch_id": batch_id,
            "rebuild_triggered_at": datetime.now(timezone.utc).isoformat(),
        }
        return self.persist_processed_cohort(
            cohort=cohort,
            user_context=user_context,
            surgery_code=surgery.get("surgery_code") or "",
            surgery_name=surgery.get("surgery_name") or "",
            source_filename=batch.get("source_filename") or "rebuild_from_import_rows",
            uploaded_by_email=batch.get("uploaded_by_email") or user_context.email,
            sms_sender_id=surgery.get("sms_sender_id"),
            import_metadata=import_metadata,
            progress_callback=progress_callback,
        )

    def get_patient_timeline(
        self,
        user_context: UserContext,
        surgery_id: str,
        nhs_number: str,
    ) -> dict:
        if not self.client:
            return {"events": [], "attempts": []}
        if not user_context.is_authorized:
            raise AuthorizationError("You must sign in before viewing patient history.")

        patient_rows = (
            self.client.table("patients")
            .select("id,full_name,date_of_birth,phone,registration_date")
            .eq("surgery_id", surgery_id)
            .eq("nhs_number", nhs_number)
            .limit(1)
            .execute()
            .data
            or []
        )
        if not patient_rows:
            return {"events": [], "attempts": []}

        patient = patient_rows[0]
        event_rows = (
            self.client.table("vaccination_events")
            .select("canonical_vaccine,vaccine_program,raw_vaccine_name,event_date,event_done_at_id")
            .neq("canonical_vaccine", "Unmapped")
            .eq("patient_id", patient["id"])
            .order("event_date", desc=True)
            .limit(5000)
            .execute()
            .data
            or []
        )
        recommendation_rows = (
            self.client.table("recall_recommendations")
            .select("id,vaccine_group,due_date,status")
            .eq("patient_id", patient["id"])
            .order("due_date", desc=True)
            .limit(5000)
            .execute()
            .data
            or []
        )

        recall_map = {row["id"]: row for row in recommendation_rows if row.get("id")}
        attempt_rows = self.list_attempt_rows_for_recommendations(list(recall_map))
        for row in attempt_rows:
            recommendation = recall_map.get(row.get("recommendation_id"))
            row["vaccine_group"] = recommendation.get("vaccine_group") if recommendation else None
            row["due_date"] = recommendation.get("due_date") if recommendation else None
            row["recall_status"] = recommendation.get("status") if recommendation else None

        return {
            "patient": patient,
            "events": event_rows,
            "attempts": attempt_rows,
        }

    def _attempt_summary_map(self, recommendation_ids: List[str]) -> Dict[str, dict]:
        if not self.client or not recommendation_ids:
            return {}

        attempts: List[dict] = []
        for chunk in self._iter_chunks(recommendation_ids, self.UPDATE_IDS_CHUNK_SIZE):
            rows = (
                self.client.table("recall_attempts")
                .select("recommendation_id,sent_at,communication_method,outcome")
                .in_("recommendation_id", chunk)
                .order("sent_at", desc=True)
                .limit(5000)
                .execute()
                .data
                or []
            )
            attempts.extend(rows)

        summary: Dict[str, dict] = {}
        for attempt in attempts:
            recommendation_id = attempt["recommendation_id"]
            existing = summary.get(recommendation_id)
            if existing is None:
                summary[recommendation_id] = {
                    "attempt_count": 1,
                    "last_attempt_at": attempt.get("sent_at"),
                    "last_attempt_method": attempt.get("communication_method"),
                    "last_attempt_outcome": attempt.get("outcome"),
                }
            else:
                existing["attempt_count"] += 1
        return summary

    def _get_bulk_sms_batch(self, batch_id: str) -> Optional[dict]:
        if not self.client:
            return None
        rows = (
            self.client.table("bulk_sms_batches")
            .select(
                "id,surgery_id,prepared_by_email,prepared_by_name,status,ready_count,"
                "blocked_count,selection_summary,export_rows,blocked_rows,self_book_url,"
                "created_at,updated_at"
            )
            .eq("id", batch_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        return rows[0] if rows else None

    def _get_recall_batch(self, batch_id: str) -> Optional[dict]:
        if not self.client:
            return None
        rows = (
            self.client.table("recall_batches")
            .select(
                "id,surgery_id,prepared_by_email,prepared_by_name,delivery_method,status,"
                "selected_count,ready_count,blocked_count,selection_summary,export_rows,"
                "blocked_rows,self_book_url,created_at,updated_at"
            )
            .eq("id", batch_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        return rows[0] if rows else None

    def _get_recommendation(self, recommendation_id: str) -> Optional[dict]:
        if not self.client:
            return None
        rows = (
            self.client.table("recall_recommendations")
            .select("id,surgery_id,status,is_active")
            .eq("id", recommendation_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        return rows[0] if rows else None

    def _get_recommendations(self, recommendation_ids: List[str]) -> List[dict]:
        if not self.client or not recommendation_ids:
            return []

        rows: List[dict] = []
        for chunk in self._iter_chunks(recommendation_ids, self.UPDATE_IDS_CHUNK_SIZE):
            rows.extend(
                (
                    self.client.table("recall_recommendations")
                    .select("id,surgery_id,status,is_active")
                    .in_("id", chunk)
                    .execute()
                    .data
                    or []
                )
            )
        return rows

    def _load_import_row_payloads(self, batch_id: str) -> List[dict]:
        if not self.client:
            return []

        payloads: List[dict] = []
        offset = 0
        while True:
            rows = (
                self.client.table("import_rows")
                .select("raw_payload")
                .eq("batch_id", batch_id)
                .range(offset, offset + self.UPDATE_IDS_CHUNK_SIZE - 1)
                .execute()
                .data
                or []
            )
            if not rows:
                break
            payloads.extend(
                row["raw_payload"]
                for row in rows
                if isinstance(row.get("raw_payload"), dict)
            )
            if len(rows) < self.UPDATE_IDS_CHUNK_SIZE:
                break
            offset += self.UPDATE_IDS_CHUNK_SIZE
        return payloads

    def _load_recommendation_rows_for_batch(self, batch_id: str) -> List[dict]:
        if not self.client:
            return []

        rows: List[dict] = []
        offset = 0
        page_size = 1000
        while True:
            page = (
                self.client.table("recall_recommendations")
                .select("status,vaccine_group,is_active,due_date")
                .eq("batch_id", batch_id)
                .range(offset, offset + page_size - 1)
                .execute()
                .data
                or []
            )
            if not page:
                break
            rows.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
        return rows

    def _count_rows_by_field(
        self,
        rows: Iterable[dict],
        *,
        field_name: str,
        default_value: str,
    ) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for row in rows:
            value = str(row.get(field_name) or default_value)
            counts[value] = counts.get(value, 0) + 1
        return counts

    def _parse_notes_json(self, notes: Optional[str]) -> dict:
        if not notes:
            return {}
        try:
            parsed = json.loads(notes)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def _parse_iso_date(self, value: Optional[str]) -> Optional[date]:
        if not value:
            return None
        try:
            return date.fromisoformat(str(value))
        except Exception:
            return None

    def _parse_notes_json(self, notes: Optional[str]) -> dict:
        if not notes:
            return {}
        try:
            parsed = json.loads(notes)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def _delete_for_surgery(self, table_name: str, surgery_id: str) -> int:
        if not self.client:
            return 0

        total = 0
        while True:
            rows = (
                self.client.table(table_name)
                .select("id")
                .eq("surgery_id", surgery_id)
                .limit(self.UPDATE_IDS_CHUNK_SIZE)
                .execute()
                .data
                or []
            )
            if not rows:
                break

            ids = [row["id"] for row in rows if row.get("id")]
            if not ids:
                break

            self.client.table(table_name).delete(
                returning="minimal"
            ).in_("id", ids).execute()
            total += len(ids)

        return total

    def _get_surgery_by_id(self, surgery_id: str) -> Optional[dict]:
        if not self.client:
            return None
        rows = (
            self.client.table("surgeries")
            .select("id,surgery_code,surgery_name,sms_sender_id")
            .eq("id", surgery_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        return rows[0] if rows else None

    def _bulk_insert(
        self,
        table_name: str,
        rows: List[dict],
        chunk_size: int,
        progress_callback: Optional[Callable[[str, float, str], None]] = None,
        progress_stage: Optional[str] = None,
        progress_start: float = 0.0,
        progress_end: float = 1.0,
        progress_label: Optional[str] = None,
    ) -> None:
        if not self.client or not rows:
            return
        total_chunks = max((len(rows) + chunk_size - 1) // chunk_size, 1)
        for chunk_index, chunk in enumerate(self._iter_chunks(rows, chunk_size), start=1):
            self.client.table(table_name).insert(
                chunk,
                default_to_null=False,
                returning="minimal",
            ).execute()
            if progress_callback and progress_stage:
                progress = progress_start + ((progress_end - progress_start) * (chunk_index / total_chunks))
                label = progress_label or f"Writing {table_name}"
                self._notify_progress(
                    progress_callback,
                    progress_stage,
                    progress,
                    f"{label} {chunk_index:,}/{total_chunks:,}...",
                )

    def _bulk_upsert(
        self,
        table_name: str,
        rows: List[dict],
        on_conflict: str,
        chunk_size: int,
        progress_callback: Optional[Callable[[str, float, str], None]] = None,
        progress_stage: Optional[str] = None,
        progress_start: float = 0.0,
        progress_end: float = 1.0,
        progress_label: Optional[str] = None,
    ) -> None:
        if not self.client or not rows:
            return
        total_chunks = max((len(rows) + chunk_size - 1) // chunk_size, 1)
        for chunk_index, chunk in enumerate(self._iter_chunks(rows, chunk_size), start=1):
            self.client.table(table_name).upsert(
                chunk,
                on_conflict=on_conflict,
                default_to_null=False,
                returning="minimal",
            ).execute()
            if progress_callback and progress_stage:
                progress = progress_start + ((progress_end - progress_start) * (chunk_index / total_chunks))
                label = progress_label or f"Writing {table_name}"
                self._notify_progress(
                    progress_callback,
                    progress_stage,
                    progress,
                    f"{label} {chunk_index:,}/{total_chunks:,}...",
                )

    def _iter_chunks(self, rows: List[dict], chunk_size: int) -> Iterable[List[dict]]:
        for index in range(0, len(rows), chunk_size):
            yield rows[index:index + chunk_size]

    def _dedupe_dict_rows(self, rows: List[dict], key_fields: List[str]) -> List[dict]:
        deduped: Dict[Tuple[object, ...], dict] = {}
        for row in rows:
            key = tuple(row.get(field) for field in key_fields)
            deduped[key] = row
        return list(deduped.values())

    def _notify_progress(
        self,
        progress_callback: Optional[Callable[[str, float, str], None]],
        stage: str,
        progress: float,
        message: str,
    ) -> None:
        if progress_callback:
            progress_callback(stage, progress, message)

    def _json_safe(self, value):
        if value is None:
            return None
        if isinstance(value, dict):
            return {str(key): self._json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._json_safe(item) for item in value]
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None
            return value
        if isinstance(value, (str, int, bool)):
            return value

        # Handle pandas / numpy scalar values without importing them directly.
        isoformat = getattr(value, "isoformat", None)
        if callable(isoformat):
            try:
                return isoformat()
            except Exception:
                pass

        item = getattr(value, "item", None)
        if callable(item):
            try:
                return self._json_safe(item())
            except Exception:
                pass

        if str(value).lower() in {"nan", "nat", "none"}:
            return None

        return str(value)
