from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from safestart2.supabase_store import SupabaseStore, UserContext


class _FakeResponse:
    def __init__(self, data):
        self.data = data


class _FakeQuery:
    def __init__(
        self,
        rows: list[dict],
        log: list[tuple[int, int]],
        *,
        table_name: str,
        event_patient_chunks: list[tuple[str, ...]] | None = None,
    ) -> None:
        self._rows = rows
        self._log = log
        self._table_name = table_name
        self._event_patient_chunks = event_patient_chunks
        self._start = 0
        self._end = len(rows) - 1
        self._filters: dict[str, object] = {}
        self._neq_filters: dict[str, object] = {}
        self._update_payload: dict[str, object] | None = None

    def select(self, _fields: str):
        return self

    def range(self, start: int, end: int):
        self._start = start
        self._end = end
        self._log.append((start, end))
        return self

    def limit(self, value: int):
        self._start = 0
        self._end = max(0, value - 1)
        return self

    def eq(self, field: str, value: object):
        self._filters[field] = value
        return self

    def neq(self, field: str, value: object):
        self._neq_filters[field] = value
        return self

    def in_(self, field: str, values: list[object]):
        values_tuple = tuple(str(value) for value in values)
        self._filters[field] = set(values_tuple)
        if self._table_name == "vaccination_events" and self._event_patient_chunks is not None:
            self._event_patient_chunks.append(values_tuple)
        return self

    def update(self, payload: dict[str, object], returning: str | None = None):
        self._update_payload = dict(payload)
        return self

    def execute(self):
        filtered = [
            row for row in self._rows
            if all(
                (
                    str(row.get(field) or "") in value
                    if isinstance(value, set)
                    else row.get(field) == value
                )
                for field, value in self._filters.items()
            )
            and all(row.get(field) != value for field, value in self._neq_filters.items())
        ]
        if self._update_payload is not None:
            updated_rows = []
            for row in filtered[self._start:self._end + 1]:
                row.update(self._update_payload)
                updated_rows.append(dict(row))
            return _FakeResponse(updated_rows)
        return _FakeResponse(filtered[self._start:self._end + 1])

    def order(self, _field: str, desc: bool = False):
        self._rows = sorted(
            self._rows,
            key=lambda row: str(row.get(_field) or ""),
            reverse=desc,
        )
        return self


class _FakeClient:
    def __init__(
        self,
        tables: dict[str, list[dict]],
        query_logs: dict[str, list[tuple[int, int]]],
        *,
        event_patient_chunks: list[tuple[str, ...]] | None = None,
    ) -> None:
        self._tables = tables
        self._query_logs = query_logs
        self._event_patient_chunks = event_patient_chunks

    def table(self, name: str):
        if name not in self._tables:
            raise AssertionError(f"Unexpected table lookup: {name}")
        return _FakeQuery(
            self._tables[name],
            self._query_logs.setdefault(name, []),
            table_name=name,
            event_patient_chunks=self._event_patient_chunks,
        )


class SupabaseStoreTests(unittest.TestCase):
    def test_rebuild_surgery_from_batch_reprocesses_stored_row_payloads(self) -> None:
        query_logs: dict[str, list[tuple[int, int]]] = {}
        store = object.__new__(SupabaseStore)
        store.client = _FakeClient(
            {
                "import_batches": [
                    {
                        "id": "batch-1",
                        "surgery_id": "s1",
                        "uploaded_by_email": "staff@example.com",
                        "source_filename": "upload.csv",
                        "row_count": 1,
                        "patient_count": 1,
                        "recommendation_count": 1,
                        "unvaccinated_count": 0,
                        "imported_at": "2026-03-10T09:00:00+00:00",
                        "notes": '{"reference_date":"2026-03-09","lookahead_days":30}',
                    }
                ]
            },
            query_logs,
        )
        store._get_surgery_by_id = lambda surgery_id: {
            "id": surgery_id,
            "surgery_code": "ABC123",
            "surgery_name": "Example Surgery",
            "sms_sender_id": "SafeStart2",
        }
        raw_payloads = [
            {
                "source_patient_id": "1",
                "first_name": "Import",
                "last_name": "Patient",
                "nhs_number": "7000000001",
                "sex": "F",
                "date_of_birth": "1950-01-10",
                "registration_date": "2000-01-01",
                "raw_vaccine_name": "Unknown",
                "phone": "07486321744",
                "email": "import@example.com",
                "event_date": None,
                "event_done_at_id": "evt-1",
            }
        ]
        store._load_import_row_payloads = lambda batch_id: raw_payloads
        store._parse_notes_json = lambda notes: {"reference_date": "2026-03-09", "lookahead_days": 30}
        store.get_alias_overrides = lambda surgery_id=None: {"custom": ("MMR", "routine_child")}
        cleared: list[str] = []
        persisted: list[dict] = []
        store.clear_import_data = lambda user_context, surgery_id: cleared.append(surgery_id) or {}
        store.persist_processed_cohort = (
            lambda **kwargs: persisted.append(kwargs)
            or {"patients": 1, "events": 1, "recommendations": 1}
        )
        user_context = UserContext(
            email="staff@example.com",
            full_name="Staff User",
            role="staff",
            surgery_id="s1",
        )

        with patch("safestart2.supabase_store.process_immunizeme_rows") as process_rows:
            process_rows.return_value = "cohort"
            result = store.rebuild_surgery_from_batch(user_context=user_context, batch_id="batch-1")

        process_rows.assert_called_once_with(
            raw_payloads,
            reference_date=date(2026, 3, 9),
            lookahead_days=30,
            overrides={"custom": ("MMR", "routine_child")},
        )
        self.assertEqual(cleared, ["s1"])
        self.assertEqual(persisted[0]["cohort"], "cohort")
        self.assertEqual(result, {"patients": 1, "events": 1, "recommendations": 1})

    def test_run_flu_season_rollover_updates_target_rows_and_deactivates_duplicates(self) -> None:
        query_logs: dict[str, list[tuple[int, int]]] = {}
        recall_rows = [
            {
                "id": "rec-1",
                "surgery_id": "s1",
                "patient_id": "p1",
                "vaccine_group": "Flu",
                "recommendation_type": "seasonal",
                "program_area": "seasonal_adult",
                "due_date": "2025-09-01",
                "status": "overdue",
                "is_active": True,
                "updated_at": "2026-03-01T10:00:00+00:00",
            },
            {
                "id": "rec-2",
                "surgery_id": "s1",
                "patient_id": "p2",
                "vaccine_group": "Flu",
                "recommendation_type": "seasonal",
                "program_area": "seasonal_adult",
                "due_date": "2026-09-01",
                "status": "overdue",
                "is_active": True,
                "updated_at": "2026-03-02T10:00:00+00:00",
            },
            {
                "id": "rec-3",
                "surgery_id": "s1",
                "patient_id": "p2",
                "vaccine_group": "Flu",
                "recommendation_type": "seasonal",
                "program_area": "seasonal_adult",
                "due_date": "2025-09-01",
                "status": "overdue",
                "is_active": True,
                "updated_at": "2026-03-01T09:00:00+00:00",
            },
            {
                "id": "rec-4",
                "surgery_id": "s1",
                "patient_id": "p3",
                "vaccine_group": "Flu",
                "recommendation_type": "seasonal",
                "program_area": "seasonal_child",
                "due_date": "2026-09-01",
                "status": "overdue",
                "is_active": True,
                "updated_at": "2026-03-03T10:00:00+00:00",
            },
            {
                "id": "rec-5",
                "surgery_id": "s2",
                "patient_id": "other",
                "vaccine_group": "Flu",
                "recommendation_type": "seasonal",
                "program_area": "seasonal_adult",
                "due_date": "2025-09-01",
                "status": "overdue",
                "is_active": True,
                "updated_at": "2026-03-01T10:00:00+00:00",
            },
        ]
        store = object.__new__(SupabaseStore)
        store.client = _FakeClient({"recall_recommendations": recall_rows}, query_logs)
        user_context = UserContext(
            email="staff@example.com",
            full_name="Staff User",
            role="staff",
            surgery_id="s1",
        )

        result = store.run_flu_season_rollover(
            user_context=user_context,
            surgery_id="s1",
            reference_date=date(2026, 3, 8),
        )

        self.assertEqual(result["target_due_date"], "2026-09-01")
        self.assertEqual(result["target_status"], "due_soon")
        self.assertEqual(result["examined_count"], 4)
        self.assertEqual(result["updated_count"], 3)
        self.assertEqual(result["deactivated_count"], 1)
        self.assertEqual(recall_rows[0]["due_date"], "2026-09-01")
        self.assertEqual(recall_rows[0]["status"], "due_soon")
        self.assertEqual(recall_rows[1]["status"], "due_soon")
        self.assertFalse(recall_rows[2]["is_active"])
        self.assertEqual(recall_rows[3]["status"], "due_soon")
        self.assertEqual(recall_rows[4]["due_date"], "2025-09-01")

    def test_run_covid_season_rollover_updates_target_rows_and_deactivates_duplicates(self) -> None:
        query_logs: dict[str, list[tuple[int, int]]] = {}
        recall_rows = [
            {
                "id": "covid-1",
                "surgery_id": "s1",
                "patient_id": "p1",
                "vaccine_group": "COVID-19",
                "recommendation_type": "seasonal",
                "program_area": "seasonal_adult",
                "due_date": "2026-01-01",
                "status": "overdue",
                "is_active": True,
                "updated_at": "2026-03-01T10:00:00+00:00",
            },
            {
                "id": "covid-2",
                "surgery_id": "s1",
                "patient_id": "p2",
                "vaccine_group": "COVID-19",
                "recommendation_type": "seasonal",
                "program_area": "seasonal_adult",
                "due_date": "2026-04-13",
                "status": "overdue",
                "is_active": True,
                "updated_at": "2026-03-02T10:00:00+00:00",
            },
            {
                "id": "covid-3",
                "surgery_id": "s1",
                "patient_id": "p2",
                "vaccine_group": "COVID-19",
                "recommendation_type": "seasonal",
                "program_area": "seasonal_adult",
                "due_date": "2026-01-01",
                "status": "overdue",
                "is_active": True,
                "updated_at": "2026-03-01T09:00:00+00:00",
            },
            {
                "id": "covid-4",
                "surgery_id": "s2",
                "patient_id": "other",
                "vaccine_group": "COVID-19",
                "recommendation_type": "seasonal",
                "program_area": "seasonal_adult",
                "due_date": "2026-01-01",
                "status": "overdue",
                "is_active": True,
                "updated_at": "2026-03-01T10:00:00+00:00",
            },
        ]
        store = object.__new__(SupabaseStore)
        store.client = _FakeClient({"recall_recommendations": recall_rows}, query_logs)
        user_context = UserContext(
            email="staff@example.com",
            full_name="Staff User",
            role="staff",
            surgery_id="s1",
        )

        result = store.run_covid_season_rollover(
            user_context=user_context,
            surgery_id="s1",
            reference_date=date(2026, 3, 9),
        )

        self.assertEqual(result["target_due_date"], "2026-04-13")
        self.assertEqual(result["target_status"], "due_soon")
        self.assertEqual(result["examined_count"], 3)
        self.assertEqual(result["updated_count"], 2)
        self.assertEqual(result["deactivated_count"], 1)
        self.assertEqual(recall_rows[0]["due_date"], "2026-04-13")
        self.assertEqual(recall_rows[0]["status"], "due_soon")
        self.assertEqual(recall_rows[1]["status"], "due_soon")
        self.assertFalse(recall_rows[2]["is_active"])
        self.assertEqual(recall_rows[3]["due_date"], "2026-01-01")

    def test_list_import_batches_paginates_past_first_page(self) -> None:
        batch_rows = [
            {
                "id": f"batch-{index}",
                "surgery_id": "s1",
                "uploaded_by_email": "staff@example.com",
                "source_filename": f"upload-{index}.csv",
                "row_count": 100 + index,
                "patient_count": 50 + index,
                "recommendation_count": 20 + index,
                "unvaccinated_count": 5,
                "imported_at": f"2026-03-{(index % 28) + 1:02d}T09:00:00+00:00",
                "notes": "{}",
            }
            for index in range(205)
        ]
        query_logs: dict[str, list[tuple[int, int]]] = {}
        store = object.__new__(SupabaseStore)
        store.client = _FakeClient({"import_batches": batch_rows}, query_logs)
        store.list_accessible_surgeries = lambda _user_context: [
            {
                "id": "s1",
                "surgery_code": "ABC123",
                "surgery_name": "Example Surgery",
                "email": "practice@example.com",
            }
        ]

        user_context = UserContext(
            email="staff@example.com",
            full_name="Staff User",
            role="staff",
            surgery_id="s1",
        )

        results = store.list_import_batches(user_context, surgery_id="s1")

        self.assertEqual(len(results), 205)
        self.assertEqual(
            query_logs["import_batches"],
            [(0, 99), (100, 199), (200, 299)],
        )
        self.assertEqual(results[0]["surgery_code"], "ABC123")
        self.assertEqual(results[-1]["surgery_name"], "Example Surgery")

    def test_list_active_recalls_paginates_past_first_page(self) -> None:
        rows = [
            {
                "id": f"rec-{index}",
                "surgery_id": "s1",
                "nhs_number": f"{7000000000 + index}",
                "full_name": f"Patient {index:04d}",
                "date_of_birth": "2010-01-01",
                "phone": "07000000000",
                "email": f"patient{index}@example.com",
                "recommendation_type": "routine",
                "vaccine_group": "MMR",
                "program_area": "routine_child",
                "due_date": "2026-03-08",
                "status": "due_now",
                "priority": 20,
                "reason": "Missing MMR",
                "explanation": {},
                "updated_at": "2026-03-08T09:00:00+00:00",
            }
            for index in range(1205)
        ]
        query_logs: dict[str, list[tuple[int, int]]] = {}
        store = object.__new__(SupabaseStore)
        store.client = _FakeClient({"v_active_recalls": rows}, query_logs)
        store.list_accessible_surgeries = lambda _user_context: [
            {
                "id": "s1",
                "surgery_code": "ABC123",
                "surgery_name": "Example Surgery",
                "email": "practice@example.com",
            }
        ]
        store._attempt_summary_map = lambda recommendation_ids: {
            recommendation_ids[0]: {
                "attempt_count": 2,
                "last_attempt_at": "2026-03-07T10:00:00+00:00",
                "last_attempt_method": "bulk_sms",
                "last_attempt_outcome": "sent",
            }
        }

        user_context = UserContext(
            email="staff@example.com",
            full_name="Staff User",
            role="staff",
            surgery_id="s1",
        )

        results = store.list_active_recalls(user_context, surgery_id="s1")

        self.assertEqual(len(results), 1205)
        self.assertEqual(query_logs["v_active_recalls"], [(0, 999), (1000, 1999)])
        self.assertEqual(results[0]["surgery_code"], "ABC123")
        self.assertEqual(results[0]["attempt_count"], 2)
        self.assertEqual(results[-1]["surgery_name"], "Example Surgery")

    def test_list_patients_with_vaccination_events_paginates_patients_and_events(self) -> None:
        patient_rows = [
            {
                "id": f"patient-{index}",
                "surgery_id": "s1",
                "nhs_number": f"{7000000000 + index}",
                "full_name": f"Patient {index:04d}",
                "date_of_birth": "2010-01-01",
                "phone": "07000000000",
                "email": f"patient{index}@example.com",
                "registration_date": "2020-01-01",
            }
            for index in range(1205)
        ]
        event_rows = [
            {
                "patient_id": f"patient-{index}",
                "canonical_vaccine": "MMR",
                "event_date": f"2026-03-{(index % 28) + 1:02d}",
            }
            for index in range(1205)
        ]
        event_rows.extend(
            {
                "patient_id": f"patient-{index % 500}",
                "canonical_vaccine": "Flu",
                "event_date": f"2026-02-{(index % 28) + 1:02d}",
            }
            for index in range(1000)
        )
        query_logs: dict[str, list[tuple[int, int]]] = {}
        event_patient_chunks: list[tuple[str, ...]] = []
        store = object.__new__(SupabaseStore)
        store.client = _FakeClient(
            {
                "patients": patient_rows,
                "vaccination_events": event_rows,
            },
            query_logs,
            event_patient_chunks=event_patient_chunks,
        )
        store.list_accessible_surgeries = lambda _user_context: [
            {
                "id": "s1",
                "surgery_code": "ABC123",
                "surgery_name": "Example Surgery",
                "email": "practice@example.com",
            }
        ]

        user_context = UserContext(
            email="staff@example.com",
            full_name="Staff User",
            role="staff",
            surgery_id="s1",
        )

        results = store.list_patients_with_vaccination_events(user_context, surgery_id="s1")

        self.assertEqual(len(results), 1205)
        self.assertEqual(query_logs["patients"], [(0, 999), (1000, 1999)])
        self.assertEqual(
            query_logs["vaccination_events"],
            [(0, 999), (1000, 1999), (0, 999), (0, 999)],
        )
        self.assertEqual(len(event_patient_chunks), 4)
        self.assertEqual(len(event_patient_chunks[0]), 500)
        self.assertEqual(event_patient_chunks[0], event_patient_chunks[1])
        self.assertEqual(len(event_patient_chunks[2]), 500)
        self.assertEqual(len(event_patient_chunks[3]), 205)
        self.assertEqual(sum(item["event_count"] for item in results), 2205)
        self.assertEqual(results[0]["surgery_code"], "ABC123")


if __name__ == "__main__":
    unittest.main()
