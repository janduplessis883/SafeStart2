from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from safestart2.models import Patient, ProcessedCohort, Recommendation, VaccineEvent
from safestart2.workflow import (
    classify_recall_workflow,
    compare_processed_cohorts,
    summarize_patient_recall,
)


class WorkflowTests(unittest.TestCase):
    def test_classify_recall_workflow_ready_to_text(self) -> None:
        recall = {"phone": "07486321744", "status": "due_now"}
        self.assertEqual(
            classify_recall_workflow(recall, [], sent_recently_days=14, today_local=date(2026, 3, 8)),
            "Ready to text",
        )

    def test_classify_recall_workflow_prepared_today(self) -> None:
        recall = {"phone": "07486321744", "status": "due_now"}
        attempts = [
            {
                "communication_method": "bulk_sms",
                "outcome": "prepared",
                "sent_at": "2026-03-08T09:00:00+00:00",
            }
        ]
        self.assertEqual(
            classify_recall_workflow(recall, attempts, sent_recently_days=14, today_local=date(2026, 3, 8)),
            "Prepared today",
        )

    def test_summarize_patient_recall(self) -> None:
        recall = {
            "phone": "07486321744",
            "status": "due_now",
            "vaccines": ["MMR", "PCV"],
        }
        patient_timeline = {
            "events": [
                {"event_date": "2025-02-26", "canonical_vaccine": "MMR"},
            ],
            "attempts": [
                {"sent_at": "2026-03-07T12:00:00+00:00", "communication_method": "bulk_sms"},
            ],
        }

        summary = summarize_patient_recall(
            recall,
            [],
            patient_timeline,
            sent_recently_days=14,
            today_local=date(2026, 3, 8),
        )

        self.assertEqual(summary["workflow_state"], "Ready to text")
        self.assertEqual(summary["due_vaccines"], ["MMR", "PCV"])
        self.assertEqual(pd.Timestamp(summary["last_vaccination_date"]).date(), date(2025, 2, 26))
        self.assertEqual(str(summary["last_outreach_method"]), "bulk_sms")

    def test_compare_processed_cohorts(self) -> None:
        previous_patient = Patient(
            nhs_number="7357254488",
            source_patient_id="1",
            first_name="Maya",
            last_name="Fernandes",
            sex="F",
            date_of_birth=date(2024, 2, 26),
            phone="07486321744",
            email=None,
            registration_date=None,
            vaccine_events=[
                VaccineEvent(
                    canonical_vaccine="MenB",
                    vaccine_program="routine_child",
                    raw_vaccine_name="MenB 1",
                    event_date=date(2024, 4, 22),
                )
            ],
        )
        current_patient = Patient(
            nhs_number="7357254488",
            source_patient_id="1",
            first_name="Maya",
            last_name="Fernandes",
            sex="F",
            date_of_birth=date(2024, 2, 26),
            phone="07486321744",
            email=None,
            registration_date=None,
            vaccine_events=[
                VaccineEvent(
                    canonical_vaccine="MenB",
                    vaccine_program="routine_child",
                    raw_vaccine_name="MenB 1",
                    event_date=date(2024, 4, 22),
                ),
                VaccineEvent(
                    canonical_vaccine="MMR",
                    vaccine_program="routine_child",
                    raw_vaccine_name="MMR 1",
                    event_date=date(2025, 2, 26),
                ),
            ],
        )
        previous = ProcessedCohort(
            patients=[previous_patient],
            recommendations=[
                Recommendation(
                    patient_nhs_number="7357254488",
                    patient_name="Maya Fernandes",
                    date_of_birth=date(2024, 2, 26),
                    phone="07486321744",
                    email=None,
                    recommendation_type="routine",
                    vaccine_group="MMR",
                    program_area="routine_child",
                    due_date=date(2025, 2, 25),
                    status="overdue",
                    priority=10,
                    reason="Missing MMR",
                    explanation={},
                ),
                Recommendation(
                    patient_nhs_number="7357254488",
                    patient_name="Maya Fernandes",
                    date_of_birth=date(2024, 2, 26),
                    phone="07486321744",
                    email=None,
                    recommendation_type="seasonal",
                    vaccine_group="Flu",
                    program_area="seasonal_child",
                    due_date=date(2025, 9, 1),
                    status="due_soon",
                    priority=30,
                    reason="Flu due",
                    explanation={},
                ),
            ],
            warnings=[],
            raw_rows=1,
            mapped_rows=1,
            unvaccinated_patients=0,
        )
        current = ProcessedCohort(
            patients=[current_patient],
            recommendations=[
                Recommendation(
                    patient_nhs_number="7357254488",
                    patient_name="Maya Fernandes",
                    date_of_birth=date(2024, 2, 26),
                    phone="07486321744",
                    email=None,
                    recommendation_type="seasonal",
                    vaccine_group="Flu",
                    program_area="seasonal_child",
                    due_date=date(2025, 9, 1),
                    status="due_now",
                    priority=20,
                    reason="Flu due now",
                    explanation={},
                ),
                Recommendation(
                    patient_nhs_number="7357254488",
                    patient_name="Maya Fernandes",
                    date_of_birth=date(2024, 2, 26),
                    phone="07486321744",
                    email=None,
                    recommendation_type="routine",
                    vaccine_group="PCV",
                    program_area="routine_child",
                    due_date=date(2025, 3, 1),
                    status="due_now",
                    priority=20,
                    reason="PCV due",
                    explanation={},
                ),
            ],
            warnings=[],
            raw_rows=2,
            mapped_rows=2,
            unvaccinated_patients=0,
        )

        comparison = compare_processed_cohorts(previous, current)

        self.assertEqual(comparison["new_vaccine_events"], 1)
        self.assertEqual(comparison["new_recall_count"], 1)
        self.assertEqual(comparison["resolved_recall_count"], 1)
        self.assertEqual(comparison["status_change_count"], 1)
        self.assertEqual(comparison["patients_with_status_changes"], 1)


if __name__ == "__main__":
    unittest.main()
