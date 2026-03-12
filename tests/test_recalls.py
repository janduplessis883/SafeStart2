from __future__ import annotations

import unittest
from datetime import date

from safestart2.recalls import group_recalls


class RecallGroupingTests(unittest.TestCase):
    def test_future_recalls_only_group_when_due_date_matches(self) -> None:
        recalls = [
            {
                "id": "r1",
                "surgery_id": "s1",
                "nhs_number": "1001",
                "full_name": "Ada Example",
                "due_date": "2026-03-20",
                "status": "due_soon",
                "priority": 20,
                "vaccine_group": "PCV",
                "program_area": "routine_adult",
                "reason": "PCV recall",
            },
            {
                "id": "r2",
                "surgery_id": "s1",
                "nhs_number": "1001",
                "full_name": "Ada Example",
                "due_date": "2026-03-20",
                "status": "due_soon",
                "priority": 20,
                "vaccine_group": "Shingles",
                "program_area": "routine_adult",
                "reason": "Shingles recall",
            },
            {
                "id": "r3",
                "surgery_id": "s1",
                "nhs_number": "1001",
                "full_name": "Ada Example",
                "due_date": "2026-04-20",
                "status": "due_soon",
                "priority": 20,
                "vaccine_group": "RSV",
                "program_area": "routine_adult",
                "reason": "RSV recall",
            },
        ]

        grouped = group_recalls(recalls, today_local=date(2026, 3, 10))

        self.assertEqual(len(grouped), 2)
        self.assertEqual(grouped[0]["vaccines"], ["PCV", "Shingles"])
        self.assertEqual(grouped[0]["due_date"], "2026-03-20")
        self.assertEqual(grouped[0]["message_due_mode"], "future")
        self.assertEqual(grouped[1]["vaccines"], ["RSV"])
        self.assertEqual(grouped[1]["due_date"], "2026-04-20")

    def test_past_due_recalls_group_per_patient_and_preserve_original_due_dates(self) -> None:
        recalls = [
            {
                "id": "r1",
                "surgery_id": "s1",
                "nhs_number": "1001",
                "full_name": "Ada Example",
                "due_date": "2026-03-01",
                "status": "due_now",
                "priority": 20,
                "vaccine_group": "PCV",
                "program_area": "routine_adult",
                "reason": "PCV recall",
            },
            {
                "id": "r2",
                "surgery_id": "s1",
                "nhs_number": "1001",
                "full_name": "Ada Example",
                "due_date": "2026-02-01",
                "status": "overdue",
                "priority": 10,
                "vaccine_group": "Shingles",
                "program_area": "routine_adult",
                "reason": "Shingles recall",
            },
            {
                "id": "r3",
                "surgery_id": "s1",
                "nhs_number": "1001",
                "full_name": "Ada Example",
                "due_date": "2026-03-20",
                "status": "due_soon",
                "priority": 30,
                "vaccine_group": "RSV",
                "program_area": "routine_adult",
                "reason": "RSV recall",
            },
        ]

        grouped = group_recalls(recalls, today_local=date(2026, 3, 10))

        self.assertEqual(len(grouped), 2)
        overdue_group = grouped[0]
        future_group = grouped[1]

        self.assertEqual(overdue_group["status"], "overdue")
        self.assertTrue(overdue_group["has_overdue_vaccines"])
        self.assertEqual(overdue_group["vaccines"], ["PCV", "Shingles"])
        self.assertEqual(overdue_group["due_date"], "2026-02-01")
        self.assertEqual(overdue_group["original_due_dates"], ["2026-02-01", "2026-03-01"])
        self.assertEqual(
            [(item["vaccine"], item["due_date"]) for item in overdue_group["due_items"]],
            [("Shingles", "2026-02-01"), ("PCV", "2026-03-01")],
        )
        self.assertEqual(overdue_group["message_due_mode"], "overdue")

        self.assertEqual(future_group["vaccines"], ["RSV"])
        self.assertEqual(future_group["message_due_mode"], "future")


if __name__ == "__main__":
    unittest.main()
