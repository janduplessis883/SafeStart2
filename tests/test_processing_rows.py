from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from safestart2.processing import build_patients_from_dataframe, build_patients_from_rows, process_immunizeme_rows


class ProcessImmunizeMeRowsTests(unittest.TestCase):
    def test_process_immunizeme_rows_uses_internal_raw_payloads(self) -> None:
        rows = [
            {
                "source_patient_id": "1",
                "first_name": "Maya",
                "last_name": "Fernandes",
                "nhs_number": "7357254488",
                "sex": "F",
                "date_of_birth": "2024-02-26",
                "registration_date": "2024-03-01",
                "raw_vaccine_name": "Custom MMR dose",
                "phone": "07486321744",
                "event_date": "2025-02-26",
                "event_done_at_id": "evt-1",
            }
        ]
        overrides = {
            "custom mmr dose": ("MMR", "routine_child"),
        }

        cohort = process_immunizeme_rows(
            rows,
            reference_date=date(2026, 3, 8),
            lookahead_days=30,
            overrides=overrides,
        )

        self.assertEqual(cohort.raw_rows, 1)
        self.assertEqual(len(cohort.patients), 1)
        self.assertEqual(cohort.patients[0].vaccine_events[0].canonical_vaccine, "MMR")

    def test_shingles_second_dose_is_due_six_months_after_first_dose(self) -> None:
        rows = [
            {
                "source_patient_id": "1",
                "first_name": "Nicoletta",
                "last_name": "Example",
                "nhs_number": "7000000001",
                "sex": "F",
                "date_of_birth": "1959-01-10",
                "registration_date": "2000-01-01",
                "raw_vaccine_name": "Shingles",
                "phone": "07486321744",
                "email": "nicoletta@example.com",
                "event_date": "2025-09-15",
                "event_done_at_id": "evt-1",
            }
        ]

        cohort = process_immunizeme_rows(
            rows,
            reference_date=date(2026, 3, 8),
            lookahead_days=30,
            overrides=None,
        )

        shingles_recalls = [item for item in cohort.recommendations if item.vaccine_group == "Shingles"]
        self.assertEqual(len(shingles_recalls), 1)
        self.assertEqual(shingles_recalls[0].due_date, date(2026, 3, 15))

    def test_unvaccinated_patients_become_shingles_eligible_at_cutoff(self) -> None:
        rows = [
            {
                "source_patient_id": "1",
                "first_name": "Shingles",
                "last_name": "Eligible",
                "nhs_number": "7000000003",
                "sex": "F",
                "date_of_birth": "1958-09-01",
                "registration_date": "2000-01-01",
                "raw_vaccine_name": "Unknown",
                "phone": "07486321744",
                "email": "eligible@example.com",
                "event_date": None,
                "event_done_at_id": "evt-3",
            }
        ]

        cohort = process_immunizeme_rows(
            rows,
            reference_date=date(2026, 3, 8),
            lookahead_days=30,
            overrides=None,
        )

        vaccines = {item.vaccine_group for item in cohort.recommendations}
        statuses = {item.status for item in cohort.recommendations}
        self.assertIn("Pneumococcal", vaccines)
        self.assertIn("Shingles", vaccines)
        self.assertIn("Flu", vaccines)
        self.assertEqual(statuses, {"unvaccinated"})

    def test_unvaccinated_patients_with_pre_cutoff_shingles_due_date_are_not_eligible(self) -> None:
        rows = [
            {
                "source_patient_id": "1",
                "first_name": "PreCutoff",
                "last_name": "Patient",
                "nhs_number": "7000000004",
                "sex": "F",
                "date_of_birth": "1950-01-10",
                "registration_date": "2000-01-01",
                "raw_vaccine_name": "Unknown",
                "phone": "07486321744",
                "email": "precutoff@example.com",
                "event_date": None,
                "event_done_at_id": "evt-4",
            }
        ]

        cohort = process_immunizeme_rows(
            rows,
            reference_date=date(2026, 3, 8),
            lookahead_days=30,
            overrides=None,
        )

        vaccines = {item.vaccine_group for item in cohort.recommendations}
        self.assertNotIn("Shingles", vaccines)

    def test_shingles_second_dose_is_not_recalled_for_pre_cutoff_first_dose_cohorts(self) -> None:
        rows = [
            {
                "source_patient_id": "1",
                "first_name": "Older",
                "last_name": "Patient",
                "nhs_number": "7000000005",
                "sex": "F",
                "date_of_birth": "1950-01-10",
                "registration_date": "2000-01-01",
                "raw_vaccine_name": "Shingles",
                "phone": "07486321744",
                "email": "older@example.com",
                "event_date": "2024-09-15",
                "event_done_at_id": "evt-5",
            }
        ]

        cohort = process_immunizeme_rows(
            rows,
            reference_date=date(2026, 3, 8),
            lookahead_days=30,
            overrides=None,
        )

        shingles_recalls = [item for item in cohort.recommendations if item.vaccine_group == "Shingles"]
        self.assertEqual(shingles_recalls, [])

    def test_build_patients_from_rows_treats_nat_event_date_as_missing(self) -> None:
        patients, warnings, mapped_rows = build_patients_from_rows(
            [
                {
                    "source_patient_id": "1",
                    "first_name": "Excel",
                    "last_name": "Patient",
                    "nhs_number": "7000000002",
                    "sex": "F",
                    "date_of_birth": "2024-02-26",
                    "registration_date": "2024-03-01",
                    "raw_vaccine_name": "MMR",
                    "phone": "07486321744",
                    "email": "excel@example.com",
                    "event_date": pd.NaT,
                    "event_done_at_id": "evt-2",
                }
            ]
        )

        self.assertEqual(len(warnings), 0)
        self.assertEqual(mapped_rows, 1)
        self.assertEqual(len(patients), 1)
        self.assertIsNone(patients[0].vaccine_events[0].event_date)

    def test_build_patients_supports_19_64_header_variants_and_source_id_fallback(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date of birth": "1949-10-12",
                    "Email address": "alice@example.com",
                    "First name": "Alice",
                    "Preferred telephone number": "07486321744",
                    "Sex": "Female",
                    "Surname": "Example",
                    "BREAKDOWN 19-50yrs: Patient ID": "49865396",
                    "IMMUNIZEME PT 19 - 64yrs: Patient ID": "49865396",
                    "ImmunizeMe - vaccines 19 - 64yrs: Event date": "2025-01-12",
                    "ImmunizeMe - vaccines 19 - 64yrs: Patient ID": "49865396",
                    "ImmunizeMe - vaccines 19 - 64yrs: Vaccination type": "Shingles",
                }
            ]
        )
        patients, warnings, mapped_rows = build_patients_from_dataframe(df)

        self.assertEqual(len(warnings), 0)
        self.assertEqual(mapped_rows, 1)
        self.assertEqual(len(patients), 1)
        self.assertEqual(patients[0].nhs_number, "49865396")

    def test_build_patients_supports_0_18_header_variants(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date of birth": "2010-03-15",
                    "Email address": "child@example.com",
                    "First name": "Paloma",
                    "NHS number": "7072522115",
                    "Preferred telephone number": "07919070364",
                    "Surname": "Garro",
                    "ImmunizeMe - vaccines 0 - 18yrs: Event date": "2011-06-20",
                    "ImmunizeMe - vaccines 0 - 18yrs: Patient ID": "49828201",
                    "ImmunizeMe - vaccines 0 - 18yrs: Vaccination type": "Measles/Mumps/Rubella 1",
                    "IMMUNIZEME PT 0 - 18yrs: Patient ID": "49828201",
                }
            ]
        )
        patients, warnings, mapped_rows = build_patients_from_dataframe(df)

        self.assertEqual(len(warnings), 0)
        self.assertEqual(mapped_rows, 1)
        self.assertEqual(len(patients), 1)
        self.assertEqual(len(patients[0].vaccine_events), 1)
        self.assertEqual(patients[0].vaccine_events[0].canonical_vaccine, "MMR")

    def test_build_patients_supports_51_120_header_variants(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date of birth": "1949-10-12",
                    "Email address": "alice@example.com (Unverified)",
                    "First name": "Alice",
                    "NHS number": "4681934003",
                    "Preferred telephone number": "07486321744",
                    "Surname": "Example",
                    "ImmunizeMe - vaccines 51-120yrs: Event date": "2025-01-12",
                    "ImmunizeMe - vaccines 51-120yrs: Patient ID": "49827137",
                    "ImmunizeMe - vaccines 51-120yrs: Vaccination type": "Influenza Vaccine 1",
                    "IMMUNIZEME PT 51-120yrs: Patient ID": "49827137",
                }
            ]
        )
        patients, warnings, mapped_rows = build_patients_from_dataframe(df)

        self.assertEqual(len(warnings), 0)
        self.assertEqual(mapped_rows, 1)
        self.assertEqual(len(patients), 1)
        self.assertEqual(len(patients[0].vaccine_events), 1)
        self.assertEqual(patients[0].vaccine_events[0].canonical_vaccine, "Flu")
        self.assertEqual(patients[0].email, "alice@example.com")


if __name__ == "__main__":
    unittest.main()
