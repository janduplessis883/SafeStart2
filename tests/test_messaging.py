from __future__ import annotations

import unittest
from datetime import date

from safestart2.messaging import build_email_message, build_outreach_message


class MessagingTests(unittest.TestCase):
    def test_build_outreach_message_uses_overdue_wording_without_due_dates(self) -> None:
        recall = {
            "full_name": "Nada Example",
            "vaccines_display": "PCV, Shingles",
            "due_date": "2026-03-08",
            "message_due_mode": "overdue",
            "surgery_name": "Stanhope Mews Surgery",
        }

        message = build_outreach_message(
            recall,
            today_local=date(2026, 3, 8),
        )

        self.assertIn("you are eligible for the following vaccines: PCV, Shingles.", message)
        self.assertNotIn("08/03/2026", message)

    def test_build_outreach_message_uses_future_due_date_wording(self) -> None:
        recall = {
            "full_name": "Nada Example",
            "vaccines_display": "Shingles",
            "due_date": "2026-03-18",
            "message_due_mode": "future",
            "surgery_name": "Stanhope Mews Surgery",
        }

        message = build_outreach_message(
            recall,
            today_local=date(2026, 3, 8),
        )

        self.assertIn("the following vaccines become due on 18/03/2026: Shingles.", message)

    def test_build_email_message_includes_nhs_link_and_overdue_wording(self) -> None:
        recall = {
            "full_name": "Nada Example",
            "vaccines_display": "PCV, Shingles",
            "due_date": "2026-03-08",
            "message_due_mode": "overdue",
            "surgery_name": "Stanhope Mews Surgery",
        }

        message = build_email_message(
            recall,
            today_local=date(2026, 3, 8),
        )

        self.assertEqual(
            message,
            "Dear Nada, you are eligible for the following vaccines: PCV, Shingles.\n"
            "Read more about vaccinations on the NHS vaccination website at https://www.nhs.uk/vaccinations/\n"
            "We will send a self-book link via SMS to arrange this.\n"
            "Regards,\n"
            "Stanhope Mews Surgery",
        )


if __name__ == "__main__":
    unittest.main()
