import unittest

import datetime

from eligibility import assess_eligibility

from main import (
    find_recent_singapore_quant_jobs,
    is_quant_intern_role,
    is_singapore_job,
    is_target_role,
    is_undergrad_technical_job,
    parse_quant_firms,
    parse_singapore_internships,
)


class SingaporeJobFilterTests(unittest.TestCase):
    def test_existing_title_filter_behavior(self):
        self.assertTrue(is_target_role("Software Engineer Intern"))
        self.assertTrue(is_target_role("Technology Summer Analyst"))
        self.assertTrue(is_target_role("Software Engineering Industrial Attachment"))
        self.assertTrue(is_target_role("Data Engineering Co-op"))
        self.assertTrue(is_target_role("Machine Learning Engineer, Marketing Intern"))
        self.assertTrue(is_target_role("GenAI Product Development Intern"))
        self.assertTrue(is_target_role("Quant Trading Winternship"))
        self.assertTrue(is_target_role("Software Engineer Trainee"))
        self.assertTrue(is_target_role("Accelerator Program - Backend Engineer"))
        self.assertFalse(is_target_role("Marketing Data Intern"))
        self.assertFalse(is_target_role("Engineering Intern, HVAC"))
        self.assertFalse(is_target_role("Finance Research Intern"))
        self.assertFalse(is_target_role("AI Product Manager Intern"))
        self.assertFalse(is_target_role("Software Engineer"))
        self.assertFalse(is_target_role("International Software Engineer"))

    def test_undergraduate_eligibility_filter(self):
        self.assertFalse(is_undergrad_technical_job({
            "title": "Quantitative Research Intern - PhD",
            "description": "Research role",
        }))
        self.assertFalse(is_undergrad_technical_job({
            "title": "Quantitative Research Intern",
            "description": "Candidates must be currently pursuing a PhD.",
        }))
        self.assertTrue(is_undergrad_technical_job({
            "title": "Quantitative Research Intern",
            "description": "Open to Bachelor, Master, or PhD students.",
        }))

    def test_structured_eligibility_extracts_detailed_requirements(self):
        assessment = assess_eligibility(
            "Software Engineer Intern",
            """
            <p>Open to Bachelor's or Master's students graduating in 2028.</p>
            <p>This is a 6 month internship. Candidates must already have the
            legal right to work in Singapore.</p>
            """,
        )

        self.assertEqual(assessment.verdict, "likely_eligible")
        self.assertEqual(assessment.degree_levels, ["Bachelor's", "Master's"])
        self.assertEqual(assessment.graduation_years, [2028])
        self.assertEqual(assessment.duration, "6 month")
        self.assertEqual(
            assessment.work_authorization,
            "Existing work authorization required",
        )

    def test_structured_eligibility_rejects_exclusive_postgraduate_roles(self):
        descriptions = [
            "PhD candidates only.",
            "You must be currently pursuing a Master's degree.",
            "Open to postgraduate students only.",
        ]

        for description in descriptions:
            with self.subTest(description=description):
                self.assertEqual(
                    assess_eligibility(
                        "Quantitative Research Intern",
                        description,
                    ).verdict,
                    "ineligible",
                )

    def test_structured_eligibility_keeps_unknown_and_inclusive_roles(self):
        self.assertEqual(
            assess_eligibility("Software Engineer Intern", "Team player").verdict,
            "unknown",
        )
        self.assertEqual(
            assess_eligibility(
                "Quantitative Research Intern",
                "Open to Bachelor, Master, or PhD students.",
            ).verdict,
            "likely_eligible",
        )

    def test_quant_specific_title_filter(self):
        self.assertTrue(is_quant_intern_role("Quantitative Research Internship"))
        self.assertTrue(is_quant_intern_role("Algorithm Development Intern"))
        self.assertTrue(is_quant_intern_role("Software Engineering Internship"))
        self.assertFalse(is_quant_intern_role("Quantitative Researcher"))
        self.assertFalse(is_quant_intern_role("Marketing Data Intern"))

    def test_accepts_singapore_location_variants(self):
        accepted_jobs = [
            {"location": "Singapore"},
            {"location": "Singapore, Singapore"},
            {"location": "Bedok, SG"},
            {"country": "sg"},
            {"location": "Remote - Singapore"},
            {"location": ["Hong Kong", "Singapore"]},
        ]

        for job in accepted_jobs:
            with self.subTest(job=job):
                self.assertTrue(is_singapore_job(job))

    def test_rejects_non_singapore_and_unknown_locations(self):
        rejected_jobs = [
            {"location": "Bengaluru"},
            {"location": "Ho Chi Minh, Vietnam", "country": "VN"},
            {"location": "Petaling Jaya, Malaysia", "country": "MY"},
            {"location": "Remote"},
            {"location": "APAC"},
            {"location": "Worldwide"},
            {"location": None},
            {},
            {"location": "SGP"},
            {"location": "Singaporean applicants preferred"},
        ]

        for job in rejected_jobs:
            with self.subTest(job=job):
                self.assertFalse(is_singapore_job(job))

    def test_representative_source_payloads(self):
        source_jobs = {
            "jobspy": ({"location": "Singapore, S00, SG"}, True),
            "internsg": ({"location": "SG Work from Home"}, True),
            "greenhouse": ({"location": "Bengaluru"}, False),
            "lever": (
                {
                    "country": "SG",
                    "location": ["Singapore, Singapore"],
                },
                True,
            ),
            "smartrecruiters": (
                {
                    "country": "my",
                    "location": ["Petaling Jaya, Malaysia", "Petaling Jaya"],
                },
                False,
            ),
        }

        for source, (job, expected) in source_jobs.items():
            with self.subTest(source=source):
                self.assertEqual(is_singapore_job(job), expected)


class QuantIndexTests(unittest.TestCase):
    QUANT_MARKDOWN = """
## Hudson River Trading
**Website**: [Hudson River Trading](https://example.com/hrt)

## DRW
**Website**: [DRW](https://example.com/drw)
"""

    SINGAPORE_MARKDOWN = """
| Company | Role | Track | Application | Date Added |
|---|---|:---:|:---:|:---:|
| [Hudson River Trading](https://example.com/company) | Software Engineering Internship - Summer 2027 | <a href="https://example.com/track">Track</a> | <a href="https://example.com/apply?a=1&amp;b=2">Apply</a> | 14 Jul 2026 |
| [DRW](https://example.com/company) | Trader | <a href="https://example.com/track">Track</a> | <a href="https://example.com/trader">Apply</a> | 14 Jul 2026 |
| [Unrelated Tech](https://example.com/company) | Software Engineer Intern | <a href="https://example.com/track">Track</a> | <a href="https://example.com/unrelated">Apply</a> | 14 Jul 2026 |
| [DRW](https://example.com/company) | Software Developer Intern | <a href="https://example.com/track">Track</a> | <a href="https://example.com/old">Apply</a> | 01 Jun 2026 |
"""

    def test_parses_quant_firms(self):
        self.assertEqual(
            parse_quant_firms(self.QUANT_MARKDOWN),
            {"hudsonrivertrading", "drw"},
        )

    def test_parses_singapore_jobs_and_unescapes_url(self):
        jobs = parse_singapore_internships(self.SINGAPORE_MARKDOWN)

        self.assertEqual(len(jobs), 4)
        self.assertEqual(
            jobs[0]["job_url"],
            "https://example.com/apply?a=1&b=2",
        )
        self.assertEqual(jobs[0]["date_added"], datetime.date(2026, 7, 14))

    def test_intersects_firms_roles_and_recency(self):
        jobs = find_recent_singapore_quant_jobs(
            self.QUANT_MARKDOWN,
            self.SINGAPORE_MARKDOWN,
            today=datetime.date(2026, 7, 15),
        )

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["company"], "Hudson River Trading")
        self.assertEqual(jobs[0]["location"], "Singapore")


if __name__ == "__main__":
    unittest.main()
