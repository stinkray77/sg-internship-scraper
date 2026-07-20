import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from source_adapters import (
    fetch_ashby,
    fetch_bespoke,
    fetch_greenhouse,
    fetch_lever,
    fetch_smartrecruiters,
    fetch_workday,
    load_source_registry,
    validate_source_registry,
)


def response(value=None, text=""):
    result = MagicMock()
    result.json.return_value = value
    result.text = text
    return result


def source(adapter, **config):
    return {
        "id": f"test_{adapter}",
        "company": "Test Finance",
        "adapter": adapter,
        "enabled": True,
        "config": config,
    }


class RegistryTests(unittest.TestCase):
    def test_repository_registry_is_valid_and_contains_core_bespoke_set(self):
        sources = load_source_registry()
        ids = {item["id"] for item in sources}
        expected = {
            "bespoke_jane_street",
            "bespoke_citadel",
            "bespoke_imc",
            "bespoke_jump",
            "bespoke_sig",
            "bespoke_squarepoint",
            "bespoke_alphagrep",
            "bespoke_quantedge",
            "bespoke_dtl",
            "bespoke_sgx",
        }
        self.assertTrue(expected.issubset(ids))

    def test_registry_rejects_duplicate_ids(self):
        duplicate = source("greenhouse", token="one")
        with self.assertRaisesRegex(ValueError, "duplicate source id"):
            validate_source_registry([duplicate, duplicate.copy()])

    def test_registry_requires_version_one(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sources.json"
            path.write_text(json.dumps({"version": 2, "sources": []}))
            with self.assertRaisesRegex(ValueError, "version 1"):
                load_source_registry(path)

    def test_registry_rejects_snapshot_bespoke_source(self):
        invalid = source("bespoke", url="https://example.com")
        invalid["lifecycle_mode"] = "snapshot"
        with self.assertRaisesRegex(ValueError, "complete snapshot"):
            validate_source_registry([invalid])


class StandardAdapterTests(unittest.TestCase):
    def test_greenhouse_normalizes_job(self):
        requester = MagicMock(return_value=response({"jobs": [{
            "id": 1,
            "title": "Software Engineer Intern",
            "absolute_url": "https://example.com/1",
            "location": {"name": "Singapore"},
            "content": "Bachelor students",
        }]}))
        candidates, fetched = fetch_greenhouse(
            source("greenhouse", token="test"), requester
        )
        self.assertEqual(fetched, 1)
        self.assertEqual(candidates[0].location, "Singapore")

    def test_lever_paginates(self):
        first = [{"id": str(index), "text": "Role"} for index in range(100)]
        second = [{
            "id": "target",
            "text": "Software Engineer Intern",
            "hostedUrl": "https://example.com/target",
            "country": "SG",
            "categories": {"location": "Singapore"},
        }]
        requester = MagicMock(side_effect=[response(first), response(second)])
        candidates, fetched = fetch_lever(
            source("lever", token="test"), requester
        )
        self.assertEqual(requester.call_count, 2)
        self.assertEqual(fetched, 101)
        self.assertEqual(candidates[-1].country, "SG")

    def test_smartrecruiters_paginates(self):
        first = {
            "content": [{"id": str(index), "name": "Role"} for index in range(100)],
            "totalFound": 101,
        }
        second = {
            "content": [{
                "id": "target",
                "name": "Data Engineering Intern",
                "location": {"country": "sg", "city": "Singapore"},
            }],
            "totalFound": 101,
        }
        requester = MagicMock(side_effect=[response(first), response(second)])
        candidates, fetched = fetch_smartrecruiters(
            source("smartrecruiters", token="test"), requester
        )
        self.assertEqual(fetched, 101)
        self.assertEqual(candidates[-1].country, "sg")

    def test_smartrecruiters_fetches_detail_only_for_likely_match(self):
        listing = {
            "content": [
                {
                    "id": "my-role",
                    "name": "Software Engineer Intern",
                    "location": {"country": "my", "city": "Kuala Lumpur"},
                },
                {
                    "id": "sg-role",
                    "name": "Data Engineering Intern",
                    "location": {"country": "sg", "city": "Singapore"},
                },
            ],
            "totalFound": 2,
        }
        detail = {"jobAd": {"sections": {
            "jobDescription": {"text": "Build data systems"},
            "qualifications": {"text": "Open to Bachelor students"},
        }}}
        requester = MagicMock(side_effect=[response(listing), response(detail)])

        candidates, fetched = fetch_smartrecruiters(
            source("smartrecruiters", token="test"),
            requester,
        )

        self.assertEqual(fetched, 2)
        self.assertEqual(requester.call_count, 2)
        self.assertEqual(candidates[0].description, "")
        self.assertIn("Bachelor students", candidates[1].description)

    def test_workday_fetches_detail_only_for_candidate(self):
        listing = {
            "jobPostings": [{
                "title": "Quantitative Analytics Off-Cycle Analyst",
                "locationsText": "Singapore",
                "externalPath": "/job/Singapore/Role_JR-1",
                "postedOn": "Posted Yesterday",
            }],
            "total": 1,
        }
        detail = {"jobPostingInfo": {
            "jobDescription": "Open to Bachelor students",
            "externalUrl": "https://example.com/apply",
            "location": "Singapore",
        }}
        requester = MagicMock(side_effect=[response(listing), response(detail)])
        candidates, fetched = fetch_workday(source(
            "workday",
            host="https://example.wd.test",
            tenant="example",
            site="Careers",
        ), requester)
        self.assertEqual(fetched, 1)
        self.assertEqual(requester.call_count, 2)
        self.assertIn("Bachelor", candidates[0].description)

    def test_workday_detail_failure_keeps_listing_with_warning(self):
        listing = {
            "jobPostings": [{
                "title": "Software Engineer Intern",
                "locationsText": "Singapore",
                "externalPath": "/job/Singapore/Role_JR-1",
                "postedOn": "Posted Yesterday",
            }],
            "total": 1,
        }
        warnings = []
        requester = MagicMock(side_effect=[
            response(listing),
            RuntimeError("detail unavailable"),
        ])

        candidates, fetched = fetch_workday(
            source(
                "workday",
                host="https://example.wd.test",
                tenant="example",
                site="Careers",
            ),
            requester,
            warnings=warnings,
        )

        self.assertEqual(fetched, 1)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].description, "")
        self.assertIn("detail unavailable", warnings[0])

    def test_ashby_skips_unlisted_jobs(self):
        requester = MagicMock(return_value=response({"jobs": [
            {"id": "1", "title": "Software Intern", "isListed": False},
            {
                "id": "2",
                "title": "Software Engineer Intern",
                "isListed": True,
                "location": "Singapore",
                "jobUrl": "https://example.com/2",
            },
        ]}))
        candidates, fetched = fetch_ashby(
            source("ashby", board="test"), requester
        )
        self.assertEqual(fetched, 2)
        self.assertEqual([candidate.external_id for candidate in candidates], ["2"])


class BespokeAdapterTests(unittest.TestCase):
    def test_extracts_jobposting_json_ld(self):
        html = """
        <html><body>Careers
        <script type="application/ld+json">
        {
          "@type": "JobPosting",
          "title": "Quantitative Research Intern",
          "url": "https://example.com/job/1",
          "datePosted": "2026-07-16",
          "jobLocation": {"address": {
            "addressLocality": "Singapore",
            "addressCountry": "SG"
          }}
        }
        </script></body></html>
        """
        requester = MagicMock(return_value=response(text=html))
        candidates, fetched = fetch_bespoke(source(
            "bespoke",
            url="https://example.com/careers",
            page_marker="Careers",
        ), requester)
        self.assertEqual(fetched, 1)
        self.assertEqual(candidates[0].location, ["Singapore"])

    def test_missing_contract_marker_is_an_error(self):
        requester = MagicMock(return_value=response(text="redesigned page"))
        with self.assertRaisesRegex(ValueError, "page marker"):
            fetch_bespoke(source(
                "bespoke",
                url="https://example.com/careers",
                page_marker="Careers",
            ), requester)

    def test_fetches_configured_additional_pages(self):
        first = '<html>Results <a href="/job/one">AI Intern</a></html>'
        second = '<html>Results <a href="/job/two">Data Intern</a></html>'
        requester = MagicMock(side_effect=[
            response(text=first),
            response(text=second),
        ])
        candidates, fetched = fetch_bespoke(source(
            "bespoke",
            url="https://example.com/jobs",
            additional_urls=["https://example.com/jobs/10"],
            job_link_pattern="/job/",
            page_marker="Results",
            default_location="Singapore",
        ), requester)

        self.assertEqual(fetched, 2)
        self.assertEqual(len(candidates), 2)
        self.assertEqual(requester.call_count, 2)


if __name__ == "__main__":
    unittest.main()
