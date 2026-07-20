import datetime
import unittest
from unittest.mock import MagicMock, patch

import main
from eligibility import assess_eligibility
from source_adapters import SourceFetchResult


def sample_job(**overrides):
    job = {
        "site": "greenhouse_test",
        "source_label": "Test Finance official careers",
        "company": "Test Finance",
        "title": "Software Engineer Intern",
        "job_url": "https://example.com/job/1",
        "location": "Singapore",
        "date_posted": "2026-07-16",
        "description": "Open to Bachelor students",
    }
    job.update(overrides)
    return job


class LifecycleTests(unittest.TestCase):
    def test_categories_change_the_content_fingerprint(self):
        job = sample_job(categories=["SWE"])

        self.assertNotEqual(
            main.make_content_fingerprint(job),
            main.make_content_fingerprint({**job, "categories": ["QUANT", "SWE"]}),
        )

    def setUp(self):
        self.conn = MagicMock()
        self.cursor = self.conn.cursor.return_value
        self.stats = main.PipelineStats("test")

    def test_new_observation_records_event_and_queues_atomically(self):
        self.cursor.fetchone.return_value = None
        with (
            patch("main.DRY_RUN", False),
            patch("main.enqueue_job", return_value=True) as enqueue,
        ):
            outcome = main.observe_job(
                self.conn,
                "greenhouse_test_1",
                "greenhouse_test",
                "1",
                sample_job(),
                self.stats,
            )

        self.assertEqual(outcome, main.LifecycleOutcome("new", True))
        self.assertEqual(self.stats.matched, 1)
        self.assertEqual(enqueue.call_args.args[1], "greenhouse_test_1")
        self.assertEqual(
            enqueue.call_args.args[2]["lifecycle_event"],
            "new",
        )
        self.assertFalse(enqueue.call_args.kwargs["commit"])
        self.conn.commit.assert_called_once()

    def test_material_change_records_update_without_alert(self):
        first_seen = datetime.datetime(2026, 7, 1, tzinfo=datetime.timezone.utc)
        self.cursor.fetchone.return_value = (
            "active",
            "old-fingerprint",
            1,
            {"title": "Old title"},
            first_seen,
        )
        with (
            patch("main.DRY_RUN", False),
            patch("main.enqueue_job") as enqueue,
        ):
            outcome = main.observe_job(
                self.conn,
                "greenhouse_test_1",
                "greenhouse_test",
                "1",
                sample_job(title="Data Engineering Intern"),
                self.stats,
            )

        self.assertEqual(outcome.event_type, "updated")
        enqueue.assert_not_called()
        event_sql = "\n".join(
            call.args[0] for call in self.cursor.execute.call_args_list
        )
        self.assertIn("INSERT INTO job_lifecycle_events", event_sql)

    def test_backfilled_observation_establishes_baseline_silently(self):
        first_seen = datetime.datetime(2026, 7, 1, tzinfo=datetime.timezone.utc)
        self.cursor.fetchone.return_value = (
            "active",
            None,
            0,
            {"title": "Software Engineer Intern"},
            first_seen,
        )
        with (
            patch("main.DRY_RUN", False),
            patch("main.enqueue_job") as enqueue,
        ):
            outcome = main.observe_job(
                self.conn,
                "greenhouse_test_1",
                "greenhouse_test",
                "1",
                sample_job(),
                self.stats,
            )

        self.assertEqual(outcome.event_type, "unchanged")
        enqueue.assert_not_called()
        event_inserts = [
            call for call in self.cursor.execute.call_args_list
            if "INSERT INTO job_lifecycle_events" in call.args[0]
        ]
        self.assertEqual(event_inserts, [])

    def test_reopened_observation_uses_versioned_delivery_id(self):
        first_seen = datetime.datetime(2026, 7, 1, tzinfo=datetime.timezone.utc)
        self.cursor.fetchone.return_value = (
            "closed",
            "old-fingerprint",
            2,
            {"title": "Software Engineer Intern"},
            first_seen,
        )
        with (
            patch("main.DRY_RUN", False),
            patch("main.enqueue_job", return_value=True) as enqueue,
        ):
            outcome = main.observe_job(
                self.conn,
                "greenhouse_test_1",
                "greenhouse_test",
                "1",
                sample_job(),
                self.stats,
            )

        self.assertEqual(outcome.event_type, "reopened")
        self.assertEqual(
            enqueue.call_args.args[1],
            "greenhouse_test_1:reopened:3",
        )
        self.assertTrue(enqueue.call_args.kwargs["bypass_recent_dedupe"])
        self.assertEqual(
            enqueue.call_args.args[2]["first_seen_at"],
            first_seen.isoformat(),
        )

    def test_third_successful_snapshot_miss_closes_job_without_alert(self):
        self.cursor.fetchall.return_value = [(
            "greenhouse_test_1",
            2,
            4,
            {"title": "Software Engineer Intern"},
        )]

        closed = main.reconcile_source_snapshot(
            self.conn,
            "greenhouse_test",
            set(),
        )

        self.assertEqual(closed, ["greenhouse_test_1"])
        sql = "\n".join(call.args[0] for call in self.cursor.execute.call_args_list)
        self.assertIn("lifecycle_status = 'closed'", sql)
        self.assertIn("INSERT INTO job_lifecycle_events", sql)
        self.conn.commit.assert_called_once()

    def test_earlier_snapshot_miss_only_increments_counter(self):
        self.cursor.fetchall.return_value = [(
            "greenhouse_test_1",
            0,
            1,
            {"title": "Software Engineer Intern"},
        )]

        closed = main.reconcile_source_snapshot(
            self.conn,
            "greenhouse_test",
            {"greenhouse_test_2"},
        )

        self.assertEqual(closed, [])
        sql = "\n".join(call.args[0] for call in self.cursor.execute.call_args_list)
        self.assertIn("missing_snapshot_count = %s", sql)
        self.assertNotIn("lifecycle_status = 'closed'", sql)

    def test_ineligible_observation_is_not_stored_or_queued(self):
        assessment = assess_eligibility(
            "Quantitative Research Intern",
            "PhD candidates only.",
        )
        with patch("main.enqueue_job") as enqueue:
            outcome = main.observe_job(
                self.conn,
                "greenhouse_test_1",
                "greenhouse_test",
                "1",
                sample_job(),
                self.stats,
                assessment,
            )

        self.assertEqual(outcome.event_type, "excluded")
        self.conn.cursor.assert_not_called()
        enqueue.assert_not_called()

    def test_failed_snapshot_does_not_reconcile_missing_jobs(self):
        source = {
            "id": "greenhouse_test",
            "company": "Test Finance",
            "adapter": "greenhouse",
            "enabled": True,
            "lifecycle_mode": "snapshot",
            "config": {"token": "test"},
        }
        with (
            patch("main.DRY_RUN", False),
            patch("main.load_source_registry", return_value=[source]),
            patch(
                "main.fetch_source",
                return_value=SourceFetchResult(
                    "greenhouse_test",
                    error="source unavailable",
                ),
            ),
            patch("main.get_db_connection", return_value=self.conn),
            patch("main.reconcile_source_snapshot") as reconcile,
        ):
            stats = main.scrape_registry_pipelines()

        self.assertEqual(len(stats[0].errors), 1)
        reconcile.assert_not_called()

    def test_successful_snapshot_reconciles_observed_ids(self):
        source = {
            "id": "greenhouse_test",
            "company": "Test Finance",
            "adapter": "greenhouse",
            "enabled": True,
            "lifecycle_mode": "snapshot",
            "config": {"token": "test"},
        }
        with (
            patch("main.DRY_RUN", False),
            patch("main.load_source_registry", return_value=[source]),
            patch(
                "main.fetch_source",
                return_value=SourceFetchResult("greenhouse_test"),
            ),
            patch("main.get_db_connection", return_value=self.conn),
            patch(
                "main.reconcile_source_snapshot",
                return_value=[],
            ) as reconcile,
        ):
            stats = main.scrape_registry_pipelines()

        self.assertEqual(stats[0].errors, [])
        reconcile.assert_called_once_with(
            self.conn,
            "greenhouse_test",
            set(),
        )


class MigrationTests(unittest.TestCase):
    def test_init_db_creates_lifecycle_tables_and_silent_backfill(self):
        conn = MagicMock()
        with patch("main.get_db_connection", return_value=conn):
            main.init_db()

        sql = "\n".join(
            call.args[0] for call in conn.cursor.return_value.execute.call_args_list
        )
        self.assertIn("CREATE TABLE IF NOT EXISTS job_observations", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS job_lifecycle_events", sql)
        self.assertIn("delivery_mode TEXT NOT NULL DEFAULT 'immediate'", sql)
        self.assertIn("INSERT INTO job_observations", sql)
        self.assertIn("'backfilled'", sql)
        self.assertIn("system_canary_%", sql)
        self.assertIn("lifecycle_event", sql)
        conn.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
