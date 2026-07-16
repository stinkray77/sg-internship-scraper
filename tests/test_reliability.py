import datetime
import unittest
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd

import main


def response_with_json(value, status_code=200):
    response = MagicMock()
    response.status_code = status_code
    response.text = "response"
    response.json.return_value = value
    return response


class DeliveryTests(unittest.TestCase):
    def test_telegram_success_uses_escaped_html(self):
        response = response_with_json({}, status_code=200)
        with (
            patch("main.os.getenv", side_effect=lambda name: "value"),
            patch("main.requests.post", return_value=response) as post,
        ):
            result = main.send_telegram_alert({
                "site": "A&B",
                "company": "One < Two",
                "title": "C++ & Python",
                "job_url": "https://example.com/?a=1&b=2",
            })

        self.assertTrue(result.success)
        payload = post.call_args.kwargs["json"]
        self.assertEqual(payload["parse_mode"], "HTML")
        self.assertIn("A&amp;B", payload["text"])
        self.assertIn("One &lt; Two", payload["text"])
        self.assertIn("C++ &amp; Python", payload["text"])
        self.assertEqual(post.call_args.kwargs["timeout"], 15)

    def test_telegram_timeout_is_a_failed_result(self):
        with (
            patch("main.os.getenv", side_effect=lambda name: "value"),
            patch("main.requests.post", side_effect=main.requests.Timeout("late")),
        ):
            result = main.send_telegram_alert({})

        self.assertFalse(result.success)
        self.assertIn("late", result.error)

    def test_telegram_rate_limit_returns_server_retry_delay(self):
        response = response_with_json(
            {"parameters": {"retry_after": 23}},
            status_code=429,
        )
        response.text = "Too Many Requests"
        with (
            patch("main.os.getenv", side_effect=lambda name: "value"),
            patch("main.requests.post", return_value=response),
        ):
            result = main.send_telegram_alert({})

        self.assertFalse(result.success)
        self.assertEqual(result.retry_after_seconds, 23)

    def test_enqueue_uses_atomic_insert(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value
        cursor.fetchone.side_effect = [None, ("source_1",)]
        stats = main.PipelineStats("test")

        with patch("main.DRY_RUN", False):
            queued = main.enqueue_job(
                conn,
                "source_1",
                {
                    "site": "Test",
                    "company": "Company",
                    "title": "Software Engineer Intern",
                    "job_url": "https://example.com/job",
                    "location": "Singapore",
                },
                stats,
            )

        self.assertTrue(queued)
        self.assertEqual(stats.queued, 1)
        insert_sql = cursor.execute.call_args_list[1].args[0]
        self.assertIn("ON CONFLICT (job_id) DO NOTHING", insert_sql)
        conn.commit.assert_called_once()

    def test_enqueue_normalizes_pandas_values_for_json(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value
        cursor.fetchone.side_effect = [None, ("source_1",)]
        stats = main.PipelineStats("test")

        with patch("main.DRY_RUN", False):
            main.enqueue_job(
                conn,
                "source_1",
                {
                    "site": "Test",
                    "company": float("nan"),
                    "title": "Software Engineer Intern",
                    "date_posted": pd.Timestamp("2026-07-16"),
                },
                stats,
            )

        params = cursor.execute.call_args_list[1].args[1]
        self.assertEqual(params[1], "Unknown Company")
        self.assertEqual(params[4].adapted["date_posted"], "2026-07-16T00:00:00")

    def test_claim_uses_skip_locked_and_commits(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value
        cursor.fetchone.return_value = ("job_1", {"title": "Role"}, 1)

        claimed = main.claim_due_delivery(conn)

        self.assertEqual(claimed[0], "job_1")
        self.assertIn("FOR UPDATE SKIP LOCKED", cursor.execute.call_args.args[0])
        conn.commit.assert_called_once()

    def test_claim_can_be_restricted_to_one_job(self):
        conn = MagicMock()
        main.claim_due_delivery(conn, only_job_id="canary_1")

        params = conn.cursor.return_value.execute.call_args.args[1]
        self.assertEqual(params[1:3], ("canary_1", "canary_1"))

    def test_failure_becomes_dead_at_max_attempts(self):
        conn = MagicMock()

        status = main.fail_delivery(
            conn,
            "job_1",
            main.MAX_DELIVERY_ATTEMPTS,
            "failed",
        )

        self.assertEqual(status, "dead")
        params = conn.cursor.return_value.execute.call_args.args[1]
        self.assertEqual(params[0], "dead")
        self.assertTrue(params[1])

    def test_delivery_loop_persists_success(self):
        conn = MagicMock()
        payload = {"title": "Software Engineer Intern"}
        with (
            patch("main.DRY_RUN", False),
            patch("main.get_db_connection", return_value=conn),
            patch(
                "main.claim_due_delivery",
                side_effect=[("job_1", payload, 1), None],
            ),
            patch(
                "main.send_telegram_alert",
                return_value=main.DeliveryResult(True),
            ),
            patch("main.complete_delivery") as complete,
        ):
            failures = main.deliver_pending_jobs()

        self.assertEqual(failures, 0)
        complete.assert_called_once_with(conn, "job_1")
        conn.close.assert_called_once()

    def test_delivery_loop_waits_and_retries_rate_limited_row(self):
        conn = MagicMock()
        payload = {"title": "Software Engineer Intern"}
        with (
            patch("main.DRY_RUN", False),
            patch("main.get_db_connection", return_value=conn),
            patch(
                "main.claim_due_delivery",
                side_effect=[("job_1", payload, 1), None],
            ),
            patch(
                "main.send_telegram_alert",
                side_effect=[
                    main.DeliveryResult(False, "HTTP 429", 23),
                    main.DeliveryResult(True),
                ],
            ) as send,
            patch("main.time.sleep") as sleep,
            patch("main.complete_delivery") as complete,
            patch("main.fail_delivery") as fail,
        ):
            failures = main.deliver_pending_jobs()

        self.assertEqual(failures, 0)
        self.assertEqual(send.call_count, 2)
        sleep.assert_called_once_with(24)
        complete.assert_called_once_with(conn, "job_1")
        fail.assert_not_called()

    def test_canary_delivers_only_its_queue_row(self):
        enqueue_conn = MagicMock()
        status_conn = MagicMock()
        status_conn.cursor.return_value.fetchone.return_value = ("sent",)
        with (
            patch("main.get_db_connection", side_effect=[enqueue_conn, status_conn]),
            patch("main.enqueue_job", return_value=True) as enqueue,
            patch("main.deliver_pending_jobs", return_value=0) as deliver,
        ):
            exit_code = main.run_canary()

        self.assertEqual(exit_code, 0)
        canary_id = enqueue.call_args.args[1]
        self.assertTrue(canary_id.startswith("system_canary_"))
        self.assertEqual(
            enqueue.call_args.args[2]["message_type"],
            "canary",
        )
        deliver.assert_called_once_with(only_job_id=canary_id)

    def test_main_canary_skips_scrapers(self):
        with (
            patch("main.DB_URL", "database"),
            patch("main.BOT_TOKEN", "token"),
            patch("main.CHAT_ID", "chat"),
            patch("main.init_db") as init_db,
            patch("main.run_canary", return_value=0) as canary,
            patch("main.run_pipeline") as broad,
        ):
            exit_code = main.main(["--canary"])

        self.assertEqual(exit_code, 0)
        init_db.assert_called_once()
        canary.assert_called_once()
        broad.assert_not_called()

    def test_official_sources_run_before_aggregators(self):
        calls = []
        with (
            patch(
                "main.scrape_registry_pipelines",
                side_effect=lambda: calls.append("registry") or [],
            ),
            patch(
                "main.scrape_singapore_quant_pipeline",
                side_effect=lambda: calls.append("quant") or main.PipelineStats("quant"),
            ),
            patch(
                "main.scrape_internsg_pipeline",
                side_effect=lambda: calls.append("internsg") or main.PipelineStats("internsg"),
            ),
            patch(
                "main.run_pipeline",
                side_effect=lambda: calls.append("jobspy") or main.PipelineStats("jobspy"),
            ),
        ):
            exit_code = main.main(["--dry-run"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(calls, ["registry", "quant", "internsg", "jobspy"])

    def test_dry_run_skips_database_and_delivery(self):
        empty_stats = lambda name: main.PipelineStats(name)
        pipeline_names = [
            "run_pipeline",
            "scrape_internsg_pipeline",
            "scrape_registry_pipelines",
            "scrape_singapore_quant_pipeline",
        ]
        with ExitStack() as stack:
            for name in pipeline_names:
                result = [] if name == "scrape_registry_pipelines" else empty_stats(name)
                stack.enter_context(
                    patch(f"main.{name}", return_value=result)
                )
            init_db = stack.enter_context(patch("main.init_db"))
            deliver = stack.enter_context(patch("main.deliver_pending_jobs"))
            exit_code = main.main(["--dry-run"])

        self.assertEqual(exit_code, 0)
        init_db.assert_not_called()
        deliver.assert_not_called()


class PaginationTests(unittest.TestCase):
    def test_jobspy_uses_overlap_and_result_limit(self):
        jobs = pd.DataFrame([{
            "id": "1",
            "site": "indeed",
            "title": "Software Engineer Intern",
            "company": "Example",
            "location": "Singapore, SG",
            "job_url": "https://example.com/job",
        }])
        with (
            patch("main.DRY_RUN", True),
            patch("main.scrape_jobs", return_value=jobs) as scrape,
        ):
            stats = main.run_pipeline()

        self.assertEqual(scrape.call_args.kwargs["hours_old"], 72)
        self.assertEqual(scrape.call_args.kwargs["results_wanted"], 50)
        self.assertEqual(stats.queued, 1)

    def test_internsg_follows_next_page(self):
        date_text = datetime.date.today().strftime("%d %b")
        first_page = f"""
        <div class="ast-row">
          <div class="ast-col-lg-3">First Company</div>
          <div class="ast-col-lg-3"><a href="/job/first-role/">Software Engineer Intern</a></div>
          <div class="ast-col-lg-2">Singapore, SG</div>
          <span class="badge-success">{date_text}</span>
        </div>
        <a class="next page-numbers" href="/jobs/2">Next</a>
        """
        second_page = f"""
        <div class="ast-row">
          <div class="ast-col-lg-3">Second Company</div>
          <div class="ast-col-lg-3"><a href="/job/second-role/">Data Engineering Intern</a></div>
          <div class="ast-col-lg-2">Singapore, SG</div>
          <span class="badge-success">{date_text}</span>
        </div>
        """
        responses = [MagicMock(text=first_page), MagicMock(text=second_page)]
        with (
            patch("main.DRY_RUN", True),
            patch("main.http_get", side_effect=responses) as get,
        ):
            stats = main.scrape_internsg_pipeline()

        self.assertEqual(get.call_count, 2)
        self.assertEqual(stats.fetched, 2)
        self.assertEqual(stats.queued, 2)


if __name__ == "__main__":
    unittest.main()
