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
    def test_quiet_hour_boundaries_use_singapore_time(self):
        singapore = main.DELIVERY_TIMEZONE
        before_end = datetime.datetime(2026, 7, 20, 7, 59, tzinfo=singapore)
        at_end = datetime.datetime(2026, 7, 20, 8, 0, tzinfo=singapore)
        midnight = datetime.datetime(2026, 7, 20, 0, 0, tzinfo=singapore)
        before_start = datetime.datetime(2026, 7, 19, 23, 59, tzinfo=singapore)

        self.assertTrue(main.is_quiet_hours(before_end))
        self.assertFalse(main.is_quiet_hours(at_end))
        self.assertTrue(main.is_quiet_hours(midnight))
        self.assertFalse(main.is_quiet_hours(before_start))
        mode, due_at = main.delivery_policy_for_time(before_end)
        self.assertEqual(mode, "digest")
        self.assertEqual(due_at.astimezone(singapore).hour, 8)

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
        self.assertEqual(
            payload["link_preview_options"],
            {"is_disabled": True},
        )

    def test_telegram_reopened_card_shows_detailed_eligibility(self):
        message = main.build_telegram_message({
            "lifecycle_event": "reopened",
            "company": "Example Capital",
            "title": "Quant Developer Intern",
            "job_url": "https://example.com/apply?a=1&b=2",
            "location": "Singapore",
            "date_posted": "2026-07-16T10:00:00+00:00",
            "first_seen_at": "2026-07-01T10:00:00+00:00",
            "source_label": "Example Capital official careers",
            "eligibility": {
                "verdict": "likely_eligible",
                "degree_levels": ["Bachelor's", "Master's"],
                "graduation_years": [2028],
                "duration": "6 months",
                "work_authorization": "Sponsorship not available",
            },
        })

        self.assertIn("REOPENED INTERNSHIP", message)
        self.assertIn("Likely undergrad eligible", message)
        self.assertIn("Bachelor&#x27;s, Master&#x27;s", message)
        self.assertIn("2028", message)
        self.assertIn("Sponsorship not available", message)
        self.assertIn("01 Jul 2026", message)
        self.assertIn("a=1&amp;b=2", message)

    def test_telegram_card_omits_unavailable_optional_fields(self):
        message = main.build_telegram_message({
            "company": "Example",
            "title": "Software Engineer Intern",
            "location": "Singapore",
            "job_url": "https://example.com",
            "eligibility": {"verdict": "unknown"},
        })

        self.assertIn("Requirements unclear", message)
        self.assertNotIn("<b>Degree:</b>", message)
        self.assertNotIn("<b>Graduation:</b>", message)

    def test_overseas_quant_card_does_not_imply_visa_or_relocation(self):
        message = main.build_telegram_message({
            "company": "Example Quant",
            "title": "Quantitative Developer Intern",
            "location": "London",
            "job_url": "https://example.com",
            "is_overseas_quant": True,
            "eligibility": {"verdict": "unknown"},
        })

        self.assertIn("Work rights:</b> Not stated; verify posting", message)
        self.assertIn("Relocation:</b> Not stated; verify posting", message)

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
        dedupe_sql = cursor.execute.call_args_list[0].args[0]
        self.assertIn("payload->>'job_url'", dedupe_sql)
        insert_sql = cursor.execute.call_args_list[1].args[0]
        self.assertIn("ON CONFLICT (job_id) DO NOTHING", insert_sql)
        conn.commit.assert_called_once()

    def test_overseas_quant_dedupe_keeps_distinct_offices(self):
        london = {
            "company": "Example Quant",
            "title": "Software Engineer Intern",
            "location": "London",
            "is_overseas_quant": True,
        }
        new_york = {**london, "location": "New York"}

        self.assertNotEqual(
            main.make_dedupe_key(london),
            main.make_dedupe_key(new_york),
        )

    def test_enqueue_during_quiet_hours_persists_digest_mode_and_due_time(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value
        cursor.fetchone.side_effect = [None, ("source_1",)]
        stats = main.PipelineStats("test")
        singapore = main.DELIVERY_TIMEZONE
        now = datetime.datetime(2026, 7, 20, 1, 30, tzinfo=singapore)

        with patch("main.DRY_RUN", False):
            main.enqueue_job(
                conn,
                "source_1",
                {"company": "Example", "title": "Software Intern"},
                stats,
                now=now,
            )

        params = cursor.execute.call_args_list[1].args[1]
        self.assertEqual(params[-1], "digest")
        self.assertEqual(params[-2].astimezone(singapore).hour, 8)

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
        self.assertIn("delivery_mode = 'immediate'", cursor.execute.call_args.args[0])
        conn.commit.assert_called_once()

    def test_digest_claim_is_bounded_and_isolated(self):
        conn = MagicMock()
        conn.cursor.return_value.fetchall.return_value = []

        main.claim_due_digest(conn, limit=8)

        sql, params = conn.cursor.return_value.execute.call_args.args
        self.assertIn("delivery_mode = 'digest'", sql)
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertEqual(params[-1], 8)

    def test_digest_message_groups_once_and_shows_all_tags(self):
        message = main.build_digest_message([
            {
                "company": "Quant & Co",
                "title": "Quant Software Engineer Intern",
                "location": "Singapore",
                "categories": ["QUANT", "SWE"],
                "job_url": "https://example.com/quant?a=1&b=2",
            },
            {
                "company": "Data Co",
                "title": "Data Engineer Intern",
                "location": "Singapore",
                "categories": ["DATA"],
                "job_url": "https://example.com/data",
            },
        ])

        self.assertLess(message.index("<b>QUANT</b>"), message.index("<b>DATA</b>"))
        self.assertEqual(message.count("Quant &amp; Co"), 1)
        self.assertIn("[QUANT] [SWE]", message)
        self.assertIn("a=1&amp;b=2", message)

    def test_eight_job_digest_stays_below_telegram_safety_limit(self):
        jobs = [{
            "company": "Company " + "x" * 200,
            "title": "Software Engineer Intern " + "y" * 400,
            "location": "Singapore " + "z" * 200,
            "categories": ["SWE"],
            "job_url": f"https://example.com/{index}",
        } for index in range(8)]

        message = main.build_digest_message(jobs)

        self.assertLessEqual(
            len(main.BeautifulSoup(message, "html.parser").get_text()),
            3500,
        )

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

    def test_digest_delivery_marks_the_whole_batch_sent(self):
        conn = MagicMock()
        created_at = datetime.datetime(2026, 7, 20)
        rows = [
            ("job_1", {"title": "Software Intern"}, 1, created_at),
            ("job_2", {"title": "Data Intern"}, 1, created_at),
        ]
        with (
            patch("main.DRY_RUN", False),
            patch("main.get_db_connection", return_value=conn),
            patch("main.claim_due_digest", side_effect=[rows, []]),
            patch("main.claim_due_delivery", return_value=None),
            patch(
                "main.send_telegram_message",
                return_value=main.DeliveryResult(True),
            ),
            patch("main.complete_deliveries") as complete,
        ):
            failures = main.deliver_pending_jobs()

        self.assertEqual(failures, 0)
        complete.assert_called_once_with(conn, ["job_1", "job_2"])

    def test_digest_failure_requeues_every_row(self):
        conn = MagicMock()
        created_at = datetime.datetime(2026, 7, 20)
        rows = [
            ("job_1", {"title": "Software Intern"}, 1, created_at),
            ("job_2", {"title": "Data Intern"}, 2, created_at),
        ]
        with (
            patch("main.DRY_RUN", False),
            patch("main.get_db_connection", return_value=conn),
            patch("main.claim_due_digest", side_effect=[rows, []]),
            patch("main.claim_due_delivery", return_value=None),
            patch(
                "main.send_telegram_message",
                return_value=main.DeliveryResult(False, "failed"),
            ),
            patch("main.fail_delivery", return_value="failed") as fail,
        ):
            failures = main.deliver_pending_jobs()

        self.assertEqual(failures, 2)
        self.assertEqual(fail.call_count, 2)

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
        self.assertEqual(enqueue.call_args.kwargs["delivery_mode"], "immediate")
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
    def test_jobspy_merges_bunge_duplicates_and_prefers_singapore_direct_url(self):
        linkedin = pd.DataFrame([{
            "id": "li-1",
            "site": "linkedin",
            "title": "Quantitative Trading Developer (Internship)",
            "company": "Bunge",
            "location": "",
            "job_url": "https://linkedin.example/job/1",
            "job_url_direct": None,
        }])
        indeed = pd.DataFrame([{
            "id": "in-1",
            "site": "indeed",
            "title": "Quantitative Trading Developer (Internship)",
            "company": "Bunge",
            "location": "SG",
            "job_url": "https://indeed.example/job/1",
            "job_url_direct": "https://jobs.bunge.com/job/47243?utm_source=indeed",
        }])

        jobs = main.merge_jobspy_results([linkedin, indeed])

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["location"], "SG")
        self.assertEqual(jobs[0]["job_url"], "https://jobs.bunge.com/job/47243")
        self.assertEqual(jobs[0]["discovery_sites"], ["indeed", "linkedin"])

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

    def test_jobspy_accepts_missing_location_as_explicitly_inferred(self):
        jobs = pd.DataFrame([{
            "id": "1",
            "site": "linkedin",
            "title": "Software Engineer Intern",
            "company": "Example",
            "location": None,
            "job_url": "https://example.com/job",
        }])
        with (
            patch("main.DRY_RUN", True),
            patch("main.scrape_jobs", return_value=jobs),
        ):
            stats = main.run_pipeline()

        self.assertEqual(stats.queued, 1)
        self.assertEqual(stats.inferred_location, 1)

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
        detail = """
        <script type="application/ld+json">
        {"@type":"JobPosting","description":"Open to Bachelor students"}
        </script>
        """
        responses = [
            MagicMock(text=first_page),
            MagicMock(text=detail),
            MagicMock(text=second_page),
            MagicMock(text=detail),
        ]
        with (
            patch("main.DRY_RUN", True),
            patch("main.http_get", side_effect=responses) as get,
        ):
            stats = main.scrape_internsg_pipeline()

        self.assertEqual(get.call_count, 4)
        self.assertEqual(stats.fetched, 2)
        self.assertEqual(stats.queued, 2)

    def test_internsg_extracts_nested_jobposting_description(self):
        page = """
        <script type="application/ld+json">
        {"@graph":[{"@type":"WebPage"},{"@type":"JobPosting",
        "description":"<p>Open to undergraduate students.</p>"}]}
        </script>
        """

        description = main.extract_internsg_description(page)

        self.assertIn("undergraduate students", description)


if __name__ == "__main__":
    unittest.main()
