"""Run with TEST_DATABASE_URL pointing at a disposable PostgreSQL database."""
import os
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch
import psycopg2
from psycopg2.extras import Json, execute_values
from identity import backfill_identities, resolve_identity
import main

DSN = os.getenv('TEST_DATABASE_URL')


@unittest.skipUnless(DSN, 'TEST_DATABASE_URL is required for PostgreSQL integration tests')
class PostgresTests(unittest.TestCase):
    def connection(self):
        return psycopg2.connect(DSN, options='-c search_path=scraper_test,public')

    def setUp(self):
        with psycopg2.connect(DSN) as conn:
            with conn.cursor() as cur:
                cur.execute('DROP SCHEMA IF EXISTS scraper_test CASCADE')
                cur.execute('CREATE SCHEMA scraper_test')
        self.factory = patch('main.get_db_connection', side_effect=self.connection)
        self.factory.start()
        self.quiet = patch('main.QUIET_HOURS_END', 0)
        self.quiet.start()
        main.init_db()

    def tearDown(self):
        self.factory.stop()
        self.quiet.stop()

    def job(self, req='1', url='https://firm.example/job/1'):
        return dict(source_id='greenhouse_firm', external_id=req, company='Firm',
                    title='Software Intern', location='Singapore', job_url=url)

    def enqueue(self, identifier, job):
        with self.connection() as conn:
            return main.enqueue_job(conn, identifier, job, main.PipelineStats('test'))

    def test_concurrent_cross_source_copy_queues_once(self):
        barrier = threading.Barrier(2)
        def submit(index):
            barrier.wait()
            job = self.job()
            if index:
                job.update(source_id='jobspy', site='indeed', external_id='copy')
            return self.enqueue('obs-' + str(index), job)
        with ThreadPoolExecutor(2) as pool:
            self.assertEqual(sum(pool.map(submit, range(2))), 1)

    def test_distinct_requisitions_and_atomic_claims(self):
        self.assertTrue(self.enqueue('one', self.job()))
        self.assertTrue(self.enqueue('two', self.job('2', 'https://firm.example/job/2')))
        with self.connection() as a, self.connection() as b:
            one = main.claim_due_delivery(a)
            two = main.claim_due_delivery(b)
            self.assertNotEqual(one[0], two[0])
            self.assertIsNone(main.claim_due_delivery(a))

    def test_idempotent_migration_preserves_sent_history(self):
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute('INSERT INTO seen_jobs(job_id, company, title, site, payload) VALUES (%s,%s,%s,%s,%s)',
                            ('legacy', 'Firm', 'Software Intern', 'greenhouse_firm', Json(self.job())))
        main.init_db()
        main.init_db()
        self.assertFalse(self.enqueue('new-copy', self.job()))
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT delivery_status, canonical_id FROM seen_jobs WHERE job_id='legacy'")
                status, identity = cur.fetchone()
                self.assertEqual(status, 'sent')
                self.assertTrue(identity)

    def test_blocked_startup_times_out_and_closes_connection(self):
        blocked = self.connection()
        with blocked.cursor() as cur:
            cur.execute("SET lock_timeout = '200ms'")
        blocked.commit()
        try:
            with self.connection() as blocker:
                with blocker.cursor() as cur:
                    cur.execute('SELECT pg_advisory_xact_lock(%s)', (734819221,))
                with patch('main.get_db_connection', return_value=blocked), \
                        patch('main.DB_MIGRATION_LOCK_TIMEOUT_MS', 200):
                    with self.assertRaises(psycopg2.errors.LockNotAvailable):
                        main.init_db()
                self.assertTrue(blocked.closed)
            # A later run can initialize once the blocking transaction ends.
            main.init_db()
        finally:
            blocked.close()

    def test_failed_startup_rolls_back_schema_changes_and_closes_connection(self):
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DROP TABLE scraper_source_runs')
        failed = self.connection()
        with patch('main.get_db_connection', return_value=failed), \
                patch('main.backfill_identities', side_effect=RuntimeError('backfill failed')):
            with self.assertRaisesRegex(RuntimeError, 'backfill failed'):
                main.init_db()
        self.assertTrue(failed.closed)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('scraper_source_runs')")
                self.assertIsNone(cur.fetchone()[0])
        main.init_db()

    def test_batched_backfill_matches_live_identity_resolution(self):
        jobs = [
            self.job(),
            {**self.job(), 'source_id': 'jobspy', 'external_id': 'copy'},
            self.job('2', 'https://firm.example/job/2'),
            {**self.job('3', ''), 'location': 'London'},
            {**self.job('4', ''), 'location': 'London'},
            {'company': 'Other', 'title': 'Software Intern'},
            {'company': 'Other', 'title': 'Software Intern'},
            {**self.job(), 'title': 'Updated Software Intern'},
        ]
        with self.connection() as conn:
            with conn.cursor() as cur:
                expected = [resolve_identity(cur, job) for job in jobs]
                cur.execute('SELECT alias, canonical_id FROM job_identity_aliases ORDER BY alias')
                expected_aliases = cur.fetchall()
                cur.execute('DELETE FROM job_identity_aliases')
                cur.execute('DELETE FROM canonical_jobs')
                execute_values(cur, '''INSERT INTO seen_jobs(job_id,company,title,site,payload)
                    VALUES %s''', [(f'legacy-{i:02}', job['company'], job['title'],
                                    job.get('source_id'), Json(job)) for i, job in enumerate(jobs)])
                backfill_identities(cur)
                cur.execute('SELECT canonical_id FROM seen_jobs ORDER BY job_id')
                self.assertEqual([row[0] for row in cur.fetchall()], expected)
                cur.execute('SELECT alias, canonical_id FROM job_identity_aliases ORDER BY alias')
                self.assertEqual(cur.fetchall(), expected_aliases)
                # Existing identities and aliases are reused on a later migration.
                cur.execute("UPDATE seen_jobs SET canonical_id = NULL")
                backfill_identities(cur)
                cur.execute('SELECT canonical_id FROM seen_jobs ORDER BY job_id')
                self.assertEqual([row[0] for row in cur.fetchall()], expected)

    def test_backfill_uses_batches_for_thousands_of_history_rows(self):
        with self.connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, '''INSERT INTO seen_jobs(job_id,company,title,site,payload)
                    VALUES %s''', [(f'old-{i}', 'Firm', 'Software Intern', 'greenhouse_firm',
                                    Json(self.job(str(i), f'https://firm.example/job/{i}')))
                                   for i in range(3100)])
            with conn.cursor() as cur:
                counter = MagicMock(wraps=cur)
                # execute_values needs the real connection encoding for mogrify.
                counter.connection = conn
                backfill_identities(counter)
                # Four setup queries, plus batches for identities, two aliases per
                # job, and delivery-history updates. No per-job SQL round trips.
                self.assertLessEqual(counter.execute.call_count, 32)
                cur.execute('SELECT count(DISTINCT canonical_id) FROM seen_jobs')
                self.assertEqual(cur.fetchone()[0], 3100)

    def test_health_summary_and_run_persistence(self):
        from source_health import record_run, write_summary
        with self.connection() as conn:
            stats = main.PipelineStats('test', fetched=2, matched=1)
            record_run(conn, stats)
            report = write_summary([stats], conn)
            self.assertIn('Employer coverage', report)
            with conn.cursor() as cur:
                cur.execute('SELECT count(*) FROM scraper_source_runs')
                self.assertEqual(cur.fetchone()[0], 1)

    def test_eligibility_transition_alerts_once_and_does_not_close_presence(self):
        job = self.job()
        stats = main.PipelineStats('test')
        with self.connection() as conn:
            main.observe_job(conn, 'observed', 'greenhouse_firm', '1', {**job, 'alert_eligible': False}, stats)
            main.observe_job(conn, 'observed', 'greenhouse_firm', '1', {**job, 'alert_eligible': True}, stats)
            main.observe_job(conn, 'observed', 'greenhouse_firm', '1', {**job, 'alert_eligible': True}, stats)
            with conn.cursor() as cur:
                cur.execute('SELECT count(*) FROM seen_jobs')
                self.assertEqual(cur.fetchone()[0], 1)

    def test_legacy_source_runs_table_is_preserved(self):
        # Exact legacy column names, types and required fields observed in production.
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DROP TABLE scraper_source_runs')
                cur.execute("""CREATE TABLE source_runs (
                    id SERIAL PRIMARY KEY, source TEXT NOT NULL, started_at TEXT NOT NULL,
                    finished_at TEXT, status TEXT NOT NULL, result_count INTEGER DEFAULT 0,
                    error TEXT)""")
                cur.execute("""INSERT INTO source_runs(source,started_at,status,result_count)
                    VALUES ('legacy-source','2026-01-01T00:00:00','success',7)""")
        main.init_db()
        main.init_db()
        from source_health import record_run
        with self.connection() as conn:
            record_run(conn, main.PipelineStats('new-source', fetched=2, matched=1))
            with conn.cursor() as cur:
                cur.execute('SELECT source,started_at,status,result_count FROM source_runs')
                self.assertEqual(cur.fetchall(), [('legacy-source','2026-01-01T00:00:00','success',7)])
                cur.execute('SELECT source_id,fetched,matched FROM scraper_source_runs')
                self.assertEqual(cur.fetchall(), [('new-source',2,1)])

if __name__ == '__main__':
    unittest.main()
