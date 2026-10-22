"""Run with TEST_DATABASE_URL pointing at a disposable PostgreSQL database."""
import os
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
import psycopg2
from psycopg2.extras import Json
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
