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

if __name__ == '__main__':
    unittest.main()
