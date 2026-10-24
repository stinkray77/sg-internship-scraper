from contextlib import ExitStack, nullcontext, redirect_stdout
import io
import os
import unittest
from unittest.mock import MagicMock, patch

import psycopg2

import main
from coordination import advisory_lock, CoordinationError, DELIVERY_LOCK


DIRECT_URL = 'postgresql://user:password@direct.example/database'


class AdvisoryLockTests(unittest.TestCase):
    def test_invalid_configuration_never_connects_or_exposes_credentials(self):
        for dsn in (None, '', 'not a dsn password=secret',
                    'postgresql://user:secret@ep-test-pooler.aws.neon.tech/db'):
            with self.subTest(dsn=bool(dsn)), patch('coordination.psycopg2.connect') as connect:
                with self.assertRaises(CoordinationError) as error:
                    with advisory_lock(dsn, DELIVERY_LOCK):
                        self.fail('Invalid configuration acquired a lock')
                self.assertNotIn('secret', str(error.exception))
                connect.assert_not_called()

    def test_lock_is_explicitly_released_on_the_same_direct_connection(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.side_effect = [(True,), (True,)]
        with patch('coordination.psycopg2.connect', return_value=conn) as connect:
            with advisory_lock(DIRECT_URL, DELIVERY_LOCK) as acquired:
                self.assertTrue(acquired)
                self.assertTrue(conn.autocommit)
                conn.close.assert_not_called()
        self.assertEqual(connect.call_args.args, (DIRECT_URL,))
        self.assertEqual(connect.call_args.kwargs['connect_timeout'], 10)
        queries = [c.args[0] for c in cur.execute.call_args_list]
        self.assertIn('pg_try_advisory_lock', queries[1])
        self.assertIn('pg_advisory_unlock', queries[2])
        conn.close.assert_called_once()

    def test_contention_closes_connection_without_unlocking_someone_else(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = (False,)
        with patch('coordination.psycopg2.connect', return_value=conn):
            with advisory_lock(DIRECT_URL, DELIVERY_LOCK) as acquired:
                self.assertFalse(acquired)
        self.assertEqual(cur.execute.call_count, 2)
        conn.close.assert_called_once()

    def test_release_failure_is_reported_and_connection_is_closed(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.side_effect = [(True,), (False,)]
        with patch('coordination.psycopg2.connect', return_value=conn):
            with self.assertRaises(CoordinationError):
                with advisory_lock(DIRECT_URL, DELIVERY_LOCK):
                    pass
        conn.close.assert_called_once()

    def test_cleanup_preserves_the_original_work_failure(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.side_effect = [(True,), psycopg2.OperationalError('secret')]
        with patch('coordination.psycopg2.connect', return_value=conn):
            with self.assertLogs(level='WARNING') as logs:
                with self.assertRaisesRegex(ValueError, 'work failed'):
                    with advisory_lock(DIRECT_URL, DELIVERY_LOCK):
                        raise ValueError('work failed')
        self.assertNotIn('secret', ''.join(logs.output))
        conn.close.assert_called_once()

    def test_connection_failure_is_sanitized_without_retry(self):
        with patch('coordination.psycopg2.connect', side_effect=psycopg2.OperationalError('password=secret')) as connect:
            with self.assertRaises(CoordinationError) as error:
                with advisory_lock(DIRECT_URL, DELIVERY_LOCK):
                    pass
        self.assertNotIn('secret', str(error.exception))
        self.assertEqual(connect.call_count, 1)

    def test_acquisition_query_failure_closes_the_direct_connection(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.execute.side_effect = [None, psycopg2.OperationalError('password=secret')]
        with patch('coordination.psycopg2.connect', return_value=conn):
            with self.assertRaises(CoordinationError) as error:
                with advisory_lock(DIRECT_URL, DELIVERY_LOCK):
                    self.fail('A failed acquisition entered protected work')
        self.assertNotIn('secret', str(error.exception))
        conn.close.assert_called_once()


class CoordinationRoutingTests(unittest.TestCase):
    def test_delivery_contention_does_not_open_data_connection_or_send(self):
        with patch('main.DRY_RUN', False), \
                patch('main.advisory_lock', return_value=nullcontext(False)), \
                patch('main.get_db_connection') as data, \
                patch('main.send_telegram_alert') as send, \
                patch('main.claim_due_delivery') as claim:
            self.assertEqual(main.deliver_pending_jobs(), 0)
        data.assert_not_called()
        claim.assert_not_called()
        send.assert_not_called()

    def test_isolated_canary_keeps_restricted_delivery_without_global_lock(self):
        with patch('main.DRY_RUN', False), patch('main.advisory_lock') as lock, \
                patch('main._deliver_due_jobs', return_value=0) as deliver:
            self.assertEqual(main.deliver_pending_jobs('system_canary_test'), 0)
        lock.assert_not_called()
        deliver.assert_called_once_with('system_canary_test')

    def test_missing_direct_url_fails_before_initialization_or_sending(self):
        for group in ('official', 'discovery', 'all'):
            with self.subTest(group=group), patch('main.DRY_RUN', False), \
                    patch('main.DB_URL', 'data'), patch('main.BOT_TOKEN', 'token'), \
                    patch('main.CHAT_ID', 'chat'), patch('main.UNPOOLED_DB_URL', None), \
                    patch('main.init_db') as initialize, patch('main.DeliveryWorker') as worker:
                self.assertEqual(main.main(['--group', group]), 1)
                initialize.assert_not_called()
                worker.assert_not_called()

    def test_pipeline_contention_skips_scraper_and_is_successful(self):
        calls = []
        def pipeline():
            calls.append('scraped')
            return []
        worker = MagicMock(failures=0, errors=[])
        with ExitStack() as stack:
            for name, value in (('DB_URL', 'data'), ('BOT_TOKEN', 'token'),
                                ('CHAT_ID', 'chat'), ('UNPOOLED_DB_URL', DIRECT_URL), ('DRY_RUN', False)):
                stack.enter_context(patch('main.' + name, value))
            stack.enter_context(patch('main.init_db'))
            stack.enter_context(patch('main.DeliveryWorker', return_value=worker))
            stack.enter_context(patch('main.scrape_registry_pipelines', pipeline))
            stack.enter_context(patch('main.get_db_connection'))
            stack.enter_context(patch('main.write_summary'))
            lock = stack.enter_context(patch('main.advisory_lock', return_value=nullcontext(False)))
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(main.main(['--group', 'official']), 0)
        self.assertEqual(calls, [])
        self.assertIn('pipeline', lock.call_args.args[1])

    def test_dry_run_never_opens_a_lock_connection(self):
        with patch('main.DRY_RUN', True), patch('main.advisory_lock') as lock:
            self.assertEqual(main.deliver_pending_jobs(), 0)
        lock.assert_not_called()

    def test_maintenance_does_not_require_a_direct_url(self):
        with ExitStack() as stack:
            for name, value in (('DB_URL', 'data'), ('BOT_TOKEN', 'token'), ('CHAT_ID', 'chat'),
                                ('UNPOOLED_DB_URL', None), ('DRY_RUN', False)):
                stack.enter_context(patch('main.' + name, value))
            stack.enter_context(patch('main.init_db'))
            stack.enter_context(patch('main.get_db_connection'))
            stack.enter_context(patch('main.run_maintenance', return_value=[]))
            stack.enter_context(patch('main.write_summary'))
            lock = stack.enter_context(patch('main.advisory_lock'))
            with redirect_stdout(io.StringIO()):
                self.assertEqual(main.main(['--group', 'maintenance']), 0)
        lock.assert_not_called()

    def test_jobspy_child_does_not_inherit_the_direct_database_secret(self):
        import jobspy_fetch
        with patch.dict(os.environ, {'DATABASE_URL_UNPOOLED': DIRECT_URL}), \
                patch('jobspy_fetch.subprocess.run', return_value=MagicMock(stderr='', returncode=0)) as run, \
                patch('pathlib.Path.read_text', return_value='[]'):
            jobspy_fetch.fetch_jobspy_shard()
        self.assertNotIn('DATABASE_URL_UNPOOLED', run.call_args.kwargs['env'])
