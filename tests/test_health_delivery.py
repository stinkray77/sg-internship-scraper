import datetime as dt
import threading
import unittest
from unittest.mock import MagicMock
from delivery_worker import DeliveryWorker
from source_health import health_warnings
from source_adapters import fetch_html


class HealthTests(unittest.TestCase):
    def test_repeated_errors_and_empty_after_positive(self):
        now=dt.datetime.now(dt.timezone.utc)
        row=lambda fetched,errors:dict(completed_at=now,fetched=fetched,matched=0,errors=errors)
        self.assertTrue(health_warnings([row(0,['error'])]*3))
        self.assertTrue(health_warnings([row(0,[])]*3+[row(1,[])]))
        self.assertFalse(health_warnings([row(0,[])]*3))

    def test_worker_delivers_before_scraping_finishes(self):
        delivered=threading.Event()
        calls=[]
        def deliver():
            calls.append(1);delivered.set();return 0
        worker=DeliveryWorker(deliver,interval=0.01)
        worker.start()
        self.assertTrue(delivered.wait(1))
        worker.finish()
        self.assertGreaterEqual(len(calls),2)
        self.assertFalse(worker.thread.is_alive())

    def test_missing_html_contract_is_not_clean_empty(self):
        source={'id':'html_test','company':'Firm','config':dict(url='https://example.com',row_selector='.job',link_selector='a')}
        requester=MagicMock(return_value=MagicMock(text='<html>Access denied</html>'))
        with self.assertRaises(ValueError):fetch_html(source,requester)
