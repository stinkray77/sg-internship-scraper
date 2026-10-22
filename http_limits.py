"""Shared request concurrency and per-source deadlines."""
import contextlib
import threading
import time
from urllib.parse import urlsplit
from urllib3.util.retry import Retry

GLOBAL = threading.BoundedSemaphore(4)
HOSTS = {}
LOCK = threading.Lock()
CONTEXT = threading.local()


@contextlib.contextmanager
def source_deadline(seconds=180):
    previous = getattr(CONTEXT, 'deadline', None)
    CONTEXT.deadline = time.monotonic() + seconds
    try: yield
    finally: CONTEXT.deadline = previous


@contextlib.contextmanager
def request_slot(url):
    deadline = getattr(CONTEXT, 'deadline', None)
    def remaining():
        value = deadline-time.monotonic() if deadline else 60
        if value <= 0: raise TimeoutError('Source deadline reached; snapshot incomplete')
        return value
    with LOCK: host = HOSTS.setdefault(urlsplit(url).netloc, threading.BoundedSemaphore(1))
    if not host.acquire(timeout=remaining()): raise TimeoutError('Host request queue deadline')
    try:
        if not GLOBAL.acquire(timeout=remaining()): raise TimeoutError('Global request queue deadline')
        try: yield min(20, remaining())
        finally: GLOBAL.release()
    finally: host.release()


class BoundedRetry(Retry):
    def sleep(self, response=None):
        delay = self.get_retry_after(response) if response is not None and self.respect_retry_after_header else None
        delay = delay if delay is not None else self.get_backoff_time()
        deadline = getattr(CONTEXT, 'deadline', None)
        if delay > 30 or (deadline is not None and time.monotonic() + delay >= deadline):
            raise TimeoutError('Retry delay exceeds source budget; try on next scheduled run')
        if delay > 0:
            time.sleep(delay)
