"""Session advisory locks held only on dedicated, unpooled connections."""
from contextlib import contextmanager
import logging

import psycopg2
from psycopg2.extensions import parse_dsn


DELIVERY_LOCK = "scraper:v2:delivery"
PIPELINE_LOCK_PREFIX = "scraper:v2:pipeline:"


class CoordinationError(RuntimeError):
    """A credential-free configuration or lock-management failure."""


def validate_direct_url(dsn):
    if not dsn or not dsn.strip():
        raise CoordinationError("DATABASE_URL_UNPOOLED is required for coordinated live runs")
    try:
        settings = parse_dsn(dsn)
    except psycopg2.Error:
        raise CoordinationError("DATABASE_URL_UNPOOLED is not a valid PostgreSQL connection string") from None
    if any("-pooler." in host or host.endswith("-pooler")
           for host in settings.get("host", "").lower().split(",")):
        raise CoordinationError("DATABASE_URL_UNPOOLED must use a direct endpoint, not a Neon pooler")


@contextmanager
def advisory_lock(dsn, key):
    """Yield acquisition status; release on the same session, even on failure."""
    validate_direct_url(dsn)
    conn = None
    acquired = False
    body_failed = False
    try:
        try:
            conn = psycopg2.connect(dsn, connect_timeout=10,
                                    application_name="sg-internship-scraper-coordination")
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = '10s'")
                cur.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", (key,))
                acquired = bool(cur.fetchone()[0])
        except psycopg2.Error:
            raise CoordinationError(f"Could not acquire direct coordination lock: {key}") from None
        try:
            yield acquired
        except BaseException:
            body_failed = True
            raise
    finally:
        if conn is not None:
            try:
                if acquired:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (key,))
                        if not cur.fetchone()[0]:
                            raise CoordinationError("Coordination lock was no longer owned")
            except (psycopg2.Error, CoordinationError):
                if body_failed:
                    logging.warning("Could not release coordination lock %s while handling an earlier error", key)
                else:
                    raise CoordinationError(f"Could not release direct coordination lock: {key}") from None
            finally:
                conn.close()


def check_coordination(dsn):
    """Exercise contention and release on an isolated key; no application writes."""
    key = "scraper:v2:validation"
    with advisory_lock(dsn, key) as owner:
        if not owner:
            raise CoordinationError("Another coordination check is already running")
        with advisory_lock(dsn, key) as contender:
            if contender:
                raise CoordinationError("A competing connection acquired the owned check lock")
        with advisory_lock(dsn, key + ":independent") as independent:
            if not independent:
                raise CoordinationError("An independent check lock was unexpectedly blocked")
    with advisory_lock(dsn, key) as next_owner:
        if not next_owner:
            raise CoordinationError("The check lock was not released for the next owner")


if __name__ == "__main__":
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(description="Check direct database lock coordination without sending alerts")
    parser.add_argument("--check", action="store_true", required=True)
    parser.parse_args()
    try:
        check_coordination(os.environ.get("DATABASE_URL_UNPOOLED"))
    except CoordinationError as error:
        print(f"Coordination check failed: {error}", file=sys.stderr)
        sys.exit(1)
    print("Coordination check passed: contention, independent keys, and reacquisition after release.")
