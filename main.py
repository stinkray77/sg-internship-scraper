import os
import argparse
import datetime
import hashlib
import html
import json
import requests
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
import psycopg2
import pandas as pd
from psycopg2.extras import Json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin
from jobspy import scrape_jobs
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from source_adapters import fetch_source, load_source_registry
from eligibility import EligibilityAssessment, assess_eligibility

# LOAD FIRST
load_dotenv()

# --- CONFIGURATION ---
DB_URL = os.environ.get("DATABASE_URL")
BOT_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

QUANT_INTERNSHIP_INDEX_URL = (
    "https://raw.githubusercontent.com/northwesternfintech/"
    "2027QuantInternships/main/README.md"
)
SINGAPORE_INTERNSHIP_INDEX_URL = (
    "https://raw.githubusercontent.com/didtheyghostme/"
    "Singapore-Summer2026-TechInternships/main/README.md"
)
QUANT_JOB_MAX_AGE_DAYS = 14


def positive_env_int(name: str, default: int) -> int:
    raw_value = os.environ.get(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as e:
        raise RuntimeError(f"{name} must be an integer") from e
    if value <= 0:
        raise RuntimeError(f"{name} must be positive")
    return value


JOB_LOOKBACK_HOURS = positive_env_int("JOB_LOOKBACK_HOURS", 72)
RESULTS_PER_SOURCE = positive_env_int("RESULTS_PER_SOURCE", 50)
HTTP_TIMEOUT_SECONDS = 20
TELEGRAM_TIMEOUT_SECONDS = 15
MAX_TELEGRAM_RATE_LIMIT_RETRIES = 3
TELEGRAM_RETRY_BUFFER_SECONDS = 1
MAX_DELIVERY_ATTEMPTS = 5
STALE_DELIVERY_MINUTES = 30
CLOSE_AFTER_SUCCESSFUL_MISSES = 3
DRY_RUN = False


@dataclass
class DeliveryResult:
    success: bool
    error: str | None = None
    retry_after_seconds: int | None = None


@dataclass
class PipelineStats:
    source: str
    fetched: int = 0
    matched: int = 0
    queued: int = 0
    duplicates: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass
class LifecycleOutcome:
    event_type: str
    queued: bool = False


def build_http_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


HTTP_SESSIONS = threading.local()


def get_http_session() -> requests.Session:
    session = getattr(HTTP_SESSIONS, "session", None)
    if session is None:
        session = build_http_session()
        HTTP_SESSIONS.session = session
    return session


def http_request(method: str, url: str, **kwargs) -> requests.Response:
    kwargs.setdefault("timeout", HTTP_TIMEOUT_SECONDS)
    response = get_http_session().request(method, url, **kwargs)
    response.raise_for_status()
    return response


def http_get(url: str, **kwargs) -> requests.Response:
    return http_request("GET", url, **kwargs)

def get_db_connection():
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS seen_jobs (
            job_id TEXT PRIMARY KEY,
            company TEXT,
            title TEXT,
            site TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            payload JSONB,
            dedupe_key TEXT,
            delivery_status TEXT NOT NULL DEFAULT 'sent',
            attempt_count INTEGER NOT NULL DEFAULT 0,
            last_attempt_at TIMESTAMP,
            next_attempt_at TIMESTAMP,
            sent_at TIMESTAMP,
            last_error TEXT
        );
    """)
    migrations = [
        "ALTER TABLE seen_jobs ADD COLUMN IF NOT EXISTS payload JSONB",
        "ALTER TABLE seen_jobs ADD COLUMN IF NOT EXISTS dedupe_key TEXT",
        "ALTER TABLE seen_jobs ADD COLUMN IF NOT EXISTS delivery_status TEXT NOT NULL DEFAULT 'sent'",
        "ALTER TABLE seen_jobs ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE seen_jobs ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMP",
        "ALTER TABLE seen_jobs ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMP",
        "ALTER TABLE seen_jobs ADD COLUMN IF NOT EXISTS sent_at TIMESTAMP",
        "ALTER TABLE seen_jobs ADD COLUMN IF NOT EXISTS last_error TEXT",
    ]
    for migration in migrations:
        cur.execute(migration)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS seen_jobs_delivery_queue_idx
        ON seen_jobs (delivery_status, next_attempt_at);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS seen_jobs_recent_dedupe_idx
        ON seen_jobs (dedupe_key, created_at);
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS job_observations (
            observation_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            external_id TEXT NOT NULL,
            company TEXT NOT NULL,
            title TEXT NOT NULL,
            job_url TEXT,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            eligibility JSONB NOT NULL DEFAULT '{}'::jsonb,
            lifecycle_status TEXT NOT NULL DEFAULT 'active',
            content_fingerprint TEXT,
            lifecycle_version INTEGER NOT NULL DEFAULT 1,
            missing_snapshot_count INTEGER NOT NULL DEFAULT 0,
            first_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            closed_at TIMESTAMPTZ,
            UNIQUE (source_id, external_id)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS job_lifecycle_events (
            event_id BIGSERIAL PRIMARY KEY,
            observation_id TEXT NOT NULL REFERENCES job_observations(observation_id),
            event_type TEXT NOT NULL,
            event_version INTEGER NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            before_payload JSONB,
            after_payload JSONB,
            UNIQUE (observation_id, event_version, event_type)
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS job_observations_source_status_idx
        ON job_observations (source_id, lifecycle_status, last_seen_at);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS job_lifecycle_events_observation_idx
        ON job_lifecycle_events (observation_id, occurred_at);
    """)
    cur.execute("""
        INSERT INTO job_observations (
            observation_id, source_id, external_id, company, title, job_url,
            payload, eligibility, lifecycle_status, lifecycle_version,
            first_seen_at, last_seen_at, last_changed_at
        )
        SELECT
            jobs.job_id,
            COALESCE(NULLIF(jobs.payload->>'source_id', ''), NULLIF(jobs.site, ''), 'legacy'),
            jobs.job_id,
            COALESCE(NULLIF(jobs.company, ''), 'Unknown Company'),
            COALESCE(NULLIF(jobs.title, ''), 'Untitled Role'),
            jobs.payload->>'job_url',
            COALESCE(jobs.payload, '{}'::jsonb),
            COALESCE(jobs.payload->'eligibility', '{}'::jsonb),
            'active',
            0,
            COALESCE(jobs.created_at, CURRENT_TIMESTAMP),
            COALESCE(jobs.sent_at, jobs.created_at, CURRENT_TIMESTAMP),
            COALESCE(jobs.sent_at, jobs.created_at, CURRENT_TIMESTAMP)
        FROM seen_jobs AS jobs
        WHERE jobs.job_id NOT LIKE 'system_canary_%'
          AND COALESCE(jobs.payload->>'message_type', '') <> 'canary'
          AND jobs.payload->>'lifecycle_event' IS NULL
        ON CONFLICT DO NOTHING;
    """)
    cur.execute("""
        INSERT INTO job_lifecycle_events (
            observation_id, event_type, event_version, occurred_at, after_payload
        )
        SELECT observation_id, 'backfilled', 0, first_seen_at, payload
        FROM job_observations AS observations
        WHERE observations.lifecycle_version = 0
        ON CONFLICT DO NOTHING;
    """)
    conn.commit()
    cur.close()
    conn.close()

def _message_value(value: object, limit: int = 300) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) > limit:
        text = f"{text[:limit - 1].rstrip()}…"
    return html.escape(text)


def _format_message_date(value: object) -> str:
    if not value:
        return "Not provided"
    if isinstance(value, (datetime.datetime, datetime.date)):
        parsed = value
    else:
        raw = str(value)
        try:
            parsed = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = datetime.date.fromisoformat(raw)
            except ValueError:
                return raw
    return parsed.strftime("%d %b %Y")


def _format_location(value: object) -> str:
    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(item) for item in value if item)
    return str(value or "Not provided")


def build_telegram_message(job: dict) -> str:
    if job.get("message_type") == "canary":
        return (
            "🧪 <b>SCRAPER CANARY</b>\n\n"
            f"<b>{_message_value(job.get('company'), 180)}</b>\n"
            f"{_message_value(job.get('title'), 300)}\n\n"
            "✅ Database queue and Telegram delivery are working."
        )

    event_type = job.get("lifecycle_event", "new")
    heading = "🔁 REOPENED INTERNSHIP" if event_type == "reopened" else "🟢 NEW INTERNSHIP"
    eligibility = job.get("eligibility") or {}
    verdict_labels = {
        "likely_eligible": "Likely undergrad eligible",
        "ineligible": "Postgraduate-only requirement",
        "unknown": "Requirements unclear",
    }
    lines = [
        f"<b>{heading}</b>",
        "",
        f"<b>{_message_value(job.get('company') or 'Unknown Company', 180)}</b>",
        _message_value(job.get("title") or "Untitled Role", 300),
        "",
        f"📍 <b>Location:</b> {_message_value(_format_location(job.get('location')), 250)}",
        (
            "🎓 <b>Eligibility:</b> "
            f"{_message_value(verdict_labels.get(eligibility.get('verdict'), 'Requirements unclear'))}"
        ),
    ]
    optional_eligibility = (
        ("Degree", ", ".join(eligibility.get("degree_levels") or [])),
        ("Graduation", ", ".join(str(year) for year in eligibility.get("graduation_years") or [])),
        ("Duration", eligibility.get("duration")),
        ("Work rights", eligibility.get("work_authorization")),
    )
    for label, value in optional_eligibility:
        if value:
            lines.append(f"   • <b>{label}:</b> {_message_value(value, 250)}")
    lines.extend([
        "",
        f"📅 <b>Posted:</b> {_message_value(_format_message_date(job.get('date_posted')))}",
        f"👀 <b>First seen:</b> {_message_value(_format_message_date(job.get('first_seen_at')))}",
        f"🔎 <b>Source:</b> {_message_value(job.get('source_label') or job.get('site') or 'Unknown', 180)}",
        "",
        (
            f"🔗 <a href=\"{html.escape(str(job.get('job_url') or '#')[:1000], quote=True)}\">"
            "View and apply</a>"
        ),
    ])
    message = "\n".join(lines)
    if len(BeautifulSoup(message, "html.parser").get_text()) > 3500:
        raise ValueError("Telegram message exceeds the 3500-character safety limit")
    return message


def send_telegram_alert(job) -> DeliveryResult:
    msg = build_telegram_message(job)
    
    # Check if credentials exist before trying to send
    import os
    bot_token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not bot_token or not chat_id:
        error = "Telegram credentials not found in environment variables"
        print(f"❌ CRITICAL: {error}.")
        return DeliveryResult(False, error)

    tg_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": msg,
        "parse_mode": "HTML",
        "link_preview_options": {"is_disabled": True},
    }
    
    try:
        response = requests.post(
            tg_url,
            json=payload,
            timeout=TELEGRAM_TIMEOUT_SECONDS,
        )
        if response.status_code != 200:
            error = f"HTTP {response.status_code}: {response.text[:500]}"
            print(f"❌ TELEGRAM API ERROR: {error}")
            retry_after_seconds = None
            if response.status_code == 429:
                try:
                    retry_after_seconds = int(
                        response.json().get("parameters", {}).get("retry_after")
                    )
                    if retry_after_seconds <= 0:
                        retry_after_seconds = None
                except (AttributeError, TypeError, ValueError):
                    retry_after_seconds = None
            return DeliveryResult(False, error, retry_after_seconds)
        else:
            print("✅ Message successfully sent to Telegram!")
            return DeliveryResult(True)
    except requests.RequestException as e:
        print(f"❌ TELEGRAM CONNECTION FAILED: {e}")
        return DeliveryResult(False, str(e))


def make_dedupe_key(job: dict) -> str:
    company = normalize_company_name(
        str(job.get("company_key") or job.get("company", ""))
    )
    title = re.sub(r"[^a-z0-9]", "", str(job.get("title", "")).lower())
    return hashlib.sha256(f"{company}:{title}".encode()).hexdigest()


def json_safe(value):
    """Convert scraper values, including pandas scalars, into JSON-safe values."""
    if value is None:
        return None
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return json_safe(value.item())
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def normalize_job_payload(job: dict) -> dict:
    payload = json_safe(job)
    if not payload.get("company"):
        payload["company"] = "Unknown Company"
    if not payload.get("title"):
        payload["title"] = "Untitled Role"
    return payload


def enqueue_job(
    conn,
    job_id: str,
    job: dict,
    stats: PipelineStats,
    *,
    bypass_recent_dedupe: bool = False,
    commit: bool = True,
    count_match: bool = True,
) -> bool:
    """Persist a candidate for delivery, or print it during dry-run."""
    job = normalize_job_payload(job)
    if count_match:
        stats.matched += 1
    if DRY_RUN:
        print(f"🧪 Would enqueue: {job['title']} at {job['company']}")
        stats.queued += 1
        return True

    dedupe_key = make_dedupe_key(job)
    cur = conn.cursor()
    try:
        if not bypass_recent_dedupe:
            cur.execute(
                """
                SELECT job_id
                FROM seen_jobs
                WHERE dedupe_key = %s
                  AND delivery_status IN ('pending', 'sending', 'failed', 'sent')
                  AND created_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
                LIMIT 1
                """,
                (dedupe_key,),
            )
            if cur.fetchone() is not None:
                stats.duplicates += 1
                return False

        cur.execute(
            """
            INSERT INTO seen_jobs (
                job_id, company, title, site, payload, dedupe_key,
                delivery_status, attempt_count, next_attempt_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, 'pending', 0, CURRENT_TIMESTAMP)
            ON CONFLICT (job_id) DO NOTHING
            RETURNING job_id
            """,
            (
                job_id,
                job.get("company"),
                job.get("title"),
                job.get("site"),
                Json(job, dumps=json.dumps),
                dedupe_key,
            ),
        )
        queued = cur.fetchone() is not None
        if commit:
            conn.commit()
        if queued:
            stats.queued += 1
        else:
            stats.duplicates += 1
        return queued
    except Exception:
        if commit:
            conn.rollback()
        raise
    finally:
        cur.close()


def make_content_fingerprint(job: dict) -> str:
    material = {
        "company": job.get("company"),
        "title": job.get("title"),
        "job_url": job.get("job_url"),
        "location": job.get("location"),
        "country": job.get("country"),
        "date_posted": job.get("date_posted"),
        "eligibility": job.get("eligibility"),
    }
    encoded = json.dumps(json_safe(material), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _record_lifecycle_event(
    cur,
    observation_id: str,
    event_type: str,
    event_version: int,
    before_payload: dict | None,
    after_payload: dict,
    occurred_at: datetime.datetime,
) -> None:
    cur.execute(
        """
        INSERT INTO job_lifecycle_events (
            observation_id, event_type, event_version, occurred_at,
            before_payload, after_payload
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (
            observation_id,
            event_type,
            event_version,
            occurred_at,
            Json(json_safe(before_payload), dumps=json.dumps) if before_payload else None,
            Json(json_safe(after_payload), dumps=json.dumps),
        ),
    )


def observe_job(
    conn,
    observation_id: str,
    source_id: str,
    external_id: str,
    job: dict,
    stats: PipelineStats,
    assessment: EligibilityAssessment | None = None,
) -> LifecycleOutcome:
    """Upsert lifecycle state and queue only new or reopened opportunities."""
    assessment = assessment or assess_eligibility(
        job.get("title", ""),
        job.get("description", ""),
    )
    if assessment.verdict == "ineligible":
        return LifecycleOutcome("excluded")

    stats.matched += 1
    payload = normalize_job_payload(job)
    payload["source_id"] = source_id
    payload["eligibility"] = assessment.to_payload()
    payload.pop("description", None)
    fingerprint = make_content_fingerprint(payload)

    if DRY_RUN:
        queued = enqueue_job(
            conn,
            observation_id,
            {**payload, "lifecycle_event": "new"},
            stats,
            count_match=False,
        )
        return LifecycleOutcome("new", queued)

    now = datetime.datetime.now(datetime.timezone.utc)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT lifecycle_status, content_fingerprint, lifecycle_version,
                   payload, first_seen_at
            FROM job_observations
            WHERE observation_id = %s
            FOR UPDATE
            """,
            (observation_id,),
        )
        existing = cur.fetchone()
        queued = False
        event_type = "unchanged"

        if existing is None:
            version = 1
            first_seen_at = now
            cur.execute(
                """
                INSERT INTO job_observations (
                    observation_id, source_id, external_id, company, title,
                    job_url, payload, eligibility, lifecycle_status,
                    content_fingerprint, lifecycle_version,
                    missing_snapshot_count, first_seen_at, last_seen_at,
                    last_changed_at, closed_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, 'active', %s, %s,
                    0, %s, %s, %s, NULL
                )
                """,
                (
                    observation_id,
                    source_id,
                    external_id,
                    payload["company"],
                    payload["title"],
                    payload.get("job_url"),
                    Json(payload, dumps=json.dumps),
                    Json(assessment.to_payload(), dumps=json.dumps),
                    fingerprint,
                    version,
                    now,
                    now,
                    now,
                ),
            )
            event_type = "new"
            _record_lifecycle_event(
                cur, observation_id, event_type, version, None, payload, now
            )
        else:
            status, old_fingerprint, version, old_payload, first_seen_at = existing
            old_payload = old_payload or {}
            if status == "closed":
                version += 1
                event_type = "reopened"
                changed_at = now
            elif old_fingerprint and old_fingerprint != fingerprint:
                version += 1
                event_type = "updated"
                changed_at = now
            else:
                changed_at = None

            cur.execute(
                """
                UPDATE job_observations
                SET source_id = %s,
                    external_id = %s,
                    company = %s,
                    title = %s,
                    job_url = %s,
                    payload = %s,
                    eligibility = %s,
                    lifecycle_status = 'active',
                    content_fingerprint = %s,
                    lifecycle_version = %s,
                    missing_snapshot_count = 0,
                    last_seen_at = %s,
                    last_changed_at = COALESCE(%s, last_changed_at),
                    closed_at = NULL
                WHERE observation_id = %s
                """,
                (
                    source_id,
                    external_id,
                    payload["company"],
                    payload["title"],
                    payload.get("job_url"),
                    Json(payload, dumps=json.dumps),
                    Json(assessment.to_payload(), dumps=json.dumps),
                    fingerprint,
                    version,
                    now,
                    changed_at,
                    observation_id,
                ),
            )
            if event_type in {"updated", "reopened"}:
                _record_lifecycle_event(
                    cur,
                    observation_id,
                    event_type,
                    version,
                    old_payload,
                    payload,
                    now,
                )

        if event_type in {"new", "reopened"}:
            delivery_id = observation_id
            if event_type == "reopened":
                delivery_id = f"{observation_id}:reopened:{version}"
            delivery_payload = {
                **payload,
                "lifecycle_event": event_type,
                "first_seen_at": json_safe(first_seen_at),
            }
            queued = enqueue_job(
                conn,
                delivery_id,
                delivery_payload,
                stats,
                bypass_recent_dedupe=event_type == "reopened",
                commit=False,
                count_match=False,
            )

        conn.commit()
        return LifecycleOutcome(event_type, queued)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def reconcile_source_snapshot(
    conn,
    source_id: str,
    observed_ids: set[str],
) -> list[str]:
    """Close jobs missing from three consecutive successful full snapshots."""
    now = datetime.datetime.now(datetime.timezone.utc)
    cur = conn.cursor()
    closed = []
    try:
        cur.execute(
            """
            SELECT observation_id, missing_snapshot_count, lifecycle_version, payload
            FROM job_observations
            WHERE source_id = %s
              AND lifecycle_status = 'active'
              AND NOT (observation_id = ANY(%s::text[]))
            FOR UPDATE
            """,
            (source_id, sorted(observed_ids)),
        )
        for observation_id, misses, version, payload in cur.fetchall():
            next_misses = misses + 1
            if next_misses < CLOSE_AFTER_SUCCESSFUL_MISSES:
                cur.execute(
                    """
                    UPDATE job_observations
                    SET missing_snapshot_count = %s
                    WHERE observation_id = %s
                    """,
                    (next_misses, observation_id),
                )
                continue

            version += 1
            after_payload = {**(payload or {}), "lifecycle_status": "closed"}
            cur.execute(
                """
                UPDATE job_observations
                SET lifecycle_status = 'closed',
                    lifecycle_version = %s,
                    missing_snapshot_count = %s,
                    last_changed_at = %s,
                    closed_at = %s
                WHERE observation_id = %s
                """,
                (version, next_misses, now, now, observation_id),
            )
            _record_lifecycle_event(
                cur,
                observation_id,
                "closed",
                version,
                payload or {},
                after_payload,
                now,
            )
            closed.append(observation_id)
        conn.commit()
        return closed
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def claim_due_delivery(conn, only_job_id: str | None = None):
    """Atomically claim one due delivery and return its persisted payload."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH candidate AS (
                SELECT job_id
                FROM seen_jobs
                WHERE attempt_count < %s
                  AND (%s IS NULL OR job_id = %s)
                  AND (
                      (delivery_status IN ('pending', 'failed')
                       AND COALESCE(next_attempt_at, CURRENT_TIMESTAMP) <= CURRENT_TIMESTAMP)
                      OR
                      (delivery_status = 'sending'
                       AND last_attempt_at < CURRENT_TIMESTAMP - (%s * INTERVAL '1 minute'))
                  )
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE seen_jobs AS jobs
            SET delivery_status = 'sending',
                attempt_count = jobs.attempt_count + 1,
                last_attempt_at = CURRENT_TIMESTAMP,
                last_error = NULL
            FROM candidate
            WHERE jobs.job_id = candidate.job_id
            RETURNING jobs.job_id, jobs.payload, jobs.attempt_count
            """,
            (
                MAX_DELIVERY_ATTEMPTS,
                only_job_id,
                only_job_id,
                STALE_DELIVERY_MINUTES,
            ),
        )
        claimed = cur.fetchone()
        conn.commit()
        return claimed
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def complete_delivery(conn, job_id: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE seen_jobs
            SET delivery_status = 'sent',
                sent_at = CURRENT_TIMESTAMP,
                next_attempt_at = NULL,
                last_error = NULL
            WHERE job_id = %s
            """,
            (job_id,),
        )
        conn.commit()
    finally:
        cur.close()


def fail_delivery(
    conn,
    job_id: str,
    attempt_count: int,
    error: str,
) -> str:
    terminal = attempt_count >= MAX_DELIVERY_ATTEMPTS
    status = "dead" if terminal else "failed"
    delay_minutes = min(15 * (2 ** max(attempt_count - 1, 0)), 360)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE seen_jobs
            SET delivery_status = %s,
                next_attempt_at = CASE
                    WHEN %s THEN NULL
                    ELSE CURRENT_TIMESTAMP + (%s * INTERVAL '1 minute')
                END,
                last_error = %s
            WHERE job_id = %s
            """,
            (status, terminal, delay_minutes, error[:2000], job_id),
        )
        conn.commit()
        return status
    finally:
        cur.close()


def deliver_pending_jobs(only_job_id: str | None = None) -> int:
    """Deliver all currently due jobs and return the number of failures."""
    if DRY_RUN:
        return 0

    failures = 0
    conn = get_db_connection()
    try:
        while True:
            claimed = claim_due_delivery(conn, only_job_id=only_job_id)
            if claimed is None:
                break

            job_id, payload, attempt_count = claimed
            result = send_telegram_alert(payload)
            rate_limit_retries = 0
            while (
                not result.success
                and result.retry_after_seconds is not None
                and rate_limit_retries < MAX_TELEGRAM_RATE_LIMIT_RETRIES
            ):
                wait_seconds = (
                    result.retry_after_seconds + TELEGRAM_RETRY_BUFFER_SECONDS
                )
                rate_limit_retries += 1
                print(
                    f"⏳ Telegram rate limit for {job_id}; waiting "
                    f"{wait_seconds}s before retry "
                    f"{rate_limit_retries}/{MAX_TELEGRAM_RATE_LIMIT_RETRIES}."
                )
                time.sleep(wait_seconds)
                result = send_telegram_alert(payload)
            if result.success:
                complete_delivery(conn, job_id)
            else:
                failures += 1
                status = fail_delivery(
                    conn,
                    job_id,
                    attempt_count,
                    result.error or "Unknown Telegram failure",
                )
                print(f"⚠️ Delivery {job_id} marked {status}.")
    finally:
        conn.close()
    return failures

def is_target_role(title: str) -> bool:
    """
    Returns True if the job title matches your career targets (SWE/Quant).
    Returns False for irrelevant roles (HR, Sales, Marketing).
    """
    title_lower = str(title or "").lower()
    
    blacklist_pattern = (
        r"\b(hr|human resources|accounting|civil|mechanical|electrical|retail|"
        r"design|product management|product manager|project management)\b"
    )
    if re.search(blacklist_pattern, title_lower):
        return False

    strong_technical_pattern = (
        r"\b(software|swe|developer|programmer|machine learning|ml|backend|"
        r"frontend|fullstack|cloud|systems|platform|infrastructure|devops|"
        r"security|computer vision|nlp|ai|genai|llm|blockchain)\b|"
        r"\bdata\s+(engineer|engineering|scientist|analyst)\b"
    )
    if re.search(r"\b(sales|marketing)\b", title_lower) and not re.search(
        strong_technical_pattern,
        title_lower,
    ):
        return False

    whitelist_pattern = (
        r"\b(software|swe|developer|programmer|technology|"
        r"quant|quantitative|trading|trader|algorithm|algorithmic|researcher|"
        r"data|ai|genai|llm|machine learning|ml|backend|frontend|fullstack|"
        r"cloud|systems|platform|infrastructure|devops|security|computer vision|"
        r"nlp|blockchain|fintech)\b|\brisk\s+(model|modelling|modeling|analytics|"
        r"technology|engineering)\b"
    )
    has_target = bool(re.search(whitelist_pattern, title_lower))

    student_role_pattern = (
        r"\b(intern|internship|co-?op|industrial attachment|technical trainee|"
        r"summer analyst|off-cycle analyst|winternship|spring insight|"
        r"insight program(?:me)?|apprenticeship|traineeship|trainee|"
        r"accelerator program(?:me)?)\b"
    )
    is_intern = bool(re.search(student_role_pattern, title_lower))

    return has_target and is_intern


def is_quant_intern_role(title: str) -> bool:
    """Return whether a title is a technical internship at a quant firm."""
    title_lower = str(title or "").lower()
    blacklist_pattern = r"\b(sales|marketing|hr|human resources|accounting|retail|design)\b"
    if re.search(blacklist_pattern, title_lower):
        return False

    student_role_pattern = (
        r"\b(intern|internship|co-?op|industrial attachment|technical trainee|"
        r"summer analyst|off-cycle analyst|winternship|spring insight|"
        r"insight program(?:me)?|apprenticeship|traineeship|trainee|"
        r"accelerator program(?:me)?)\b"
    )
    if not re.search(student_role_pattern, title_lower):
        return False

    quant_role_pattern = (
        r"\b(software|swe|developer|engineer|engineering|quant|quantitative|"
        r"trading|trader|research|researcher|algorithm|algorithmic|data|ai|"
        r"machine learning|ml|backend|frontend|fullstack)\b"
    )
    return bool(re.search(quant_role_pattern, title_lower))


def is_explicitly_phd_only(title: str, description: str = "") -> bool:
    """Compatibility helper for explicit postgraduate-only requirements."""
    return assess_eligibility(title, description).verdict == "ineligible"


def is_undergrad_technical_job(job: dict) -> bool:
    return (
        is_target_role(job.get("title", ""))
        and assess_eligibility(
            job.get("title", ""),
            job.get("description", ""),
        ).verdict != "ineligible"
    )

SINGAPORE_LOCATION_PATTERN = re.compile(
    r"(?<![a-z0-9])(?:singapore|sg)(?![a-z0-9])",
    re.IGNORECASE,
)


def _contains_singapore_location(value) -> bool:
    """Return whether a string or collection explicitly names Singapore."""
    if isinstance(value, str):
        return bool(SINGAPORE_LOCATION_PATTERN.search(value))
    if isinstance(value, (list, tuple, set)):
        return any(_contains_singapore_location(item) for item in value)
    return False


def is_singapore_job(job: dict) -> bool:
    """Return whether a normalized job is explicitly anchored to Singapore."""
    return (
        _contains_singapore_location(job.get("country"))
        or
        _contains_singapore_location(job.get("location"))
    )


def normalize_company_name(name: str) -> str:
    """Normalize a company name for matching across independent indexes."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def parse_quant_firms(markdown: str) -> set[str]:
    """Extract normalized firm names from the quant internship index."""
    firm_names = re.findall(
        r"^## (.+)\n\*\*Website\*\*:",
        markdown,
        flags=re.MULTILINE,
    )
    return {normalize_company_name(name) for name in firm_names}


def parse_singapore_internships(markdown: str) -> list[dict]:
    """Extract jobs from the verified Singapore internship Markdown table."""
    jobs = []

    for line in markdown.splitlines():
        if not line.startswith("| ["):
            continue

        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != 5:
            continue

        company_match = re.match(r"\[([^]]+)\]\([^)]+\)", cells[0])
        application_match = re.search(r'href="([^"]+)"', cells[3])
        if not company_match or not application_match:
            continue

        try:
            date_added = datetime.datetime.strptime(
                cells[4],
                "%d %b %Y",
            ).date()
        except ValueError:
            continue

        jobs.append({
            "company": company_match.group(1),
            "title": cells[1],
            "job_url": html.unescape(application_match.group(1)),
            "date_added": date_added,
        })

    return jobs


def find_recent_singapore_quant_jobs(
    quant_markdown: str,
    singapore_markdown: str,
    today: datetime.date | None = None,
) -> list[dict]:
    """Intersect quant firms with recent verified Singapore internships."""
    today = today or datetime.date.today()
    cutoff = today - datetime.timedelta(days=QUANT_JOB_MAX_AGE_DAYS)
    quant_firms = parse_quant_firms(quant_markdown)
    matching_jobs = []

    for job in parse_singapore_internships(singapore_markdown):
        if job["date_added"] < cutoff:
            continue
        if normalize_company_name(job["company"]) not in quant_firms:
            continue

        job_data = {
            "site": "SG Quant Index",
            "title": job["title"],
            "company": job["company"],
            "job_url": job["job_url"],
            "date_posted": job["date_added"],
            "location": "Singapore",
        }
        if is_quant_intern_role(job_data["title"]) and is_singapore_job(job_data):
            matching_jobs.append(job_data)

    return matching_jobs


def run_pipeline():
    print("🚀 Running Broad Catch-All Pipeline for SG...")
    stats = PipelineStats("JobSpy")
    conn = None
    broad_search = "(software OR developer OR data OR quant OR AI OR machine learning OR engineer) AND intern"

    try:
        jobs = scrape_jobs(
            site_name=["linkedin", "indeed", "glassdoor"],
            search_term=broad_search,
            location="Singapore",
            results_wanted=RESULTS_PER_SOURCE,
            hours_old=JOB_LOOKBACK_HOURS,
            country_indeed='Singapore'
        )

        if jobs is None or jobs.empty:
            print("⚠️ No results found.")
            return stats

        stats.fetched = len(jobs)
        if not DRY_RUN:
            conn = get_db_connection()

        for _, row in jobs.iterrows():
            job_data = row.to_dict()
            title = row['title']
            company = row['company']

            if not is_target_role(job_data.get("title", "")):
                print(f"🗑️ Filtered out non-target role: {title} at {company}")
                continue

            if not is_singapore_job(job_data):
                print(f"🌏 Filtered out non-Singapore job: {title} at {company}")
                continue

            raw_id = str(row['id'])
            site = str(row['site'])
            assessment = assess_eligibility(
                job_data.get("title", ""),
                job_data.get("description", ""),
            )
            if assessment.verdict == "ineligible":
                print(f"🎓 Filtered out postgraduate-only role: {title} at {company}")
                continue
            job_data["source_id"] = site
            job_data["source_label"] = f"{site.title()} job listing"
            unique_id = f"{site}_{raw_id}"
            observe_job(
                conn,
                unique_id,
                site,
                raw_id,
                job_data,
                stats,
                assessment,
            )

    except Exception as e:
        print(f"❌ Pipeline Error: {e}")
        stats.errors.append(str(e))
    finally:
        if conn is not None:
            conn.close()
    return stats


def scrape_registry_pipelines():
    """Fetch configured ATS and bespoke sources, then enqueue sequentially."""
    print("🚀 Running configured official-source pipelines...")
    sources = [
        source
        for source in load_source_registry()
        if source.get("enabled") and source.get("mode", "direct") == "direct"
    ]
    results_by_id = {}
    with ThreadPoolExecutor(max_workers=min(4, len(sources) or 1)) as executor:
        futures = {
            executor.submit(fetch_source, source, http_request): source
            for source in sources
        }
        for future in as_completed(futures):
            source = futures[future]
            try:
                results_by_id[source["id"]] = future.result()
            except Exception as error:
                results_by_id[source["id"]] = None
                print(f"⚠️ {source['id']} fetch failed: {error}")

    conn = None if DRY_RUN else get_db_connection()
    stats_list = []
    try:
        for source in sources:
            stats = PipelineStats(source["id"])
            result = results_by_id.get(source["id"])
            if result is None:
                stats.errors.append("source worker failed")
                stats_list.append(stats)
                continue
            stats.fetched = result.fetched
            stats.warnings.extend(result.warnings)
            for warning in result.warnings:
                print(f"⚠️ {source['id']} warning: {warning}")
            if result.error:
                print(f"⚠️ {source['id']} source error: {result.error}")
                stats.errors.append(result.error)
                stats_list.append(stats)
                continue

            observed_ids = set()
            for candidate in result.candidates:
                job_data = candidate.to_payload()
                if not is_target_role(job_data.get("title", "")):
                    continue
                if not is_singapore_job(job_data):
                    continue
                assessment = assess_eligibility(
                    job_data.get("title", ""),
                    job_data.get("description", ""),
                )
                if assessment.verdict == "ineligible":
                    continue
                unique_id = f"{source['id']}_{candidate.external_id}"
                observed_ids.add(unique_id)
                observe_job(
                    conn,
                    unique_id,
                    source["id"],
                    candidate.external_id,
                    job_data,
                    stats,
                    assessment,
                )
            if (
                not DRY_RUN
                and source.get("lifecycle_mode") == "snapshot"
            ):
                closed = reconcile_source_snapshot(
                    conn,
                    source["id"],
                    observed_ids,
                )
                if closed:
                    print(f"📪 {source['id']} marked {len(closed)} job(s) closed.")
            stats_list.append(stats)
    finally:
        if conn is not None:
            conn.close()
    return stats_list


def extract_internsg_description(page_html: str) -> str:
    soup = BeautifulSoup(page_html, "html.parser")

    def iter_job_postings(value):
        if isinstance(value, list):
            for item in value:
                yield from iter_job_postings(item)
        elif isinstance(value, dict):
            if value.get("@type") == "JobPosting":
                yield value
            for item in value.values():
                if isinstance(item, (dict, list)):
                    yield from iter_job_postings(item)

    for script in soup.select('script[type="application/ld+json"]'):
        try:
            document = json.loads(script.string or script.get_text())
        except (TypeError, json.JSONDecodeError):
            continue
        for posting in iter_job_postings(document):
            description = posting.get("description")
            if description:
                return str(description)
    return ""


def scrape_internsg_pipeline():
    print("🚀 Running InternSG Pipeline...")
    stats = PipelineStats("InternSG")
    target_url = "https://www.internsg.com/jobs/?f_0=1&f_p=107&f_i=61&filter_s="
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    lookback_days = (JOB_LOOKBACK_HOURS + 23) // 24
    cutoff = datetime.date.today() - datetime.timedelta(days=lookback_days)
    conn = None

    try:
        if not DRY_RUN:
            conn = get_db_connection()
        next_url = target_url

        for _ in range(20):
            response = http_get(next_url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            listing_rows = soup.select('div.ast-row')
            if not listing_rows:
                raise ValueError("InternSG listing structure returned no job rows")

            page_dates = []
            for listing_row in listing_rows:
                link = listing_row.select_one('a[href*="/job/"]')
                if link is None:
                    continue

                url = urljoin(next_url, link.get('href'))
                title = link.get_text(strip=True)
                if len(title) <= 5:
                    continue
                stats.fetched += 1

                date_element = listing_row.select_one('span.badge-success')
                date_posted = None
                if date_element:
                    try:
                        parsed = datetime.datetime.strptime(
                            f"{date_element.get_text(strip=True)} "
                            f"{datetime.date.today().year}",
                            "%d %b %Y",
                        ).date()
                        if parsed > datetime.date.today() + datetime.timedelta(days=7):
                            parsed = parsed.replace(year=parsed.year - 1)
                        date_posted = parsed
                        page_dates.append(parsed)
                    except ValueError:
                        pass

                if date_posted is not None and date_posted < cutoff:
                    continue
                if not is_target_role(title):
                    continue

                location_element = (
                    listing_row.select_one('div.ast-col-lg-2')
                    if listing_row else None
                )
                location = (
                    location_element.get_text(" ", strip=True)
                    if location_element else None
                )
                company_element = listing_row.select_one('div.ast-col-lg-3')
                company = "InternSG Listing"
                if company_element:
                    company_parts = list(company_element.stripped_strings)
                    if company_parts:
                        company = company_parts[0]

                job_data = {
                    "site": "InternSG",
                    "title": title,
                    "company": company,
                    "job_url": url,
                    "location": location,
                    "date_posted": date_posted,
                }

                if not is_singapore_job(job_data):
                    print(f"🌏 Filtered out non-Singapore InternSG job: {title}")
                    continue

                canonical_url = url.split('?', 1)[0].rstrip('/')
                try:
                    detail_response = http_get(canonical_url, headers=headers)
                    job_data["description"] = extract_internsg_description(
                        detail_response.text
                    )
                except Exception as error:
                    warning = f"detail {url}: {error}"
                    stats.warnings.append(warning)
                    print(f"⚠️ InternSG {warning}")

                assessment = assess_eligibility(
                    job_data.get("title", ""),
                    job_data.get("description", ""),
                )
                if assessment.verdict == "ineligible":
                    continue

                raw_id = canonical_url.split('/')[-1]
                unique_id = f"internsg_{raw_id}"
                job_data["source_id"] = "internsg"
                job_data["source_label"] = "InternSG"
                observe_job(
                    conn,
                    unique_id,
                    "internsg",
                    raw_id,
                    job_data,
                    stats,
                    assessment,
                )

            next_link = soup.select_one('a.next.page-numbers, a[rel="next"]')
            if not next_link or not next_link.get('href'):
                break
            if page_dates and min(page_dates) < cutoff:
                break
            next_url = urljoin(next_url, next_link.get('href'))
    except Exception as e:
        print(f"❌ InternSG Pipeline Error: {e}")
        stats.errors.append(str(e))
    finally:
        if conn is not None:
            conn.close()
    return stats

def scrape_singapore_quant_pipeline():
    print("🚀 Running Singapore Quant Index Pipeline...")
    stats = PipelineStats("SG Quant Index")
    conn = None

    try:
        quant_response = http_get(QUANT_INTERNSHIP_INDEX_URL)
        singapore_response = http_get(SINGAPORE_INTERNSHIP_INDEX_URL)
        parsed_singapore_jobs = parse_singapore_internships(singapore_response.text)
        stats.fetched = len(parsed_singapore_jobs)
        jobs = find_recent_singapore_quant_jobs(
            quant_response.text,
            singapore_response.text,
        )
        if not jobs:
            print("⚠️ No recent Singapore quant internships found.")
            return stats

        if not DRY_RUN:
            conn = get_db_connection()
        for job in jobs:
            url_hash = hashlib.sha256(job["job_url"].encode()).hexdigest()[:24]
            unique_id = f"sg_quant_{url_hash}"
            job["source_id"] = "sg_quant"
            job["source_label"] = "Singapore Quant Internship Index"
            observe_job(
                conn,
                unique_id,
                "sg_quant",
                url_hash,
                job,
                stats,
            )
    except Exception as e:
        print(f"❌ Singapore Quant Pipeline Error: {e}")
        stats.errors.append(str(e))
    finally:
        if conn is not None:
            conn.close()
    return stats


def print_run_summary(stats_list: list[PipelineStats]) -> None:
    print("\n📊 Pipeline summary")
    for stats in stats_list:
        print(
            f"- {stats.source}: fetched={stats.fetched}, "
            f"matched={stats.matched}, queued={stats.queued}, "
            f"duplicates={stats.duplicates}, warnings={len(stats.warnings)}, "
            f"errors={len(stats.errors)}"
        )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Singapore internship scraper")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and filter jobs without database writes or Telegram messages",
    )
    mode.add_argument(
        "--canary",
        action="store_true",
        help="Send exactly one labeled alert through the database delivery queue",
    )
    return parser.parse_args(argv)


def run_canary() -> int:
    timestamp = datetime.datetime.now(datetime.timezone.utc)
    canary_id = f"system_canary_{timestamp.strftime('%Y%m%dT%H%M%S%fZ')}"
    canary_job = {
        "site": "System Canary",
        "message_type": "canary",
        "title": f"Delivery canary {timestamp.isoformat()}",
        "company": "sg-internship-scraper",
        "job_url": "https://github.com/stinkray77/sg-internship-scraper",
        "location": "Singapore",
        "date_posted": timestamp,
    }
    stats = PipelineStats("System Canary")
    conn = get_db_connection()
    try:
        if not enqueue_job(conn, canary_id, canary_job, stats):
            print("❌ Canary could not be queued.")
            return 1
    finally:
        conn.close()

    failures = deliver_pending_jobs(only_job_id=canary_id)
    if failures:
        print("❌ Canary delivery failed and remains queued for retry.")
        return 1

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT delivery_status FROM seen_jobs WHERE job_id = %s",
                (canary_id,),
            )
            row = cur.fetchone()
        finally:
            cur.close()
    finally:
        conn.close()
    if not row or row[0] != "sent":
        print("❌ Canary was not persisted as sent.")
        return 1
    print(f"✅ Canary delivered and persisted as sent: {canary_id}")
    return 0


def main(argv=None) -> int:
    global DRY_RUN
    args = parse_args(argv)
    DRY_RUN = args.dry_run

    if not DRY_RUN:
        missing = [
            name
            for name, value in (
                ("DATABASE_URL", DB_URL),
                ("TELEGRAM_TOKEN", BOT_TOKEN),
                ("TELEGRAM_CHAT_ID", CHAT_ID),
            )
            if not value
        ]
        if missing:
            print(f"❌ Missing required configuration: {', '.join(missing)}")
            return 1
        try:
            init_db()
        except Exception as e:
            print(f"❌ Database initialization failed: {e}")
            return 1

    if args.canary:
        return run_canary()

    pipelines = [
        scrape_registry_pipelines,
        scrape_singapore_quant_pipeline,
        scrape_internsg_pipeline,
        run_pipeline,
    ]
    stats_list = []
    for pipeline in pipelines:
        try:
            pipeline_stats = pipeline()
            if isinstance(pipeline_stats, list):
                stats_list.extend(pipeline_stats)
            else:
                stats_list.append(pipeline_stats)
        except Exception as e:
            source = pipeline.__name__
            print(f"❌ {source} failed: {e}")
            stats_list.append(PipelineStats(source=source, errors=[str(e)]))

    delivery_failures = 0
    if not DRY_RUN:
        try:
            delivery_failures = deliver_pending_jobs()
        except Exception as e:
            print(f"❌ Delivery queue failed: {e}")
            delivery_failures = 1

    print_run_summary(stats_list)
    source_errors = sum(len(stats.errors) for stats in stats_list)
    if source_errors or delivery_failures:
        print(
            f"❌ Run completed with {source_errors} source error(s) "
            f"and {delivery_failures} delivery failure(s)."
        )
        return 1

    mode = "dry-run" if DRY_RUN else "live"
    print(f"✅ All data pipelines complete ({mode}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
