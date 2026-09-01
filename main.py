import os
import argparse
import datetime
import hashlib
import html
import json
import logging
import requests
import re
import sys
import threading
import time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from functools import lru_cache
import psycopg2
import pandas as pd
from psycopg2.extras import Json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from jobspy import scrape_jobs
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from source_adapters import fetch_source, load_source_registry
from eligibility import EligibilityAssessment, assess_eligibility
from categories import CATEGORY_ORDER, classify_job_categories, primary_category

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
JOB_SEARCH_SHARDS = (
    "(software OR developer OR backend OR frontend OR fullstack OR firmware OR systems) AND (intern OR internship)",
    "(data OR AI OR machine learning OR reinforcement learning OR security OR cloud OR infrastructure) AND (intern OR internship)",
    "(quant OR quantitative OR trading OR trader OR technology OR IT) AND (intern OR internship)",
)
JOBSPY_SITES = ("linkedin", "indeed", "glassdoor")
SINGAPORE_INDEX_SOURCE_ID = "sg_tech_index"
GLOBAL_QUANT_INDEX_SOURCE_ID = "global_quant_index"
SOURCE_COVERAGE_VERSION = 1
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


def bounded_env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw_value = os.environ.get(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer") from error
    if not minimum <= value <= maximum:
        raise RuntimeError(f"{name} must be between {minimum} and {maximum}")
    return value


JOB_LOOKBACK_HOURS = positive_env_int("JOB_LOOKBACK_HOURS", 72)
RESULTS_PER_SOURCE = positive_env_int("RESULTS_PER_SOURCE", 50)
DELIVERY_TIMEZONE_NAME = os.environ.get("DELIVERY_TIMEZONE", "Asia/Singapore")
try:
    DELIVERY_TIMEZONE = ZoneInfo(DELIVERY_TIMEZONE_NAME)
except ZoneInfoNotFoundError as error:
    raise RuntimeError(
        f"DELIVERY_TIMEZONE is not a known timezone: {DELIVERY_TIMEZONE_NAME}"
    ) from error
QUIET_HOURS_START = bounded_env_int("QUIET_HOURS_START", 0, 0, 23)
QUIET_HOURS_END = bounded_env_int("QUIET_HOURS_END", 8, 0, 23)
DIGEST_MAX_JOBS = bounded_env_int("DIGEST_MAX_JOBS", 8, 1, 8)
HTTP_TIMEOUT_SECONDS = 20
TELEGRAM_TIMEOUT_SECONDS = 15
MAX_TELEGRAM_RATE_LIMIT_RETRIES = 3
TELEGRAM_RETRY_BUFFER_SECONDS = 1
MAX_DELIVERY_ATTEMPTS = 5
STALE_DELIVERY_MINUTES = 30
CLOSE_AFTER_SUCCESSFUL_MISSES = 3
DRY_RUN = False


def is_quiet_hours(now: datetime.datetime | None = None) -> bool:
    """Return whether a timestamp falls inside the configured local quiet window."""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    local_hour = now.astimezone(DELIVERY_TIMEZONE).hour
    if QUIET_HOURS_START == QUIET_HOURS_END:
        return False
    if QUIET_HOURS_START < QUIET_HOURS_END:
        return QUIET_HOURS_START <= local_hour < QUIET_HOURS_END
    return local_hour >= QUIET_HOURS_START or local_hour < QUIET_HOURS_END


def delivery_policy_for_time(
    now: datetime.datetime | None = None,
) -> tuple[str, datetime.datetime]:
    """Choose immediate delivery or the next local quiet-hours endpoint."""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if not is_quiet_hours(now):
        return "immediate", now

    local_now = now.astimezone(DELIVERY_TIMEZONE)
    due_date = local_now.date()
    if QUIET_HOURS_START > QUIET_HOURS_END and local_now.hour >= QUIET_HOURS_START:
        due_date += datetime.timedelta(days=1)
    due_local = datetime.datetime.combine(
        due_date,
        datetime.time(hour=QUIET_HOURS_END),
        tzinfo=DELIVERY_TIMEZONE,
    )
    return "digest", due_local.astimezone(datetime.timezone.utc)


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
    filtered_role: int = 0
    filtered_location: int = 0
    filtered_eligibility: int = 0
    inferred_location: int = 0
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
            last_error TEXT,
            delivery_mode TEXT NOT NULL DEFAULT 'immediate'
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
        "ALTER TABLE seen_jobs ADD COLUMN IF NOT EXISTS delivery_mode TEXT NOT NULL DEFAULT 'immediate'",
    ]
    for migration in migrations:
        cur.execute(migration)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS seen_jobs_delivery_mode_queue_idx
        ON seen_jobs (delivery_mode, delivery_status, next_attempt_at);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS seen_jobs_recent_dedupe_idx
        ON seen_jobs (dedupe_key, created_at);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS seen_jobs_recent_url_idx
        ON seen_jobs ((payload->>'job_url'), created_at);
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
        CREATE TABLE IF NOT EXISTS source_sync_state (
            scope_id TEXT PRIMARY KEY,
            coverage_version INTEGER NOT NULL,
            last_success_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_fetched_count INTEGER NOT NULL DEFAULT 0
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
        (
            "🏷 <b>Categories:</b> "
            + " ".join(
                f"[{_message_value(category, 20)}]"
                for category in (job.get("categories") or ["TECH"])
            )
        ),
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
        ("Relocation", eligibility.get("relocation_support")),
    )
    for label, value in optional_eligibility:
        if value:
            lines.append(f"   • <b>{label}:</b> {_message_value(value, 250)}")
    if job.get("is_overseas_quant"):
        if not eligibility.get("work_authorization"):
            lines.append("   • <b>Work rights:</b> Not stated; verify posting")
        if not eligibility.get("relocation_support"):
            lines.append("   • <b>Relocation:</b> Not stated; verify posting")
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


def send_telegram_message(message: str) -> DeliveryResult:
    # Check if credentials exist before trying to send
    bot_token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not bot_token or not chat_id:
        error = "Telegram credentials not found in environment variables"
        print(f"❌ CRITICAL: {error}.")
        return DeliveryResult(False, error)

    tg_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
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


def send_telegram_alert(job) -> DeliveryResult:
    return send_telegram_message(build_telegram_message(job))


def make_dedupe_key(job: dict) -> str:
    company = canonical_company_name(
        str(job.get("company_key") or job.get("company", ""))
    )
    title = re.sub(r"[^a-z0-9]", "", str(job.get("title", "")).lower())
    location_scope = ""
    if job.get("is_overseas_quant"):
        location_scope = re.sub(
            r"[^a-z0-9]",
            "",
            _format_location(job.get("location")).lower(),
        )
    return hashlib.sha256(
        f"{company}:{title}:{location_scope}".encode()
    ).hexdigest()


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
    if payload.get("job_url"):
        payload["job_url"] = canonicalize_job_url(payload["job_url"])
    payload["categories"] = classify_job_categories(payload)
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
    delivery_mode: str | None = None,
    now: datetime.datetime | None = None,
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
    scheduled_mode, due_at = delivery_policy_for_time(now)
    delivery_mode = delivery_mode or scheduled_mode
    if delivery_mode not in {"immediate", "digest"}:
        raise ValueError("delivery_mode must be 'immediate' or 'digest'")
    if delivery_mode == "immediate":
        due_at = now or datetime.datetime.now(datetime.timezone.utc)
    cur = conn.cursor()
    try:
        if not bypass_recent_dedupe:
            cur.execute(
                """
                SELECT job_id
                FROM seen_jobs
                WHERE (dedupe_key = %s OR payload->>'job_url' = %s)
                  AND delivery_status IN ('pending', 'sending', 'failed', 'sent')
                  AND created_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
                LIMIT 1
                """,
                (dedupe_key, job.get("job_url")),
            )
            if cur.fetchone() is not None:
                stats.duplicates += 1
                return False

        cur.execute(
            """
            INSERT INTO seen_jobs (
                job_id, company, title, site, payload, dedupe_key,
                delivery_status, attempt_count, next_attempt_at, delivery_mode
            )
            VALUES (%s, %s, %s, %s, %s, %s, 'pending', 0, %s, %s)
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
                due_at,
                delivery_mode,
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
        "categories": job.get("categories"),
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
    *,
    baseline: bool = False,
) -> LifecycleOutcome:
    """Upsert lifecycle state and queue only new or reopened opportunities."""
    assessment = assessment or assess_eligibility(
        job.get("title", ""),
        job.get("description", ""),
    )
    if assessment.verdict == "ineligible":
        return LifecycleOutcome("excluded")

    stats.matched += 1
    job = dict(job)
    if is_known_quant_company(job.get("company")):
        job["source_tags"] = list({*(job.get("source_tags") or []), "QUANT"})
    payload = normalize_job_payload(job)
    payload["source_id"] = source_id
    payload["eligibility"] = assessment.to_payload()
    payload.pop("description", None)
    fingerprint = make_content_fingerprint(payload)

    if DRY_RUN:
        if baseline:
            print(f"🧪 Would backfill silently: {payload['title']} at {payload['company']}")
            return LifecycleOutcome("backfilled")
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
            version = 0 if baseline else 1
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
            event_type = "backfilled" if baseline else "new"
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

        if event_type in {"new", "reopened"} and not baseline:
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


def source_baseline_pending(
    conn,
    scope_id: str,
    coverage_version: int = SOURCE_COVERAGE_VERSION,
) -> bool:
    """Return whether a newly expanded source still needs a silent baseline."""
    if DRY_RUN:
        return True
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT coverage_version FROM source_sync_state WHERE scope_id = %s",
            (scope_id,),
        )
        row = cur.fetchone()
        return row is None or row[0] < coverage_version
    finally:
        cur.close()


def complete_source_baseline(
    conn,
    scope_id: str,
    fetched_count: int,
    coverage_version: int = SOURCE_COVERAGE_VERSION,
) -> None:
    if DRY_RUN:
        return
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO source_sync_state (
                scope_id, coverage_version, last_success_at, last_fetched_count
            )
            VALUES (%s, %s, CURRENT_TIMESTAMP, %s)
            ON CONFLICT (scope_id) DO UPDATE
            SET coverage_version = EXCLUDED.coverage_version,
                last_success_at = EXCLUDED.last_success_at,
                last_fetched_count = EXCLUDED.last_fetched_count
            """,
            (scope_id, coverage_version, fetched_count),
        )
        conn.commit()
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
                  AND delivery_mode = 'immediate'
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


def claim_due_digest(conn, limit: int = DIGEST_MAX_JOBS) -> list[tuple]:
    """Atomically claim one bounded batch of due digest rows."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH candidates AS (
                SELECT job_id
                FROM seen_jobs
                WHERE attempt_count < %s
                  AND delivery_mode = 'digest'
                  AND (
                      (delivery_status IN ('pending', 'failed')
                       AND COALESCE(next_attempt_at, CURRENT_TIMESTAMP) <= CURRENT_TIMESTAMP)
                      OR
                      (delivery_status = 'sending'
                       AND last_attempt_at < CURRENT_TIMESTAMP - (%s * INTERVAL '1 minute'))
                  )
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            )
            UPDATE seen_jobs AS jobs
            SET delivery_status = 'sending',
                attempt_count = jobs.attempt_count + 1,
                last_attempt_at = CURRENT_TIMESTAMP,
                last_error = NULL
            FROM candidates
            WHERE jobs.job_id = candidates.job_id
            RETURNING jobs.job_id, jobs.payload, jobs.attempt_count, jobs.created_at
            """,
            (MAX_DELIVERY_ATTEMPTS, STALE_DELIVERY_MINUTES, limit),
        )
        claimed = list(cur.fetchall())
        conn.commit()
        return sorted(claimed, key=lambda row: (row[3], row[0]))
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


def complete_deliveries(conn, job_ids: list[str]) -> None:
    if not job_ids:
        return
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE seen_jobs
            SET delivery_status = 'sent',
                sent_at = CURRENT_TIMESTAMP,
                next_attempt_at = NULL,
                last_error = NULL
            WHERE job_id = ANY(%s::text[])
            """,
            (job_ids,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
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


def build_digest_message(jobs: list[dict]) -> str:
    """Render each job once under its first ordered category."""
    grouped = {category: [] for category in CATEGORY_ORDER}
    for job in jobs:
        grouped[primary_category(job)].append(job)

    lines = [
        "🌙 <b>OVERNIGHT INTERNSHIP DIGEST</b>",
        "",
        f"{len(jobs)} new role{'s' if len(jobs) != 1 else ''} found during quiet hours.",
    ]
    item_number = 0
    for category in CATEGORY_ORDER:
        category_jobs = grouped[category]
        if not category_jobs:
            continue
        lines.extend(["", f"<b>{_message_value(category, 20)}</b>"])
        for job in category_jobs:
            item_number += 1
            categories = " ".join(
                f"[{_message_value(tag, 20)}]"
                for tag in (job.get("categories") or ["TECH"])
            )
            reopened = "🔁 " if job.get("lifecycle_event") == "reopened" else ""
            url = html.escape(str(job.get("job_url") or "#")[:1000], quote=True)
            lines.extend([
                (
                    f"{item_number}. {reopened}<b>"
                    f"{_message_value(job.get('company') or 'Unknown Company', 80)}</b>"
                ),
                _message_value(job.get("title") or "Untitled Role", 140),
                (
                    f"{categories} · "
                    f"{_message_value(_format_location(job.get('location')), 80)}"
                ),
                f'<a href="{url}">View and apply</a>',
            ])

    message = "\n".join(lines)
    if len(BeautifulSoup(message, "html.parser").get_text()) > 3500:
        raise ValueError("Telegram digest exceeds the 3500-character safety limit")
    return message


def _send_with_rate_limit(send, label: str) -> DeliveryResult:
    result = send()
    rate_limit_retries = 0
    while (
        not result.success
        and result.retry_after_seconds is not None
        and rate_limit_retries < MAX_TELEGRAM_RATE_LIMIT_RETRIES
    ):
        wait_seconds = result.retry_after_seconds + TELEGRAM_RETRY_BUFFER_SECONDS
        rate_limit_retries += 1
        print(
            f"⏳ Telegram rate limit for {label}; waiting {wait_seconds}s before "
            f"retry {rate_limit_retries}/{MAX_TELEGRAM_RATE_LIMIT_RETRIES}."
        )
        time.sleep(wait_seconds)
        result = send()
    return result


def deliver_pending_jobs(only_job_id: str | None = None) -> int:
    """Deliver all currently due jobs and return the number of failures."""
    if DRY_RUN:
        return 0

    failures = 0
    conn = get_db_connection()
    try:
        if only_job_id is None:
            while True:
                digest_rows = claim_due_digest(conn)
                if not digest_rows:
                    break
                job_ids = [row[0] for row in digest_rows]
                payloads = [row[1] for row in digest_rows]
                result = _send_with_rate_limit(
                    lambda: send_telegram_message(build_digest_message(payloads)),
                    f"digest ({len(job_ids)} jobs)",
                )
                if result.success:
                    complete_deliveries(conn, job_ids)
                    continue
                failures += len(digest_rows)
                for job_id, _payload, attempt_count, _created_at in digest_rows:
                    status = fail_delivery(
                        conn,
                        job_id,
                        attempt_count,
                        result.error or "Unknown Telegram digest failure",
                    )
                    print(f"⚠️ Digest delivery {job_id} marked {status}.")

        while True:
            claimed = claim_due_delivery(conn, only_job_id=only_job_id)
            if claimed is None:
                break

            job_id, payload, attempt_count = claimed
            result = _send_with_rate_limit(
                lambda: send_telegram_alert(payload),
                job_id,
            )
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
    
    strong_technical_pattern = (
        r"\b(software|swe|developer|programmer|machine learning|ml|backend|"
        r"frontend|fullstack|cloud|systems|platform|infrastructure|devops|"
        r"security|cybersecurity|computer vision|nlp|ai|genai|llm|blockchain|"
        r"reinforcement learning|robotics|firmware|embedded|database|automation|"
        r"information systems?|information technology|business intelligence|"
        r"enterprise architecture|solutions? architect|mobile|web|npu|gpu|"
        r"research scientist|model efficiency|quality engineering)\b|"
        r"\bdata\s+(engineer|engineering|scientist|analyst)\b"
    )
    has_strong_technical = bool(re.search(strong_technical_pattern, title_lower))

    hard_blacklist_pattern = (
        r"\b(hr|human resources|accounting|retail|product management|"
        r"product manager|project management|graphic design|product design|"
        r"industrial engineering|civil engineering|mechanical engineering|"
        r"chemical engineering)\b"
    )
    if re.search(hard_blacklist_pattern, title_lower):
        return False
    if re.search(r"\belectrical\b", title_lower) and not has_strong_technical:
        return False
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
        r"cybersecurity|nlp|blockchain|fintech|reinforcement learning|robotics|"
        r"firmware|embedded|database|automation|information systems?|"
        r"information technology|business intelligence|enterprise architecture|"
        r"solutions? architect|mobile|web|npu|gpu|research scientist|"
        r"model efficiency|quality engineering)\b|"
        r"\brisk\s+(model|modelling|modeling|analytics|"
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
GENERIC_LOCATION_PATTERN = re.compile(
    r"^\s*(?:remote|hybrid|onsite|on-site|apac|asia(?: pacific)?|worldwide|global)\s*$",
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


def infer_singapore_from_search(job: dict) -> bool:
    """Fill absent or generic metadata from an explicitly Singapore-scoped query."""
    if is_singapore_job(job):
        return False
    values = [job.get("country"), job.get("location")]
    flattened = []
    for value in values:
        if isinstance(value, (list, tuple, set)):
            flattened.extend(str(item).strip() for item in value if item)
        elif value:
            flattened.append(str(value).strip())
    if flattened and any(not GENERIC_LOCATION_PATTERN.fullmatch(value) for value in flattened):
        return False
    job["location"] = "[Inference] Singapore, from search scope"
    job["location_inferred"] = True
    return True


def normalize_company_name(name: str) -> str:
    """Normalize a company name for matching across independent indexes."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def company_names_match(first: object, second: object) -> bool:
    first_normalized = normalize_company_name(str(first or ""))
    second_normalized = normalize_company_name(str(second or ""))
    if not first_normalized or not second_normalized:
        return False
    if first_normalized == second_normalized:
        return True
    return min(len(first_normalized), len(second_normalized)) >= 5 and (
        first_normalized in second_normalized
        or second_normalized in first_normalized
    )


@lru_cache(maxsize=1)
def _known_quant_company_names() -> tuple[str, ...]:
    names = []
    for source in load_source_registry():
        if "QUANT" not in source.get("tags", []):
            continue
        names.extend([source["company"], *source.get("aliases", [])])
    return tuple(normalize_company_name(name) for name in names)


@lru_cache(maxsize=512)
def canonical_company_name(name: str) -> str:
    normalized = normalize_company_name(name)
    for source in load_source_registry():
        candidates = {
            normalize_company_name(candidate)
            for candidate in [source["company"], *source.get("aliases", [])]
        }
        if normalized in candidates:
            return normalize_company_name(
                source.get("dedupe_company") or source["company"]
            )
    return normalized


def is_known_quant_company(company: object) -> bool:
    normalized = normalize_company_name(str(company or ""))
    if not normalized:
        return False
    for known in _known_quant_company_names():
        if company_names_match(normalized, known):
            return True
    return False


def parse_quant_firms(markdown: str) -> set[str]:
    """Extract normalized firm names from the quant internship index."""
    firm_names = re.findall(
        r"^## (.+)\n\*\*Website\*\*:",
        markdown,
        flags=re.MULTILINE,
    )
    return {normalize_company_name(name) for name in firm_names}


def canonicalize_job_url(url: object) -> str:
    raw = html.unescape(str(url or "")).strip()
    if not raw:
        return ""
    parts = urlsplit(raw)
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if not key.lower().startswith("utm_")
        and key.lower() not in {"ref", "refid", "source", "trk"}
    ]
    return urlunsplit((
        parts.scheme,
        parts.netloc.lower(),
        parts.path.rstrip("/"),
        urlencode(query),
        "",
    ))


def parse_singapore_internships(markdown: str) -> list[dict]:
    """Extract jobs from the verified Singapore internship Markdown table."""
    jobs = []

    for line in markdown.splitlines():
        if not line.startswith("| ["):
            continue

        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 5:
            continue

        company_match = re.match(r"\[([^]]+)\]\([^)]+\)", cells[0])
        tracking_match = re.search(r'href="([^"]+)"', cells[-3])
        application_match = re.search(r'href="([^"]+)"', cells[-2])
        if not company_match or not application_match:
            continue

        try:
            date_added = datetime.datetime.strptime(
                cells[-1].replace("Sept ", "Sep "),
                "%d %b %Y",
            ).date()
        except ValueError:
            continue

        company = company_match.group(1)
        title = " | ".join(cells[1:-3]).replace("\\ |", "|").strip()
        job_url = canonicalize_job_url(html.unescape(application_match.group(1)))
        tracking_url = (
            html.unescape(tracking_match.group(1)) if tracking_match else None
        )
        tracking_id_match = re.search(r"/job/([a-z0-9-]+)", tracking_url or "", re.I)
        external_id = (
            tracking_id_match.group(1)
            if tracking_id_match
            else hashlib.sha256(
                f"{normalize_company_name(company)}:{title.lower()}:{job_url}".encode()
            ).hexdigest()[:24]
        )

        jobs.append({
            "external_id": external_id,
            "company": company,
            "title": title,
            "job_url": job_url,
            "tracking_url": tracking_url,
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
        if not any(company_names_match(job["company"], firm) for firm in quant_firms):
            continue

        job_data = {
            "site": "SG Quant Index",
            "title": job["title"],
            "company": job["company"],
            "job_url": job["job_url"],
            "date_posted": job["date_added"],
            "location": "Singapore",
            "source_tags": ["QUANT"],
        }
        if is_quant_intern_role(job_data["title"]) and is_singapore_job(job_data):
            matching_jobs.append(job_data)

    return matching_jobs


QUANT_ROLE_LABELS = {
    "swe": "Software Engineering Internship",
    "qd": "Quantitative Developer Internship",
    "qr": "Quantitative Research Internship",
    "qt": "Quantitative Trading Internship",
    "trading": "Trading Internship",
    "devops/sre": "DevOps / Site Reliability Internship",
    "hw": "Hardware Engineering Internship",
    "data": "Data Internship",
}


def parse_global_quant_internships(markdown: str) -> list[dict]:
    """Parse active application links from the global quant internship index."""
    jobs = []
    company = None
    location = None
    for raw_line in markdown.splitlines():
        line = raw_line.strip()
        if line.startswith("## "):
            company = line[3:].strip()
            location = None
            continue
        if line.startswith("**Locations**:"):
            location = line.split(":", 1)[1].strip() or None
            continue
        if not company or not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|", 1)]
        if len(cells) != 2 or cells[0].lower() in {"role", "-------"}:
            continue
        role_code, links_cell = cells
        for label, raw_url in re.findall(
            r"\[([^]]*)\]\((https?://[^)]+)\)",
            links_cell,
        ):
            clean_label = re.sub(
                r"[^a-z0-9+#. -]",
                "",
                label,
                flags=re.IGNORECASE,
            ).strip()
            if re.search(r"\b(new grad|full[ -]?time|graduate)\b", clean_label, re.I):
                continue
            job_url = canonicalize_job_url(raw_url)
            title = QUANT_ROLE_LABELS.get(
                role_code.lower(),
                f"{role_code} Internship",
            )
            if clean_label:
                title = f"{title} ({clean_label})"
            external_id = hashlib.sha256(
                f"{normalize_company_name(company)}:{role_code.lower()}:{job_url}".encode()
            ).hexdigest()[:24]
            jobs.append({
                "external_id": external_id,
                "site": "Global Quant Internship Index",
                "source_id": GLOBAL_QUANT_INDEX_SOURCE_ID,
                "source_label": "Global Quant Internship Index",
                "source_tags": ["QUANT"],
                "company": company,
                "title": title,
                "job_url": job_url,
                "location": (
                    f"[Unverified, firm-level] {location}"
                    if location
                    else "[Unverified] Location not listed"
                ),
                "location_confidence": "firm_index_unverified",
                "is_overseas_quant": not _contains_singapore_location(location),
            })
    return jobs


class _JobSpyErrorCollector(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.ERROR)
        self.messages = []

    def emit(self, record):
        self.messages.append((record.name, record.getMessage()))


def _present(value: object) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    return bool(str(value).strip())


def _job_richness(job: dict) -> tuple[int, int, int, int]:
    return (
        int(is_singapore_job(job)),
        int(_present(job.get("job_url_direct"))),
        len(str(job.get("description") or "")),
        int(_present(job.get("date_posted"))),
    )


def merge_jobspy_results(frames: list[pd.DataFrame]) -> list[dict]:
    """Merge repeated query and cross-site copies before applying filters."""
    exact = {}
    for frame in frames:
        for _, row in frame.iterrows():
            job = json_safe(row.to_dict())
            key = (
                str(job.get("site") or ""),
                str(job.get("id") or job.get("job_url") or ""),
            )
            existing = exact.get(key)
            if existing is None or _job_richness(job) > _job_richness(existing):
                exact[key] = job

    merged = {}
    for job in exact.values():
        key = (
            canonical_company_name(str(job.get("company") or "")),
            re.sub(r"[^a-z0-9]", "", str(job.get("title") or "").lower()),
        )
        existing = merged.get(key)
        if existing is None:
            candidate = dict(job)
            candidate["discovery_sites"] = [str(job.get("site") or "JobSpy")]
            merged[key] = candidate
            continue

        preferred, other = (
            (job, existing)
            if _job_richness(job) > _job_richness(existing)
            else (existing, job)
        )
        combined = dict(preferred)
        for field in ("location", "country", "description", "date_posted", "job_url_direct"):
            if not _present(combined.get(field)) and _present(other.get(field)):
                combined[field] = other[field]
        sites = {
            *(existing.get("discovery_sites") or []),
            str(job.get("site") or "JobSpy"),
        }
        combined["discovery_sites"] = sorted(sites)
        merged[key] = combined

    jobs = []
    for job in merged.values():
        direct_url = job.get("job_url_direct")
        if _present(direct_url):
            job["job_url"] = direct_url
        job["job_url"] = canonicalize_job_url(job.get("job_url"))
        jobs.append(job)
    return jobs


def run_pipeline():
    print("🚀 Running sharded JobSpy pipeline for SG...")
    stats = PipelineStats("JobSpy")
    conn = None

    try:
        collector = _JobSpyErrorCollector()
        loggers = [logging.getLogger(f"JobSpy:{site.title()}") for site in JOBSPY_SITES]
        for logger in loggers:
            logger.addHandler(collector)
        frames = []
        failed_shards = 0
        try:
            for search_term in JOB_SEARCH_SHARDS:
                try:
                    frame = scrape_jobs(
                        site_name=list(JOBSPY_SITES),
                        search_term=search_term,
                        location="Singapore",
                        results_wanted=RESULTS_PER_SOURCE,
                        hours_old=JOB_LOOKBACK_HOURS,
                        country_indeed="Singapore",
                    )
                except Exception as error:
                    failed_shards += 1
                    stats.warnings.append(f"query shard failed: {error}")
                    continue
                if frame is None or frame.empty:
                    stats.warnings.append(f"query shard returned zero rows: {search_term}")
                    continue
                frames.append(frame)
                stats.fetched += len(frame)
                counts = frame["site"].value_counts().to_dict()
                for site in JOBSPY_SITES:
                    if counts.get(site, 0) >= RESULTS_PER_SOURCE:
                        stats.warnings.append(
                            f"{site} query shard saturated at {RESULTS_PER_SOURCE}: {search_term}"
                        )
        finally:
            for logger in loggers:
                logger.removeHandler(collector)

        for logger_name, message in collector.messages:
            warning = f"{logger_name}: {message}"
            if warning not in stats.warnings:
                stats.warnings.append(warning)
        for warning in stats.warnings:
            print(f"⚠️ JobSpy warning: {warning}")

        if not frames:
            print("⚠️ No results found.")
            if failed_shards == len(JOB_SEARCH_SHARDS) or collector.messages:
                stats.errors.append("all JobSpy query shards failed or returned no results")
            return stats

        jobs = merge_jobspy_results(frames)
        if not DRY_RUN:
            conn = get_db_connection()

        for job_data in jobs:
            title = job_data.get("title")
            company = job_data.get("company")

            if not is_target_role(job_data.get("title", "")):
                print(f"🗑️ Filtered out non-target role: {title} at {company}")
                stats.filtered_role += 1
                continue

            if not is_singapore_job(job_data):
                if infer_singapore_from_search(job_data):
                    stats.inferred_location += 1
                else:
                    print(f"🌏 Filtered out non-Singapore job: {title} at {company}")
                    stats.filtered_location += 1
                    continue

            assessment = assess_eligibility(
                job_data.get("title", ""),
                job_data.get("description", ""),
            )
            if assessment.verdict == "ineligible":
                print(f"🎓 Filtered out postgraduate-only role: {title} at {company}")
                stats.filtered_eligibility += 1
                continue
            sites = job_data.get("discovery_sites") or [job_data.get("site") or "JobSpy"]
            source_label = " + ".join(site.title() for site in sites) + " job listing"
            job_data["source_id"] = "jobspy"
            job_data["source_label"] = source_label
            raw_id = make_dedupe_key(job_data)[:24]
            unique_id = f"jobspy_{raw_id}"
            observe_job(
                conn,
                unique_id,
                "jobspy",
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
    registry = load_source_registry()
    sources = [
        source
        for source in registry
        if source.get("enabled") and source.get("mode", "direct") == "direct"
    ]
    discovery_only_sources = [
        source
        for source in registry
        if source.get("enabled") and source.get("mode") == "discovery_only"
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

            if source["adapter"] == "bespoke" and result.fetched == 0:
                warning = "bespoke source returned zero candidates"
                stats.warnings.append(warning)
                print(f"⚠️ {source['id']} {warning}")

            observed_ids = set()
            is_global_quant_source = "QUANT" in source.get("tags", [])
            baseline_scope = f"global_quant_official:{source['id']}"
            baseline = (
                source_baseline_pending(conn, baseline_scope)
                if is_global_quant_source
                else False
            )
            for candidate in result.candidates:
                job_data = candidate.to_payload()
                role_match = (
                    is_quant_intern_role(job_data.get("title", ""))
                    if is_global_quant_source
                    else is_target_role(job_data.get("title", ""))
                )
                if not role_match:
                    stats.filtered_role += 1
                    continue
                if not is_global_quant_source and not is_singapore_job(job_data):
                    stats.filtered_location += 1
                    continue
                if is_global_quant_source:
                    job_data["is_overseas_quant"] = not is_singapore_job(job_data)
                assessment = assess_eligibility(
                    job_data.get("title", ""),
                    job_data.get("description", ""),
                )
                if assessment.verdict == "ineligible":
                    stats.filtered_eligibility += 1
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
                    baseline=baseline,
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
            if not DRY_RUN and is_global_quant_source and baseline:
                complete_source_baseline(
                    conn,
                    baseline_scope,
                    stats.fetched,
                )
            stats_list.append(stats)
        for source in discovery_only_sources:
            stats_list.append(PipelineStats(
                source["id"],
                warnings=["direct scraping disabled; covered by discovery indexes"],
            ))
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
                    stats.filtered_role += 1
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
                    stats.filtered_location += 1
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
                    stats.filtered_eligibility += 1
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
    print("🚀 Running verified Singapore and global quant index pipelines...")
    singapore_stats = PipelineStats("Singapore Tech Index")
    quant_stats = PipelineStats("Global Quant Index")
    conn = None

    try:
        singapore_response = http_get(SINGAPORE_INTERNSHIP_INDEX_URL)
        singapore_jobs = parse_singapore_internships(singapore_response.text)
        if not singapore_jobs:
            raise ValueError("Singapore tech index returned no parseable jobs")
        singapore_stats.fetched = len(singapore_jobs)

        quant_markdown = ""
        try:
            quant_response = http_get(QUANT_INTERNSHIP_INDEX_URL)
            quant_markdown = quant_response.text
        except Exception as error:
            quant_stats.errors.append(str(error))
            singapore_stats.warnings.append(
                f"quant classification unavailable: {error}"
            )

        quant_firms = parse_quant_firms(quant_markdown) if quant_markdown else set()
        if not DRY_RUN:
            conn = get_db_connection()
        singapore_baseline = source_baseline_pending(
            conn,
            SINGAPORE_INDEX_SOURCE_ID,
        )
        singapore_observed_ids = set()
        for parsed in singapore_jobs:
            assessment = assess_eligibility(parsed["title"], "")
            if assessment.verdict == "ineligible":
                singapore_stats.filtered_eligibility += 1
                continue
            is_quant = is_known_quant_company(parsed["company"]) or any(
                company_names_match(parsed["company"], firm)
                for firm in quant_firms
            )
            job = {
                "site": "Singapore Tech Index",
                "source_id": SINGAPORE_INDEX_SOURCE_ID,
                "source_label": "Verified Singapore Tech Internship Index",
                "source_tags": ["QUANT"] if is_quant else [],
                "company": parsed["company"],
                "title": parsed["title"],
                "job_url": parsed["job_url"],
                "tracking_url": parsed.get("tracking_url"),
                "date_posted": parsed["date_added"],
                "location": "Singapore",
            }
            unique_id = f"{SINGAPORE_INDEX_SOURCE_ID}_{parsed['external_id']}"
            singapore_observed_ids.add(unique_id)
            observe_job(
                conn,
                unique_id,
                SINGAPORE_INDEX_SOURCE_ID,
                parsed["external_id"],
                job,
                singapore_stats,
                assessment,
                baseline=singapore_baseline,
            )

        if not DRY_RUN:
            reconcile_source_snapshot(
                conn,
                SINGAPORE_INDEX_SOURCE_ID,
                singapore_observed_ids,
            )
            if singapore_baseline:
                complete_source_baseline(
                    conn,
                    SINGAPORE_INDEX_SOURCE_ID,
                    singapore_stats.fetched,
                )

        if quant_markdown:
            quant_jobs = parse_global_quant_internships(quant_markdown)
            if not quant_jobs:
                raise ValueError("global quant index returned no parseable jobs")
            quant_stats.fetched = len(quant_jobs)
            quant_baseline = source_baseline_pending(
                conn,
                GLOBAL_QUANT_INDEX_SOURCE_ID,
            )
            quant_observed_ids = set()
            for job in quant_jobs:
                assessment = assess_eligibility(job["title"], "")
                if assessment.verdict == "ineligible":
                    quant_stats.filtered_eligibility += 1
                    continue
                external_id = job.pop("external_id")
                unique_id = f"{GLOBAL_QUANT_INDEX_SOURCE_ID}_{external_id}"
                quant_observed_ids.add(unique_id)
                observe_job(
                    conn,
                    unique_id,
                    GLOBAL_QUANT_INDEX_SOURCE_ID,
                    external_id,
                    job,
                    quant_stats,
                    assessment,
                    baseline=quant_baseline,
                )
            if not DRY_RUN:
                reconcile_source_snapshot(
                    conn,
                    GLOBAL_QUANT_INDEX_SOURCE_ID,
                    quant_observed_ids,
                )
                if quant_baseline:
                    complete_source_baseline(
                        conn,
                        GLOBAL_QUANT_INDEX_SOURCE_ID,
                        quant_stats.fetched,
                    )
    except Exception as e:
        print(f"❌ Internship index pipeline error: {e}")
        singapore_stats.errors.append(str(e))
    finally:
        if conn is not None:
            conn.close()
    return [singapore_stats, quant_stats]


def print_run_summary(stats_list: list[PipelineStats]) -> None:
    print("\n📊 Pipeline summary")
    for stats in stats_list:
        print(
            f"- {stats.source}: fetched={stats.fetched}, "
            f"matched={stats.matched}, queued={stats.queued}, "
            f"duplicates={stats.duplicates}, filtered_role={stats.filtered_role}, "
            f"filtered_location={stats.filtered_location}, "
            f"filtered_eligibility={stats.filtered_eligibility}, "
            f"inferred_location={stats.inferred_location}, "
            f"warnings={len(stats.warnings)}, "
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
        if not enqueue_job(
            conn,
            canary_id,
            canary_job,
            stats,
            delivery_mode="immediate",
        ):
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
