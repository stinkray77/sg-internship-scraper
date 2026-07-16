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
MAX_DELIVERY_ATTEMPTS = 5
STALE_DELIVERY_MINUTES = 30
DRY_RUN = False


@dataclass
class DeliveryResult:
    success: bool
    error: str | None = None


@dataclass
class PipelineStats:
    source: str
    fetched: int = 0
    matched: int = 0
    queued: int = 0
    duplicates: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


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
    conn.commit()
    cur.close()
    conn.close()

def send_telegram_alert(job) -> DeliveryResult:
    site = job.get('site', 'Unknown')
    title = job.get('title', 'No Title')
    company = job.get('company', 'No Company')
    url = job.get('job_url', '#')
    date_posted = job.get('date_posted') or 'Recent'
    message_type = job.get("message_type", "job")
    heading = "[CANARY] Scraper delivery check" if message_type == "canary" else f"Internship ({site})"
    
    msg = (
        f"🇸🇬 <b>{html.escape(str(heading))}</b>\n\n"
        f"🏢 <b>{html.escape(str(company))}</b>\n"
        f"👨‍💻 {html.escape(str(title))}\n"
        f"📅 <b>Posted:</b> {html.escape(str(date_posted))}\n"
        f"🔗 <a href=\"{html.escape(str(url), quote=True)}\">Apply Here</a>"
    )
    
    # Check if credentials exist before trying to send
    import os
    bot_token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not bot_token or not chat_id:
        error = "Telegram credentials not found in environment variables"
        print(f"❌ CRITICAL: {error}.")
        return DeliveryResult(False, error)

    tg_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}
    
    try:
        response = requests.post(
            tg_url,
            json=payload,
            timeout=TELEGRAM_TIMEOUT_SECONDS,
        )
        if response.status_code != 200:
            error = f"HTTP {response.status_code}: {response.text[:500]}"
            print(f"❌ TELEGRAM API ERROR: {error}")
            return DeliveryResult(False, error)
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
) -> bool:
    """Persist a candidate for delivery, or print it during dry-run."""
    job = normalize_job_payload(job)
    stats.matched += 1
    if DRY_RUN:
        print(f"🧪 Would enqueue: {job['title']} at {job['company']}")
        stats.queued += 1
        return True

    dedupe_key = make_dedupe_key(job)
    cur = conn.cursor()
    try:
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
        conn.commit()
        if queued:
            stats.queued += 1
        else:
            stats.duplicates += 1
        return queued
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
    """Reject roles that explicitly require PhD study, not inclusive degree lists."""
    title_lower = str(title or "").lower()
    if re.search(r"\bph\.?d\b", title_lower):
        return True

    description_text = BeautifulSoup(
        str(description or ""),
        "html.parser",
    ).get_text(" ", strip=True).lower()
    if not re.search(r"\bph\.?d\b|doctoral", description_text):
        return False
    inclusive_degree = re.search(
        r"\b(bachelor|undergraduate|b\.?s\.?|master|m\.?s\.?)\b.{0,80}"
        r"\b(ph\.?d|doctoral)\b|"
        r"\b(ph\.?d|doctoral)\b.{0,80}"
        r"\b(bachelor|undergraduate|b\.?s\.?|master|m\.?s\.?)\b",
        description_text,
    )
    if inclusive_degree:
        return False
    return bool(re.search(
        r"\b(ph\.?d candidates? only|doctoral candidates? only|"
        r"must be (?:currently )?pursuing (?:a )?ph\.?d|"
        r"currently pursuing (?:a )?ph\.?d|ph\.?d students? only)\b",
        description_text,
    ))


def is_undergrad_technical_job(job: dict) -> bool:
    return (
        is_target_role(job.get("title", ""))
        and not is_explicitly_phd_only(
            job.get("title", ""),
            job.get("description", ""),
        )
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

            if not is_undergrad_technical_job(job_data):
                print(f"🗑️ Filtered out non-target role: {title} at {company}")
                continue

            if not is_singapore_job(job_data):
                print(f"🌏 Filtered out non-Singapore job: {title} at {company}")
                continue

            raw_id = str(row['id'])
            site = row['site']
            unique_id = f"{site}_{raw_id}"
            enqueue_job(conn, unique_id, job_data, stats)

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
            if result.error:
                print(f"⚠️ {source['id']} source error: {result.error}")
                stats.errors.append(result.error)
                stats_list.append(stats)
                continue

            for candidate in result.candidates:
                job_data = candidate.to_payload()
                if not is_undergrad_technical_job(job_data):
                    continue
                if not is_singapore_job(job_data):
                    continue

                # Descriptions are used only for eligibility filtering. Keeping full
                # HTML in the queue wastes database space and leaks irrelevant copy.
                job_data.pop("description", None)
                unique_id = f"{source['id']}_{candidate.external_id}"
                enqueue_job(conn, unique_id, job_data, stats)
            stats_list.append(stats)
    finally:
        if conn is not None:
            conn.close()
    return stats_list

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
                raw_id = canonical_url.split('/')[-1]
                unique_id = f"internsg_{raw_id}"
                enqueue_job(conn, unique_id, job_data, stats)

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
            enqueue_job(conn, unique_id, job, stats)
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
