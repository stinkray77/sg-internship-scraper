# sg-internship-scraper

Hourly Singapore internship scraper that finds CS, software engineering, data, AI/ML, and quant internship roles, deduplicates them in PostgreSQL, and sends new matches to Telegram.

## What It Scrapes

The scraper combines broad discovery with configurable official sources:

- JobSpy broad search over LinkedIn, Indeed, and Glassdoor
- InternSG IT internship listings
- Greenhouse, Lever, SmartRecruiters, Workday, and Ashby company boards
- Public career pages for selected finance and trading employers
- A recent-job intersection between the Northwestern FinTech quant internship index and the verified Singapore internship index

Company sources are declared in `sources.json`. Each entry specifies the employer,
adapter, public board configuration, aliases, and whether direct scraping is enabled.
The registry includes trading firms, banks, asset managers, exchanges, fintechs,
and the original technology companies.

The bespoke registry tracks Jane Street, Citadel/Citadel Securities, IMC, Jump,
SIG, Squarepoint, AlphaGrep, Quantedge, Dynamic Technology Lab, and SGX. Sources
that block free HTTP access or require a JavaScript browser are marked
`discovery_only` and remain covered by JobSpy and the GitHub indexes.

## How It Works

1. Load environment variables from `.env` or GitHub Actions secrets.
2. Create or migrate the delivery and lifecycle tables.
3. Fetch official sources with at most four workers, then process database writes sequentially.
4. Filter technical student roles, require an explicit Singapore location, and
   classify description-based eligibility.
5. Record each job observation and lifecycle event in PostgreSQL.
6. Atomically enqueue only new or reopened jobs for Telegram delivery.
7. Reconcile complete official ATS snapshots and close jobs after three clean misses.
8. Deliver due alerts to Telegram and persist success or retry state.
9. Print per-source fetched, matched, queued, duplicate, warning, and error counts.

Title filtering requires a technical keyword and a student-role term. Supported
terms include internship, co-op, industrial attachment, trainee, summer analyst,
off-cycle analyst, winternship, insight programme, apprenticeship, and accelerator
programme. Non-target roles such as sales, marketing, HR, accounting, product or
project management, retail, and design are excluded. Explicit postgraduate-only
roles are rejected, while inclusive bachelor/master/PhD postings remain eligible.
Graduation years, internship duration, and work-authorization language are
extracted for display but do not hide a job.

Location filtering is fail-closed: a posting must explicitly contain `Singapore` or the country code `SG`. Singapore-anchored onsite, hybrid, and remote roles are accepted. Generic remote, APAC, worldwide, and missing locations are rejected.

The quant index pipeline uses one repository to identify quant firms and a separate Singapore-specific repository to identify verified local postings. Only matching target-role internships added in the last 14 days are considered. Official application links from the Singapore index are retained, and recent exact company-title matches are suppressed across sources.

Official open postings are not discarded based on age. On first enabling a board,
each current match alerts once, after which PostgreSQL deduplication suppresses it.
JobSpy retains a 72-hour overlap window and requests 50 results per site by
default. Paginated adapters enforce finite page limits.

Official sources run before aggregators so cross-source deduplication retains the
official application URL when both sources find the same company and title.

## Requirements

- Python 3.10+
- PostgreSQL database
- Telegram bot token
- Telegram chat ID

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Environment Variables

Create a local `.env` file with:

```env
DATABASE_URL=postgresql://user:password@host:port/dbname
TELEGRAM_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_chat_id
JOB_LOOKBACK_HOURS=72
RESULTS_PER_SOURCE=50
DELIVERY_TIMEZONE=Asia/Singapore
QUIET_HOURS_START=0
QUIET_HOURS_END=8
DIGEST_MAX_JOBS=8
```

The first three values are required for live runs. The remaining settings are
optional. The same required names are used as GitHub Actions secrets.

## Running Locally

From the repository root:

```bash
source venv/bin/activate
python main.py
```

Or without activating the virtual environment:

```bash
venv/bin/python main.py
```

Running the script performs live network requests, writes to the configured database, and may send Telegram messages.

To inspect current matches without database or Telegram access:

```bash
venv/bin/python main.py --dry-run
```

Dry-run still performs live read-only requests, but it does not initialize the database, enqueue jobs, retry alerts, or call Telegram.

To validate the real database and Telegram path with exactly one labeled message:

```bash
venv/bin/python main.py --canary
```

Canary mode skips every scraper, enqueues a timestamped `[CANARY]` row, claims
only that row, and verifies that PostgreSQL records it as `sent`. It cannot flush
other pending alerts.

## GitHub Actions

The workflow in `.github/workflows/scraper.yml` runs tests and then runs the scraper hourly:

```yaml
cron: '17 * * * *'
```

It can also be run manually from the GitHub Actions UI through
`workflow_dispatch`. Manual runs offer `dry-run`, `canary`, and `live`, defaulting
to `dry-run`; scheduled runs are always live.
Runs share a concurrency group and have a 30-minute timeout, preventing scheduled and manual executions from overlapping.

The workflow uses standard GitHub-hosted Linux runners and no paid scraping
service. Standard runners are free for public repositories. Database-provider
free-tier limits remain provider-specific.

Required GitHub repository secrets:

- `DATABASE_URL`
- `TELEGRAM_TOKEN`
- `TELEGRAM_CHAT_ID`

Optional delivery configuration:

- `DELIVERY_TIMEZONE` defaults to `Asia/Singapore`.
- `QUIET_HOURS_START` defaults to `0` and is inclusive.
- `QUIET_HOURS_END` defaults to `8` and is exclusive.
- `DIGEST_MAX_JOBS` defaults to `8` and must be between 1 and 8.

Jobs found from 00:00 through 07:59 Singapore time are held for a compact
category-grouped digest. The first workflow run at or after 08:00 sends due
digest batches before individual daytime alerts. Jobs found from 08:00 through
23:59 remain immediate. Setting the quiet-hours start and end to the same hour
disables digest scheduling.

## Database

The scraper creates this table automatically:

```sql
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
```

`job_id` is source-prefixed, for example `greenhouse_12345` or `internsg_some-role-slug`.
Existing databases are migrated automatically with `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, and existing rows remain treated as successfully sent.

New alerts move through `pending`, `sending`, `failed`, `sent`, or `dead`. The
`delivery_mode` is fixed to `immediate` or `digest` when a row is enqueued, and
existing rows migrate to `immediate`. Separate atomic claims prevent daytime
workers from taking digest rows early. Failed alerts retry up to five times with
exponential backoff. A delivery left in `sending` for more than 30 minutes is
eligible for recovery.

Every lifecycle payload stores all matching role categories in the fixed order
`QUANT`, `AI/ML`, `DATA`, `SWE`, then `TECH` as the fallback. A role is `QUANT`
when its title is quant-specific or its official source is tagged as a known
quant firm. Digest jobs are displayed once under their first category while all
their tags remain visible.

Lifecycle state is separate from delivery state:

- `job_observations` stores the latest normalized payload, structured eligibility,
  fingerprint, active/closed state, timestamps, and successful-snapshot misses.
- `job_lifecycle_events` records append-only `backfilled`, `new`, `updated`,
  `closed`, and `reopened` transitions.

Existing non-canary queue rows are backfilled silently. New and reopened jobs send
detailed Telegram cards; updates and closures are recorded without alerts. Only
registry sources explicitly marked `lifecycle_mode: snapshot` can close jobs.
Limited-window and bespoke sources update their last-seen state but never infer
that a missing job has closed.

## Current Limitations

- Eligibility descriptions use conservative deterministic parsing. Ambiguous
  requirements remain `unknown` rather than being rejected.
- Sources without a usable description remain eligible with an unknown verdict.
- Location accuracy depends on metadata supplied by each job source.
- GitHub indexes are discovery sources and may still lag official company career pages.
- Some scrapers depend on external HTML or third-party APIs that may change.
- Bespoke sources without stable free HTTP access fall back to discovery sources rather than CAPTCHA bypasses, paid proxies, or browser automation.
- JobSpy controls some network behavior internally, outside the shared HTTP retry client.
- Telegram has no idempotency key. Retrying an ambiguous timeout prioritizes not losing an alert, but can rarely produce a duplicate.

## Main Entry Point

```bash
python main.py
```

The script runs all configured pipelines in sequence.
