# Singapore and sponsored overseas internship scraper

Finds technical student opportunities across employer boards, JobSpy, InternSG,
and configured internship indexes. PostgreSQL stores observations, opportunity
identities, delivery history, source health and discovery candidates. Telegram
receives new opportunities and reopenings.

## Coverage

The registry includes the original employers and **12 additional validated public
boards**: Schonfeld, ExodusPoint, Marshall Wace, AQR, ServiceNow, Databricks,
Datadog, Cloudflare, Palantir, Canva, Wise and Nium.

[employers.json](employers.json) records a live audit of **50 additional employers**
(25 funds and 25 technology employers), including verification timestamps,
careers URLs and coverage gaps. `direct monitoring` means a structured board was
validated; `discovery only` means public career information was reachable but a
direct adapter was not validated; `unavailable` records blocked or failed access.
Neither registry membership nor discovery coverage guarantees that every job is
found. The audit is a dated snapshot, not a continuously verified coverage claim.

`sources.json` configures Greenhouse, Lever, SmartRecruiters, Workday, Ashby,
Workable, structured JobPosting pages and explicitly configured HTML selectors.
New domains are never automatically enabled. Daily maintenance reports unfamiliar
employers seen by aggregators and checks available company links for careers
pages; source adapters still require verification before onboarding.

Saved board responses and a benchmark of 30 timestamped real postings live in
`tests/fixtures`. Positive sponsorship parser cases are tested separately; the
benchmark does not claim that its overseas technology jobs provide sponsorship.

## Matching and overseas opportunities

- Technical student titles support spelling, spacing and Unicode dash variants,
  including full-stack, IT/QA/SRE, co-op and industrial attachment.
- Ambiguous student titles such as Summer Analyst require a technical task in the
  description. Generic technology or Excel mentions alone are insufficient.
- Explicit postgraduate-only requirements remain excluded. Unknown descriptions
  do not exclude otherwise clear technical titles.
- Singapore roles remain eligible. Overseas quant technical internships remain
  broadly eligible, with unknown visa support labeled clearly.
- Other overseas technical internships require explicit visa support in the role
  or a verified, applicable internship programme policy. Relocation funding or
  immigration assistance alone does not establish sponsorship.
- Benefits retain evidence excerpts, official source links, verification dates
  and scope. Posting restrictions override general policies; conditional support
  is labeled conditional. Support availability is not a visa approval guarantee.

`support_policies.json` starts empty rather than assuming firm-wide benefits.
A configured programme policy needs `id`, `company`, an official HTTPS `url`,
a `programme_marker`, regex `programme_terms`, and optional `offices`.
Maintenance checks the marker and re-extracts support weekly. Policies with no
successful verification within 30 days cannot qualify new overseas non-quant
alerts. Citadel and Citadel Securities must be researched separately; no Sydney
or Hong Kong benefit is inferred simply from the employer name.

## Delivery and timing

Official boards are processed as they finish. A separate worker checks committed
queue entries every five seconds while scraping continues. Database locks
serialize delivery across workflows and preserve atomic claims and retries.

Official sources are scheduled every ten minutes, discovery sources every thirty
minutes, and maintenance daily. GitHub Actions scheduling and source response
times are best-effort; the schedule is not a ten-minute delivery guarantee.
The operational target is delivery within 15 seconds of enqueueing when Telegram
is healthy and quiet hours are inactive.

**Quiet hours remain 00:00–07:59 Singapore time.** Overnight jobs wait for the
existing category-grouped digest from 08:00. Daytime discoveries are immediate.
Newly enabled sources send their current unseen eligible jobs using the same
quiet hours, deduplication and Telegram rate limits.

Cards include source-grounded responsibility excerpts, skills, dates and overseas
support evidence. Missing facts are not invented. First-seen times use Singapore
time; posting, employer-update and index-added dates are distinguished. Digests
remain compact, and optional excerpts are trimmed to meet Telegram limits.

## Setup and commands

Python 3.10+, PostgreSQL and a Telegram bot/chat are required for live operation.

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m unittest discover -s tests -v
```

Required live environment variables:

```env
DATABASE_URL=postgresql://user:password@host:5432/database
TELEGRAM_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

Optional settings: `JOB_LOOKBACK_HOURS=72`, `RESULTS_PER_SOURCE=50`,
`DELIVERY_TIMEZONE=Asia/Singapore`, `QUIET_HOURS_START=0`, `QUIET_HOURS_END=8`,
and `DIGEST_MAX_JOBS=8`. Equal quiet-hour start/end disables quiet hours.

```bash
# Read-only network checks: no database writes or Telegram sends.
.venv/bin/python main.py --dry-run --group official
.venv/bin/python main.py --dry-run --group discovery
.venv/bin/python main.py --dry-run --group maintenance

# Live operation; creates/migrates tables, records jobs and may send alerts.
.venv/bin/python main.py --group official
.venv/bin/python main.py --group discovery
.venv/bin/python main.py --group maintenance
.venv/bin/python main.py --group all

# Exactly one labeled database/Telegram canary; skips scraping.
.venv/bin/python main.py --canary
```

The default group is `all`. Manual GitHub Actions runs default to dry-run.
Official cron: `7,17,27,37,47,57 * * * *`; discovery: `12,42 * * * *`;
maintenance: `32 3 * * *` (all UTC).

## Identity, migration and lifecycle

Application URLs and employer-scoped requisition IDs map source observations to
canonical opportunities. Distinct requisitions and conflicting application URLs
are preserved, even with identical titles. Matching without a strong identifier
is conservative. Exact historical JobSpy IDs/URLs preserve legacy observations.

Migrations are additive and idempotent. They backfill identity aliases while
preserving existing job IDs, sent status, pending deliveries and lifecycle events.
Migration itself sends nothing. Existing index/source baselines are not reset.
Changing code or schedules provides rollback without deleting delivery history.

Only complete authoritative employer snapshots can infer closure after three
successful misses. Pagination exhaustion, request failures and discovery-index
omissions cannot close jobs. Presence is recorded separately from alert eligibility;
a role that becomes sponsored can alert once without masquerading as a reopened
posting. Description changes update observations without new-job alerts.

Telegram retries are persisted with bounded attempts and rate-limit handling.
Telegram offers no idempotency key, so an ambiguous send timeout can still cause
a rare duplicate on retry.

## Health, freshness and tests

The health tracker uses `scraper_source_runs`; any older `source_runs` table is
left untouched. Manual workflow mode `schema` reads table definitions without
reading job records or changing the database.

GitHub Actions summaries show source counts, durations, coverage gaps, rejection
counts, operational warnings and immediate queue-to-send latency. Source history
is retained for 90 days. Warnings cover three consecutive failures, three empty
fetches after recent positive coverage, and seven days without matches after
previously producing them. No operational Telegram messages are added.

`indexes.json` stores explicit URLs, stable source IDs, parser types and recruiting
years. Maintenance checks file commit dates daily and flags expired years or 30
days without changes. Replacement URLs require fetching/parsing validation;
the scraper never guesses a next-year URL. Freshness warnings do not remove jobs.

The test workflow runs unit/fixture tests and a disposable PostgreSQL 17 service.
To run integration tests locally, supply a **disposable** database:

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost/test_database \
  .venv/bin/python -m unittest discover -s tests/integration -v
```

Integration tests drop and recreate the `scraper_test` schema. They cover migration
idempotency, historical delivery preservation, concurrent enqueueing, queue claims,
source-health SQL and transitions from ineligible to eligible observations.

Shared HTTP requests have global/per-host concurrency limits and source deadlines.
JobSpy calls run in subprocesses with a 120-second shard timeout. External sites
can still change, block requests or omit descriptions; those gaps are reported.
