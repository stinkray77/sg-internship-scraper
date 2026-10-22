"""Persistent source health, coverage and GitHub Actions summaries."""
import datetime as dt
import json
import os
from pathlib import Path
from psycopg2.extras import Json


def migrate_health_schema(cur):
    cur.execute('''CREATE TABLE IF NOT EXISTS scraper_source_runs (
        run_id BIGSERIAL PRIMARY KEY, source_id TEXT NOT NULL,
        completed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        duration_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
        fetched INTEGER NOT NULL, matched INTEGER NOT NULL, queued INTEGER NOT NULL,
        complete BOOLEAN NOT NULL, errors JSONB NOT NULL, warnings JSONB NOT NULL,
        rejections JSONB NOT NULL)''')
    cur.execute('CREATE INDEX IF NOT EXISTS scraper_source_runs_recent_idx ON scraper_source_runs(source_id, completed_at DESC)')
    cur.execute('''CREATE TABLE IF NOT EXISTS employer_candidates (
        company_key TEXT PRIMARY KEY, company TEXT NOT NULL, careers_url TEXT,
        verification TEXT NOT NULL DEFAULT 'unverified', evidence_url TEXT,
        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS maintenance_state (
        task_key TEXT PRIMARY KEY, checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        payload JSONB NOT NULL)''')


def health_warnings(history, now=None):
    now = now or dt.datetime.now(dt.timezone.utc)
    warnings=[]
    if len(history) >= 3 and all(r['errors'] for r in history[:3]):
        warnings.append('Three consecutive fetch failures')
    recent=[r for r in history if now-r['completed_at'] <= dt.timedelta(days=7)]
    if len(history) >= 3 and all(not r['errors'] and r['fetched']==0 for r in history[:3]) and any(r['fetched']>0 for r in recent):
        warnings.append('Three empty fetches after positive coverage in the last seven days')
    matched=[r for r in history if r['matched']>0]
    if matched and now-matched[0]['completed_at'] >= dt.timedelta(days=7):
        warnings.append('No matches for seven days after previously producing matches')
    return warnings


def record_run(conn, stats):
    with conn.cursor() as cur:
        cur.execute('''INSERT INTO scraper_source_runs(source_id,duration_seconds,fetched,matched,queued,complete,errors,warnings,rejections)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
            (stats.source, stats.duration_seconds, stats.fetched, stats.matched, stats.queued,
             stats.complete and not stats.errors, Json(stats.errors), Json(stats.warnings),
             Json({'role':stats.filtered_role,'location_or_visa':stats.filtered_location,'eligibility':stats.filtered_eligibility})))
        cur.execute('DELETE FROM scraper_source_runs WHERE completed_at < CURRENT_TIMESTAMP - INTERVAL \'90 days\'')
        cur.execute('''SELECT completed_at,fetched,matched,errors FROM scraper_source_runs
            WHERE source_id=%s ORDER BY completed_at DESC LIMIT 30000''',(stats.source,))
        history=[dict(zip(('completed_at','fetched','matched','errors'),r)) for r in cur.fetchall()]
    conn.commit()
    stats.warnings.extend(health_warnings(history))


def record_candidate(conn, job):
    import re
    company=str(job.get('company') or '').strip()
    if not company or company=='Unknown Company': return
    key=re.sub(r'[^a-z0-9]','',company.casefold())
    with conn.cursor() as cur:
        cur.execute('''INSERT INTO employer_candidates(company_key,company,evidence_url,careers_url)
            VALUES (%s,%s,%s,%s) ON CONFLICT(company_key) DO UPDATE
            SET last_seen_at=CURRENT_TIMESTAMP,evidence_url=EXCLUDED.evidence_url,
            careers_url=COALESCE(EXCLUDED.careers_url,employer_candidates.careers_url)''',
            (key,company,job.get('job_url'),job.get('company_url')))
    conn.commit()


def markdown_safe(value):
    return str(value).replace('|','/').replace('\n',' ').replace('<','&lt;').replace('>','&gt;')


def coverage_report():
    path=Path(__file__).with_name('employers.json')
    rows=json.loads(path.read_text()).get('employers',[]) if path.exists() else []
    lines=['### Employer coverage', '', '| Employer | Coverage | Last checked | Reason |', '|---|---|---|---|']
    for row in rows:
        lines.append('| '+' | '.join(markdown_safe(row.get(k,'')) for k in ('company','coverage','verified_at','reason'))+' |')
    return '\n'.join(lines)


def write_summary(stats_list, conn=None, extra=()):
    lines=['## Internship scraper', '', '| Source | Seconds | Fetched | Matched | Queued | Complete |', '|---|---:|---:|---:|---:|---|']
    for s in stats_list:
        lines.append(f'| {markdown_safe(s.source)} | {s.duration_seconds:.1f} | {s.fetched} | {s.matched} | {s.queued} | {s.complete and not s.errors} |')
        for warning in [*s.warnings,*s.errors]: lines.append(f'\n- **{markdown_safe(s.source)}:** {markdown_safe(warning)}')
    if conn:
        with conn.cursor() as cur:
            cur.execute('''SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (sent_at-created_at))),
                percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (sent_at-created_at)))
                FROM seen_jobs WHERE delivery_status='sent' AND delivery_mode='immediate'
                AND sent_at >= CURRENT_TIMESTAMP - INTERVAL '1 day' ''')
            # percentile expressions are kept together to avoid comparing quiet-hour holding to delivery latency.
            latency=cur.fetchone()
            if latency and latency[0] is not None:
                lines.append(f'\nImmediate queue-to-send latency, last day: median {latency[0]:.1f}s; p95 {latency[1]:.1f}s.')
        conn.commit()
    lines.extend(str(x) for x in extra)
    lines.append(coverage_report())
    report='\n'.join(lines)+'\n'
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as f: f.write(report)
    return report
