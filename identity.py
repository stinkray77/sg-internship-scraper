"""Persistent opportunity identities, independent of source observation IDs."""
import hashlib
import json
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from psycopg2.extras import Json


def canonical_url(value):
    parts = urlsplit(str(value or '').strip())
    if parts.scheme not in ('http', 'https') or not parts.netloc:
        return ''
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
             if not k.lower().startswith('utm_') and k.lower() not in ('source', 'ref', 'refid', 'trk')]
    return urlunsplit(('https', parts.netloc.lower(), parts.path.rstrip('/'), urlencode(sorted(query)), ''))


def normalized(value):
    return re.sub(r'[^a-z0-9]', '', str(value or '').lower())


def identity_metadata(job):
    source = str(job.get('source_id') or job.get('site') or '')
    requisition = job.get('requisition_id')
    # Board IDs are employer-scoped; aggregator IDs are source observations only.
    if not requisition and source.startswith(('greenhouse_', 'lever_', 'workday_', 'ashby_', 'smartrecruiters_')):
        requisition = job.get('external_id')
    return {
        'company': normalized(job.get('company_key') or job.get('company')),
        'title': normalized(job.get('title')),
        'location': normalized(json.dumps(job.get('location'), sort_keys=True)),
        'intake': ' '.join(re.findall(r'\b20\d{2}\b', str(job.get('title') or ''))),
        'source': source,
        'requisition': str(requisition or ''),
    }


def aliases_for(job):
    meta = identity_metadata(job)
    aliases = []
    for field in ('job_url_direct', 'job_url'):
        url = canonical_url(job.get(field))
        if url:
            aliases.append('url:' + url)
    if meta['requisition']:
        aliases.append('req:' + meta['company'] + ':' + meta['source'] + ':' + meta['requisition'])
    return sorted(set(aliases))


def compatible(first, second):
    if any(first.get(k) and second.get(k) and first[k] != second[k]
           for k in ('company', 'title', 'location', 'intake')):
        return False
    # Two known requisitions with no common URL must not merge by title.
    return not (first.get('requisition') and second.get('requisition'))


def migrate_identity_schema(cur):
    cur.execute('''CREATE TABLE IF NOT EXISTS canonical_jobs (
        canonical_id TEXT PRIMARY KEY, metadata JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS job_identity_aliases (
        alias TEXT PRIMARY KEY, canonical_id TEXT NOT NULL REFERENCES canonical_jobs(canonical_id))''')
    cur.execute('ALTER TABLE seen_jobs ADD COLUMN IF NOT EXISTS canonical_id TEXT')
    cur.execute('CREATE INDEX IF NOT EXISTS seen_jobs_canonical_idx ON seen_jobs(canonical_id)')
    cur.execute("CREATE INDEX IF NOT EXISTS canonical_jobs_company_idx ON canonical_jobs ((metadata->>'company'))")


def resolve_identity(cur, job):
    meta = identity_metadata(job)
    # All identity and delivery checks for an employer run under the same xact lock.
    cur.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))', ('jobs:' + meta['company'],))
    aliases = aliases_for(job)
    if aliases:
        cur.execute('SELECT canonical_id FROM job_identity_aliases WHERE alias = ANY(%s) ORDER BY alias LIMIT 1', (aliases,))
        row = cur.fetchone()
        canonical_id = row[0] if row else None
    else:
        canonical_id = None
    if not canonical_id:
        cur.execute("SELECT canonical_id, metadata FROM canonical_jobs WHERE metadata->>'company' = %s AND metadata->>'title' = %s", (meta['company'], meta['title']))
        candidates = [r for r in cur.fetchall() if compatible(r[1], meta)]
        canonical_id = candidates[0][0] if len(candidates) == 1 else None
    if not canonical_id:
        seed = aliases[0] if aliases else json.dumps(meta, sort_keys=True)
        canonical_id = hashlib.sha256(seed.encode()).hexdigest()
        cur.execute('INSERT INTO canonical_jobs(canonical_id, metadata) VALUES (%s, %s) ON CONFLICT DO NOTHING', (canonical_id, Json(meta)))
    for alias in aliases:
        cur.execute('INSERT INTO job_identity_aliases(alias, canonical_id) VALUES (%s, %s) ON CONFLICT DO NOTHING', (alias, canonical_id))
    return canonical_id


def backfill_identities(cur):
    cur.execute("SELECT job_id, company, title, site, payload FROM seen_jobs WHERE canonical_id IS NULL AND job_id NOT LIKE 'system_canary_%' ORDER BY created_at, job_id")
    for job_id, company, title, site, payload in cur.fetchall():
        job = {'company': company, 'title': title, 'site': site, **(payload or {})}
        canonical_id = resolve_identity(cur, job)
        cur.execute('UPDATE seen_jobs SET canonical_id = %s WHERE job_id = %s', (canonical_id, job_id))


def observation_identity(job):
    """Stable per-source key; never use a title alone as an observation identity."""
    raw = job.get('id') or canonical_url(job.get('job_url_direct') or job.get('job_url'))
    if not raw:
        raw = json.dumps(identity_metadata(job), sort_keys=True)
    return hashlib.sha256((str(job.get('site') or '') + ':' + str(raw)).encode()).hexdigest()[:32]
