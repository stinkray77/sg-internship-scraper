"""Bounded daily checks; never auto-enable newly discovered employer domains."""
import datetime as dt
import json
import re
from pathlib import Path
from urllib.parse import urlsplit, urljoin
from bs4 import BeautifulSoup
from psycopg2.extras import Json
from job_content import support_evidence

ROOT=Path(__file__).parent


def load_indexes():
    document=json.loads((ROOT/'indexes.json').read_text())
    if document.get('version')!=1: raise ValueError('Unsupported index configuration version')
    seen=set()
    for entry in document.get('indexes',[]):
        if entry['id'] in seen or entry.get('parser') not in ('singapore','global_quant'):
            raise ValueError('Invalid or duplicate index configuration')
        if urlsplit(entry['url']).scheme!='https': raise ValueError('Index URL must use HTTPS')
        seen.add(entry['id'])
    return [e for e in document['indexes'] if e.get('enabled')]


def due(conn,key,hours):
    if conn is None:return True
    with conn.cursor() as cur:
        cur.execute('SELECT checked_at,payload FROM maintenance_state WHERE task_key=%s',(key,))
        row=cur.fetchone()
    conn.commit()
    return not row or dt.datetime.now(dt.timezone.utc)-row[0]>=dt.timedelta(hours=hours)


def save(conn,key,payload):
    if conn is None:return
    with conn.cursor() as cur:
        cur.execute('''INSERT INTO maintenance_state(task_key,payload) VALUES(%s,%s)
            ON CONFLICT(task_key) DO UPDATE SET checked_at=CURRENT_TIMESTAMP,payload=EXCLUDED.payload''',(key,Json(payload)))
    conn.commit()


def policy_cache(conn):
    if conn is None:return []
    with conn.cursor() as cur:
        cur.execute("SELECT payload FROM maintenance_state WHERE task_key LIKE 'policy:%'")
        policies=[row[0] for row in cur.fetchall() if row[0].get('verified_at')]
    conn.commit()
    return policies


def index_freshness(entry,requester,now=None):
    now=now or dt.datetime.now(dt.timezone.utc);warnings=[]
    if entry['recruiting_year']<now.year: warnings.append('Recruiting year has passed')
    parts=urlsplit(entry['url'])
    if parts.netloc=='raw.githubusercontent.com':
        owner,repo,ref,*path=parts.path.strip('/').split('/')
        response=requester('GET',f'https://api.github.com/repos/{owner}/{repo}/commits',params={'path':'/'.join(path),'sha':ref,'per_page':1})
        commits=response.json()
        if not isinstance(commits,list) or not commits: raise ValueError('No index commit metadata')
        updated=dt.datetime.fromisoformat(commits[0]['commit']['committer']['date'].replace('Z','+00:00'))
        if now-updated>dt.timedelta(days=30): warnings.append('Index file has not changed for 30 days')
    else:warnings.append('Index freshness metadata unavailable')
    return warnings


def run_maintenance(conn,requester,policies=()):
    notes=[]
    for entry in load_indexes():
        key='index:'+entry['id']
        if not due(conn,key,24):continue
        try:
            warnings=index_freshness(entry,requester)
            notes.extend(f"- {entry['id']}: {w}" for w in warnings)
            save(conn,key,{'warnings':warnings})
        except Exception as error:notes.append(f"- {entry['id']}: freshness check failed: {error}")
    for policy in policies:
        key='policy:'+policy['id']
        if not due(conn,key,24*7):continue
        try:
            response=requester('GET',policy['url'])
            text=BeautifulSoup(response.text,'html.parser').get_text(' ',strip=True)
            if not policy.get('programme_marker') or policy['programme_marker'].casefold() not in text.casefold():
                raise ValueError('Internship programme scope marker absent')
            evidence=support_evidence(response.text,policy['url'])
            payload={**policy,'evidence':evidence,'verified_at':dt.datetime.now(dt.timezone.utc).isoformat()}
            save(conn,key,payload)
        except Exception as error:notes.append(f"- {policy.get('company')}: policy revalidation failed: {error}")
    if conn is not None:
        known=json.loads((ROOT/'employers.json').read_text())['employers']
        known_keys={re.sub(r'[^a-z0-9]','',r['company'].casefold()) for r in known}
        with conn.cursor() as cur:
            cur.execute('SELECT company_key,company,evidence_url,careers_url FROM employer_candidates ORDER BY last_seen_at DESC LIMIT 100')
            candidates=cur.fetchall()
        conn.commit()
        checked=0
        for key,company,evidence_url,company_url in candidates:
            if key in known_keys:continue
            if checked>=20:break
            checked+=1
            if not due(conn,'employer:'+key,24):continue
            payload={'company':company,'evidence_url':evidence_url,'status':'needs official careers URL'}
            if company_url and urlsplit(company_url).scheme=='https':
                host=urlsplit(company_url).hostname or ''
                import ipaddress
                try:
                    public=ipaddress.ip_address(host).is_global
                except ValueError:
                    public='.' in host and not host.endswith(('.local','.internal'))
                if public:
                    try:
                        response=requester('GET',company_url)
                        soup=BeautifulSoup(response.text,'html.parser')
                        links=[urljoin(company_url,a['href']) for a in soup.select('a[href]')
                               if re.search(r'careers?|jobs|join us',a.get_text(' ',strip=True),re.I)]
                        if links:
                            payload.update(careers_url=links[0],status='candidate careers link found; adapter verification required')
                        else:
                            payload.update(careers_url=company_url,status='company page reachable; careers link not found')
                    except Exception as error:
                        payload['status']='company URL validation failed: '+str(error)[:160]
            notes.append(f"- Employer onboarding candidate: {company}; {payload['status']}; evidence: {evidence_url}; careers: {payload.get('careers_url','unverified')}")
            save(conn,'employer:'+key,payload)
    return notes
