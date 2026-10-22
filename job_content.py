"""Grounded description excerpts and role-scoped overseas support evidence."""
import datetime as dt
import json
import re
from pathlib import Path
from urllib.parse import urlsplit
from bs4 import BeautifulSoup
from role_patterns import STUDENT_ROLE_PATTERN, normalize_role_title

TECH_TASK = re.compile(r'\b(?:build|develop|implement|design|maintain|test|debug|deploy|train|research|analy[sz]e)\w*\b.{0,100}\b(?:software|applications?|APIs?|algorithms?|models?|data pipelines?|machine learning|systems?|code|databases?)\b', re.I)
TECH_SKILL = re.compile(r'\b(?:Python|JavaScript|TypeScript|Java|C\+\+|C#|SQL|Go|Rust|React|Linux|Kubernetes|Docker|TensorFlow|PyTorch|AWS|Azure|GCP)\b', re.I)
NEGATIVE_VISA = re.compile(r'\b(?:no|without)\s+(?:visa\s+)?sponsorship\b|\b(?:do(?:es)? not|cannot|can.t|unable to|will not)\s+(?:provide\s+)?sponsor\w*|\bsponsorship\s+(?:is\s+)?not\s+(?:available|provided|offered)|\b(?:must|need to)\s+(?:already\s+)?(?:have|possess)\s+(?:the\s+)?(?:legal\s+)?right to work\b', re.I)
POSITIVE_VISA = re.compile(r'\b(?:visa|work (?:permit|visa))\s+sponsorship\s+(?:(?:is|will be|may be)\s+)?(?:available|provided|offered)|\b(?:provide|offer|provides|offers)\s+(?:\w+\s+){0,3}(?:visa|work permit)\s+sponsorship|\bsponsor\s+(?:\w+\s+){0,3}(?:visas?|work permits?)\b|\b(?:visa|work permit)\s+(?:costs?|fees?)\s+(?:are\s+)?(?:covered|paid)\b', re.I)
CONDITIONAL = re.compile(r'\b(?:may|might|case.by.case|subject to|where eligible|depending|consider)\b', re.I)


def text_blocks(value):
    soup = BeautifulSoup(str(value or ''), 'html.parser')
    for tag in soup(['script', 'style', 'nav', 'footer']):
        tag.decompose()
    text = soup.get_text('\n', strip=True)
    blocks = re.split(r'\n+|(?<=[.!?])\s+', text)
    return [re.sub(r'\s+', ' ', b).strip() for b in blocks if b.strip()]


def extract_content(description):
    blocks = text_blocks(description)
    text = ' '.join(blocks)
    skills = list(dict.fromkeys(m.group(0) for m in TECH_SKILL.finditer(text)))[:5]
    responsibilities = [b[:240] for b in blocks if TECH_TASK.search(b)][:2]
    def excerpt(pattern):
        return next((b[:240] for b in blocks if re.search(pattern, b, re.I)), None)
    return {
        'description': '\n'.join(blocks)[:20000],
        'responsibilities': responsibilities,
        'skills': skills,
        'internship_dates': excerpt(r'\b(?:start date|commenc|from\s+\w+\s+20\d\d|internship period)\b'),
        'application_deadline': excerpt(r'\b(?:application deadline|apply by|applications? close|closing date)\b'),
    }


def ambiguous_technical_student(title, description):
    if not re.search(STUDENT_ROLE_PATTERN, normalize_role_title(title)):
        return False
    blocks = text_blocks(description)
    # Skills alone (including Excel or boilerplate company technology) are insufficient.
    return any(TECH_TASK.search(block) for block in blocks)


def support_evidence(description, url, now=None):
    now = now or dt.datetime.now(dt.timezone.utc)
    blocks = text_blocks(description)
    negative = next((b for b in blocks if NEGATIVE_VISA.search(b)), None)
    positive = next((b for b in blocks if POSITIVE_VISA.search(b)), None)
    evidence = []
    if negative:
        evidence.append(dict(kind='visa', status='unavailable', excerpt=negative[:500]))
    elif positive:
        evidence.append(dict(kind='visa', status='conditional' if CONDITIONAL.search(positive) else 'available', excerpt=positive[:500]))
    for kind, pattern in (
        ('relocation', r'\brelocation\s+(?:assistance|support|package|allowance)|\brelocation\s+(?:costs?|expenses?)'),
        ('housing', r'\b(?:housing|accommodation)\s+(?:provided|allowance|support)|\b(?:provide|paid|free)\s+(?:housing|accommodation)\b'),
        ('travel', r'\b(?:travel|flight)\s+(?:expenses?|costs?|reimbursement|allowance)|\b(?:paid|funded)\s+(?:travel|flights?)\b'),
        ('immigration', r'\bimmigration\s+(?:assistance|support)\b'),
    ):
        for block in blocks:
            if re.search(pattern, block, re.I):
                negative_benefit = re.search(r'\b(?:no|without|not provided|not available|do not provide)\b', block, re.I)
                evidence.append(dict(kind=kind, status='unavailable' if negative_benefit else 'stated', excerpt=block[:500]))
                break
    for item in evidence:
        item.update(source_url=url, verified_at=now.isoformat(), scope='posting')
    return evidence


def applicable_policy(job, policies, now=None):
    now = now or dt.datetime.now(dt.timezone.utc)
    matches = []
    for policy in policies:
        if str(policy.get('company', '')).casefold() != str(job.get('company', '')).casefold():
            continue
        if not policy.get('programme_terms') or not any(re.search(term, job.get('title', ''), re.I) for term in policy['programme_terms']):
            continue
        offices = policy.get('offices') or []
        if offices and not any(o.casefold() in str(job.get('location', '')).casefold() for o in offices):
            continue
        try:
            verified = dt.datetime.fromisoformat(policy['verified_at'])
            if verified.tzinfo is None or not dt.timedelta(0) <= now - verified <= dt.timedelta(days=30):
                continue
        except (KeyError, TypeError, ValueError):
            continue
        # Store only revalidated official documents. Maintenance supplies the evidence.
        for item in policy.get('evidence', []):
            if item.get('kind') == 'visa' and item.get('status') in ('available', 'conditional'):
                matches.append({**item, 'source_url': policy['url'], 'verified_at': policy['verified_at'], 'scope': 'internship programme'})
    return matches


def overseas_eligible(job, singapore, quant, policies=()):
    evidence = support_evidence(job.get('description'), job.get('job_url'))
    visa = next((e for e in evidence if e['kind'] == 'visa'), None)
    if not visa:
        evidence.extend(applicable_policy(job, policies))
    job['support_evidence'] = evidence
    job['is_overseas'] = not singapore
    job['visa_support'] = next((e['status'] for e in evidence if e['kind'] == 'visa'), 'unknown')
    if singapore or quant:
        return True
    location = str(job.get('location') or '').strip()
    if not location or job.get('location_confidence') == 'firm_index_unverified':
        return False
    return job['visa_support'] in ('available', 'conditional')


def load_policies(path=None):
    path = Path(path) if path else Path(__file__).with_name('support_policies.json')
    return json.loads(path.read_text()).get('policies', []) if path.exists() else []
