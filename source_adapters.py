import datetime
import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup


@dataclass
class JobCandidate:
    source_id: str
    external_id: str
    company: str
    title: str
    job_url: str
    location: object = None
    country: object = None
    date_posted: object = None
    description: str = ""
    company_key: str | None = None
    source_label: str | None = None
    source_tags: list[str] = field(default_factory=list)

    def to_payload(self) -> dict:
        return {
            "site": self.source_id,
            "source_id": self.source_id,
            "company": self.company,
            "company_key": self.company_key or self.company,
            "title": self.title,
            "job_url": self.job_url,
            "location": self.location,
            "country": self.country,
            "date_posted": self.date_posted,
            "description": self.description,
            "source_label": self.source_label or self.source_id,
            "source_tags": self.source_tags,
        }


@dataclass
class SourceFetchResult:
    source_id: str
    candidates: list[JobCandidate] = field(default_factory=list)
    fetched: int = 0
    error: str | None = None
    warnings: list[str] = field(default_factory=list)


REQUIRED_SOURCE_FIELDS = {"id", "company", "adapter", "enabled", "config"}
SUPPORTED_ADAPTERS = {
    "greenhouse",
    "lever",
    "smartrecruiters",
    "workday",
    "ashby",
    "bespoke",
}


def load_source_registry(path: str | Path | None = None) -> list[dict]:
    registry_path = Path(path) if path else Path(__file__).with_name("sources.json")
    with registry_path.open(encoding="utf-8") as registry_file:
        document = json.load(registry_file)
    if document.get("version") != 1:
        raise ValueError("sources.json must use registry version 1")
    sources = document.get("sources")
    if not isinstance(sources, list):
        raise ValueError("sources.json must contain a sources list")
    validate_source_registry(sources)
    return sources


def validate_source_registry(sources: list[dict]) -> None:
    seen_ids = set()
    for index, source in enumerate(sources):
        missing = REQUIRED_SOURCE_FIELDS - source.keys()
        if missing:
            raise ValueError(
                f"source {index} is missing: {', '.join(sorted(missing))}"
            )
        source_id = source["id"]
        if not isinstance(source_id, str) or not re.fullmatch(r"[a-z0-9_]+", source_id):
            raise ValueError(f"invalid source id: {source_id!r}")
        if source_id in seen_ids:
            raise ValueError(f"duplicate source id: {source_id}")
        seen_ids.add(source_id)
        if source["adapter"] not in SUPPORTED_ADAPTERS:
            raise ValueError(
                f"unsupported adapter {source['adapter']!r} for {source_id}"
            )
        if not isinstance(source["enabled"], bool):
            raise ValueError(f"enabled must be boolean for {source_id}")
        if not isinstance(source["config"], dict):
            raise ValueError(f"config must be an object for {source_id}")
        tags = source.get("tags", [])
        if not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags):
            raise ValueError(f"tags must be a list of strings for {source_id}")
        lifecycle_mode = source.get("lifecycle_mode", "seen_only")
        if lifecycle_mode not in {"seen_only", "snapshot"}:
            raise ValueError(f"invalid lifecycle mode for {source_id}")
        if lifecycle_mode == "snapshot" and source["adapter"] == "bespoke":
            raise ValueError(
                f"bespoke source {source_id} cannot claim a complete snapshot"
            )


def _candidate(
    source: dict,
    external_id,
    title,
    job_url,
    **kwargs,
) -> JobCandidate:
    raw_id = str(external_id or job_url or title)
    if not external_id:
        raw_id = hashlib.sha256(raw_id.encode()).hexdigest()[:24]
    return JobCandidate(
        source_id=source["id"],
        external_id=str(raw_id),
        company=source["company"],
        company_key=source.get("dedupe_company") or source["company"],
        source_label=f"{source['company']} official careers",
        source_tags=list(source.get("tags", [])),
        title=str(title or ""),
        job_url=str(job_url or source["config"].get("url", "")),
        **kwargs,
    )


def fetch_source(source: dict, requester) -> SourceFetchResult:
    if not source.get("enabled", False):
        return SourceFetchResult(source["id"])
    if source.get("mode", "direct") == "discovery_only":
        return SourceFetchResult(source["id"])

    adapter = source["adapter"]
    try:
        warnings = []
        if adapter in {"smartrecruiters", "workday"}:
            fetcher = {
                "smartrecruiters": fetch_smartrecruiters,
                "workday": fetch_workday,
            }[adapter]
            candidates, fetched = fetcher(
                source,
                requester,
                warnings=warnings,
            )
        else:
            candidates, fetched = {
                "greenhouse": fetch_greenhouse,
                "lever": fetch_lever,
                "ashby": fetch_ashby,
                "bespoke": fetch_bespoke,
            }[adapter](source, requester)
        return SourceFetchResult(source["id"], candidates, fetched, warnings=warnings)
    except Exception as error:
        return SourceFetchResult(source["id"], error=str(error))


def fetch_greenhouse(source: dict, requester):
    token = source["config"]["token"]
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
    document = requester("GET", url).json()
    if not isinstance(document.get("jobs"), list):
        raise ValueError("Greenhouse response has no jobs list")
    jobs = document["jobs"]
    candidates = []
    for job in jobs:
        location = (job.get("location") or {}).get("name")
        candidates.append(_candidate(
            source,
            job.get("id"),
            job.get("title"),
            job.get("absolute_url"),
            location=location,
            date_posted=job.get("updated_at"),
            description=job.get("content") or "",
        ))
    return candidates, len(jobs)


def fetch_lever(source: dict, requester):
    token = source["config"]["token"]
    jobs = []
    for skip in range(0, 10_000, 100):
        url = f"https://api.lever.co/v0/postings/{token}"
        page = requester(
            "GET",
            url,
            params={"mode": "json", "skip": skip, "limit": 100},
        ).json()
        if not isinstance(page, list):
            raise ValueError("Lever response is not a list")
        jobs.extend(page)
        if len(page) < 100:
            break
    candidates = []
    for job in jobs:
        categories = job.get("categories") or {}
        candidates.append(_candidate(
            source,
            job.get("id"),
            job.get("text"),
            job.get("hostedUrl"),
            location=categories.get("allLocations") or categories.get("location"),
            country=job.get("country"),
            date_posted=job.get("createdAt"),
            description=job.get("descriptionPlain") or job.get("description") or "",
        ))
    return candidates, len(jobs)


def fetch_smartrecruiters(source: dict, requester, warnings=None):
    token = source["config"]["token"]
    jobs = []
    offset = 0
    for _ in range(100):
        url = f"https://api.smartrecruiters.com/v1/companies/{token}/postings"
        page = requester(
            "GET",
            url,
            params={"offset": offset, "limit": 100},
        ).json()
        content = page.get("content")
        if not isinstance(content, list):
            raise ValueError("SmartRecruiters response has no content list")
        jobs.extend(content)
        offset += len(content)
        if not content or offset >= page.get("totalFound", offset):
            break
    candidates = []
    warnings = warnings if warnings is not None else []
    for job in jobs:
        location = job.get("location") or {}
        raw_id = job.get("id")
        title = job.get("name") or ""
        location_values = [location.get("fullLocation"), location.get("city")]
        description = ""
        if _looks_like_student_technical(title) and _looks_like_singapore(
            [*location_values, location.get("country")]
        ):
            try:
                detail_url = job.get("ref") or (
                    f"https://api.smartrecruiters.com/v1/companies/"
                    f"{token}/postings/{raw_id}"
                )
                detail = requester("GET", detail_url).json()
                sections = ((detail.get("jobAd") or {}).get("sections") or {})
                description = " ".join(
                    str((sections.get(name) or {}).get("text") or "")
                    for name in (
                        "jobDescription",
                        "qualifications",
                        "additionalInformation",
                    )
                ).strip()
            except Exception as error:
                warnings.append(f"detail {raw_id}: {error}")
        candidates.append(_candidate(
            source,
            raw_id,
            title,
            f"https://jobs.smartrecruiters.com/{token}/{raw_id}",
            location=location_values,
            country=location.get("country"),
            date_posted=job.get("releasedDate"),
            description=description,
        ))
    return candidates, len(jobs)


def fetch_workday(source: dict, requester, warnings=None):
    config = source["config"]
    host = config["host"].rstrip("/")
    tenant = config["tenant"]
    site = config["site"]
    base = f"{host}/wday/cxs/{tenant}/{site}"
    jobs = []
    warnings = warnings if warnings is not None else []
    offset = 0
    for _ in range(100):
        page = requester(
            "POST",
            f"{base}/jobs",
            json={"appliedFacets": {}, "limit": 20, "offset": offset},
        ).json()
        postings = page.get("jobPostings")
        if not isinstance(postings, list):
            raise ValueError("Workday response has no jobPostings list")
        jobs.extend(postings)
        offset += len(postings)
        if not postings or offset >= page.get("total", offset):
            break

    candidates = []
    for job in jobs:
        external_path = job.get("externalPath") or ""
        title = job.get("title") or ""
        location = job.get("locationsText")
        description = ""
        detail = {}
        if _looks_like_student_technical(title) and _looks_like_singapore(location):
            try:
                detail_document = requester("GET", f"{base}{external_path}").json()
                detail = detail_document.get("jobPostingInfo") or {}
                description = detail.get("jobDescription") or ""
            except Exception as error:
                warnings.append(f"detail {external_path}: {error}")
        url = detail.get("externalUrl") or urljoin(host, external_path)
        candidates.append(_candidate(
            source,
            external_path,
            title,
            url,
            location=detail.get("location") or location,
            country=detail.get("country"),
            date_posted=detail.get("startDate") or job.get("postedOn"),
            description=description,
        ))
    return candidates, len(jobs)


def fetch_ashby(source: dict, requester):
    board = source["config"]["board"]
    url = f"https://api.ashbyhq.com/posting-api/job-board/{board}"
    document = requester("GET", url).json()
    jobs = document.get("jobs")
    if not isinstance(jobs, list):
        raise ValueError("Ashby response has no jobs list")
    candidates = []
    for job in jobs:
        if job.get("isListed") is False:
            continue
        candidates.append(_candidate(
            source,
            job.get("id"),
            job.get("title"),
            job.get("jobUrl") or job.get("applyUrl"),
            location=job.get("location"),
            date_posted=job.get("publishedAt"),
            description=job.get("descriptionHtml") or job.get("descriptionPlain") or "",
        ))
    return candidates, len(jobs)


def _iter_json_ld(value):
    if isinstance(value, list):
        for item in value:
            yield from _iter_json_ld(item)
    elif isinstance(value, dict):
        if value.get("@type") == "JobPosting":
            yield value
        for key in ("@graph", "itemListElement"):
            if key in value:
                yield from _iter_json_ld(value[key])


def _json_ld_location(posting):
    locations = posting.get("jobLocation") or []
    if isinstance(locations, dict):
        locations = [locations]
    values = []
    countries = []
    for location in locations:
        address = location.get("address") or {}
        values.extend(filter(None, [
            address.get("addressLocality"),
            address.get("addressRegion"),
        ]))
        country = address.get("addressCountry")
        if isinstance(country, dict):
            country = country.get("name")
        if country:
            countries.append(country)
    return values, countries


def fetch_bespoke(source: dict, requester):
    config = source["config"]
    response = requester("GET", config["url"])
    soup = BeautifulSoup(response.text, "html.parser")
    candidates = []
    posting_count = 0

    for script in soup.select('script[type="application/ld+json"]'):
        try:
            document = json.loads(script.string or script.get_text())
        except (TypeError, json.JSONDecodeError):
            continue
        for posting in _iter_json_ld(document):
            posting_count += 1
            location, country = _json_ld_location(posting)
            organization = posting.get("hiringOrganization") or {}
            candidate = _candidate(
                source,
                posting.get("identifier", {}).get("value")
                if isinstance(posting.get("identifier"), dict) else None,
                posting.get("title"),
                posting.get("url") or config["url"],
                location=location or config.get("default_location"),
                country=country,
                date_posted=posting.get("datePosted"),
                description=posting.get("description") or "",
            )
            if organization.get("name"):
                candidate.company = organization["name"]
            candidates.append(candidate)

    link_pattern = config.get("job_link_pattern")
    if link_pattern:
        compiled = re.compile(link_pattern, re.IGNORECASE)
        seen_urls = {candidate.job_url for candidate in candidates}
        for link in soup.select("a[href]"):
            url = urljoin(config["url"], link.get("href"))
            if url in seen_urls or not compiled.search(url):
                continue
            title = link.get_text(" ", strip=True)
            if not title or len(title) > 200:
                continue
            posting_count += 1
            container = link.find_parent(["tr", "li", "article"])
            context = container.get_text(" ", strip=True) if container else (
                link.parent.get_text(" ", strip=True) if link.parent else title
            )
            if config.get("location_from_title") or len(context) > 500:
                context = title
            candidates.append(_candidate(
                source,
                None,
                title,
                url,
                location=context if _looks_like_singapore(context) else config.get("default_location"),
            ))
            seen_urls.add(url)

    marker = config.get("page_marker")
    if marker and marker.lower() not in response.text.lower():
        raise ValueError(f"expected page marker {marker!r} was not found")

    for additional_url in config.get("additional_urls", []):
        additional_source = dict(source)
        additional_config = dict(config)
        additional_config["url"] = additional_url
        additional_config.pop("additional_urls", None)
        additional_source["config"] = additional_config
        additional_candidates, additional_count = fetch_bespoke(
            additional_source,
            requester,
        )
        candidates.extend(additional_candidates)
        posting_count += additional_count
    return candidates, posting_count


def _looks_like_singapore(value) -> bool:
    return bool(re.search(r"(?<![a-z0-9])(singapore|sgp?|sg)(?![a-z0-9])", str(value or ""), re.I))


def _looks_like_student_technical(title: str) -> bool:
    return bool(
        re.search(r"\b(intern|internship|co-?op|trainee|analyst|wintern)\b", title, re.I)
        and re.search(
            r"\b(software|technology|engineer|developer|data|quant|quantitative|trading|"
            r"research|machine learning|ml|ai|security|infrastructure|risk)\b",
            title,
            re.I,
        )
    )
