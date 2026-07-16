import re
from dataclasses import dataclass, field

from bs4 import BeautifulSoup


@dataclass(frozen=True)
class EligibilityAssessment:
    verdict: str
    degree_levels: list[str] = field(default_factory=list)
    graduation_years: list[int] = field(default_factory=list)
    duration: str | None = None
    work_authorization: str | None = None
    reasons: list[str] = field(default_factory=list)

    def to_payload(self) -> dict:
        return {
            "verdict": self.verdict,
            "degree_levels": self.degree_levels,
            "graduation_years": self.graduation_years,
            "duration": self.duration,
            "work_authorization": self.work_authorization,
            "reasons": self.reasons,
        }


def description_text(value: object) -> str:
    return re.sub(
        r"\s+",
        " ",
        BeautifulSoup(str(value or ""), "html.parser").get_text(" ", strip=True),
    ).strip()


def _degree_levels(text: str) -> list[str]:
    levels = []
    patterns = (
        ("Bachelor's", r"\b(?:bachelor'?s?|undergraduate|b\.?s\.?)\b"),
        ("Master's", r"\b(?:master'?s?|postgraduate|m\.?s\.?)\b"),
        ("PhD", r"\b(?:ph\.?d|doctoral)\b"),
    )
    for label, pattern in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            levels.append(label)
    return levels


def _graduation_years(text: str) -> list[int]:
    years = set()
    for match in re.finditer(
        r"(?:graduat(?:e|es|ing|ion)|class of).{0,45}\b(20(?:2[6-9]|3[0-2]))\b|"
        r"\b(20(?:2[6-9]|3[0-2]))\b.{0,45}(?:graduat(?:e|es|ing|ion)|class of)",
        text,
        re.IGNORECASE,
    ):
        years.add(int(match.group(1) or match.group(2)))
    return sorted(years)


def _duration(text: str) -> str | None:
    match = re.search(
        r"\b(\d{1,2}(?:\s*(?:-|to)\s*\d{1,2})?\s*(?:weeks?|months?))\b",
        text,
        re.IGNORECASE,
    )
    if not match:
        return None
    return re.sub(r"\s+", " ", match.group(1)).strip()


def _work_authorization(text: str) -> str | None:
    if re.search(
        r"\b(?:singapore citizens?|singaporeans?)\s*(?:and|or|/)\s*(?:permanent residents?|prs?)\b|"
        r"\b(?:citizens?|permanent residents?|prs?)\s+of singapore\b",
        text,
        re.IGNORECASE,
    ):
        return "Singapore citizen or PR required"
    if re.search(
        r"\b(?:no|without)\s+(?:visa\s+)?sponsorship\b|"
        r"\b(?:visa\s+)?sponsorship\s+(?:is\s+)?not\s+(?:available|provided)\b",
        text,
        re.IGNORECASE,
    ):
        return "Sponsorship not available"
    if re.search(
        r"\b(?:must|need to)\s+(?:already\s+)?(?:have|possess)\s+"
        r"(?:the\s+)?(?:legal\s+)?right to work\b|"
        r"\bauthorized to work\b",
        text,
        re.IGNORECASE,
    ):
        return "Existing work authorization required"
    if re.search(
        r"\b(?:visa\s+)?sponsorship\s+(?:is\s+)?(?:available|provided)\b",
        text,
        re.IGNORECASE,
    ):
        return "Sponsorship available"
    return None


def assess_eligibility(title: object, description: object = "") -> EligibilityAssessment:
    title_text = description_text(title)
    body_text = description_text(description)
    combined = f"{title_text} {body_text}".strip()
    levels = _degree_levels(combined)
    has_bachelor = "Bachelor's" in levels
    title_postgraduate_only = bool(re.search(
        r"\b(?:ph\.?d|doctoral|master'?s?|postgraduate)\b",
        title_text,
        re.IGNORECASE,
    )) and not bool(re.search(
        r"\b(?:bachelor'?s?|undergraduate|b\.?s\.?)\b",
        title_text,
        re.IGNORECASE,
    ))

    exclusive_postgraduate = bool(re.search(
        r"\b(?:ph\.?d|doctoral|master'?s?|postgraduate)\s+"
        r"(?:students?|candidates?)\s+only\b|"
        r"\b(?:must|should)\s+be\s+(?:currently\s+)?pursuing\s+"
        r"(?:a\s+)?(?:ph\.?d|doctorate|master'?s?|postgraduate\s+degree)\b|"
        r"\bcurrently\s+pursuing\s+(?:a\s+)?(?:ph\.?d|doctorate|master'?s?)\b",
        combined,
        re.IGNORECASE,
    ))

    reasons = []
    if (title_postgraduate_only or exclusive_postgraduate) and not has_bachelor:
        verdict = "ineligible"
        reasons.append("Posting is explicitly limited to postgraduate candidates")
    elif has_bachelor or re.search(
        r"\b(?:undergraduate|university|college)\s+students?\b|"
        r"\bcurrently\s+pursuing\s+(?:a\s+)?degree\b",
        combined,
        re.IGNORECASE,
    ):
        verdict = "likely_eligible"
        reasons.append("Posting explicitly includes undergraduate-level candidates")
    else:
        verdict = "unknown"
        reasons.append("No explicit undergraduate eligibility requirement found")

    return EligibilityAssessment(
        verdict=verdict,
        degree_levels=levels,
        graduation_years=_graduation_years(combined),
        duration=_duration(combined),
        work_authorization=_work_authorization(combined),
        reasons=reasons,
    )
