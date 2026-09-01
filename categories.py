import re


CATEGORY_ORDER = ("QUANT", "AI/ML", "DATA", "SWE", "TECH")

_QUANT_ROLE = re.compile(
    r"\b(quant(?:itative)?|quant research|quant developer|algorithmic trad(?:er|ing)|"
    r"systematic trad(?:er|ing)|trading intern|trader intern)\b",
    re.IGNORECASE,
)
_AI_ML_ROLE = re.compile(
    r"\b(machine learning|artificial intelligence|generative ai|genai|deep learning|"
    r"reinforcement learning|computer vision|natural language processing|nlp|"
    r"large language model|llm|model efficiency|ml engineer|ai engineer|ai/ml)\b",
    re.IGNORECASE,
)
_DATA_ROLE = re.compile(
    r"\b(data (?:engineer|engineering|scientist|science|analyst|analytics)|"
    r"analytics engineer|business intelligence|bi engineer)\b",
    re.IGNORECASE,
)
_SWE_ROLE = re.compile(
    r"\b(software|swe|developer|development engineer|backend|back-end|frontend|front-end|"
    r"full[ -]?stack|platform engineer|infrastructure engineer|devops|site reliability|sre|"
    r"security engineer|cybersecurity|cloud engineer|systems engineer|programmer|"
    r"firmware|embedded|robotics|information systems?|enterprise architecture|"
    r"solutions? architect|mobile engineer|web engineer)\b",
    re.IGNORECASE,
)


def classify_job_categories(job: dict) -> list[str]:
    """Return all matching job categories in stable display order."""
    title = str(job.get("title") or "")
    source_tags = {
        str(tag).upper()
        for tag in (job.get("source_tags") or [])
        if tag
    }
    matches = {
        "QUANT": "QUANT" in source_tags or bool(_QUANT_ROLE.search(title)),
        "AI/ML": bool(_AI_ML_ROLE.search(title)),
        "DATA": bool(_DATA_ROLE.search(title)),
        "SWE": bool(_SWE_ROLE.search(title)),
    }
    categories = [category for category in CATEGORY_ORDER[:-1] if matches[category]]
    return categories or ["TECH"]


def primary_category(job: dict) -> str:
    categories = job.get("categories") or classify_job_categories(job)
    category_set = set(categories)
    return next(
        (category for category in CATEGORY_ORDER if category in category_set),
        "TECH",
    )
