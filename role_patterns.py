"""Shared title normalization and student-role vocabulary."""

import re
import unicodedata


STUDENT_ROLE_PATTERN = (
    r"\b(interns?|internships?|co\s*op|industrial attachments?|technical trainee|"
    r"summer analyst|off\s+cycle analyst|winternships?|spring insight|"
    r"insight program(?:me)?|apprenticeships?|traineeships?|trainees?|"
    r"accelerator program(?:me)?)\b"
)


def normalize_role_title(title: object) -> str:
    text = unicodedata.normalize("NFKC", str(title or "")).casefold()
    text = re.sub(r"[-‐‑‒–—_/]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return re.sub(r"\b(front|back|full)\s+(end|stack)\b", r"\1\2", text)
