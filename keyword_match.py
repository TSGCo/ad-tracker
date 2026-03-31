"""Advertiser keyword matching with word boundaries (phrase-aware)."""

import re


def advertiser_keyword_boundary_pattern(keyword: str) -> str:
    """
    Regex for use with LOWER(text) or case-insensitive matching.
    \\b prevents "Barr" from matching "Barry", "Barraza", "Barringer", etc.
    """
    kw = (keyword or "").strip()
    if not kw:
        return ""
    return rf"\b{re.escape(kw.lower())}\b"


def text_matches_advertiser_keyword(text: str, keyword: str) -> bool:
    if not (keyword or "").strip():
        return True
    pat = advertiser_keyword_boundary_pattern(keyword)
    if not pat:
        return True
    return bool(re.search(pat, (text or "").lower()))
