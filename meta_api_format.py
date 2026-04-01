"""Normalize Meta Ads Archive API fields for display, CSV export, and filtering."""

from __future__ import annotations

import json
import re
from urllib.parse import parse_qs, urlparse

# Shown when Meta omits delivery_by_region / demographic_distribution (common on low-reach ads).
META_MISSING_DETAIL = "Not reported by Meta"


def meta_or_placeholder(text: str) -> str:
    t = (text or "").strip()
    return t if t else META_MISSING_DETAIL


def meta_ad_library_url(ad_id: str) -> str:
    """Public Ads Library page for a creative (works in browser; render_ad links often break)."""
    aid = (ad_id or "").strip()
    if not aid:
        return ""
    return f"https://www.facebook.com/ads/library/?id={aid}"


def clean_meta_ad_url(url: str | None, ad_id: str) -> str:
    """Ads Library URL without access_token (token must not appear in exports or UI)."""
    aid = (ad_id or "").strip()
    if aid:
        return meta_ad_library_url(aid)
    if not url:
        return ""
    try:
        parsed = urlparse(str(url).strip())
        ids = parse_qs(parsed.query).get("id", [])
        if ids:
            return meta_ad_library_url(ids[0])
    except Exception:
        pass
    return str(url).split("?", 1)[0]


def meta_bound_mid(val) -> float:
    """Midpoint of Meta lower_bound/upper_bound for numeric filters and sorting."""
    if val is None or val == "":
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    d = val
    if isinstance(val, str):
        s = val.strip()
        if not s or s == "{}":
            return 0.0
        try:
            d = json.loads(s)
        except json.JSONDecodeError:
            try:
                return float(s)
            except ValueError:
                return 0.0
    if not isinstance(d, dict):
        return 0.0
    lo = d.get("lower_bound") or d.get("lower")
    hi = d.get("upper_bound") or d.get("upper")
    try:
        lo_f = float(lo) if lo is not None and str(lo).strip() != "" else None
        hi_f = float(hi) if hi is not None and str(hi).strip() != "" else None
    except (TypeError, ValueError):
        return 0.0
    if lo_f is not None and hi_f is not None:
        return (lo_f + hi_f) / 2.0
    if lo_f is not None:
        return lo_f
    if hi_f is not None:
        return hi_f
    return 0.0


def meta_display_range_to_mid(val) -> float:
    """Map a Meta range label (or raw API value) to a number for min/max spend filters."""
    if val is None or val == "":
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s or s == "{}":
        return 0.0
    if s.startswith("{"):
        return meta_bound_mid(s)
    if s.startswith("≥"):
        m = re.match(r"≥\s*([\d.]+)", s)
        return float(m.group(1)) if m else 0.0
    if s.startswith("≤"):
        m = re.match(r"≤\s*([\d.]+)", s)
        return float(m.group(1)) if m else 0.0
    m = re.match(r"^([\d.]+)\s*[–\-]\s*([\d.]+)\s*$", s)
    if m:
        a, b = float(m.group(1)), float(m.group(2))
        return (a + b) / 2.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def meta_bound_label(val) -> str:
    """Human-readable range for CSV/table (empty if unknown)."""
    if val is None or val == "":
        return ""
    d = val
    if isinstance(val, str):
        s = val.strip()
        if not s or s == "{}":
            return ""
        try:
            d = json.loads(s)
        except json.JSONDecodeError:
            return s
    if not isinstance(d, dict):
        return str(val)
    lo = d.get("lower_bound") or d.get("lower")
    hi = d.get("upper_bound") or d.get("upper")
    if lo is not None and hi is not None and str(lo) != "" and str(hi) != "":
        return f"{lo}–{hi}"
    if lo is not None and str(lo) != "":
        return f"≥{lo}"
    if hi is not None and str(hi) != "":
        return f"≤{hi}"
    return ""


def format_meta_demographics(demo) -> str:
    """Single summary for demographic_distribution (list of age/gender/percentage cells)."""
    if demo is None:
        return ""
    if isinstance(demo, list):
        if not demo:
            return ""
        cells = [x for x in demo if isinstance(x, dict)]
        if not cells:
            return ""
        try:
            ranked = sorted(
                cells,
                key=lambda x: -float(x.get("percentage") or 0),
            )
        except (TypeError, ValueError):
            return f"{len(cells)} segments"
        parts = []
        for x in ranked:
            try:
                pct = float(x.get("percentage") or 0) * 100
            except (TypeError, ValueError):
                pct = 0.0
            age = x.get("age") or "?"
            gender = x.get("gender") or "?"
            parts.append(f"{age} {gender}: {pct:.1f}%")
        return "; ".join(parts)
    if isinstance(demo, dict):
        if not demo:
            return ""
        g = demo.get("gender") or demo.get("genders")
        a = demo.get("age") or demo.get("ages")
        if g is not None or a is not None:
            parts = [str(p) for p in (g, a) if p is not None]
            return " / ".join(parts)
        return ""
    return str(demo)[:500]


def meta_metric_display(val) -> str:
    """Email / plain text: Meta-reported range only (no midpoint)."""
    return meta_bound_label(val)
