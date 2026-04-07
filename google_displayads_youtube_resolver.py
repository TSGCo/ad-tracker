"""
Resolve Google Ads creative preview URLs (displayads-formats.googleusercontent.com)
to direct media links, especially YouTube watch URLs.

Logic follows the SerpApi / Terry Tan approach: fetch the content.js response, unescape
embedded HTML strings, then parse YouTube video IDs from common embed patterns.

Note: YouTube "unlisted" is a visibility flag on YouTube's side; this script only
resolves the canonical watch URL (e.g. https://www.youtube.com/watch?v=...) from
Transparency-style preview links.

Creative pages on https://adstransparency.google.com/.../creative/CR... do not embed the
displayads ``content.js`` URL in static HTML; use optional Playwright to capture it from
network responses (``pip install playwright`` then ``playwright install chromium``).
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import re
import sys
from typing import Any
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30
SESSION_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/javascript,*/*;q=0.8",
}

UNESCAPE_CHARS: dict[str, str] = {}

UNESCAPE_CHARS["\\0"] = "\0"
UNESCAPE_CHARS["\\a"] = "\a"
UNESCAPE_CHARS["\\b"] = "\b"
UNESCAPE_CHARS["\\t"] = "\t"
UNESCAPE_CHARS["\\n"] = "\n"
UNESCAPE_CHARS["\\v"] = "\v"
UNESCAPE_CHARS["\\f"] = "\f"
UNESCAPE_CHARS["\\r"] = "\r"
UNESCAPE_CHARS["\\u"] = "\\u"
UNESCAPE_CHARS["\\x"] = "\\x"

JS_UNESCAPES = {
    "\\u005c": "\\",
    "\\u0027": "'",
    "\\u0022": '"',
    "\\u003e": ">",
    "\\u003c": "<",
    "\\u0026": "&",
    "\\u003d": "=",
    "\\u002d": "-",
    "\\u003b": ";",
    "\\u0060": "`",
}

ESCAPED_CONTROL_CHARS = [r"\u0003", r"\u000b", r"\u0019", r"\u001d", r"\u001c"]
CONTROL_CHARS = [json.loads(f'"{m}"') for m in ESCAPED_CONTROL_CHARS]


def unescape_js(string: str) -> str:
    s = string
    s = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m.group(1), 8)), s)
    s = re.sub(
        r"\\x([0-9a-fA-F]{1,2})",
        lambda m: chr(int(m.group(1), 16)),
        s,
    )

    def _decode_u_run(m: re.Match[str]) -> str:
        chunk = m.group(0)
        try:
            return json.loads(f'"{chunk}"')
        except json.JSONDecodeError:
            return chunk

    s2 = re.sub(r"(?<!\\)(\\u[0-9a-fA-F]{4})+", _decode_u_run, s)
    try:
        s2.encode("utf-8")
        s = s2
    except UnicodeEncodeError:
        s = re.sub(r"(\\u[0-9a-fA-F]{4})+", _decode_u_run, s)

    keys = "|".join(re.escape(k) for k in JS_UNESCAPES)
    s = re.sub(
        rf"(?<!\\)(\\(?:{keys}))",
        lambda m: JS_UNESCAPES.get(m.group(1).lower(), m.group(0)),
        s,
        flags=re.IGNORECASE,
    )

    esc_union = "|".join(re.escape(x) for x in ESCAPED_CONTROL_CHARS)
    s = re.sub(rf"(?<!\\)(\\(?:{esc_union}))", "", s, flags=re.IGNORECASE)

    ctrl_union = "|".join(re.escape(c) for c in CONTROL_CHARS)
    if ctrl_union:
        s = re.sub(rf"({ctrl_union})", "", s)

    s = re.sub(r"\\.", lambda m: UNESCAPE_CHARS.get(m.group(0), m.group(0)[1:]), s)
    return s


def prioritize_video_url(url: str, strip_ui_features: bool = True) -> str:
    """Optionally drop uiFeatures (per SerpApi blog) to prefer video/HTML preview paths."""
    if not strip_ui_features:
        return url
    parsed = urlparse(url)
    q = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != "uiFeatures"]
    new_query = urlencode(q)
    return urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )


_RE_PREVIEW_IMAGE = re.compile(
    r"previewservice\.insertPreviewImageContent\('fletch.+', 'fletch.+', '(.+?)'",
    re.DOTALL,
)
_RE_PREVIEW_HTML = re.compile(
    r"previewservice\.insertPreviewHtmlContent\('fletch.+', 'fletch.+', '(.+?)'",
    re.DOTALL,
)
_RE_YT_LIMA = re.compile(r"yt_video_id': '(.+?)'")
_RE_YT_AD_DATA = re.compile(r"(?:video_id|video_videoId)': '(.+?)'")
_RE_GOOGLEVIDEO = re.compile(
    r"CDATA\[(https?://(?:[\w-]+\.)?googlevideo\.com[^\s]*?)\]"
)
_RE_SEEDTAG = re.compile(r"CDATA\[(https://s\.seedtag\.com.+?)\]")
_RE_SEEDTAG_XML = re.compile(r"https\S+\.xml")
_RE_SEEDTAG_CDATA = re.compile(r"\[CDATA\[(.+?)\]\]>")


def extract_media(
    url: str,
    session: requests.Session | None = None,
    *,
    strip_ui_features: bool = True,
) -> dict[str, Any] | None:
    """
    Fetch a displayads-formats content.js URL and return a dict:
    - {"type": "url", "value": "<direct url>"} for image / youtube / video / seedtag
    - {"type": "html", "value": "<html string>"} when only raw HTML is available
    Returns None on HTTP/network errors.
    """
    sess = session or requests.Session()
    candidates = [prioritize_video_url(url, strip_ui_features=strip_ui_features)]
    if strip_ui_features and candidates[0] != url:
        candidates.append(url)

    content: str | None = None
    last_status: int | None = None
    for fetch_url in candidates:
        try:
            r = sess.get(fetch_url, headers=SESSION_HEADERS, timeout=DEFAULT_TIMEOUT)
        except requests.RequestException as e:
            logger.warning("Request failed for %s: %s", fetch_url, e)
            return None
        last_status = r.status_code
        if r.status_code == 200:
            content = r.text
            break
        logger.debug("HTTP %s for %s", r.status_code, fetch_url)

    if content is None:
        logger.warning("HTTP %s (tried %s variant(s))", last_status, len(candidates))
        return None

    m_img = _RE_PREVIEW_IMAGE.search(content)
    if m_img:
        return {"type": "url", "value": m_img.group(1)}

    m_html = _RE_PREVIEW_HTML.search(content)
    if not m_html:
        return None

    html = unescape_js(m_html.group(1))

    if "lima-exp-data" in html:
        vid = _RE_YT_LIMA.search(html)
        if vid:
            return {"type": "url", "value": f"https://www.youtube.com/watch?v={vid.group(1)}"}
    if "youtube" in html and "var adData" in html:
        vid = _RE_YT_AD_DATA.search(html)
        if vid:
            return {"type": "url", "value": f"https://www.youtube.com/watch?v={vid.group(1)}"}
    if "googlevideo.com" in html:
        gv = _RE_GOOGLEVIDEO.search(html)
        if gv:
            return {"type": "url", "value": gv.group(1)}
    if "seedtag.com" in html:
        st_m = _RE_SEEDTAG.search(html)
        if st_m:
            seed_url = st_m.group(1)
            try:
                st_r = sess.get(seed_url, headers=SESSION_HEADERS, timeout=DEFAULT_TIMEOUT)
                st_r.raise_for_status()
                xml_u = _RE_SEEDTAG_XML.search(st_r.text)
                if xml_u:
                    xml_r = sess.get(xml_u.group(0), headers=SESSION_HEADERS, timeout=DEFAULT_TIMEOUT)
                    xml_r.raise_for_status()
                    found = _RE_SEEDTAG_CDATA.findall(xml_r.text)
                    if found:
                        return {"type": "url", "value": found[-1]}
            except requests.RequestException as e:
                logger.warning("Seedtag chain failed: %s", e)

    return {"type": "html", "value": html}


def extract_youtube_watch_url(
    url: str,
    session: requests.Session | None = None,
    *,
    strip_ui_features: bool = True,
) -> str | None:
    """If the creative resolves to a youtube.com watch URL, return it; else None."""
    result = extract_media(url, session=session, strip_ui_features=strip_ui_features)
    if not result or result.get("type") != "url":
        return None
    val = result.get("value") or ""
    if "youtube.com/watch" in val or "youtu.be/" in val:
        return val
    return None


DISPLAYADS_URL_RE = re.compile(
    r"https://displayads-formats\.googleusercontent\.com/[^\s\"'<>]+",
    re.IGNORECASE,
)


def find_displayads_urls(text: str) -> list[str]:
    return list(dict.fromkeys(DISPLAYADS_URL_RE.findall(text)))


TRANSPARENCY_CREATIVE_URL_RE = re.compile(
    r"https://adstransparency\.google\.com/advertiser/AR\d+/creative/CR\d+[^\s\"'<>]*",
    re.IGNORECASE,
)


def find_transparency_creative_urls(text: str) -> list[str]:
    return list(dict.fromkeys(TRANSPARENCY_CREATIVE_URL_RE.findall(text)))


def is_transparency_creative_url(url: str) -> bool:
    p = urlparse(url.strip())
    if "adstransparency.google.com" not in (p.netloc or "").lower():
        return False
    return bool(re.match(r"/advertiser/AR\d+/creative/CR\d+", p.path or "", re.I))


def normalize_displayads_url(url: str) -> str:
    """Fix HTML-escaped query strings (e.g. ``&amp;``) from DOM snapshots."""
    return html.unescape(url.strip())


def fetch_displayads_urls_from_transparency_creative_page(
    page_url: str,
    *,
    wait_ms: int = 10_000,
    navigation_timeout_ms: int = 60_000,
) -> list[str]:
    """
    Load a Transparency Center creative page and return ``content.js`` preview URLs.

    Requires Playwright + Chromium (not installed with the base ``requests`` stack).
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise ImportError(
            "Transparency Center creative URLs need a headless browser to obtain the "
            "preview link. Install: pip install playwright && playwright install chromium"
        ) from e

    ordered: list[str] = []
    seen: set[str] = set()

    def add(raw: str) -> None:
        u = normalize_displayads_url(raw)
        if not u or "displayads-formats.googleusercontent.com" not in u:
            return
        if "content.js" not in u:
            return
        if u not in seen:
            seen.add(u)
            ordered.append(u)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()

            def on_response(resp) -> None:
                add(resp.url)

            page.on("response", on_response)
            page.goto(page_url, wait_until="domcontentloaded", timeout=navigation_timeout_ms)
            page.wait_for_timeout(wait_ms)
            for u in find_displayads_urls(page.content()):
                add(u)
        finally:
            browser.close()

    return ordered


def resolve_transparency_creative_page(
    transparency_url: str,
    session: requests.Session | None = None,
    *,
    strip_ui_features: bool = True,
    wait_ms: int = 10_000,
) -> tuple[dict[str, Any] | None, str | None]:
    """
    Open a ``adstransparency.google.com/.../creative/CR...`` page and resolve to media.

    Returns ``(extract_media result, content_js_url_used)`` or ``(None, None)``.
    """
    content_urls = fetch_displayads_urls_from_transparency_creative_page(
        transparency_url, wait_ms=wait_ms
    )
    if not content_urls:
        return None, None
    sess = session or requests.Session()
    fallback: tuple[dict[str, Any], str] | None = None
    for cu in content_urls:
        result = extract_media(cu, sess, strip_ui_features=strip_ui_features)
        if result is None:
            continue
        val = str(result.get("value") or "")
        if result.get("type") == "url" and ("youtube.com/watch" in val or "youtu.be/" in val):
            return result, cu
        if fallback is None:
            fallback = (result, cu)
    if fallback:
        return fallback[0], fallback[1]
    return None, content_urls[0]


def _normalize_youtube_url(url: str) -> str:
    u = url.strip()
    if "youtu.be/" in u:
        m = re.search(r"youtu\.be/([a-zA-Z0-9_-]+)", u)
        if m:
            return f"https://www.youtube.com/watch?v={m.group(1)}"
    return u


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Resolve Google ad preview URLs to direct media: "
            "displayads-formats content.js URLs, or adstransparency.google.com creative pages (Playwright)."
        )
    )
    parser.add_argument(
        "urls",
        nargs="*",
        help="One or more content.js URLs or adstransparency.google.com .../creative/CR... URLs",
    )
    parser.add_argument(
        "-f",
        "--file",
        help="Read text from file; displayads and Transparency creative URLs inside are resolved",
    )
    parser.add_argument(
        "--youtube-only",
        action="store_true",
        help=(
            "Print only video targets: youtube.com/watch (or youtu.be) and direct "
            "googlevideo.com stream URLs from YouTube-served creatives"
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON lines: input_url, result_type, value",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
    )
    parser.add_argument(
        "--no-strip-ui-features",
        action="store_true",
        help="Do not remove uiFeatures= from the URL (default strips per SerpApi blog, then retries original on non-200).",
    )
    parser.add_argument(
        "--transparency-wait-ms",
        type=int,
        default=10_000,
        metavar="MS",
        help="After DOM load, wait this many ms for preview requests (Transparency Center + Playwright only).",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING)

    to_resolve: list[str] = []
    if args.file:
        with open(args.file, encoding="utf-8", errors="replace") as f:
            blob = f.read()
        to_resolve.extend(find_displayads_urls(blob))
        to_resolve.extend(find_transparency_creative_urls(blob))
    to_resolve.extend(args.urls)

    # De-dupe preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for u in to_resolve:
        u = u.strip()
        if u and u not in seen:
            seen.add(u)
            unique.append(u)

    if not unique:
        print(
            "No URLs to resolve. Pass content.js or Transparency creative URLs, or -f with such links in the file.",
            file=sys.stderr,
        )
        return 1

    strip_ui = not args.no_strip_ui_features
    session = requests.Session()
    exit_code = 0
    for u in unique:
        content_js_used: str | None = None
        if is_transparency_creative_url(u):
            try:
                result, content_js_used = resolve_transparency_creative_page(
                    u,
                    session=session,
                    strip_ui_features=strip_ui,
                    wait_ms=args.transparency_wait_ms,
                )
            except ImportError as e:
                exit_code = 1
                print(f"# {u}", file=sys.stderr)
                print(str(e), file=sys.stderr)
                continue
            if result is None:
                exit_code = 1
                if args.json:
                    print(
                        json.dumps(
                            {
                                "input": u,
                                "error": "no_preview_url",
                                "hint": "Try increasing --transparency-wait-ms or check the page in a browser.",
                            }
                        )
                    )
                else:
                    print(f"# failed (no content.js captured): {u}", file=sys.stderr)
                continue
        else:
            result = extract_media(u, session=session, strip_ui_features=strip_ui)
            if result is None:
                exit_code = 1
                if args.json:
                    print(json.dumps({"input": u, "error": "request_failed"}))
                else:
                    print(f"# failed: {u}", file=sys.stderr)
                continue

        rtype = result["type"]
        val = result.get("value", "")

        if args.youtube_only:
            if rtype != "url":
                pass
            elif "youtube.com/watch" in val or "youtu.be/" in val:
                print(_normalize_youtube_url(val))
            elif "googlevideo.com" in val and (
                "source=youtube" in val or "source%3Dyoutube" in val
            ):
                print(val)
            else:
                # try to scrape watch / embed links from returned HTML
                if isinstance(val, str):
                    for m in re.finditer(
                        r"(?:https?://)?(?:www\.)?youtube\.com/watch\?v=[a-zA-Z0-9_-]+",
                        val,
                    ):
                        found = m.group(0)
                        print(found if found.startswith("http") else "https://" + found.lstrip("/"))
                    for m in re.finditer(
                        r"(?:https?://)?(?:www\.)?youtube\.com/embed/([a-zA-Z0-9_-]+)",
                        val,
                    ):
                        print(f"https://www.youtube.com/watch?v={m.group(1)}")
        elif args.json:
            out: dict[str, Any] = {"input": u, "type": rtype}
            if content_js_used:
                out["content_js"] = content_js_used
            if rtype == "html" and isinstance(val, str) and len(val) > 2000:
                out["value_preview"] = val[:2000] + "..."
            else:
                out["value"] = val
            print(json.dumps(out, ensure_ascii=False))
        else:
            print(f"# {u}")
            if content_js_used:
                print(f"content.js={content_js_used}")
            print(f"type={rtype}")
            if rtype == "html" and isinstance(val, str) and len(val) > 500:
                print(val[:500] + "\n... [truncated]")
            else:
                print(val)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
