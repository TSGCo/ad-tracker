import json
import logging
import math
import os
import re
import uuid
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

import toml

_logger = logging.getLogger(__name__)

_SCI_NOTATION_RE = re.compile(r"^[-+]?(?:\d+\.?\d*|\.\d+)[eE][-+]?\d+$")


def canonical_ad_id(raw) -> str:
    if raw is None:
        return ""
    if isinstance(raw, bool):
        return ""
    if isinstance(raw, int):
        return str(raw)
    if isinstance(raw, float):
        if math.isnan(raw):
            return ""
        try:
            if raw == int(raw):
                return str(int(raw))
        except OverflowError:
            pass
        return str(raw).strip()
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "nat", "null"):
        return ""
    if s.upper().startswith("CR"):
        return s
    if _SCI_NOTATION_RE.match(s):
        try:
            return str(int(Decimal(s)))
        except (InvalidOperation, ValueError, OverflowError):
            return s
    if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
        try:
            return str(int(s))
        except ValueError:
            return s
    try:
        f = float(s)
        if not math.isnan(f) and f == int(f):
            return str(int(f))
    except ValueError:
        pass
    return s

SHEET_HEADERS = [
    "id", "email", "advertiser_keyword", "geography", "platforms",
    "created_at", "last_notified_at", "last_seen_ad_ids",
]

_MAX_LAST_SEEN_CELL_CHARS = 48_000


def _parse_last_seen_ids(cell_value: str) -> list:
    if not cell_value or not str(cell_value).strip():
        return []
    s = str(cell_value).strip()
    if s.startswith("["):
        try:
            out = json.loads(s)
            parsed = [canonical_ad_id(x) for x in out if x is not None and str(x).strip()]
            return [x for x in parsed if x]
        except json.JSONDecodeError:
            return []
    parsed = [canonical_ad_id(ln) for ln in s.splitlines() if ln.strip()]
    return [x for x in parsed if x]


def _serialize_last_seen_ids_for_cell(ids: list) -> str:
    strings = []
    for x in ids or []:
        t = canonical_ad_id(x)
        if t:
            strings.append(t)
    seen = set()
    deduped = []
    for t in reversed(strings):
        if t not in seen:
            seen.add(t)
            deduped.append(t)
    strings = list(reversed(deduped))
    parts = []
    total = 0
    for t in reversed(strings):
        sep = 1 if parts else 0
        if total + sep + len(t) > _MAX_LAST_SEEN_CELL_CHARS:
            break
        parts.append(t)
        total += sep + len(t)
    kept = list(reversed(parts))
    return "\n".join(kept)

_injected_spreadsheet_id = None
_injected_gcp = None


def set_sheets_config_from_app(spreadsheet_id: Optional[str], gcp_service_account: Optional[dict]):
    global _injected_spreadsheet_id, _injected_gcp
    _injected_spreadsheet_id = (spreadsheet_id or "").strip() or None
    _injected_gcp = gcp_service_account if isinstance(gcp_service_account, dict) else None


def _get_sheets_config():
    spreadsheet_id = _injected_spreadsheet_id or os.environ.get("SPREADSHEET_ID", "").strip()
    gcp_secrets = _injected_gcp or {}

    if not gcp_secrets:
        gcp_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        if gcp_json:
            try:
                gcp_secrets = json.loads(gcp_json)
            except json.JSONDecodeError:
                pass
    if not gcp_secrets:
        secrets_path = Path(".streamlit/secrets.toml")
        if secrets_path.exists():
            try:
                secrets = toml.load(secrets_path)
                if not spreadsheet_id:
                    spreadsheet_id = (secrets.get("spreadsheet_id") or "").strip()
                if not gcp_secrets:
                    gcp_secrets = secrets.get("gcp_service_account") or {}
            except Exception:
                pass
    if not gcp_secrets:
        gcp_path = Path(".streamlit/gcp_service_account.json")
        if gcp_path.exists():
            try:
                with open(gcp_path) as f:
                    gcp_secrets = json.load(f)
            except Exception:
                pass
    return (spreadsheet_id or None), gcp_secrets


def is_sheets_configured() -> bool:
    _id, gcp = _get_sheets_config()
    return bool(_id and gcp)


def _sheet_client():
    import gspread
    _id, gcp = _get_sheets_config()
    if not _id or not gcp:
        raise ValueError(
            "Subscriptions require Google Sheets. Set SPREADSHEET_ID and GCP credentials "
            "(Streamlit: spreadsheet_id + gcp_service_account in secrets; "
            "GitHub Actions: SPREADSHEET_ID + GCP_SERVICE_ACCOUNT_JSON)."
        )
    gc = gspread.service_account_from_dict(gcp)
    return gc.open_by_key(_id).sheet1


def _row_to_sub(row: list) -> Optional[dict]:
    if len(row) < len(SHEET_HEADERS):
        return None
    try:
        platforms_str = row[4] or "Google,Meta,X"
        platforms = [p.strip() for p in platforms_str.split(",") if p.strip()]
        last_seen = row[7] if len(row) > 7 else ""
        last_seen_ids = _parse_last_seen_ids(last_seen)
        return {
            "id": row[0],
            "email": row[1] or "",
            "advertiser_keyword": row[2] or "",
            "geography": row[3] or "",
            "platforms": platforms or ["Google", "Meta", "X"],
            "created_at": row[5] or "",
            "last_notified_at": row[6] if len(row) > 6 and row[6] else None,
            "last_seen_ad_ids": last_seen_ids,
        }
    except (IndexError, TypeError):
        return None


def _sub_to_row(sub: dict) -> list:
    return [
        sub.get("id", ""),
        sub.get("email", ""),
        sub.get("advertiser_keyword", ""),
        sub.get("geography", ""),
        ",".join(sub.get("platforms", [])),
        sub.get("created_at", ""),
        sub.get("last_notified_at") or "",
        _serialize_last_seen_ids_for_cell(sub.get("last_seen_ad_ids", [])),
    ]


def _load_from_sheets() -> dict:
    sh = _sheet_client()
    rows = sh.get_all_values()
    if not rows or rows[0] != SHEET_HEADERS:
        return {}
    out = {}
    for row_num, r in enumerate(rows[1:], start=2):
        sub = _row_to_sub(r)
        if sub and sub.get("id"):
            sub["sheet_row_number"] = row_num
            out[sub["id"]] = sub
    return out


def _save_to_sheets(subscriptions: dict):
    sh = _sheet_client()
    rows = [SHEET_HEADERS]
    for sub in subscriptions.values():
        rows.append(_sub_to_row(sub))
    if rows:
        sh.update(rows, "A1")


def _ensure_sheet_headers():
    sh = _sheet_client()
    rows = sh.get_all_values()
    if not rows or rows[0] != SHEET_HEADERS:
        sh.update([SHEET_HEADERS], "A1")


def load_subscriptions() -> dict:
    return _load_from_sheets()


def save_subscriptions(subscriptions: dict):
    _save_to_sheets(subscriptions)


def add_subscription(
    email: str,
    advertiser_keyword: str = "",
    geography: str = "",
    platforms: list = None,
) -> Optional[str]:
    subscriptions = load_subscriptions()

    for sub in subscriptions.values():
        if (
            sub["email"].lower() == email.lower()
            and (sub.get("advertiser_keyword") or "").lower() == (advertiser_keyword or "").lower()
            and (sub.get("geography") or "").lower() == (geography or "").lower()
        ):
            return None

    sub_id = str(uuid.uuid4())
    subscriptions[sub_id] = {
        "id": sub_id,
        "email": email,
        "advertiser_keyword": advertiser_keyword or "",
        "geography": geography or "",
        "platforms": platforms or ["Google", "Meta", "X"],
        "created_at": datetime.utcnow().isoformat(),
        "last_notified_at": None,
        "last_seen_ad_ids": [],
    }
    _ensure_sheet_headers()
    save_subscriptions(subscriptions)
    return sub_id


def remove_subscription(sub_id: str) -> bool:
    subscriptions = load_subscriptions()
    if sub_id in subscriptions:
        del subscriptions[sub_id]
        save_subscriptions(subscriptions)
        return True
    return False


def get_subscriptions_for_email(email: str) -> list:
    subscriptions = load_subscriptions()
    return [s for s in subscriptions.values() if s["email"].lower() == email.lower()]


def update_last_seen(sub_id: str, ad_ids: list, timestamp: str, sheet_row_number: Optional[int] = None):
    sh = _sheet_client()
    value_h = _serialize_last_seen_ids_for_cell(list(ad_ids) if ad_ids else [])
    payload = [[timestamp, value_h]]

    if sheet_row_number is not None and sheet_row_number >= 2:
        row_num = int(sheet_row_number)
        try:
            sh.update(f"G{row_num}:H{row_num}", payload)
        except Exception as e:
            _logger.warning("update_last_seen row update failed: %s", e)
            _update_last_seen_by_id(sh, sub_id, payload)
        return

    rows = sh.get_all_values()
    if not rows or rows[0] != SHEET_HEADERS:
        return
    _update_last_seen_by_id(sh, sub_id, payload, rows)


def _update_last_seen_by_id(sh, sub_id: str, payload: list, rows: Optional[list] = None):
    if rows is None:
        rows = sh.get_all_values()
    if not rows or rows[0] != SHEET_HEADERS:
        return
    try:
        id_col = rows[0].index("id")
    except ValueError:
        id_col = 0
    sub_id_str = str(sub_id).strip()
    for i in range(1, len(rows)):
        row = rows[i]
        if id_col >= len(row):
            continue
        row_id = (row[id_col] or "").strip()
        if row_id == sub_id_str:
            row_num = i + 1
            sh.update(f"G{row_num}:H{row_num}", payload)
            return
