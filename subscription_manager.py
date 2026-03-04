import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import toml

SHEET_HEADERS = [
    "id", "email", "advertiser_keyword", "geography", "platforms",
    "created_at", "last_notified_at", "last_seen_ad_ids",
]

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
        last_seen = row[7] if len(row) > 7 else "[]"
        try:
            last_seen_ids = json.loads(last_seen) if last_seen else []
        except json.JSONDecodeError:
            last_seen_ids = []
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
        json.dumps(sub.get("last_seen_ad_ids", [])),
    ]


def _load_from_sheets() -> dict:
    sh = _sheet_client()
    rows = sh.get_all_values()
    if not rows or rows[0] != SHEET_HEADERS:
        return {}
    out = {}
    for r in rows[1:]:
        sub = _row_to_sub(r)
        if sub and sub.get("id"):
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
    ids_to_store = list(ad_ids)[-1500:] if ad_ids else []
    value_h = json.dumps(ids_to_store)
    payload = [[timestamp, value_h]]

    if sheet_row_number is not None and sheet_row_number >= 2:
        row_num = int(sheet_row_number)
        try:
            sh.update(f"G{row_num}:H{row_num}", payload)
        except Exception:
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
