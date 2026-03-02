import json
import os
import uuid
from datetime import datetime
from pathlib import Path

SUBSCRIPTIONS_FILE = "subscriptions.json"


def load_subscriptions() -> dict:
    if not Path(SUBSCRIPTIONS_FILE).exists():
        return {}
    with open(SUBSCRIPTIONS_FILE, "r") as f:
        return json.load(f)


def save_subscriptions(subscriptions: dict):
    with open(SUBSCRIPTIONS_FILE, "w") as f:
        json.dump(subscriptions, f, indent=2, default=str)


def add_subscription(email: str, advertiser_keyword: str = "", geography: str = "", platforms: list = None) -> str:
    """Add a new subscription. Returns the subscription ID, or None if duplicate."""
    subscriptions = load_subscriptions()

    for sub_id, sub in subscriptions.items():
        if (sub["email"].lower() == email.lower()
                and sub["advertiser_keyword"].lower() == advertiser_keyword.lower()
                and sub["geography"].lower() == geography.lower()):
            return None

    sub_id = str(uuid.uuid4())
    subscriptions[sub_id] = {
        "id": sub_id,
        "email": email,
        "advertiser_keyword": advertiser_keyword,
        "geography": geography,
        "platforms": platforms or ["Google", "Meta", "X"],
        "created_at": datetime.utcnow().isoformat(),
        "last_notified_at": None,
        "last_seen_ad_ids": [],
    }
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


def update_last_seen(sub_id: str, ad_ids: list, timestamp: str):
    subscriptions = load_subscriptions()
    if sub_id in subscriptions:
        subscriptions[sub_id]["last_seen_ad_ids"] = ad_ids
        subscriptions[sub_id]["last_notified_at"] = timestamp
        save_subscriptions(subscriptions)