import smtplib
import logging
import json
import re
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
import requests
import time

from subscription_manager import load_subscriptions, update_last_seen
from x_ads_scraper import (
    download_and_extract_csv,
    filter_by_advertiser,
    standardize_columns,
    expand_geography_search,
)

import toml, pathlib

secrets_path = pathlib.Path(".streamlit/secrets.toml")
secrets = toml.load(secrets_path)

EMAIL_CFG = secrets.get("email", {})
SMTP_HOST = EMAIL_CFG.get("smtp_host", "smtp.gmail.com")
SMTP_PORT = int(EMAIL_CFG.get("smtp_port", 587))
SMTP_USER = EMAIL_CFG.get("smtp_user", "")
SMTP_PASS = EMAIL_CFG.get("smtp_password", "")
FROM_ADDR = EMAIL_CFG.get("from_address", SMTP_USER)

GCP_SECRETS = secrets.get("gcp_service_account", {})
META_TOKEN = secrets.get("meta_access_token", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)



def send_email(to_address: str, subject: str, html_body: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = FROM_ADDR
    msg["To"] = to_address
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(FROM_ADDR, to_address, msg.as_string())
    logger.info(f"Email sent to {to_address}: {subject}")


def build_email_html(subscription: dict, new_ads: list[dict]) -> str:
    advertiser = subscription.get("advertiser_keyword") or "(any)"
    geography = subscription.get("geography") or "(any)"
    platforms = ", ".join(subscription.get("platforms", []))

    rows_html = ""
    for ad in new_ads[:50]:
        url = ad.get("Ad Url", "")
        link = f'<a href="{url}">{url[:60]}…</a>' if url else "N/A"
        rows_html += f"""
        <tr>
          <td>{ad.get('Platform','')}</td>
          <td>{ad.get('Advertiser Name','')}</td>
          <td>{ad.get('Start Date','')}</td>
          <td>{ad.get('Geography Targeting','')}</td>
          <td>{ad.get('Impressions','')}</td>
          <td>{ad.get('Spend','')}</td>
          <td>{link}</td>
        </tr>"""

    return f"""
    <html><body>
    <h2>🗳️ Political Ads Alert</h2>
    <p>New ads were detected matching your subscription:</p>
    <ul>
      <li><b>Advertiser keyword:</b> {advertiser}</li>
      <li><b>Geography:</b> {geography}</li>
      <li><b>Platforms:</b> {platforms}</li>
    </ul>
    <p><b>{len(new_ads)} new ad(s) found:</b></p>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-size:13px;">
      <thead style="background:#f0f0f0;">
        <tr>
          <th>Platform</th><th>Advertiser</th><th>Start Date</th>
          <th>Geography</th><th>Impressions</th><th>Spend</th><th>Ad URL</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
    <p style="color:#888;font-size:11px;">
      You're receiving this because you subscribed at the Political Ads Tracker.<br>
      To unsubscribe, visit the app and remove your alert.
    </p>
    </body></html>"""


def fetch_google_ads(advertiser_keyword: str, geography: str) -> pd.DataFrame:
    from google.oauth2 import service_account
    from google.cloud import bigquery

    credentials = service_account.Credentials.from_service_account_info(GCP_SECRETS)
    client = bigquery.Client(credentials=credentials)
    expanded_geography = expand_geography_search(geography)

    query = """
    WITH advertiser_base AS (
      SELECT advertiser_id, advertiser_name
      FROM `bigquery-public-data.google_political_ads.advertiser_stats`
      WHERE LOWER(advertiser_name) LIKE LOWER(@advertiser_name)
    ),
    creatives AS (
      SELECT ad_id, advertiser_id, ad_type, ad_url,
             date_range_start, date_range_end, impressions,
             (spend_range_min_usd + spend_range_max_usd)/2 AS spend_usd,
             geo_targeting_included
      FROM `bigquery-public-data.google_political_ads.creative_stats`
      WHERE (@geography = "" OR REGEXP_CONTAINS(LOWER(geo_targeting_included), LOWER(@geography)))
    )
    SELECT a.advertiser_name AS `Advertiser Name`,
           c.ad_id AS `Ad Id`, c.ad_url AS `Ad Url`,
           c.date_range_start AS `Start Date`, c.date_range_end AS `End Date`,
           c.ad_type AS `Ad Type`, c.geo_targeting_included AS `Geography Targeting`,
           c.impressions AS `Impressions`, c.spend_usd AS `Spend`
    FROM advertiser_base a
    LEFT JOIN creatives c ON a.advertiser_id = c.advertiser_id
    ORDER BY c.date_range_start DESC
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("advertiser_name", "STRING", f"%{advertiser_keyword}%"),
        bigquery.ScalarQueryParameter("geography", "STRING", expanded_geography),
    ])
    rows = client.query(query, job_config=job_config).result()
    df = pd.DataFrame([dict(r) for r in rows])
    if not df.empty:
        df["Platform"] = "Google"
    return df


def fetch_meta_ads(advertiser_keyword: str, geography: str) -> pd.DataFrame:
    base_url = "https://graph.facebook.com/v17.0/ads_archive"
    fields = ("id,page_name,ad_delivery_start_time,ad_delivery_stop_time,"
              "ad_snapshot_url,spend,impressions,delivery_by_region")
    params = {
        "access_token": META_TOKEN,
        "ad_type": "POLITICAL_AND_ISSUE_ADS",
        "ad_reached_countries": json.dumps(["US"]),
        "fields": fields,
        "limit": 100,
        "search_terms": advertiser_keyword,
    }
    all_ads, url, page_count = [], base_url, 0
    while page_count < 10:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for ad in data.get("data", []):
            if advertiser_keyword.lower() in (ad.get("page_name") or "").lower():
                all_ads.append(ad)
        paging = data.get("paging", {})
        if not paging.get("next"):
            break
        url, params, page_count = paging["next"], {}, page_count + 1
        time.sleep(0.5)

    rows = []
    for ad in all_ads:
        regions = [r.get("region", "") for r in (ad.get("delivery_by_region") or []) if isinstance(r, dict)]
        geo = ", ".join(regions)
        if geography:
            exp = expand_geography_search(geography)
            if not any(re.search(exp, r, re.IGNORECASE) for r in regions):
                continue
        rows.append({
            "Platform": "Meta",
            "Advertiser Name": ad.get("page_name", ""),
            "Ad Id": ad.get("id", ""),
            "Ad Url": ad.get("ad_snapshot_url", ""),
            "Start Date": ad.get("ad_delivery_start_time", ""),
            "End Date": ad.get("ad_delivery_stop_time", ""),
            "Geography Targeting": geo,
            "Impressions": ad.get("impressions", ""),
            "Spend": ad.get("spend", ""),
        })
    return pd.DataFrame(rows)


def fetch_x_ads(advertiser_keyword: str, geography: str) -> pd.DataFrame:
    df = download_and_extract_csv()
    df = standardize_columns(df)
    if advertiser_keyword:
        df = filter_by_advertiser(df, advertiser_keyword)
    if geography and "Geography Targeting" in df.columns:
        exp = expand_geography_search(geography)
        df = df[df["Geography Targeting"].astype(str).str.contains(exp, case=False, na=False, regex=True)]
    df["Platform"] = "X"
    return df



def run_notifications():
    subscriptions = load_subscriptions()
    if not subscriptions:
        logger.info("No subscriptions found. Exiting.")
        return

    logger.info(f"Processing {len(subscriptions)} subscription(s)...")

    for sub_id, sub in subscriptions.items():
        email = sub["email"]
        advertiser = sub.get("advertiser_keyword", "")
        geography = sub.get("geography", "")
        platforms = sub.get("platforms", ["Google", "Meta", "X"])
        seen_ids = set(sub.get("last_seen_ad_ids", []))

        logger.info(f"Checking subscription {sub_id} for {email} | advertiser={advertiser!r} geo={geography!r}")

        all_new_ads = []

        try:
            if "Google" in platforms and advertiser:
                df_g = fetch_google_ads(advertiser, geography)
                for _, row in df_g.iterrows():
                    ad_id = str(row.get("Ad Id", ""))
                    if ad_id and ad_id not in seen_ids:
                        all_new_ads.append(row.to_dict())
        except Exception as e:
            logger.error(f"Google fetch failed for {sub_id}: {e}")

        try:
            if "Meta" in platforms and advertiser:
                df_m = fetch_meta_ads(advertiser, geography)
                for _, row in df_m.iterrows():
                    ad_id = str(row.get("Ad Id", ""))
                    if ad_id and ad_id not in seen_ids:
                        all_new_ads.append(row.to_dict())
        except Exception as e:
            logger.error(f"Meta fetch failed for {sub_id}: {e}")

        try:
            if "X" in platforms and advertiser:
                df_x = fetch_x_ads(advertiser, geography)
                for _, row in df_x.iterrows():
                    ad_id = str(row.get("Ad Id", ""))
                    if ad_id and ad_id not in seen_ids:
                        all_new_ads.append(row.to_dict())
        except Exception as e:
            logger.error(f"X fetch failed for {sub_id}: {e}")

        if all_new_ads:
            logger.info(f"Found {len(all_new_ads)} new ads for {email}. Sending email...")
            subject = f"🗳️ {len(all_new_ads)} new political ad(s) — {advertiser or geography}"
            html = build_email_html(sub, all_new_ads)
            try:
                send_email(email, subject, html)
                new_ids = list(seen_ids) + [str(a.get("Ad Id", "")) for a in all_new_ads]
                update_last_seen(sub_id, new_ids[-5000:], datetime.utcnow().isoformat())  # keep last 5k IDs
            except Exception as e:
                logger.error(f"Failed to send email to {email}: {e}")
        else:
            logger.info(f"No new ads for {email}.")


if __name__ == "__main__":
    run_notifications()