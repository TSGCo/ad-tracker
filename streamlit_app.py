import streamlit as st
from google.cloud import bigquery
import pandas as pd
import requests
import time
import json
import re
from x_ads_scraper import download_and_extract_csv, filter_by_advertiser, standardize_columns, expand_geography_search

st.set_page_config(layout="wide")

DISPLAY_ROW_LIMIT = 10_000


def _get_bq_client():
    if not hasattr(st, "secrets") or not st.secrets or not st.secrets.get("gcp_service_account"):
        return None
    from google.oauth2 import service_account
    from google.cloud import bigquery
    creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    return bigquery.Client(credentials=creds)


st.markdown("<h1 style='text-align: center;'>Ads Tracker</h1>", unsafe_allow_html=True)

st.markdown("<h2 style='text-align: left;'><span style='color: #4285F4;'>G</span><span style='color: #EA4335;'>o</span><span style='color: #FBBC05;'>o</span><span style='color: #4285F4;'>g</span><span style='color: #EA4335;'>l</span><span style='color: #FBBC05;'>e</span></h2>", unsafe_allow_html=True)

from subscription_manager import set_sheets_config_from_app
if hasattr(st, "secrets") and st.secrets:
    set_sheets_config_from_app(
        st.secrets.get("spreadsheet_id"),
        st.secrets.get("gcp_service_account"),
    )

search_cols = st.columns([1, 1])
with search_cols[0]:
    advertiser_name = st.text_input("Search by Keyword", "")
with search_cols[1]:
    google_geo = st.text_input("Search by Geography", "")


@st.cache_data(ttl=86400)
def run_query(advertiser_name, geography=""):
    client = _get_bq_client()
    if client is None:
        return pd.DataFrame()

    expanded_geography = expand_geography_search(geography)

    query = """
    WITH advertiser_base AS (
      SELECT
        advertiser_id,
        advertiser_name
      FROM `bigquery-public-data.google_political_ads.advertiser_stats`
      WHERE LOWER(advertiser_name) LIKE LOWER(@advertiser_name)
    ),

    creatives AS (
      SELECT
        ad_id,
        advertiser_id,
        ad_type,
        ad_url,
        date_range_start,
        date_range_end,
        impressions,
        spend_range_min_usd,
        spend_range_max_usd,
        (spend_range_min_usd + spend_range_max_usd)/2 AS spend_usd,
        geo_targeting_included,
        age_targeting,
        gender_targeting
      FROM `bigquery-public-data.google_political_ads.creative_stats`
      WHERE (@geography = "" OR REGEXP_CONTAINS(LOWER(geo_targeting_included), LOWER(@geography)))
    )

    SELECT
      a.advertiser_name AS screen_name,
      c.ad_id AS tweet_id,
      c.ad_url AS tweet_url,
      c.date_range_start AS day_of_start_date_adgroup,
      c.date_range_end AS day_of_end_date_adgroup,
      c.ad_type AS targeting_name,
      c.geo_targeting_included AS geo_targeting,
      c.gender_targeting AS gender_targeting,
      c.age_targeting AS age_targeting,
      c.impressions AS impressions,
      c.spend_usd AS spend_usd
    FROM advertiser_base a
    LEFT JOIN creatives c
      ON a.advertiser_id = c.advertiser_id
    ORDER BY c.date_range_start DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "advertiser_name", "STRING", f"%{advertiser_name}%"
            ),
            bigquery.ScalarQueryParameter(
                "geography", "STRING", expanded_geography
            ),
        ]
    )

    query_job = client.query(query, job_config=job_config)
    rows = query_job.result()

    df = pd.DataFrame([dict(row) for row in rows])


    df = df.rename(columns={
        "screen_name": "Advertiser Name",
        "tweet_id": "Ad Id",
        "tweet_url": "Ad Url",
        "day_of_start_date_adgroup": "Start Date",
        "day_of_end_date_adgroup": "End Date",
        "targeting_name": "Ad Type",
        "geo_targeting": "Geography Targeting",
        "gender_targeting": "Gender Targeting",
        "age_targeting": "Age Targeting",
        "impressions": "Impressions",
        "spend_usd": "Spend"
    })

    return df


def apply_simple_filters(df, prefix):
    if df is None or df.empty:
        return df

    if "Spend" in df.columns:
        df = df.copy()
        df["Spend"] = pd.to_numeric(df["Spend"], errors="coerce").fillna(0)
    else:
        df = df.copy()

    cols = st.columns([1, 1, 1])
    with cols[0]:
        min_spend = st.number_input("Min Spend (USD)", min_value=0.0, value=0.0, format="%.2f", key=f"{prefix}_min_spend")
    with cols[1]:
        max_default = float(df["Spend"].max()) if "Spend" in df.columns and not df["Spend"].empty else 0.0
        max_spend = st.number_input("Max Spend (USD)", min_value=0.0, value=max_default, format="%.2f", key=f"{prefix}_max_spend")
    with cols[2]:
        keyword = st.text_input("Keyword (Ad Url / Ad Type / Advertiser)", key=f"{prefix}_keyword")

    advertisers = []
    if "Advertiser Name" in df.columns:
        advertisers = sorted(df["Advertiser Name"].dropna().unique().tolist())
    adv_sel = st.multiselect("Advertiser", advertisers, key=f"{prefix}_adv_sel")

    filtered = df
    if "Spend" in filtered.columns:
        filtered = filtered[(filtered["Spend"] >= float(min_spend)) & (filtered["Spend"] <= float(max_spend))]
    if adv_sel:
        filtered = filtered[filtered.get("Advertiser Name", "").isin(adv_sel)]
    if keyword:
        mask = (
            filtered.get("Ad Url", "").astype(str).str.contains(keyword, case=False, na=False)
            | filtered.get("Ad Type", "").astype(str).str.contains(keyword, case=False, na=False)
            | filtered.get("Advertiser Name", "").astype(str).str.contains(keyword, case=False, na=False)
        )
        filtered = filtered[mask]

    return filtered


if advertiser_name or google_geo:
    if _get_bq_client() is None:
        st.warning("Google Ads search requires GCP credentials. Set **gcp_service_account** in Streamlit app secrets (e.g. in Streamlit Cloud).")
        df = pd.DataFrame()
    else:
        with st.spinner("Fetching advertiser data..."):
            df = run_query(advertiser_name, google_geo)
    if not df.empty:
        st.success(f"Returned {len(df)} records")

        st.markdown("**Filters (Google)**")
        df_filtered = apply_simple_filters(df, "google")

        if df_filtered is None or df_filtered.empty:
            st.warning("No results match the filters")
        else:
            n_total = len(df_filtered)
            df_show = df_filtered.head(DISPLAY_ROW_LIMIT)
            cap_note = f" (first {DISPLAY_ROW_LIMIT:,} shown; download CSV for full data)" if n_total > DISPLAY_ROW_LIMIT else ""
            st.markdown(f"**Showing {len(df_show)} of {n_total} records**{cap_note}")
            st.dataframe(df_show, column_config={
                "Ad Url": st.column_config.LinkColumn()
            }, height=400, width="stretch")

            csv = df_filtered.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Download Filtered CSV",
                data=csv,
                file_name=f"google_ads_filtered.csv",
                mime="text/csv",
            )

            csv_full = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Download Full CSV",
                data=csv_full,
                file_name=f"google_ads_full.csv",
                mime="text/csv",
            )
    else:
        st.warning("No results found.")

st.markdown("<h2 style='text-align: left;'><span style='color: #0084F3;'>M</span><span style='color: #0084F3;'>e</span><span style='color: #0084F3;'>t</span><span style='color: #0084F3;'>a</span></h2>", unsafe_allow_html=True)

meta_cols = st.columns([1, 1])
with meta_cols[0]:
    meta_advertiser_name = st.text_input("Search by Keyword", "", key="meta_advertiser")
with meta_cols[1]:
    meta_geo = st.text_input("Search by Geography", "", key="meta_geo")

@st.cache_data(ttl=86400)
def fetch_meta_ads(advertiser_name, geography=""):
    meta_access_token = st.secrets["meta_access_token"]

    base_url = "https://graph.facebook.com/v17.0/ads_archive"
    fields = (
        "id,page_id,page_name,bylines,"
        "ad_creation_time,ad_delivery_start_time,ad_delivery_stop_time,"
        "ad_creative_bodies,ad_creative_link_titles,ad_snapshot_url,"
        "spend,impressions,currency,"
        "ad_reached_countries,delivery_by_region,publisher_platforms,demographic_distribution"
    )

    try:
        countries_param = json.dumps(["US"])

        params = {
            "access_token": meta_access_token,
            "ad_type": "POLITICAL_AND_ISSUE_ADS",
            "ad_reached_countries": countries_param,
            "fields": fields,
            "limit": 100,
            "search_terms": advertiser_name,
        }

        all_ads = []
        url = base_url
        page_count = 0
        max_pages = 10

        while True:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                code = data["error"].get("code")
                if code == 613:
                    time.sleep(60)
                    response = requests.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                else:
                    st.error(f"API Error: {data['error'].get('message')}")
                    return pd.DataFrame()

            batch = data.get("data", [])

            matched = []
            for ad in batch:
                page_name = (ad.get("page_name") or "").lower()
                if advertiser_name.lower() in page_name:
                    matched.append(ad)

            if matched:
                all_ads.extend(matched)

            page_count += 1

            paging = data.get("paging", {})
            next_url = paging.get("next")
            if not next_url or page_count >= max_pages:
                break

            url = next_url
            params = {}
            time.sleep(0.5)

        if not all_ads:
            return pd.DataFrame()

        rows = []
        for ad in all_ads:
            demo = ad.get("demographic_distribution") or {}
            gender_targeting = None
            age_targeting = None
            if isinstance(demo, dict):
                gender_targeting = demo.get("gender") or demo.get("genders")
                age_targeting = demo.get("age") or demo.get("ages")
                if gender_targeting is None:
                    gender_targeting = json.dumps(demo)
                if age_targeting is None:
                    age_targeting = json.dumps(demo)
            else:
                gender_targeting = str(demo)
                age_targeting = str(demo)
            delivery_by_region = ad.get("delivery_by_region") or []
            geo_targeting = ""
            if isinstance(delivery_by_region, list):
                regions = [region.get("region", "") for region in delivery_by_region if isinstance(region, dict)]
                geo_targeting = ", ".join(regions) if regions else ""

            if geography:
                expanded_geo = expand_geography_search(geography)
                if not any(re.search(expanded_geo, region, re.IGNORECASE) for region in regions):
                    continue

            row = {
                "Advertiser Name": ad.get("page_name") or advertiser_name,
                "Ad Id": ad.get("id", ""),
                "Ad Url": ad.get("ad_snapshot_url", ""),
                "Start Date": ad.get("ad_delivery_start_time", ""),
                "End Date": ad.get("ad_delivery_stop_time", ""),
                "Ad Type": "POLITICAL_AND_ISSUE_ADS",
                "Geography Targeting": geo_targeting,
                "Gender Targeting": gender_targeting,
                "Age Targeting": age_targeting,
                "Impressions": ad.get("impressions", ""),
                "Spend": ad.get("spend", ""),
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        try:
            df["Start Date"] = pd.to_datetime(df["Start Date"]) 
        except Exception:
            pass
        return df

    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching Meta ads: {e}")
        return pd.DataFrame()

if meta_advertiser_name or meta_geo:
    with st.spinner("Fetching Meta advertiser data..."):
        df_meta = fetch_meta_ads(meta_advertiser_name, meta_geo)
    
    if not df_meta.empty:
        st.success(f"Returned {len(df_meta)} records")
        df_meta = df_meta.sort_values("Start Date", ascending=False)
        st.markdown("**Filters (Meta)**")
        df_meta_filtered = apply_simple_filters(df_meta, "meta")

        if df_meta_filtered is None or df_meta_filtered.empty:
            st.warning("No results match the filters")
        else:
            n_total = len(df_meta_filtered)
            df_show = df_meta_filtered.head(DISPLAY_ROW_LIMIT)
            cap_note = f" (first {DISPLAY_ROW_LIMIT:,} shown; download CSV for full data)" if n_total > DISPLAY_ROW_LIMIT else ""
            st.markdown(f"**Showing {len(df_show)} of {n_total} records**{cap_note}")
            st.dataframe(df_show, column_config={
                "Ad Url": st.column_config.LinkColumn()
            }, height=400, width="stretch")

            csv = df_meta_filtered.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Download Filtered CSV",
                data=csv,
                file_name=f"meta_political_ads_filtered.csv",
                mime="text/csv",
            )

            csv_full = df_meta.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Download Full CSV",
                data=csv_full,
                file_name=f"meta_political_ads_full.csv",
                mime="text/csv",
            )
    else:
        st.warning("No results found.")


st.header("X")

x_cols = st.columns([1, 1])
with x_cols[0]:
    x_advertiser_name = st.text_input("Search by Keyword", "", key="x_advertiser")
with x_cols[1]:
    x_geo = st.text_input("Search by Geography", "", key="x_geo")

@st.cache_data(ttl=86400)
def fetch_x_ads(advertiser_name, geography=""):
    try:
        df = download_and_extract_csv()
        df = standardize_columns(df)
        
        if advertiser_name:
            df = filter_by_advertiser(df, advertiser_name)
        
        if geography and "Geography Targeting" in df.columns:
            expanded_geo = expand_geography_search(geography)
            try:
                mask = df["Geography Targeting"].astype(str).str.contains(
                    expanded_geo, case=False, na=False, regex=True
                )
                df = df[mask]
            except Exception:
                df = df[
                    df["Geography Targeting"].astype(str).str.lower().str.contains(
                        geography.lower(), na=False
                    )
                ]
        
        if "Start Date" in df.columns:
            try:
                df["Start Date"] = pd.to_datetime(df["Start Date"], errors="coerce")
                df = df.sort_values("Start Date", ascending=False)
            except Exception:
                pass
        
        return (df, None)
    except Exception as e:
        return (pd.DataFrame(), str(e))

if x_advertiser_name or x_geo:
    with st.spinner("Fetching X advertiser data..."):
        df_x_filtered, x_fetch_error = fetch_x_ads(x_advertiser_name, x_geo)
    if x_fetch_error:
        st.error(f"Error fetching X political ads data: {x_fetch_error}")
    elif not df_x_filtered.empty:
        st.success(f"Returned {len(df_x_filtered)} records")
        st.markdown("**Filters (X)**")
        df_x_display = apply_simple_filters(df_x_filtered, "x")

        if df_x_display is None or df_x_display.empty:
            st.warning("No results match the filters")
        else:
            n_total = len(df_x_display)
            df_show = df_x_display.head(DISPLAY_ROW_LIMIT)
            cap_note = f" (first {DISPLAY_ROW_LIMIT:,} shown; download CSV for full data)" if n_total > DISPLAY_ROW_LIMIT else ""
            st.markdown(f"**Showing {len(df_show)} of {n_total} records**{cap_note}")
            st.dataframe(df_show, column_config={
                "Ad Url": st.column_config.LinkColumn()
            }, height=400, width="stretch")

            csv = df_x_display.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Download Filtered CSV",
                data=csv,
                file_name=f"x_political_ads_filtered.csv",
                mime="text/csv",
            )

            csv_full = df_x_filtered.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Download Full CSV",
                data=csv_full,
                file_name=f"x_political_ads_full.csv",
                mime="text/csv",
            )
    else:
        st.warning("No X political ads found for this advertiser. Data is updated every 2 days from X's official disclosure page.")


st.markdown("**Download combined CSV**")


def _gather_datasets():
    parts = []
    if "df" in globals() and isinstance(globals().get("df"), pd.DataFrame) and not globals().get("df").empty:
        df_copy = globals().get("df").copy()
        df_copy["Platform"] = "Google"
        parts.append(df_copy)
    if "df_meta" in globals() and isinstance(globals().get("df_meta"), pd.DataFrame) and not globals().get("df_meta").empty:
        df_meta_copy = globals().get("df_meta").copy()
        df_meta_copy["Platform"] = "Meta"
        parts.append(df_meta_copy)
    if "df_x_filtered" in globals() and isinstance(globals().get("df_x_filtered"), pd.DataFrame) and not globals().get("df_x_filtered").empty:
        df_x_copy = globals().get("df_x_filtered").copy()
        df_x_copy["Platform"] = "X"
        parts.append(df_x_copy)
    return parts

all_parts = _gather_datasets()
if all_parts:
    combined = pd.concat(all_parts, ignore_index=True, sort=False)
    csv_all = combined.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download combined CSV",
        data=csv_all,
        file_name="all_ads_combined.csv",
        mime="text/csv",
    )
else:
    st.info("No datasets available to combine. Fetch Google, Meta, or X results first.")


from alerts_ui import show_alerts_ui
show_alerts_ui()