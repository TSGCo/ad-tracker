import requests
import pandas as pd
import zipfile
import io
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

X_DATA_BASE_URL = "https://business.x.com/content/dam/business-twitter/political-ads-data"

STATE_MAPPING = {
    'al': 'alabama', 'ak': 'alaska', 'az': 'arizona', 'ar': 'arkansas',
    'ca': 'california', 'co': 'colorado', 'ct': 'connecticut', 'de': 'delaware',
    'fl': 'florida', 'ga': 'georgia', 'hi': 'hawaii', 'id': 'idaho',
    'il': 'illinois', 'in': 'indiana', 'ia': 'iowa', 'ks': 'kansas',
    'ky': 'kentucky', 'la': 'louisiana', 'me': 'maine', 'md': 'maryland',
    'ma': 'massachusetts', 'mi': 'michigan', 'mn': 'minnesota', 'ms': 'mississippi',
    'mo': 'missouri', 'mt': 'montana', 'ne': 'nebraska', 'nv': 'nevada',
    'nh': 'new hampshire', 'nj': 'new jersey', 'nm': 'new mexico', 'ny': 'new york',
    'nc': 'north carolina', 'nd': 'north dakota', 'oh': 'ohio', 'ok': 'oklahoma',
    'or': 'oregon', 'pa': 'pennsylvania', 'ri': 'rhode island', 'sc': 'south carolina',
    'sd': 'south dakota', 'tn': 'tennessee', 'tx': 'texas', 'ut': 'utah',
    'vt': 'vermont', 'va': 'virginia', 'wa': 'washington', 'wv': 'west virginia',
    'wi': 'wisconsin', 'wy': 'wyoming', 'dc': 'district of columbia'
}


def generate_possible_dates(days_back=7):
    dates = []
    today = datetime.now()
    
    for i in range(days_back):
        date = today - timedelta(days=i)
        formatted_date = f"{date.day:02d}-{date.strftime('%B')}-{date.year}"
        dates.append((formatted_date, date))
    
    return dates


def find_latest_data_file():
    possible_dates = generate_possible_dates(days_back=7)
    
    for date_str, date_obj in possible_dates:
        url = f"{X_DATA_BASE_URL}/{date_str}-political-ads-data.zip"
        
        try:
            logger.info(f"Checking for file: {date_str}")
            response = requests.get(url, timeout=10, stream=True, allow_redirects=True)
            
            if response.status_code == 200:
                logger.info(f"Found latest data file: {date_str}")
                return url, date_str
        except requests.RequestException as e:
            logger.debug(f"Date {date_str} not found: {e}")
            continue
    
    logger.warning("Could not find any recent X political ads data file")
    return None, None


def download_and_extract_csv():
    url, date_str = find_latest_data_file()

    if not url:
        raise Exception("Could not find latest X political ads data file")

    try:
        logger.info(f"Downloading X political ads data from: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        logger.info(f"Extracting file from ZIP")

        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            file_list = zip_file.namelist()
            logger.info(f"Files in ZIP: {file_list}")

            csv_files = [f for f in file_list if f.endswith('.csv') and not f.startswith('__MACOSX')]
            xlsx_files = [f for f in file_list if f.endswith('.xlsx') and not f.startswith('__MACOSX')]

            if csv_files:
                file_path = csv_files[0]
                logger.info(f"Reading CSV: {file_path}")
                with zip_file.open(file_path) as f:
                    raw = f.read()
                for encoding in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
                    try:
                        df = pd.read_csv(
                            io.BytesIO(raw),
                            encoding=encoding,
                            on_bad_lines="skip",
                            low_memory=False,
                        )
                        break
                    except Exception:
                        continue
                else:
                    df = pd.read_csv(io.BytesIO(raw), on_bad_lines="skip", low_memory=False)

            elif xlsx_files:
                file_path = xlsx_files[0]
                logger.info(f"Reading XLSX: {file_path}")
                with zip_file.open(file_path) as f:
                    df = pd.read_excel(io.BytesIO(f.read()))

            else:
                raise Exception(f"No CSV or XLSX files found in ZIP. Contents: {file_list}")

        logger.info(f"Successfully loaded {len(df)} rows from X political ads data")
        return df

    except requests.RequestException as e:
        logger.error(f"Error downloading file: {e}")
        raise Exception(f"Failed to download X political ads data: {e}")
    except zipfile.BadZipFile as e:
        logger.error(f"Error extracting ZIP: {e}")
        raise Exception(f"Failed to extract ZIP file: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


def filter_by_advertiser(df, keyword):
    if not keyword:
        return df
    
    search_columns = [col for col in df.columns if col.lower() in [
        'advertiser name', 'screen name', 'ad type', 'ad id', 'ad url'
    ]]
    
    if not search_columns:
        logger.warning("Could not find searchable columns. Available columns: " + str(df.columns.tolist()))
        return df
    
    mask = False
    for col in search_columns:
        if col in df.columns:
            mask = mask | df[col].astype(str).str.lower().str.contains(keyword.lower(), na=False)
    
    filtered_df = df[mask]
    
    return filtered_df


def expand_geography_search(geography_query):
    if not geography_query:
        return geography_query
    
    query_lower = geography_query.lower().strip()
    
    if query_lower in STATE_MAPPING:
        full_name = STATE_MAPPING[query_lower]
        return f"(?:{query_lower}|{full_name})"

    for abbr, full_name in STATE_MAPPING.items():
        if query_lower == full_name:
            return f"(?:{abbr}|{full_name})"
    
    return geography_query


def standardize_columns(df):
    column_mapping = {
        'Screen Name': 'Advertiser Name',
        'Tweet Id': 'Ad Id',
        'Tweet Url': 'Ad Url',
        'Day of Start Date Adgroup': 'Start Date',
        'Day of End Date Adgroup': 'End Date',
        'Targeting Name': 'Ad Type',
        'Interest Targeting': 'Interest Targeting',
        'Geo Targeting': 'Geography Targeting',
        'Gender Targeting': 'Gender Targeting',
        'Age Targeting': 'Age Targeting',
        'Impressions': 'Impressions',
        'Spend_USD': 'Spend',
    }
    
    rename_dict = {}
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            rename_dict[old_col] = new_col
    
    df = df.rename(columns=rename_dict)
    
    return df
