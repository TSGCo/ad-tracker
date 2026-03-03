# Political Ads Tracker

Track political ads from Google, Meta, and X. Search by advertiser or geography and subscribe to email alerts when new ads appear.

## Run the app

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Email alerts (optional)

Alerts are stored in **Google Sheets** so the app and the notifier use the same list.

1. **Create a Google Sheet** and copy the spreadsheet ID from the URL (the part between `/d/` and `/edit`).
2. **Share the sheet** with your GCP service account email (`client_email` in your key) as Editor.
3. **Add to secrets** (Streamlit Cloud or `.streamlit/secrets.toml`):
   - `spreadsheet_id` = your sheet ID  
   - `gcp_service_account` = your GCP key  
   - `[email]` = SMTP settings (e.g. Gmail + [App Password](https://support.google.com/accounts/answer/185833))
4. **GitHub Actions:** In repo Settings → Secrets, add `SMTP_HOST`, `SMTP_USER`, `SMTP_PASSWORD`, `META_ACCESS_TOKEN`, `GCP_SERVICE_ACCOUNT_JSON`, `SPREADSHEET_ID`. The notifier runs hourly; you can also run it manually under Actions → Run Ad Notifier.
