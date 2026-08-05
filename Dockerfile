FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

# Install Python deps, then the Chromium browser + its OS libraries.
# `playwright install --with-deps chromium` installs the exact browser build
# matching the pip-installed playwright version, plus the system libs that
# you previously listed in packages.txt.
COPY requirements.txt .
RUN pip install -r requirements.txt \
    && playwright install --with-deps chromium

COPY . .

# Cloud Run injects $PORT (default 8080). Streamlit must bind to it.
ENV PORT=8080
EXPOSE 8080

CMD streamlit run streamlit_app.py \
    --server.port=$PORT \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --server.enableCORS=false \
    --server.enableXsrfProtection=true \
    --browser.gatherUsageStats=false
