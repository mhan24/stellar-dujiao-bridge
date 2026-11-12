FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DB_PATH=/data/bridge.db

WORKDIR /app

# requirements.txt is the locked dependency set generated from pyproject.toml.
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Run unprivileged: the SQLite database lives in /data together with its WAL files.
RUN groupadd --system --gid 10001 bridge \
 && useradd --system --uid 10001 --gid bridge --no-create-home bridge \
 && mkdir -p /data \
 && chown -R bridge:bridge /data /app
USER bridge

VOLUME ["/data"]
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=4)"]

CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8080"]
