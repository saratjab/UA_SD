FROM python:3.12-slim

# DB_PATH points into /data so Central's SQLite file survives redeploys when
# a volume is mounted there (Railway: attach a volume at /data).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DB_PATH=/data/wm_central.db

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --create-home --uid 10001 app \
    && mkdir -p /data /engine-state \
    && chown app:app /data /engine-state

COPY common/ ./common/
COPY WM_Central/ ./WM_Central/
COPY WM_WS/ ./WM_WS/
COPY WM_FO/ ./WM_FO/

USER app

CMD ["python", "-m", "WM_Central.WM_Central"]

