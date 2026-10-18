FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --create-home --uid 10001 app \
    && mkdir -p /data \
    && chown app:app /data

COPY common/ ./common/
COPY WM_Central/ ./WM_Central/
COPY WM_WS/ ./WM_WS/
COPY WM_FO/ ./WM_FO/

USER app

