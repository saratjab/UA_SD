FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

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

# Railway and local `docker run` start the headless Central service by default.
# Configuration is read from environment variables; no interactive stdin is needed.
CMD ["python", "-m", "WM_Central.main"]

