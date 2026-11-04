FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/
COPY scripts/ /app/scripts/
COPY configs/ /app/configs/
COPY sql/ /app/sql/
COPY data/sample_raw_events.json /app/data/sample_raw_events.json

ENTRYPOINT ["python"]
