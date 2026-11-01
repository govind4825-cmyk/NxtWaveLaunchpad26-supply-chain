FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY rasad ./rasad
COPY web ./web
COPY scripts ./scripts

# District boundaries for alerts that name areas without a polygon. If the download
# fails the app still runs; those alerts are listed as "not on map".
RUN python scripts/fetch_boundaries.py || echo "boundary download skipped"

EXPOSE 8000
CMD ["uvicorn", "rasad.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
