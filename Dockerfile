FROM python:3.10-slim

WORKDIR /app

COPY requirements-prod.txt .
RUN pip install --no-cache-dir -r requirements-prod.txt

COPY pyproject.toml .
COPY src/ src/
COPY scripts/ scripts/

RUN pip install --no-cache-dir -e .

CMD ["python", "-m", "bixi_availability_prediction.data.ingest_station_status"]
