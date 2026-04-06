"""
ETL pipeline to ingest real-time station availability data from Bixi's GBFS API.

Fetches station status (bike/dock availability) and station information (name, location, capacity),
merges them into a single DataFrame, and saves results as a timestamped parquet file.

Designed to be scheduled every 15 minutes via cron or similar scheduler.
"""

import io
import os
import logging
from datetime import datetime, timezone

import boto3
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from bixi_availability_prediction.config.constants import STATION_STATUS_URL, STATION_CAPACITY_URL

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

BIXI_ENV = os.environ.get("BIXI_ENV")
S3_BUCKET_MAP = {
    "dev": os.environ.get("BIXI_S3_BUCKET_DEV"),
    "prod": os.environ.get("BIXI_S3_BUCKET_PROD"),
}
S3_PREFIX = os.environ.get("BIXI_S3_PREFIX", "data")
LOCAL_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")


def fetch_station_status(url: str = STATION_STATUS_URL) -> pd.DataFrame:
    """Fetch real-time station availability from the GBFS station_status endpoint."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()

    last_updated = datetime.fromtimestamp(payload["last_updated"], tz=timezone.utc)
    stations = payload["data"]["stations"]

    df = pd.DataFrame(stations)
    df["last_updated_utc"] = last_updated
    df["last_reported_utc"] = pd.to_datetime(df["last_reported"], unit="s", utc=True)

    return df[
        [
            "station_id",
            "num_bikes_available",
            "num_docks_available",
            "is_installed",
            "is_renting",
            "is_returning",
            "last_reported_utc",
            "last_updated_utc",
        ]
    ]


def fetch_station_info(url: str = STATION_CAPACITY_URL) -> pd.DataFrame:
    """Fetch static station metadata from the GBFS station_information endpoint."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    stations = response.json()["data"]["stations"]

    df = pd.DataFrame(stations)
    return df[["station_id", "name", "lat", "lon", "capacity"]]


def build_availability_snapshot() -> pd.DataFrame:
    """Merge station status with station info into a single enriched snapshot."""
    status_df = fetch_station_status()
    info_df = fetch_station_info()

    merged = status_df.merge(info_df, on="station_id", how="left")
    merged["ingested_at_utc"] = datetime.now(timezone.utc)

    return merged


def save_snapshot(df: pd.DataFrame, env: str = BIXI_ENV) -> str:
    """Save snapshot as a timestamped parquet file.

    Behaviour depends on the env parameter (driven by BIXI_ENV):
      - "local": saves to local disk
      - "dev":   saves to the dev S3 bucket (BIXI_S3_BUCKET_DEV)
      - "prod":  saves to the prod S3 bucket (BIXI_S3_BUCKET_PROD)
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"station_availability_{timestamp}.parquet"

    logging.info(f'Environment detected {env}')

    if env == "local":
        os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
        output = os.path.join(LOCAL_DATA_DIR, filename)
        df.to_parquet(output, index=False)
    else:
        bucket = S3_BUCKET_MAP.get(env)
        if not bucket:
            raise RuntimeError(
                f"S3 bucket not configured for env '{env}'. "
                f"Set BIXI_S3_BUCKET_{env.upper()} environment variable."
            )
        key = f"{S3_PREFIX}/{filename}"
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        
        s3 = boto3.client("s3")
        s3.upload_fileobj(buffer, bucket, key)
        output = f"s3://{bucket}/{key}"

    logging.info(f"Saved {len(df)} rows to {output}")
    return output


def run_pipeline() -> None:
    """Execute a single ingestion cycle: fetch, merge, and persist."""
    logging.info("Starting station availability ingestion...")
    snapshot = build_availability_snapshot()
    save_snapshot(snapshot)
    logging.info("Ingestion complete.")


if __name__ == "__main__":
    run_pipeline()
