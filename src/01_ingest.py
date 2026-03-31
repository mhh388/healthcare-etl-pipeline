"""
src/01_ingest.py
────────────────
Step 1 of the Healthcare ETL Pipeline.

What this script does:
  1. Downloads CMS Medicare Part D Prescriber data (public dataset)
  2. Validates the raw file (row count, expected columns, file size)
  3. Uploads raw data to S3 (raw zone of our data lake)
  4. Logs a manifest entry (filename, row count, timestamp, S3 path)

Run:
  python src/01_ingest.py
  python src/01_ingest.py --year 2021   (download a different year)
  python src/01_ingest.py --local-only  (skip S3, save locally only)
"""

import sys
import json
import hashlib
import argparse
from pathlib import Path
from datetime import datetime, timezone

import boto3
import requests
import pandas as pd
from tqdm import tqdm
from loguru import logger

# Add project root to path so we can import config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import (
    AWS_REGION, S3_BUCKET, S3_RAW_PREFIX,
    CMS_DATASET_YEAR, CHUNK_SIZE, LOCAL_RAW_DIR,
    validate_config, print_config
)


# ── Constants ─────────────────────────────────────────────────────────────────

# CMS Medicare Part D Prescribers — By Provider and Drug
# Direct CSV download URL (no API key required, 100% public)
CMS_DOWNLOAD_URLS = {
    "2023": (
        "https://data.cms.gov/sites/default/files/2025-04/"
        "0d5915ce-002c-4d87-bde8-24ffb08bb6cc/MUP_DPR_RY25_P04_V10_DY23_NPIBN.csv"
    ),
    "2022": (
        "https://data.cms.gov/sites/default/files/2024-04/"
        "MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv"
    ),
}

# Columns we expect in the raw CMS file
EXPECTED_COLUMNS = [
    "Prscrbr_NPI",
    "Prscrbr_Last_Org_Name",
    "Prscrbr_First_Name",
    "Prscrbr_City",
    "Prscrbr_State_Abrvtn",
    "Prscrbr_State_FIPS",
    "Prscrbr_Type",
    "Prscrbr_Type_Src",
    "Brnd_Name",
    "Gnrc_Name",
    "Tot_Clms",
    "Tot_30day_Eqv_Day_Suply",
    "Tot_Day_Suply",
    "Tot_Drug_Cst",
    "Tot_Benes",
    "GE65_Sprsn_Flag",
    "GE65_Tot_Clms",
    "GE65_Tot_30day_Eqv_Day_Suply",
    "GE65_Tot_Drug_Cst",
    "GE65_Tot_Benes",
]

MIN_EXPECTED_ROWS = 1_000_000  # CMS dataset is always millions of rows


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_s3_client():
    """Return a boto3 S3 client."""
    return boto3.client("s3", region_name=AWS_REGION)


def compute_md5(filepath: Path) -> str:
    """Compute MD5 checksum of a file for integrity validation."""
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
    return md5.hexdigest()


# ── Core Functions ────────────────────────────────────────────────────────────

def download_cms_data(year: str, output_dir: Path) -> Path:
    """
    Download CMS Medicare Part D data for a given year.
    Shows a progress bar and saves to output_dir/cms_partd_{year}.csv

    Returns:
        Path to the downloaded file.
    """
    url = CMS_DOWNLOAD_URLS.get(year)
    if not url:
        available = list(CMS_DOWNLOAD_URLS.keys())
        raise ValueError(f"No URL for year {year}. Available: {available}")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"cms_partd_{year}.csv"

    if output_path.exists():
        logger.info(f"File already exists locally: {output_path} — skipping download")
        return output_path

    logger.info(f"Downloading CMS Medicare Part D data for {year}...")
    logger.info(f"Source: {url}")

    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    block_size = 8192

    with open(output_path, "wb") as f, tqdm(
        desc=f"cms_partd_{year}.csv",
        total=total_size,
        unit="iB",
        unit_scale=True,
        unit_divisor=1024,
    ) as progress:
        for chunk in response.iter_content(block_size):
            size = f.write(chunk)
            progress.update(size)

    file_size_mb = output_path.stat().st_size / 1024 / 1024
    logger.success(f"Download complete: {output_path} ({file_size_mb:.1f} MB)")
    return output_path


def validate_raw_data(filepath: Path) -> dict:
    """
    Validate the downloaded CSV file.

    Checks:
      - File exists and is non-empty
      - Expected columns are present
      - Row count exceeds minimum threshold
      - No completely empty columns

    Returns:
        dict with validation results and summary stats
    """
    logger.info(f"Validating raw data: {filepath.name}")

    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    file_size_mb = filepath.stat().st_size / 1024 / 1024
    if file_size_mb < 1:
        raise ValueError(f"File too small ({file_size_mb:.2f} MB) — download may have failed")

    # Read just the first chunk to validate structure (don't load entire file)
    df_sample = pd.read_csv(filepath, nrows=1000, low_memory=False)

    # Check columns
    actual_cols = set(df_sample.columns.tolist())
    expected_cols = set(EXPECTED_COLUMNS)
    missing_cols = expected_cols - actual_cols
    extra_cols = actual_cols - expected_cols

    if missing_cols:
        logger.warning(f"Missing expected columns: {missing_cols}")
    if extra_cols:
        logger.info(f"Extra columns found (OK): {extra_cols}")

    # Count total rows efficiently (without loading whole file)
    logger.info("Counting rows (this takes ~30 seconds for large files)...")
    row_count = sum(1 for _ in open(filepath)) - 1  # subtract header

    if row_count < MIN_EXPECTED_ROWS:
        raise ValueError(
            f"Row count {row_count:,} is below minimum {MIN_EXPECTED_ROWS:,} — "
            f"file may be incomplete"
        )

    # Check for completely empty columns in sample
    empty_cols = df_sample.columns[df_sample.isnull().all()].tolist()
    if empty_cols:
        logger.warning(f"Completely empty columns in sample: {empty_cols}")

    validation_result = {
        "status": "passed",
        "filename": filepath.name,
        "file_size_mb": round(file_size_mb, 2),
        "row_count": row_count,
        "column_count": len(df_sample.columns),
        "missing_expected_cols": list(missing_cols),
        "empty_cols_in_sample": empty_cols,
        "sample_row_count": len(df_sample),
        "md5_checksum": compute_md5(filepath),
        "validated_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.success(
        f"Validation passed ✓ — "
        f"{row_count:,} rows, {len(df_sample.columns)} columns, "
        f"{file_size_mb:.1f} MB"
    )
    return validation_result


def upload_to_s3(
    filepath: Path,
    year: str,
    s3_client,
    bucket: str,
    prefix: str,
) -> str:
    """
    Upload raw CSV to S3 raw zone using a partitioned key structure.

    S3 key format:
        raw/source=cms/dataset=partd_prescribers/year=2022/cms_partd_2022.csv

    Returns:
        Full S3 URI of the uploaded file.
    """
    s3_key = (
        f"{prefix}/"
        f"source=cms/"
        f"dataset=partd_prescribers/"
        f"year={year}/"
        f"{filepath.name}"
    )
    s3_uri = f"s3://{bucket}/{s3_key}"

    logger.info(f"Uploading to S3: {s3_uri}")
    file_size_mb = filepath.stat().st_size / 1024 / 1024

    # Use multipart upload for large files (automatic with transfer config)
    from boto3.s3.transfer import TransferConfig
    config = TransferConfig(
        multipart_threshold=1024 * 25,   # 25 MB threshold
        max_concurrency=10,
        multipart_chunksize=1024 * 25,
        use_threads=True,
    )

    with tqdm(
        total=filepath.stat().st_size,
        unit="iB",
        unit_scale=True,
        desc=f"→ S3 upload",
    ) as progress:
        s3_client.upload_file(
            str(filepath),
            bucket,
            s3_key,
            Config=config,
            Callback=lambda bytes_transferred: progress.update(bytes_transferred),
        )

    logger.success(f"Upload complete: {s3_uri} ({file_size_mb:.1f} MB)")
    return s3_uri


def write_manifest(
    validation_result: dict,
    s3_uri: str,
    output_dir: Path,
):
    """
    Write a manifest JSON file recording what was ingested, when, and where.
    This is key for data lineage tracking — a real data engineering practice.
    """
    manifest = {
        **validation_result,
        "s3_uri": s3_uri,
        "pipeline_stage": "ingestion",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / f"manifest_{validation_result['filename']}.json"

    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    logger.info(f"Manifest written: {manifest_path}")
    return manifest_path


# ── Main Pipeline Step ────────────────────────────────────────────────────────

def run_ingestion(year: str, local_only: bool = False):
    """
    Run the full ingestion step:
      1. Download CMS data
      2. Validate raw file
      3. Upload to S3 (unless --local-only)
      4. Write manifest
    """
    logger.info("=" * 60)
    logger.info("  STEP 1 — DATA INGESTION")
    logger.info("=" * 60)

    if not local_only:
        validate_config()
        print_config()

    # 1. Download
    raw_file = download_cms_data(year, output_dir=LOCAL_RAW_DIR)

    # 2. Validate
    validation = validate_raw_data(raw_file)

    # 3. Upload to S3
    s3_uri = "local-only"
    if not local_only:
        s3_client = get_s3_client()
        s3_uri = upload_to_s3(
            filepath=raw_file,
            year=year,
            s3_client=s3_client,
            bucket=S3_BUCKET,
            prefix=S3_RAW_PREFIX,
        )
    else:
        logger.info("--local-only flag set — skipping S3 upload")

    # 4. Write manifest
    manifest_dir = LOCAL_RAW_DIR / "manifests"
    write_manifest(validation, s3_uri, manifest_dir)

    logger.info("=" * 60)
    logger.success("  STEP 1 COMPLETE ✓")
    logger.info(f"  Raw file  : {raw_file}")
    logger.info(f"  S3 URI    : {s3_uri}")
    logger.info(f"  Rows      : {validation['row_count']:,}")
    logger.info("=" * 60)

    return {"raw_file": raw_file, "s3_uri": s3_uri, "validation": validation}


# ── CLI Entry Point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Healthcare ETL Pipeline — Step 1: Ingest CMS Data"
    )
    parser.add_argument(
        "--year",
        default=CMS_DATASET_YEAR,
        choices=list(CMS_DOWNLOAD_URLS.keys()),
        help=f"CMS dataset year (default: {CMS_DATASET_YEAR})",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Download data locally only — skip S3 upload (good for testing)",
    )
    args = parser.parse_args()

    # Configure logger
    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)
    logger.add(
        LOCAL_RAW_DIR.parent / "logs" / "ingestion_{time}.log",
        level="DEBUG",
        rotation="10 MB",
    )

    result = run_ingestion(year=args.year, local_only=args.local_only)
