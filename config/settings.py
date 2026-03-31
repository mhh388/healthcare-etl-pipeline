"""
config/settings.py
──────────────────
Centralized configuration for the Healthcare ETL Pipeline.
Reads from .env file — never hardcode credentials here.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


# ── AWS ──────────────────────────────────────────────────────
AWS_REGION       = os.getenv("AWS_REGION", "us-east-1")
AWS_PROFILE      = os.getenv("AWS_PROFILE", "default")

# ── S3 Data Lake ─────────────────────────────────────────────
S3_BUCKET        = os.getenv("S3_BUCKET")
S3_RAW_PREFIX    = os.getenv("S3_RAW_PREFIX", "raw")
S3_PROCESSED_PREFIX = os.getenv("S3_PROCESSED_PREFIX", "processed")
S3_ATHENA_RESULTS   = os.getenv("S3_ATHENA_RESULTS", "athena-results")

# Convenience: full S3 paths
S3_RAW_PATH       = f"s3://{S3_BUCKET}/{S3_RAW_PREFIX}"
S3_PROCESSED_PATH = f"s3://{S3_BUCKET}/{S3_PROCESSED_PREFIX}"
S3_ATHENA_PATH    = f"s3://{S3_BUCKET}/{S3_ATHENA_RESULTS}"

# ── Glue ─────────────────────────────────────────────────────
GLUE_DATABASE    = os.getenv("GLUE_DATABASE", "healthcare_db")
GLUE_CRAWLER     = os.getenv("GLUE_CRAWLER_NAME", "healthcare_crawler")

# ── Athena ───────────────────────────────────────────────────
ATHENA_DATABASE  = os.getenv("ATHENA_DATABASE", "healthcare_db")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")

# ── CMS Data ─────────────────────────────────────────────────
CMS_DATA_URL     = os.getenv(
    "CMS_DATA_URL",
    "https://data.cms.gov/provider-summary-by-type-of-service/"
    "medicare-part-d-prescribers/medicare-part-d-prescribers-by-"
    "provider-and-drug/data"
)
CMS_DATASET_YEAR = os.getenv("CMS_DATASET_YEAR", "2022")

# ── Pipeline ─────────────────────────────────────────────────
PIPELINE_ENV     = os.getenv("PIPELINE_ENV", "development")
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO")
CHUNK_SIZE       = int(os.getenv("CHUNK_SIZE", "100000"))

# ── Local paths (for dev/testing without hitting AWS) ────────
LOCAL_DATA_DIR   = BASE_DIR / "data"
LOCAL_RAW_DIR    = LOCAL_DATA_DIR / "raw"
LOCAL_PROCESSED_DIR = LOCAL_DATA_DIR / "processed"


def validate_config():
    """Raise early if required config is missing."""
    required = {
        "S3_BUCKET": S3_BUCKET,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            f"Did you copy .env.example to .env and fill it in?"
        )


def print_config():
    """Print current config (safe — no secrets)."""
    print(f"""
╔══════════════════════════════════════════╗
║     Healthcare ETL Pipeline Config       ║
╠══════════════════════════════════════════╣
║  Environment : {PIPELINE_ENV:<26}║
║  AWS Region  : {AWS_REGION:<26}║
║  S3 Bucket   : {S3_BUCKET or 'NOT SET':<26}║
║  Raw Path    : {S3_RAW_PREFIX:<26}║
║  Processed   : {S3_PROCESSED_PREFIX:<26}║
║  Glue DB     : {GLUE_DATABASE:<26}║
║  Athena DB   : {ATHENA_DATABASE:<26}║
╚══════════════════════════════════════════╝
    """)


if __name__ == "__main__":
    validate_config()
    print_config()
