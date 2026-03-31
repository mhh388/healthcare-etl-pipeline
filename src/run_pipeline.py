"""
src/run_pipeline.py
───────────────────
Master orchestrator — runs all 4 pipeline steps in sequence.

Run:
  python src/run_pipeline.py                  # Full pipeline
  python src/run_pipeline.py --local-only     # No AWS (local dev/testing)
  python src/run_pipeline.py --sample 10000  # Quick test run with 10k rows
  python src/run_pipeline.py --skip-ingest    # Skip download if data exists
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone

from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingest          import run_ingestion
from src.transform       import run_transform
from src.quality_check   import run_quality_checks
from config.settings     import CMS_DATASET_YEAR, LOCAL_PROCESSED_DIR


def run_pipeline(
    year: str,
    local_only: bool = False,
    sample: int = None,
    skip_ingest: bool = False,
    strict_quality: bool = False,
):
    start_time = datetime.now(timezone.utc)

    logger.info("╔══════════════════════════════════════════════╗")
    logger.info("║   Healthcare ETL Pipeline — Full Run         ║")
    logger.info(f"║   Year       : {year:<30}║")
    logger.info(f"║   Mode       : {'LOCAL' if local_only else 'AWS':<30}║")
    logger.info(f"║   Sample     : {str(sample) if sample else 'Full dataset':<30}║")
    logger.info("╚══════════════════════════════════════════════╝")

    pipeline_log = {
        "year": year,
        "local_only": local_only,
        "sample": sample,
        "started_at": start_time.isoformat(),
        "steps": {},
    }

    # ── Step 1: Ingest ────────────────────────────────────────
    if not skip_ingest:
        logger.info("\n>>> STEP 1/4: Data Ingestion")
        ingest_result = run_ingestion(year=year, local_only=local_only)
        pipeline_log["steps"]["ingest"] = {
            "status": "success",
            "rows": ingest_result["validation"]["row_count"],
            "s3_uri": ingest_result["s3_uri"],
        }
    else:
        logger.info("\n>>> STEP 1/4: Skipped (--skip-ingest)")
        pipeline_log["steps"]["ingest"] = {"status": "skipped"}

    # ── Step 2: Transform ─────────────────────────────────────
    logger.info("\n>>> STEP 2/4: PySpark Transformation")
    transform_result = run_transform(year=year, local_only=local_only, sample=sample)
    pipeline_log["steps"]["transform"] = {
        "status": "success",
        "input_rows": transform_result["raw_row_count"],
        "output_rows": transform_result["processed_row_count"],
    }

    # ── Step 3: Quality Checks ────────────────────────────────
    logger.info("\n>>> STEP 3/4: Data Quality Checks")
    quality_result = run_quality_checks(year=year, local_only=local_only, strict=strict_quality)
    pipeline_log["steps"]["quality"] = {
        "status": "success" if quality_result["critical_failures"] == 0 else "failed",
        "overall": quality_result["overall_status"],
        "pass_rate": quality_result["pass_rate"],
    }

    # ── Step 4: Athena Queries (skip in local-only mode) ──────
    if not local_only:
        logger.info("\n>>> STEP 4/4: Athena Analytics Queries")
        athena_result = run_athena_analytics(year=year)
        pipeline_log["steps"]["athena"] = {
            "status": "success",
            "queries_run": athena_result["queries_run"],
            "queries_succeeded": athena_result["queries_succeeded"],
        }
    else:
        logger.info("\n>>> STEP 4/4: Athena skipped (--local-only mode)")
        pipeline_log["steps"]["athena"] = {"status": "skipped"}

    # ── Summary ───────────────────────────────────────────────
    end_time  = datetime.now(timezone.utc)
    elapsed   = (end_time - start_time).total_seconds()
    pipeline_log["completed_at"] = end_time.isoformat()
    pipeline_log["elapsed_seconds"] = round(elapsed, 1)
    pipeline_log["status"] = "success"

    # Save pipeline run log
    log_dir = LOCAL_PROCESSED_DIR / "pipeline_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"pipeline_run_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_path, "w") as f:
        json.dump(pipeline_log, f, indent=2)

    logger.info("\n╔══════════════════════════════════════════════╗")
    logger.success("║   PIPELINE COMPLETE ✓                        ║")
    logger.info(f"║   Elapsed    : {elapsed:.1f}s{' '*(28-len(str(round(elapsed,1))))  }║")
    logger.info(f"║   Run log    : {str(log_path.name):<30}║")
    logger.info("╚══════════════════════════════════════════════╝")

    return pipeline_log


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Healthcare ETL Pipeline — Full Orchestrator"
    )
    parser.add_argument("--year",         default=CMS_DATASET_YEAR)
    parser.add_argument("--local-only",   action="store_true")
    parser.add_argument("--sample",       type=int, default=None)
    parser.add_argument("--skip-ingest",  action="store_true")
    parser.add_argument("--strict",       action="store_true")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)

    run_pipeline(
        year=args.year,
        local_only=args.local_only,
        sample=args.sample,
        skip_ingest=args.skip_ingest,
        strict_quality=args.strict,
    )
