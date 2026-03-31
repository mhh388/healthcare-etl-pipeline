"""
src/03_quality_check.py
───────────────────────
Step 3 of the Healthcare ETL Pipeline.

What this script does:
  1. Reads the processed Parquet data from S3 (or local)
  2. Runs a suite of data quality checks:
     - Completeness checks (null rates per column)
     - Uniqueness checks (duplicate NPI + drug combos)
     - Range checks (costs, claims must be positive)
     - Consistency checks (senior claims ≤ total claims)
     - Distribution checks (flag statistical outliers)
  3. Produces a quality report with PASS/FAIL per check
  4. Raises an error if any critical checks fail (pipeline stops)

This is what separates junior data work from senior data engineering —
always validate your data before it reaches analysts or dashboards.

Run:
  python src/03_quality_check.py
  python src/03_quality_check.py --local-only
  python src/03_quality_check.py --strict   (fail on any warning too)
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Literal

import pandas as pd
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import (
    AWS_REGION, S3_BUCKET, S3_PROCESSED_PREFIX,
    LOCAL_PROCESSED_DIR, CMS_DATASET_YEAR, validate_config
)


# ── Data Structures ───────────────────────────────────────────────────────────

@dataclass
class QualityCheck:
    name: str
    category: str                    # completeness | uniqueness | range | consistency
    severity: Literal["critical", "warning", "info"]
    status: Literal["pass", "fail", "skip"] = "skip"
    value: float = None
    threshold: float = None
    message: str = ""
    details: dict = field(default_factory=dict)


# ── Quality Check Functions ───────────────────────────────────────────────────

def check_completeness(df: pd.DataFrame) -> list[QualityCheck]:
    """Check null rates for each column. Critical columns must be 100% complete."""
    checks = []

    critical_cols = ["prescriber_npi", "generic_name", "total_claims", "total_drug_cost"]
    warning_cols  = ["prescriber_specialty", "prescriber_state", "prescriber_city"]

    all_cols = critical_cols + warning_cols + [
        c for c in df.columns if c not in critical_cols + warning_cols
    ]

    for col in all_cols:
        if col not in df.columns:
            continue

        null_rate = df[col].isnull().mean()
        severity  = "critical" if col in critical_cols else "warning" if col in warning_cols else "info"
        threshold = 0.0 if col in critical_cols else 0.10  # 10% null allowed for non-critical

        check = QualityCheck(
            name=f"completeness_{col}",
            category="completeness",
            severity=severity,
            value=round(null_rate, 4),
            threshold=threshold,
            details={"null_count": int(df[col].isnull().sum()), "total_rows": len(df)},
        )

        if null_rate <= threshold:
            check.status  = "pass"
            check.message = f"Null rate {null_rate:.2%} ≤ threshold {threshold:.2%}"
        else:
            check.status  = "fail"
            check.message = f"Null rate {null_rate:.2%} exceeds threshold {threshold:.2%}"

        checks.append(check)

    return checks


def check_uniqueness(df: pd.DataFrame) -> list[QualityCheck]:
    """Check for unexpected duplicates in key combinations."""
    checks = []

    # NPI + generic_name should be unique after transformation
    if "prescriber_npi" in df.columns and "generic_name" in df.columns:
        total = len(df)
        unique = df[["prescriber_npi", "generic_name"]].drop_duplicates().shape[0]
        dup_rate = 1 - (unique / total) if total > 0 else 0

        check = QualityCheck(
            name="uniqueness_npi_drug",
            category="uniqueness",
            severity="critical",
            value=round(dup_rate, 4),
            threshold=0.0,
            details={"total_rows": total, "unique_combinations": unique, "duplicates": total - unique},
        )
        check.status  = "pass" if dup_rate == 0 else "fail"
        check.message = (
            f"No duplicates found ✓" if dup_rate == 0
            else f"{total - unique:,} duplicate NPI+drug combinations found"
        )
        checks.append(check)

    return checks


def check_ranges(df: pd.DataFrame) -> list[QualityCheck]:
    """Check that numeric columns are within expected ranges."""
    checks = []

    numeric_checks = [
        # (column, min_val, max_val, severity)
        ("total_claims",        0,    1_000_000, "critical"),
        ("total_drug_cost",     0, 1_000_000_000, "critical"),
        ("total_beneficiaries", 0,    1_000_000, "warning"),
        ("cost_per_claim",      0,      100_000, "warning"),
        ("senior_claim_rate",   0,            1, "warning"),
    ]

    for col, min_val, max_val, severity in numeric_checks:
        if col not in df.columns:
            continue

        col_data  = df[col].dropna()
        below_min = (col_data < min_val).sum()
        above_max = (col_data > max_val).sum()
        violations = below_min + above_max

        check = QualityCheck(
            name=f"range_{col}",
            category="range",
            severity=severity,
            value=violations,
            threshold=0,
            details={
                "min_expected": min_val,
                "max_expected": max_val,
                "actual_min": float(col_data.min()) if len(col_data) else None,
                "actual_max": float(col_data.max()) if len(col_data) else None,
                "below_min": int(below_min),
                "above_max": int(above_max),
            },
        )
        check.status  = "pass" if violations == 0 else "fail"
        check.message = (
            f"All {len(col_data):,} values in range [{min_val}, {max_val}]"
            if violations == 0
            else f"{violations:,} values outside range [{min_val}, {max_val}]"
        )
        checks.append(check)

    return checks


def check_consistency(df: pd.DataFrame) -> list[QualityCheck]:
    """Check logical consistency between related columns."""
    checks = []

    # Senior claims should never exceed total claims
    if all(c in df.columns for c in ["senior_total_claims", "total_claims"]):
        mask = (
            df["senior_total_claims"].notna() &
            df["total_claims"].notna() &
            (df["senior_total_claims"] > df["total_claims"])
        )
        violations = mask.sum()

        check = QualityCheck(
            name="consistency_senior_le_total_claims",
            category="consistency",
            severity="critical",
            value=int(violations),
            threshold=0,
            details={"violations": int(violations), "total_rows": len(df)},
        )
        check.status  = "pass" if violations == 0 else "fail"
        check.message = (
            "Senior claims ≤ total claims for all rows ✓"
            if violations == 0
            else f"{violations:,} rows where senior claims > total claims"
        )
        checks.append(check)

    # Senior drug cost should never exceed total drug cost
    if all(c in df.columns for c in ["senior_drug_cost", "total_drug_cost"]):
        mask = (
            df["senior_drug_cost"].notna() &
            df["total_drug_cost"].notna() &
            (df["senior_drug_cost"] > df["total_drug_cost"])
        )
        violations = mask.sum()

        check = QualityCheck(
            name="consistency_senior_cost_le_total_cost",
            category="consistency",
            severity="warning",
            value=int(violations),
            threshold=0,
            details={"violations": int(violations)},
        )
        check.status  = "pass" if violations == 0 else "fail"
        check.message = (
            "Senior drug cost ≤ total drug cost for all rows ✓"
            if violations == 0
            else f"{violations:,} rows where senior cost > total cost"
        )
        checks.append(check)

    return checks


def check_distributions(df: pd.DataFrame) -> list[QualityCheck]:
    """Flag statistical outliers that might indicate data issues."""
    checks = []

    # Check if any single state has >50% of records (would be suspicious)
    if "prescriber_state" in df.columns:
        state_counts  = df["prescriber_state"].value_counts(normalize=True)
        max_state_pct = state_counts.iloc[0] if len(state_counts) > 0 else 0
        max_state     = state_counts.index[0] if len(state_counts) > 0 else "unknown"

        check = QualityCheck(
            name="distribution_state_concentration",
            category="distribution",
            severity="warning",
            value=round(float(max_state_pct), 4),
            threshold=0.50,
            details={
                "dominant_state": max_state,
                "dominant_state_pct": round(float(max_state_pct), 4),
                "total_states": len(state_counts),
            },
        )
        check.status  = "pass" if max_state_pct <= 0.50 else "fail"
        check.message = (
            f"State distribution OK: {max_state} has {max_state_pct:.1%} of records"
        )
        checks.append(check)

    return checks


# ── Report Generation ─────────────────────────────────────────────────────────

def summarize_results(checks: list[QualityCheck]) -> dict:
    """Summarize all check results into a single report."""
    total       = len(checks)
    passed      = sum(1 for c in checks if c.status == "pass")
    failed      = sum(1 for c in checks if c.status == "fail")
    critical_failures = [c for c in checks if c.status == "fail" and c.severity == "critical"]

    summary = {
        "overall_status": "FAILED" if critical_failures else ("PASSED" if failed == 0 else "PASSED_WITH_WARNINGS"),
        "total_checks":    total,
        "passed":          passed,
        "failed":          failed,
        "critical_failures": len(critical_failures),
        "pass_rate":       round(passed / total * 100, 1) if total > 0 else 0,
        "checks": [
            {
                "name":      c.name,
                "category":  c.category,
                "severity":  c.severity,
                "status":    c.status,
                "value":     c.value,
                "threshold": c.threshold,
                "message":   c.message,
                "details":   c.details,
            }
            for c in checks
        ],
        "run_at": datetime.now(timezone.utc).isoformat(),
    }
    return summary


# ── Main Pipeline Step ────────────────────────────────────────────────────────

def run_quality_checks(year: str, local_only: bool = False, strict: bool = False):
    """
    Run all quality checks on the processed data.
    Raises RuntimeError if critical checks fail.
    """
    logger.info("=" * 60)
    logger.info("  STEP 3 — DATA QUALITY CHECKS")
    logger.info("=" * 60)

    if not local_only:
        validate_config()

    # Load processed data (use pandas for quality checks — lighter than Spark)
    if local_only:
        processed_dir = LOCAL_PROCESSED_DIR / f"dataset=partd_prescribers/year={year}"
        parquet_files = list(processed_dir.rglob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(
                f"No parquet files found in {processed_dir}\n"
                f"Run 02_transform.py first."
            )
        df = pd.read_parquet(processed_dir)
    else:
        import boto3
        s3_path = f"s3://{S3_BUCKET}/{S3_PROCESSED_PREFIX}/dataset=partd_prescribers/year={year}/"
        df = pd.read_parquet(s3_path, storage_options={"anon": False})

    logger.info(f"Loaded {len(df):,} rows for quality checking")

    # Run all checks
    all_checks = []
    all_checks += check_completeness(df)
    all_checks += check_uniqueness(df)
    all_checks += check_ranges(df)
    all_checks += check_consistency(df)
    all_checks += check_distributions(df)

    # Print results
    logger.info("\n── Quality Check Results ──────────────────────────────")
    for check in all_checks:
        icon = "✅" if check.status == "pass" else "❌" if check.status == "fail" else "⏭"
        severity_tag = f"[{check.severity.upper()}]" if check.status == "fail" else ""
        logger.info(f"  {icon} {check.name:<50} {severity_tag} {check.message}")

    # Generate summary
    summary = summarize_results(all_checks)

    # Save report
    report_dir = LOCAL_PROCESSED_DIR / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"quality_report_{year}.json"
    with open(report_path, "w") as f:
        json.dump(summary, f, indent=2, default=lambda x: int(x) if hasattr(x, 'item') else str(x))

    # Final status
    logger.info("=" * 60)
    logger.info(f"  Overall Status : {summary['overall_status']}")
    logger.info(f"  Checks Passed  : {summary['passed']}/{summary['total_checks']} ({summary['pass_rate']}%)")
    logger.info(f"  Report saved   : {report_path}")
    logger.info("=" * 60)

    # Raise if critical failures
    if summary["critical_failures"] > 0:
        raise RuntimeError(
            f"Pipeline halted: {summary['critical_failures']} critical quality check(s) failed. "
            f"Review {report_path} for details."
        )

    if strict and summary["failed"] > 0:
        raise RuntimeError(
            f"Pipeline halted (--strict mode): {summary['failed']} quality check(s) failed."
        )

    return summary


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Healthcare ETL Pipeline — Step 3: Data Quality Checks"
    )
    parser.add_argument("--year",       default=CMS_DATASET_YEAR)
    parser.add_argument("--local-only", action="store_true")
    parser.add_argument("--strict",     action="store_true",
                        help="Fail on any check failure, not just critical ones")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)

    run_quality_checks(year=args.year, local_only=args.local_only, strict=args.strict)
