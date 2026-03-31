"""
src/04_athena_queries.py
────────────────────────
Step 4 of the Healthcare ETL Pipeline.

What this script does:
  1. Creates the Glue database and registers the processed Parquet data
  2. Runs a suite of analytics SQL queries via AWS Athena
  3. Saves query results to S3 and locally
  4. Prints a summary of key business insights

Why Athena?
  - Serverless SQL on S3 — no database to manage
  - Pay per query (~$5 per TB scanned)
  - Parquet + partitioning makes it very cheap
  - Results can feed directly into Tableau, Power BI, QuickSight

Run:
  python src/04_athena_queries.py
  python src/04_athena_queries.py --query top_drugs
"""

import sys
import time
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone

import boto3
import pandas as pd
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import (
    AWS_REGION, S3_BUCKET,
    S3_PROCESSED_PREFIX, S3_ATHENA_PATH,
    ATHENA_DATABASE, ATHENA_WORKGROUP,
    LOCAL_PROCESSED_DIR, CMS_DATASET_YEAR,
    validate_config
)


# ── SQL Analytics Queries ─────────────────────────────────────────────────────
# These demonstrate the kind of business questions a data analyst would answer.
# Written with Athena-compatible SQL (Presto/Trino dialect).

ANALYTICS_QUERIES = {

    "top_drugs_by_cost": {
        "description": "Top 20 most expensive drugs by total Medicare spend",
        "sql": """
            SELECT
                generic_name,
                drug_type,
                COUNT(DISTINCT prescriber_npi)      AS prescriber_count,
                SUM(total_claims)                   AS total_claims,
                SUM(total_beneficiaries)            AS total_patients,
                SUM(total_drug_cost)                AS total_spend,
                ROUND(AVG(cost_per_claim), 2)       AS avg_cost_per_claim,
                ROUND(AVG(cost_per_beneficiary), 2) AS avg_cost_per_patient
            FROM {database}.partd_prescribers
            WHERE data_year = {year}
              AND total_drug_cost IS NOT NULL
            GROUP BY generic_name, drug_type
            ORDER BY total_spend DESC
            LIMIT 20
        """,
    },

    "prescriber_specialty_analysis": {
        "description": "Prescribing patterns by medical specialty",
        "sql": """
            SELECT
                prescriber_specialty,
                COUNT(DISTINCT prescriber_npi)      AS prescriber_count,
                COUNT(*)                            AS drug_record_count,
                SUM(total_claims)                   AS total_claims,
                SUM(total_drug_cost)                AS total_spend,
                ROUND(AVG(cost_per_claim), 2)       AS avg_cost_per_claim,
                ROUND(
                    SUM(total_drug_cost) / NULLIF(SUM(total_claims), 0), 2
                )                                   AS weighted_cost_per_claim
            FROM {database}.partd_prescribers
            WHERE data_year = {year}
              AND prescriber_specialty IS NOT NULL
              AND total_claims > 100
            GROUP BY prescriber_specialty
            ORDER BY total_spend DESC
            LIMIT 25
        """,
    },

    "state_level_summary": {
        "description": "Medicare drug spending summary by state",
        "sql": """
            SELECT
                prescriber_state,
                COUNT(DISTINCT prescriber_npi)      AS unique_prescribers,
                COUNT(DISTINCT generic_name)        AS unique_drugs,
                SUM(total_claims)                   AS total_claims,
                SUM(total_beneficiaries)            AS total_patients,
                SUM(total_drug_cost)                AS total_spend,
                ROUND(
                    SUM(total_drug_cost) / NULLIF(SUM(total_beneficiaries), 0), 2
                )                                   AS spend_per_patient,
                ROUND(
                    SUM(senior_drug_cost) / NULLIF(SUM(total_drug_cost), 0) * 100, 2
                )                                   AS senior_spend_pct
            FROM {database}.partd_prescribers
            WHERE data_year = {year}
              AND prescriber_state IS NOT NULL
            GROUP BY prescriber_state
            ORDER BY total_spend DESC
        """,
    },

    "brand_vs_generic": {
        "description": "Brand vs generic drug utilization and cost comparison",
        "sql": """
            SELECT
                drug_type,
                COUNT(DISTINCT generic_name)        AS unique_drugs,
                COUNT(DISTINCT prescriber_npi)      AS unique_prescribers,
                SUM(total_claims)                   AS total_claims,
                SUM(total_drug_cost)                AS total_spend,
                ROUND(AVG(cost_per_claim), 2)       AS avg_cost_per_claim,
                ROUND(
                    SUM(total_drug_cost) / NULLIF(SUM(total_claims), 0), 2
                )                                   AS weighted_avg_cost_per_claim,
                ROUND(
                    SUM(total_claims) * 100.0 /
                    SUM(SUM(total_claims)) OVER (), 2
                )                                   AS claim_share_pct,
                ROUND(
                    SUM(total_drug_cost) * 100.0 /
                    SUM(SUM(total_drug_cost)) OVER (), 2
                )                                   AS spend_share_pct
            FROM {database}.partd_prescribers
            WHERE data_year = {year}
            GROUP BY drug_type
        """,
    },

    "senior_utilization": {
        "description": "Senior (65+) Medicare utilization analysis",
        "sql": """
            SELECT
                prescriber_state,
                SUM(total_claims)                   AS total_claims,
                SUM(senior_total_claims)            AS senior_claims,
                SUM(total_drug_cost)                AS total_spend,
                SUM(senior_drug_cost)               AS senior_spend,
                ROUND(
                    SUM(senior_total_claims) * 100.0 /
                    NULLIF(SUM(total_claims), 0), 2
                )                                   AS senior_claim_rate_pct,
                ROUND(
                    SUM(senior_drug_cost) * 100.0 /
                    NULLIF(SUM(total_drug_cost), 0), 2
                )                                   AS senior_spend_rate_pct
            FROM {database}.partd_prescribers
            WHERE data_year = {year}
              AND senior_suppression_flag != 'suppressed'
              AND prescriber_state IS NOT NULL
            GROUP BY prescriber_state
            ORDER BY senior_spend_rate_pct DESC
        """,
    },
}


# ── Athena Helpers ────────────────────────────────────────────────────────────

def get_athena_client():
    return boto3.client("athena", region_name=AWS_REGION)


def get_glue_client():
    return boto3.client("glue", region_name=AWS_REGION)


def create_glue_database(glue_client, database: str):
    """Create Glue database if it doesn't exist."""
    try:
        glue_client.create_database(
            DatabaseInput={"Name": database, "Description": "Healthcare ETL Pipeline"}
        )
        logger.info(f"Created Glue database: {database}")
    except glue_client.exceptions.AlreadyExistsException:
        logger.info(f"Glue database already exists: {database}")


def create_athena_table(athena_client, database: str, year: str):
    """Create or replace the Athena table pointing to S3 processed data."""
    s3_location = f"s3://{S3_BUCKET}/{S3_PROCESSED_PREFIX}/dataset=partd_prescribers/year={year}/"

    ddl = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {database}.partd_prescribers (
        prescriber_npi          STRING,
        prescriber_last_name    STRING,
        prescriber_first_name   STRING,
        prescriber_city         STRING,
        state_fips              STRING,
        prescriber_specialty    STRING,
        specialty_source        STRING,
        brand_name              STRING,
        generic_name            STRING,
        total_claims            FLOAT,
        total_30day_supply      FLOAT,
        total_day_supply        FLOAT,
        total_drug_cost         FLOAT,
        total_beneficiaries     FLOAT,
        senior_suppression_flag STRING,
        senior_total_claims     FLOAT,
        senior_30day_supply     FLOAT,
        senior_drug_cost        FLOAT,
        senior_beneficiaries    FLOAT,
        cost_per_claim          FLOAT,
        cost_per_beneficiary    FLOAT,
        senior_claim_rate       FLOAT,
        drug_type               STRING,
        data_year               INT,
        processed_at            STRING,
        pipeline_version        STRING
    )
    PARTITIONED BY (prescriber_state STRING)
    STORED AS PARQUET
    LOCATION '{s3_location}'
    TBLPROPERTIES ('parquet.compress'='SNAPPY')
    """

    run_athena_query(athena_client, ddl, database, wait=True)
    logger.info(f"Athena table created/confirmed: {database}.partd_prescribers")

    # Load partitions
    msck = f"MSCK REPAIR TABLE {database}.partd_prescribers"
    run_athena_query(athena_client, msck, database, wait=True)
    logger.info("Partitions loaded via MSCK REPAIR TABLE")


def run_athena_query(
    athena_client,
    sql: str,
    database: str,
    wait: bool = True,
    timeout_seconds: int = 300,
) -> str:
    """Execute an Athena query and optionally wait for completion. Returns QueryExecutionId."""
    response = athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={
            "OutputLocation": S3_ATHENA_PATH,
        },
        WorkGroup=ATHENA_WORKGROUP,
    )
    execution_id = response["QueryExecutionId"]

    if not wait:
        return execution_id

    # Poll until done
    start_time = time.time()
    while True:
        status_resp = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = status_resp["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            elapsed = time.time() - start_time
            scanned = status_resp["QueryExecution"]["Statistics"].get(
                "DataScannedInBytes", 0
            ) / 1024 / 1024
            logger.debug(f"Query {execution_id[:8]}… succeeded in {elapsed:.1f}s, scanned {scanned:.2f} MB")
            return execution_id

        elif state in ("FAILED", "CANCELLED"):
            reason = status_resp["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
            raise RuntimeError(f"Athena query failed: {reason}\nSQL: {sql[:200]}")

        elif time.time() - start_time > timeout_seconds:
            raise TimeoutError(f"Athena query timed out after {timeout_seconds}s")

        time.sleep(2)


def fetch_results(athena_client, execution_id: str) -> pd.DataFrame:
    """Fetch query results from Athena and return as a DataFrame."""
    paginator = athena_client.get_paginator("get_query_results")
    rows, headers = [], []

    for page_idx, page in enumerate(
        paginator.paginate(QueryExecutionId=execution_id)
    ):
        result_rows = page["ResultSet"]["Rows"]
        if page_idx == 0:
            headers = [col["VarCharValue"] for col in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
        for row in result_rows:
            rows.append([col.get("VarCharValue", "") for col in row["Data"]])

    return pd.DataFrame(rows, columns=headers)


# ── Main Pipeline Step ────────────────────────────────────────────────────────

def run_athena_analytics(
    year: str,
    query_names: list[str] = None,
):
    """Run analytics queries against the processed data in Athena."""
    logger.info("=" * 60)
    logger.info("  STEP 4 — ATHENA ANALYTICS QUERIES")
    logger.info("=" * 60)

    validate_config()

    athena_client = get_athena_client()
    glue_client   = get_glue_client()

    # Setup Glue + Athena table
    create_glue_database(glue_client, ATHENA_DATABASE)
    create_athena_table(athena_client, ATHENA_DATABASE, year)

    # Determine which queries to run
    queries_to_run = query_names or list(ANALYTICS_QUERIES.keys())
    results_summary = {}

    output_dir = LOCAL_PROCESSED_DIR / "query_results"
    output_dir.mkdir(parents=True, exist_ok=True)

    for query_name in queries_to_run:
        if query_name not in ANALYTICS_QUERIES:
            logger.warning(f"Unknown query: {query_name} — skipping")
            continue

        query_info = ANALYTICS_QUERIES[query_name]
        sql = query_info["sql"].format(database=ATHENA_DATABASE, year=year).strip()

        logger.info(f"\n── Running: {query_name}")
        logger.info(f"   {query_info['description']}")

        try:
            execution_id = run_athena_query(athena_client, sql, ATHENA_DATABASE, wait=True)
            df = fetch_results(athena_client, execution_id)

            # Save results locally
            result_path = output_dir / f"{query_name}_{year}.csv"
            df.to_csv(result_path, index=False)

            results_summary[query_name] = {
                "status": "success",
                "rows_returned": len(df),
                "execution_id": execution_id,
                "saved_to": str(result_path),
            }

            # Print top 5 rows
            logger.info(f"   → {len(df)} rows returned")
            print(f"\n{'─'*60}")
            print(f"  {query_info['description']}")
            print(f"{'─'*60}")
            print(df.head(5).to_string(index=False))
            print()

        except Exception as e:
            logger.error(f"   Query failed: {e}")
            results_summary[query_name] = {"status": "failed", "error": str(e)}

    # Save run summary
    summary = {
        "year": year,
        "queries_run": len(queries_to_run),
        "queries_succeeded": sum(1 for r in results_summary.values() if r["status"] == "success"),
        "results": results_summary,
        "run_at": datetime.now(timezone.utc).isoformat(),
    }
    summary_path = output_dir / f"query_summary_{year}.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    logger.info("=" * 60)
    logger.success(f"  STEP 4 COMPLETE ✓")
    logger.info(f"  Queries run     : {summary['queries_run']}")
    logger.info(f"  Queries success : {summary['queries_succeeded']}")
    logger.info(f"  Results saved   : {output_dir}")
    logger.info("=" * 60)

    return summary


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Healthcare ETL Pipeline — Step 4: Athena Analytics"
    )
    parser.add_argument("--year",  default=CMS_DATASET_YEAR)
    parser.add_argument("--query", nargs="*",
                        choices=list(ANALYTICS_QUERIES.keys()),
                        help="Run specific queries (default: all)")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)

    run_athena_analytics(year=args.year, query_names=args.query)
