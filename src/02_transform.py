"""
src/02_transform.py
───────────────────
Step 2 of the Healthcare ETL Pipeline.

What this script does:
  1. Reads raw CMS CSV from S3 (or locally)
  2. Cleans and standardizes the data with PySpark
  3. Applies business transformations (derived columns, aggregations)
  4. Writes cleaned data to S3 processed zone as Parquet (columnar, compressed)
  5. Produces a transformation summary report

Why Parquet?
  - 10x smaller than CSV (columnar compression)
  - 100x faster for analytical queries in Athena
  - Native format for AWS Glue and Databricks

Run:
  python src/02_transform.py
  python src/02_transform.py --local-only   (read/write local files)
  python src/02_transform.py --sample 50000 (process a sample for testing)
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone

from loguru import logger

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, LongType
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import (
    AWS_REGION, S3_BUCKET,
    S3_RAW_PREFIX, S3_PROCESSED_PREFIX,
    LOCAL_RAW_DIR, LOCAL_PROCESSED_DIR,
    CMS_DATASET_YEAR, validate_config
)


# ── Schema Definition ─────────────────────────────────────────────────────────
# Explicitly define schema — never rely on PySpark's inferred types for prod

CMS_RAW_SCHEMA = StructType([
    StructField("Prscrbr_NPI",                      StringType(),  True),
    StructField("Prscrbr_Last_Org_Name",             StringType(),  True),
    StructField("Prscrbr_First_Name",                StringType(),  True),
    StructField("Prscrbr_City",                      StringType(),  True),
    StructField("Prscrbr_State_Abrvtn",              StringType(),  True),
    StructField("Prscrbr_State_FIPS",                StringType(),  True),
    StructField("Prscrbr_Type",                      StringType(),  True),
    StructField("Prscrbr_Type_Src",                  StringType(),  True),
    StructField("Brnd_Name",                         StringType(),  True),
    StructField("Gnrc_Name",                         StringType(),  True),
    StructField("Tot_Clms",                          FloatType(),   True),
    StructField("Tot_30day_Eqv_Day_Suply",           FloatType(),   True),
    StructField("Tot_Day_Suply",                     FloatType(),   True),
    StructField("Tot_Drug_Cst",                      FloatType(),   True),
    StructField("Tot_Benes",                         FloatType(),   True),
    StructField("GE65_Sprsn_Flag",                   StringType(),  True),
    StructField("GE65_Tot_Clms",                     FloatType(),   True),
    StructField("GE65_Tot_30day_Eqv_Day_Suply",      FloatType(),   True),
    StructField("GE65_Tot_Drug_Cst",                 FloatType(),   True),
    StructField("GE65_Tot_Benes",                    FloatType(),   True),
])


# ── Spark Session ─────────────────────────────────────────────────────────────

def create_spark_session(local_mode: bool = False) -> SparkSession:
    """
    Create and configure a SparkSession.
    - local_mode: runs on your laptop (no cluster needed)
    - production: would connect to EMR or Databricks cluster
    """
    builder = (
        SparkSession.builder
        .appName("HealthcareETL_Transform")
        .config("spark.sql.adaptive.enabled", "true")          # AQE for perf
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")           # tune for local
    )

    if local_mode:
        builder = builder.master("local[*]")
    else:
        # Add S3 access configs for AWS
        builder = (
            builder
            .master("local[*]")  # Change to cluster URL in production
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # Reduce Spark noise in logs

    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Spark master: {spark.sparkContext.master}")
    return spark


# ── Transformation Functions ──────────────────────────────────────────────────

def read_raw_data(spark: SparkSession, path: str, sample: int = None) -> DataFrame:
    """Read raw CMS CSV into a Spark DataFrame."""
    logger.info(f"Reading raw data from: {path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")   # Always use explicit schema in prod
        .schema(CMS_RAW_SCHEMA)
        .csv(path)
    )

    if sample:
        logger.info(f"Sampling {sample:,} rows for testing")
        df = df.limit(sample)

    row_count = df.count()
    col_count = len(df.columns)
    logger.info(f"Loaded {row_count:,} rows × {col_count} columns")
    return df


def clean_data(df: DataFrame) -> DataFrame:
    """
    Apply data cleaning transformations:
    - Standardize string columns (trim whitespace, uppercase states)
    - Handle suppressed values (CMS uses '*' for small counts)
    - Cast types and handle nulls
    - Remove duplicate NPI + drug combinations
    """
    logger.info("Applying data cleaning transformations...")

    df_clean = (
        df
        # Standardize text fields
        .withColumn("Prscrbr_State_Abrvtn",
                    F.upper(F.trim(F.col("Prscrbr_State_Abrvtn"))))
        .withColumn("Prscrbr_City",
                    F.initcap(F.trim(F.col("Prscrbr_City"))))
        .withColumn("Prscrbr_Type",
                    F.trim(F.col("Prscrbr_Type")))
        .withColumn("Brnd_Name",
                    F.trim(F.col("Brnd_Name")))
        .withColumn("Gnrc_Name",
                    F.trim(F.col("Gnrc_Name")))

        # Replace suppression flag '*' with null for numeric analysis
        # CMS suppresses values when fewer than 11 beneficiaries to protect privacy
        .withColumn("GE65_Sprsn_Flag",
                    F.when(F.col("GE65_Sprsn_Flag") == "*", "suppressed")
                     .otherwise(F.col("GE65_Sprsn_Flag")))

        # Remove rows with no NPI (can't link to a prescriber)
        .filter(F.col("Prscrbr_NPI").isNotNull())

        # Remove rows with zero or negative claims (data quality issue)
        .filter(F.col("Tot_Clms") > 0)

        # Drop exact duplicates
        .dropDuplicates(["Prscrbr_NPI", "Gnrc_Name"])
    )

    clean_count = df_clean.count()
    logger.info(f"After cleaning: {clean_count:,} rows")
    return df_clean


def add_derived_columns(df: DataFrame, year: str) -> DataFrame:
    """
    Add business-logic derived columns:
    - cost_per_claim: average drug cost per claim
    - cost_per_beneficiary: total drug cost per beneficiary
    - senior_claim_rate: % of claims from 65+ beneficiaries
    - drug_type: brand vs generic classification
    - pipeline metadata columns (year, processed_at)
    """
    logger.info("Adding derived columns...")

    df_enriched = (
        df
        # Cost efficiency metrics
        .withColumn("cost_per_claim",
                    F.when(F.col("Tot_Clms") > 0,
                           F.round(F.col("Tot_Drug_Cst") / F.col("Tot_Clms"), 2))
                     .otherwise(F.lit(None).cast(FloatType())))

        .withColumn("cost_per_beneficiary",
                    F.when(F.col("Tot_Benes") > 0,
                           F.round(F.col("Tot_Drug_Cst") / F.col("Tot_Benes"), 2))
                     .otherwise(F.lit(None).cast(FloatType())))

        # Senior (65+) utilization rate
        .withColumn("senior_claim_rate",
                    F.when(
                        (F.col("GE65_Sprsn_Flag") != "suppressed") &
                        (F.col("Tot_Clms") > 0),
                        F.round(F.col("GE65_Tot_Clms") / F.col("Tot_Clms"), 4)
                    ).otherwise(F.lit(None).cast(FloatType())))

        # Drug type: brand vs generic
        # Simple heuristic: if Brnd_Name is not null/empty, it's a brand drug
        .withColumn("drug_type",
                    F.when(
                        F.col("Brnd_Name").isNotNull() & (F.col("Brnd_Name") != ""),
                        "brand"
                    ).otherwise("generic"))

        # Rename columns to snake_case for Athena compatibility
        .withColumnRenamed("Prscrbr_NPI",          "prescriber_npi")
        .withColumnRenamed("Prscrbr_Last_Org_Name", "prescriber_last_name")
        .withColumnRenamed("Prscrbr_First_Name",    "prescriber_first_name")
        .withColumnRenamed("Prscrbr_City",          "prescriber_city")
        .withColumnRenamed("Prscrbr_State_Abrvtn",  "prescriber_state")
        .withColumnRenamed("Prscrbr_State_FIPS",    "state_fips")
        .withColumnRenamed("Prscrbr_Type",          "prescriber_specialty")
        .withColumnRenamed("Prscrbr_Type_Src",      "specialty_source")
        .withColumnRenamed("Brnd_Name",             "brand_name")
        .withColumnRenamed("Gnrc_Name",             "generic_name")
        .withColumnRenamed("Tot_Clms",              "total_claims")
        .withColumnRenamed("Tot_30day_Eqv_Day_Suply", "total_30day_supply")
        .withColumnRenamed("Tot_Day_Suply",         "total_day_supply")
        .withColumnRenamed("Tot_Drug_Cst",          "total_drug_cost")
        .withColumnRenamed("Tot_Benes",             "total_beneficiaries")
        .withColumnRenamed("GE65_Sprsn_Flag",       "senior_suppression_flag")
        .withColumnRenamed("GE65_Tot_Clms",         "senior_total_claims")
        .withColumnRenamed("GE65_Tot_30day_Eqv_Day_Suply", "senior_30day_supply")
        .withColumnRenamed("GE65_Tot_Drug_Cst",     "senior_drug_cost")
        .withColumnRenamed("GE65_Tot_Benes",        "senior_beneficiaries")

        # Pipeline metadata — essential for data lineage
        .withColumn("data_year",       F.lit(int(year)))
        .withColumn("processed_at",    F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("pipeline_version", F.lit("1.0.0"))
    )

    logger.info(f"Derived columns added. Final schema has {len(df_enriched.columns)} columns")
    return df_enriched


def write_processed_data(
    df: DataFrame,
    output_path: str,
    year: str,
    partition_cols: list = None,
) -> dict:
    """
    Write transformed data to Parquet format, partitioned by state.
    Partitioning by state means Athena only reads relevant partitions — huge cost savings.
    """
    partition_cols = partition_cols or ["prescriber_state"]
    full_output_path = f"{output_path}/dataset=partd_prescribers/year={year}"

    logger.info(f"Writing processed data to: {full_output_path}")
    logger.info(f"Format: Parquet (snappy compressed), partitioned by: {partition_cols}")

    (
        df.write
        .mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(full_output_path)
    )

    logger.success(f"Write complete: {full_output_path}")
    return {
        "output_path": full_output_path,
        "partition_cols": partition_cols,
        "format": "parquet",
        "compression": "snappy",
    }


def generate_transform_report(df_raw: DataFrame, df_processed: DataFrame, year: str) -> dict:
    """Generate a summary report of what the transformation did."""
    raw_count = df_raw.count()
    processed_count = df_processed.count()

    report = {
        "year": year,
        "raw_row_count": raw_count,
        "processed_row_count": processed_count,
        "rows_removed": raw_count - processed_count,
        "removal_rate_pct": round((raw_count - processed_count) / raw_count * 100, 2),
        "output_columns": df_processed.columns,
        "column_count": len(df_processed.columns),
        "transformed_at": datetime.now(timezone.utc).isoformat(),
    }

    # Basic stats on key numeric columns
    stats = df_processed.select(
        F.mean("total_claims").alias("avg_claims"),
        F.mean("total_drug_cost").alias("avg_drug_cost"),
        F.mean("cost_per_claim").alias("avg_cost_per_claim"),
        F.countDistinct("prescriber_npi").alias("unique_prescribers"),
        F.countDistinct("generic_name").alias("unique_drugs"),
        F.countDistinct("prescriber_state").alias("unique_states"),
    ).collect()[0]

    report["stats"] = {
        "avg_claims_per_record":    round(stats["avg_claims"] or 0, 2),
        "avg_drug_cost_per_record": round(stats["avg_drug_cost"] or 0, 2),
        "avg_cost_per_claim":       round(stats["avg_cost_per_claim"] or 0, 2),
        "unique_prescribers":       stats["unique_prescribers"],
        "unique_drugs":             stats["unique_drugs"],
        "unique_states":            stats["unique_states"],
    }

    return report


# ── Main Pipeline Step ────────────────────────────────────────────────────────

def run_transform(year: str, local_only: bool = False, sample: int = None):
    """
    Run the full transformation step:
      1. Create Spark session
      2. Read raw data from S3 (or local)
      3. Clean data
      4. Add derived columns
      5. Write to processed zone as Parquet
      6. Generate and save transformation report
    """
    logger.info("=" * 60)
    logger.info("  STEP 2 — DATA TRANSFORMATION")
    logger.info("=" * 60)

    if not local_only:
        validate_config()

    # Determine input/output paths
    if local_only:
        input_path  = str(LOCAL_RAW_DIR / f"cms_partd_{year}.csv")
        output_path = str(LOCAL_PROCESSED_DIR)
    else:
        input_path  = f"s3a://{S3_BUCKET}/{S3_RAW_PREFIX}/source=cms/dataset=partd_prescribers/year={year}/cms_partd_{year}.csv"
        output_path = f"s3a://{S3_BUCKET}/{S3_PROCESSED_PREFIX}"

    # 1. Create Spark
    spark = create_spark_session(local_mode=local_only)

    try:
        # 2. Read
        df_raw = read_raw_data(spark, input_path, sample=sample)

        # 3. Clean
        df_clean = clean_data(df_raw)

        # 4. Enrich
        df_processed = add_derived_columns(df_clean, year)

        # 5. Write
        write_info = write_processed_data(df_processed, output_path, year)

        # 6. Report
        report = generate_transform_report(df_raw, df_processed, year)
        report["write_info"] = write_info

        # Save report locally
        report_dir = LOCAL_PROCESSED_DIR / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / f"transform_report_{year}.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info("=" * 60)
        logger.success("  STEP 2 COMPLETE ✓")
        logger.info(f"  Input rows    : {report['raw_row_count']:,}")
        logger.info(f"  Output rows   : {report['processed_row_count']:,}")
        logger.info(f"  Rows removed  : {report['rows_removed']:,} ({report['removal_rate_pct']}%)")
        logger.info(f"  Unique drugs  : {report['stats']['unique_drugs']:,}")
        logger.info(f"  Unique NPIs   : {report['stats']['unique_prescribers']:,}")
        logger.info(f"  Output path   : {write_info['output_path']}")
        logger.info("=" * 60)

        return report

    finally:
        spark.stop()


# ── CLI Entry Point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Healthcare ETL Pipeline — Step 2: Transform with PySpark"
    )
    parser.add_argument("--year", default=CMS_DATASET_YEAR,
                        help="Dataset year")
    parser.add_argument("--local-only", action="store_true",
                        help="Read/write locally — skip S3")
    parser.add_argument("--sample", type=int, default=None,
                        help="Process only N rows (for testing)")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)

    run_transform(
        year=args.year,
        local_only=args.local_only,
        sample=args.sample,
    )
