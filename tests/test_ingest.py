"""
tests/test_ingest.py
────────────────────
Unit tests for the ingestion step.
Uses moto to mock AWS S3 — no real AWS calls made during tests.

Run:
  pytest tests/test_ingest.py -v
"""

import json
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
import boto3
from moto import mock_s3

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingest import validate_raw_data, compute_md5


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_csv(tmp_path):
    """Create a minimal valid CMS-format CSV for testing."""
    csv_content = """Prscrbr_NPI,Prscrbr_Last_Org_Name,Prscrbr_First_Name,Prscrbr_City,Prscrbr_State_Abrvtn,Prscrbr_State_FIPS,Prscrbr_Type,Prscrbr_Type_Src,Brnd_Name,Gnrc_Name,Tot_Clms,Tot_30day_Eqv_Day_Suply,Tot_Day_Suply,Tot_Drug_Cst,Tot_Benes,GE65_Sprsn_Flag,GE65_Tot_Clms,GE65_Tot_30day_Eqv_Day_Suply,GE65_Tot_Drug_Cst,GE65_Tot_Benes
1234567890,Smith,John,New York,NY,36,Internal Medicine,S,Lipitor,Atorvastatin Calcium,150,150,4500,1200.50,45,,100,3000,800.25,30
1234567891,Jones,Mary,Boston,MA,25,Family Practice,S,,Metformin Hcl,200,200,6000,400.00,60,,150,4500,300.00,45
1234567892,Brown,Robert,Chicago,IL,17,Cardiology,S,Eliquis,Apixaban,75,75,2250,3500.75,25,,50,1500,2333.83,18
"""
    csv_file = tmp_path / "test_cms.csv"
    csv_file.write_text(csv_content)
    return csv_file


@pytest.fixture
def large_sample_csv(tmp_path):
    """Create a CSV with enough rows to pass minimum row count check."""
    import io
    header = "Prscrbr_NPI,Prscrbr_Last_Org_Name,Prscrbr_First_Name,Prscrbr_City,Prscrbr_State_Abrvtn,Prscrbr_State_FIPS,Prscrbr_Type,Prscrbr_Type_Src,Brnd_Name,Gnrc_Name,Tot_Clms,Tot_30day_Eqv_Day_Suply,Tot_Day_Suply,Tot_Drug_Cst,Tot_Benes,GE65_Sprsn_Flag,GE65_Tot_Clms,GE65_Tot_30day_Eqv_Day_Suply,GE65_Tot_Drug_Cst,GE65_Tot_Benes\n"
    rows = []
    for i in range(1_000_001):  # Just above MIN_EXPECTED_ROWS
        rows.append(f"{1000000000+i},Smith,John,NYC,NY,36,Internal Medicine,S,,Metformin,100,100,3000,500.00,30,,70,2100,350.00,21")

    csv_file = tmp_path / "large_test_cms.csv"
    with open(csv_file, "w") as f:
        f.write(header)
        f.write("\n".join(rows))
    return csv_file


# ── Tests: compute_md5 ────────────────────────────────────────────────────────

def test_compute_md5_returns_string(tmp_path):
    f = tmp_path / "test.txt"
    f.write_text("hello world")
    result = compute_md5(f)
    assert isinstance(result, str)
    assert len(result) == 32  # MD5 hex is always 32 chars


def test_compute_md5_same_file_same_hash(tmp_path):
    f = tmp_path / "test.txt"
    f.write_text("consistent content")
    assert compute_md5(f) == compute_md5(f)


def test_compute_md5_different_files_different_hash(tmp_path):
    f1 = tmp_path / "f1.txt"
    f2 = tmp_path / "f2.txt"
    f1.write_text("content A")
    f2.write_text("content B")
    assert compute_md5(f1) != compute_md5(f2)


# ── Tests: validate_raw_data ──────────────────────────────────────────────────

def test_validate_raises_if_file_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        validate_raw_data(tmp_path / "nonexistent.csv")


def test_validate_raises_if_file_too_small(tmp_path):
    f = tmp_path / "tiny.csv"
    f.write_text("col1,col2\n1,2")  # Less than 1 MB
    with pytest.raises(ValueError, match="too small"):
        validate_raw_data(f)


def test_validate_raises_if_too_few_rows(sample_csv):
    """3-row file should fail minimum row count."""
    with pytest.raises(ValueError, match="Row count"):
        validate_raw_data(sample_csv)


def test_validate_returns_dict_with_expected_keys(large_sample_csv):
    result = validate_raw_data(large_sample_csv)
    assert isinstance(result, dict)
    expected_keys = {"status", "filename", "file_size_mb", "row_count",
                     "column_count", "md5_checksum", "validated_at"}
    assert expected_keys.issubset(result.keys())


def test_validate_status_is_passed(large_sample_csv):
    result = validate_raw_data(large_sample_csv)
    assert result["status"] == "passed"


def test_validate_row_count_is_accurate(large_sample_csv):
    result = validate_raw_data(large_sample_csv)
    assert result["row_count"] == 1_000_001


# ── Tests: S3 upload (mocked) ─────────────────────────────────────────────────

@mock_s3
def test_upload_to_s3_creates_object(tmp_path):
    from src.ingest import upload_to_s3

    # Create mock bucket
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    # Create a test file
    test_file = tmp_path / "cms_partd_2022.csv"
    test_file.write_text("col1,col2\n1,2\n3,4")

    uri = upload_to_s3(
        filepath=test_file,
        year="2022",
        s3_client=s3,
        bucket="test-bucket",
        prefix="raw",
    )

    assert uri.startswith("s3://test-bucket/raw/")
    assert "cms_partd_2022.csv" in uri

    # Verify object exists in S3
    objects = s3.list_objects_v2(Bucket="test-bucket")
    keys = [obj["Key"] for obj in objects.get("Contents", [])]
    assert any("cms_partd_2022.csv" in k for k in keys)
