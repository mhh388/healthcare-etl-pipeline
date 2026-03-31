# 🏥 Healthcare ETL Pipeline on AWS
> End-to-end data engineering project: CMS Medicare data → S3 Data Lake → Glue → Athena

## 🗺️ Architecture
```
CMS Public Data (HTTPS)
        │
        ▼
┌─────────────────┐
│  01_ingest.py   │  Download + validate raw data
└────────┬────────┘
         │ raw CSV/parquet
         ▼
┌─────────────────┐
│  AWS S3 Bucket  │  s3://your-bucket/raw/
│  (Data Lake)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  02_transform   │  PySpark cleaning + transformation
│     .py         │
└────────┬────────┘
         │ cleaned parquet
         ▼
┌─────────────────┐
│  AWS S3 Bucket  │  s3://your-bucket/processed/
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   AWS Glue      │  Data catalog + schema discovery
│   Crawler       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  AWS Athena     │  SQL analytics queries
└─────────────────┘
```

## 📁 Project Structure
```
healthcare-etl-pipeline/
├── README.md
├── requirements.txt
├── .env.example              # AWS config template (never commit .env!)
├── .gitignore
├── config/
│   └── settings.py           # Centralized config
├── src/
│   ├── __init__.py
│   ├── 01_ingest.py          # Download CMS data → S3
│   ├── 02_transform.py       # PySpark transformations
│   ├── 03_quality_check.py   # Data validation
│   └── 04_athena_queries.py  # Run SQL analytics via Athena
├── sql/
│   ├── create_tables.sql     # Athena DDL
│   └── analytics_queries.sql # Business insight queries
├── tests/
│   ├── __init__.py
│   ├── test_ingest.py
│   ├── test_transform.py
│   └── test_quality.py
├── notebooks/
│   └── exploration.ipynb     # EDA notebook
├── .github/
│   └── workflows/
│       └── ci.yml            # GitHub Actions CI/CD (Phase 3)
└── docs/
    └── architecture.md       # Architecture notes for your portfolio
```

## 🚀 Setup Guide (Mac)

### Prerequisites
Make sure you have these installed. Run each check in your terminal:
```bash
python3 --version      # Need 3.9+
pip3 --version
git --version
aws --version          # If missing, install below
java -version          # Need Java 11+ for PySpark
```

### Step 1 — Install AWS CLI (if not already installed)
```bash
# Install via Homebrew (recommended)
brew install awscli

# Verify
aws --version
```

### Step 2 — Install Java (required for PySpark)
```bash
# Check if you have it
java -version

# If not, install via Homebrew
brew install openjdk@11

# Add to your shell profile (~/.zshrc or ~/.bash_profile)
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 11)' >> ~/.zshrc
source ~/.zshrc
```

### Step 3 — Clone & set up the project
```bash
# Clone your repo (after you create it on GitHub)
git clone https://github.com/YOUR_USERNAME/healthcare-etl-pipeline.git
cd healthcare-etl-pipeline

# Create a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 4 — Configure AWS credentials
```bash
# Configure your AWS CLI with your credentials
aws configure

# You'll be prompted for:
# AWS Access Key ID:     [from AWS Console → IAM → Your user → Security credentials]
# AWS Secret Access Key: [same place]
# Default region name:   us-east-1
# Default output format: json

# Verify it works
aws s3 ls
```

### Step 5 — Set up your environment file
```bash
# Copy the example env file
cp .env.example .env

# Open .env in VS Code and fill in your values
code .env
```

### Step 6 — Create your S3 bucket
```bash
# Replace 'your-name' with something unique (bucket names are global)
aws s3 mb s3://healthcare-etl-mengqi --region us-east-1

# Verify
aws s3 ls
```

### Step 7 — Run the pipeline
```bash
# Step by step
python src/01_ingest.py        # Download CMS data → S3
python src/02_transform.py     # Transform with PySpark
python src/03_quality_check.py # Validate data quality
python src/04_athena_queries.py # Run analytics queries

# Or run all at once
python src/run_pipeline.py
```

## 📊 Dataset
**CMS Medicare Part D Prescribers (Public)**
- Source: https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers
- Size: ~3M rows, ~25 columns
- Free, no account required
- Contains: prescriber info, drug names, claim counts, total costs

## 🧪 Running Tests
```bash
pytest tests/ -v
```

## 📈 What This Demonstrates (for your resume)
- Scalable data ingestion from public APIs
- PySpark transformation and data cleaning
- AWS S3 data lake (raw → processed zones)
- AWS Glue data cataloging
- AWS Athena SQL analytics
- Data quality validation
- CI/CD with GitHub Actions
- Git version control and project documentation

## 🔍 Key Analytical Findings

### Outlier Analysis: Ultra-High Cost Drugs (>$100k per claim)
Running the quality check pipeline surfaced 5 records exceeding $100,000
cost-per-claim. Investigation confirmed these are **not data errors** but
legitimate rare disease treatments:

| Drug | Indication | Cost/Claim |
|---|---|---|
| Elapegademase-Lvlr (Revcovi) | ADA-SCID (rare immune deficiency) | $256,508 |
| Glycerol Phenylbutyrate (Ravicti) | Urea cycle disorders | $120,891 |
| Asfotase Alfa (Strensiq) | Hypophosphatasia (rare bone disease) | $104–135k |
| Asciminib HCl (Scemblix) | Chronic myeloid leukemia | $107,363 |

**Insight:** These orphan drugs represent <0.001% of claims but
disproportionately impact total Medicare spend — a pattern relevant
to pharmacy benefit management and formulary design decisions.

**Pipeline response:** Flagged as WARNING (not CRITICAL) since values
are medically valid. Documented for downstream analyst awareness.