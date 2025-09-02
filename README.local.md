# MorningFlow - Spark + S3 (Flight Delays)

Beginner-friendly Spark job that reads the Databricks flight delays dataset from S3, computes aggregations, and writes results back to S3. Includes `.env` support and AWS preflight checks.

## Quick Start
```bash
# 1) Create venv & install
cd "/Users/edwardjr/Downloads/upwork /Engineering/MorningFlow"
python3.11 -m venv .venv && source .venv/bin/activate
python -m pip install --upgrade pip
pip install pyspark==3.5.1 pandas==2.2.2 pyarrow==16.1.0 loguru==0.7.2 boto3==1.34.148 delta-spark==3.2.0 python-dotenv==1.0.1

# 2) Java 11 and AWS CLI
brew install --cask temurin@11 && export JAVA_HOME=$(/usr/libexec/java_home -v11)
brew install awscli

# 3) Credentials (choose ONE)
aws configure --profile morningflow && export AWS_PROFILE=morningflow
# or
printf "AWS_ACCESS_KEY_ID=...\nAWS_SECRET_ACCESS_KEY=...\nAWS_REGION=<region>\nAWS_DEFAULT_REGION=<region>\nBUCKET=morningflow\n" > .env
set -a; source .env; set +a

# 4) Upload dataset
mkdir -p data
curl -L -o data/departuredelays.csv https://raw.githubusercontent.com/databricks/LearningSparkV2/master/databricks-datasets/learning-spark-v2/flights/departuredelays.csv
aws s3 cp data/departuredelays.csv s3://morningflow/flights/raw/departuredelays.csv

# 5) Run
python scripts/flight_delays_s3.py --bucket morningflow --sample 100000
```

## Prerequisites (macOS)
- Homebrew
- Python 3.11 (venv)
- Java 11 (Temurin)
- AWS CLI v2

## Configure AWS (profile or .env)
- Profile (recommended):
```bash
aws configure --profile morningflow
export AWS_PROFILE=morningflow
aws sts get-caller-identity
```
- `.env` (gitignored):
```bash
AWS_ACCESS_KEY_ID=YOUR_KEY
AWS_SECRET_ACCESS_KEY=YOUR_SECRET
AWS_SESSION_TOKEN=
AWS_REGION=<your-region>
AWS_DEFAULT_REGION=<your-region>
BUCKET=morningflow
```
Load it: `set -a; source .env; set +a`

## Minimal IAM policy
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "BucketMeta","Effect": "Allow","Action": ["s3:ListBucket","s3:GetBucketLocation"],"Resource": "arn:aws:s3:::morningflow"},
    {"Sid": "ObjectRW","Effect": "Allow","Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucketMultipartUploads","s3:AbortMultipartUpload"],"Resource": "arn:aws:s3:::morningflow/*"}
  ]
}
```

## Run the Spark job (full)
```bash
source .venv/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v11)
python scripts/flight_delays_s3.py --bucket morningflow --profile morningflow --region <bucket-region>
```
Outputs:
- `s3://morningflow/flights/output/avg_delay_by_origin/`
- `s3://morningflow/flights/output/avg_delay_by_destination/`
- `s3://morningflow/flights/output/route_stats/`
- `s3://morningflow/flights/output/daily_delay_counts/`

## Airflow (schedule daily 6 AM PT)
- DAG: `airflow/dags/morningflow_spark.py`
- Assumes this project path and a working `.venv`.
- Quick local run (system Airflow):
```bash
pip install "apache-airflow==2.9.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
airflow db init
export AIRFLOW_HOME=~/airflow
mkdir -p "$AIRFLOW_HOME/dags"
cp -R airflow/dags/* "$AIRFLOW_HOME/dags/"
airflow users create --role Admin --username admin --password admin --firstname a --lastname d --email a@d
airflow webserver -p 8080 &
airflow scheduler &
```
- In UI: enable `morningflow_flight_delays` (runs 6 AM PT daily). To test now: trigger DAG manually.

## Challenges we encountered (and fixes)
- Missing AWS CLI / Java 11 → install via Homebrew and set `JAVA_HOME`.
- Credentials not loaded → use AWS profile or `.env`.
- AccessDenied (403) → add ListBucket/GetBucketLocation + object R/W.
- PATH_NOT_FOUND → upload CSV to the expected S3 prefix.
- Date parsing in Spark ≥3 → handle MMddHHmm with assumed year.
- .env formatting → one variable per line.

## Top 3 long‑term real‑world use cases
- Airline operations analytics (route/airport KPIs, alerts, staffing).
- Data lake inputs for ML (delay prediction, optimization, forecasting).
- BI/reporting via Athena/Databricks/Redshift Spectrum over S3/Delta.

## GitHub
```bash
git init
git add .
git commit -m "MorningFlow: Spark + S3 flight delays job + README"
git branch -M main
git remote add origin git@github.com:<you>/<repo>.git
git push -u origin main
```

## Next
- Switch Airflow to Docker Compose or Databricks Jobs if preferred.
