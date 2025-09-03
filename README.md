# MorningFlow (Spark + Amazon S3)

A simple, reproducible example that reads a public flight delays file, processes it with Apache Spark, and saves summary results back to your Amazon S3 bucket.

You don’t need to be a programmer to run it. Just follow the steps below.

## What you’ll get
- A working Spark job you can run any time
- Results saved in your S3 bucket (easy to view or download)
- Optional scheduling with Airflow so it runs every morning

## What you need (macOS)
- An AWS account and an S3 bucket name (example we use: `morningflow`)
- Homebrew (software installer for Mac)
- Java 11, Python 3.11, AWS CLI

## 1) One‑time setup (copy/paste)
Open Terminal and run these in order:

```bash
# Go to the project folder
cd "/Users/edwardjr/Downloads/upwork /Engineering/MorningFlow"

# Create a private Python environment and install tools
python3.11 -m venv .venv && source .venv/bin/activate
python -m pip install --upgrade pip
pip install pyspark==3.5.1 pandas==2.2.2 pyarrow==16.1.0 loguru==0.7.2 boto3==1.34.148 delta-spark==3.2.0 python-dotenv==1.0.1

# Install Java 11 and AWS command line
brew install --cask temurin@11 && export JAVA_HOME=$(/usr/libexec/java_home -v11)
brew install awscli
```

## 2) Connect to your AWS account
Pick ONE of the two ways below.

Option A: Use an AWS profile (recommended)
```bash
aws configure --profile morningflow   # enter Access Key, Secret, region
export AWS_PROFILE=morningflow
aws sts get-caller-identity
```

Option B: Use a .env file in this folder
```bash
# Create file named .env with your details (one per line)
AWS_ACCESS_KEY_ID=YOUR_KEY
AWS_SECRET_ACCESS_KEY=YOUR_SECRET
AWS_REGION=us-west-2
AWS_DEFAULT_REGION=us-west-2
BUCKET=morningflow

# Load it for the current terminal session
set -a; source .env; set +a
aws sts get-caller-identity
```

Minimum S3 permissions for your user
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "BucketMeta","Effect": "Allow","Action": ["s3:ListBucket","s3:GetBucketLocation"],"Resource": "arn:aws:s3:::morningflow"},
    {"Sid": "ObjectRW","Effect": "Allow","Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucketMultipartUploads","s3:AbortMultipartUpload"],"Resource": "arn:aws:s3:::morningflow/*"}
  ]
}
```

## 3) Put the sample data in your S3 bucket
```bash
mkdir -p data
curl -L -o data/departuredelays.csv \
  https://raw.githubusercontent.com/databricks/LearningSparkV2/master/databricks-datasets/learning-spark-v2/flights/departuredelays.csv
aws s3 cp data/departuredelays.csv s3://morningflow/flights/raw/departuredelays.csv
```

## 4) Run the job
```bash
source .venv/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v11)
python scripts/flight_delays_s3.py --bucket morningflow
```

What happens:
- The script reads your file from `s3://morningflow/flights/raw/departuredelays.csv`
- It computes summaries (average delays, route stats, etc.)
- It writes results to `s3://morningflow/flights/output/` in folders:
  - `avg_delay_by_origin/`
  - `avg_delay_by_destination/`
  - `route_stats/`
  - `daily_delay_counts/`

Check in S3 (Terminal):
```bash
aws s3 ls s3://morningflow/flights/output/
```

## 5) (Optional) Schedule daily with Airflow
If you want this to run every morning at 6 AM PT.

Install Airflow and start it:
```bash
pip install "apache-airflow==2.9.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
export AIRFLOW_HOME=~/airflow
airflow db migrate
mkdir -p "$AIRFLOW_HOME/dags"
cp -R airflow/dags/* "$AIRFLOW_HOME/dags/"
airflow webserver -p 8080 &
airflow scheduler &
```

Open the Airflow UI at http://localhost:8080, find the DAG named `morningflow_flight_delays`, turn it on, and click “Trigger DAG” to run immediately.

