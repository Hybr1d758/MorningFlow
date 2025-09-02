# MorningFlow (Spark + Amazon S3)

A simple, reproducible example that reads a public flight delays file, processes it with Apache Spark, and saves summary results back to your Amazon S3 bucket.

You don’t need to be a programmer to run it. Just follow the steps below.

## What you’ll get
- A working Spark job you can run any time
- Results saved in your S3 bucket (easy to view or download)
- Optional scheduling with Airflow so it runs every morning
- Optional email alerts on success/failure

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

Email alerts (optional)
```bash
# Who should receive emails
export ALERT_EMAIL="you@example.com"
# Get success emails too (optional)
export EMAIL_ON_SUCCESS=true

# Set your SMTP details (example for Gmail with an App Password)
export AIRFLOW__SMTP__SMTP_HOST="smtp.gmail.com"
export AIRFLOW__SMTP__SMTP_PORT="587"
export AIRFLOW__SMTP__SMTP_STARTTLS=True
export AIRFLOW__SMTP__SMTP_USER="your@gmail.com"
export AIRFLOW__SMTP__SMTP_PASSWORD="your_app_password"
export AIRFLOW__SMTP__SMTP_MAIL_FROM="your@gmail.com"
```
Restart Airflow after setting these.

## 6) (Optional) Run everything with Docker
No manual Python/Java installs. Docker will run the job in a container.

Build the image (do this once):
```bash
docker build -t morningflow:latest .
```

Run using environment variables (no profile needed):
```bash
docker run --rm \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" \
  -e AWS_REGION=$AWS_REGION \
  -e BUCKET=morningflow \
  morningflow:latest --bucket morningflow
```

Or run using your local AWS profile (mount your creds):
```bash
# Linux/macOS path to your AWS credentials/config
# (~/.aws must exist and contain your profile)
docker run --rm \
  -v $HOME/.aws:/root/.aws:ro \
  -e AWS_PROFILE=morningflow \
  -e BUCKET=morningflow \
  morningflow:latest --bucket morningflow
```

### Docker Compose: Airflow in containers
Start Airflow (webserver + scheduler) in Docker, using the DAG in this repo:
```bash
docker compose up -d airflow-init
docker compose up -d airflow-webserver airflow-scheduler
# Open: http://localhost:8080 (user: admin / pass: admin)
```
- To run the Spark job once in a container:
```bash
docker compose run --rm spark-run --bucket ${BUCKET:-morningflow}
```
- To stop everything:
```bash
docker compose down
```

## Reproduce on another Mac
```bash
# 1) Clone the repo
git clone https://github.com/Hybr1d758/MorningFlow.git
cd MorningFlow

# 2) Follow steps 1–4 above, or use Docker (step 6)
```

## Common problems and quick fixes
- “Unable to locate credentials” → Run `aws configure --profile morningflow` or load `.env`.
- “AccessDenied (403)” → Your IAM user needs the S3 permissions shown above.
- “Unable to locate a Java Runtime” → Install Java 11 and run `export JAVA_HOME=$(/usr/libexec/java_home -v11)`.
- “File not found” → Make sure the CSV is uploaded to `s3://<bucket>/flights/raw/departuredelays.csv`.

## What this project is doing (in plain English)
- We download a sample CSV of flight departures and delays.
- Spark reads the CSV and cleans it.
- We calculate useful summaries (like average delay per airport).
- The results are stored in your S3 bucket so you can view or analyze them later.
