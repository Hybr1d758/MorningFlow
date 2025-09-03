"""
Airflow DAG: morningflow_flight_delays
- Purpose: Schedule and trigger the local Spark job that reads flight delays from S3
  and writes aggregations back to S3.
- Schedule: Daily at 6 AM America/Los_Angeles (PT)
- How it runs: Uses BashOperator to execute `scripts/flight_delays_s3.py` with
  the configured bucket and AWS profile.
"""

import os
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Project path (host or container)
PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/airflow/morningflow")

# S3 bucket and AWS profile. You can override these via Airflow Variables/Env.
BUCKET = os.getenv("BUCKET", "morningflow")
AWS_PROFILE = os.getenv("AWS_PROFILE", "morningflow")

# Define the DAG with timezone-aware start_date and a daily schedule at 06:00 PT
with DAG(
    dag_id="morningflow_flight_delays",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Los_Angeles"),
    schedule="0 6 * * *",  # 6 AM PT daily
    catchup=False,
    tags=["spark", "s3", "morningflow"],
    description="Run the MorningFlow Spark job daily",
) as dag:
    # Bash script: run Python directly
    bash_cmd = f'''
set -e
cd "{PROJECT_DIR}"
export JAVA_HOME=${{JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}}
python scripts/flight_delays_s3.py --bucket {BUCKET} --profile {AWS_PROFILE}
'''

    run_flight_delays = BashOperator(
        task_id="run_flight_delays",
        bash_command=bash_cmd,
        env={"AWS_PROFILE": AWS_PROFILE, "PROJECT_DIR": PROJECT_DIR},
        doc_md="""
        Executes the flight delays Spark job. The job reads from s3a://<bucket>/flights/raw/
        and writes aggregations under s3a://<bucket>/flights/output/.
        """,
    )

    run_flight_delays
