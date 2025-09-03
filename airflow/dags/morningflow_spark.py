"""
Airflow DAG: morningflow_flight_delays
- Purpose: Schedule and trigger the local Spark job that reads flight delays from S3
  and writes aggregations back to S3.
- Schedule: Daily at 6 AM America/Los_Angeles (PT)
- How it runs: Uses BashOperator to execute `scripts/flight_delays_s3.py` with
  the configured bucket and AWS profile.

Alerts:
- Email only: set ALERT_EMAIL (comma-separated) and ensure Airflow SMTP is configured.
  You can also set EMAIL_ON_SUCCESS=true to receive success notifications.
"""

import os
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email  # uses Airflow's SMTP config

# Project path (host or container). In Docker image we use /opt/airflow/morningflow
PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/airflow/morningflow")

# S3 bucket and AWS profile. You can override these via Airflow Variables/Env.
BUCKET = os.getenv("BUCKET", "morningflow")
AWS_PROFILE = os.getenv("AWS_PROFILE", "morningflow")

# Email alert configuration via environment (defaults to your address if not set)
ALERT_EMAILS = [e.strip() for e in os.getenv("ALERT_EMAIL", "eddefnyarkoj@gmail.com").split(",") if e.strip()]


def _format_context_message(context) -> str:
	"""Build a short message from task context for alerts."""
	dag_id = context.get("dag_run").dag_id if context.get("dag_run") else context.get("dag" ).dag_id
	task_id = context.get("task_instance").task_id
	state = context.get("task_instance").state
	run_id = context.get("dag_run").run_id if context.get("dag_run") else "manual"
	log_url = context.get("task_instance").log_url
	return f"[{state}] {dag_id}.{task_id} run_id={run_id}\nLogs: {log_url}"


def on_failure(context):
	if not ALERT_EMAILS:
		return
	msg = _format_context_message(context)
	try:
		send_email(to=ALERT_EMAILS, subject="Airflow Task Failed", html_content=msg.replace("\n", "<br/>"))
	except Exception:
		pass


def on_success(context):
	if os.getenv("EMAIL_ON_SUCCESS", "false").lower() not in {"1", "true", "yes"}:
		return
	if not ALERT_EMAILS:
		return
	msg = _format_context_message(context)
	try:
		send_email(to=ALERT_EMAILS, subject="Airflow Task Succeeded", html_content=msg.replace("\n", "<br/>"))
	except Exception:
		pass


# Default args for email-on-failure via Airflow core (works alongside callbacks)
default_args = {}
if ALERT_EMAILS:
	default_args.update({
		"email": ALERT_EMAILS,
		"email_on_failure": True,
		"email_on_retry": False,
	})

# Define the DAG with timezone-aware start_date and a daily schedule at 06:00 PT
with DAG(
	dag_id="morningflow_flight_delays",
	start_date=pendulum.datetime(2025, 1, 1, tz="America/Los_Angeles"),
	schedule="0 6 * * *",  # 6 AM PT daily
	catchup=False,
	tags=["spark", "s3", "morningflow"],
	description="Run the MorningFlow Spark job daily",
	default_args=default_args,
) as dag:
	# Bash script: run Python directly (works in container and on host)
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
		on_failure_callback=on_failure,
		on_success_callback=on_success,
	)

	run_flight_delays
