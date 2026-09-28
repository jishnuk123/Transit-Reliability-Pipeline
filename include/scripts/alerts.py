"""
Failure alerting.

Sends an email via Gmail's SMTP server when an Airflow task fails.
Designed to be used as a DAG-level `on_failure_callback`, which Airflow
automatically calls whenever any task in the DAG fails, passing a
`context` dictionary with details about the failure.
"""

import os
import smtplib
from email.mime.text import MIMEText

from dotenv import load_dotenv

load_dotenv()


def send_failure_alert(context):
    """
    Airflow calls this automatically on task failure, passing a `context`
    dict containing details like which task failed and why. We pull the
    relevant pieces out of it to build a useful, specific alert message.
    """
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    exception = context.get("exception")
    log_url = task_instance.log_url

    subject = f"[ALERT] Airflow task failed: {dag_id}.{task_id}"
    body = (
        f"Task '{task_id}' in DAG '{dag_id}' failed.\n\n"
        f"Exception: {exception}\n\n"
        f"Logs: {log_url}"
    )

    sender = os.getenv("ALERT_EMAIL_ADDRESS")
    app_password = os.getenv("ALERT_EMAIL_APP_PASSWORD")

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = sender  # sending the alert to yourself

    # Gmail's SMTP server, over an SSL-encrypted connection on port 465.
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender, app_password)
        server.sendmail(sender, [sender], msg.as_string())

    print(f"Failure alert email sent for {dag_id}.{task_id}")