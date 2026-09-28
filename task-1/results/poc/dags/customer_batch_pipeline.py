"""POC batch pipeline for Sprint 5 / Task 1.

The pipeline consolidates customer data that lives in several heterogeneous
sources and produces a consolidated report:

  * CSV file storage  -> ``delivery_statuses.csv`` (delivery statuses per order)
  * PostgreSQL        -> ``users``, ``orders``, ``payments``

It demonstrates the four capabilities required by the task:

  1. Reading from a data source (file system **and** database).
  2. Data analysis with conditional branching (``BranchPythonOperator``).
  3. E-mail notification on success **and** on failure (``EmailOperator`` +
     ``on_failure_callback``).
  4. Retry policy on pipeline steps (``retries`` + exponential backoff, plus a
     deliberately flaky load task that fails once and then succeeds).

Everything is orchestrated by Apache Airflow, which is the technology chosen
and justified in ``../solution-justification.md``.
"""

from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

DATA_DIR = os.environ.get("POC_DATA_DIR", "/opt/airflow/data")
BUSINESS_DB_CONN = os.environ.get("POC_BUSINESS_CONN", "business_db")
DEFAULT_VOLUME_THRESHOLD = int(os.environ.get("POC_VOLUME_THRESHOLD", "100"))
FLAKY_LOAD = os.environ.get("POC_FLAKY_LOAD", "true").lower() == "true"

DELIVERY_CSV = os.path.join(DATA_DIR, "delivery_statuses.csv")
REPORT_PATH = os.path.join(DATA_DIR, "customer_report.jsonl")
FLAKY_MARKER = os.path.join(DATA_DIR, ".load_attempt")

ALERT_RECIPIENTS = ["data-oncall@example.com"]
SUCCESS_RECIPIENTS = ["data-team@example.com"]


def _notify_failure(context) -> None:
    """on_failure_callback: send an alert e-mail with run details."""
    ti = context["task_instance"]
    dag_id = context["dag"].dag_id
    subject = f"[FAILED] {dag_id}.{ti.task_id} (run {context['run_id']})"
    html = f"""
    <h3>Batch pipeline failed</h3>
    <ul>
      <li><b>DAG:</b> {dag_id}</li>
      <li><b>Task:</b> {ti.task_id}</li>
      <li><b>Run id:</b> {context['run_id']}</li>
      <li><b>Logical date:</b> {context['logical_date']}</li>
      <li><b>Try number:</b> {ti.try_number}</li>
      <li><b>Exception:</b> {context.get('exception')}</li>
      <li><b>Log:</b> <a href="{ti.log_url}">{ti.log_url}</a></li>
    </ul>
    """
    log.error("Sending failure notification: %s", subject)
    send_email(to=ALERT_RECIPIENTS, subject=subject, html_content=html)


DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email": ALERT_RECIPIENTS,
    "email_on_failure": False,  # a richer custom callback is used instead
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
    "on_failure_callback": _notify_failure,
}


def _prepare_run(**context) -> None:
    """Reset per-run state so the retry demo is reproducible."""
    if os.path.exists(FLAKY_MARKER):
        os.remove(FLAKY_MARKER)
    log.info("Prepared run %s, conf=%s", context["run_id"], context["dag_run"].conf)


def _extract_delivery_statuses(**context) -> dict:
    with open(DELIVERY_CSV, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    log.info("Extracted %s delivery-status rows from %s", len(rows), DELIVERY_CSV)
    return {"source": "csv://delivery_statuses.csv", "rows": len(rows)}


def _extract_table(table: str):
    def _extract(**context) -> dict:
        hook = PostgresHook(postgres_conn_id=BUSINESS_DB_CONN)
        count = hook.get_first(f"SELECT count(*) FROM {table}")[0]
        log.info("Extracted %s rows from PostgreSQL table %s", count, table)
        return {"source": f"postgres://{table}", "rows": count}

    return _extract


def _consolidate(**context) -> dict:
    """Join all sources and write the consolidated report as JSON Lines."""
    hook = PostgresHook(postgres_conn_id=BUSINESS_DB_CONN)
    users = hook.get_records("SELECT id, email, segment FROM users")
    orders = hook.get_records("SELECT user_id, amount FROM orders")
    payments = hook.get_records(
        "SELECT o.user_id, p.amount "
        "FROM payments p JOIN orders o ON o.id = p.order_id "
        "WHERE p.status = 'PAID'"
    )
    with open(DELIVERY_CSV, newline="", encoding="utf-8") as fh:
        deliveries = {r["order_id"]: r["delivery_status"] for r in csv.DictReader(fh)}

    per_user = {uid: {"orders_count": 0, "orders_amount": 0.0, "paid_amount": 0.0} for uid, *_ in users}
    for user_id, amount in orders:
        if user_id in per_user:
            per_user[user_id]["orders_count"] += 1
            per_user[user_id]["orders_amount"] += float(amount)
    for user_id, amount in payments:
        if user_id in per_user:
            per_user[user_id]["paid_amount"] += float(amount)

    report_rows = []
    for user_id, email, segment in users:
        agg = per_user[user_id]
        report_rows.append(
            {
                "customer_id": user_id,
                "email": email,
                "segment": segment,
                "orders_count": agg["orders_count"],
                "orders_amount": round(agg["orders_amount"], 2),
                "paid_amount": round(agg["paid_amount"], 2),
                "last_delivery_status": deliveries.get(str(user_id), "UNKNOWN"),
            }
        )

    with open(REPORT_PATH, "w", encoding="utf-8") as fh:
        for row in report_rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = {
        "report_rows": len(report_rows),
        "records_processed": len(orders) + len(payments) + len(deliveries),
        "total_amount": round(sum(r["orders_amount"] for r in report_rows), 2),
        "report_path": REPORT_PATH,
    }
    log.info("Consolidated report: %s", summary)
    return summary


def _validate_report(**context) -> None:
    """Data-quality gate. Supports a simulated failure for the demo."""
    conf = context["dag_run"].conf or {}
    if conf.get("simulate_failure"):
        raise ValueError("Simulated data-quality failure (dag_run.conf.simulate_failure=true)")
    summary = context["ti"].xcom_pull(task_ids="consolidate") or {}
    if summary.get("report_rows", 0) == 0:
        raise ValueError("Consolidation produced an empty report")
    log.info("Validation passed: %s report rows", summary["report_rows"])


def _branch_on_volume(**context) -> str:
    """Branch: process in chunks for large volumes, otherwise at once."""
    summary = context["ti"].xcom_pull(task_ids="consolidate") or {}
    conf = context["dag_run"].conf or {}
    threshold = int(conf.get("volume_threshold", DEFAULT_VOLUME_THRESHOLD))
    processed = int(summary.get("records_processed", 0))
    if processed >= threshold:
        log.info("records_processed=%s >= threshold=%s -> process_in_chunks", processed, threshold)
        return "process_in_chunks"
    log.info("records_processed=%s < threshold=%s -> process_at_once", processed, threshold)
    return "process_at_once"


def _process_in_chunks(**context) -> dict:
    summary = context["ti"].xcom_pull(task_ids="consolidate") or {}
    rows = summary.get("report_rows", 0)
    chunk_size = 500
    chunks = max(1, -(-rows // chunk_size))
    log.info("Large-volume path: %s rows processed in %s chunk(s) of %s", rows, chunks, chunk_size)
    return {"mode": "chunked", "chunks": chunks}


def _process_at_once(**context) -> dict:
    summary = context["ti"].xcom_pull(task_ids="consolidate") or {}
    log.info("Small-volume path: %s rows processed in a single pass", summary.get("report_rows", 0))
    return {"mode": "single-pass"}


def _load_report(**context) -> int:
    """Load the report into PostgreSQL. Fails once to demonstrate retries."""
    attempt = 0
    if os.path.exists(FLAKY_MARKER):
        try:
            attempt = int(open(FLAKY_MARKER, encoding="utf-8").read().strip() or "0")
        except ValueError:
            attempt = 0
    attempt += 1
    with open(FLAKY_MARKER, "w", encoding="utf-8") as fh:
        fh.write(str(attempt))

    if FLAKY_LOAD and attempt < 2:
        raise RuntimeError(
            f"Transient connection error on load attempt #{attempt}; Airflow will retry the task"
        )

    with open(REPORT_PATH, encoding="utf-8") as fh:
        rows = [json.loads(line) for line in fh if line.strip()]

    hook = PostgresHook(postgres_conn_id=BUSINESS_DB_CONN)
    hook.run("TRUNCATE TABLE customer_reports")
    hook.insert_rows(
        table="customer_reports",
        rows=[
            (
                r["customer_id"],
                r["email"],
                r["segment"],
                r["orders_count"],
                r["orders_amount"],
                r["paid_amount"],
                r["last_delivery_status"],
            )
            for r in rows
        ],
        target_fields=[
            "customer_id",
            "email",
            "segment",
            "orders_count",
            "orders_amount",
            "paid_amount",
            "last_delivery_status",
        ],
    )
    log.info("Loaded %s rows into customer_reports (attempt #%s)", len(rows), attempt)
    return len(rows)


with DAG(
    dag_id="customer_batch_pipeline",
    description="POC: batch consolidation of customer data from CSV + PostgreSQL (Task 1)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["poc", "batch", "task-1"],
    params={"volume_threshold": DEFAULT_VOLUME_THRESHOLD, "simulate_failure": False},
) as dag:
    prepare_run = PythonOperator(task_id="prepare_run", python_callable=_prepare_run)

    extract_delivery_statuses = PythonOperator(
        task_id="extract_delivery_statuses", python_callable=_extract_delivery_statuses
    )
    extract_orders = PythonOperator(task_id="extract_orders", python_callable=_extract_table("orders"))
    extract_payments = PythonOperator(task_id="extract_payments", python_callable=_extract_table("payments"))
    extract_users = PythonOperator(task_id="extract_users", python_callable=_extract_table("users"))

    consolidate = PythonOperator(task_id="consolidate", python_callable=_consolidate)
    validate_report = PythonOperator(task_id="validate_report", python_callable=_validate_report)
    branch_on_volume = BranchPythonOperator(task_id="branch_on_volume", python_callable=_branch_on_volume)

    process_in_chunks = PythonOperator(task_id="process_in_chunks", python_callable=_process_in_chunks)
    process_at_once = PythonOperator(task_id="process_at_once", python_callable=_process_at_once)

    load_report = PythonOperator(
        task_id="load_report",
        python_callable=_load_report,
        # one of the two branch targets is always skipped -> wait for the other one
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    notify_success = EmailOperator(
        task_id="notify_success",
        to=SUCCESS_RECIPIENTS,
        subject="[SUCCESS] customer_batch_pipeline {{ ds }}",
        html_content="""
        <h3>Batch pipeline finished successfully</h3>
        <p>DAG <b>{{ dag.dag_id }}</b> for logical date <b>{{ ds }}</b> completed.</p>
        <p>Report rows loaded: <b>{{ ti.xcom_pull(task_ids='load_report') }}</b></p>
        <p>Conf: {{ dag_run.conf }}</p>
        """,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    prepare_run >> [
        extract_delivery_statuses,
        extract_orders,
        extract_payments,
        extract_users,
    ] >> consolidate >> validate_report >> branch_on_volume
    branch_on_volume >> [process_in_chunks, process_at_once] >> load_report >> notify_success >> end
