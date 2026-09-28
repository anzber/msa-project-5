FROM apache/airflow:2.10.5

# Providers required by the POC DAG:
#   - postgres   : PostgresHook (read/write business data)
#   - common-sql : shared SQL abstractions used by the Postgres provider
#   - smtp       : EmailOperator / SMTP notifications
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
