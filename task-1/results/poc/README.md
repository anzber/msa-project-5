# POC (Apache Airflow) - Task 1

```bash
docker compose up -d --build
```

- Airflow UI: http://localhost:8080 (airflow / airflow)
- Mailpit UI: http://localhost:8025 (перехват писем)
- Business DB: localhost:5433, БД batchdb, batch / batch

Запуск пайплайна `customer_batch_pipeline`:

```bash
docker compose exec airflow-scheduler airflow dags unpause customer_batch_pipeline
docker compose exec airflow-scheduler airflow dags trigger customer_batch_pipeline
```
