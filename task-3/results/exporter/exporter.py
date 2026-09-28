"""Простой экспорт одной таблицы PostgreSQL в CSV.

Параметры берутся из переменных окружения:
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
  EXPORT_TABLE - имя экспортируемой таблицы (по умолчанию shipments)
  OUTPUT_DIR   - каталог для CSV (по умолчанию /export)

Пример: k8s CronJob ежедневно в 20:00 выгружает таблицу перевозок в CSV.
"""

from __future__ import annotations

import os
import sys
import time

import psycopg2

DB_HOST = os.getenv("DB_HOST", "transport-postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "transport")
DB_USER = os.getenv("DB_USER", "transport")
DB_PASSWORD = os.getenv("DB_PASSWORD", "transport")

EXPORT_TABLE = os.getenv("EXPORT_TABLE", "shipments")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/export")


def main() -> int:
    started = time.time()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stamp = time.strftime("%Y%m%d", time.gmtime())
    out_path = os.path.join(OUTPUT_DIR, f"{EXPORT_TABLE}_{stamp}.csv")

    print(f"[export] connecting to {DB_HOST}:{DB_PORT}/{DB_NAME} as {DB_USER}", flush=True)
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=10,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {EXPORT_TABLE}")
            total = cur.fetchone()[0]
            print(f"[export] table {EXPORT_TABLE}: {total} rows", flush=True)

            with open(out_path, "w", encoding="utf-8", newline="") as fh:
                cur.copy_expert(
                    f"COPY (SELECT * FROM {EXPORT_TABLE}) TO STDOUT WITH CSV HEADER",
                    fh,
                )

            with conn.cursor() as check:
                check.execute(
                    "SELECT count(*) FROM pg_stat_user_tables WHERE relname = %s",
                    (EXPORT_TABLE,),
                )
    finally:
        conn.close()

    written = sum(1 for _ in open(out_path, encoding="utf-8")) - 1
    size = os.path.getsize(out_path)
    elapsed = time.time() - started
    print(f"[export] wrote {out_path}: {written} data rows, {size} bytes, {elapsed:.2f}s", flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"[export] ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
