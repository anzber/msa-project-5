-- Business database schema for the POC (business-db / batchdb).
-- Loaded automatically by the postgres entrypoint from /docker-entrypoint-initdb.d.

CREATE TABLE IF NOT EXISTS users (
    id          SERIAL PRIMARY KEY,
    email       TEXT NOT NULL UNIQUE,
    full_name   TEXT NOT NULL,
    segment     TEXT NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id),
    amount      NUMERIC(12, 2) NOT NULL,
    status      TEXT NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS payments (
    id          SERIAL PRIMARY KEY,
    order_id    INTEGER NOT NULL REFERENCES orders(id),
    amount      NUMERIC(12, 2) NOT NULL,
    status      TEXT NOT NULL,
    paid_at     TIMESTAMP
);

-- Result table produced by the Airflow pipeline.
CREATE TABLE IF NOT EXISTS customer_reports (
    customer_id           INTEGER PRIMARY KEY,
    email                 TEXT NOT NULL,
    segment               TEXT,
    orders_count          INTEGER NOT NULL,
    orders_amount         NUMERIC(14, 2) NOT NULL,
    paid_amount           NUMERIC(14, 2) NOT NULL,
    last_delivery_status  TEXT,
    updated_at            TIMESTAMP NOT NULL DEFAULT now()
);
