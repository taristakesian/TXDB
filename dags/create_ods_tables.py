from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime

POSTGRES_CONN_ID = "ods_postgres"

DDL_SQL = r"""
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- финальный справочник продуктов
CREATE TABLE IF NOT EXISTS all_product (
    id_name UUID PRIMARY KEY,
    name TEXT NOT NULL,
    item_page TEXT,
    market_page TEXT
);

-- финальная витрина "стакан" с Skinport items
CREATE TABLE IF NOT EXISTS skinport_stakan (
    id BIGSERIAL PRIMARY KEY,
    id_name UUID NOT NULL REFERENCES all_product(id_name),
    suggested_price NUMERIC,
    date_extracted TIMESTAMPTZ NOT NULL DEFAULT now(),
    min_price NUMERIC,
    max_price NUMERIC,
    mean_price NUMERIC,
    median_price NUMERIC
);

-- финальная витрина history (развернутые агрегаты)
CREATE TABLE IF NOT EXISTS skinport_history (
    id BIGSERIAL PRIMARY KEY,
    id_name UUID NOT NULL REFERENCES all_product(id_name),
    last_24_hours_min NUMERIC,
    last_24_hours_max NUMERIC,
    last_24_hours_avg NUMERIC,
    last_24_hours_median NUMERIC,
    last_24_hours_volume INTEGER,
    last_7_days_min NUMERIC,
    last_7_days_max NUMERIC,
    last_7_days_avg NUMERIC,
    last_7_days_median NUMERIC,
    last_7_days_volume INTEGER,
    last_30_days_min NUMERIC,
    last_30_days_max NUMERIC,
    last_30_days_avg NUMERIC,
    last_30_days_median NUMERIC,
    last_30_days_volume INTEGER,
    last_90_days_min NUMERIC,
    last_90_days_max NUMERIC,
    last_90_days_avg NUMERIC,
    last_90_days_median NUMERIC,
    last_90_days_volume INTEGER,
    date_extracted TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- финальная витрина BitSkins
CREATE TABLE IF NOT EXISTS bitskins_stakan (
    id BIGSERIAL PRIMARY KEY,
    id_name UUID NOT NULL REFERENCES all_product(id_name),
    date_extracted TIMESTAMPTZ,
    price NUMERIC
);
"""

def _create_tables():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(DDL_SQL)

default_args = {"owner": "tigran", "retries": 0}
with DAG(
    dag_id="create_ods_tables",
    default_args=default_args,
    start_date=datetime.datetime(2025, 11, 1),
    schedule=None,
    catchup=False,
    description="Создание финальных таблиц в ODS Postgres",
) as dag:
    PythonOperator(task_id="create_tables", python_callable=_create_tables)
