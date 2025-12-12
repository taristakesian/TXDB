from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime

POSTGRES_CONN_ID = "ods_postgres"

DDL_SQL = r"""
-- 1. Витрина истории продаж по товару (динамика цены, динамика покупок)
CREATE TABLE IF NOT EXISTS mart_sales_history (
    id BIGSERIAL PRIMARY KEY,
    item_name TEXT,
    date_extracted DATE,
    avg_price_24h NUMERIC,
    volume_24h INTEGER,
    UNIQUE(item_name, date_extracted)
);

-- 2. Витрина по группам (AK, AWP и т.д.)
CREATE TABLE IF NOT EXISTS mart_group_analytics (
    id BIGSERIAL PRIMARY KEY,
    group_name TEXT,
    date_extracted DATE,
    avg_price NUMERIC,
    total_volume INTEGER,
    UNIQUE(group_name, date_extracted)
);

-- 3. Витрина стаканов (сравнение Skinport и BitSkins)
DROP TABLE IF EXISTS mart_order_books;
CREATE TABLE IF NOT EXISTS mart_order_books (
    id BIGSERIAL PRIMARY KEY,
    item_name TEXT,
    skinport_price NUMERIC,
    bitskins_price NUMERIC,
    price_diff NUMERIC,
    date_extracted TIMESTAMPTZ,
    UNIQUE(item_name, date_extracted)
);

-- 4. Витрина арбитража (разница цен)
CREATE TABLE IF NOT EXISTS mart_arbitrage (
    id BIGSERIAL PRIMARY KEY,
    item_name TEXT,
    buy_platform TEXT,
    sell_platform TEXT,
    profit_abs NUMERIC,
    profit_pct NUMERIC,
    date_extracted TIMESTAMPTZ,
    UNIQUE(item_name, date_extracted)
);

-- 5. Витрина волатильности (разброс цен за 7 дней)
CREATE TABLE IF NOT EXISTS mart_volatility (
    id BIGSERIAL PRIMARY KEY,
    item_name TEXT,
    spread_7d NUMERIC,
    volatility_index NUMERIC,
    volume_7d INTEGER,
    date_extracted DATE,
    UNIQUE(item_name, date_extracted)
);
"""

def _create_tables():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(DDL_SQL)

default_args = {"owner": "tigran", "retries": 0}
with DAG(
    dag_id="create_marts_tables",
    default_args=default_args,
    start_date=datetime.datetime(2025, 11, 1),
    schedule=None,
    catchup=False,
    description="Создание таблиц витрин данных (Data Marts)",
) as dag:
    PythonOperator(task_id="create_marts_tables", python_callable=_create_tables)

