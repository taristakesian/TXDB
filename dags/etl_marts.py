from __future__ import annotations
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "ods_postgres"

# SQL скрипты для наполнения витрин
ETL_SQL = r"""
-- === 1. Mart Sales History (Backfill support) ===
INSERT INTO mart_sales_history (item_name, date_extracted, avg_price_24h, volume_24h)
WITH daily_data AS (
    SELECT DISTINCT ON (p.name, h.date_extracted::date)
        p.name as item_name,
        h.date_extracted::date as date_extracted,
        h.last_24_hours_avg,
        h.last_24_hours_volume
    FROM skinport_history h
    JOIN all_product p ON h.id_name = p.id_name
    -- Remove date filter to process all history
    ORDER BY p.name, h.date_extracted::date, h.date_extracted DESC
)
SELECT * FROM daily_data
ON CONFLICT (item_name, date_extracted) 
DO UPDATE SET 
    avg_price_24h = EXCLUDED.avg_price_24h,
    volume_24h = EXCLUDED.volume_24h;

-- === 2. Mart Group Analytics (Backfill support) ===
WITH daily_history AS (
    SELECT DISTINCT ON (h.id_name, h.date_extracted::date)
        h.id_name,
        h.date_extracted::date as report_date,
        h.last_24_hours_avg,
        h.last_24_hours_volume
    FROM skinport_history h
    ORDER BY h.id_name, h.date_extracted::date, h.date_extracted DESC
),
classified AS (
    SELECT 
        p.name,
        CASE 
            WHEN p.name ILIKE '%AK-47%' THEN 'AK-47'
            WHEN p.name ILIKE '%AWP%' THEN 'AWP'
            WHEN p.name ILIKE '%M4A4%' THEN 'M4A4'
            WHEN p.name ILIKE '%M4A1-S%' THEN 'M4A1-S'
            WHEN p.name ILIKE '%Desert Eagle%' THEN 'Desert Eagle'
            WHEN p.name ILIKE '%Knife%' OR p.name ILIKE '%Karambit%' OR p.name ILIKE '%Bayonet%' OR p.name ILIKE '%Butterfly%' THEN 'Knife'
            WHEN p.name ILIKE '%Gloves%' OR p.name ILIKE '%Hand Wraps%' THEN 'Gloves'
            ELSE NULL
        END as group_name,
        h.last_24_hours_avg,
        h.last_24_hours_volume,
        h.report_date
    FROM daily_history h
    JOIN all_product p ON h.id_name = p.id_name
)
INSERT INTO mart_group_analytics (group_name, date_extracted, avg_price, total_volume)
SELECT 
    group_name,
    report_date,
    AVG(last_24_hours_avg) as avg_price,
    SUM(last_24_hours_volume) as total_volume
FROM classified
WHERE group_name IS NOT NULL
GROUP BY group_name, report_date
ON CONFLICT (group_name, date_extracted)
DO UPDATE SET
    avg_price = EXCLUDED.avg_price,
    total_volume = EXCLUDED.total_volume;

-- === 3. Mart Order Books (Stakan) ===
-- Сравниваем цены (для стаканов берем только сегодняшний срез, если история не генерировалась)
WITH skinport_latest AS (
    SELECT DISTINCT ON (id_name, date_extracted::date) *
    FROM skinport_stakan
    ORDER BY id_name, date_extracted::date, date_extracted DESC
),
bitskins_latest AS (
    SELECT DISTINCT ON (id_name, date_extracted::date) *
    FROM bitskins_stakan
    ORDER BY id_name, date_extracted::date, date_extracted DESC
)
INSERT INTO mart_order_books (item_name, skinport_price, bitskins_price, price_diff, date_extracted)
SELECT 
    p.name,
    s.suggested_price,
    b.price,
    (s.suggested_price - b.price),
    s.date_extracted
FROM skinport_latest s
JOIN all_product p ON s.id_name = p.id_name
JOIN bitskins_latest b ON s.id_name = b.id_name AND s.date_extracted::date = b.date_extracted::date
ON CONFLICT (item_name, date_extracted) DO NOTHING;

-- === 4. Mart Arbitrage ===
WITH skinport_latest AS (
    SELECT DISTINCT ON (id_name, date_extracted::date) *
    FROM skinport_stakan
    ORDER BY id_name, date_extracted::date, date_extracted DESC
),
bitskins_latest AS (
    SELECT DISTINCT ON (id_name, date_extracted::date) *
    FROM bitskins_stakan
    ORDER BY id_name, date_extracted::date, date_extracted DESC
)
INSERT INTO mart_arbitrage (item_name, buy_platform, sell_platform, profit_abs, profit_pct, date_extracted)
SELECT
    p.name,
    CASE WHEN s.suggested_price < b.price THEN 'Skinport' ELSE 'BitSkins' END as buy_platform,
    CASE WHEN s.suggested_price < b.price THEN 'BitSkins' ELSE 'Skinport' END as sell_platform,
    ABS(s.suggested_price - b.price) as profit_abs,
    CASE 
        WHEN LEAST(s.suggested_price, b.price) > 0 
        THEN ABS(s.suggested_price - b.price) / LEAST(s.suggested_price, b.price) * 100
        ELSE 0 
    END as profit_pct,
    s.date_extracted
FROM skinport_latest s
JOIN all_product p ON s.id_name = p.id_name
JOIN bitskins_latest b ON s.id_name = b.id_name AND s.date_extracted::date = b.date_extracted::date
WHERE ABS(s.suggested_price - b.price) > 0
ON CONFLICT (item_name, date_extracted) DO NOTHING;

-- === 5. Mart Volatility (Backfill support) ===
INSERT INTO mart_volatility (item_name, spread_7d, volatility_index, volume_7d, date_extracted)
WITH daily_data AS (
    SELECT DISTINCT ON (p.name, h.date_extracted::date)
        p.name as item_name,
        (h.last_7_days_max - h.last_7_days_min) as spread_7d,
        CASE 
            WHEN h.last_7_days_avg > 0 
            THEN (h.last_7_days_max - h.last_7_days_min) / h.last_7_days_avg 
            ELSE 0 
        END as volatility_index,
        h.last_7_days_volume as volume_7d,
        h.date_extracted::date as date_extracted
    FROM skinport_history h
    JOIN all_product p ON h.id_name = p.id_name
    ORDER BY p.name, h.date_extracted::date, h.date_extracted DESC
)
SELECT * FROM daily_data
ON CONFLICT (item_name, date_extracted)
DO UPDATE SET
    spread_7d = EXCLUDED.spread_7d,
    volatility_index = EXCLUDED.volatility_index,
    volume_7d = EXCLUDED.volume_7d;
"""

def _process_marts():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    # Разбиваем на команды для удобства отладки
    stmts = [s.strip() for s in ETL_SQL.split(';\n') if s.strip()]
    for i, stmt in enumerate(stmts, start=1):
        try:
            print(f"\n=== Running Mart SQL {i} ===")
            print(stmt[:200] + "...")
            hook.run(stmt)
        except Exception as e:
            print(f"Error in statement {i}: {e}")
            raise

default_args = {"owner": "tigran", "retries": 0}
with DAG(
    dag_id="etl_marts_daily",
    default_args=default_args,
    start_date=datetime.datetime(2025, 11, 1),
    schedule="@daily",
    catchup=False,
    description="Наполнение витрин данных (после ODS ETL)",
) as dag:
    PythonOperator(task_id="process_marts", python_callable=_process_marts)
