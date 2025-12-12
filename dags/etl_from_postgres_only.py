from __future__ import annotations

import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "ods_postgres"

FILTER_SQL = r"""
-- === ALL_PRODUCT: upsert по PK (id_name) ===
INSERT INTO all_product (id_name, name, item_page, market_page)
SELECT DISTINCT
  uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8',
                   COALESCE(p->>'market_hash_name', p->>'name'))::uuid AS id_name,
  COALESCE(p->>'market_hash_name', p->>'name') AS name,
  p->>'item_page'  AS item_page,
  p->>'market_page' AS market_page
FROM (
  SELECT payload AS p
  FROM skinport_items
  WHERE (created_at AT TIME ZONE 'UTC')::date = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
  UNION ALL
  SELECT payload
  FROM skinport_sales_history
  WHERE (created_at AT TIME ZONE 'UTC')::date = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
  UNION ALL
  SELECT payload
  FROM skinport_out_of_stock
  WHERE (created_at AT TIME ZONE 'UTC')::date = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
  UNION ALL
  -- BitSkins тоже даёт справочник (берём name как market_hash_name)
  SELECT payload
  FROM bitskins_market_raw
  WHERE (created_at AT TIME ZONE 'UTC')::date = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
) src
WHERE COALESCE(src.p->>'market_hash_name', src.p->>'name') IS NOT NULL
  AND COALESCE(src.p->>'market_hash_name', src.p->>'name') <> ''
ON CONFLICT (id_name) DO NOTHING;

-- === SKINPORT_STAKAN: дедуп -> UPDATE ===
WITH staged AS (
  SELECT
    uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8',
                     COALESCE(payload->>'market_hash_name', payload->>'name'))::uuid AS id_name,
    created_at::timestamptz AS date_extracted,
    NULLIF((payload->>'suggested_price'),'')::numeric AS suggested_price,
    NULLIF((payload->>'min_price'),'')::numeric      AS min_price,
    NULLIF((payload->>'max_price'),'')::numeric      AS max_price,
    NULLIF((payload->>'mean_price'),'')::numeric     AS mean_price,
    NULLIF((payload->>'median_price'),'')::numeric   AS median_price,
    created_at                                      AS src_ts
  FROM skinport_items
  WHERE (created_at AT TIME ZONE 'UTC')::date = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
),
dedup AS (
  SELECT *
  FROM (
    SELECT s.*, ROW_NUMBER() OVER (PARTITION BY id_name, date_extracted ORDER BY src_ts DESC) rn
    FROM staged s
    WHERE s.id_name IS NOT NULL
  ) z
  WHERE rn = 1
)
UPDATE skinport_stakan t
SET suggested_price = d.suggested_price,
    min_price       = d.min_price,
    max_price       = d.max_price,
    mean_price      = d.mean_price,
    median_price    = d.median_price
FROM dedup d
WHERE t.id_name = d.id_name AND t.date_extracted = d.date_extracted;

-- === SKINPORT_STAKAN: дедуп -> INSERT ===
WITH staged AS (
  SELECT
    uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8',
                     COALESCE(payload->>'market_hash_name', payload->>'name'))::uuid AS id_name,
    created_at::timestamptz AS date_extracted,
    NULLIF((payload->>'suggested_price'),'')::numeric AS suggested_price,
    NULLIF((payload->>'min_price'),'')::numeric      AS min_price,
    NULLIF((payload->>'max_price'),'')::numeric      AS max_price,
    NULLIF((payload->>'mean_price'),'')::numeric     AS mean_price,
    NULLIF((payload->>'median_price'),'')::numeric   AS median_price,
    created_at                                      AS src_ts
  FROM skinport_items
  WHERE (created_at AT TIME ZONE 'UTC')::date = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
),
dedup AS (
  SELECT *
  FROM (
    SELECT s.*, ROW_NUMBER() OVER (PARTITION BY id_name, date_extracted ORDER BY src_ts DESC) rn
    FROM staged s
    WHERE s.id_name IS NOT NULL
  ) z
  WHERE rn = 1
)
INSERT INTO skinport_stakan (
  id_name, suggested_price, date_extracted, min_price, max_price, mean_price, median_price
)
SELECT d.id_name, d.suggested_price, d.date_extracted, d.min_price, d.max_price, d.mean_price, d.median_price
FROM dedup d
WHERE NOT EXISTS (
  SELECT 1 FROM skinport_stakan t
  WHERE t.id_name = d.id_name AND t.date_extracted = d.date_extracted
);

-- === SKINPORT_HISTORY: дедуп -> UPDATE ===
WITH staged AS (
  SELECT
    uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8',
                     COALESCE(payload->>'market_hash_name', payload->>'name'))::uuid AS id_name,
    NULLIF((payload->'last_24_hours'->>'min'),'')::numeric    AS last_24_hours_min,
    NULLIF((payload->'last_24_hours'->>'max'),'')::numeric    AS last_24_hours_max,
    NULLIF((payload->'last_24_hours'->>'avg'),'')::numeric    AS last_24_hours_avg,
    NULLIF((payload->'last_24_hours'->>'median'),'')::numeric AS last_24_hours_median,
    NULLIF((payload->'last_24_hours'->>'volume'),'')::int     AS last_24_hours_volume,
    NULLIF((payload->'last_7_days'->>'min'),'')::numeric      AS last_7_days_min,
    NULLIF((payload->'last_7_days'->>'max'),'')::numeric      AS last_7_days_max,
    NULLIF((payload->'last_7_days'->>'avg'),'')::numeric      AS last_7_days_avg,
    NULLIF((payload->'last_7_days'->>'median'),'')::numeric   AS last_7_days_median,
    NULLIF((payload->'last_7_days'->>'volume'),'')::int       AS last_7_days_volume,
    NULLIF((payload->'last_30_days'->>'min'),'')::numeric     AS last_30_days_min,
    NULLIF((payload->'last_30_days'->>'max'),'')::numeric     AS last_30_days_max,
    NULLIF((payload->'last_30_days'->>'avg'),'')::numeric     AS last_30_days_avg,
    NULLIF((payload->'last_30_days'->>'median'),'')::numeric  AS last_30_days_median,
    NULLIF((payload->'last_30_days'->>'volume'),'')::int      AS last_30_days_volume,
    NULLIF((payload->'last_90_days'->>'min'),'')::numeric     AS last_90_days_min,
    NULLIF((payload->'last_90_days'->>'max'),'')::numeric     AS last_90_days_max,
    NULLIF((payload->'last_90_days'->>'avg'),'')::numeric     AS last_90_days_avg,
    NULLIF((payload->'last_90_days'->>'median'),'')::numeric  AS last_90_days_median,
    NULLIF((payload->'last_90_days'->>'volume'),'')::int      AS last_90_days_volume,
    created_at::timestamptz                                   AS date_extracted,
    created_at                                                AS src_ts
  FROM skinport_sales_history
  WHERE (created_at AT TIME ZONE 'UTC')::date = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
),
dedup AS (
  SELECT *
  FROM (
    SELECT s.*, ROW_NUMBER() OVER (PARTITION BY id_name, date_extracted ORDER BY src_ts DESC) rn
    FROM staged s
    WHERE s.id_name IS NOT NULL
  ) z
  WHERE rn = 1
)
UPDATE skinport_history t
SET last_24_hours_min    = d.last_24_hours_min,
    last_24_hours_max    = d.last_24_hours_max,
    last_24_hours_avg    = d.last_24_hours_avg,
    last_24_hours_median = d.last_24_hours_median,
    last_24_hours_volume = d.last_24_hours_volume,
    last_7_days_min      = d.last_7_days_min,
    last_7_days_max      = d.last_7_days_max,
    last_7_days_avg      = d.last_7_days_avg,
    last_7_days_median   = d.last_7_days_median,
    last_7_days_volume   = d.last_7_days_volume,
    last_30_days_min     = d.last_30_days_min,
    last_30_days_max     = d.last_30_days_max,
    last_30_days_avg     = d.last_30_days_avg,
    last_30_days_median  = d.last_30_days_median,
    last_30_days_volume  = d.last_30_days_volume,
    last_90_days_min     = d.last_90_days_min,
    last_90_days_max     = d.last_90_days_max,
    last_90_days_avg     = d.last_90_days_avg,
    last_90_days_median  = d.last_90_days_median,
    last_90_days_volume  = d.last_90_days_volume
FROM dedup d
WHERE t.id_name = d.id_name AND t.date_extracted = d.date_extracted;

-- === SKINPORT_HISTORY: дедуп -> INSERT ===
WITH staged AS (
  SELECT
    uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8',
                     COALESCE(payload->>'market_hash_name', payload->>'name'))::uuid AS id_name,
    NULLIF((payload->'last_24_hours'->>'min'),'')::numeric    AS last_24_hours_min,
    NULLIF((payload->'last_24_hours'->>'max'),'')::numeric    AS last_24_hours_max,
    NULLIF((payload->'last_24_hours'->>'avg'),'')::numeric    AS last_24_hours_avg,
    NULLIF((payload->'last_24_hours'->>'median'),'')::numeric AS last_24_hours_median,
    NULLIF((payload->'last_24_hours'->>'volume'),'')::int     AS last_24_hours_volume,
    NULLIF((payload->'last_7_days'->>'min'),'')::numeric      AS last_7_days_min,
    NULLIF((payload->'last_7_days'->>'max'),'')::numeric      AS last_7_days_max,
    NULLIF((payload->'last_7_days'->>'avg'),'')::numeric      AS last_7_days_avg,
    NULLIF((payload->'last_7_days'->>'median'),'')::numeric   AS last_7_days_median,
    NULLIF((payload->'last_7_days'->>'volume'),'')::int       AS last_7_days_volume,
    NULLIF((payload->'last_30_days'->>'min'),'')::numeric     AS last_30_days_min,
    NULLIF((payload->'last_30_days'->>'max'),'')::numeric     AS last_30_days_max,
    NULLIF((payload->'last_30_days'->>'avg'),'')::numeric     AS last_30_days_avg,
    NULLIF((payload->'last_30_days'->>'median'),'')::numeric  AS last_30_days_median,
    NULLIF((payload->'last_30_days'->>'volume'),'')::int      AS last_30_days_volume,
    NULLIF((payload->'last_90_days'->>'min'),'')::numeric     AS last_90_days_min,
    NULLIF((payload->'last_90_days'->>'max'),'')::numeric     AS last_90_days_max,
    NULLIF((payload->'last_90_days'->>'avg'),'')::numeric     AS last_90_days_avg,
    NULLIF((payload->'last_90_days'->>'median'),'')::numeric  AS last_90_days_median,
    NULLIF((payload->'last_90_days'->>'volume'),'')::int      AS last_90_days_volume,
    created_at::timestamptz                                   AS date_extracted,
    created_at                                                AS src_ts
  FROM skinport_sales_history
  WHERE (created_at AT TIME ZONE 'UTC')::date = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
),
dedup AS (
  SELECT *
  FROM (
    SELECT s.*, ROW_NUMBER() OVER (PARTITION BY id_name, date_extracted ORDER BY src_ts DESC) rn
    FROM staged s
    WHERE s.id_name IS NOT NULL
  ) z
  WHERE rn = 1
)
INSERT INTO skinport_history (
  id_name,
  last_24_hours_min, last_24_hours_max, last_24_hours_avg, last_24_hours_median, last_24_hours_volume,
  last_7_days_min, last_7_days_max, last_7_days_avg, last_7_days_median, last_7_days_volume,
  last_30_days_min, last_30_days_max, last_30_days_avg, last_30_days_median, last_30_days_volume,
  last_90_days_min, last_90_days_max, last_90_days_avg, last_90_days_median, last_90_days_volume,
  date_extracted
)
SELECT
  d.id_name,
  d.last_24_hours_min, d.last_24_hours_max, d.last_24_hours_avg, d.last_24_hours_median, d.last_24_hours_volume,
  d.last_7_days_min,   d.last_7_days_max,   d.last_7_days_avg,   d.last_7_days_median,   d.last_7_days_volume,
  d.last_30_days_min,  d.last_30_days_max,  d.last_30_days_avg,  d.last_30_days_median,  d.last_30_days_volume,
  d.last_90_days_min,  d.last_90_days_max,  d.last_90_days_avg,  d.last_90_days_median,  d.last_90_days_volume,
  d.date_extracted
FROM dedup d
WHERE NOT EXISTS (
  SELECT 1 FROM skinport_history t
  WHERE t.id_name = d.id_name AND t.date_extracted = d.date_extracted
);

-- === BITSKINS_STAKAN: дедуп -> UPDATE ===
WITH staged AS (
  SELECT
    COALESCE(payload->>'market_hash_name', payload->>'name') AS canon_name,
    uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8',
                     COALESCE(payload->>'market_hash_name', payload->>'name'))::uuid AS id_name,
    NULLIF((payload->>'created_at'),'')::timestamptz AS date_extracted,
    NULLIF((payload->>'price'),'')::numeric          AS price,
    created_at                                       AS src_ts
  FROM bitskins_market_raw
  WHERE (created_at AT TIME ZONE 'UTC')::date = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
),
dedup AS (
  SELECT *
  FROM (
    SELECT s.*, ROW_NUMBER() OVER (PARTITION BY id_name, date_extracted ORDER BY src_ts DESC) rn
    FROM staged s
    WHERE s.canon_name IS NOT NULL AND s.canon_name <> '' AND s.id_name IS NOT NULL
  ) z
  WHERE rn = 1
)
UPDATE bitskins_stakan t
SET price = d.price
FROM dedup d
WHERE t.id_name = d.id_name AND t.date_extracted = d.date_extracted;

-- === BITSKINS_STAKAN: дедуп -> INSERT ===
TRUNCATE TABLE bitskins_stakan;

WITH staged AS (
  SELECT
    COALESCE(payload->>'market_hash_name', payload->>'name') AS canon_name,
    uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8',
                     COALESCE(payload->>'market_hash_name', payload->>'name'))::uuid AS id_name,
    NULLIF((payload->>'created_at'),'')::timestamptz AS date_extracted,
    NULLIF((payload->>'price'),'')::numeric          AS price,
    created_at                                       AS src_ts
  FROM bitskins_market_raw
  WHERE (created_at AT TIME ZONE 'UTC')::date = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
),
dedup AS (
  SELECT *
  FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY id_name, date_extracted ORDER BY src_ts DESC) AS rn
    FROM staged s
    WHERE s.canon_name IS NOT NULL AND s.canon_name <> '' AND s.id_name IS NOT NULL
          AND s.date_extracted IS NOT NULL
  ) z
  WHERE rn = 1
)
INSERT INTO bitskins_stakan (id_name, date_extracted, price)
SELECT id_name, date_extracted, price
FROM dedup;
"""

def _process_today():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    stmts = [s.strip() for s in FILTER_SQL.split(';\n') if s.strip()]
    for i, stmt in enumerate(stmts, start=1):
        try:
            print(f"\n=== RUN {i}/{len(stmts)} ===")
            print(stmt[:800] + ("..." if len(stmt) > 800 else ""))
            hook.run(stmt)
        except Exception as e:
            print("\n!!! FAILED statement:", i)
            print(stmt)
            print("Error:", repr(e))
            raise


default_args = {"owner": "tigran", "retries": 0}

with DAG(
    dag_id="etl_from_postgres_only",
    default_args=default_args,
    start_date=datetime.datetime(2025, 11, 1),
    schedule=None,
    catchup=False,
    description="Берем ТОЛЬКО из staging Postgres (JSONB) и наполняем финальные таблицы (UTC-day)",
) as dag:
    PythonOperator(task_id="process_today", python_callable=_process_today)
