from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime

POSTGRES_CONN_ID = "ods_postgres"

FILTER_SQL = r"""
-- === ALL_PRODUCT: можно оставить upsert (тут PK = id_name) ===
INSERT INTO all_product (id_name, name, item_page, market_page)
SELECT DISTINCT
  uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', (p->>'market_hash_name'))::uuid AS id_name,
  p->>'market_hash_name' AS name,
  p->>'item_page'  AS item_page,
  p->>'market_page' AS market_page
FROM (
  SELECT payload AS p
  FROM skinport_items
  WHERE created_at::date = ((now() AT TIME ZONE %(tz)s)::date)
  UNION ALL
  SELECT payload
  FROM skinport_sales_history
  WHERE created_at::date = ((now() AT TIME ZONE %(tz)s)::date)
  UNION ALL
  SELECT payload
  FROM skinport_out_of_stock
  WHERE created_at::date = ((now() AT TIME ZONE %(tz)s)::date)
) src
ON CONFLICT (id_name) DO NOTHING;

-- === SKINPORT_STAKAN: дедуп -> UPDATE ===
WITH staged AS (
  SELECT
    uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', (payload->>'market_hash_name'))::uuid AS id_name,
    created_at::timestamptz                                                               AS date_extracted,
    NULLIF((payload->>'suggested_price'),'')::numeric                                      AS suggested_price,
    NULLIF((payload->>'min_price'),'')::numeric                                           AS min_price,
    NULLIF((payload->>'max_price'),'')::numeric                                           AS max_price,
    NULLIF((payload->>'mean_price'),'')::numeric                                          AS mean_price,
    NULLIF((payload->>'median_price'),'')::numeric                                        AS median_price,
    created_at                                                                             AS src_ts
  FROM skinport_items
  WHERE created_at::date = ((now() AT TIME ZONE %(tz)s)::date)
),
dedup AS (
  SELECT *
  FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY id_name, date_extracted ORDER BY src_ts DESC) AS rn
    FROM staged s
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

-- === SKINPORT_STAKAN: дедуп -> INSERT новых ===
WITH staged AS (
  SELECT
    uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', (payload->>'market_hash_name'))::uuid AS id_name,
    created_at::timestamptz                                                               AS date_extracted,
    NULLIF((payload->>'suggested_price'),'')::numeric                                      AS suggested_price,
    NULLIF((payload->>'min_price'),'')::numeric                                           AS min_price,
    NULLIF((payload->>'max_price'),'')::numeric                                           AS max_price,
    NULLIF((payload->>'mean_price'),'')::numeric                                          AS mean_price,
    NULLIF((payload->>'median_price'),'')::numeric                                        AS median_price,
    created_at                                                                             AS src_ts
  FROM skinport_items
  WHERE created_at::date = ((now() AT TIME ZONE %(tz)s)::date)
),
dedup AS (
  SELECT *
  FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY id_name, date_extracted ORDER BY src_ts DESC) AS rn
    FROM staged s
  ) z
  WHERE rn = 1
)
INSERT INTO skinport_stakan (
  id_name, suggested_price, date_extracted, min_price, max_price, mean_price, median_price
)
SELECT
  d.id_name, d.suggested_price, d.date_extracted, d.min_price, d.max_price, d.mean_price, d.median_price
FROM dedup d
WHERE NOT EXISTS (
  SELECT 1 FROM skinport_stakan t
  WHERE t.id_name = d.id_name AND t.date_extracted = d.date_extracted
);

-- === SKINPORT_HISTORY: дедуп -> UPDATE ===
WITH staged AS (
  SELECT
    uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', (payload->>'market_hash_name'))::uuid AS id_name,
    NULLIF((payload->'last_24_hours'->>'min'),'')::numeric     AS last_24_hours_min,
    NULLIF((payload->'last_24_hours'->>'max'),'')::numeric     AS last_24_hours_max,
    NULLIF((payload->'last_24_hours'->>'avg'),'')::numeric     AS last_24_hours_avg,
    NULLIF((payload->'last_24_hours'->>'median'),'')::numeric  AS last_24_hours_median,
    NULLIF((payload->'last_24_hours'->>'volume'),'')::int      AS last_24_hours_volume,
    NULLIF((payload->'last_7_days'->>'min'),'')::numeric       AS last_7_days_min,
    NULLIF((payload->'last_7_days'->>'max'),'')::numeric       AS last_7_days_max,
    NULLIF((payload->'last_7_days'->>'avg'),'')::numeric       AS last_7_days_avg,
    NULLIF((payload->'last_7_days'->>'median'),'')::numeric    AS last_7_days_median,
    NULLIF((payload->'last_7_days'->>'volume'),'')::int        AS last_7_days_volume,
    NULLIF((payload->'last_30_days'->>'min'),'')::numeric      AS last_30_days_min,
    NULLIF((payload->'last_30_days'->>'max'),'')::numeric      AS last_30_days_max,
    NULLIF((payload->'last_30_days'->>'avg'),'')::numeric      AS last_30_days_avg,
    NULLIF((payload->'last_30_days'->>'median'),'')::numeric   AS last_30_days_median,
    NULLIF((payload->'last_30_days'->>'volume'),'')::int       AS last_30_days_volume,
    NULLIF((payload->'last_90_days'->>'min'),'')::numeric      AS last_90_days_min,
    NULLIF((payload->'last_90_days'->>'max'),'')::numeric      AS last_90_days_max,
    NULLIF((payload->'last_90_days'->>'avg'),'')::numeric      AS last_90_days_avg,
    NULLIF((payload->'last_90_days'->>'median'),'')::numeric   AS last_90_days_median,
    NULLIF((payload->'last_90_days'->>'volume'),'')::int       AS last_90_days_volume,
    created_at::timestamptz                                    AS date_extracted,
    created_at                                                 AS src_ts
  FROM skinport_sales_history
  WHERE created_at::date = ((now() AT TIME ZONE %(tz)s)::date)
),
dedup AS (
  SELECT *
  FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY id_name, date_extracted ORDER BY src_ts DESC) AS rn
    FROM staged s
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
    uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', (payload->>'market_hash_name'))::uuid AS id_name,
    NULLIF((payload->'last_24_hours'->>'min'),'')::numeric     AS last_24_hours_min,
    NULLIF((payload->'last_24_hours'->>'max'),'')::numeric     AS last_24_hours_max,
    NULLIF((payload->'last_24_hours'->>'avg'),'')::numeric     AS last_24_hours_avg,
    NULLIF((payload->'last_24_hours'->>'median'),'')::numeric  AS last_24_hours_median,
    NULLIF((payload->'last_24_hours'->>'volume'),'')::int      AS last_24_hours_volume,
    NULLIF((payload->'last_7_days'->>'min'),'')::numeric       AS last_7_days_min,
    NULLIF((payload->'last_7_days'->>'max'),'')::numeric       AS last_7_days_max,
    NULLIF((payload->'last_7_days'->>'avg'),'')::numeric       AS last_7_days_avg,
    NULLIF((payload->'last_7_days'->>'median'),'')::numeric    AS last_7_days_median,
    NULLIF((payload->'last_7_days'->>'volume'),'')::int        AS last_7_days_volume,
    NULLIF((payload->'last_30_days'->>'min'),'')::numeric      AS last_30_days_min,
    NULLIF((payload->'last_30_days'->>'max'),'')::numeric      AS last_30_days_max,
    NULLIF((payload->'last_30_days'->>'avg'),'')::numeric      AS last_30_days_avg,
    NULLIF((payload->'last_30_days'->>'median'),'')::numeric   AS last_30_days_median,
    NULLIF((payload->'last_30_days'->>'volume'),'')::int       AS last_30_days_volume,
    NULLIF((payload->'last_90_days'->>'min'),'')::numeric      AS last_90_days_min,
    NULLIF((payload->'last_90_days'->>'max'),'')::numeric      AS last_90_days_max,
    NULLIF((payload->'last_90_days'->>'avg'),'')::numeric      AS last_90_days_avg,
    NULLIF((payload->'last_90_days'->>'median'),'')::numeric   AS last_90_days_median,
    NULLIF((payload->'last_90_days'->>'volume'),'')::int       AS last_90_days_volume,
    created_at::timestamptz                                    AS date_extracted,
    created_at                                                 AS src_ts
  FROM skinport_sales_history
  WHERE created_at::date = ((now() AT TIME ZONE %(tz)s)::date)
),
dedup AS (
  SELECT *
  FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY id_name, date_extracted ORDER BY src_ts DESC) AS rn
    FROM staged s
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
    uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', (payload->>'market_hash_name'))::uuid AS id_name,
    NULLIF((payload->>'created_at'),'')::timestamptz AS date_extracted,
    NULLIF((payload->>'price'),'')::numeric          AS price,
    created_at                                       AS src_ts
  FROM bitskins_market_raw
  WHERE created_at::date = ((now() AT TIME ZONE %(tz)s)::date)
),
dedup AS (
  SELECT *
  FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY id_name, date_extracted ORDER BY src_ts DESC) AS rn
    FROM staged s
  ) z
  WHERE rn = 1
)
UPDATE bitskins_stakan t
SET price = d.price
FROM dedup d
WHERE t.id_name = d.id_name AND t.date_extracted = d.date_extracted;

-- === BITSKINS_STAKAN: дедуп -> INSERT ===
WITH staged AS (
  SELECT
    uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', (payload->>'market_hash_name'))::uuid AS id_name,
    NULLIF((payload->>'created_at'),'')::timestamptz AS date_extracted,
    NULLIF((payload->>'price'),'')::numeric          AS price,
    created_at                                       AS src_ts
  FROM bitskins_market_raw
  WHERE created_at::date = ((now() AT TIME ZONE %(tz)s)::date)
),
dedup AS (
  SELECT *
  FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY id_name, date_extracted ORDER BY src_ts DESC) AS rn
    FROM staged s
  ) z
  WHERE rn = 1
)
INSERT INTO bitskins_stakan (id_name, date_extracted, price)
SELECT d.id_name, d.date_extracted, d.price
FROM dedup d
WHERE NOT EXISTS (
  SELECT 1 FROM bitskins_stakan t
  WHERE t.id_name = d.id_name AND t.date_extracted = d.date_extracted
);
"""



def _process_today(tz: str = "Europe/Stockholm"):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(FILTER_SQL, parameters={"tz": tz})

default_args = {"owner": "tigran", "retries": 0}

with DAG(
    dag_id="etl_from_postgres_only",
    default_args=default_args,
    start_date=datetime.datetime(2025, 11, 1),
    schedule=None,      # запускается триггером с дага из п.1
    catchup=False,
    description="Берем ТОЛЬКО из staging Postgres (JSONB) и наполняем финальные таблицы",
) as dag:
    PythonOperator(task_id="process_today", python_callable=_process_today, op_kwargs={"tz": "Europe/Stockholm"})
