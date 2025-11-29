from __future__ import annotations
import datetime
from typing import List, Dict

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "ods_postgres"
NAMESPACE_DNS = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"  # для uuid_generate_v5

default_args = {"owner": "tigran", "retries": 0}

def _db_checks(**context) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Универсальный раннер: возвращает список dict-строк
    def run(sql: str, params: dict | None = None) -> List[Dict]:
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or {})
                if cur.description is None:
                    return []
                colnames = [d.name if hasattr(d, "name") else d[0] for d in cur.description]
                rows = cur.fetchall()
                return [dict(zip(colnames, r)) for r in rows]

    problems: List[tuple[str, List[Dict]]] = []

    # расширение (молчаливо)
    hook.run('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')

    # -------- all_product --------
    rows = run("""
        SELECT id_name, name
        FROM all_product
        WHERE id_name IS NULL OR name IS NULL
        LIMIT 50;
    """)
    if rows:
        problems.append(("all_product: nulls in key fields", rows))

    rows = run("""
        SELECT id_name, name
        FROM all_product
        WHERE id_name IS DISTINCT FROM uuid_generate_v5(%(ns)s::uuid, name)
        LIMIT 50;
    """, {"ns": NAMESPACE_DNS})
    if rows:
        problems.append(("all_product: id_name != uuid_v5(name)", rows))

    rows = run("""
        SELECT name, COUNT(DISTINCT id_name) AS cnt
        FROM all_product
        GROUP BY name
        HAVING COUNT(DISTINCT id_name) > 1
        ORDER BY cnt DESC, name
        LIMIT 50;
    """)
    if rows:
        problems.append(("all_product: one name maps to >1 id_name", rows))

    # -------- skinport_stakan --------
    rows = run("""
        SELECT id_name, date_extracted, COUNT(*) AS cnt
        FROM skinport_stakan
        GROUP BY id_name, date_extracted
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC, date_extracted DESC
        LIMIT 50;
    """)
    if rows:
        problems.append(("skinport_stakan: duplicates (id_name, date_extracted)", rows))

    rows = run("""
        SELECT s.id, s.id_name, s.date_extracted
        FROM skinport_stakan s
        LEFT JOIN all_product p ON p.id_name = s.id_name
        WHERE p.id_name IS NULL
        LIMIT 50;
    """)
    if rows:
        problems.append(("skinport_stakan: orphan fk to all_product", rows))

    rows = run("""
        SELECT id, id_name, date_extracted
        FROM skinport_stakan
        WHERE id_name IS NULL OR date_extracted IS NULL
        LIMIT 50;
    """)
    if rows:
        problems.append(("skinport_stakan: nulls in key fields", rows))

    rows = run("""
        SELECT id, id_name, date_extracted, min_price, max_price, mean_price, median_price, suggested_price
        FROM skinport_stakan
        WHERE (min_price  < 0)
           OR (max_price  < 0)
           OR (mean_price < 0)
           OR (median_price < 0)
           OR (suggested_price < 0)
           OR (min_price IS NOT NULL AND max_price IS NOT NULL AND min_price > max_price)
        LIMIT 50;
    """)
    if rows:
        problems.append(("skinport_stakan: invalid price values", rows))

    rows = run("""
        SELECT id, id_name, date_extracted
        FROM skinport_stakan
        WHERE date_extracted > now() + interval '5 minutes'
        LIMIT 50;
    """)
    if rows:
        problems.append(("skinport_stakan: future dates", rows))

    # -------- skinport_history --------
    rows = run("""
        SELECT id_name, date_extracted, COUNT(*) AS cnt
        FROM skinport_history
        GROUP BY id_name, date_extracted
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC, date_extracted DESC
        LIMIT 50;
    """)
    if rows:
        problems.append(("skinport_history: duplicates (id_name, date_extracted)", rows))

    rows = run("""
        SELECT h.id, h.id_name, h.date_extracted
        FROM skinport_history h
        LEFT JOIN all_product p ON p.id_name = h.id_name
        WHERE p.id_name IS NULL
        LIMIT 50;
    """)
    if rows:
        problems.append(("skinport_history: orphan fk to all_product", rows))

    rows = run("""
        SELECT id, id_name, date_extracted
        FROM skinport_history
        WHERE date_extracted > now() + interval '5 minutes'
        LIMIT 50;
    """)
    if rows:
        problems.append(("skinport_history: future dates", rows))

    rows = run("""
        SELECT id, id_name, date_extracted,
               last_24_hours_min AS min, last_24_hours_median AS median, last_24_hours_max AS max, last_24_hours_avg AS avg
        FROM skinport_history
        WHERE
          (last_24_hours_min    IS NOT NULL AND last_24_hours_max IS NOT NULL AND last_24_hours_min > last_24_hours_max)
       OR (last_24_hours_median IS NOT NULL AND last_24_hours_min IS NOT NULL AND last_24_hours_median < last_24_hours_min)
       OR (last_24_hours_median IS NOT NULL AND last_24_hours_max IS NOT NULL AND last_24_hours_median > last_24_hours_max)
       OR (last_24_hours_avg    IS NOT NULL AND last_24_hours_min IS NOT NULL AND last_24_hours_avg < last_24_hours_min)
       OR (last_24_hours_avg    IS NOT NULL AND last_24_hours_max IS NOT NULL AND last_24_hours_avg > last_24_hours_max)
        LIMIT 50;
    """)
    if rows:
        problems.append(("skinport_history: invariants broken for last_24_hours", rows))

    for horizon in ("last_7_days", "last_30_days", "last_90_days"):
        rows = run(f"""
            SELECT id, id_name, date_extracted,
                   {horizon}_min AS min, {horizon}_median AS median, {horizon}_max AS max, {horizon}_avg AS avg
            FROM skinport_history
            WHERE
              ({horizon}_min    IS NOT NULL AND {horizon}_max IS NOT NULL AND {horizon}_min > {horizon}_max)
           OR ({horizon}_median IS NOT NULL AND {horizon}_min IS NOT NULL AND {horizon}_median < {horizon}_min)
           OR ({horizon}_median IS NOT NULL AND {horizon}_max IS NOT NULL AND {horizon}_median > {horizon}_max)
           OR ({horizon}_avg    IS NOT NULL AND {horizon}_min IS NOT NULL AND {horizon}_avg < {horizon}_min)
           OR ({horizon}_avg    IS NOT NULL AND {horizon}_max IS NOT NULL AND {horizon}_avg > {horizon}_max)
            LIMIT 50;
        """)
        if rows:
            problems.append((f"skinport_history: invariants broken for {horizon}", rows))

    rows = run("""
        SELECT id, id_name, date_extracted,
               last_24_hours_volume, last_7_days_volume, last_30_days_volume, last_90_days_volume
        FROM skinport_history
        WHERE COALESCE(last_24_hours_volume,0) < 0
           OR COALESCE(last_7_days_volume,0)  < 0
           OR COALESCE(last_30_days_volume,0) < 0
           OR COALESCE(last_90_days_volume,0) < 0
        LIMIT 50;
    """)
    if rows:
        problems.append(("skinport_history: negative volumes", rows))

    # -------- bitskins_stakan --------
    rows = run("""
        SELECT id_name, date_extracted, COUNT(*) AS cnt
        FROM bitskins_stakan
        GROUP BY id_name, date_extracted
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC, date_extracted DESC
        LIMIT 50;
    """)
    if rows:
        problems.append(("bitskins_stakan: duplicates (id_name, date_extracted)", rows))

    rows = run("""
        SELECT b.id, b.id_name, b.date_extracted
        FROM bitskins_stakan b
        LEFT JOIN all_product p ON p.id_name = b.id_name
        WHERE p.id_name IS NULL
        LIMIT 50;
    """)
    if rows:
        problems.append(("bitskins_stakan: orphan fk to all_product", rows))

    rows = run("""
        SELECT id, id_name, date_extracted, price
        FROM bitskins_stakan
        WHERE price < 0
        LIMIT 50;
    """)
    if rows:
        problems.append(("bitskins_stakan: negative price", rows))

    rows = run("""
        SELECT id, id_name, date_extracted
        FROM bitskins_stakan
        WHERE date_extracted > now() + interval '5 minutes'
        LIMIT 50;
    """)
    if rows:
        problems.append(("bitskins_stakan: future dates", rows))

    # -------- summaries в лог --------
    log = context["ti"].log
    for name, sql in [
        ("all_product", "SELECT COUNT(*) AS rows_total FROM all_product;"),
        ("skinport_stakan", """
            SELECT COUNT(*) AS rows_total,
                   SUM(CASE WHEN min_price IS NULL THEN 1 ELSE 0 END)   AS null_min,
                   SUM(CASE WHEN max_price IS NULL THEN 1 ELSE 0 END)   AS null_max,
                   SUM(CASE WHEN mean_price IS NULL THEN 1 ELSE 0 END)  AS null_mean,
                   SUM(CASE WHEN median_price IS NULL THEN 1 ELSE 0 END)AS null_median
            FROM skinport_stakan;
        """),
        ("skinport_history", """
            SELECT COUNT(*) AS rows_total,
                   SUM(CASE WHEN last_24_hours_avg IS NULL THEN 1 ELSE 0 END) AS null_24h_avg,
                   SUM(CASE WHEN last_7_days_avg   IS NULL THEN 1 ELSE 0 END) AS null_7d_avg,
                   SUM(CASE WHEN last_30_days_avg  IS NULL THEN 1 ELSE 0 END) AS null_30d_avg,
                   SUM(CASE WHEN last_90_days_avg  IS NULL THEN 1 ELSE 0 END) AS null_90d_avg
            FROM skinport_history;
        """),
        ("bitskins_stakan", """
            SELECT COUNT(*) AS rows_total,
                   SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS null_price
            FROM bitskins_stakan;
        """),
    ]:
        vals = run(sql)
        log.info("SUMMARY %s: %s", name, (vals[0] if vals else {}))

    if problems:
        for title, rows in problems:
            log.error("DB CHECK FAIL: %s", title)
            for r in rows:
                log.error("  %s", r)
        raise AirflowException(f"DB checks failed: {len(problems)} problem groups")
    else:
        log.info("DB CHECKS PASS: no problems found.")

with DAG(
    dag_id="db_checks_ods",
    default_args=default_args,
    start_date=datetime.datetime(2025, 11, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    description="Проверки качества данных в ODS (дубли/NULL/валидность/ссылки)",
) as dag:
    PythonOperator(
        task_id="run_db_checks",
        python_callable=_db_checks,
    )
