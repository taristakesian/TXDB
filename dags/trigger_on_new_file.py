from __future__ import annotations
import os
import pendulum
import datetime
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

WATCH_DIR = "/opt/airflow/data"           # где лежат файлы
TARGET_DAG_ID = "etl_from_postgres_only"  # какой DAG триггерим

default_args = {"owner": "tigran", "retries": 0}

def make_exists_callable(table_name: str, conn_id: str = "ods_postgres"):
    """
    Вернёт python_callable для PythonSensor.
    True, когда в table_name есть строки, у которых дата(created_at) по UTC = сегодняшнему дню (UTC).
    """
    def _exists(**context) -> bool:
        hook = PostgresHook(postgres_conn_id=conn_id)
        sql = f"""
        SELECT COUNT(*) AS c
        FROM {table_name}
        WHERE (created_at AT TIME ZONE 'UTC')::date = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
        """
        row = hook.get_first(sql)
        cnt = int(row[0]) if row else 0
        print(f"[{table_name}] UTC rows today: {cnt}")
        return cnt > 0
    return _exists

with DAG(
    dag_id="trigger_on_new_file",
    default_args=default_args,
    start_date=datetime.datetime(2025, 11, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    description="Триггерит обработку, когда появляются файлы за сегодня (items, sales)",
) as dag:

    # items
    wait_items_today = PythonSensor(
        task_id="wait_items_today",
        python_callable=make_exists_callable("skinport_items"),
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )
    trigger_items = TriggerDagRunOperator(
        task_id="trigger_items_processing",
        trigger_dag_id=TARGET_DAG_ID,
        reset_dag_run=True,
        wait_for_completion=False,
    )
    wait_items_today >> trigger_items

    # sales history
    wait_sales_today = PythonSensor(
        task_id="wait_sales_today",
        python_callable=make_exists_callable("skinport_sales_history"),
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )
    trigger_sales = TriggerDagRunOperator(
        task_id="trigger_sales_processing",
        trigger_dag_id=TARGET_DAG_ID,
        reset_dag_run=True,
        wait_for_completion=False,
    )
    wait_sales_today >> trigger_sales

    # bitskins market
    wait_bitskins_today = PythonSensor(
        task_id="wait_bitskins_today",
        python_callable=make_exists_callable("bitskins_market_raw"),
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )
    trigger_bitskins = TriggerDagRunOperator(
        task_id="trigger_bitskins_processing",
        trigger_dag_id=TARGET_DAG_ID,
        reset_dag_run=True,
        wait_for_completion=False,
    )
    wait_bitskins_today >> trigger_bitskins
