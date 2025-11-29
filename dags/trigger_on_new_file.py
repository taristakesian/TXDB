from __future__ import annotations
import os
import pendulum
import datetime
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

WATCH_DIR = "/opt/airflow/data"           # где лежат файлы
TARGET_DAG_ID = "etl_from_postgres_only"  # какой DAG триггерим

default_args = {"owner": "tigran", "retries": 0}

def make_exists_callable(prefix: str):
    """Проверка наличия файла prefix_YYYY-MM-DD.json за 'сегодня' (по таймзоне DAG)."""
    def _check(**context) -> bool:
        tz = context["dag"].timezone
        stamp = pendulum.now(tz.name).format("YYYY-MM-DD")
        path = os.path.join(WATCH_DIR, f"{prefix}_{stamp}.json")
        context["ti"].log.info("Checking file: %s exists=%s", path, os.path.exists(path))
        return os.path.exists(path)
    return _check

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
