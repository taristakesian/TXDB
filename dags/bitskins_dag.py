from __future__ import annotations

import datetime
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

from etl.bitskins_client import BitSkinsClient


BITSKINS_API_KEY = Variable.get("BITSKINS_API_KEY")
POSTGRES_CONN_ID = "ods_postgres"          # твой conn_id из Airflow
POSTGRES_TABLE = "bitskins_market_raw"     # таблица в ODS


def _extract_bitskins(**context) -> str:
    """
    Тянем данные с BitSkins и сохраняем в JSON-файл.
    Возвращаем путь к файлу через XCom.
    """
    client = BitSkinsClient(amount_skins_unload=10, api_key=BITSKINS_API_KEY)
    items = client.get_items()

    out_dir = "/opt/airflow/data/bitskins"
    os.makedirs(out_dir, exist_ok=True)

    ds = context["ds"]  # дата выполнения DAG (YYYY-MM-DD)
    file_path = os.path.join(out_dir, f"bitskins_{ds}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False)

    print("Saved BitSkins data to:", file_path)
    return file_path


def _load_bitskins_to_postgres(**context) -> None:
    """
    Берём JSON-файл из XCom и пишем его в ODS Postgres как jsonb.
    """
    ti = context["ti"]
    file_path: str = ti.xcom_pull(task_ids="extract_bitskins")

    if not file_path:
        raise RuntimeError("Не найден путь к файлу из XCom (extract_bitskins).")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict):
        rows_json = [json.dumps(data)]
    else:
        rows_json = [json.dumps(row) for row in data]

    if not rows_json:
        print("Файл пустой, в Postgres ничего не пишем.")
        return

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # создаём таблицу, если её ещё нет
    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
            id          BIGSERIAL PRIMARY KEY,
            payload     JSONB NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    # вставляем строки
    hook.insert_rows(
        table=POSTGRES_TABLE,
        rows=[(row,) for row in rows_json],
        target_fields=["payload"],
        commit_every=1000,
    )

    print(f"В Postgres ({POSTGRES_TABLE}) записано {len(rows_json)} строк.")


default_args = {
    "owner": "tigran",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=2),
}

start_date = datetime.datetime(2025, 11, 6)

with DAG(
    dag_id="bitskins_market_to_ods",
    default_args=default_args,
    description="Выгрузка BitSkins → ODS Postgres",
    start_date=start_date,
    schedule="@daily",
    catchup=False,
) as dag:

    extract_bitskins = PythonOperator(
        task_id="extract_bitskins",
        python_callable=_extract_bitskins,
    )

    load_bitskins_to_postgres = PythonOperator(
        task_id="load_bitskins_to_postgres",
        python_callable=_load_bitskins_to_postgres,
    )

    extract_bitskins >> load_bitskins_to_postgres
