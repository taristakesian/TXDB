from __future__ import annotations

import datetime
import json

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# модуль – это твой файл telegram_skinport.py,
# он должен лежать в PYTHONPATH у Airflow (часто /opt/airflow/dags)
from dags.etl.telegram_skinport import (
    send_request_and_download_sync,
    upload_to_minio,
)

from dotenv import load_dotenv

load_dotenv()

MONGO_CONN_ID = "mongo_default"
MONGO_DB_NAME = "skinport_db"
MONGO_COLLECTION = "skinport_history"
POSTGRES_CONN_ID = "ods_postgres"
POSTGRES_TABLE = "skinport_history"


def _send_request_and_download(**context) -> str:
    """
    1) Отправляет REQUEST_URL в Избранное
    2) Ждёт, пока Telegram прикрепит history.json
    3) Скачивает файл в DOWNLOAD_DIR (из telegram_skinport.py)
    4) Возвращает путь к файлу (XCom)
    """
    file_path = send_request_and_download_sync()
    return file_path

def _upload_to_minio_task(**context) -> None:
    """
    Берёт путь к history.json из XCom и загружает его в MinIO.
    """
    ti = context["ti"]
    file_path: str = ti.xcom_pull(task_ids="send_request_and_download")

    if not file_path:
        raise RuntimeError("Не найден путь к файлу из XCom (send_request_and_download).")

    # Только MinIO
    upload_to_minio(file_path)


def _load_to_mongo(**context) -> None:
    """
    Читает history.json и записывает документы в MongoDB через MongoHook.
    """
    ti = context["ti"]
    file_path: str = ti.xcom_pull(task_ids="send_request_and_download")

    if not file_path:
        raise RuntimeError("Не найден путь к файлу из XCom (send_request_and_download).")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # допускаем, что в файле либо список, либо один объект
    if isinstance(data, dict):
        docs = [data]
    else:
        docs = list(data)

    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    collection = hook.get_collection(MONGO_DB_NAME, MONGO_COLLECTION)

    if docs:
        result = collection.insert_many(docs)
        print(f"В Mongo записано {len(result.inserted_ids)} документов.")
    else:
        print("Файл пустой, в Mongo ничего не пишем.")


def _load_to_postgres(**context) -> None:
    """
    Читает history.json и пишет его в Postgres как jsonb через PostgresHook.
    """
    ti = context["ti"]
    file_path: str = ti.xcom_pull(task_ids="send_request_and_download")

    if not file_path:
        raise RuntimeError("Не найден путь к файлу из XCom (send_request_and_download).")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict):
        rows = [json.dumps(data)]
    else:
        rows = [json.dumps(item) for item in data]

    if not rows:
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

    # вставляем строки (по одной jsonb в поле payload)
    hook.insert_rows(
        table=POSTGRES_TABLE,
        rows=[(row,) for row in rows],
        target_fields=["payload"],
        commit_every=1000,
    )
    print(f"В Postgres ({POSTGRES_TABLE}) записано {len(rows)} строк.")


default_args = {
    "owner": "tigran",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=10),
}

start_date = datetime.datetime(2025, 11, 6)
end_date = start_date + datetime.timedelta(days=30)

with DAG(
    dag_id="skinport_via_telegram",
    default_args=default_args,
    description="Telegram → history.json → MinIO + Mongo + Postgres",
    start_date=start_date,
    end_date=end_date,
    schedule="@daily",
    catchup=True,
) as dag:

    send_request_and_download = PythonOperator(
        task_id="send_request_and_download",
        python_callable=_send_request_and_download,
    )

    upload_to_minio_op = PythonOperator(
        task_id="upload_to_minio",
        python_callable=_upload_to_minio_task,
    )

    load_to_mongo_op = PythonOperator(
        task_id="load_to_mongo",
        python_callable=_load_to_mongo,
    )

    load_to_postgres_op = PythonOperator(
        task_id="load_to_postgres",
        python_callable=_load_to_postgres,
    )

    # одна точка входа (скачивание файла) → три независимых "ручки"
    send_request_and_download >> [upload_to_minio_op, load_to_mongo_op, load_to_postgres_op]