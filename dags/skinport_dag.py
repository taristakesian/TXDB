from __future__ import annotations

import datetime
import json
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv
import pendulum

# Убедитесь, что этот модуль доступен в PYTHONPATH вашего Airflow
from etl.telegram_request import (
    send_request_and_download_sync,
    upload_to_minio,
)

load_dotenv()

# --- ОБЩИЕ КОНСТАНТЫ ---
MONGO_CONN_ID = "mongo_default"
MONGO_DB_NAME = "skinport_db"
POSTGRES_CONN_ID = "ods_postgres"

# --- URL ДЛЯ API ЗАПРОСОВ ---
REQUEST_URL_ALL_ITEMS = "https://api.skinport.com/v1/items?app_id=730&currency=USD"

REQUEST_URL_OUT_OF_STOCK = "https://api.skinport.com/v1/sales/out-of-stock?app_id=730&currency=USD"

REQUEST_URL_SALES = (
    "https://api.skinport.com/v1/sales/history?"
    "app_id=730&currency=USD"
    # "&market_hash_name="
    # "AK-47+%7C+Blue+Laminate+%28Factory+New%29%2C"
    # "AK-47+%7C+Bloodsport+%28Well-Worn%29%2C"
    # "AK-47+%7C+Bloodsport+%28Minimal+Wear%29%2C"
    # "AK-47+%7C+Bloodsport+%28Field-Tested%29%2C"
    # "AK-47+%7C+Bloodsport+%28Factory+New%29%2C"
    # "AK-47+%7C+Black+Laminate+%28Well-Worn%29%2C"
    # "AK-47+%7C+Black+Laminate+%28Minimal+Wear%29%2C"
    # "AK-47+%7C+Black+Laminate+%28Field-Tested%29%2C"
    # "AK-47+%7C+Black+Laminate+%28Factory+New%29%2C"
    # "AK-47+%7C+Black+Laminate+%28Battle-Scarred%29%2C"
    # "AK-47+%7C+Baroque+Purple+%28Well-Worn%29%2C"
    # "AK-47+%7C+Baroque+Purple+%28Minimal+Wear%29%2C"
    # "AK-47+%7C+Baroque+Purple+%28Field-Tested%29%2C"
    # "AK-47+%7C+Baroque+Purple+%28Factory+New%29%2C"
    # "AK-47+%7C+Baroque+Purple+%28Battle-Scarred%29%2C"
    # "AK-47+%7C+Asiimov+%28Well-Worn%29%2C"
    # "AK-47+%7C+Asiimov+%28Minimal+Wear%29%2C"
    # "AK-47+%7C+Asiimov+%28Field-Tested%29%2C"
    # "AK-47+%7C+Asiimov+%28Factory+New%29%2C"
    # "AK-47+%7C+Asiimov+%28Battle-Scarred%29%2C"
    # "AK-47+%7C+Aquamarine+Revenge+%28Well-Worn%29%2C"
    # "AK-47+%7C+Aquamarine+Revenge+%28Minimal+Wear%29%2C"
    # "AK-47+%7C+Aquamarine+Revenge+%28Field-Tested%29%2C"
    # "AK-47+%7C+Aquamarine+Revenge+%28Factory+New%29%2C"
    # "AK-47+%7C+Aquamarine+Revenge+%28Battle-Scarred%29"
)

# --- ОБЩИЕ ФУНКЦИИ (PythonCallables) ---

def _send_request_and_download(request_url: str) -> str:
    """Отправляет запрос и скачивает файл, возвращая путь для XCom."""
    print(f"Отправляем запрос для URL: {request_url}")
    file_path = send_request_and_download_sync(request_url)
    return file_path


def _rename_with_ds(
    task_id_for_xcom: str,
    prefix: str | None = None,
    date_source: str = "now",  # "now" | "logical" | "interval_end"
    **context
) -> str:
    """
    Переименовывает скачанный файл, добавляя дату к имени.
    date_source:
      - "now"          — текущая дата во время выполнения в таймзоне DAG (то, что ты хочешь)
      - "logical"      — логическая дата Airflow (старый behavior: context['ds'])
      - "interval_end" — конец дата-интервала (часто = сегодняшнее число для daily)
    """
    ti = context["ti"]
    dag_tz = context["dag"].timezone  # таймзона DAG из конфигурации (Europe/Helsinki у тебя)
    src: str = ti.xcom_pull(task_ids=task_id_for_xcom)
    if not src:
        raise RuntimeError(f"Не найден исходный файл из XCom (задача: {task_id_for_xcom}).")

    if date_source == "now":
        stamp = pendulum.now(dag_tz.name).format("YYYY-MM-DD")
    elif date_source == "interval_end":
        stamp = context["data_interval_end"].in_timezone(dag_tz).to_date_string()
    else:  # "logical"
        stamp = context["ds"]  # логическая дата, как было

    src_path = Path(src)
    dst_dir = src_path.parent
    stem = prefix if prefix else src_path.stem
    dst_path = dst_dir / f"{stem}_{stamp}{src_path.suffix}"

    if dst_path.exists():
        dst_path.unlink()
    os.rename(src_path, dst_path)
    print(f"Файл переименован: {src_path} -> {dst_path}")
    return str(dst_path)


def _upload_to_minio_task(task_id_for_xcom: str, **context) -> None:
    """Загружает файл из XCom в MinIO."""
    ti = context["ti"]
    file_path: str = ti.xcom_pull(task_ids=task_id_for_xcom)
    if not file_path:
        raise RuntimeError(f"Не найден путь к файлу из XCom (задача: {task_id_for_xcom}).")
    
    print(f"Загружаем файл {file_path} в MinIO.")
    upload_to_minio(file_path)


def _load_to_mongo(task_id_for_xcom: str, collection_name: str, **context) -> None:
    """Загружает данные из файла (XCom) в указанную коллекцию MongoDB."""
    ti = context["ti"]
    file_path: str = ti.xcom_pull(task_ids=task_id_for_xcom)
    if not file_path:
        raise RuntimeError(f"Не найден путь к файлу из XCom (задача: {task_id_for_xcom}).")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    docs = [data] if isinstance(data, dict) else list(data)
    if not docs:
        print("Файл пустой, в Mongo ничего не пишем.")
        return

    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    collection = hook.get_collection(MONGO_DB_NAME, collection_name)
    result = collection.insert_many(docs)
    print(f"В Mongo ({collection_name}) записано {len(result.inserted_ids)} документов.")


def _load_to_postgres(task_id_for_xcom: str, table_name: str, **context) -> None:
    """Загружает данные из файла (XCom) в указанную таблицу Postgres."""
    ti = context["ti"]
    file_path: str = ti.xcom_pull(task_ids=task_id_for_xcom)
    if not file_path:
        raise RuntimeError(f"Не найден путь к файлу из XCom (задача: {task_id_for_xcom}).")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = [json.dumps(data)] if isinstance(data, dict) else [json.dumps(item) for item in data]
    if not rows:
        print("Файл пустой, в Postgres ничего не пишем.")
        return

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id          BIGSERIAL PRIMARY KEY,
            payload     JSONB NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    hook.insert_rows(
        table=table_name,
        rows=[(row,) for row in rows],
        target_fields=["payload"],
        commit_every=1000,
    )
    print(f"В Postgres ({table_name}) записано {len(rows)} строк.")


# --- ОБЩИЕ АРГУМЕНТЫ ДЛЯ DAG'ов ---
default_args = {
    "owner": "tigran",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=2),
}
start_date = datetime.datetime(2025, 11, 6)
end_date = start_date + datetime.timedelta(days=30)

# --- DAG 1: ВЫГРУЗКА ВСЕХ ПРЕДМЕТОВ ---
with DAG(
    dag_id="skinport_all_items_etl",
    default_args=default_args,
    description="Загрузка всех предметов в продаже с Skinport",
    start_date=start_date,
    end_date=end_date,
    schedule="@daily",
    catchup=False,
    tags=["skinport", "api"],
) as dag1:
    TASK_ID_DOWNLOAD = "download_all_items"
    TASK_ID_RENAME = "stamp_all_items"

    download_task = PythonOperator(
        task_id=TASK_ID_DOWNLOAD,
        python_callable=_send_request_and_download,
        op_kwargs={"request_url": REQUEST_URL_ALL_ITEMS},
    )

    rename_task = PythonOperator(
        task_id=TASK_ID_RENAME,
        python_callable=_rename_with_ds,
        op_kwargs={"task_id_for_xcom": TASK_ID_DOWNLOAD, "prefix": "skinport_items", "date_source": "now"},
        provide_context=True,
    )

    upload_to_minio_op = PythonOperator(
        task_id="upload_all_items_to_minio",
        python_callable=_upload_to_minio_task,
        op_kwargs={"task_id_for_xcom": TASK_ID_RENAME},
        provide_context=True,
    )

    load_to_mongo_op = PythonOperator(
        task_id="load_all_items_to_mongo",
        python_callable=_load_to_mongo,
        op_kwargs={"task_id_for_xcom": TASK_ID_RENAME, "collection_name": "skinport_items"},
        provide_context=True,
    )

    load_to_postgres_op = PythonOperator(
        task_id="load_all_items_to_postgres",
        python_callable=_load_to_postgres,
        op_kwargs={"task_id_for_xcom": TASK_ID_RENAME, "table_name": "skinport_items"},
        provide_context=True,
    )

    download_task >> rename_task >> [upload_to_minio_op, load_to_mongo_op, load_to_postgres_op]

# --- DAG 2: ВЫГРУЗКА ИСТОРИИ ПРОДАЖ ---
with DAG(
    dag_id="skinport_sales_history_etl",
    default_args=default_args,
    description="Загрузка истории продаж с Skinport",
    start_date=start_date,
    end_date=end_date,
    schedule="@daily",
    catchup=False,
    tags=["skinport", "api"],
) as dag2:
    TASK_ID_DOWNLOAD = "download_sales_history"
    TASK_ID_RENAME = "stamp_sales_history"

    download_task = PythonOperator(
        task_id=TASK_ID_DOWNLOAD,
        python_callable=_send_request_and_download,
        op_kwargs={"request_url": REQUEST_URL_SALES},
    )

    rename_task = PythonOperator(
        task_id=TASK_ID_RENAME,
        python_callable=_rename_with_ds,
        op_kwargs={"task_id_for_xcom": TASK_ID_DOWNLOAD, "prefix": "skinport_sales_history", "date_source": "now"},
        provide_context=True,
    )

    upload_to_minio_op = PythonOperator(
        task_id="upload_sales_history_to_minio",
        python_callable=_upload_to_minio_task,
        op_kwargs={"task_id_for_xcom": TASK_ID_RENAME},
        provide_context=True,
    )

    load_to_mongo_op = PythonOperator(
        task_id="load_sales_history_to_mongo",
        python_callable=_load_to_mongo,
        op_kwargs={"task_id_for_xcom": TASK_ID_RENAME, "collection_name": "skinport_sales_history"},
        provide_context=True,
    )

    load_to_postgres_op = PythonOperator(
        task_id="load_sales_history_to_postgres",
        python_callable=_load_to_postgres,
        op_kwargs={"task_id_for_xcom": TASK_ID_RENAME, "table_name": "skinport_sales_history"},
        provide_context=True,
    )

    download_task >> rename_task >> [upload_to_minio_op, load_to_mongo_op, load_to_postgres_op]
