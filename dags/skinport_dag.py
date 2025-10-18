from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

# Пути внутри контейнера Airflow
DAGS_DIR   = Path(__file__).parent
SCRIPT     = DAGS_DIR / "etl" / "etl_jobs.py"
NAMES_FILE = DAGS_DIR / "config" / "skinport_names.txt"

# Переменные/настройки
DB_PATH       = Variable.get("CS2_DB_PATH", default_var=str(DAGS_DIR / "TXDB" / "cs2_market.db"))
STEAM_COOKIE  = Variable.get("STEAM_COOKIE", default_var="")  # положи сюда свои куки Steam (UI: Admin->Variables)
TRADABLE_ONLY = Variable.get("SKINPORT_TRADABLE_ONLY", default_var="False").lower() == "true"
IMAGES_LIMIT  = int(Variable.get("SKINPORT_IMAGES_LIMIT", default_var="150"))
IMAGES_SLEEP  = float(Variable.get("SKINPORT_IMAGES_SLEEP", default_var="0.2"))

default_args = {
    "owner": "tigran",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="skinport_pipeline",
    start_date=datetime(2025, 10, 1),
    schedule="0 6 * * *",          # ежедневно 06:00 (меняй при необходимости)
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["cs2", "skinport", "etl"],
) as dag:

    # 0) проверка наличия файла с именами и (опц.) cookie для истории
    def _history_ready() -> bool:
        has_file = NAMES_FILE.exists() and NAMES_FILE.stat().st_size > 0
        has_cookie = bool(STEAM_COOKIE)
        # если нет cookie — историю пропускаем, остальные задачи всё-равно выполнятся
        return has_file and has_cookie

    history_ready = ShortCircuitOperator(
        task_id="history_ready",
        python_callable=_history_ready,
        ignore_downstream_trigger_rules=False,
    )

    # 1) Ассортимент (Skinport /v1/items). По умолчанию также подтягивает агр. историю и складывает как справку
    load_catalog = BashOperator(
        task_id="load_catalog",
        bash_command=(
            "python {{params.script}} load_catalog "
            + ("--tradable-only " if TRADABLE_ONLY else "")
            # если не нужны агрегаты Skinport — добавь --no-agg
        ),
        params={"script": str(SCRIPT)},
        env={
            "CS2_DB_PATH": DB_PATH,
            # можно прокинуть и иные ENV для клиентов
        },
    )

    # 2) НЕагрегированная история (Steam pricehistory) — только если есть файл и cookie
    load_history = BashOperator(
        task_id="load_history",
        bash_command="python {{params.script}} load_history --names-file {{params.names_file}}",
        params={"script": str(SCRIPT), "names_file": str(NAMES_FILE)},
        env={
            "CS2_DB_PATH": DB_PATH,
            "STEAM_COOKIE": STEAM_COOKIE,  # Variable в UI
        },
    )

    # 3) Фото отдельно (og:image со страниц Skinport)
    load_images = BashOperator(
        task_id="load_images",
        bash_command="python {{params.script}} load_images --limit {{params.limit}} --sleep {{params.sleep}}",
        params={"script": str(SCRIPT), "limit": IMAGES_LIMIT, "sleep": IMAGES_SLEEP},
        env={"CS2_DB_PATH": DB_PATH},
    )

    # Граф задач: ассортимент → (история при готовности) → фото
    load_catalog >> history_ready >> load_history >> load_images
