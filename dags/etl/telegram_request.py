from __future__ import annotations

import asyncio
import os
import time
from typing import Optional

import requests

# --- telethon (защитный импорт, чтобы DAG не падал при отсутствии пакета) ---
try:
    from minio import Minio
    from telethon import TelegramClient
    from telethon.tl.custom import Message
    from telethon.errors import SessionPasswordNeededError
except ModuleNotFoundError:  # airflow-init, где telethon ещё не установлен
    TelegramClient = None  # type: ignore
    Message = None  # type: ignore

    class SessionPasswordNeededError(Exception):  # type: ignore
        """Fallback, если telethon не установлен."""
        pass

from dotenv import load_dotenv

load_dotenv()
# -----------------------------------------------------------
# НАСТРОЙКИ – заполни под себя
# -----------------------------------------------------------

# твой api_id / api_hash / phone
API_ID: Optional[int] = os.getenv("API_ID")
API_HASH: Optional[str] = os.getenv("API_HASH")
PHONE: Optional[str] = os.getenv("PHONE")
BASE_DIR = os.path.dirname(__file__)
SESSION_PATH = os.path.join(BASE_DIR, "skinport_session")

# Строка URL, которую нужно послать в Избранное (твой огромный sales/history с AK-47)
REQUEST_URL = (
    "https://api.skinport.com/v1/sales/history?"
    "app_id=730&currency=EUR&market_hash_name="
    "AK-47+%7C+Blue+Laminate+%28Factory+New%29%2C"
    "AK-47+%7C+Bloodsport+%28Well-Worn%29%2C"
    "AK-47+%7C+Bloodsport+%28Minimal+Wear%29%2C"
    "AK-47+%7C+Bloodsport+%28Field-Tested%29%2C"
    "AK-47+%7C+Bloodsport+%28Factory+New%29%2C"
    "AK-47+%7C+Black+Laminate+%28Well-Worn%29%2C"
    "AK-47+%7C+Black+Laminate+%28Minimal+Wear%29%2C"
    "AK-47+%7C+Black+Laminate+%28Field-Tested%29%2C"
    "AK-47+%7C+Black+Laminate+%28Factory+New%29%2C"
    "AK-47+%7C+Black+Laminate+%28Battle-Scarred%29%2C"
    "AK-47+%7C+Baroque+Purple+%28Well-Worn%29%2C"
    "AK-47+%7C+Baroque+Purple+%28Minimal+Wear%29%2C"
    "AK-47+%7C+Baroque+Purple+%28Field-Tested%29%2C"
    "AK-47+%7C+Baroque+Purple+%28Factory+New%29%2C"
    "AK-47+%7C+Baroque+Purple+%28Battle-Scarred%29%2C"
    "AK-47+%7C+Asiimov+%28Well-Worn%29%2C"
    "AK-47+%7C+Asiimov+%28Minimal+Wear%29%2C"
    "AK-47+%7C+Asiimov+%28Field-Tested%29%2C"
    "AK-47+%7C+Asiimov+%28Factory+New%29%2C"
    "AK-47+%7C+Asiimov+%28Battle-Scarred%29%2C"
    "AK-47+%7C+Aquamarine+Revenge+%28Well-Worn%29%2C"
    "AK-47+%7C+Aquamarine+Revenge+%28Minimal+Wear%29%2C"
    "AK-47+%7C+Aquamarine+Revenge+%28Field-Tested%29%2C"
    "AK-47+%7C+Aquamarine+Revenge+%28Factory+New%29%2C"
    "AK-47+%7C+Aquamarine+Revenge+%28Battle-Scarred%29"
)

# Куда сохранять history.json (внутри контейнера Airflow)
DOWNLOAD_DIR = "/opt/airflow/data"

# Webhook, который пишет файл в Postgres
POSTGRES_WEBHOOK_URL: Optional[str] = None  # "https://your-host/webhook/import_skinport"

# MinIO (S3-совместимое хранилище)
MINIO_ENDPOINT: Optional[str] = "minio:9000"        # из docker-compose, внутри сети
MINIO_ACCESS_KEY: Optional[str] = "minio"
MINIO_SECRET_KEY: Optional[str] = "minio12345"
MINIO_BUCKET: str = "raw"
MINIO_SECURE: bool = False  # True, если MinIO за HTTPS


# -----------------------------------------------------------
# Вспомогательные функции
# -----------------------------------------------------------

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


async def _get_client() -> TelegramClient:
    """
    Создаёт и возвращает Telethon-клиент.

    Первый запуск попросит код из Telegram и (если включена 2FA) пароль.
    Дальше сессия будет сохраняться в SESSION_NAME.session.
    """
    if TelegramClient is None:
        raise RuntimeError(
            "telethon не установлен в этом окружении. "
            "Убедись, что он добавлен в _PIP_ADDITIONAL_REQUIREMENTS для Airflow."
        )

    if API_ID is None or API_HASH is None or PHONE is None:
        raise RuntimeError("Заполни API_ID, API_HASH и PHONE в конфиге.")

    client = TelegramClient(SESSION_PATH, API_ID, API_HASH)
    await client.connect()

    if not await client.is_user_authorized():
        # 1) просим код по SMS/Telegram
        await client.send_code_request(PHONE)
        code = input("Введите код из Telegram: ")

        try:
            # 2) пробуем залогиниться только по коду
            await client.sign_in(PHONE, code)
        except SessionPasswordNeededError:
            # 3) если включена 2FA – просим пароль
            password = input("У тебя включена двухфакторка.\nВведите пароль 2FA: ")
            await client.sign_in(password=password)

    return client


async def send_request_message(request_url: str) -> Message:
    """
    Отправляет строку-запрос в 'Избранное' (Saved Messages) и возвращает Message.
    """
    client = await _get_client()
    async with client:
        msg = await client.send_message("me", request_url)
        return msg


async def wait_for_json_on_message(
    msg: Message,
    download_dir: str,
    timeout_sec: int = 60,
    poll_interval: float = 2.0,
) -> str:
    """
    Ждём, пока к этому же сообщению Telegram не прикрутит history.json,
    потом качаем его.
    """
    ensure_dir(download_dir)
    client = msg.client  # тот же клиент

    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        # перечитываем сообщение по id – вдруг к нему прикрепили файл
        fresh: Optional[Message] = await client.get_messages("me", ids=msg.id)

        if fresh and fresh.document:
            # имя файла, если Telegram его знает
            file_name = getattr(fresh.file, "name", None)
            # MIME тип
            mime_type = getattr(fresh.file, "mime_type", "") or ""

            if (file_name and file_name.endswith(".json")) or "json" in mime_type:
                path = await fresh.download_media(file=download_dir)
                return path

        await asyncio.sleep(poll_interval)

    raise TimeoutError("Не дождались json-файла от Telegram для этого сообщения.")


async def send_request_and_download(request_url: str, download_dir: str) -> str:
    """
    Отправляет запрос-URL в Избранное и скачивает history.json в download_dir.

    Возвращает путь к скачанному файлу.
    """
    client = await _get_client()
    async with client:
        msg = await client.send_message("me", request_url)
        file_path = await wait_for_json_on_message(msg, download_dir)
        return file_path


def send_request_and_download_sync(
    request_url: str = REQUEST_URL,
    download_dir: str = DOWNLOAD_DIR,
) -> str:
    """
    Синхронная обёртка – удобно вызывать из Airflow.
    """
    return asyncio.run(send_request_and_download(request_url, download_dir))


def upload_to_postgres(file_path: str) -> None:
    """
    Шлём файл на HTTP-webhook, который уже пишет в PostgreSQL.

    Ожидается, что сервер принимает multipart/form-data:
    files={'file': open(..., 'rb')}.
    """
    if not POSTGRES_WEBHOOK_URL:
        print("POSTGRES_WEBHOOK_URL не задан – загрузку в Postgres пропускаем.")
        return

    with open(file_path, "rb") as f:
        resp = requests.post(
            POSTGRES_WEBHOOK_URL,
            files={"file": (os.path.basename(file_path), f, "application/json")},
            timeout=60,
        )
    resp.raise_for_status()
    print(f"Файл {file_path} отправлен на webhook, статус {resp.status_code}")


def get_minio_client() -> Minio:
    """
    Создаёт MinIO-клиент по настройкам выше.
    """
    if not MINIO_ENDPOINT or not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise RuntimeError(
            "MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY не заданы."
        )

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    return client


def upload_to_minio(file_path: str, object_name: Optional[str] = None) -> None:
    """
    Грузит файл в MinIO в бакет MINIO_BUCKET.

    object_name — имя объекта в бакете (если None, берётся имя файла).
    """
    client = get_minio_client()

    # создаём бакет, если его ещё нет
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    if object_name is None:
        object_name = os.path.basename(file_path)

    client.fput_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        file_path=file_path,
    )
    print(f"Файл {file_path} загружен в MinIO как {MINIO_BUCKET}/{object_name}")


if __name__ == "__main__":
    # для отладки вне Airflow
    downloaded_file = send_request_and_download_sync()
    print("Downloaded file:", downloaded_file)

    # upload_to_postgres(downloaded_file)
    # upload_to_minio(downloaded_file)